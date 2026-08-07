package accesslog

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/net/http2"
)

// maxSniffBody caps how many bytes of a request body we will read to extract a
// UE id. The bodies we sniff (AuthenticationInfo, PolicyAssociationRequest) are
// well under 1 KiB; this guards against ever buffering a large/unexpected body.
const maxSniffBody = 8 << 10 // 8 KiB

// readIdleTimeoutPeriod / timeoutPeriod mirror the values used by
// free5gc/openapi's internal HTTP/2 clients. pingTimeoutPeriod deliberately
// does NOT: openapi uses 1s, and this is raised to 3s.
//
// After ReadIdleTimeout of no frames arriving, the transport sends a PING and
// tears the connection down if no PONG comes back within PingTimeout. At 1s
// that check was firing on connections that were merely busy rather than dead:
// a peer under load could not turn a PING around inside a second, so healthy
// connections were killed and redialled mid-run (observed in
// Ty_log/Free5gc/C6525100g_HTTPconnum_0806, where the main UDM->UDR connection
// died at +813.6ms and a replacement took over 2.8ms later). Every such kill
// re-splits traffic onto a fresh socket and muddies the per-connection
// measurements this experiment exists to take.
//
// 3s is a compromise: still well below Go's 15s default, so a genuinely dead
// peer is detected promptly, but wide enough that ordinary head-of-line delay
// on a loaded connection no longer reads as failure. This does not eliminate
// reconnects -- it only removes the ones caused by the health check being
// impatient.
const (
	readIdleTimeoutPeriod = 1 * time.Second
	pingTimeoutPeriod     = 3 * time.Second
	timeoutPeriod         = 10 * time.Second
)

// connsPerPeer is how many HTTP/2 connections this NF opens to each peer NF up
// front, and how many round-robin slots requests are dealt across. It is 4.
//
// Each slot is a separate http2.Transport with its own private pool, so N slots
// mean N connections held from the start, with requests handed to them one after
// another in turn.
//
// The history matters for reading this number. It was 2 for the original
// round-robin experiment (HTTP_MULTI_CONN_ROUNDROBIN_PLAN_0806.md), then went
// back to 1 (HTTP2_IDLETIMEOUT_FIX_PLAN_0807.md) once that comparison turned out
// never to have run as designed: the server's 1ms IdleTimeout tore every
// connection down between requests, so each slot handed out a freshly dialled
// socket every time instead of holding one -- measured at RQ5/UE10 as exactly
// 1.0 requests per socket, 84 requests over UDM->UDR opening 84 connections.
// With the server-side IdleTimeout now at 500ms, connections survive the gaps
// between requests, so N slots finally mean N concurrent long-lived sockets.
// 4 is what this experiment measures against that fixed baseline of 1.
//
// Growth beyond these 4 is still permitted and expected:
// StrictMaxConcurrentStreams is deliberately left unset (see below), so when a
// slot's in-flight streams reach the peer's 250-stream limit the transport dials
// an additional connection by itself. 4 is a floor, not a cap -- which is why
// conn_slot rather than conn is the field that shows whether the split is even.
const connsPerPeer = 4

// loggingRoundTripper wraps separate HTTP/2 transports for https (h2) and
// cleartext (h2c), choosing per request by URL scheme exactly like
// openapi.CallAPI's inner clients do, and records one HTTP access-log entry per
// request from the requester's (this NF's) point of view.
type loggingRoundTripper struct {
	// One slot per connection to each peer. Every element is a SEPARATE
	// http2.Transport with its own pool, which is what makes them distinct TCP
	// connections rather than one shared one.
	tls   [connsPerPeer]http.RoundTripper // h2 over TLS  (https)
	clear [connsPerPeer]http.RoundTripper // h2c cleartext (http)

	// next is the round-robin cursor, shared by both schemes. Atomic rather
	// than mutex-guarded: this is on every request's path, and an atomic add is
	// a single instruction with no contention point, so the cost does not grow
	// with concurrency the way lock acquisition would.
	next atomic.Uint64
}

func newLoggingRoundTripper() *loggingRoundTripper {
	l := &loggingRoundTripper{}
	for i := 0; i < connsPerPeer; i++ {
		// Each iteration builds a SEPARATE http2.Transport. Field values are
		// identical across slots; only the instance identity differs, and that
		// is precisely what yields one connection per slot.
		// StrictMaxConcurrentStreams is deliberately NOT set here, i.e. it keeps
		// its default of false. Setting it true was tried (see
		// Ty_log/Free5gc/C6525100g_NFHTTPonly1conn_0806v1) and made things
		// worse for measurement: instead of holding one steady connection the
		// pair churned through 2-7 of them, each living only 25-120ms before
		// being torn down and replaced. Blocked-in-RoundTrip requests keep a
		// connection idle enough for the ping health check to time out and kill
		// it, so the "limit" produced a stream of short-lived sockets and made
		// the logs harder to read rather than easier. (That run predates the
		// PingTimeout increase above and used the old 1s value, which is part of
		// why it churned so hard; Strict is still not re-enabled here.)
		//
		// With the default, a transport may dial an extra connection when its
		// in-flight stream count reaches the peer's limit. That is accepted:
		// connsPerPeer sets how many connections are held from the start, so
		// that the number in use can be compared against the single-connection
		// baseline; it is not meant to cap the total.
		l.tls[i] = &http2.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // matches openapi default
			ReadIdleTimeout: readIdleTimeoutPeriod,
			PingTimeout:     pingTimeoutPeriod,
		}
		l.clear[i] = &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				d := &net.Dialer{}
				return d.DialContext(ctx, network, addr)
			},
			ReadIdleTimeout: readIdleTimeoutPeriod,
			PingTimeout:     pingTimeoutPeriod,
		}
	}
	return l
}

func (l *loggingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// Take the address rather than the array: a Go array is a value, so
	// assigning it would copy every element on each request.
	pool := &l.clear
	if req.URL != nil && req.URL.Scheme == "https" {
		pool = &l.tls
	}

	// Round-robin over the connsPerPeer transports. The cursor is shared
	// between the tls and clear pools; this deployment is http-only, so only
	// the clear pool is ever indexed in practice and sharing costs nothing.
	//
	// Add returns the value AFTER incrementing, so subtracting 1 makes the
	// first request land on slot 0 and keeps connSlot 0-based in the log.
	connSlot := int((l.next.Add(1) - 1) % connsPerPeer)
	base := pool[connSlot]

	dst := dstNFFromURL(req)
	method := req.Method
	uri := ""
	if req.URL != nil {
		uri = req.URL.String()
	}

	// For the few request types whose UE id lives only in the request body
	// (not the URI), sniff the body before sending and recover the UE id. The
	// body is fully buffered and restored so the outgoing request is unchanged.
	ueID := sniffUEID(req)

	// wroteTime records the instant every frame of this request (HEADERS plus
	// all DATA) has been handed to the kernel socket buffer. Splitting the
	// request leg at this point separates sender-side queueing (waiting for the
	// shared clientConn write lock) from everything that happens afterwards in
	// the kernel and on the receiving NF.
	//
	// gotFirstByte records when the response HEADERS reached this process's
	// HTTP/2 read loop. It splits the response leg the same way: what precedes
	// it is the peer's write path plus the wire, what follows it is this process
	// receiving the body and waking the goroutine blocked below in RoundTrip.
	//
	// Both callbacks are request-scoped, not frame-scoped: WroteRequest fires
	// once after the last frame is written, GotFirstResponseByte once on the
	// first response byte. Each closure captures this call's own local variable,
	// so concurrent RoundTrips never interfere and no correlation id is needed.
	//
	// Neither callback may log or block: WroteRequest runs on the stream's write
	// goroutine and GotFirstResponseByte on the connection's single read loop,
	// which serves every stream on that connection. Any I/O there would stall
	// all of them. They only stamp a local variable; the record is enqueued
	// after RoundTrip returns, on the normal asynchronous path.
	//
	// The transport may write the request more than once (an idempotent request
	// retried after a connection error); keep the first write so the recorded
	// value always pairs with reqTime below.
	var wroteTime, gotFirstByte time.Time

	// connID identifies the TCP connection this request went out on, as
	// "localIP:localPort". A socket's local port is unique within this process
	// for the socket's whole lifetime, so grouping log lines by connID recovers
	// exactly which requests shared a connection.
	//
	// connReused reports whether the transport handed back an existing
	// connection (true) or had to establish a new one (false). Every false is
	// the birth of a new connection, so the timestamps of the false records show
	// WHEN the pool grew — which is what distinguishes load-driven expansion
	// from a burst of dials at start-up.
	var connID string
	var connReused bool

	trace := &httptrace.ClientTrace{
		// GotConn fires once, after the transport has picked (or dialled) the
		// connection for this request and before the request is written. Unlike
		// the two callbacks below it runs on this calling goroutine, not on a
		// shared loop, but it follows the same rule anyway: stamp locals only,
		// never log or block.
		//
		// Deliberately no IsZero()-style guard here. wroteTime keeps its first
		// value because a retry must still pair with reqTime; for the
		// connection the opposite is wanted — a retry means a different
		// connection, and the last one is the one that actually carried the
		// request, so overwriting is correct.
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Conn != nil {
				connID = info.Conn.LocalAddr().String()
			}
			connReused = info.Reused
		},
		WroteRequest: func(httptrace.WroteRequestInfo) {
			if wroteTime.IsZero() {
				wroteTime = time.Now()
			}
		},
		GotFirstResponseByte: func() {
			gotFirstByte = time.Now()
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	reqTime := time.Now()
	resp, err := base.RoundTrip(req)
	respTime := time.Now()

	// Always log, even on transport error, so failed attempts are visible.
	LogHTTP(dst, method, uri, ueID, connID, connSlot, connReused, reqTime, wroteTime, gotFirstByte, respTime)
	return resp, err
}

// sniffUEID returns the UE id (e.g. "imsi-999700000000001" / "suci-0-999-...")
// for request types that carry it only in the body, or "" otherwise. It only
// buffers the body for the small set of known endpoints, so every other request
// is untouched and pays no cost. When it does read the body, it restores it so
// the request can still be sent normally.
func sniffUEID(req *http.Request) string {
	if req.Method != http.MethodPost || req.URL == nil || req.Body == nil {
		return ""
	}
	field, ok := bodyUEIDField(req.URL.Path)
	if !ok {
		return ""
	}

	body, err := io.ReadAll(io.LimitReader(req.Body, maxSniffBody))
	_ = req.Body.Close()
	// Restore the body (and GetBody, used by the HTTP/2 transport when it has to
	// retry the request) from the bytes we buffered, so the outgoing request is
	// byte-for-byte unchanged whether or not it is later retried.
	restoreBody(req, body)
	if err != nil {
		return ""
	}

	return extractStringField(body, field)
}

// restoreBody resets req.Body, req.GetBody and req.ContentLength to serve the
// given bytes. The HTTP/2 transport calls GetBody() to obtain a fresh reader
// when it retries an idempotent request after a connection-level error, so both
// Body and GetBody must point at the same buffered bytes.
func restoreBody(req *http.Request, body []byte) {
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
}

// bodyUEIDField maps a request path to the JSON field that holds the UE id in
// that request's body, for the endpoints whose URI does not carry the UE id.
//   - POST /nausf-auth/v1/ue-authentications        -> AuthenticationInfo.supiOrSuci
//   - POST /npcf-am-policy-control/v1/policies       -> PolicyAssociationRequest.supi
func bodyUEIDField(path string) (string, bool) {
	switch {
	case strings.HasSuffix(path, "/nausf-auth/v1/ue-authentications"):
		return "supiOrSuci", true
	case strings.HasSuffix(path, "/npcf-am-policy-control/v1/policies"):
		return "supi", true
	}
	return "", false
}

// extractStringField pulls a single top-level string field out of a small JSON
// object body. Returns "" if the body is not valid JSON or the field is absent.
func extractStringField(body []byte, field string) string {
	// Decode into a generic map; these bodies are tiny so this is cheap and
	// robust to field ordering / extra fields.
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err != nil {
		return ""
	}
	raw, ok := obj[field]
	if !ok {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}
	return s
}

// InboundLogger returns a gin middleware that records one HTTP access-log entry
// per *incoming* request from the receiver's (this NF's, the server's) point of
// view. It is the server-side counterpart of the loggingRoundTripper and writes
// to the SAME HTTP_log.txt with the SAME fields (src is "NaN" because the sender
// NF cannot be identified server-side; dst is this NF).
//
// Register it once, right after the existing inbound middleware, e.g.:
//
//	router.Use(metrics.InboundMetrics())
//	router.Use(accesslog.InboundLogger())
func InboundLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		method := c.Request.Method
		uri := inboundURI(c.Request)

		// Timestamp first: sniffing reads and unmarshals the whole body, and that
		// cost belongs to this NF's own processing, not to the request's journey
		// from the caller. Taking reqTime beforehand keeps the request leg free
		// of it.
		reqTime := time.Now()

		// For the few request types whose UE id lives only in the body, sniff it
		// before the handler runs and restore the body so the handler is
		// unaffected. Every other request is untouched and pays no cost.
		ueID := sniffInboundUEID(c.Request)

		c.Next()
		respTime := time.Now()

		LogHTTPInbound(method, uri, ueID, reqTime, respTime)
	}
}

// inboundURI reconstructs a full request URI for an incoming server request so
// it matches the client-view "uri" (which is req.URL.String(), i.e. scheme + host
// + path + query). Server requests have an empty URL.Scheme/Host, so we fill them
// from the connection (TLS => https) and the Host header.
func inboundURI(req *http.Request) string {
	if req == nil || req.URL == nil {
		return ""
	}
	u := *req.URL // shallow copy; do not mutate the request's URL
	if u.Host == "" {
		u.Host = req.Host
	}
	if u.Scheme == "" {
		if req.TLS != nil {
			u.Scheme = "https"
		} else {
			u.Scheme = "http"
		}
	}
	return u.String()
}

// sniffInboundUEID returns the UE id for incoming request types that carry it
// only in the body, or "" otherwise. Mirrors sniffUEID but reads the server-side
// request body and restores it so the gin handler can still read it.
func sniffInboundUEID(req *http.Request) string {
	if req == nil || req.Method != http.MethodPost || req.URL == nil || req.Body == nil {
		return ""
	}
	field, ok := bodyUEIDField(req.URL.Path)
	if !ok {
		return ""
	}

	body, err := io.ReadAll(io.LimitReader(req.Body, maxSniffBody))
	_ = req.Body.Close()
	// Restore the body so the downstream handler reads the same bytes.
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	if err != nil {
		return ""
	}
	return extractStringField(body, field)
}

// Client returns an *http.Client that logs every request and otherwise behaves
// like free5gc/openapi's internal HTTP/2 clients. Inject it into a service
// Configuration via configuration.SetHTTPClient(accesslog.Client()).
//
// A single shared client is returned so connection pools are reused across all
// service configurations within the NF.
func Client() *http.Client {
	return sharedClient
}

var sharedClient = &http.Client{
	Transport: newLoggingRoundTripper(),
	Timeout:   timeoutPeriod,
}

// dstNFFromURL derives the destination NF name from the request URL path. SBI
// URIs look like /namf-comm/v1/..., /nudm-sdm/v2/..., /nnrf-nfm/v1/... ; the
// "n<nf>-..." service prefix's <nf> is the destination NF. Falls back to the
// host if the prefix is not recognized.
func dstNFFromURL(req *http.Request) string {
	if req.URL == nil {
		return ""
	}
	path := req.URL.Path
	// strip leading slash and take the first segment, e.g. "nudm-sdm"
	seg := path
	if i := strings.IndexByte(strings.TrimPrefix(seg, "/"), '/'); i >= 0 {
		seg = strings.TrimPrefix(seg, "/")[:i]
	} else {
		seg = strings.TrimPrefix(seg, "/")
	}
	if nf, ok := nfFromServicePrefix(seg); ok {
		return nf
	}
	return req.URL.Host
}

// nfFromServicePrefix maps an SBI service prefix segment (e.g. "nudm-sdm") to
// the owning NF name. Covers the registration-path services.
func nfFromServicePrefix(seg string) (string, bool) {
	if !strings.HasPrefix(seg, "n") {
		return "", false
	}
	// seg is like "nudm-sdm", "nnrf-nfm", "namf-comm", "nausf-auth", "nudr-dr"
	body := seg[1:]
	dash := strings.IndexByte(body, '-')
	if dash <= 0 {
		return "", false
	}
	switch body[:dash] {
	case "amf":
		return "AMF", true
	case "ausf":
		return "AUSF", true
	case "udm":
		return "UDM", true
	case "udr":
		return "UDR", true
	case "nrf":
		return "NRF", true
	case "pcf":
		return "PCF", true
	case "nssf":
		return "NSSF", true
	case "smf":
		return "SMF", true
	case "nef":
		return "NEF", true
	case "chf":
		return "CHF", true
	case "bsf":
		return "BSF", true
	}
	return "", false
}
