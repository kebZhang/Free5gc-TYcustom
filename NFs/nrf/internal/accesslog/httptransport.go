package accesslog

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"golang.org/x/net/http2"
)

// maxSniffBody caps how many bytes of a request body we will read to extract a
// UE id. The bodies we sniff (AuthenticationInfo, PolicyAssociationRequest) are
// well under 1 KiB; this guards against ever buffering a large/unexpected body.
const maxSniffBody = 8 << 10 // 8 KiB

// These mirror the timeouts used by free5gc/openapi's internal HTTP/2 clients
// so behaviour is unchanged apart from the added logging.
const (
	readIdleTimeoutPeriod = 1 * time.Second
	pingTimeoutPeriod     = 1 * time.Second
	timeoutPeriod         = 10 * time.Second
)

// loggingRoundTripper wraps separate HTTP/2 transports for https (h2) and
// cleartext (h2c), choosing per request by URL scheme exactly like
// openapi.CallAPI's inner clients do, and records one HTTP access-log entry per
// request from the requester's (this NF's) point of view.
type loggingRoundTripper struct {
	tls   http.RoundTripper // h2 over TLS  (https)
	clear http.RoundTripper // h2c cleartext (http)
}

func newLoggingRoundTripper() *loggingRoundTripper {
	return &loggingRoundTripper{
		tls: &http2.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // matches openapi default
			ReadIdleTimeout: readIdleTimeoutPeriod,
			PingTimeout:     pingTimeoutPeriod,
		},
		clear: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				d := &net.Dialer{}
				return d.DialContext(ctx, network, addr)
			},
			ReadIdleTimeout: readIdleTimeoutPeriod,
			PingTimeout:     pingTimeoutPeriod,
		},
	}
}

func (l *loggingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	base := l.clear
	if req.URL != nil && req.URL.Scheme == "https" {
		base = l.tls
	}

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

	reqTime := time.Now()
	resp, err := base.RoundTrip(req)
	respTime := time.Now()

	// Always log, even on transport error, so failed attempts are visible.
	LogHTTP(dst, method, uri, ueID, reqTime, respTime)
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
