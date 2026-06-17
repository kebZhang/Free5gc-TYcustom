package accesslog

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"time"

	"golang.org/x/net/http2"
)

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

	reqTime := time.Now()
	resp, err := base.RoundTrip(req)
	respTime := time.Now()

	// Always log, even on transport error, so failed attempts are visible.
	LogHTTP(dst, method, uri, reqTime, respTime)
	return resp, err
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
