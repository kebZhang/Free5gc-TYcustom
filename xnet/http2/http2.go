// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package http2 implements the HTTP/2 protocol.
//
// This package is low-level and intended to be used directly by very
// few people. Most users will use it indirectly through the automatic
// use by the net/http package (from Go 1.6 and later).
// For use in earlier Go versions see ConfigureServer. (Transport support
// requires Go 1.6 or later)
//
// See https://http2.github.io/ for more information on HTTP/2.
package http2 // import "golang.org/x/net/http2"

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http/httpguts"
)

var (
	VerboseLogs    bool
	logFrameWrites bool
	logFrameReads  bool

	// Enabling extended CONNECT by causes browsers to attempt to use
	// WebSockets-over-HTTP/2. This results in problems when the server's websocket
	// package doesn't support extended CONNECT.
	//
	// Disable extended CONNECT by default for now.
	//
	// Issue #71128.
	disableExtendedConnectProtocol = true
)

func init() {
	e := os.Getenv("GODEBUG")
	if strings.Contains(e, "http2debug=1") {
		VerboseLogs = true
	}
	if strings.Contains(e, "http2debug=2") {
		VerboseLogs = true
		logFrameWrites = true
		logFrameReads = true
	}
	if strings.Contains(e, "http2xconnect=1") {
		disableExtendedConnectProtocol = false
	}
}

const (
	// ClientPreface is the string that must be sent by new
	// connections from clients.
	ClientPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

	// SETTINGS_MAX_FRAME_SIZE default
	// https://httpwg.org/specs/rfc7540.html#rfc.section.6.5.2
	initialMaxFrameSize = 16384

	// NextProtoTLS is the NPN/ALPN protocol negotiated during
	// HTTP/2's TLS setup.
	NextProtoTLS = "h2"

	// https://httpwg.org/specs/rfc7540.html#SettingValues
	initialHeaderTableSize = 4096

	initialWindowSize = 65535 // 6.9.2 Initial Flow Control Window Size

	defaultMaxReadFrameSize = 1 << 20
)

var (
	clientPreface = []byte(ClientPreface)
)

type streamState int

// HTTP/2 stream states.
//
// See http://tools.ietf.org/html/rfc7540#section-5.1.
//
// For simplicity, the server code merges "reserved (local)" into
// "half-closed (remote)". This is one less state transition to track.
// The only downside is that we send PUSH_PROMISEs slightly less
// liberally than allowable. More discussion here:
// https://lists.w3.org/Archives/Public/ietf-http-wg/2016JulSep/0599.html
//
// "reserved (remote)" is omitted since the client code does not
// support server push.
const (
	stateIdle streamState = iota
	stateOpen
	stateHalfClosedLocal
	stateHalfClosedRemote
	stateClosed
)

var stateName = [...]string{
	stateIdle:             "Idle",
	stateOpen:             "Open",
	stateHalfClosedLocal:  "HalfClosedLocal",
	stateHalfClosedRemote: "HalfClosedRemote",
	stateClosed:           "Closed",
}

func (st streamState) String() string {
	return stateName[st]
}

// Setting is a setting parameter: which setting it is, and its value.
type Setting struct {
	// ID is which setting is being set.
	// See https://httpwg.org/specs/rfc7540.html#SettingFormat
	ID SettingID

	// Val is the value.
	Val uint32
}

func (s Setting) String() string {
	return fmt.Sprintf("[%v = %d]", s.ID, s.Val)
}

// Valid reports whether the setting is valid.
func (s Setting) Valid() error {
	// Limits and error codes from 6.5.2 Defined SETTINGS Parameters
	switch s.ID {
	case SettingEnablePush:
		if s.Val != 1 && s.Val != 0 {
			return ConnectionError(ErrCodeProtocol)
		}
	case SettingInitialWindowSize:
		if s.Val > 1<<31-1 {
			return ConnectionError(ErrCodeFlowControl)
		}
	case SettingMaxFrameSize:
		if s.Val < 16384 || s.Val > 1<<24-1 {
			return ConnectionError(ErrCodeProtocol)
		}
	case SettingEnableConnectProtocol:
		if s.Val != 1 && s.Val != 0 {
			return ConnectionError(ErrCodeProtocol)
		}
	}
	return nil
}

// A SettingID is an HTTP/2 setting as defined in
// https://httpwg.org/specs/rfc7540.html#iana-settings
type SettingID uint16

const (
	SettingHeaderTableSize       SettingID = 0x1
	SettingEnablePush            SettingID = 0x2
	SettingMaxConcurrentStreams  SettingID = 0x3
	SettingInitialWindowSize     SettingID = 0x4
	SettingMaxFrameSize          SettingID = 0x5
	SettingMaxHeaderListSize     SettingID = 0x6
	SettingEnableConnectProtocol SettingID = 0x8
)

var settingName = map[SettingID]string{
	SettingHeaderTableSize:       "HEADER_TABLE_SIZE",
	SettingEnablePush:            "ENABLE_PUSH",
	SettingMaxConcurrentStreams:  "MAX_CONCURRENT_STREAMS",
	SettingInitialWindowSize:     "INITIAL_WINDOW_SIZE",
	SettingMaxFrameSize:          "MAX_FRAME_SIZE",
	SettingMaxHeaderListSize:     "MAX_HEADER_LIST_SIZE",
	SettingEnableConnectProtocol: "ENABLE_CONNECT_PROTOCOL",
}

func (s SettingID) String() string {
	if v, ok := settingName[s]; ok {
		return v
	}
	return fmt.Sprintf("UNKNOWN_SETTING_%d", uint16(s))
}

// validWireHeaderFieldName reports whether v is a valid header field
// name (key). See httpguts.ValidHeaderName for the base rules.
//
// Further, http2 says:
//
//	"Just as in HTTP/1.x, header field names are strings of ASCII
//	characters that are compared in a case-insensitive
//	fashion. However, header field names MUST be converted to
//	lowercase prior to their encoding in HTTP/2. "
func validWireHeaderFieldName(v string) bool {
	if len(v) == 0 {
		return false
	}
	for _, r := range v {
		if !httpguts.IsTokenRune(r) {
			return false
		}
		if 'A' <= r && r <= 'Z' {
			return false
		}
	}
	return true
}

func httpCodeString(code int) string {
	switch code {
	case 200:
		return "200"
	case 404:
		return "404"
	}
	return strconv.Itoa(code)
}

// from pkg io
type stringWriter interface {
	WriteString(s string) (n int, err error)
}

// A closeWaiter is like a sync.WaitGroup but only goes 1 to 0 (open to closed).
type closeWaiter chan struct{}

// Init makes a closeWaiter usable.
// It exists because so a closeWaiter value can be placed inside a
// larger struct and have the Mutex and Cond's memory in the same
// allocation.
func (cw *closeWaiter) Init() {
	*cw = make(chan struct{})
}

// Close marks the closeWaiter as closed and unblocks any waiters.
func (cw closeWaiter) Close() {
	close(cw)
}

// Wait waits for the closeWaiter to become closed.
func (cw closeWaiter) Wait() {
	<-cw
}

// bufferedWriter is a buffered writer that writes to w.
// Its buffered writer is lazily allocated as needed, to minimize
// idle memory usage with many connections.
type bufferedWriter struct {
	_           incomparable
	conn        net.Conn      // immutable
	bw          *bufio.Writer // non-nil when data is buffered
	byteTimeout time.Duration // immutable, WriteByteTimeout

	// TYcustom: W instrumentation. Every field here is connection-local and
	// needs no lock, because at most one frame is being written to a connection
	// at any instant (serverConn.writingFrame), and the happens-before between
	// the serve goroutine and a writeFrameAsync goroutine is supplied by the
	// `go` statement that starts it and by sc.wroteFrameCh on the way back.
	//
	// This type is used by the server only -- newBufferedWriter has exactly one
	// caller -- so none of this touches the client path.
	wSink    chan<- ResponseHeadersFlushedEvent // nil disables W entirely
	connID   string                             // immutable, == sc.remoteAddrStr
	produced uint64                             // bytes handed to bufio so far
	accepted uint64                             // bytes the kernel has taken so far
	pending  *ServerRequestTrace                // armed by writeHeaderBlock, consumed by the next Write
	markers  wMarkerRing                        // offsets awaiting settlement, ascending
}

func newBufferedWriter(conn net.Conn, timeout time.Duration, wSink chan<- ResponseHeadersFlushedEvent, connID string) *bufferedWriter {
	return &bufferedWriter{
		conn:        conn,
		byteTimeout: timeout,
		wSink:       wSink,
		connID:      connID,
	}
}

// armResponseHeaderMarker records that the very next Write carries the last
// fragment of tr's response header block. Passing nil clears it.
//
// Invariant: at most one marker is ever pending, and it always belongs to the
// frame written immediately afterwards. (*bufferedWriter).Write is the only
// consumer and clears it as soon as it has converted it to a byte offset.
func (w *bufferedWriter) armResponseHeaderMarker(tr *ServerRequestTrace) { w.pending = tr }

// bufWriterPoolBufferSize is the size of bufio.Writer's
// buffers created using bufWriterPool.
//
// TODO: pick a less arbitrary value? this is a bit under
// (3 x typical 1500 byte MTU) at least. Other than that,
// not much thought went into it.
const bufWriterPoolBufferSize = 4 << 10

var bufWriterPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewWriterSize(nil, bufWriterPoolBufferSize)
	},
}

func (w *bufferedWriter) Available() int {
	if w.bw == nil {
		return bufWriterPoolBufferSize
	}
	return w.bw.Available()
}

func (w *bufferedWriter) Write(p []byte) (n int, err error) {
	if w.bw == nil {
		bw := bufWriterPool.Get().(*bufio.Writer)
		bw.Reset((*bufferedWriterTimeoutWriter)(w))
		w.bw = bw
	}
	// TYcustom: turn the pending marker into an exact byte offset BEFORE handing
	// p to bufio. Framer.endWrite calls this once per whole frame, so
	// produced+len(p) is precisely the offset one past that frame's last byte --
	// no assumption about padding or priority fields, unlike computing it from
	// the fragment length. It must happen before the bufio call because bufio
	// may pass straight through to the kernel inside that same call, and the
	// marker has to already be queued when it does.
	if w.pending != nil {
		if !w.markers.push(wMarker{endOffset: w.produced + uint64(len(p)), trace: w.pending}) {
			responseHeadersFlushedDrops.Add(1)
		}
		w.pending = nil
	}
	n, err = w.bw.Write(p)
	w.produced += uint64(n)
	return n, err
}

func (w *bufferedWriter) Flush() error {
	bw := w.bw
	if bw == nil {
		return nil
	}
	err := bw.Flush()
	// TYcustom self-check, free of charge. A successful Flush means every
	// buffered byte reached the kernel, so every marker must have been settled
	// and the two byte counters must agree. If they do not, the offset
	// bookkeeping has a bug and the W data is invalid -- count it rather than
	// letting the run look clean.
	//
	// The failing branch is expected after a write error, where Reset(nil) below
	// discards buffered bytes and the counters part company permanently. That
	// connection is finished anyway.
	if err == nil && (w.markers.n != 0 || w.produced != w.accepted) {
		wAccountingErrors.Add(1)
	}
	bw.Reset(nil)
	bufWriterPool.Put(bw)
	w.bw = nil
	return err
}

// settleMarkers emits a W event for every marker the kernel has now consumed.
// They share one timestamp: this is one write, not several.
//
// Markers left unsettled when a connection dies are deliberately not flushed
// here. Doing so would mean touching this state from the serve goroutine's
// teardown while a writeFrameAsync goroutine may still be running. Those
// responses simply have no W, and offline analysis counts them as missing.
func (w *bufferedWriter) settleMarkers(at time.Time, writeErr bool) {
	// Count first, so every event in the batch can carry the same batch size --
	// that number is the measurement.
	var batch uint16
	for i := uint32(0); i < w.markers.n; i++ {
		if w.markers.buf[(w.markers.head+i)%wMarkerRingSize].endOffset > w.accepted {
			break
		}
		batch++
	}
	for i := uint16(0); i < batch; i++ {
		m := w.markers.pop()
		if m.trace == nil || w.wSink == nil {
			continue
		}
		ev := ResponseHeadersFlushedEvent{
			At:              at,
			ConnID:          w.connID,
			StreamID:        m.trace.StreamID,
			ServerRequestID: m.trace.ID,
			BatchSize:       batch,
			Err:             writeErr,
		}
		// Non-blocking by construction. This runs on the only goroutine writing
		// to the socket, and serverConn.writeHeaders already blocks a handler
		// goroutine until its frame is written, so blocking here would push back
		// directly onto request handling. A full queue costs a dropped log line,
		// never a stalled response.
		select {
		case w.wSink <- ev:
		default:
			responseHeadersFlushedDrops.Add(1)
		}
	}
}

type bufferedWriterTimeoutWriter bufferedWriter

func (w *bufferedWriterTimeoutWriter) Write(p []byte) (n int, err error) {
	// TYcustom: this is the real socket write, and the only place that has both
	// the connection-local marker state and the knowledge that bytes have
	// actually been accepted by the kernel. Hooking writeWithByteTimeout instead
	// would not work: it is a package-level function with no connection state,
	// and on this deployment WriteByteTimeout is zero, so it is a single
	// pass-through call to conn.Write.
	//
	// accepted advances by the bytes genuinely taken, so a short write leaves
	// the marker unsettled rather than reporting a W that never happened.
	bw := (*bufferedWriter)(w)
	n, err = writeWithByteTimeout(bw.conn, bw.byteTimeout, p)
	bw.accepted += uint64(n)
	if m := bw.markers.peek(); m != nil && bw.accepted >= m.endOffset {
		bw.settleMarkers(time.Now(), err != nil)
	}
	return n, err
}

// writeWithByteTimeout writes to conn.
// If more than timeout passes without any bytes being written to the connection,
// the write fails.
func writeWithByteTimeout(conn net.Conn, timeout time.Duration, p []byte) (n int, err error) {
	if timeout <= 0 {
		return conn.Write(p)
	}
	for {
		conn.SetWriteDeadline(time.Now().Add(timeout))
		nn, err := conn.Write(p[n:])
		n += nn
		if n == len(p) || nn == 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
			// Either we finished the write, made no progress, or hit the deadline.
			// Whichever it is, we're done now.
			conn.SetWriteDeadline(time.Time{})
			return n, err
		}
	}
}

func mustUint31(v int32) uint32 {
	if v < 0 || v > 2147483647 {
		panic("out of range")
	}
	return uint32(v)
}

// bodyAllowedForStatus reports whether a given response status code
// permits a body. See RFC 7230, section 3.3.
func bodyAllowedForStatus(status int) bool {
	switch {
	case status >= 100 && status <= 199:
		return false
	case status == 204:
		return false
	case status == 304:
		return false
	}
	return true
}

type httpError struct {
	_       incomparable
	msg     string
	timeout bool
}

func (e *httpError) Error() string   { return e.msg }
func (e *httpError) Timeout() bool   { return e.timeout }
func (e *httpError) Temporary() bool { return true }

var errTimeout error = &httpError{msg: "http2: timeout awaiting response headers", timeout: true}

type connectionStater interface {
	ConnectionState() tls.ConnectionState
}

var sorterPool = sync.Pool{New: func() interface{} { return new(sorter) }}

type sorter struct {
	v []string // owned by sorter
}

func (s *sorter) Len() int           { return len(s.v) }
func (s *sorter) Swap(i, j int)      { s.v[i], s.v[j] = s.v[j], s.v[i] }
func (s *sorter) Less(i, j int) bool { return s.v[i] < s.v[j] }

// Keys returns the sorted keys of h.
//
// The returned slice is only valid until s used again or returned to
// its pool.
func (s *sorter) Keys(h http.Header) []string {
	keys := s.v[:0]
	for k := range h {
		keys = append(keys, k)
	}
	s.v = keys
	sort.Sort(s)
	return keys
}

func (s *sorter) SortStrings(ss []string) {
	// Our sorter works on s.v, which sorter owns, so
	// stash it away while we sort the user's buffer.
	save := s.v
	s.v = ss
	sort.Sort(s)
	s.v = save
}

// incomparable is a zero-width, non-comparable type. Adding it to a struct
// makes that struct also non-comparable, and generally doesn't add
// any size (as long as it's first).
type incomparable [0]func()
