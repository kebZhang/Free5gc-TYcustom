// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// TYcustom: fork-local server-side instrumentation for the nine-point HTTP/2
// latency experiment (HTTP_3detailLog_PLAN_0826.md, phases 2 and 3 -- points G
// and W).
//
// The overriding constraint here is that G is taken on the serve goroutine.
// There is exactly one of those per connection, and with one connection per NF
// pair it is a strictly serialised resource carrying every request between two
// NFs. A host with idle cores says nothing about whether that goroutine has
// headroom. So the design below spends allocations to avoid work there:
//
//   - ServerRequestTrace is an inline value field of stream, not a separate
//     heap object;
//   - stream implements context.Context itself, so attaching the trace to the
//     request needs no context.WithValue node;
//   - the trace pointer is threaded through scheduleHandler/unstartedHandler as
//     an argument, so the serve goroutine never walks a context chain.
//
// Net cost on the serve goroutine: one nil check, one time.Now(), one
// assignment.

package http2

import (
	"context"
	"sync/atomic"
	"time"
)

// serverRequestIDCounter numbers inbound requests within this process.
//
// TODO(TYcustom, plan §19.5 R2): this is one cache line shared by every
// connection, touched once per inbound request on the serve goroutine. It is
// strictly redundant -- (conn, stream_id) already identifies a request uniquely
// within one server pod, which is all the server-record/W-event join needs. If a
// mutex or cache profile shows contention here, move the counter onto serverConn
// (the serve goroutine is single threaded, so it would not even need to be
// atomic) and change the offline join key to (dst, conn, server_request_id).
var serverRequestIDCounter atomic.Uint64

// ServerRequestTrace holds the fork-local state for one inbound HTTP/2 request.
// Its lifetime is the request's; it lives inline inside the stream.
//
// Mutability contract -- violating it is a data race:
//
//   - ID, ConnID and StreamID are written once when the request is created, on
//     the serve goroutine, and are read-only afterwards. The W path reads them
//     from the frame-writer goroutine, which is safe because the pointer only
//     reaches that goroutine through a chain of channel sends.
//
//   - HandlerGo is written by the serve goroutine immediately before
//     `go sc.runHandler(...)` and read by the handler goroutine. The `go`
//     statement itself supplies the happens-before edge, which is why a plain
//     assignment is correct here and no atomic is needed.
//
//   - The W path must NEVER read HandlerGo. The frame-writer goroutine has no
//     happens-before relationship with the serve goroutine's write of it.
type ServerRequestTrace struct {
	// ID is unique within this process for the life of the process. It joins
	// this request's server access-log line to its W event.
	ID uint64

	// ConnID is sc.remoteAddrStr, i.e. "clientIP:clientPort" as this pod sees
	// it. It is a per-connection immutable string, so copying it costs nothing.
	// The peer records the identical string as its own conn (LocalAddr).
	ConnID string

	// StreamID is the HTTP/2 stream id. (ConnID, StreamID) is the cross-pod
	// exact-join key against the client's access-log line.
	StreamID uint32

	// HandlerGo is G: the instant just before the handler goroutine is started.
	// Zero if the request never reached that point.
	HandlerGo time.Time
}

type serverTraceKey struct{}

// ServerRequestTraceFromContext returns the trace for an inbound request, or nil
// if there is none (HTTP/1, or a server not built from this fork).
//
// This runs on the per-request handler goroutine, not on any serialised
// resource, so the single Value() lookup here does not need optimising away the
// way the serve-goroutine path does.
func ServerRequestTraceFromContext(ctx context.Context) *ServerRequestTrace {
	tr, _ := ctx.Value(serverTraceKey{}).(*ServerRequestTrace)
	return tr
}

// stream implements context.Context so that a request can carry its trace
// without allocating a context.WithValue node. Everything except the trace key
// forwards to st.ctx, the *cancelCtx that newStream already created.
//
// This keeps context.WithCancel(st) on its efficient path: parentCancelCtx looks
// the parent's *cancelCtx up via Value (which falls through to st.ctx) and then
// checks that it matches parent.Done() (which is st.ctx.Done(), the same
// channel). Both conditions hold, so deriving a child context from a request
// behaves exactly as it did before -- no extra watchdog goroutine.
//
// The four method names below do not collide with any existing stream method or
// field (the deadline fields are readDeadline/writeDeadline).

func (st *stream) Deadline() (time.Time, bool) { return st.ctx.Deadline() }
func (st *stream) Done() <-chan struct{}       { return st.ctx.Done() }
func (st *stream) Err() error                  { return st.ctx.Err() }

func (st *stream) Value(key any) any {
	if key == (serverTraceKey{}) {
		return &st.trace
	}
	return st.ctx.Value(key)
}

// initTrace fills in the immutable identity of an inbound request. Called once,
// on the serve goroutine, while the request is being created.
func (st *stream) initTrace() {
	st.trace.ID = serverRequestIDCounter.Add(1)
	st.trace.ConnID = st.sc.remoteAddrStr
	st.trace.StreamID = st.id
}

// --- W: response HEADERS reaching the kernel -------------------------------

// ResponseHeadersFlushedEvent reports that the last byte of one response's
// HEADERS/CONTINUATION block has been accepted by the underlying net.Conn.Write.
//
// It is a fixed-size value: the socket writer must be able to hand one off
// without allocating. ConnID points at the connection's own immutable address
// string, so copying it copies a header, not bytes.
type ResponseHeadersFlushedEvent struct {
	// At is W. Every event settled by the same underlying Write shares one
	// timestamp -- taking a separate clock reading per marker would measure the
	// loop, not the write.
	At time.Time

	// ConnID and StreamID mirror the fields on ServerRequestTrace, so a W event
	// can be cross-checked against the server access-log line it joins to.
	ConnID   string
	StreamID uint32

	// ServerRequestID is the join key against that line.
	ServerRequestID uint64

	// BatchSize is how many markers this one underlying Write settled in total,
	// including this one. It is the direct measure of how much the server's
	// write path is batching responses together, which at high request rates is
	// the effect this whole experiment is looking for. It is not a timestamp.
	BatchSize uint16

	// Err is true when the underlying Write reported an error. Such an event
	// records that the response did not get out cleanly; it is not a valid W.
	Err bool
}

var responseHeadersFlushedDrops atomic.Uint64

// wAccountingErrors counts violations of the marker bookkeeping invariant
// checked in (*bufferedWriter).Flush. It should stay at zero for a healthy run;
// a non-zero value means the byte offsets have drifted and the W data from that
// run cannot be trusted.
var wAccountingErrors atomic.Uint64

// WAccountingErrors reports the marker-bookkeeping self-check failure count.
func WAccountingErrors() uint64 { return wAccountingErrors.Load() }

// ResponseHeadersFlushedDrops returns how many W events this process could not
// deliver, either because the consumer's queue was full or because a
// connection's marker ring overflowed. A non-zero value means the W record is
// incomplete and the run's completeness accounting must say so.
//
// Process-level equals NF-level here: each NF builds exactly one http2.Server.
func ResponseHeadersFlushedDrops() uint64 { return responseHeadersFlushedDrops.Load() }

// wMarkerRingSize bounds how many response header blocks can be waiting inside
// one connection's 4 KiB write buffer at once.
//
// A HEADERS frame is at least a 9 byte header plus its fragment, so no more than
// roughly 140 can fit in the buffer before it is forced out to the kernel. 256
// is comfortable margin at 16 bytes per slot, i.e. 4 KiB per connection, and
// this deployment holds very few connections. Overflow is counted as a drop; it
// is never allowed to allocate or block.
const wMarkerRingSize = 256

type wMarker struct {
	// endOffset is the connection-lifetime byte offset one past this response's
	// last header byte. Comparing it against the count of bytes the kernel has
	// accepted is what makes W exact regardless of how frames were batched.
	endOffset uint64
	trace     *ServerRequestTrace
}

// wMarkerRing is a fixed-capacity FIFO. A plain slice would be wrong here: the
// obvious `ring = ring[1:]` pop makes append periodically reallocate, and that
// allocation would land on the socket write path, which is exactly what the
// design forbids.
type wMarkerRing struct {
	buf  [wMarkerRingSize]wMarker
	head uint32 // index of the oldest marker
	n    uint32 // number of markers held
}

func (r *wMarkerRing) push(m wMarker) bool {
	if r.n == wMarkerRingSize {
		return false
	}
	r.buf[(r.head+r.n)%wMarkerRingSize] = m
	r.n++
	return true
}

func (r *wMarkerRing) peek() *wMarker {
	if r.n == 0 {
		return nil
	}
	return &r.buf[r.head]
}

func (r *wMarkerRing) pop() wMarker {
	m := r.buf[r.head]
	r.buf[r.head] = wMarker{} // don't retain the trace
	r.head = (r.head + 1) % wMarkerRingSize
	r.n--
	return m
}
