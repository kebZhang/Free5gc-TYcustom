// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// TYcustom: fork-local client-side instrumentation for the eleven-point HTTP/2
// latency experiment (HTTP_3detailLog_PLAN_0826.md, phase 1 / point M,
// HTTP_10thlog_0903.md, point M2, and point M3 below).
//
// Nothing here runs unless the caller explicitly attaches a *ClientRequestTrace
// to the request context. With no trace attached every hook added to
// transport.go is a single nil pointer comparison.

package http2

import (
	"context"
	"sync/atomic"
	"time"
)

// ClientConnIdentity is the immutable identity of one ClientConn, computed once
// in newClientConn.
//
// It exists so that per-request code never has to call LocalAddr().String()
// again: that call allocates, and on this deployment it would run on every
// outbound SBI request.
type ClientConnIdentity struct {
	// LocalAddr is "clientIP:clientPort". This is the canonical correlation key
	// for the experiment: the peer sees the identical string as
	// net.Conn.RemoteAddr().String() (and therefore as http.Request.RemoteAddr)
	// whenever the path between the two pods does not rewrite the source
	// address.
	LocalAddr string

	// RemoteAddr is the dial target. Diagnostics only; never a join key.
	RemoteAddr string
}

type clientRequestTraceKey struct{}

// ClientRequestTrace carries the fork-local measurements for one outbound
// request. Attach it with WithClientRequestTrace and read it back after
// RoundTrip returns.
//
// Lifetime: the trace is per *request*, not per attempt, and deliberately
// survives transport-level retries. shouldRetryRequest shallow-copies the
// Request (`newReq := *req`), so the ctx field -- and therefore this trace -- is
// carried into the next attempt. The M / stream / conn fields consequently
// describe the most recent attempt; Attempts() is what tells you whether there
// was more than one.
//
// Concurrency: writeRequest runs on the goroutine started by
// `go cs.doRequest(...)` in (*ClientConn).roundTrip, while roundTrip itself can
// return early through its <-ctx.Done() and <-cs.reqCancel branches. The caller
// then reads this trace while writeRequest may still be running, so every
// measurement field must be accessed atomically. Do not "simplify" these to
// plain fields; `go test -race` will fail.
//
// ClientRequestTrace is also its own context node: rather than paying for a
// separate context.WithValue node it implements context.Context directly and
// forwards to the parent. One allocation per request instead of two.
type ClientRequestTrace struct {
	// parent is set by WithClientRequestTrace before the trace is published and
	// is never mutated afterwards. A ClientRequestTrace must not be used as a
	// context.Context before WithClientRequestTrace has been called on it.
	parent context.Context

	attempts  atomic.Uint32
	mUnixNano atomic.Int64
	// TYcustom M2: the instant this attempt SUCCEEDED in acquiring
	// cc.reqHeaderMu. The lock wait itself (M2 - M) is deliberately NOT computed
	// here -- offline analysis subtracts the two serialised stamps, which keeps
	// the critical section down to one clock read and one store.
	mAcqUnixNano atomic.Int64
	// TYcustom M3: the instant this attempt RELEASED cc.reqHeaderMu, stamped
	// just after the release rather than just before it. See writeRequest for
	// why that side was chosen and what it costs offline. The time spent holding
	// the lock (M3 - M2) is, like the wait, deliberately NOT computed here.
	mRelUnixNano atomic.Int64
	streamID     atomic.Uint32
	connID       atomic.Pointer[ClientConnIdentity]
}

// WithClientRequestTrace returns a context carrying tr.
//
// tr itself becomes the context node, so this adds no allocation beyond tr. Do
// not call it twice on the same trace.
func WithClientRequestTrace(parent context.Context, tr *ClientRequestTrace) context.Context {
	tr.parent = parent
	return tr
}

func clientRequestTraceFromContext(ctx context.Context) *ClientRequestTrace {
	tr, _ := ctx.Value(clientRequestTraceKey{}).(*ClientRequestTrace)
	return tr
}

// The four context.Context methods. Deadline/Done/Err forward verbatim, so
// deriving a child context from a ClientRequestTrace still takes the efficient
// path inside package context: parentCancelCtx finds the parent's *cancelCtx
// through Value and matches it against Done(), and both come straight from
// t.parent.

func (t *ClientRequestTrace) Deadline() (time.Time, bool) { return t.parent.Deadline() }
func (t *ClientRequestTrace) Done() <-chan struct{}       { return t.parent.Done() }
func (t *ClientRequestTrace) Err() error                  { return t.parent.Err() }

func (t *ClientRequestTrace) Value(key any) any {
	if key == (clientRequestTraceKey{}) {
		return t
	}
	return t.parent.Value(key)
}

// Attempts reports how many times RoundTripOpt has handed this request to a
// ClientConn, i.e. how many transport attempts were made. 0 means the request
// never reached a connection at all (GetClientConn failed on the first try).
//
// retry_count in the access log is Attempts()-1. Note the counter is
// incremented only once a connection has been obtained, so attempts that failed
// inside GetClientConn are not counted.
func (t *ClientRequestTrace) Attempts() uint32 { return t.attempts.Load() }

// ReqHeaderMuStart returns M: the instant the most recent attempt was about to
// start contending for the connection's reqHeaderMu. The zero Time means it was
// never reached (the request failed before that point).
//
// The returned Time has no monotonic reading. It is meant for serialisation and
// offline correlation only; never subtract it from a Time that does carry one.
func (t *ClientRequestTrace) ReqHeaderMuStart() time.Time {
	ns := t.mUnixNano.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// ReqHeaderMuAcquired returns M2: the instant the most recent attempt actually
// took cc.reqHeaderMu. The zero Time means it was never taken -- either the
// request failed before reaching the lock at all (then ReqHeaderMuStart is also
// zero), or it was cancelled WHILE queued for the lock (then ReqHeaderMuStart is
// set and this is not). That second combination is a distinct outcome and must
// be counted separately offline, not folded into "incomplete".
//
// Note the converse does not hold: an attempt can acquire the lock and then fail
// in awaitOpenSlotForStreamLocked, leaving StreamID() at 0 while this is set. So
// StreamID() != 0 implies this is set, but not the other way round.
//
// Like ReqHeaderMuStart, the returned Time carries no monotonic reading. The
// lock wait is M2 - M, computed offline from the two serialised stamps; see
// HTTP_10thlog_0903.md section 2.4 for the two pitfalls that subtraction has.
func (t *ClientRequestTrace) ReqHeaderMuAcquired() time.Time {
	ns := t.mAcqUnixNano.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// ReqHeaderMuReleased returns M3: the instant the most recent attempt released
// cc.reqHeaderMu, i.e. the end of the serialised send path. The zero Time means
// the lock was never released by this attempt, which -- because both release
// sites are stamped -- can only mean it was never acquired either (then
// ReqHeaderMuAcquired is also zero).
//
// M3 is stamped AFTER the release, not before it, so it is the instant the lock
// was already available to the next waiter rather than the instant this attempt
// finished its last work under it. The difference is a channel receive, tens of
// nanoseconds, EXCEPT when the goroutine is preempted between the release and
// the stamp. Offline analysis must therefore tolerate M3 of one request landing
// slightly after M2 of the next request on the same connection: that ordering is
// a preemption artefact, not a measurement error, and such pairs must be counted
// rather than clamped to zero silently. Stamping before the release would have
// made the ordering exact but would have put a second clock read inside the
// critical section, which M2's contract forbids.
//
// The lock hold time is M3 - M2, computed offline. Note this is NOT the same as
// M3 - the time the request was written: cc.reqHeaderMu covers only the header
// path (stream-id allocation plus encodeAndWriteHeaders). A request body is
// written after the release, under cc.wmu alone, so for a request with a body
// wrote_time is later than M3 by however long the body took. That gap is
// exactly what M3 exists to separate from the hold time.
//
// Like the other stamps here, the returned Time carries no monotonic reading.
func (t *ClientRequestTrace) ReqHeaderMuReleased() time.Time {
	ns := t.mRelUnixNano.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// StreamID returns the HTTP/2 stream ID assigned to the most recent attempt.
// 0 means no stream was ever allocated, which is exactly the set of records
// that must be excluded from the offline join.
func (t *ClientRequestTrace) StreamID() uint32 { return t.streamID.Load() }

// Conn returns the identity of the connection the most recent attempt used, or
// nil if no connection was reached. The pointer is to an object owned by the
// ClientConn, so reading it allocates nothing.
func (t *ClientRequestTrace) Conn() *ClientConnIdentity { return t.connID.Load() }
