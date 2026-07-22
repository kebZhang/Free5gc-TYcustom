// Package msgtrace carries a per-message processing trace for the NGAP worker,
// keyed by goroutine, so the AMF can record — asynchronously and off the data
// path — how long each uplink NAS message spent inside the worker and around
// each downstream SBI call it triggered.
//
// Why goroutine-local (same reasoning as the sibling recvtime package): a single
// uplink NAS message is decoded and handled entirely synchronously inside ONE
// worker goroutine (ngap worker -> Dispatch -> dispatchMain -> nas.HandleNAS ->
// gmm FSM -> consumer.Send*). The GMM package spawns no goroutines and the FSM
// dispatches events synchronously, so every downstream SBI call triggered by one
// NAS message runs on that same goroutine with no channel hop in between. That
// makes a goroutine-keyed accumulator unambiguous for the whole duration and
// lets us avoid threading an extra parameter through the generated dispatcher
// code (dispatcher_generated.go, DO NOT EDIT).
//
// The recorded points (see AMF_WORKER_LOG_PLAN.md):
//   - Recv  (T0): SCTP read time of the message (carried in from the scheduler).
//   - Start (T2): worker picked the message off its channel and began handling.
//   - each SBI call: Before (T3) entering consumer.Send*, After (T6) it returned.
//   - End   (T8): the handler returned; the trace is flushed to AMF_worker_log.
//
// This never affects data-plane correctness: it only feeds an asynchronous log.
// A missed lookup (e.g. an unexpected call path) simply yields no worker record.
package msgtrace

import (
	"sync"
	"time"
)

// SBICall is one downstream SBI interaction the worker made while handling the
// current NAS message. Before/After are AMF-clock timestamps taken immediately
// around the consumer Send* call (T3/T6), so (After-Before) is the full time the
// worker was blocked in that call, and (req_time-Before)/(After-resp_time) — once
// joined with HTTP_log — isolate the serialize/connect and deserialize costs the
// HTTP_log window does not see.
type SBICall struct {
	Call   string
	Before time.Time
	After  time.Time
}

// Trace accumulates one uplink NAS message's worker timeline.
type Trace struct {
	UeID    string
	NasType string
	Recv    time.Time // T0
	Start   time.Time // T2
	End     time.Time // T8
	SBI     []SBICall
}

var byG sync.Map // goroutine id (uint64) -> *Trace

// Begin starts a trace for the current goroutine, recording T0 (recv) and T2
// (start). Pair with End (which removes it) so a reused worker goroutine never
// leaks or bleeds one message's SBI list into the next.
func Begin(recv, start time.Time) {
	byG.Store(goroutineID(), &Trace{Recv: recv, Start: start})
}

// SetID fills in the UE id and NAS type once the NAS layer has decoded them
// (they are not known yet at Begin time). No-op if no trace is active.
func SetID(ueID, nasType string) {
	if v, ok := byG.Load(goroutineID()); ok {
		t := v.(*Trace)
		t.UeID = ueID
		t.NasType = nasType
	}
}

// AddSBI appends one downstream SBI call (T3/T6) to the current trace. No-op if
// no trace is active (e.g. an SBI call made outside NAS handling, such as NF
// registration at startup).
func AddSBI(call string, before, after time.Time) {
	if v, ok := byG.Load(goroutineID()); ok {
		t := v.(*Trace)
		t.SBI = append(t.SBI, SBICall{Call: call, Before: before, After: after})
	}
}

// Track is a convenience wrapper for consumer Send* functions: it captures T3
// now and returns a closure that captures T6 and appends the call. Intended use:
//
//	defer msgtrace.Track("AUSF_ue-authentications")()
//
// The outer call runs at defer-registration time (function entry, = T3); the
// returned func runs when the function returns (= T6). No-op if no trace active.
func Track(call string) func() {
	before := time.Now()
	return func() {
		AddSBI(call, before, time.Now())
	}
}

// End records T8, removes the trace for the current goroutine, and returns it
// (or nil if none was active) so the caller can flush it to the log.
func End(end time.Time) *Trace {
	if v, ok := byG.LoadAndDelete(goroutineID()); ok {
		t := v.(*Trace)
		t.End = end
		return t
	}
	return nil
}
