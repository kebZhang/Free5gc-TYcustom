// Package msgtrace carries a per-message processing trace for the NGAP worker,
// so the AMF can record — asynchronously and off the data path — how long each
// uplink NAS message spent inside the worker and around each downstream SBI call
// it triggered.
//
// The Trace is passed EXPLICITLY (as a *Trace), never looked up by goroutine id.
// An earlier implementation kept it goroutine-local (a global sync.Map keyed by a
// goroutine id parsed out of runtime.Stack). Measurement showed that this became
// a load-dependent latency source of its own: ~21 runtime.Stack calls per NAS
// message, plus a single global map contended by hundreds of worker goroutines,
// landing exactly on the AMF's hottest path. At RQ800 it inflated AMF-local
// median latency ~7x. Both costs are gone here: creating and threading a plain
// struct pointer is a few pointer dereferences with no shared container at all.
//
// How the pointer reaches the SBI consumers without threading a parameter
// through the generated dispatcher (dispatcher_generated.go, DO NOT EDIT):
// nas.HandleNAS creates the Trace and binds it to the AmfUe (AmfUe.WorkerTrace)
// right after NAS decode; the consumers already hold that *AmfUe and read it back
// from there. HandleNAS unbinds on return.
//
// Concurrency: one uplink NAS message is decoded and handled entirely
// synchronously inside ONE worker goroutine (ngap worker -> Dispatch ->
// dispatchMain -> nas.HandleNAS -> gmm FSM -> consumer.Send*). The GMM package
// spawns no goroutines and the FSM dispatches events synchronously, so every SBI
// call triggered by one NAS message runs on that same goroutine. The scheduler
// additionally serialises messages per UE (hash-by-UEID, or per-association in
// dGNB mode), so a given AmfUe's WorkerTrace is only ever touched by one
// goroutine at a time. No lock is needed.
//
// The recorded points (see AMF_WORKER_LOG_PLAN.md):
//   - Start (T2): nas.HandleNAS entry, i.e. the worker began handling it.
//   - each SBI call: Before (T3) entering consumer.Send*, After (T6) it returned.
//   - End   (T8): the handler returned; the trace is flushed to AMF_worker_log.
//
// T0/T1 (the SCTP read time) is deliberately NOT carried here: it is already
// recorded as the UL sctp_time in AMF_log, and the two were byte-for-byte
// identical in the previous implementation. Dropping it is what removes the need
// to pass anything across the generated dispatcher.
//
// This never affects data-plane correctness: it only feeds an asynchronous log.
// Every method is nil-safe, so a call path with no trace bound (e.g. NF
// registration at startup, or NRF discovery) simply records nothing.
package msgtrace

import "time"

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

// Trace accumulates one uplink NAS message's worker timeline. T8 is not stored:
// it is taken with time.Now() at flush time, in the HandleNAS defer that already
// holds this pointer.
type Trace struct {
	UeID    string
	NasType string
	Start   time.Time // T2
	SBI     []SBICall
}

// New starts a trace, recording T2 (start). It is a plain allocation: no table
// lookup, no stack walk.
func New(start time.Time) *Trace {
	return &Trace{Start: start}
}

// SetID fills in the UE id and NAS type once the NAS layer has decoded them
// (they are not known yet at New time). nil-safe.
func (t *Trace) SetID(ueID, nasType string) {
	if t == nil {
		return
	}
	t.UeID = ueID
	t.NasType = nasType
}

// AddSBI appends one downstream SBI call (T3/T6) to the trace. nil-safe.
func (t *Trace) AddSBI(call string, before, after time.Time) {
	if t == nil {
		return
	}
	t.SBI = append(t.SBI, SBICall{Call: call, Before: before, After: after})
}

// Track is a convenience wrapper for consumer Send* functions: it captures T3
// now and returns a closure that captures T6 and appends the call. Intended use:
//
//	defer ue.WorkerTrace.Track("AUSF_ue-authentications")()
//
// The outer call runs at defer-registration time (function entry, = T3); the
// returned func runs when the function returns (= T6).
//
// nil-safe, and on the nil path it does no work at all — not even time.Now() —
// so SBI calls made outside NAS handling cost nothing.
func (t *Trace) Track(call string) func() {
	if t == nil {
		return noop
	}
	before := time.Now()
	return func() {
		t.AddSBI(call, before, time.Now())
	}
}

// noop is a shared do-nothing closure returned by Track on the nil path, so that
// path allocates nothing.
func noop() {}
