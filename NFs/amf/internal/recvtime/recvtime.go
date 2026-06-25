// Package recvtime carries the SCTP-read timestamp of the NGAP message currently
// being processed, keyed by goroutine, so it can be attached to the asynchronous
// AMF_log once the NAS layer has identified the message type.
//
// Why goroutine-local instead of a parameter: a single NGAP message is decoded
// and handled entirely synchronously inside one worker goroutine
// (ngap.Dispatch -> dispatchMain -> handler -> nas.HandleNAS). dispatchMain is
// generated code (dispatcher_generated.go, DO NOT EDIT), so threading an extra
// parameter through every handler is brittle. The worker stashes the read time
// keyed by its own goroutine id right before invoking the handler, and the NAS
// layer reads it back. Because the whole chain runs on that one goroutine with
// no channel hop in between, the value is unambiguous for the duration.
//
// It lives in its own package (not ngap) to avoid an import cycle: ngap imports
// nas, and nas needs to read this back, so both import this leaf package.
//
// This never affects data-plane correctness: it only feeds the asynchronous
// AMF_log; a missed lookup just yields the zero time.
package recvtime

import (
	"runtime"
	"strconv"
	"sync"
	"time"
)

var byG sync.Map // goroutine id (uint64) -> time.Time

// Set records t for the current goroutine. Call Clear when the handler returns
// to avoid leaking entries for reused worker goroutines.
func Set(t time.Time) {
	byG.Store(goroutineID(), t)
}

func Clear() {
	byG.Delete(goroutineID())
}

// Current returns the SCTP-read time stashed for the current goroutine, and
// whether one was present.
func Current() (time.Time, bool) {
	if v, ok := byG.Load(goroutineID()); ok {
		return v.(time.Time), true
	}
	return time.Time{}, false
}

// --- Downlink NAS type carrier ----------------------------------------------
//
// The downlink counterpart: a GMM Send* function knows the NAS type it is about
// to emit, but the actual SCTP write time is only available deep in the shared
// ngap message SendToRan. The send runs synchronously on one goroutine from the
// GMM function down to SendToRan, so the GMM function stashes the NAS type here
// and SendToRan reads it back at write time. dlByG holds (nasType, ueID).

type dlInfo struct {
	nasType string
	ueID    string
}

var dlByG sync.Map // goroutine id (uint64) -> dlInfo

// SetDLNas records the NAS type and ue id about to be sent downlink, for the
// current goroutine. Clear with ClearDLNas after the send returns.
func SetDLNas(nasType, ueID string) {
	dlByG.Store(goroutineID(), dlInfo{nasType: nasType, ueID: ueID})
}

func ClearDLNas() {
	dlByG.Delete(goroutineID())
}

// CurrentDLNas returns the pending downlink NAS type and ue id for the current
// goroutine, and whether one was present.
func CurrentDLNas() (nasType, ueID string, ok bool) {
	if v, loaded := dlByG.Load(goroutineID()); loaded {
		d := v.(dlInfo)
		return d.nasType, d.ueID, true
	}
	return "", "", false
}

// goroutineID parses the current goroutine's id from its stack header. This is
// only done a few times per NGAP message (set/clear/lookup), so the cost is
// negligible relative to NAS decoding and SBI calls on the same path.
func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// Header format: "goroutine <id> [running]:\n..."
	s := buf[:n]
	const prefix = "goroutine "
	s = s[len(prefix):]
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	id, _ := strconv.ParseUint(string(s[:i]), 10, 64)
	return id
}
