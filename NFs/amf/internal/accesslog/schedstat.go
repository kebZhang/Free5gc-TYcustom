package accesslog

import (
	"encoding/json"
	"math"
	"net/http"
	"runtime/metrics"
	"sync"
	"time"
)

// SchedTrackingPeriod mirrors the Go runtime's gTrackingPeriod
// (runtime/proc.go). The runtime only records a runnable-duration sample for
// every 8th transition out of _Grunning per goroutine, so the histogram's count
// is roughly 1/8 of the real number of scheduling events and both the count and
// the total time must be multiplied by this to recover actual values.
const SchedTrackingPeriod = 8

var (
	schedMu      sync.Mutex
	schedSamples = []metrics.Sample{
		{Name: "/sched/latencies:seconds"},
		{Name: "/sched/goroutines:goroutines"},
	}
)

// SchedSnap is a cumulative reading taken at one instant. Every field counts
// from process start and only ever grows, so a single snapshot is meaningless on
// its own -- take one before and one after a run and use the difference.
type SchedSnap struct {
	Wall       string  `json:"wall"`
	Count      uint64  `json:"count"`      // sampled runnable->running transitions
	TotalSec   float64 `json:"total_sec"`  // sampled goroutine-seconds (NOT yet x8)
	Goroutines uint64  `json:"goroutines"` // goroutines alive right now
}

// SchedSnapshot reads runtime/metrics once and folds the scheduling-latency
// histogram down to a scalar count and total. It runs only when the endpoint is
// scraped, never on a request path.
//
// schedSamples is a package-level slice reused across calls (metrics.Read fills
// it in place), so the mutex is what makes concurrent scrapes safe.
func SchedSnapshot() SchedSnap {
	schedMu.Lock()
	defer schedMu.Unlock()

	metrics.Read(schedSamples)
	h := schedSamples[0].Value.Float64Histogram()

	s := SchedSnap{Wall: time.Now().UTC().Format(time.RFC3339Nano)}
	for i, c := range h.Counts {
		if c == 0 {
			continue
		}
		// Approximate each bucket by its midpoint. The open-ended first and last
		// buckets have no finite midpoint, so collapse them onto their one
		// finite edge rather than producing an infinite total.
		lo, hi := h.Buckets[i], h.Buckets[i+1]
		if math.IsInf(lo, -1) {
			lo = hi
		}
		if math.IsInf(hi, +1) {
			hi = lo
		}
		s.Count += c
		s.TotalSec += float64(c) * (lo + hi) / 2
	}
	s.Goroutines = schedSamples[1].Value.Uint64()
	return s
}

// schedOnce guards the handler registration. http.HandleFunc panics if the same
// pattern is registered twice, and this is cheaper than relying on every caller
// to invoke RegisterSchedStat exactly once.
var schedOnce sync.Once

// RegisterSchedStat installs /debug/schedstat on the same default mux that
// net/http/pprof registers onto. Call it from main() BEFORE the goroutine that
// serves :6060 starts.
func RegisterSchedStat() {
	schedOnce.Do(func() {
		http.HandleFunc("/debug/schedstat", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(SchedSnapshot())
		})
	})
}
