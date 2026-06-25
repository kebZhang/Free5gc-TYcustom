// Package disccache is a small, process-wide cache of NRF NF-discovery results
// for this NF. It lets the NF reuse a previous Nnrf_NFDiscovery SearchResult
// instead of querying the NRF on every lookup.
//
// Design (see project discussion):
//   - One cache per NF process (this package is a singleton via the package-level
//     store). Every UE/goroutine in the NF shares it.
//   - On a cache HIT the caller returns the stored SearchResult WITHOUT sending
//     any HTTP request to the NRF — so no nnrf-disc line is produced and no
//     NRF->Mongo read is triggered (matches open5gs behaviour).
//   - On a MISS (or an entry older than the TTL) the caller queries the NRF and
//     calls Put() to replace the entry for that key.
//   - TTL forces a refresh after ttl (20 min); in a 1-5 min experiment this
//     effectively never fires, but it bounds staleness for longer runs.
//
// Concurrency: reads are the hot path (~thousands/s) and run concurrently under
// an RLock; only the rare write takes the exclusive Lock. This is the
// read-mostly pattern the project asked for.
//
// Key: the caller builds the key from only the fields that select WHICH NF to
// reach — target NF type, requester NF type, and the requested service names.
// Per-UE query fields (notably supi=, used by the NRF only for SUPI-range
// routing) are deliberately EXCLUDED, because in this deployment every NF type
// has exactly one instance: the NRF returns the same instance regardless of
// supi, so all UEs share one cache entry per (target, requester, services) and
// the NRF is queried about once per distinct lookup for the whole run rather
// than once per UE.
package disccache

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/free5gc/openapi/models"
)

// TTL is the maximum age of a cached discovery result before it is refreshed
// from the NRF. 20 minutes per the experiment spec.
const TTL = 20 * time.Minute

type entry struct {
	result   *models.SearchResult
	storedAt time.Time
}

var (
	mu    sync.RWMutex
	store = make(map[string]entry)
)

// Key builds a stable cache key from only the NF-selecting fields of a discovery
// query: target NF type, requester NF type, and the requested service names.
// supi and other per-UE fields are intentionally not part of the key (single
// instance per NF type — see the package comment), so every UE shares one entry
// per target/requester/service set. serviceNames is order-normalised so callers
// that pass the same set in different orders still hit the same entry.
func Key(targetNfType, requesterNfType models.NrfNfManagementNfType,
	serviceNames []models.ServiceName,
) string {
	svc := make([]string, 0, len(serviceNames))
	for _, s := range serviceNames {
		svc = append(svc, string(s))
	}
	sort.Strings(svc)
	return string(targetNfType) + "|" + string(requesterNfType) + "|" + strings.Join(svc, ",")
}

// Get returns the cached SearchResult for key if present and not older than
// TTL. ok is false on miss, on expiry, or when key is "".
func Get(key string) (result *models.SearchResult, ok bool) {
	if key == "" {
		return nil, false
	}
	mu.RLock()
	e, found := store[key]
	mu.RUnlock()
	if !found {
		return nil, false
	}
	if time.Since(e.storedAt) > TTL {
		return nil, false
	}
	return e.result, true
}

// Put stores (or replaces) the SearchResult for key. A "" key or nil result is
// ignored so we never cache a failed lookup. Replacing the entry for a key
// implicitly removes the previous (missed) record for that key, as required.
func Put(key string, result *models.SearchResult) {
	if key == "" || result == nil {
		return
	}
	mu.Lock()
	store[key] = entry{result: result, storedAt: time.Now()}
	mu.Unlock()
}
