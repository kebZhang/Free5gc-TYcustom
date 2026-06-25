# NRF Discovery Cache (TYcustom instrumentation)

This fork adds a per-NF, process-wide cache of NRF NF-discovery results so each
NF reuses a previous `Nnrf_NFDiscovery` `SearchResult` instead of querying the
NRF on every lookup. It mirrors the caching open5gs does in
`ogs_sbi_discover_and_send()` (cache hit → no NRF round-trip), in a deliberately
minimal form suited to a short (1–5 min), single-instance experiment.

## Behaviour

- **One cache per NF process.** `internal/disccache` is a singleton (package-level
  store). Every UE/goroutine in that NF shares it. Different NF processes have
  independent caches (AMF's cache ≠ AUSF's cache); nothing is shared across pods.
- **Hit → skip the NRF entirely.** On a cache hit the consumer returns the stored
  `SearchResult` **without sending any HTTP request**. So:
  - no `GET /nnrf-disc/...` line appears in `HTTP_log.txt` for that lookup, and
  - no NRF→MongoDB `NfProfile` read is triggered.
  This is the intended semantics (matches open5gs) and **changes the baseline**
  recorded for the "NRF has no cache, 1 disc == 1 NfProfile read" experiment: with
  this cache each NF queries the NRF roughly once per distinct query for the whole
  run, not once per UE.
- **Miss / empty / expired → query NRF, then replace the entry.** A miss queries
  the NRF as before; on success the result replaces the entry for that key
  (implicitly removing the previous, missed record for the key). A failed lookup
  is never cached.
- **Forced refresh after TTL.** `disccache.TTL = 20 * time.Minute`. An entry older
  than the TTL is treated as a miss and re-fetched. In a 1–5 min experiment this
  never fires; it bounds staleness for longer runs.

## Concurrency

Reads are the hot path (~thousands/s during a registration storm) and run
concurrently under a `sync.RWMutex` `RLock`. Only the rare write (first lookup of
a key, or a post-TTL refresh) takes the exclusive `Lock`. This is the read-mostly
pattern required for high registration rates; a plain `map` without the lock would
crash under concurrent read/write.

## Cache key

`disccache.Key(targetNfType, requesterNfType, serviceNames)` builds the key from
ONLY the fields that select **which NF** to reach. `serviceNames` is
order-normalised (sorted) so the same set in any order hits the same entry.

Per-UE query fields — notably `supi=` — are **intentionally excluded** from the
key. This deployment runs **exactly one instance per NF type**, so the NRF
returns the same instance regardless of `supi` (which it would only use for
SUPI-range routing across multiple instances). Excluding `supi` means all UEs
share one entry per `(target, requester, services)`, so the cache actually hits
across UEs and the NRF is queried about **once per distinct lookup for the whole
run** instead of once per UE — including for the AMF→UDM and AMF→PCF lookups
whose query carries `supi`.

> Caveat: this is correct **only** because every NF type is single-instance. If
> you later run multiple UDM/PCF/AUSF split by SUPI range, a `supi`-less key would
> mis-route, and `supi` (or the SUPI range) must be put back into the key.

## Instrumented NFs

Cache is applied at each NF's primary discovery entry point
(`SendSearchNFInstances`, or `SearchNFInstances` for NEF):

- **AMF, AUSF, UDM, PCF** — the UE-registration discovery path.
- **SMF, CHF, NEF, UDR** — their primary discovery entry point only. SMF/NEF have
  additional helpers that call `client.NFInstancesStoreApi.SearchNFInstances`
  directly (PDU-session / non-registration paths); those are **not** cached.

NSSF, BSF, and the NRF's own consumer issue no `nnrf-disc` on these paths and are
unchanged.

## Files

- `NFs/<nf>/internal/disccache/disccache.go` — identical cache package per NF.
- `NFs/<nf>/internal/sbi/consumer/nrf_service.go` — the discovery entry point now
  does `Key → Get (return on hit) → query NRF → Put (on success)`.

## Caveats (by design, given the experiment assumptions)

- **No failure-driven eviction.** If a discovered instance dies mid-run, the stale
  entry survives until the 20-min TTL. This is acceptable only because the
  experiment assumes instances do not fail during a 1–5 min run. For longer/HA
  runs, add "evict on request failure" (and/or NFStatus subscribe/notify).
- **No size-bounded eviction.** Entries are only replaced (by key) or expired, not
  removed on registration churn. Fine for a fixed, single-instance topology; would
  slowly grow if NfInstanceIds churn (e.g. rolling upgrades).
