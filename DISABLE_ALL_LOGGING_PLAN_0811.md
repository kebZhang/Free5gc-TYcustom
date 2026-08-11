# Plan: fully disable all TYcustom instrumentation (baseline control run)

Status: IMPLEMENTED — see "What was actually changed" at the end.

## Why

Every analysis so far rests on numbers produced by this instrumentation, and the
instrumentation's cost scales with exactly the thing being measured. Per RQ2500
run (measured from the captured files):

| | per run |
|---|---|
| bytes written | 20.22 MB |
| log lines | 62,000 (HTTP 42,000 / DB 11,000 / NGAP 6,000 / worker 3,000) |
| burst duration | 0.530 s |
| **sustained rate** | **~38 MB/s, ~107,000 lines/s** |

One UE registration crosses 21 SBI hops, and HTTP_log records **two** lines per
hop (client view + server view), plus 11 DB lines. So the number of log records
is proportional to the number of hops — the same quantity whose growing latency
is under investigation. That circularity cannot be resolved from the existing
captures; it needs an A/B run.

`/proc/pressure/io` showed the disk is not blocking anyone (≤0.7% of the burst
even as an upper bound), but PSI-io only covers block-device waits. It does not
cover any of the following, which is where this instrumentation actually spends:

- JSON line building (`appendKV` etc.) on the request goroutine
- `enqueue` into a 2M-entry channel, and blocking/dropping when full
- per-record allocation (`make([]byte, 0, 304)` × 107k/s)
- `httptrace.ClientTrace` callbacks on the HTTP/2 write path and the shared
  connection read loop
- **`sniffUEID` / `sniffInboundUEID`: `io.ReadAll` of the request body plus a
  JSON field extraction, on POST `ue-authentications` and POST `policies`, on
  BOTH client and server side**
- the gin `InboundLogger` middleware wrapping every server handler
- `msgtrace` allocation and mutation per NAS message in the AMF
- the `time.Now()` taken right after every `SCTPRead`, which exists purely to
  feed AMF_log

## The two hard requirements

1. **No log write and no log-purpose timestamp anywhere.** The four files
   (HTTP_log / DB_log / AMF_log / AMF_worker_log) must receive nothing, and the
   code that computes what would have gone into them must not run either. Code
   is disabled behind a flag, **not deleted**, so one line restores it.
2. **The 16 HTTP connections per NF pair must survive.** `connsPerPeer = 16` and
   the round-robin are the experiment's independent variable, not
   instrumentation. If the control build silently drops to 1 connection, the A/B
   comparison changes two variables at once and is worthless.

Every change below is justified against one of these two. Anything that does not
serve them was dropped (see "Deliberately NOT changed").

## Design choice

Use a **compile-time constant kill switch**, not an env var.

```go
// accesslog/accesslog.go
const Enabled = false
```

Rationale: with a `const false`, the Go compiler eliminates the guarded code and
the `if Enabled` branches entirely, so the control build pays *zero* — no branch,
no interface call, no retained allocation. An env var would leave a load+branch
on every hop and keep every closure alive; that is a weaker control, and the
question being asked is precisely "what does the instrumentation cost".

## Scope: there are SEVEN accesslog copies, not eight

`amf, ausf, nrf, nssf, pcf, udm, udr` each have `internal/accesslog/`.

`smf`, `nef`, `chf` have **no accesslog package at all** — their only mention of
`accesslog.Client()` is inside a code comment in `internal/sbi/server.go`. They
need no change whatsoever.

The seven `httptransport.go` copies are byte-identical (verified by md5). The
seven `accesslog.go` copies differ only in `srcNF` and in AMF having two extra
record kinds; the six non-AMF ones are structurally identical.

---

## The six necessary changes

### 1. `NFs/<nf>/internal/accesslog/accesslog.go` — 7 copies  → requirement 1

Add the kill switch and stop every record at the door:

1. `const Enabled = false` near the top.
2. `func init() { Init() }` → `func init() { if Enabled { Init() } }`, and an
   early `return` in `Init()` when `!Enabled`. This is what prevents the 2M-entry
   channel (`1<<21` records), the writer goroutine, and the four 1 MiB
   `bufio.Writer`s from ever being allocated — and it is also what stops
   `openLog` from **creating the four files**. A disabled build leaves no
   `/tmp/*_log.txt` behind at all, which is the acceptance signal.
3. Every `LogXxx` entry point gets `if !Enabled { return }` as its **first**
   statement, before any `make([]byte, ...)`:
   - all 7 NFs: `LogHTTP`, `LogHTTPInbound`, `LogDB`
   - AMF only: `LogNGAP`, `LogWorker`

   Guard-first is the point: it removes the per-record allocation and the JSON
   building, which dominate. The file write was never the expensive part.

These five entry points are the **only** writers to the four log files (verified:
nothing else in `NFs/` references the paths or the env vars), so this step alone
guarantees the files stay empty.

### 2. `NFs/<nf>/internal/accesslog/httptransport.go` — 7 copies  → requirements 1 AND 2

**This is the only risky edit in the whole plan.** `loggingRoundTripper` is *not*
a wrapper around a connection pool — it *is* the connection pool:

```go
type loggingRoundTripper struct {
    tls   [connsPerPeer]http.RoundTripper // the 16 connections
    clear [connsPerPeer]http.RoundTripper
    next  atomic.Uint64                   // the round-robin cursor
}
```

Round-robin dispatch (`connSlot := int((l.next.Add(1)-1) % connsPerPeer)`) and
log recording live in the *same* `RoundTrip` method. "Return a plain client
without the wrapper" would therefore drop each NF pair back to **one**
connection and destroy requirement 2.

The edit must instead **split the two responsibilities**:

4. Extract the pool into its own type, `roundRobinTransport`, holding the two
   `[connsPerPeer]http.RoundTripper` arrays and the `next` cursor, with a
   `pick()` returning `(base, connSlot)`. Its construction is verbatim the
   current `newLoggingRoundTripper` body.
5. `loggingRoundTripper` keeps a `*roundRobinTransport` and adds only the
   timestamps/trace/sniff on top of it.
6. `Client()` returns a client over the **bare `roundRobinTransport`** when
   `!Enabled`, and over `loggingRoundTripper` when enabled. Both share the same
   pool construction, so the connection behaviour is identical in both builds.
7. `sniffUEID` / `sniffInboundUEID` → `if !Enabled { return "" }` first. This is
   what removes `io.ReadAll` + `json.Unmarshal` + body re-wrap from POST
   `ue-authentications` and POST `policies`, on both client and server side.
8. `InboundLogger()` → returns a pass-through when `!Enabled` (belt-and-braces;
   step 3 below means it is never even registered).

**Hard constraints on this edit — these four constants must remain byte-for-byte
unchanged**, because they govern connection count and connection lifetime:

```go
const connsPerPeer         = 16
const readIdleTimeoutPeriod = 1 * time.Second
const pingTimeoutPeriod     = 3 * time.Second
const timeoutPeriod         = 10 * time.Second
```

Note the consequence for verification: with logging off there are no `conn` /
`conn_slot` / `conn_reused` fields any more, so the **only** way to confirm the
16 connections survived is `ss -tnp` inside a pod. That check is mandatory, not
optional.

### 3. `NFs/<nf>/internal/sbi/server.go` — 7 NFs  → requirement 1

9. Wrap the registration so the middleware is not in the chain at all:

```go
if accesslog.Enabled {
    router.Use(accesslog.InboundLogger())
}
```

A pass-through middleware would still cost one gin frame per request; not
registering costs nothing.

### 4. `NFs/amf/internal/nas/handler.go` — 1 line  → requirement 1

10. `tr := msgtrace.New(time.Now())` → only construct when `Enabled`, otherwise
    leave `tr` nil.

**This single line removes fourteen call sites' worth of timestamps**, because
every `msgtrace` method is nil-safe and does no work on the nil path. Verified
nil-safe in `msgtrace.go`: `SetID`, `AddSBI`, `Track`, `SetDLNas`, `DLNas`. In
particular `Track` returns a shared `noop` closure and **does not even call
`time.Now()`** when the receiver is nil.

The fourteen sites that are therefore **left untouched on purpose**:

| file | sites |
|---|---|
| `sbi/consumer/udm_service.go` | 99, 148, 195, 237, 288, 393 |
| `sbi/consumer/ausf_service.go` | 56, 110 |
| `sbi/consumer/nssf_service.go` | 49 |
| `sbi/consumer/pcf_service.go` | 52 |
| `gmm/message/send.go` | 190, 481, 625 (`SetDLNas`) |
| `ngap/message/send.go` | 84 (`DLNas`) |

Do **not** add guards at these sites. They are already free.

**One nil-safety trap, found and fixed during implementation.** Nil-safety covers
msgtrace's *methods*, not its *fields*. `HandleNAS`'s own defer reads
`tr.NasType` — a direct field access, which panics on a nil trace and would have
crashed the AMF on every NAS message. It now reads:

```go
if tr != nil && tr.NasType != "" {
```

Audited: this was the only field access on a possibly-nil trace in the tree.
Every other site (the fourteen above) is a method call and is genuinely safe. If
anyone later adds a `tr.<Field>` read outside `msgtrace`, it needs the same nil
check.

### 5. `NFs/{udr,pcf,nrf}/internal/dbtrace/dbtrace.go` — 3 copies  → requirement 1

11. `logDB` gets `if !accesslog.Enabled { return }` as its first statement.

UDR is the one that matters: 11 DB ops per UE registration.

Deliberately *not* done: guarding each of the 10 wrapper functions' `start :=
time.Now()` individually. Under `const false` that makes `start` an unused
variable in every one of them (30 sites across 3 NFs) and will not compile
without restructuring each function into two branches. The residual cost is one
`time.Now()` per DB call with the result thrown away — negligible next to the
`io.ReadAll`/JSON/enqueue work that step 11 does remove.

### 6. `NFs/amf/internal/ngap/service/service.go` — 1 line  → requirement 1

12. `recvTime := time.Now()` immediately after `conn.SCTPRead(buf)` exists
    *solely* to feed AMF_log (its own comment says so). Guard it so the SCTP read
    loop takes no timestamp at all. `recvTime` stays declared (it is threaded
    through `Dispatch` → the generated dispatcher → `HandleNAS` as a parameter and
    those signatures must not change) but keeps its zero value.

The zero value is already handled downstream: `logUplinkNAS` does
`if t.IsZero() { return }`. Nothing else reads it.

---

## Deliberately NOT changed

These were in the previous draft of this plan and are redundant. Adding them
would only create opportunities to break something:

| dropped step | why it is unnecessary |
|---|---|
| guard `Flush()` | `flushReq` stays nil, so it already returns on its first line. Zero callers exist in the tree anyway. |
| guard `toSBIViews` slice construction | `tr.NasType` is always `""` (`SetID` is a nil-safe no-op), so `HandleNAS`'s defer never reaches the call. |
| guard `accesslog.LogNGAP` / `LogWorker` at their AMF call sites | Already dead via step 3; with `const false` the compiler removes the call bodies. |
| guard `msgtrace.Trace` in `context/amf_ue.go` | There is no call there — only the struct field `WorkerTrace *msgtrace.Trace` and comments. A nil pointer field costs nothing. |
| guard `accesslog.LogNGAP` in `ngap/message/send.go` | Guarded by `if ... DLNas(); has`, which returns `has=false` on the nil trace, so the `time.Now()` never runs. Free via step 10. |
| any change to `smf` / `nef` / `chf` | They have no accesslog package; the only mention is a comment. |

---

## Build and deploy

Per the existing image workflow (`cloudlab/K8s/UPDATE_free5gc_custom_image.md`):

1. Build all NFs into the single `free5gc-custom` image on node-0.
2. Tag it distinctly, e.g. `free5gc-custom:nolog-0811`, so the two builds can be
   swapped by image tag alone and switched back without a rebuild.
3. Watch the three known gotchas recorded for this workflow: NRF restart
   ordering, `sequenceNumber` format, downstream NF re-registration.

## The experiment

| group | image | runs |
|---|---|---|
| A (current) | `free5gc-custom:<current>` | RQ 800 / 1000 / 1500 / 2000 / 2500, UE1000 |
| B (control) | `free5gc-custom:nolog-0811` | same five RQs |

Collect for both: `latency_RQ*_UE1000.txt` and `Cpu_Mem_RQ*_UE1000.txt`. Neither
depends on NF-internal instrumentation, so they stay comparable.

Run all five RQs in each group. If the instrumentation has an effect it will
scale with RQ, so a single high-RQ point cannot separate a fixed offset from a
load-dependent one.

## Acceptance checks — one per requirement

**Requirement 1 — nothing is logged.** Before starting group B, delete any
`/tmp/*_log.txt` left over from group A: the writer opens with `O_APPEND`, so a
stale file would otherwise look like a live one. Then after a B run, inside each
NF pod:

```sh
ls -l /tmp/HTTP_log.txt /tmp/DB_log.txt /tmp/AMF_log.txt /tmp/AMF_worker_log.txt
# expected: "No such file or directory" for all four
```

Absence, not emptiness, is the correct result: with `Init()` guarded, `openLog`
never runs, so the files are never created.

**Requirement 2 — 16 connections survive.** Inside a pod with a peer NF:

```sh
ss -tnp | grep <peer-pod-ip> | wc -l
# expected: 16 per NF pair
```

This is the only available check, since the `conn_slot` log field is gone.

## How to read the result

| outcome | meaning |
|---|---|
| B's E2E curve ≈ A's at every RQ | instrumentation is not perturbing; all prior analysis stands |
| B lower, and the **gap widens with RQ** | instrumentation is a major contributor to the measured growth; the "inter-NF transport rises 5x" finding is partly self-inflicted and the measurement approach must be redesigned |
| B lower by a roughly constant offset | fixed overhead only; the trend conclusions survive, absolute values need correction |

Also compare per-process CPU in `Cpu_Mem_*` between A and B: the difference is a
direct measure of what the instrumentation costs in CPU, independent of latency.

## Optional third group

If B differs materially from A, a group C with **HTTP_log only** (DB/NGAP/worker
disabled) localises which log dominates. Not needed unless A/B differ.

---

## What was actually changed

To re-enable instrumentation, set `const Enabled = true` in all seven
`NFs/<nf>/internal/accesslog/accesslog.go` and rebuild. Nothing else needs
touching.

| file | copies | change |
|---|---|---|
| `internal/accesslog/accesslog.go` | 7 | `const Enabled = false`; guard `init()`/`Init()`; `if !Enabled { return }` first in `LogHTTP`, `LogHTTPInbound`, `LogDB` (+ `LogNGAP`, `LogWorker` in AMF) |
| `internal/accesslog/httptransport.go` | 7 | extract `roundRobinTransport`; `Client()` returns bare pool when disabled; guard `sniffUEID`, `sniffInboundUEID`, `InboundLogger` |
| `internal/sbi/server.go` | 7 | `if accesslog.Enabled { router.Use(accesslog.InboundLogger()) }` |
| `amf/internal/nas/handler.go` | 1 | `msgtrace.New` only when `Enabled` |
| `{udr,pcf,nrf}/internal/dbtrace/dbtrace.go` | 3 | `if !accesslog.Enabled { return }` first in `logDB` |
| `amf/internal/ngap/service/service.go` | 1 | guard the post-`SCTPRead` `time.Now()` |
