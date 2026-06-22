# HTTP / MongoDB Access Logging (TYcustom instrumentation)

This fork adds low-overhead logging of (1) every outgoing HTTP request/response
from the **requester NF's** point of view, (2) every *incoming* HTTP
request/response from the **receiver NF's** point of view, and (3) every
NF↔MongoDB interaction from the **NF's** point of view. It is built for high
registration rates (~1000 reg/s, ~40–50 HTTP messages per registration) without
torn or interleaved log lines.

Both the client (sender) view and the server (receiver) view of HTTP are written
to the **same** `HTTP_log.txt` with the **same** fields, so the two halves of one
message can be analyzed together. They are told apart by `src`: the client view
records the sending NF in `src`; the server view cannot identify the sender from
the SBI URI (and direct communication carries no token), so its `src` is the
literal string `"NaN"` and its `dst` is the receiving NF.

## What is logged

### `HTTP_log.txt` — one JSON object per line (JSON Lines)

Each line is one of two views of an HTTP message, distinguished by `src`:

**Client view** — recorded by the NF that **sends** the request:

| field        | meaning                                              |
|--------------|------------------------------------------------------|
| `src`        | NF that sent the request (this NF)                   |
| `dst`        | destination NF (derived from the SBI service prefix) |
| `method`     | HTTP method                                          |
| `uri`        | full request URI                                     |
| `ue_id`      | UE id this request is for (may be ""); see note below |
| `req_time`   | when the request was sent (RFC3339Nano, UTC)         |
| `resp_time`  | when the response/error was received (RFC3339Nano)   |
| `latency_us` | resp_time − req_time, in microseconds                |

**Server view** — recorded by the NF that **receives** the request. Same fields,
same file; only the meaning of `src`/`dst`/the timestamps changes:

| field        | meaning                                                       |
|--------------|---------------------------------------------------------------|
| `src`        | always `"NaN"` — the sender NF cannot be identified server-side |
| `dst`        | the NF that received the request (this NF)                     |
| `method`     | HTTP method                                                    |
| `uri`        | full request URI (reconstructed: scheme + Host header + path) |
| `ue_id`      | UE id this request is for (may be ""); same rules as below     |
| `req_time`   | when the request **arrived** at this server (RFC3339Nano, UTC) |
| `resp_time`  | when the response was **sent back** (RFC3339Nano)             |
| `latency_us` | resp_time − req_time = server-side processing time, in µs     |

So one cross-NF hop produces two lines: a client line `{"src":SENDER,"dst":RECV,...}`
and a server line `{"src":"NaN","dst":RECV,...}`. Pair them by `ue_id` + `uri` +
time; the sender identity comes from the matching client line.

`ue_id` note: for most requests the UE id is already in the `uri`
(e.g. `.../nudm-sdm/v2/imsi-208930000000001/...`), so `ue_id` is left empty —
extract it from the `uri` instead. For the few request types whose UE id is
carried **only in the request body**, the transport sniffs the body and fills
`ue_id` so the request can still be attributed to a UE:

| request                                          | body field   |
|--------------------------------------------------|--------------|
| `POST /nausf-auth/v1/ue-authentications`         | `supiOrSuci` |
| `POST /npcf-am-policy-control/v1/policies`       | `supi`       |

Infrastructure requests that do not belong to any UE (e.g.
`GET /nnrf-disc/v1/nf-instances`, NF registration/heartbeat under `nnrf-nfm`)
always have an empty `ue_id` — by design, not because it was missed.

Example (client view, then the matching server view of the same hop):
```json
{"src":"AMF","dst":"UDM","method":"GET","uri":"http://udm:8000/nudm-sdm/v2/imsi-208930000000001/am-data?plmn-id=...","ue_id":"","req_time":"2026-06-17T09:00:00.123456Z","resp_time":"2026-06-17T09:00:00.124900Z","latency_us":1444}
{"src":"NaN","dst":"UDM","method":"GET","uri":"http://udm:8000/nudm-sdm/v2/imsi-208930000000001/am-data?plmn-id=...","ue_id":"","req_time":"2026-06-17T09:00:00.123900Z","resp_time":"2026-06-17T09:00:00.124700Z","latency_us":800}
{"src":"AMF","dst":"AUSF","method":"POST","uri":"http://ausf:8000/nausf-auth/v1/ue-authentications","ue_id":"suci-0-999-70-0-0-0-0000000001","req_time":"2026-06-17T09:00:00.130000Z","resp_time":"2026-06-17T09:00:00.135000Z","latency_us":5000}
```

### `DB_log.txt` — one JSON object per line (JSON Lines)

Recorded by the NF that issues the MongoDB operation.

| field        | meaning                                          |
|--------------|--------------------------------------------------|
| `nf`         | NF that issued the query (this NF)               |
| `mongo`      | database peer identifier (`mongodb`)             |
| `resource`   | collection / table name                          |
| `operation`  | DB operation type (find/update/else): `GetOne`, `GetMany`, `PutOne`, `DeleteOne`, `DeleteMany`, `MergePatch`, `JSONPatch`, `JSONPatchExtend`, `PullOne`, `Post` |
| `ue_id`      | UE id extracted from the query filter (may be "")|
| `req_time`   | when the DB request was issued (RFC3339Nano, UTC)|
| `resp_time`  | when the DB reply was received (RFC3339Nano)     |
| `latency_us` | resp_time − req_time, in microseconds            |

Example:
```json
{"nf":"UDR","mongo":"mongodb","resource":"subscriptionData.authenticationData.authenticationSubscription","operation":"GetOne","ue_id":"imsi-208930000000001","req_time":"2026-06-17T09:00:00.123456Z","resp_time":"2026-06-17T09:00:00.124000Z","latency_us":544}
{"nf":"UDR","mongo":"mongodb","resource":"subscriptionData.authenticationData.authenticationStatus","operation":"PutOne","ue_id":"imsi-208930000000001","req_time":"2026-06-17T09:00:00.130000Z","resp_time":"2026-06-17T09:00:00.130800Z","latency_us":800}
```

Order of lines is **not** significant — analyze by timestamp.

## Instrumented NFs (registration path)

- **HTTP (client view):** AMF, AUSF, UDM, UDR, NRF, PCF, NSSF
- **HTTP (server view):** AMF, AUSF, UDM, UDR, NRF, PCF, NSSF
- **MongoDB:** UDR, NRF, PCF (the NFs that talk to MongoDB)

## How it works (and why it's cheap)

`internal/accesslog` (one copy per NF module):
- The hot path only formats a small JSON line and pushes it onto a large
  buffered channel — it never does file I/O and never blocks the data path.
- A **single** background writer goroutine drains the channel and appends to the
  file with a buffered writer. Because there is exactly one writer, lines can
  never interleave or tear.
- If the channel is ever full it drops the record (counted via
  `accesslog.Dropped()`) rather than stalling the NF.
- Buffers are flushed to disk every 200 ms. `accesslog.Flush()` forces a
  synchronous flush (e.g. call it on shutdown before reading the files); note a
  hard pod `SIGKILL` can still lose the last ≤200 ms of records.

HTTP **client-view** interception uses the openapi `Configuration.SetHTTPClient(...)`
hook: each service client is given an `*http.Client` whose `Transport` is a
logging `RoundTripper` wrapping the same HTTP/2 (h2 / h2c) transports
free5gc/openapi uses internally, so behaviour is unchanged apart from the logging.

HTTP **server-view** interception uses a gin middleware (`accesslog.InboundLogger()`)
registered once on each NF's SBI router right after `metrics.InboundMetrics()`. It
records the arrival time before the handler runs and the completion time after
(`c.Next()`), then emits the same `HTTP_log.txt` line with `src:"NaN"`. It only
buffers a request body for the same small set of endpoints whose UE id lives in
the body (and restores it), so every other request is untouched.

MongoDB interception uses `internal/dbtrace`, a drop-in wrapper with identical
signatures to `free5gc/util/mongoapi`; call sites were switched from
`mongoapi.RestfulAPIXxx(` to `dbtrace.RestfulAPIXxx(`.

## Configuration / K8s

Log file paths are read from environment variables (per pod):

| env var         | default              |
|-----------------|----------------------|
| `HTTP_LOG_PATH` | `/tmp/HTTP_log.txt`  |
| `DB_LOG_PATH`   | `/tmp/DB_log.txt`    |

Since each NF runs as its own pod, each pod writes its own files. To collect
them, mount a volume (e.g. `emptyDir`, `hostPath`, or a PVC) and point the env
vars at it, for example:

```yaml
env:
  - name: HTTP_LOG_PATH
    value: /var/log/free5gc/HTTP_log.txt
  - name: DB_LOG_PATH
    value: /var/log/free5gc/DB_log.txt
volumeMounts:
  - name: accesslog
    mountPath: /var/log/free5gc
volumes:
  - name: accesslog
    emptyDir: {}
```
