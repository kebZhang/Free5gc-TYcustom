# `golang.org/x/net` local fork

Instrumentation fork for `HTTP_3detailLog_PLAN_0826.md` (points M / G / W).

## Provenance

| | |
|---|---|
| Module | `golang.org/x/net` |
| Version | `v0.47.0` |
| Tagged | 2025-11-11T18:55:31Z |
| Upstream VCS | https://go.googlesource.com/net |
| Upstream ref | `refs/tags/v0.47.0` |
| Upstream commit | `9a296438e54dff851a45667aa645a97003b44db5` |
| Source | `https://proxy.golang.org/golang.org/x/net/@v/v0.47.0.zip` |
| Module dirhash | `h1:Mx+4dIFzqraBXUugkia1OOvlD6LemFo1ALMHjrXDOhY=` |
| Files | 826 |

The dirhash above is the `h1:` recorded for `golang.org/x/net v0.47.0` in the
`go.sum` of every instrumented NF module, so the tree installed here is
byte-identical to what those NFs were already building against. This file is
the only addition; at the commit that introduced it the rest of the tree is
pristine upstream, which makes every later `git diff` against that commit an
exact list of the instrumentation changes.

## Why this version

All seven instrumented NFs (`amf`, `ausf`, `nrf`, `nssf`, `pcf`, `udm`, `udr`)
already require `golang.org/x/net v0.47.0` directly, and all NF modules in the
repo agree on it. Pinning the fork here keeps the build list unchanged, so an
A/B run measures the instrumentation and nothing else.

(`webconsole` is on `v0.48.0` but only indirectly, and is not on the SBI
measurement path. Do not add the replace there.)

## Why the fork is needed at all (and why the Go standard library is not)

Both directions of NF-to-NF SBI traffic go through *this* module, never through
the HTTP/2 bundled inside `net/http`:

- **Client** — `internal/accesslog/httptransport.go` installs
  `&http2.Transport{...}` from this module as the `http.RoundTripper` directly.
  `net/http`'s own HTTP/2 is never configured or reached.
- **Server** — `internal/sbi/server.go::newHttp2ServerWithIdleTimeout` serves
  `h2c.NewHandler(handler, &http2.Server{...})` from this module. Every NF runs
  `scheme: http`, so `ListenAndServe` (not TLS) is used and the standard
  library's automatic HTTP/2 is never enabled.

The existing six timestamps agree: T2 and T5 come from `httptrace` callbacks
that this module invokes (`traceWroteRequest`, `traceGotFirstResponseByte`).

## Hook points used by the plan

| Point | File | Symbol |
|---|---|---|
| M | `http2/transport.go` | `(*clientStream).writeRequest`, immediately before the `cc.reqHeaderMu <- struct{}{}` select |
| retry_count | `http2/transport.go` | `(*Transport).RoundTripOpt`, the `for retry := 0; ; retry++` attempt loop |
| stream_id | `http2/transport.go` | `(*clientStream).writeRequest`, at the existing `streamf(cs)` call site |
| G (immediate) | `http2/server.go` | `(*serverConn).scheduleHandler`, before `go sc.runHandler(rw, req, handler)` |
| G (after queueing) | `http2/server.go` | `(*serverConn).handlerDone`, before `go sc.runHandler(u.rw, u.req, u.handler)` |
| server trace | `http2/server.go` | `(*serverConn).newWriterAndRequestNoBody`, before `.WithContext(st.ctx)` |
| W marker | `http2/write.go` | `(*writeResHeaders).writeHeaderBlock`, before the final-fragment Framer write |
| W timestamp | `http2/http2.go` | `writeWithByteTimeout`, after each real `conn.Write` returns |

Two other `go sc.runHandler` sites exist and are deliberately **not**
instrumented, per plan section 10.3:

- `http2/server.go` `(*serverConn).upgradeRequest` — the first request of an
  HTTP/1.1 `Upgrade: h2c` connection.
- `http2/server.go` `(*serverConn).startPush` — HTTP/2 server push.

Neither is reachable from this deployment: the client transport uses
`AllowHTTP` + a plain-TCP `DialTLSContext`, i.e. prior-knowledge h2c, so
`ServeConnOpts.UpgradeRequest` is always nil; and no NF calls `Pusher.Push`.
G coverage over real SBI traffic is therefore complete.

## Wiring it up

Add to the `go.mod` of each instrumented NF (`NFs/<nf>/go.mod`):

    replace golang.org/x/net => ../../xnet

then run `go mod tidy` in that module. `tidy` may add `golang.org/x/term`
entries to `go.sum` for the modules that lack them today (`udm`, `amf`, `ausf`,
`nssf`); that is expected — a filesystem `replace` re-reads this module's
`go.mod`, whose requirements are unchanged from the proxy copy.

A filesystem `replace` bypasses `go.sum` verification for this module, which is
the point: the tree is modified from here on.
