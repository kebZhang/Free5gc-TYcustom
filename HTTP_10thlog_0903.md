# 第 10 个时间点：`reqHeaderMu` 成功加锁时刻 —— 代码修改计划（0903）

本文是 `HTTP_3detailLog_PLAN_0826.md` 的续篇，**只描述代码怎么改**。

0826 计划把每个 HTTP request/response 的时间点做到九个（T1/M/T2 + G/T3/T4 + W + T5/T6），
并在 §9「第一阶段边界」里写下「不增加单独的 `reqHeaderMu` 成功加锁时间」；同一份计划 §8 又说
「要证明某一个锁，需要下一阶段增加锁取得时间」。本文就是那个下一阶段。

**本次要做的事，一句话：** 在 `xnet/http2/transport.go` 抢到 `reqHeaderMu` 的那个 select 成功
分支里加一次时间戳，经 `ClientRequestTrace` 传出，作为一个新字段
`req_header_mu_acq_time` 写进现有 client 日志行。

**明确不做：** 不在进程内计算锁等待时长（不加 duration 字段），等待时长由离线脚本相减得出。

改动涉及 4 个文件（其中 2 个要复制到 7 个 NF）：

```text
xnet/http2/instrument_client.go              加 1 个字段 + 1 个 getter
xnet/http2/transport.go                      加 1 次取时（select 成功分支内）
NFs/<nf>/internal/accesslog/httptransport.go 加 1 行 load + 传 1 个实参     x7（逐字节相同）
NFs/<nf>/internal/accesslog/accesslog.go     加 1 个参数 + 1 行 append + 上调容量  x7（各不同）
```

行号均以当前 working tree 实测为准（HEAD，`git status` 干净）。

---

## 1. 现有九个时间点的取时位置（改代码前先对照）

七个已启用 access log 的 NF（`amf`、`ausf`、`nrf`、`nssf`、`pcf`、`udm`、`udr`）的
`internal/accesslog/httptransport.go` 经 `md5sum` 复核**逐字节相同**
（`5e83ba1b07a14329707a250a1760f2b0`），故客户端行号对七个 NF 同时成立。

`accesslog.go` 七份**不同**（`srcNF` 常量 + AMF 独有的 NGAP/worker 部分）：
**AMF 的行号比其余六个 NF 大 31**。下文凡涉及 `accesslog.go` 一律给出两个行号。

| 点 | 承载记录 | 文件 / 位置 | 行号 |
|---|---|---|---|
| T1 `req_time` | client JSON | `NFs/<nf>/internal/accesslog/httptransport.go` `(*loggingRoundTripper).RoundTrip` | **278** |
| **M `req_header_mu_start_time`** | client JSON | `xnet/http2/transport.go` `(*clientStream).writeRequest`，`select` **之前** | **1447-1449** |
| T2 `wrote_time` | client JSON | 同 RoundTrip 内 `httptrace.ClientTrace.WroteRequest` 闭包 | **250-254** |
| G `server_handler_go_time` | server JSON | `xnet/http2/server.go`，`go sc.runHandler(...)` 之前 | 2407 / 2447 |
| T3 `req_time` | server JSON | 同文件 `InboundLogger()` 返回的 gin middleware | **400** |
| T4 `resp_time` | server JSON | 同 middleware，`c.Next()` 返回后 | **408** |
| W `server_response_headers_flushed_time` | 独立 W event | `xnet/http2/http2.go`，真实 `conn.Write` 之后 | 408 附近 |
| T5 `got_first_byte` | client JSON | 同 RoundTrip 内 `GotFirstResponseByte` 闭包 | **255-257** |
| T6 `resp_time` | client JSON | `(*loggingRoundTripper).RoundTrip`，`base.RoundTrip` 返回后 | **280** |

时间序：`T1 → M → T2 → G → T3 → T4 → W → T5 → T6`。

**M 之后紧接的就是 `select`，而 select 成功之后到 T2 之间没有任何时间点。** 这就是要填的空。

---

## 2. 新增字段的定义与插入点

### 2.1 语义

```text
req_header_mu_acq_time   （下称 M2）
    = 本次 attempt 成功向 cc.reqHeaderMu 写入信号量、
      即真正取得该连接"发送新 request 的独占权"的那一瞬间
```

`cc.reqHeaderMu` 的定义在 `xnet/http2/transport.go:380-383`：

```go
	// reqHeaderMu is a 1-element semaphore channel controlling access to sending new requests.
	// Write to reqHeaderMu to lock it, read from it to unlock.
	reqHeaderMu chan struct{}
```

它是 **`chan struct{}` 而不是 `sync.Mutex`**（要能被 ctx 取消，`sync.Mutex.Lock()` 没法
select）。这就是为什么必须自己打点：**mutex profile 完全看不见它**，而 block profile 的栈里
没有对端地址，分不出 UDM→UDR 还是 UDM→NRF。

### 2.2 它把 `T2 - M` 拆成的两段

`T2 - M` 现在是一段混合区间。临界区的真实范围是
**`transport.go:1451` 拿锁 → `transport.go:1502` 放锁**，`T2 - M` 依次包含：

- 等 `cc.reqHeaderMu`（**1450-1456** 的 `select`）；
- `cc.mu.Lock()`（**1458**）；
- `awaitOpenSlotForStreamLocked` 里的 `cc.cond.Wait()` 等 stream 配额（**1463** → 函数体 **1736** 起）；
- `cc.addStreamLocked(cs)` 分配 stream ID（**1468**）；
- `encodeAndWriteHeaders`：抢 `cc.wmu`（**1579**）+ HPACK 编码 + 写帧 + flush（**1575-1613**）；
- `<-cc.reqHeaderMu` 放锁（**1502**）；
- 有 body 时 `writeRequestBody`（**1533**，已在锁外）：DATA 组帧与 flow control 等待；
- `traceWroteRequest`（**1543**）触发 T2。

加入 M2 后：

```text
M  → M2   = 纯 cc.reqHeaderMu 排队时间
M2 → T2   = 锁内的一切（cc.mu / stream 配额 / HPACK / wmu / 写帧 / flush [+ body]）
```

### 2.3 必须放在 `select` 的成功分支体内，不能放在 `select` 之后

`xnet/http2/transport.go:1443-1456` 现状：

```go
1443		// TYcustom M (req_header_mu_start_time): the instant this attempt begins
1444		// contending for reqHeaderMu. It must be taken before the select, not after
1445		// the send succeeds -- the wait itself is what we are measuring. One clock
1446		// read and one atomic store; nothing else is allowed here.
1447		if cs.instr != nil {
1448			cs.instr.mUnixNano.Store(time.Now().UnixNano())
1449		}
1450		select {
1451		case cc.reqHeaderMu <- struct{}{}:
1452		case <-cs.reqCancel:
1453			return errRequestCanceled
1454		case <-ctx.Done():
1455			return ctx.Err()
1456		}
```

另两个分支都是 `return`，所以写在 `select` 之后在功能上等价；但**必须写在
`case cc.reqHeaderMu <- struct{}{}:`（1451 行）分支体内**，两个理由：

1. **语义显式**：这个点的定义就是"这一次 channel send 成功了"。写在分支内，代码本身即文档，
   将来有人在 select 后面插一句语句也不会悄悄改变含义。
2. **它顺带定义了一个新的诊断状态**：`M != 0 && M2 == 0` 恰好等于**"该 request 在排队等
   `reqHeaderMu` 的过程中被 ctx 超时或 reqCancel 取消"**。当前 `timeoutPeriod = 10s`
   （`httptransport.go:47`）下这是真实可能的失败模式，而九点方案分不出"等锁时被取消"和
   "锁后失败"。见 §6.3 的分桶表。

### 2.4 为什么不在进程内算 duration（以及离线相减的两个坑）

不加 duration 字段是本次的明确决定：临界区内只做一次取时和一次 store，等待时长离线算。

代价是 `M2 - M` 要靠离线相减两个墙钟字符串。两个坑必须在分析脚本里处理，否则会**系统性地**
污染这个量（而它恰好是整套十点里数值最小的一个）：

**坑 1：解析器精度。** 两个字段都是 RFC3339Nano，带**纳秒**。但 Python 的 `datetime` 只有
**微秒**精度 —— `datetime.fromisoformat` / `strptime` 会截断纳秒，两个 stamp 各截一次，
差值就带 ±1~2 µs 误差。必须用下面任一方式：

```python
# 方式 A：pandas / numpy，底层就是 int64 纳秒
import pandas as pd
t = pd.to_datetime(s, format='ISO8601')       # Timestamp，ns 精度
wait_ns = (t_acq - t_start).value              # int，纳秒

# 方式 B：手工整数解析（无依赖，本仓库其它脚本可直接复用）
import re, calendar
_RE = re.compile(r'^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$')
def pns(s):
    m = _RE.match(s)
    if not m: return None
    y, mo, d, H, M, S = (int(m.group(i)) for i in range(1, 7))
    frac = ((m.group(7) or '') + '0' * 9)[:9]     # RFC3339Nano 去掉尾随 0，必须右补
    return calendar.timegm((y, mo, d, H, M, S, 0, 0, 0)) * 1_000_000_000 + int(frac)
```

`appendKVTime` 用的是 `time.RFC3339Nano`，它**去掉小数部分的尾随零**（`.123456000Z` 输出成
`.123456Z`），所以手工解析必须**右补零到 9 位**，不能左补、也不能假定固定 9 位。

**坑 2：墙钟跳变。** 两个 stamp 都是墙钟（`ReqHeaderMuStart()` 的 doc comment 已声明返回值
不带 monotonic reading）。速率修正（slew，上限约 500 ppm）可忽略；但 `makestep` 触发
`settimeofday` 会直接毁掉差值。采集前后各跑一次 `chronyc tracking` 确认，或在实验窗口内禁用
makestep。

免费自校验：同一条 client 行上 `M ≤ M2 ≤ T2` 必须成立，违反即为墙钟 step（或代码被改坏），
该样本整条丢弃并计数。

---

## 3. 现有基础设施盘点（说明为什么改动这么小）

| 需要的东西 | 现状 | 本次要做的 |
|---|---|---|
| 本地 fork HTTP/2 库 | `xnet/` 已是 `golang.org/x/net v0.47.0` 的 fork，`xnet/FORK_PROVENANCE.md` 记录 dirhash | 无 |
| 七个 NF 指向 fork | 七个 `go.mod` 均已有 `replace golang.org/x/net => ../../xnet`（amf:29 / ausf:22 / nrf:23 / nssf:22 / pcf:24 / udm:22 / udr:24） | 无 |
| request-scoped、并发安全的载体 | `ClientRequestTrace`（`instrument_client.go:61-71`）已是纯 atomic 字段容器 | 加 1 个 atomic 字段 + 1 个 getter |
| trace 送达 `writeRequest` | `cs.instr` 已在 `transport.go:1298` 从 ctx 取好并挂在 `clientStream` 上 | 无（**不需要新的 context 查找、不需要新的 allocation**） |
| 客户端读取快照并落盘 | `httptransport.go:290-303` 已经在 `base.RoundTrip` 返回后集中做 atomic load 再 `LogHTTP` | 加 1 行 load + 传 1 个实参 |
| JSON 构造 | `accesslog.go::LogHTTP` 已有 `appendKVTime` | 加 1 行 append + 上调容量 |
| 出站流量是否全部经过这里 | 已核实：七个 NF 的每个 consumer service 都调用 `configuration.SetHTTPClient(accesslog.Client())` | 无 |

**不需要**：新的日志文件、新的日志行、新的 writer goroutine、新的 channel、新的锁、
新的 context node、修改业务 handler、修改 OpenAPI 代码、修改 `go.mod`。

---

## 4. 具体修改位置

### 4.1 `xnet/http2/instrument_client.go`

**(a)** 在 `ClientRequestTrace` 结构体（**61-71 行**）中新增一个字段，按 8 字节对齐排在
`mUnixNano` 之后：

```go
type ClientRequestTrace struct {
	parent context.Context

	attempts  atomic.Uint32
	mUnixNano atomic.Int64
	// TYcustom M2: the instant this attempt SUCCEEDED in acquiring
	// cc.reqHeaderMu. The lock wait itself (M2 - M) is deliberately NOT computed
	// here -- offline analysis subtracts the two serialised stamps, which keeps
	// the critical section down to one clock read and one store.
	mAcqUnixNano atomic.Int64
	streamID     atomic.Uint32
	connID       atomic.Pointer[ClientConnIdentity]
}
```

结构体从 48 B 变 56 B（size class 48 → 64），**仍然落在单条 cache line 内**，新 store 命中的
是 `mUnixNano` / `streamID` 已经独占的同一条线，不引入 false sharing。

**(b)** 在 `ReqHeaderMuStart()`（**119-125 行**）之后新增一个 getter，形式照抄现有的：

```go
// ReqHeaderMuAcquired returns M2: the instant the most recent attempt actually
// took cc.reqHeaderMu. The zero Time means it was never taken -- either the
// request failed before reaching the lock at all (then ReqHeaderMuStart is also
// zero), or it was cancelled WHILE queued for the lock (then ReqHeaderMuStart is
// set and this is not). That second combination is a distinct outcome and must
// be counted separately offline, not folded into "incomplete".
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
```

### 4.2 `xnet/http2/transport.go`（改 **1450-1456 行**；1443-1449 的 M 块**完全不动**）

```go
	// TYcustom M (req_header_mu_start_time): the instant this attempt begins
	// contending for reqHeaderMu. It must be taken before the select, not after
	// the send succeeds -- the wait itself is what we are measuring. One clock
	// read and one atomic store; nothing else is allowed here.
	if cs.instr != nil {
		cs.instr.mUnixNano.Store(time.Now().UnixNano())
	}
	select {
	case cc.reqHeaderMu <- struct{}{}:
		// TYcustom M2 (req_header_mu_acq_time): the lock is now held.
		//
		// Deliberately inside this branch and not after the select: the other two
		// branches return, so "M set but M2 unset" is exactly "cancelled while
		// queued for the lock", which offline analysis counts as its own outcome.
		//
		// THIS RUNS INSIDE THE reqHeaderMu CRITICAL SECTION (held from here to the
		// release after encodeAndWriteHeaders below), which at connsPerPeer = 1
		// serialises every request this NF sends to this peer. Exactly one clock
		// read and one atomic store are permitted here -- nothing else, ever. Do
		// not add a counter, a duration computation, a log call or a second clock
		// read to this branch.
		if cs.instr != nil {
			cs.instr.mAcqUnixNano.Store(time.Now().UnixNano())
		}
	case <-cs.reqCancel:
		return errRequestCanceled
	case <-ctx.Done():
		return ctx.Err()
	}
```

要点：

- 与 M 逐字对称：**一次 `time.Now()` + 一次 atomic store**，没有局部变量、没有减法。
- 与 M 一致，**无 `IsZero()` 保护**：retry 时覆盖，描述的是最后一次 attempt（见 §6.4）。
- `time` 已在 `transport.go` 中 import，`cs.instr` 已在作用域内，**无需新增 import 或查找**。
- 注释里"只允许一次 clock read 和一次 store"是**规范，不是描述** —— 这是本次改动唯一落在
  串行区里的语句，后续任何想往这个分支里加东西的改动都必须先重新评估。

### 4.3 `NFs/<nf>/internal/accesslog/httptransport.go`（七份逐字节相同；改 **290-303 行**）

```go
	mStart := instr.ReqHeaderMuStart()
	// TYcustom M2: an atomic load, exactly like the three below -- this never
	// blocks and never waits for writeRequest to finish.
	mAcq := instr.ReqHeaderMuAcquired()
	streamID := instr.StreamID()
	retryCount := 0
	if n := instr.Attempts(); n > 0 {
		retryCount = int(n) - 1
	}

	LogHTTP(dst, method, uri, ueID, connID, connSlot, connReused,
		streamID, retryCount,
		reqTime, mStart, mAcq, wroteTime, gotFirstByte, respTime)
```

T1（**278**）、T6（**280**）、`GotConn`（**244-249**）/ `WroteRequest`（**250-254**）/
`GotFirstResponseByte`（**255-257**）三个闭包、`WithContext` 的调用次数（仍然只有一次，
**276**）**全部不动**。

### 4.4 `NFs/<nf>/internal/accesslog/accesslog.go`

**(a) 签名**（udm/udr/ausf/nrf/nssf/pcf: **522** 行；amf: **553** 行）—— 只多一个
`time.Time` 参数，插在 `headerMuStart` 之后：

```go
func LogHTTP(dstNF, method, uri, ueID, connID string, connSlot int, connReused bool,
	streamID uint32, retryCount int,
	reqTime, headerMuStart, headerMuAcq, wroteTime, gotFirstByte, respTime time.Time,
) {
```

**(b) 字段 append**，紧跟在现有 `req_header_mu_start_time` 那一行之后
（udm 等: **542** 行；amf: **573** 行）：

```go
	b = appendKVTime(b, "req_header_mu_start_time", headerMuStart, false)
	b = appendKVTime(b, "req_header_mu_acq_time", headerMuAcq, false)
```

零值渲染为 `""`，与 `wrote_time` / `got_first_byte` / `req_header_mu_start_time` 的既有约定
一致，无需特殊处理。

**(c) 容量上调（必做）** —— 现状（udm 等: **528** 行；amf: **559** 行）是 `getLine(640)`。

按 `C6525100g_NF1HTTP_500ms_3logs_0827v1`（九点版本，RQ2000/UE1000）真实日志实测：

| 记录 | 当前 capHint | p50 | p99 | **实测 max** |
|---|---|---|---|---|
| client（`LogHTTP`） | 640 | 533 | 572 | **573** |
| server（`LogHTTPInbound`） | 512 | 412 | 452 | **453** |
| W（`logWFlushed`） | 320 | — | — | **245** |

最长的 client 行是 UDM→UDR 的
`.../provisioned-data/smf-selection-subscription-data?supported-features=`（URI 152 字符）。

新字段增量：

```text
,"req_header_mu_acq_time":"2026-08-28T14:12:01.235055225Z"
 = 1(逗号) + 24(键名 22 字符 + 2 引号) + 1(冒号) + 32(时间戳 30 字符 + 2 引号) = 58 B

新的最长行 = 573 + 58 = 631 B
```

631 < 640 装得下，但只剩 9 B 余量，而这两个字段还会变长：`stream_id` 在最长行上是 1161
（4 位），同一轮实测最大 **17999**（5 位）→ +1 B；`latency_us` 在最长行上是 12284（5 位），
实测最大 **190086**（6 位）→ +1 B。最坏共现 **633 B**，余量 7 B。**必须上调。**

按 Go size class（这一段是 512 / 576 / 640 / **704** / 768 / 896 / 1024）对齐：

```go
const httpLineCap = 704   // 实测 max 573 + 58 = 631；见 HTTP_10thlog_0903.md 4.4(c)
...
	bp := getLine(httpLineCap)
```

**不要为了保险给 2048。** `getLine`（AMF **70-86** 行）在稳态命中池时只做
`cap(*bp) >= capHint` 比较 + 一次 reslice，**根本不看 capHint 的数值**，所以调大对稳态每行
成本增量为 0；但 `linePool` 被所有记录类型共用，且队列中每条排队记录都持有自己的 buffer，
内存上界是 `queueCapacity(2^21) × capHint`。704 给 633 留 71 B 余量，够且不浪费。

**(d) 加一个精确的越界计数器（建议）。** 不要靠事后 `awk` 抽样验证容量，加一个只在真正
realloc 时才计数的探针，整轮实验必须为 0：

```go
// lineRealloc counts lines whose buffer had to grow mid-build, i.e. httpLineCap
// was too small. Every such line paid a growslice plus a memcpy of everything
// written so far. This must be 0 for a run to be considered clean.
var lineRealloc atomic.Uint64

// LineReallocs reports that count. Read it next to Dropped().
func LineReallocs() uint64 { return lineRealloc.Load() }
```

在 `LogHTTP` 里：

```go
	bp := getLine(httpLineCap)
	b := *bp
	c0 := cap(b)          // 池可能给回更大的 buffer，所以记实际起始 cap
	... 所有 append ...
	if cap(b) > c0 {
		lineRealloc.Add(1)
	}
	*bp = b
	enqueue(kindHTTP, bp)
```

**(e) doc comment**：按现有风格补上新参数说明（doc 起始行 udm 等 **476**；amf **507**），
保留"字段原名原序不变、新字段按时间序插入"的既有约定。

**(f) `LogHTTPInbound` 的 `getLine(512)`（udm 等 586；amf 617）不需要动**（实测 max 453）。

### 4.5 七个 NF 的落地清单

| NF | `accesslog.go`（签名 / 容量 / 字段 / doc） | `httptransport.go` | 备注 |
|---|---|---|---|
| amf | 553 / 559 / 573 / 507 | ✅ | 另有 NGAP + worker 日志，**不受影响** |
| ausf | 522 / 528 / 542 / 476 | ✅ | |
| nrf | 522 / 528 / 542 / 476 | ✅ | 另有 `internal/dbtrace/`，不受影响 |
| nssf | 522 / 528 / 542 / 476 | ✅ | |
| pcf | 522 / 528 / 542 / 476 | ✅ | |
| udm | 522 / 528 / 542 / 476 | ✅ | |
| udr | 522 / 528 / 542 / 476 | ✅ | |

`httptransport.go` 七份逐字节相同 → **改一份，复制七份**，改完立刻用 `md5sum` 复核。
`accesslog.go` 七份不同，必须逐个改同名同序的那几行。

`bsf`、`chf`、`n3iwf`、`nef`、`smf`、`tngf`、`upf` 没有 `internal/accesslog/`，也没有
`replace golang.org/x/net => ../../xnet`，**不在本次范围内**。

### 4.6 编译与竞态自查

```bash
cd Free5gc-TYcustom
for n in amf ausf nrf nssf pcf udm udr; do
  (cd NFs/$n && go build ./...) || echo "BUILD FAIL: $n"
done
for n in amf ausf nrf nssf pcf udm udr; do
  md5sum NFs/$n/internal/accesslog/httptransport.go
done | awk '{print $1}' | sort -u | wc -l    # 必须是 1

(cd xnet && go vet ./http2/)
(cd xnet && go test -race -run 'TestTransport|TestClientConn' ./http2/)
```

`-race` 是硬要求：`instrument_client.go:49-56` 的并发说明明确要求这些字段必须 atomic 访问
（`writeRequest` 在 `go cs.doRequest(...)` 的 goroutine 上写，而 `roundTrip` 可能从
`<-ctx.Done()` / `<-cs.reqCancel` 提前返回、由 wrapper 并发读）。写成普通共享字段会挂。

---

## 5. 这个 log 记录在哪个文件的什么位置

### 5.1 文件

**`HTTP_log.txt`**（路径由 `HTTP_LOG_PATH` 覆盖，默认 `/tmp/HTTP_log.txt`，见 `accesslog.go`
的 `envHTTPPath` / `defaultHTTPPath`，AMF **98** / **103** 行）。

**不新增文件，也不新增日志行。** 新字段挂在**已经存在的 client JSON 行**上，即 `LogHTTP`
产生的那一行。

| 记录 | 产生函数 | 落到哪个文件 | 本次是否改动 |
|---|---|---|---|
| client 行（T1/M/**M2**/T2/T5/T6） | `LogHTTP` | `HTTP_log.txt` | **加 1 个字段** |
| server 行（G/T3/T4） | `LogHTTPInbound` | `HTTP_log.txt`（同一文件） | 不动 |
| W event | `logWFlushed` | `HTTP_log.txt`（同一文件，独立行） | 不动 |
| DB 行 | `LogDB` | `DB_log.txt` | 不动 |
| NGAP 行（仅 AMF） | `LogNGAP` | `AMF_log.txt` | 不动 |
| worker 行（仅 AMF） | `LogWorker` | `AMF_worker_log.txt` | 不动 |

### 5.2 行内位置

严格插在 `req_header_mu_start_time` 之后、`wrote_time` 之前 —— **字段顺序与时间顺序一致**，
沿用 0826 计划 §6 的"原名原序不变、新字段按时间序插入"约定，现有分析脚本不受影响。

改动前（取自 RQ2000 真实日志的最长行，573 B）：

```json
{"src":"UDM","dst":"UDR","method":"GET","uri":"http://free5gc-udr-sbi:8000/nudr-dr/v2/subscription-data/imsi-999700000000052/99970/provisioned-data/smf-selection-subscription-data?supported-features=","ue_id":"","conn":"192.168.88.242:50340","conn_slot":0,"conn_reused":true,"stream_id":1161,"retry_count":0,"req_time":"2026-08-28T14:12:01.294019914Z","req_header_mu_start_time":"2026-08-28T14:12:01.294034872Z","wrote_time":"2026-08-28T14:12:01.297683741Z","got_first_byte":"2026-08-28T14:12:01.306251573Z","resp_time":"2026-08-28T14:12:01.306304883Z","latency_us":12284}
```

改动后（**只新增一个字段，其余全部保持原名原序**，631 B）：

```json
{"src":"UDM","dst":"UDR","method":"GET","uri":"http://free5gc-udr-sbi:8000/nudr-dr/v2/subscription-data/imsi-999700000000052/99970/provisioned-data/smf-selection-subscription-data?supported-features=","ue_id":"","conn":"192.168.88.242:50340","conn_slot":0,"conn_reused":true,"stream_id":1161,"retry_count":0,"req_time":"2026-08-28T14:12:01.294019914Z","req_header_mu_start_time":"2026-08-28T14:12:01.294034872Z","req_header_mu_acq_time":"2026-08-28T14:12:01.294051230Z","wrote_time":"2026-08-28T14:12:01.297683741Z","got_first_byte":"2026-08-28T14:12:01.306251573Z","resp_time":"2026-08-28T14:12:01.306304883Z","latency_us":12284}
```

`latency_us` 仍然是 `respTime - reqTime`（`T6 - T1`，用 monotonic 差），**语义与行内位置都
不变**。

### 5.3 对 server 行和 W event 的影响

**零影响。** M2 是纯客户端发送路径上的点，`LogHTTPInbound`（server 行）与 `logWFlushed`
（W event）都不涉及，两者的 `getLine` 容量也不需要动。

---

## 6. 与原先九个点的匹配关系

### 6.1 M2 是行内字段，不需要任何 join

M2 与 T1、M、T2、T5、T6 **写在同一条 JSON 行里**，共享同一个 `LogHTTP` 调用、同一个
`ClientRequestTrace`、同一个 `clientStream`、同一次 transport attempt。同一行就是同一个
request。

### 6.2 join key 完全不变

0826 计划 §17.3 的两步 exact join 一个字符都不用改：

```text
第一步  server JSON  <-> W event        key = (dst, server_request_id)
第二步  client JSON  <-> 已合并的记录   key = (dst, conn, stream_id)
```

`req_header_mu_acq_time` 是**非 key 的度量字段**，随 client 行一起被带进 join 结果。
duplicate-key 检查、`retry_count == 0` 过滤、`stream_id == 0` 排除、本地端口复用的 duplicate
统计等既有规则原样适用。

**同 attempt 保证**：§4.2 的 store 与 `streamID` / `connID` 的 store（`transport.go:1479-1482`）
位于同一次 attempt 的同一段直线代码上，中间没有 `return` 之外的分叉。所以
`(M, M2, stream_id, conn)` 天然属于同一个 attempt。

### 6.3 M2 不会减少任何一个完整样本（结构性保证）

一条可以直接写进离线脚本的断言：

```text
stream_id != 0  ⟹  addStreamLocked(1468) 执行过
                ⟹  1451 的 select 成功分支走过
                ⟹  M2 一定有值
```

所以 **"十点齐全"的样本集 == "九点齐全"的样本集**，一条不少。等锁期间被取消的请求根本没写
出去（`wroteTime` 为零、`stream_id` 为 0），在九点规则下**本来就已经被排除**。

反向不成立：拿到锁但随后 `awaitOpenSlotForStreamLocked` 失败（1463-1467）的样本 `stream_id`
仍是 0，却已经有 M2 值。这是保守偏差，对主分析样本集无影响。

退化情形分桶表（必须按这张表统计，不能一律算 incomplete）：

| M | M2 | 含义 | 离线处理 |
|---|---|---|---|
| 有 | 有 | 正常，十点完整 | **主分析样本**（还需 `retry_count == 0`、`stream_id != 0` 且九点齐全） |
| 有 | 空 | **在排队等 `reqHeaderMu` 时被 ctx 超时 / reqCancel 取消** | 单独 bucket 统计；本次新增的诊断能力 |
| 空 | 空 | 根本没走到锁（`GetClientConn` 失败 / dial 失败） | 与现有 `stream_id == 0` 的失败样本同桶 |
| 空 | 有 | **不可能**；若出现说明代码被改坏 | 断言失败，整轮数据作废 |

### 6.4 retry 语义与现有约定一致

`mAcqUnixNano` 与 M、`stream_id`、`conn` 一样**无 `IsZero()` 保护**，retry 时被覆盖，描述的是
**最后一次 attempt**；而 `wrote_time` 有 `IsZero()` 保护（`httptransport.go:250-254`），保留的
是**第一次**写完成的时间。两者在 `retry_count > 0` 时指向不同 attempt —— 这仍然是
"主分析只接受 `retry_count == 0`"的理由。本次改动**没有引入新的 retry 问题**。

### 6.5 十点时间线与离线计算

```text
T1 → M → M2 → T2 → G → T3 → T4 → W → T5 → T6
```

离线计算（按 §2.4 的解析要求）：

```text
req_header_mu_wait_us     = (M2 - M) / 1000      ← 纯锁等待，本次的目标量
in_lock_send_us           = (T2 - M2) / 1000
before_headermu_start_us  = (M  - T1) / 1000     ← 0826 计划已有
```

自校验恒等式：

```text
T1 <= M <= M2 <= T2                              必须 100% 严格成立
(M - T1) + (M2 - M) + (T2 - M2) == T2 - T1       整数纳秒，应精确相等
```

第二条在整数纳秒下是恒等式，**任何不相等都说明解析器把纳秒截断成了微秒**（§2.4 坑 1）。
这是一个免费而且强力的解析器自检。

---

## 7. 热路径约束（实现时必须守住的规则）

这一节是**代码约束**，不是性能分析。

### 7.1 临界区内只准两条指令

M2 是本次改动唯一落在串行区里的语句：它在 `reqHeaderMu` 临界区内（1451 拿锁 → 1502 放锁），
而 `connsPerPeer = 1` 时该 NF 对的全部请求都串行通过这里。

**允许的全部工作：1 次指针判空 + 1 次 `time.Now()` + 1 次 `atomic.Int64.Store`。**

禁止在这个分支里：算 duration、加计数器、读第二次时钟、调日志、取 context、分配内存、
碰任何锁。§4.2 的注释已把这条写进代码。

参照量：临界区内**已有**两个 atomic store（`streamID` / `connID`，1479-1481），本次是从 2 个
变 3 个并加一次取时；这与九点里 M 那一个点的成本逐指令相同（M 也是一次取时 + 一次 store，
只是它在锁外）。

### 7.2 日志写入仍然完全异步、非阻塞

链路（全部是**现有**机制，本次一个都不改）：

```text
[被测 goroutine：writeRequest，1451 分支内，临界区内]
    time.Now() + 1 × atomic.Store
        ↓ （无阻塞、无交接）
[被测 goroutine：RoundTrip wrapper，已在 T6 之后]
    1 × atomic.Load，随 LogHTTP 一起 append 进行内 buffer
        ↓ enqueue(kindHTTP, bp)  —— 非阻塞 select + default
[queue chan record，容量 1<<21]
        ↓
[writerLoop：单一 writer goroutine]
    bufio（1 MiB）→ 200 ms flush ticker → HTTP_log.txt
```

- `enqueue`（AMF `accesslog.go:377-384`）是 `select { case queue <- …: default: dropped.Add(1) }`
  —— **队列满时原子计数并丢弃，绝不阻塞被测 goroutine**；
- 只有一个 `writerLoop`（AMF `accesslog.go:262` 起）持有文件，**行永不交错**；
- flush 由 200 ms ticker 驱动 + 收尾时 `Flush()` 同步排空，**不按 request flush**；
- 输出 JSON Lines，写入顺序无关。

因为**没有新增日志行**：队列压力不变、writer 处理的行数不变、drop 概率不变、GC 需要扫描的
channel buffer 不变。唯一增量是每行 +58 B，全部落在 `writerLoop` 上。

### 7.3 不得改变的东西

- 不引入新锁，不改 `reqHeaderMu → cc.mu → wmu` 的锁顺序；
- 不改 select 的分支集合与语义（只在成功分支内加语句，取消/超时分支原样 `return`）；
- 不改 HPACK 内容、帧长度、帧顺序、write scheduler、flow control、buffer 大小、
  frame batching、`MaxConcurrentStreams`、`IdleTimeout`、handler 生命周期；
- **不加 HTTP header**（对比 0826 §17.3 的 `sbi_request_id` 备选方案 —— 那个会改变 HPACK
  编码和帧长度。本次不需要）；
- `cs.instr == nil` 时新增代码是一次 nil 比较，与 fork 现有 M/G/W 钩子行为一致。

### 7.4 clocksource 必须是 `tsc`

新增的 `time.Now()` 在 vDSO + TSC 下是几十纳秒；退化到 `acpi_pm` / `hpet` 会变成微秒级，
而这条语句在临界区内。上机前每台机器确认：

```bash
cat /sys/devices/system/clocksource/clocksource0/current_clocksource   # 必须是 tsc
```

（`LOCK_SCHED_PROFILING_GUIDE_0826.md` §2 已把这条列为前置检查。）

---

## 8. 改完之后的验收清单（代码层）

1. **clocksource**：每台机器 `current_clocksource == tsc`（§7.4）。
2. **七份一致性**：`md5sum NFs/*/internal/accesslog/httptransport.go` 七份仍然相同。
3. **编译 + vet + race**：§4.6 的命令全过。
4. **离线解析器自检**：整数纳秒下 `(M-T1) + (M2-M) + (T2-M2) == T2-T1` 必须**精确相等**。
   不等就是解析器截断了纳秒（§2.4 坑 1），先修脚本。
5. **行内序关系**：每条 `retry_count == 0`、`stream_id != 0` 的 client 行满足
   `T1 <= M <= M2 <= T2`，100% 成立。违反即墙钟 step 或代码改坏。
6. **墙钟无 step**：采集前后各跑 `chronyc tracking`。
7. **容量**：`LineReallocs()`（§4.4d）整轮必须为 **0**。旁证：
   `awk '{print length($0)}' HTTP_log.txt | sort -n | tail -1` 应 < 704。
8. **分桶统计**：按 §6.3 的四行表统计。第四行（M 空 M2 有）必须为 **0**；
   第二行（等锁时被取消）占比若不可忽略，说明 10 s timeout 已在触发，需单独报告。
9. **join 不退化**：`(dst, conn, stream_id)` duplicate == 0；完整十点样本数与改动前的完整
   九点样本数**相等**（§6.3 的结构性保证要求如此，不相等说明别处出了问题）。
10. **drop 计数**：`accesslog.Dropped()` / `WDropped()` / `WAccountingErrors()` 与改动前同
    量级。本次不增加日志行，drop 率不应上升。

---

## 9. 本次改动的边界（不做什么）

- **只增加 `reqHeaderMu` 一把锁的等待时间。** `M2 → T2` 仍然是混合区间，包含 `cc.mu`、
  stream 配额、HPACK、`cc.wmu`、写帧、flush 和 goroutine 调度。想继续拆开，需要在同一个
  `writeRequest` 里再加三个点（实现模式与本文完全相同，但**三个都在临界区内**，
  会把临界区内的取时从 1 次变 4 次，加之前必须重新评估 §7.1 的约束）：

  | 候选点 | 位置 | 切出的区间 |
  |---|---|---|
  | `cc.mu` 取得 | `transport.go:1458` `cc.mu.Lock()` 之后 | `cc.mu` 排队 |
  | stream 配额取得 | `transport.go:1467` 之后（`awaitOpenSlotForStreamLocked` 返回后） | `cc.cond.Wait()` 等配额 |
  | `cc.wmu` 取得 | `transport.go:1579-1580`（`cs` 在作用域内，`cs.instr` 直接可用） | `wmu` 排队 vs HPACK+写 |

  这三个点**不在本次范围内**。

- 不增加任何服务端时间点；G / T3 / T4 / W 全部不动。
- 不增加 client response read-loop 的时间点。
- 不在进程内计算锁等待时长，不新增 duration 字段（§2.4）。
- 不改 `latency_us` 的定义（仍是 `T6 - T1`）。
- 不改任何 join key，不引入 `run_id` / `sbi_request_id` / `conn_epoch`。
- 不承诺失败样本有完整十点；错误/缺失状态必须按 §6.3 保留并统计，**不得伪造时间点**。
- 本文只描述改动方案，**不在本文中实施源代码修改**。
