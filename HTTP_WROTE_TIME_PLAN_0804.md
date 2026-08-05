# HTTP_log 新增 `wrote_time` / `got_first_byte` 打点计划 (0804)

## 1. 目的

当前 `HTTP_log.txt` 的客户端记录只有两个时间戳：

```
req_time ──────────── 黑盒 ────────────→ resp_time
```

而"请求腿"（`server.req_time - client.req_time`）在 RQ2000 下达到 9.70ms（UDM→UDR），
这段时间里混杂了至少两类互不相关的等待，现有日志**无法区分**：

| 候选原因 | 发生位置 |
|---|---|
| ① 发送端抢 `clientConn` 写锁排队 | 发送端用户态 |
| ② 接收端 goroutine 等 Go 调度器 | 接收端进程内 |

本次改动新增**两个**时间戳，把请求腿与响应腿各切成两段：

- `wrote_time` —— 切**请求腿**，判定主因在发送端还是接收端（1.1）
- `got_first_byte` —— 切**响应腿**，分离客户端 goroutine 调度延迟（1.2 / 1.3）

### 1.1 请求腿的切分语义

```
用户态                                    内核
  ├─ req_time  ← 已有
  ├─ 抢 clientConn 写锁排队
  ├─ HPACK 编码 header
  ├─ 写 HEADERS + 全部 DATA frame ──→ socket 发送缓冲区
  └─ wrote_time ← 新增（所有 frame 已进内核 buffer）
                                          ↓ 内核择机发出 TCP
                                          ↓ 对端读循环 / HPACK 解码 / spawn / 等调度
                                     server.req_time（已有）
```

| 区间 | 含义 | 对应候选 |
|---|---|---|
| `wrote_time - req_time` | 抢写锁 + HPACK 编码 + 写入内核缓冲 | **①** |
| `server.req_time - wrote_time` | 内核发送 + 网络(单机≈0) + 对端全部前置处理 | **②** |

> **注意语义边界**：`wrote_time` 代表"字节已拷贝进内核 socket buffer"，
> **不是**"TCP 已真正发出网线"。用户态无法获知后者（需 `SO_TIMESTAMPING`）。
> 这正是我们想要的分界线：它之前纯用户态，之后是内核+对端。

### 1.2 响应腿同样在恶化，需要一并处理

实测响应腿的涨幅与请求腿同量级，**不能只做请求侧**：

| 链路 | RQ200 响应腿 | RQ2000 响应腿 | 涨幅 |
|---|---|---|---|
| udr→udm | 0.119 ms | **3.742 ms** | 31× |
| udm→amf | 0.117 ms | 3.477 ms | 30× |
| ausf→amf | 0.141 ms | 1.570 ms | 11× |

`wrote_time` 只加在客户端 `RoundTrip` 的**请求**写出路径上，
对响应腿没有任何解释力。因此本计划**同时**加入响应侧打点（见 1.3）。

### 1.3 响应全过程与打点位置

```
【服务端 = 响应发送方，例：UDR】
  ...业务 handler 执行完，响应已填入 ResponseWriter...
  ├─ gin c.Next() 返回
  └─ server.resp_time = time.Now()      ← ★ 已有（LogHTTPInbound）
  ┌──────────────── 黑盒 ────────────────┐
  │ ① h2 server 组装 response HEADERS    │
  │ ② HPACK 编码 response header         │  ← 全连接串行
  │ ③ ★ 抢 serverConn 写锁 ★             │  ← 与请求侧对称的串行点
  │ ④ 写 DATA frame → 内核 socket buffer │
  │ ⑤ 内核 TCP 发送                      │
  │ ⑥ 网络（单机 ≈ 0）                    │
  │ ⑦ 客户端读循环收帧                    │
  │ ⑧ HPACK 解码 response header         │  ← 全连接串行
  └──────────────────────────────────────┘
  └─ client.got_first_byte              ← ★ 新增（GotFirstResponseByte）
  ┌──────────────── 黑盒 ────────────────┐
  │ ⑨ 唤醒阻塞在 RoundTrip 的 goroutine  │
  │ ⑩ ★ 等 Go 调度器调度它 ★             │
  └──────────────────────────────────────┘
【客户端 = 响应接收方，例：UDM】
  └─ client.resp_time = time.Now()      ← ★ 已有（LogHTTP）
```

> **重要**：`c.Next()` 返回时响应**还没写出去**。gin 只是把数据填进
> `ResponseWriter`，真正的 HPACK 编码 + 抢写锁 + 写内核全在服务端时间戳**之后**。

**响应腿的候选延迟来源**：

| # | 来源 | 位置 | 与请求侧的对应关系 |
|---|---|---|---|
| A | **服务端抢 `serverConn` 写锁** | 服务端用户态 | 对称于请求侧 `clientConn` 写锁 |
| B | 服务端 HPACK 编码（全连接串行） | 服务端用户态 | 对称 |
| C | 内核 TCP 发送队列 | 服务端内核 | 单机下小 |
| D | 客户端读循环 + HPACK 解码 | 客户端用户态 | 全连接串行 |
| E | **等 Go 调度器唤醒 RoundTrip goroutine** | 客户端进程 | 对称于请求侧的接收端调度 |

**A 与 E 是主要嫌疑**，与请求侧同构。

**新增 `got_first_byte` 后响应腿可拆为**：

| 区间 | 含义 | 覆盖候选 |
|---|---|---|
| `got_first_byte - server.resp_time` | 服务端 HPACK 编码 + 抢写锁 + 内核 + 网络 + 客户端读循环 | A / B / C / D（仍混合） |
| `client.resp_time - got_first_byte` | **客户端 goroutine 被唤醒并调度的纯等待** | **E（可单独定位）** |

### 1.4 服务端"响应真正写出"为何不打点

理想情况下还想要一个与 `WroteRequest` 对等的服务端时间戳（响应 frame 进内核的时刻），
但 **`httptrace` 只有客户端版本，服务端没有对应的官方 hook**。

唯一的近似做法是包一层 `gin.ResponseWriter` 拦截 `Write()`，但语义不对：
`Write()` 返回只代表数据进了 `net/http` 的缓冲层，**真正的 HPACK 编码与抢写锁
发生在 handler 返回之后**，由 h2 server 完成，包装器测不到。

要拿到准确语义只能 fork `golang.org/x/net/http2`。**本计划不做**——
侵入性与维护成本远高于收益。因此响应腿的 A/B/C/D 暂时无法进一步细分，
但 E（调度延迟）可通过 `got_first_byte` 单独定位。

---

## 2. 硬性约束：完全异步写入，不得影响 UE 注册延迟

**本次改动必须与现有 HTTP_log 保持完全一致的异步写入模型** —— 热路径只记时间戳、
只做一次非阻塞入队，绝不在注册路径上做任何 I/O 或阻塞等待。

这一点在**高 req rate 下尤其关键**：RQ1500 / RQ2000 正是主要的分析场景，
任何额外引入的 latency 都会直接污染被测对象，使实验失去意义。

### 2.1 沿用现有的异步写入模型（不做任何改动）

现有 `accesslog` 已经是正确的异步模型，本次**完全沿用，一行不改**：

| 机制 | 现有实现 | 本次是否改动 |
|---|---|---|
| 热路径只做格式化 + 入队 | `LogHTTP` 拼 JSON 后调 `enqueue` | ❌ 不改 |
| **非阻塞入队，满则丢弃** | `enqueue` 的 `select { case queue <- rec: default: dropped.Add(1) }`（第 197-203 行） | ❌ 不改 |
| 单 writer goroutine 落盘 | `writerLoop` 独占文件，批量刷 | ❌ 不改 |
| 队列容量 | `1 << 21` = 2,097,152 条 | ❌ 不改 |

> **关键**：`enqueue` 用 `select/default`，队列满时**丢弃并计数**而非阻塞。
> 这保证了无论日志量多大，数据面永远不会被日志回压拖慢。本次改动不触碰这个语义。

### 2.2 新增打点必须遵守的规则

| 规则 | 本方案如何满足 |
|---|---|
| **回调内不做 I/O、不写日志** | 两个回调各只执行一次 `time.Now()`（`WroteRequest` 另加一次 `IsZero()` 判断），**不入队、不落盘**。日志仍在 `RoundTrip` 返回后走原有异步路径 |
| **回调内不阻塞** | 无锁、无 channel、无系统调用（`time.Now()` 走 vDSO） |
| **不引入跨 goroutine 共享状态** | `wroteTime` / `gotFirstByte` 均为 `RoundTrip` 的**栈上局部变量**，闭包捕获，天然 goroutine-local，无需任何同步原语 |
| **不改变请求内容** | `httptrace.WithClientTrace` 只挂回调，不接触 header / body / ContentLength |
| **不改变传输行为** | 纯观测，不影响连接复用、重试、流控 |

> **为什么回调里绝不能写日志**：`WroteRequest` 运行在 HTTP/2 的**写路径**上，
> 此时可能仍持有连接的写锁；`GotFirstResponseByte` 运行在**读循环**上，
> 该读循环是整条连接所有 stream 的唯一收帧者。
> 在任一处做 I/O 或阻塞，都会直接堵住这条连接上的全部 stream，
> 把观测手段本身变成新的瓶颈。因此两个回调都只赋值给栈上局部变量。

### 2.3 开销量化

每个出向请求新增的工作：

| 操作 | 量级 |
|---|---|
| `time.Now()` × 2（`WroteRequest` + `GotFirstResponseByte`） | ~40-100 ns（vDSO，无系统调用） |
| `httptrace.WithClientTrace` 构造 context | 1 次小对象分配，~50 ns |
| `ClientTrace` 结构体 + 闭包 | 2 次小对象分配，~50 ns |
| 日志行多约 40-80 字节 | 追加到已有 buffer，无额外分配 |
| **合计** | **约 150-250 ns / 请求** |

对照实测：**最快**的链路（RQ200 的 udm→udr 请求腿）是 0.172 ms = 172,000 ns
→ 新增开销约占 **0.1%**。

**高负载下的绝对量**（这是最需要保证的场景）：
RQ2000 时全系统约 40,000 次 SBI 请求 / 0.5 s ≈ 80,000 req/s，
新增 CPU 开销约 `80,000 × 250 ns = 20 ms/s`，即**约 2% 的单核占用**。
实测 NF 进程总 CPU 峰值仅约 50%（48 核），余量充足。

由于是异步写入，这 250 ns 中真正落在注册路径上的只有 `time.Now()` 与入队，
**落盘 I/O 完全不在热路径上**。

### 2.4 明确不做的事

- ❌ **不在 httptrace 回调里写日志或入队** —— 见 2.2 的说明
- ❌ **不新增 mutex / atomic / map** —— 栈上局部变量足够
- ❌ **不改 `enqueue` 的丢弃语义** —— 队列满时继续丢弃并计数，绝不阻塞数据面
- ❌ **不改 `writerLoop` / 队列容量 / flush 策略** —— 异步模型原样保留
- ❌ **不挂多余的 httptrace 回调** —— 只挂本方案需要的两个（`WroteRequest` + `GotFirstResponseByte`），
  不挂 `GotConn` / `WroteHeaders` / `DNSStart` 等无关回调

---

## 3. 改动范围

### 涉及的代码归属（重要）

**本次改动全部位于 free5gc 仓库内的自有插桩代码，不涉及任何公共库。**

| 代码 | 是否修改 | 说明 |
|---|---|---|
| `NFs/<nf>/internal/accesslog/httptransport.go` | ✅ 改 | 本仓库自有插桩代码 |
| `NFs/<nf>/internal/accesslog/accesslog.go` | ✅ 改 | 本仓库自有插桩代码 |
| `net/http/httptrace`（Go 标准库） | ❌ 不改 | 仅**调用**其公开 API |
| `golang.org/x/net/http2` | ❌ 不改 | 不 fork、不 replace |
| `github.com/free5gc/openapi` | ❌ 不改 | 不受影响 |

`httptrace` 是 Go 标准库为"在不改库代码的前提下观测 HTTP 客户端内部事件"而提供的
官方 hook 接口，因此本次无需 fork 或 `replace` 任何依赖，`go.mod` 不变。

> 对照：若要在"帧刚解析完"的瞬间打点（即区分 HPACK 解码排队与 goroutine 调度延迟），
> 才需要 fork `golang.org/x/net/http2`。**本计划不做这件事**，成本过高。

### 涉及的 NF（7 个）

`amf` / `ausf` / `nrf` / `nssf` / `pcf` / `udm` / `udr`

每个 NF 各有一份**独立副本**（非共享模块），路径：

```
NFs/<nf>/internal/accesslog/accesslog.go        ← 改 LogHTTP
NFs/<nf>/internal/accesslog/httptransport.go    ← 改 RoundTrip
```

### 已核实的现状

- **7 份 `httptransport.go` 完全字节相同**（md5 均为 `731851e048b6fb21636eba10149cbf74`）
  → 同一份修改可直接套用到 7 个文件
- **7 份 `accesslog.go` 的 `LogHTTP` 函数体完全相同**，仅所在行号不同：
  - `amf`：第 298 行（该文件另有 `LogNGAP` / `LogWorker`，共 444 行）
  - 其余 6 个：第 267 行（均为 340 行）
- `nef` 使用 `http.DefaultClient`，**不在本次范围内**（未接入 accesslog）

---

## 4. HTTP_log 格式：改动前后对比

### 4.1 客户端记录（发送方视角）

**改动前**（8 字段）：

```json
{"src":"UDM","dst":"UDR","method":"GET","uri":"http://free5gc-udr-sbi:8000/nudr-dr/v2/subscription-data/imsi-999700000000001/authentication-data/authentication-subscription","ue_id":"imsi-999700000000001","req_time":"2026-08-04T01:31:20.361328172Z","resp_time":"2026-08-04T01:31:20.368674597Z","latency_us":7346}
```

**改动后**（10 字段，新增 `wrote_time` 与 `got_first_byte`）：

```json
{"src":"UDM","dst":"UDR","method":"GET","uri":"http://free5gc-udr-sbi:8000/nudr-dr/v2/subscription-data/imsi-999700000000001/authentication-data/authentication-subscription","ue_id":"imsi-999700000000001","req_time":"2026-08-04T01:31:20.361328172Z","wrote_time":"2026-08-04T01:31:20.361402881Z","got_first_byte":"2026-08-04T01:31:20.368512034Z","resp_time":"2026-08-04T01:31:20.368674597Z","latency_us":7346}
```

### 4.2 逐字段说明

| 字段 | 改动前 | 改动后 | 说明 |
|---|:---:|:---:|---|
| `src` | ✔ | ✔ | 不变 |
| `dst` | ✔ | ✔ | 不变 |
| `method` | ✔ | ✔ | 不变 |
| `uri` | ✔ | ✔ | 不变 |
| `ue_id` | ✔ | ✔ | 不变 |
| `req_time` | ✔ | ✔ | 不变。交给 transport 的时刻 |
| **`wrote_time`** | ✖ | **🆕** | **新增。该请求所有 frame 已进内核 socket buffer 的时刻**（切请求腿） |
| **`got_first_byte`** | ✖ | **🆕** | **新增。响应首字节到达客户端读循环的时刻**（切响应腿） |
| `resp_time` | ✔ | ✔ | 不变 |
| `latency_us` | ✔ | ✔ | **语义不变**，仍为 `resp_time - req_time` |

**只增加两个字段，没有删除或修改任何已有字段。**

### 4.3 服务端记录（接收方视角）—— 完全不变

`LogHTTPInbound` 本次不修改，服务端行保持 8 字段、**不含两个新键**：

```json
{"src":"NaN","dst":"UDR","method":"GET","uri":"http://free5gc-udr-sbi:8000/nudr-dr/v2/...","ue_id":"imsi-999700000000001","req_time":"2026-08-04T01:31:20.361985004Z","resp_time":"2026-08-04T01:31:20.362731559Z","latency_us":746}
```

> 分析脚本用 `rec.get("wrote_time", "")` / `rec.get("got_first_byte", "")` 读取即可 ——
> 服务端行天然返回空字符串，无需按 `src == "NaN"` 特判。

### 4.4 新增信息能算出什么

**请求腿**拆成两段（此前无法区分）：

| 新指标 | 计算式 | 物理含义 |
|---|---|---|
| **① 发送端排队** | `wrote_time - req_time` | 抢 `clientConn` 写锁 + HPACK 编码 + 写入内核缓冲 |
| **② 内核+接收端** | `server.req_time - wrote_time` | 内核发送 + 网络(单机≈0) + 对端读循环/HPACK解码/spawn/**等调度**/中间件 |
| 原有请求腿 | `server.req_time - client.req_time` | = ① + ②（**与改动前完全一致**） |

**响应腿**同样拆成两段：

| 新指标 | 计算式 | 物理含义 |
|---|---|---|
| **③ 服务端写出+传输** | `got_first_byte - server.resp_time` | 服务端 HPACK 编码 + **抢 `serverConn` 写锁** + 内核 + 网络 + 客户端读循环 |
| **④ 客户端调度等待** | `client.resp_time - got_first_byte` | **唤醒并调度阻塞在 `RoundTrip` 的 goroutine 的纯等待** |
| 原有响应腿 | `client.resp_time - server.resp_time` | = ③ + ④（**与改动前完全一致**） |

> ④ 是纯粹的 goroutine 调度延迟，可直接验证"接收端调度排队"这一候选；
> ③ 仍是混合量，但至少把 RQ2000 下 3.74 ms 的响应腿切开了。

### 4.5 特殊值

| 情况 | 取值 | 分析时的处理 |
|---|---|---|
| 正常请求 | 两个字段均为 RFC3339Nano 时间戳 | 正常使用 |
| 请求写出前失败（连接建立失败等） | `wrote_time` = `""` | **跳过该记录** |
| 响应未收到（超时 / 传输错误） | `got_first_byte` = `""` | 跳过响应腿统计（请求腿仍可用） |
| HTTP/2 重试 | `wrote_time` 取第一次写出时刻 | 正常使用（`IsZero()` 保证只记第一次） |
| 服务端记录 | 两个键均不存在 | `.get()` 返回默认值，天然跳过 |

### 4.6 体积影响

每条客户端记录增加约 80 字节（两个 RFC3339Nano 时间戳字段）。

以 RQ2000 为例：当前 `HTTP_log_RQ2000_UE1000.txt` 为 11.4 MB，
其中客户端记录约占一半 → 预计增至约 **13.0 MB（+14%）**。
现有队列容量 `1<<21`（2,097,152 条）与 1 MiB writer buffer 均有充足余量。

---

## 5. 具体修改

### 5.1 `httptransport.go` — RoundTrip 增加 httptrace

**位置**：`func (l *loggingRoundTripper) RoundTrip`（第 59-84 行）

**改动 A：新增 import**

```go
import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"   // ← 新增
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/net/http2"
)
```

**改动 B：RoundTrip 主体**

```go
func (l *loggingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	base := l.clear
	if req.URL != nil && req.URL.Scheme == "https" {
		base = l.tls
	}

	dst := dstNFFromURL(req)
	method := req.Method
	uri := ""
	if req.URL != nil {
		uri = req.URL.String()
	}

	ueID := sniffUEID(req)

	// wroteTime records the instant every frame of this request (HEADERS plus
	// all DATA) has been handed to the kernel socket buffer. Splitting the
	// request leg at this point separates sender-side queueing (waiting for the
	// shared clientConn write lock) from everything that happens afterwards in
	// the kernel and on the receiving NF.
	//
	// gotFirstByte records when the first byte of the response reached this
	// process's HTTP/2 read loop. It splits the response leg the same way: what
	// precedes it is the peer's write path plus the wire, what follows it is
	// purely this process waking and scheduling the goroutine blocked below in
	// RoundTrip.
	//
	// Both callbacks are request-scoped, not frame-scoped: WroteRequest fires
	// once after the last frame is written, GotFirstResponseByte once on the
	// first response byte. Each closure captures this call's own local variable,
	// so concurrent RoundTrips never interfere and no correlation id is needed.
	//
	// Neither callback may log or block: they run on the HTTP/2 read/write path,
	// where any I/O would stall every stream on the connection. They only stamp a
	// local variable; the record is enqueued after RoundTrip returns.
	//
	// The HTTP/2 transport may retry an idempotent request after a connection
	// error, firing WroteRequest again; keep the first write so the recorded
	// value always pairs with reqTime below.
	var wroteTime, gotFirstByte time.Time
	trace := &httptrace.ClientTrace{
		WroteRequest: func(httptrace.WroteRequestInfo) {
			if wroteTime.IsZero() {
				wroteTime = time.Now()
			}
		},
		GotFirstResponseByte: func() {
			gotFirstByte = time.Now()
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	reqTime := time.Now()
	resp, err := base.RoundTrip(req)
	respTime := time.Now()

	// Always log, even on transport error, so failed attempts are visible.
	LogHTTP(dst, method, uri, ueID, reqTime, wroteTime, gotFirstByte, respTime)
	return resp, err
}
```

**要点说明**

- 两个回调都是**请求级**（非 frame 级）：`WroteRequest` 在最后一个 frame 写完后触发一次，
  `GotFirstResponseByte` 在响应首字节到达时触发一次
  → 不存在"一个 req 多个 frame 如何对应"的问题
- `ClientTrace` 绑在**该请求自己的 context** 上，闭包捕获**栈上局部变量**
  → 并发安全，无需 map、无需锁、无需 request id
- 两个回调都必定在 `base.RoundTrip()` 返回**之前**触发
  → 执行到 `LogHTTP` 时两个变量已赋值，无竞态
- **回调内绝不写日志、不入队**（见 2.2）：只赋值给局部变量，
  日志仍由 `RoundTrip` 返回后的原有异步路径写出
- 请求写出前失败时 `wroteTime` 保持零值；响应未到达时 `gotFirstByte` 保持零值
  → 均由 `formatTimeOrEmpty` 处理（见 5.2）

### 5.2 `accesslog.go` — LogHTTP 增加参数与字段

**位置**：`func LogHTTP`（amf 第 298 行；其余 6 个第 267 行）

```go
// LogHTTP records one outgoing HTTP request/response from this NF's view.
//   - dstNF:     destination NF name (best-effort, derived from URL host)
//   - method:    HTTP method
//   - uri:       full request URI
//   - ueID:      UE id this request is for (may be ""); used for requests whose
//     URI does not carry the UE id but whose body does
//   - reqTime:      when the request was handed to the transport
//   - wroteTime:    when every frame of the request had reached the kernel
//     socket buffer. Zero if the request failed before it was written.
//   - gotFirstByte: when the first byte of the response reached this process's
//     read loop. Zero if no response ever arrived.
//   - respTime:     when the response (or error) was received
//
// A zero wroteTime/gotFirstByte is emitted as "" so the reader can skip it.
func LogHTTP(dstNF, method, uri, ueID string, reqTime, wroteTime, gotFirstByte, respTime time.Time) {
	b := make([]byte, 0, 256)
	b = append(b, '{')
	b = appendKV(b, "src", srcNF, true)
	b = appendKV(b, "dst", dstNF, false)
	b = appendKV(b, "method", method, false)
	b = appendKV(b, "uri", uri, false)
	b = appendKV(b, "ue_id", ueID, false)
	b = appendKV(b, "req_time", formatTime(reqTime), false)
	b = appendKV(b, "wrote_time", formatTimeOrEmpty(wroteTime), false)
	b = appendKV(b, "got_first_byte", formatTimeOrEmpty(gotFirstByte), false)
	b = appendKV(b, "resp_time", formatTime(respTime), false)
	b = appendDurUs(b, "latency_us", respTime.Sub(reqTime))
	b = append(b, '}')
	enqueue(kindHTTP, b)
}
```

**新增辅助函数**（放在现有 `formatTime` 之后，amf 第 286 行 / 其余第 255 行附近）：

```go
// formatTimeOrEmpty renders a timestamp like formatTime but yields "" for the
// zero time, which is how a request that failed before being written is
// recorded. Emitting the key with an empty value keeps every line's field set
// identical, so the reader never has to special-case a missing key.
func formatTimeOrEmpty(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return formatTime(t)
}
```

**设计取舍**

- **字段顺序**：`wrote_time` / `got_first_byte` 按时间先后插在 `req_time` 与 `resp_time` 之间
- **保留 `latency_us` 语义不变**（仍为 `respTime - reqTime`）
  → 所有现有分析脚本无需改动即可继续工作
- **不新增 `queue_us` 之类的派生字段**：留给分析脚本算，日志只存原始时间戳
- **零值输出为 `""` 而非省略该 key**：保证每行字段集一致，解析端逻辑简单

### 5.3 服务端 `InboundLogger` 的一处顺带修正（建议同时做）

**位置**：`httptransport.go` 第 169-185 行

当前顺序把 `sniffInboundUEID`（读整个 body + JSON 解析）排在时间戳**之前**，
这段耗时被算进了请求腿，污染测量：

```go
ueID := sniffInboundUEID(c.Request)   // 读 body + json.Unmarshal
reqTime := time.Now()                 // ← 时间戳在 sniff 之后
```

改为：

```go
func InboundLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		method := c.Request.Method
		uri := inboundURI(c.Request)

		// Timestamp first: sniffing reads and unmarshals the whole body, and
		// that cost belongs to this NF's own processing, not to the request's
		// journey from the caller. Taking reqTime beforehand keeps the request
		// leg free of it.
		reqTime := time.Now()

		// For the few request types whose UE id lives only in the body, sniff it
		// before the handler runs and restore the body so the handler is
		// unaffected. Every other request is untouched and pays no cost.
		ueID := sniffInboundUEID(c.Request)

		c.Next()
		respTime := time.Now()

		LogHTTPInbound(method, uri, ueID, reqTime, respTime)
	}
}
```

> 只影响 `POST /nausf-auth/v1/ue-authentications` 与
> `POST /npcf-am-policy-control/v1/policies` 两类请求（其余请求 `sniff` 立即返回）。
> 但正因为 AMF→AUSF 是关键链路，这个修正值得一并做。
>
> **注意**：此项会让这两类请求的服务端 `latency_us` 略微变大（把 sniff 计入了 handler 侧），
> 属于口径修正而非回归。若希望与历史数据严格可比，可单独作为第二次改动。

---

## 6. 实施步骤

```
1. 先改 udm（改动量最小、验证最快）：
   NFs/udm/internal/accesslog/httptransport.go   （5.1 + 5.3）
   NFs/udm/internal/accesslog/accesslog.go       （5.2）

2. cd NFs/udm && go build ./...    确认编译通过

3. 把同样改动复制到其余 6 个 NF。
   httptransport.go 7 份字节相同 → 可直接 copy 覆盖
   accesslog.go 需逐个改（LogHTTP 函数体相同，仅行号不同）

4. 逐个 go build ./... 验证

5. 全量 grep 确认没有遗漏的旧签名调用：
   grep -rn "LogHTTP(" --include=*.go NFs/ | grep -v "func LogHTTP"
   → 应只剩 7 处 httptransport.go 的调用，且均为 8 参数版本

6. 重建镜像并部署（参考 cloudlab/K8s/UPDATE_free5gc_custom_image.md）

7. 重跑 RQ200 / RQ1000 / RQ1500 / RQ2000
```

### 编译检查清单

- [ ] 7 个 NF 均 `go build ./...` 通过
- [ ] `grep -rn "LogHTTP(" --include=*.go NFs/` 无旧的 6 参数残留（新签名为 8 参数）
- [ ] `nef` 未被误改（它用 `http.DefaultClient`，本就不接 accesslog）

---

## 7. 验证方法

### 7.1 日志格式自检

新的一行应形如：

```json
{"src":"UDM","dst":"UDR","method":"GET","uri":"http://free5gc-udr-sbi:8000/nudr-dr/v2/...","ue_id":"imsi-999700000000001","req_time":"2026-08-04T01:31:20.361328172Z","wrote_time":"2026-08-04T01:31:20.361402881Z","got_first_byte":"2026-08-04T01:31:20.368512034Z","resp_time":"2026-08-04T01:31:20.368674597Z","latency_us":7346}
```

检查项：
- `req_time <= wrote_time <= got_first_byte <= resp_time`
- 服务端记录（`"src":"NaN"`）**不含**这两个键

> **注意**：`LogHTTPInbound` 本次不改签名，服务端行没有新字段。
> 分析脚本用 `rec.get("wrote_time", "")` / `rec.get("got_first_byte", "")`
> 读取即可，服务端行天然返回空。

### 7.2 分析脚本改动

在 `HTTP_per_UE_transport_latency_2way.py` 的基础上派生一份四段拆分：

```python
def ns_or_none(t):
    return _ts_to_ns(t) if t else None

# ---- 请求腿拆分（需要客户端记录 + 服务端记录配对）----
wrote = ns_or_none(rec_cli.get("wrote_time", ""))
if wrote is not None and wrote >= req_ns_cli:
    send_queue_ms  = (wrote - req_ns_cli) / 1e6        # ① 发送端排队
    to_receiver_ms = (req_ns_srv - wrote) / 1e6        # ② 内核 + 接收端

# ---- 响应腿拆分 ----
gfb = ns_or_none(rec_cli.get("got_first_byte", ""))
if gfb is not None and gfb >= resp_ns_srv:
    srv_write_ms = (gfb - resp_ns_srv) / 1e6           # ③ 服务端写出 + 传输
    cli_sched_ms = (resp_ns_cli - gfb) / 1e6           # ④ 客户端调度等待
```

一致性校验（应恒成立，可用来验证打点正确）：

```
① + ② == 原请求腿   (server.req_time - client.req_time)
③ + ④ == 原响应腿   (client.resp_time - server.resp_time)
```

### 7.3 判读规则

**请求腿**（RQ2000 下 UDM→UDR 约 9.70 ms）：

| 结果 | 结论 | 后续方向 |
|---|---|---|
| ① 占大头 | 发送端写锁排队是主因 | 每对 NF 开连接池，多条 TCP 分摊写锁 |
| ② 占大头 | 接收端排队是主因 | 查 `GOMAXPROCS`、UDR 副本数、读循环 HPACK 串行 |
| 两者相当 | 两个都要治 | 分别验证 |

**响应腿**（RQ2000 下 UDR→UDM 约 3.74 ms）：

| 结果 | 结论 | 后续方向 |
|---|---|---|
| ④ 占大头 | **客户端 goroutine 调度延迟**是主因 | 查客户端 `GOMAXPROCS`、并发 RoundTrip 数量 |
| ③ 占大头 | 服务端写出路径（抢 `serverConn` 写锁 / HPACK 编码）是主因 | 与请求侧的写锁问题同源，连接池同样有效 |

**横向对照**：若请求侧 ① 与响应侧 ③ 同时占大头，说明两个方向的
**写锁串行**是同一个根因的两面 —— 这将直接支持"单连接串行化"的判断，
且优化手段（增加连接数）可一次解决两侧。

同时应观察到：**低 RQ（200）下四段都应很小且大致稳定**。
若 RQ200 下 ① 或 ③ 就已经不小，说明存在与负载无关的固定开销，需另查。

---

## 8. 风险与影响

| 项 | 评估 |
|---|---|
| **请求内容** | 不变。`httptrace` 只挂回调，不修改 header/body |
| **注册路径延迟** | 热路径仅新增 2 次 `time.Now()` + 1 次 context 包装（约 150-250 ns）。写盘完全异步，不回压（见第 2 节） |
| **日志体积** | 每行增加约 80 字节，约 +14%。现有 queue 容量 2^21 与 1 MiB buffer 充裕 |
| **向后兼容** | `latency_us` 语义不变，字段只增不减 → 现有脚本仍可直接运行 |
| **5.3 的影响** | 会使两类 POST 的服务端 `latency_us` 略增（口径修正）。若需与历史严格可比，可拆为独立改动 |

### 已知不确定性（改动无法覆盖的部分）

**请求侧** `② = server.req_time - wrote_time` 内部仍混合：

- 内核 TCP 发送队列（单机 loopback 下极小）
- 对端读循环收帧 + **HPACK 解码排队**（全连接串行）
- spawn goroutine + **等 Go 调度器**
- gin 中间件链 + `sniffInboundUEID` 读 body（5.3 可去掉最后一项）

**响应侧** `③ = got_first_byte - server.resp_time` 内部仍混合：

- 服务端 HPACK 编码 + **抢 `serverConn` 写锁**
- 内核 TCP 发送 + 网络（单机 ≈ 0）
- 客户端读循环收帧 + HPACK 解码

响应侧无法进一步细分的原因见 1.4：**`httptrace` 没有服务端版本**，
拿不到与 `WroteRequest` 对等的"响应 frame 进内核"时刻。

要再往下拆（区分 HPACK 排队 vs goroutine 调度），只能靠 `runtime/trace`
抓调度事件，或 fork `golang.org/x/net/http2` 加内部打点 —— 成本显著更高，
建议先看本次四段拆分的结果再决定。

---

## 9. 参考

- 打点位置：`NFs/<nf>/internal/accesslog/httptransport.go`
- 单例 client（每对 NF 仅一条 TCP 连接的根源）：同文件 `sharedClient`，第 242-245 行
- 现有分析脚本：`cloudlab/Ty_log/Free5gc/C6525100g_PRerrorlog_0803/HTTP_per_UE_transport_latency_2way.py`
- 镜像重建流程：`cloudlab/K8s/UPDATE_free5gc_custom_image.md`
