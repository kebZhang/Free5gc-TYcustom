# 服务端 HTTP/2 `IdleTimeout` 1ms → 500ms + 客户端连接数回归 1 条 修改计划 (0807)

> **状态：代码已实施**（20 个文件：10 `server.go` + 7 `httptransport.go` + 3 `go.mod`）。
> **尚未编译验证**——本机无 Go 工具链，需在编译机执行 8.1 末尾的 `go build`。
> 依据来自 `C6525100g_NF2HTTPconn_0806v2` 的实测，
> 其中 **RQ5 UE10** 一组是决定性证据（第 1.3 节）。
> 本计划推翻了 `HTTP_MULTI_CONN_ROUNDROBIN_PLAN_0806.md` 2.2/2.3/6.4 节
> 对「连接数 > 2」的归因，见第 1.4 节。
>
> **本次含两项改动**：
> ① **服务端** `IdleTimeout` 1ms → 500ms（10 个 NF）；
> ② **客户端** `connsPerPeer` 2 → 1（7 个 NF）——开局只建 1 条连接，
> 但保留 Go 在撞 250 stream 上限时**自行扩容**的能力。
> ② 是 ① 的必要配套，理由见第 2 节 R3 注。

---

## 0. TL;DR

| # | 项 | 结论 |
|---|---|---|
| 1 | 改什么 | ① 服务端 `http2.Server.IdleTimeout`：**1ms → 500ms**；② 客户端 `connsPerPeer`：**2 → 1** |
| 2 | 那行代码在哪 | ① **不在本仓库**，在外部模块 `github.com/free5gc/util/httpwrapper`；② 在本仓库 `internal/accesslog/` |
| 3 | 怎么改 | ① 改 **10 个 NF** 的 `server.go`，自建 `http2.Server` 取代 `httpwrapper.NewHttp2Server`；② 改 **7 个 NF** 的 `httptransport.go` 一个常量 |
| 4 | 目的 | 恢复 HTTP/2 连接复用，把**重复建连开销**从 UE 注册延迟中剥离；连接数回到「默认 1 条 + 自行扩容」 |
| 5 | 现状有多糟 | RQ5 实测：**84 个请求 = 84 条连接，复用率 0%** |
| 6 | 必须注意 | ① 10 个 NF 的 `httpwrapper` import **必须删除**；② `chf`/`nef`/`smf` 的 `go.mod` **必须改**，否则编译失败（第 4.6 节） |

---

## 1. 目的与依据

### 1.1 要解决的问题

free5gc 每个 NF 的 SBI 服务端由 `httpwrapper.NewHttp2Server` 创建
（`github.com/free5gc/util`，v1.3.1 与 UDM 用的 `b2a2938f37b4` **两个版本均已逐字确认**）：

```go
h2Server := &http2.Server{
    // TODO: extends the idle time after re-use openapi client
    IdleTimeout: 1 * time.Millisecond,
}
```

`IdleTimeout` 语义：**连接上没有 active stream 的状态超过该时长，服务端发 GOAWAY 关闭连接。**
Go 官方默认值是 `0`（永不超时），是 free5gc 主动设成 1ms。

**后果**：两个请求之间只要有 >1ms 空隙，连接就被关掉。
HTTP/2「建一次、反复用」的价值被完全抵消。

### 1.2 为什么上游会设成 1ms（历史成因）

`httpwrapper.go` 自 **2021-09-13** 的 "Move and refactor util packages"
提交之后**四年多未被改动**；free5gc 主仓库中搜 `IdleTimeout`
**没有任何 issue 或设计讨论**。唯一线索是那条注释本身：

> `TODO: extends the idle time after re-use openapi client`

即：**等 openapi client 支持复用后，再把空闲时间调长。**
早期 free5gc 客户端每次调用都新建 client、不复用连接，服务端会积累大量
再也用不到的死连接，1ms 是针对那种情况的**快速回收兜底**。

**该前提今天已不成立**（`accesslog.Client()` 是单例，openapi 也有连接池），
但这行代码没跟着改，TODO 至今无人实施。

> 结论：**这是前提已失效的历史遗留值，不是经过性能权衡的设计决策。**

### 1.3 决定性证据：RQ5 UE10

高 RQ 数据受并发干扰，因果混杂。RQ5 UE10 剥离了并发，只剩「请求间隔」一个变量：

| NF 对 | 请求数 | socket 数 | 每 socket 请求数 | `conn_reused` |
|---|---|---|---|---|
| UDM→UDR | 84 | **84** | **1.0** | 0 复用 / 84 新建 |
| AMF→UDM | 54 | **54** | **1.0** | 0 复用 / 54 新建 |
| AMF→AUSF | 20 | **20** | **1.0** | 0 复用 / 20 新建 |
| AUSF→UDM | 20 | **20** | **1.0** | 0 复用 / 20 新建 |
| AMF→PCF | 10 | **10** | **1.0** | 0 复用 / 10 新建 |
| PCF→UDR | 10 | **10** | **1.0** | 0 复用 / 10 新建 |

**每个请求都用一条全新 socket，连接复用率为 0。**

请求间隔与 1ms 的关系（这是因果链的关键）：

| NF 对 | 间隔中位数 | 间隔最小 | 间隔最大 |
|---|---|---|---|
| UDM→UDR | 2.3ms | **1.4ms** | 192.7ms |
| AMF→UDM | 2.3ms | 1.8ms | **387.6ms** |
| AUSF→UDM | 9.6ms | 6.7ms | 192.9ms |
| AMF→AUSF | 12.5ms | 7.2ms | 192.8ms |
| AMF→PCF | **199.6ms** | 180.7ms | 217.4ms |
| PCF→UDR | **199.5ms** | 180.6ms | 217.7ms |

**最小间隔 1.4ms，仅比 1ms 大 0.4ms —— 这 0.4ms 就足以让服务端关掉连接。**

逐请求明细（UDM→UDR，RQ5）：

```
  #   gap_ms   lat_us  reused  slot  conn
  0      0.0     1995   False     1  192.168.88.249:54704
  1      2.1     2819   False     0  192.168.88.249:54714
  2      7.0     1431   False     1  192.168.88.249:54728
  ...
  8      2.0      855   False     1  192.168.88.249:54748
```

端口号一路递增，每条都是新握手；延迟 0.8~2.8ms **全部成功**——
连接不是因出错而死，是因「闲了 1ms」而死。

RQ5 同时排除了所有替代解释：

| 替代解释 | 否定方式 |
|---|---|
| 撞 250 stream 上限 | RQ5 几乎无并发，并发 stream ≈1，差两个数量级 |
| 客户端 `PingTimeout` 误杀 | 触发需空闲 ≥1s（`ReadIdleTimeout`），而连接活不过 2ms |
| CPU / 负载压力 | RQ5 是极低负载，系统近乎空闲 |
| 请求失败导致弃用 | 延迟 0.8~2.8ms，全部成功 |

### 1.4 须同步修正 0806 计划的错误归因

| 旧说法（`HTTP_MULTI_CONN_ROUNDROBIN_PLAN_0806.md`） | 实测结果 |
|---|---|
| 2.2 / 6.4 节：客户端撞 250 stream 上限溢出建连 | **否定**。峰值仅 37 / 65 / 156，从未撞顶 |
| 2.3 / 6.4 节：客户端 `PingTimeout` 误杀 | **否定**。前提是空闲 ≥1s，实测最大间隔仅 2~14ms（高 RQ），**该路径从未执行** |

**推论**：0806 把 `PingTimeout` 由 1s 改到 3s 的改动**未生效**（路径未触发），
因此也**没有**引入第二个变量，对照组可比性完好。

### 1.5 本次目的

1. **恢复 HTTP/2 连接复用**，消除每请求一次 TCP 握手 + HTTP/2 preface 的开销
2. 把**建连开销**从 UE 注册延迟测量中干净剥离，使后续延迟分析可信
3. 验证第 1.3 节的因果判断（改后 socket 数应显著下降）
4. **把连接数回归到「默认 1 条 + 按需自行扩容」**，使本次实验只动
   「连接寿命」一个变量（见第 2 节 R3 注、6.3 节）

---

## 2. 需求

| # | 要求 | 实现方式 |
|---|---|---|
| **R1** | 服务端 `IdleTimeout` 由 1ms 改为 **500ms** | 自建 `http2.Server{IdleTimeout: 500ms}` |
| **R2** | **所有 NF** 的服务端都改 | 10 个：amf ausf chf nef nrf nssf pcf smf udm udr |
| **R3** | 客户端**开局只建 1 条**连接，但**允许自行扩容** | `connsPerPeer` **2 → 1**（7 个 NF）；`StrictMaxConcurrentStreams` 保持不设，撞 250 stream 上限时 Go 自行扩容 |
| **R4** | 改动留在本仓库，git 可追溯 | 见第 3 节 |
| **R5** | `chf`/`nef`/`smf` 的 `x/net` 由 indirect 转直接依赖 | 手工改 `go.mod`，**不跑** `go mod tidy`（见 4.6） |

> **R3 为什么必须与 R1 同时做**：0806 的「2 条连接」在 `IdleTimeout=1ms` 下**从未真正成立**——
> 两个 slot 每次拿到的都是刚建好的新连接，因为上一条早被 GOAWAY 掉了。
> R1 一旦生效，这 2 条会**第一次**变成稳定并存的 socket，等于凭空引入一个新变量。
> 把 `connsPerPeer` 同时降回 1，本次实验才只动「连接寿命」这**一个**变量。

---

## 3. 方案选择：改调用方，不改外部库

`httpwrapper.go` **不在本仓库**（已确认 `find . -path '*httpwrapper*'` 为空，
10 个 `go.mod` **均无 `replace` 指令**）。

| | **方案 A：改调用方（采用）** | 方案 B：改外部库 + replace |
|---|---|---|
| 改动位置 | 10 个 `NFs/<nf>/internal/sbi/server.go` | 下载 util 源码 + 10 处 `replace` |
| 改动量 | 10 个文件 | 1 行代码 + 10 处 replace + 整个模块目录 |
| 风险 | 需自拼 `h2c`，但可照抄原函数 | **本项目踩过坑**：Aether 改了 `shared-libs/openapi` 却漏加 `replace`，构建**静默忽略**改动 |
| 可见性 | 改动在自己仓库，`git diff` 可见 | 藏在外部模块，易遗忘 |
| 依赖 | ✅ 10 个 NF 的 `go.mod` **均已含** `golang.org/x/net` | 需额外管理模块目录 |

**采用方案 A**：方案 B 的静默失效模式在本项目已经发生过一次。

---

## 4. 具体改动计划

### 4.1 改动范围

| 项 | 内容 |
|---|---|
| **待改文件 ①（服务端）** | `NFs/<nf>/internal/sbi/server.go` × **10** |
| NF 列表 | `amf` `ausf` `chf` `nef` `nrf` `nssf` `pcf` `smf` `udm` `udr` |
| **待改文件 ②（客户端）** | `NFs/<nf>/internal/accesslog/httptransport.go` × **7**（见 4.5） |
| NF 列表 | `amf` `ausf` `udm` `udr` `pcf` `nrf` `nssf` —— 只有这 7 个有 `internal/accesslog/` |
| **待改文件 ③（依赖）** | `NFs/{chf,nef,smf}/go.mod` × **3**（见 4.6） |
| **不改** | `bsf` `n3iwf` `tngf` `upf` —— 已确认**不调用** `NewHttp2Server` |
| **不改** | ✅ `internal/accesslog/accesslog.go`（`conn_slot` 字段保留）✅ `config/*.yaml` ✅ 其余 7 个 `go.mod` ✅ `Dockerfile.custom` |

> **`conn_slot` 字段保留不删**：改为 `connsPerPeer=1` 后它恒为 0。
> 删掉需要改 `LogHTTP` 签名 + 7 个 `accesslog.go`，且会让日志 schema 与 `0806v2` 不一致，
> 破坏 `analyze_http_conns.py` 的跨实验对比。留着零成本。

### 4.2 新增函数（10 个 NF 各加一份，放在各自 `server.go` 内）

完全照抄上游 `NewHttp2Server` 的行为，**只改 `IdleTimeout` 一个值**：

```go
// idleTimeoutPeriod replaces the 1ms that free5gc's httpwrapper.NewHttp2Server
// hard-codes into http2.Server.IdleTimeout.
//
// IdleTimeout is how long a connection may sit with no active stream before the
// server sends GOAWAY and closes it. Upstream sets 1ms behind a standing
// "TODO: extends the idle time after re-use openapi client" -- a leftover from
// when free5gc clients did not reuse connections at all and the server needed
// to reap dead ones aggressively. Clients reuse now (accesslog.Client() is a
// singleton), but the 1ms stayed.
//
// Measured with RQ5/UE10 (C6525100g_NF2HTTPconn_0806v2): every NF pair got
// exactly 1.0 requests per socket -- 84 requests over UDM->UDR opened 84 TCP
// connections, conn_reused false every time. The smallest observed gap between
// two consecutive requests was 1.4ms, i.e. only 0.4ms over the limit, which was
// enough to have the connection torn down between them.
//
// 500ms covers the median gap of every pair measured (2.3ms to 199.6ms) with
// room to spare, while still reaping genuinely idle connections far sooner than
// Go's own default of 0 (never).
const idleTimeoutPeriod = 500 * time.Millisecond

// newHttp2ServerWithIdleTimeout mirrors httpwrapper.NewHttp2Server exactly,
// apart from idleTimeoutPeriod above. It is duplicated per NF rather than
// shared because each NF is its own Go module with no common internal package.
//
// Like upstream, h2Server is wired in only through h2c.NewHandler, which covers
// cleartext HTTP/2. Under ListenAndServeTLS the h2c handler is bypassed
// entirely, so this IdleTimeout would not apply there and the connection would
// fall back to whatever ALPN negotiates. Every NF in config/ runs
// "scheme: http", so that path is unused in this deployment. If https is ever
// enabled, this function must also call http2.ConfigureServer(server, h2Server)
// AFTER setting server.TLSConfig -- assigning TLSConfig wholesale would
// otherwise discard the NextProtos that ConfigureServer installs.
func newHttp2ServerWithIdleTimeout(
	bindAddr, preMasterSecretLogPath string, handler http.Handler,
) (*http.Server, error) {
	if handler == nil {
		return nil, errors.New("server needs handler to handle request")
	}

	h2Server := &http2.Server{
		IdleTimeout: idleTimeoutPeriod,
	}
	server := &http.Server{
		Addr:    bindAddr,
		Handler: h2c.NewHandler(handler, h2Server),
	}

	if preMasterSecretLogPath != "" {
		preMasterSecretFile, err := os.OpenFile(
			preMasterSecretLogPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			return nil, fmt.Errorf(
				"create pre-master-secret log [%s] fail: %s", preMasterSecretLogPath, err)
		}
		server.TLSConfig = &tls.Config{KeyLogWriter: preMasterSecretFile}
	}

	return server, nil
}
```

### 4.3 调用点改动（两种形态，共 10 处）

**形态一 —— 赋值给 `s.httpServer`（8 个 NF）**

| NF | 文件 | 行号 |
|---|---|---|
| amf | `NFs/amf/internal/sbi/server.go` | 60 |
| ausf | `NFs/ausf/internal/sbi/server.go` | 54 |
| chf | `NFs/chf/internal/sbi/server.go` | 53 |
| nef | `NFs/nef/internal/sbi/server.go` | 83 |
| nrf | `NFs/nrf/internal/sbi/server.go` | 54 |
| pcf | `NFs/pcf/internal/sbi/server.go` | 132 |
| smf | `NFs/smf/internal/sbi/server.go` | 60 |
| udm | `NFs/udm/internal/sbi/server.go` | 53 |

```go
// 改前
if s.httpServer, err = httpwrapper.NewHttp2Server(bindAddr, tlsKeyLogPath, s.router); err != nil {
// 改后
if s.httpServer, err = newHttp2ServerWithIdleTimeout(bindAddr, tlsKeyLogPath, s.router); err != nil {
```

**形态二 —— 在 `bindRouter` 内直接 return（2 个 NF）**

| NF | 文件 | 行号 |
|---|---|---|
| nssf | `NFs/nssf/internal/sbi/server.go` | 99 |
| udr | `NFs/udr/internal/sbi/server.go` | 93 |

```go
// 改前
return httpwrapper.NewHttp2Server(bindAddr, tlsKeyLogPath, router)
// 改后
return newHttp2ServerWithIdleTimeout(bindAddr, tlsKeyLogPath, router)
```

### 4.4 ⚠️ import 调整（最容易出错的一步）

已逐个 NF 核实：**`NewHttp2Server` 是 10 个 NF 对 `httpwrapper` 的唯一使用**
（`grep -c "httpwrapper\." = 1`，且整个 `internal/sbi/` 目录内也只有这 1 处）。

**因此 10 个 NF 都必须删除这一行 import，否则 unused import 编译失败：**

```go
"github.com/free5gc/util/httpwrapper"   // ← 全部删除
```

各 NF 现有 import 状况（已核实），**需新增的部分**：

| import | 现状 | 需要新增的 NF |
|---|---|---|
| `"time"` | 10 个**全有** | 无 |
| `"fmt"` | 10 个**全有** | 无 |
| `"net/http"` | 10 个全有 | 无 |
| `"crypto/tls"` | **10 个全无** | **全部 10 个** |
| `"os"` | **10 个全无** | **全部 10 个** |
| `"errors"` | 仅 `nef` 有 | **除 nef 外的 9 个** |
| `golang.org/x/net/http2` | **10 个全无** | **全部 10 个** |
| `golang.org/x/net/http2/h2c` | **10 个全无** | **全部 10 个** |

> 注：上游用的是 `github.com/pkg/errors`，本计划改用标准库 `errors`
> （只用到 `errors.New`，行为一致，且 10 个 NF 均未引入 `pkg/errors`）。
> `nef` 已有 `"errors"`，不要重复添加。

> ⚠️ **grep 校验时要排除测试文件**：全仓库另有两处 `httpwrapper` 引用——
> `NFs/smf/internal/sbi/processor/pdu_session_test.go` 和
> `NFs/amf/internal/ngap/message/send_test.go.test`。
> 它们**不在改动范围内，不要动**，但会污染 8.1 的 grep 计数。

### 4.5 客户端：`connsPerPeer` 2 → 1（7 个 NF）

**目标语义**：两个 NF 之间**开局只建 1 条** HTTP/2 连接，
但**允许 Go 在压力下自行扩容**（撞对端 250 stream 上限时自动再拨号）。

**改动**：`NFs/<nf>/internal/accesslog/httptransport.go`，**7 个 NF 各改 1 处**。

> ✅ 已用文件哈希验证：**7 份 `httptransport.go` 字节完全一致**，
> 所以这是同一处改动复制 7 次，改完应再次 `diff -q` 确认仍然一致。

```go
// 改前
const connsPerPeer = 2
// 改后
const connsPerPeer = 1
```

**数组、循环、取模逻辑全部不用动**——`[1]http.RoundTripper`、
`for i := 0; i < 1; i++`、`% 1` 都自然成立，`connSlot` 恒为 0。

**同时替换该常量上方的注释块**（现有注释论证的是「为什么要 2 条」，与 `=1` 直接矛盾）：

```go
// connsPerPeer is how many HTTP/2 connections this NF opens to each peer NF up
// front. It is 1: the transport starts with a single connection per peer and is
// left free to add more on its own.
//
// Each slot is a separate http2.Transport with its own private pool, so N slots
// mean N connections held from the start. This was 2 for the round-robin
// experiment (HTTP_MULTI_CONN_ROUNDROBIN_PLAN_0806.md). That comparison never
// actually ran as designed: the server's 1ms IdleTimeout tore every connection
// down between requests, so each slot handed out a freshly dialled socket every
// time rather than holding one. With the server-side IdleTimeout now raised to
// 500ms, two slots would for the first time mean two genuinely long-lived
// sockets -- a second variable on top of the connection-lifetime change this
// experiment exists to measure. Hence back to 1.
//
// Growth is still permitted and expected: StrictMaxConcurrentStreams is
// deliberately left unset (see below), so when in-flight streams reach the
// peer's 250-stream limit the transport dials an additional connection by
// itself. The intent is "start at one, grow only under real pressure", not a
// hard cap of one.
const connsPerPeer = 1
```

**`StrictMaxConcurrentStreams` 保持不设**（0806 已移除，本次不恢复）——
它正是「允许自行扩容」的前提。

**不改** `accesslog.go`：`LogHTTP` 签名、`conn_slot` 字段全部保留（见 4.1 注）。

### 4.6 ⚠️ `go.mod`：`chf` / `nef` / `smf` 必须改，否则编译失败

这是**唯一会真正打断编译**的问题，且 4.4 的 import 表看不出来。

10 个 NF 的 `go.mod` 都含 `golang.org/x/net v0.47.0`，但**状态不同**（已逐个核实）：

| 状态 | NF |
|---|---|
| 直接依赖（主 `require` 块） | `amf` `ausf` `nrf` `nssf` `pcf` `udm` `udr` —— **无需处理** |
| **`// indirect`** | **`chf` `nef` `smf`** —— **必须处理** |

一旦在这 3 个 NF 的 `server.go` 里 `import "golang.org/x/net/http2"`，
`x/net` 就从间接依赖变成直接依赖，`go build` 直接失败：

```
go: updates to go.mod needed; to update it:
        go mod tidy
```

**修法：手工删掉 `// indirect` 后缀，不要跑 `go mod tidy`。**

| NF | `go.mod` 行号 | 现状 |
|---|---|---|
| `chf` | 111 | `golang.org/x/net v0.47.0 // indirect` |
| `nef` | 71 | `golang.org/x/net v0.47.0 // indirect` |
| `smf` | 77 | `golang.org/x/net v0.47.0 // indirect` |

```go
// 改前
	golang.org/x/net v0.47.0 // indirect
// 改后
	golang.org/x/net v0.47.0
```

**行位置不用动**（不必挪进主 `require` 块）：Go 只校验 `// indirect`
标记与实际使用是否一致，不校验它在哪个 `require` 块里。

**为什么不用 `go mod tidy`**：

1. `tidy` 会顺带增删一堆与本次无关的条目，污染 `git diff`，让「改了什么」不可读
2. `tidy` 会尝试联网访问 proxy；编译机若离线会直接失败
3. **本次完全不需要联网**：已核实 10 个 NF 的 `go.sum` 中
   `golang.org/x/net v0.47.0` 的**两条哈希（模块 + go.mod）均已存在**，
   `h2c` 与 `http2` 同属该模块同一版本，无需下载任何新内容

**离线验证**（不触网）：

```bash
cd NFs/<nf> && GOFLAGS=-mod=mod go build ./...
```

> ✅ 已核实：10 个 NF **均无 `vendor/` 目录**，
> 所以改 `go.mod` 后**不需要** `go mod vendor` 同步 vendor 树。

> ✅ 已核实：10 个 NF 的 `go.mod` 均为 `go 1.25.5`，
> 与 `Dockerfile.custom` 的 `golang:1.25` 匹配。

---

## 5. 为什么取 500ms

| 值 | 评估 |
|---|---|
| 1ms（上游） | 复用率 **0%**，每请求一条连接 |
| 100ms（本计划初版） | ❌ 覆盖不了 PCF 链路：`AMF→PCF` / `PCF→UDR` 间隔中位数 **~200ms** |
| **500ms（本次）** | ✅ 覆盖**全部**实测中位数（2.3~199.6ms），对最大的 PCF 中位数仍有 2.5 倍余量 |
| 5s / 30s | 更稳，但与上游差异大，且长空闲连接不回收 |
| 0（Go 默认） | 永不超时，pod 多时占资源 |

**500ms 的依据**（RQ5 UE10 实测，见 1.3）：

- 覆盖所有 NF 对的**间隔中位数**，最大者 PCF 的 199.6ms 仍有 2.5 倍余量
- 明显小于 Go 默认（永不超时），保留「闲置回收」语义

> ⚠️ **已知局限**：`AMF→UDM` 的**最大**间隔达 **387.6ms**，
> `PCF→UDR` 最大 217.7ms。500ms 覆盖得住这两个尾部值，
> 但如果真实流量存在 >500ms 的间隔（例如 UE 到达更稀疏的场景），
> 那些连接仍会被回收重建。这是**有意接受的取舍**：
> 目标是消除高频重建，而非追求连接永不关闭。

---

## 6. 风险

### 6.1 编译期风险（编译器会兜住，但会来回折腾）

| 风险 | 评估 | 处理 |
|---|---|---|
| **`chf`/`nef`/`smf` 的 `x/net` 是 `// indirect`** | **必然编译报错**，且 4.4 的 import 表看不出来 | **4.6**：手工删 `// indirect`，勿用 `go mod tidy` |
| **`httpwrapper` import 未删** | **会编译报错**（unused import） | 编译器兜底；10 个全需删（4.4） |
| **`errors` 重复 import** | `nef` 已有，重复会编译报错 | 4.4 已标注 |
| 误改到测试文件 | `smf` 的 `pdu_session_test.go` / `amf` 的 `send_test.go.test` 也含 `httpwrapper` | **不要动**；grep 校验时排除（4.4 注） |
| `go mod tidy` 联网失败 | 编译机若离线会中断 | 本次**不需要** tidy，`go.sum` 已含所需哈希（4.6） |
| 7 份 `httptransport.go` 改得不一致 | `connsPerPeer` 漏改某个 NF → **不报错，静默失效** | 改完 `diff -q` 确认 7 份仍字节一致（4.5） |

### 6.2 运行期风险

| 风险 | 评估 | 处理 |
|---|---|---|
| **漏改某个 NF 的 `server.go`** | 改完仍调用旧函数 → **不报错，静默失效** | 第 8.1 节 grep 残留数必须为 0 |
| **镜像 tag 对不上 → 跑的是旧代码** | **最危险**：不报错、有数据、数据全错 | 7.2；部署后**立即**核对 pod 内二进制时间戳 |
| 500ms 覆盖不了长尾间隔 | `AMF→UDM` 最大间隔 387.6ms，接近 500ms | 验证时看 socket 数是否降到个位数；不够则调大到 5s |
| 服务端内存 / FD 上升 | 连接活得久 → 并存连接变多 | 量级小（每连接几十 KB）；**当前每秒 241 次建连销毁的开销可能更大** |
| 半死连接多留 500ms | 对端崩溃且未正常关闭时 | 不影响正确性：客户端有 `ReadIdleTimeout=1s`+`PingTimeout=3s` 兜底 |
| **高 RQ 下 socket 数仍 >1** | **预期行为，不是失败**：连接活得久之后才第一次可能撞 250 stream 上限并自行扩容 | 见 8.2 的分档预期 |
| https 下 IdleTimeout 不生效 | 本部署 `config/` 全部 `scheme: http`，路径不触发 | 已在 4.2 注释中留痕 |
| 因果链尚无直接证据 | 目前靠排除法 + RQ5 指纹 | 本次实验即为直接证据 |

### 6.3 本次生效后的实际配置（三份 plan 叠加）

| 层 | 设置 | 效果 |
|---|---|---|
| 客户端连接数 | `connsPerPeer = 1`（本次 2→1） | 开局 1 条，撞 250 stream 时自行扩容 |
| 客户端健康检查 | `PingTimeout = 3s`（0806，保持） | 已确认该路径从未触发（1.4 节） |
| 服务端空闲回收 | `IdleTimeout = 500ms`（本次 1ms→500ms） | 连接不再每请求重建 |

**这等于回到「单连接」基线，只是修好了服务端那个让连接活不过 2ms 的 bug。**
本次实验只动**一个**变量（连接寿命），因果归因干净。

---

## 7. 部署流程（基于现有流程的增量）

代码改动后 **`Dockerfile.custom` 无需修改**（已构建全部 14 个 NF）。

### 7.1 机器 A：编译

```bash
cd /local/5GC/Free5gc-TYcustom
git pull        # 或同步改动

docker build -f Dockerfile.custom \
  -t free5gc-custom-v4.2.2:IdleTimeout500ms-0807v1 .

docker save free5gc-custom-v4.2.2:IdleTimeout500ms-0807v1 \
  -o free5gc-custom-v4.2.2-IdleTimeout500ms-0807v1.tar
```

### 7.2 ⚠️ 机器 B：注意 tag 一致性

> 现有流程中 build 出的是 `free5gc-custom-v4.2.2:NF2HTTPconn-0806v2`，
> 而 `free5gc-all-custom.yaml` 里写的是
> `repository: free5gc-custom, tag: v4.2.2-custom` ——
> **repository 与 tag 两边都对不上**。
> 若无额外处理，helm 会拉到旧镜像或拉取失败。**本次务必先核对。**

```bash
docker load -i free5gc-custom-v4.2.2-IdleTimeout500ms-0807v1.tar
docker images | grep free5gc-custom      # 以此输出为准填 yaml
```

`free5gc-all-custom.yaml` 中 10 个 NF 的 `repository`/`tag`
必须与 `docker images` 完全一致。

### 7.3 重启顺序（NRF 必须先起）

```bash
kubectl -n free5gc rollout restart deploy/free5gc-nrf
kubectl -n free5gc rollout status  deploy/free5gc-nrf
for nf in ausf udm udr nssf pcf smf amf chf webui; do
  kubectl -n free5gc rollout restart deploy/free5gc-$nf
done
kubectl -n free5gc rollout status deploy/free5gc-amf
```

随后按原流程验证 NF 注册数、跑 PacketRusher、收集
`/local/free5gcLog` 下的 HTTP_log / DB_log。

---

## 8. 验证清单

### 8.1 代码验证（编译前）

**服务端（10 个 NF）**

- [x] 10 个 `server.go` 均改为 `newHttp2ServerWithIdleTimeout`
- [x] **残留为 0**（排除测试文件）：
      `grep -rn "httpwrapper.NewHttp2Server" --include=*.go NFs/ | grep -v _test | wc -l`
- [x] **import 残留为 0**（排除测试文件）：
      `grep -rn "util/httpwrapper" --include=*.go NFs/ | grep -v _test | wc -l`
- [x] 10 个 NF 均含 `idleTimeoutPeriod = 500 * time.Millisecond`
- [x] 新增 import 齐全：`crypto/tls` `os` `http2` `h2c`（10 个），`errors`（除 nef 外 9 个）

**客户端（7 个 NF）**

- [x] 7 个 NF 均为 `connsPerPeer = 1`：
      `grep -rn "connsPerPeer = " --include=*.go NFs/ | grep -c "= 1"` 应为 **7**
- [x] `grep -rn "connsPerPeer = 2" --include=*.go NFs/ | wc -l` 应为 **0**
- [x] 7 份 `httptransport.go` 改后**仍字节一致**（两两 `diff -q` 或比对哈希）
- [x] `StrictMaxConcurrentStreams` 计数仍为 **0**（未被误恢复）
- [x] `accesslog.go` **未被改动**，`conn_slot` 字段保留（`git diff --stat` 确认）

**依赖（3 个 NF）**

- [x] `chf` / `nef` / `smf` 的 `go.mod` 中 `x/net` 已去掉 `// indirect`
- [x] `grep -rn "golang.org/x/net" NFs/*/go.mod | grep -c indirect` 应为 **0**
      （仅统计本次涉及的 10 个 NF；`bsf`/`upf` 不改，若在结果内需排除）
- [x] 其余 7 个 `go.mod` **未被改动**

**编译**

- [ ] 10 个 NF `go build ./...` 通过（**本机无 Go 工具链，需在编译机执行**）
- [ ] 编译**全程未联网拉取新模块**（`go.sum` 已足够，见 4.6）

### 8.2 实验验证

**必须复跑 RQ5 UE10** —— 它是最干净的判据（改前 100% 新建）：

- [ ] **RQ5 UE10：`conn_reused` 出现大量 `true`**（改前为 0）
- [ ] **RQ5 UE10：每 socket 请求数 ≫ 1.0**（改前恰为 1.0）
- [ ] RQ5 UE10：UDM→UDR socket 数由 **84 → 期望 1~2 条**（低负载不应触发扩容）

高 RQ 组（用已 slot-aware 的 `analyze_http_conns.py` 对比 `0806v2`）：

- [ ] **socket 数分档预期**（`connsPerPeer=1` 后）：

  | 负载 | `conn` 去重数预期 | 说明 |
  |---|---|---|
  | 低 RQ（RQ5） | **1~2** | 并发 stream ≈1，不触发扩容 |
  | 高 RQ | **>1 但比改前低 1~2 个数量级** | 撞 250 stream 上限时 Go 自行扩容，**属预期** |

  > ⚠️ 高 RQ 下 socket 数**不会**是严格的 1。连接活得久之后，
  > 单条 transport 的 in-flight stream 才**第一次**有机会堆到 250 上限
  > 并触发扩容——这正是 R3 要的「允许自行扩容」，**不是失败**。
  > 判据是**数量级下降**（如 PCF→UDR 250 → 个位数），不是绝对值等于 1。

- [ ] PCF→UDR 拨号速率由 **241/s → <10/s**
- [ ] `conn_slot` **恒为 0**（`connsPerPeer=1` 的直接体现；若出现 1 说明漏改）
- [ ] **UE reg latency 对比**（最终目的）：与 `0806v2` 同 RQ 对比 t2→t4
- [ ] 各 NF `dropped` 计数为 0

> **与 `0806v2` 对比时必须注明的变量差异**：
> `0806v2` 是「2 slot + IdleTimeout 1ms」，本次是「1 slot + IdleTimeout 500ms」。
> 但如 R3 注所述，`0806v2` 的 2 个 slot 在 1ms 下**从未真正持有 2 条长命连接**，
> 所以两组的实际差异**只有连接寿命一项**，可比性成立。

### 8.3 排错：若 socket 数**未**明显下降

**按顺序排查，每排除一层再往后推：**

| # | 检查 | 命令 / 判据 |
|---|---|---|
| 1 | **镜像真的生效了吗**（最常见） | `kubectl -n free5gc exec deploy/free5gc-udr -- ls -l /free5gc/udr` 核对时间戳；**部署后应立即做，不要等到数据异常才回头查** |
| 2 | `connsPerPeer` 真的是 1 吗 | 日志里 `conn_slot` 应恒为 0；出现 1 说明该 NF 漏改或用的旧二进制 |
| 3 | 间隔是否有 >500ms 的长尾 | 测该链路**实际请求间隔分布**；若长尾超 500ms 则调大到 **5s** 再试 |
| 4 | 间隔远小于 500ms 却仍频繁断连 | 根因不在 `IdleTimeout`，需重新排查（回到 1.3 的排除法） |

### 8.4 常见编译错误速查

| 报错 | 真因 | 修法 |
|---|---|---|
| `go: updates to go.mod needed; to update it: go mod tidy` | `chf`/`nef`/`smf` 的 `x/net` 仍是 `// indirect` | **4.6**：手工删 `// indirect`，**不要**真去跑 tidy |
| `"github.com/free5gc/util/httpwrapper" imported and not used` | 该 NF 的 `httpwrapper` import 没删 | 4.4：10 个全删 |
| `errors redeclared in this block` / duplicate import | 在 `nef` 里重复加了 `"errors"` | `nef` 已有，跳过它 |
| `undefined: http2` / `undefined: h2c` | 新增 import 漏了 | 4.4：`golang.org/x/net/http2` + `.../http2/h2c` |
| `undefined: tls.Config` / `undefined: os.OpenFile` | 漏加 `crypto/tls` / `os` | 4.4：10 个全需新增 |
| 拉取模块超时 / proxy 不可达 | 误跑了 `go mod tidy` | 本次无需联网，`go.sum` 已足够（4.6） |

---

## 9. 后续待办

本计划验证通过后，需回头修正
`HTTP_MULTI_CONN_ROUNDROBIN_PLAN_0806.md`：

- **状态**：标注 `connsPerPeer` 已由本计划 4.5 节改回 **1**，该文件描述的双连接配置**已不是当前状态**
- **2.2 节**：连接数 >2 的归因由「撞 250 溢出」改为「服务端 1ms 空闲超时」
- **2.3 节**：注明 `PingTimeout` 1s→3s **未生效**（路径未触发），未引入第二个变量
- **6.4 节**：「溢出建连」「健康检查重建」两条原因均已被实测否定
- **8 节**：其验证清单中 `conn` 去重数 = 2、每 slot 对应 conn 数等条目，
  在 `IdleTimeout=1ms` 下**注定不达标**，并非改造失败；这些条目已由本计划 8.2 取代

### 9.1 本次未做、留待后续

| 项 | 说明 |
|---|---|
| `Dockerfile.custom` 未纳入版本控制 | 它靠编译机上的 heredoc 现场生成，`git pull` 拉不到，属「改动静默不生效」的同构风险（与 Aether 漏加 `replace` 同类）。建议后续提交进本仓库 |
| https 下 `IdleTimeout` 不生效 | 已在 4.2 注释留痕；本部署全 `scheme: http`，暂不处理 |
| `IdleTimeout` 是否该取 5s | 500ms 离 `AMF→UDM` 尾部 387.6ms 较近；若 8.3 第 3 项命中则调大 |
