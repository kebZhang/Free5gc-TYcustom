# NF↔NF 多 HTTP/2 连接 + Round-Robin 改造计划 (0806)

> 基于 commit `98903eb`（`add NF-NF http transmission latency debug log in HTTP_log -- 0804`）实际代码阅读得出，
> 非记忆推断。所有引用的行号对应该 commit。

---

## 0. TL;DR（先回答四个问题）

| # | 问题 | 结论 |
|---|---|---|
| 1 | 目前两个 NF 通信是否只开一条 HTTP 连接（一条 TCP）？ | **是**。每个 NF 进程对每个对端 `host:port` 只有 **1 条 TCP**，所有 UE 的请求作为 HTTP/2 stream 复用其上。 |
| 2 | 改成一次性开 2 条连接 + Round-Robin 选连接，是否可行？ | **可行**，且改动面很小（每个 NF 只需改 `internal/accesslog/httptransport.go` 一个文件）。 |
| 3 | 请求走连接 1，响应是否一定从连接 1 回来？ | **是，100% 保证**。这是 HTTP/2 协议 + Go transport 的结构性保证，不存在跨连接错配。 |
| 4 | 改造计划 | 见第 4 节。核心：把 `loggingRoundTripper` 里的单个 `http2.Transport` **对象**换成 2 个独立对象 + 原子计数器轮询。 |

> **术语提醒**：下文所有"实例 / instance"一律指**进程内的 `http2.Transport` 对象**，
> **不是** NF 的 pod / 副本。NF 部署拓扑一个都不变。详见 2.2 节。

> **连接数固定为 2**：不引入环境变量，`connsPerPeer = 2` 是编译期常量。

---

## 1. 现状：为什么现在只有一条 TCP

### 1.1 调用链（实际代码，逐层验证）

以 AMF→UDM 的 SDM 查询为例：

```
consumer 层                     NFs/amf/internal/sbi/consumer/udm_service.go:27-49
  getSubscriberDMngmntClients(uri)
    ├─ 以 uri 为 key 缓存 APIClient（map + RWMutex），同一 uri 只建一次
    ├─ configuration.SetBasePath(uri)
    └─ configuration.SetHTTPClient(accesslog.Client())      ← :41
                                        │
openapi 层（外部模块 github.com/free5gc/openapi v1.2.3）
  openapi.CallAPI(cfg, request)
    └─ if cfg.HTTPClient() != nil { return cfg.HTTPClient().Do(request) }
                                        │
accesslog 层                    NFs/amf/internal/accesslog/httptransport.go
  sharedClient (:286-289)  ← 包级单例 var，整个 NF 进程唯一
    └─ Transport: newLoggingRoundTripper()  (:41-58)
         ├─ tls   : &http2.Transport{...}   (:43-47)   ← https 用
         └─ clear : &http2.Transport{...}   (:48-56)   ← http (h2c) 用
                                        │
golang.org/x/net v0.47.0 http2.Transport
  连接池 key = (scheme, host:port)  →  每个对端只保留一个 *ClientConn
```

### 1.2 三个"收敛点"叠加，最终收敛成 1 条 TCP

| 层级 | 收敛行为 | 代码位置 |
|---|---|---|
| **A. Client 单例** | `Client()` 返回包级 `sharedClient`，**所有** service（SDM/UECM/NRF/AUSF/PCF…）共用同一个 `*http.Client`，因此共用同一个 `loggingRoundTripper` | `httptransport.go:282-289` |
| **B. Transport 单例** | `newLoggingRoundTripper()` 只在 `sharedClient` 初始化时调用一次，里面的 `tls` / `clear` 两个 `http2.Transport` 是**固定的两个实例** | `httptransport.go:41-58, 286-289` |
| **C. http2 连接池** | `http2.Transport` 内部 `connPool` 以 `(scheme, authority)` 为 key。同一 key 命中已有 `ClientConn` 就直接复用，**不会**因为并发高就再开一条 | `golang.org/x/net/http2`（外部模块） |

> **注意 B 的一个细节**：`tls` 和 `clear` 是两个独立的 `http2.Transport`，
> 但它们按 **URL scheme** 二选一（`RoundTrip` :61-64）。
> 本项目 `config/*.yaml` 里 **全部是 `scheme: http`**
> （已核对 amfcfg/ausfcfg/udmcfg/udrcfg/pcfcfg/nrfcfg/nssfcfg 共 7 个文件），
> 所以实际运行时 **只有 `clear` 这一个 transport 在工作**，`tls` 恒为空闲。
> 这意味着当前有效连接数 = 1，而不是 2。

### 1.3 为什么"多个 APIClient"不会带来多条 TCP

这是最容易误判的一点。AMF 里确实有很多个 `APIClient`（NRF-Mngmt、NRF-Disc、UDM-SDM、UDM-UECM、AUSF、PCF、NSSF、SMF……），
`SetHTTPClient(accesslog.Client())` 在 **8 个文件**里被调用（`amf` 下 grep 结果）。

但它们传进去的是**同一个** `sharedClient` 指针。
`APIClient` 多 ≠ 连接多 —— 连接由最底层的 `http2.Transport` 连接池决定，与上层建了几个 APIClient 无关。

### 1.4 为什么"每个 UE 各自查 NRF"也不会带来多条 TCP

`NFs/amf/internal/disccache/disccache.go` 的缓存 key 是
`targetNfType | requesterNfType | serviceNames`（:59-68），**刻意排除了 supi**（见包注释 :20-27）。

因此**所有 UE 拿到的是同一个 SearchResult → 同一个 URI → 同一个 host:port**。
即便不看 disccache，本 deployment 每种 NF 只有一个实例，NRF 也只会返回同一个地址。

**结论**：不存在"不同 UE 落到不同 host:port 从而自然分裂出多条 TCP"的可能。

### 1.5 现状小结

```
AMF 进程
  └── sharedClient (唯一)
        └── clear *http2.Transport (唯一，scheme=http 时唯一在用)
              ├── ClientConn → UDM:8000    ← 1 条 TCP，承载所有 UE 的 SDM+UECM
              ├── ClientConn → AUSF:8000   ← 1 条 TCP
              ├── ClientConn → PCF:8000    ← 1 条 TCP
              └── ClientConn → NRF:8000    ← 1 条 TCP
```

**每个 NF-pair 恰好 1 条 TCP。** 这与 `HTTP_WROTE_TIME_PLAN_0804.md` 第 1.1 节
描述的"抢 `clientConn` 写锁排队"是同一个瓶颈的两种表述 —— 单条连接上的
写锁与 HPACK 编码是全连接串行的。

---

## 2. 问题 2：改成 2 条连接 + Round-Robin，可行性分析

### 2.1 结论：可行

关键在于**如何让 Go 的 http2 连接池认为这是"两个不同的目标"**。有三条路，推荐第一条。

| 方案 | 做法 | 评价 |
|---|---|---|
| **A. 多 Transport 实例（推荐）** | 建 N 个独立的 `http2.Transport`，每个有自己的连接池，因此每个对同一 host 各开 1 条 TCP。RoundTrip 时用原子计数器轮询选一个 | ✅ 改动最小（1 个文件）<br>✅ 不碰 URL、不碰 openapi 模块<br>✅ 语义干净，N 可配置 |
| B. `MaxConcurrentStreams` 限流触发扩容 | 依赖 h2 在 stream 用满时新开连接 | ❌ 行为不可控，取决于 server 的 SETTINGS，不是"固定 2 条" |
| C. 改 URL 使 authority 不同 | 给同一 IP 造两个 host 别名 | ❌ 侵入 URI，污染 HTTP_log 的 uri 字段与现有分析脚本 |

### 2.2 术语澄清：「实例」指的是什么

⚠️ **这里说的"建 N 个实例"，指的是 Go 进程内部的 `http2.Transport` 对象个数，
与 NF 的部署数量（pod 数、副本数）毫无关系。**

NF 拓扑**完全不变**：还是 1 个 AMF、1 个 UDM、1 个 UDR，各自 1 个 pod。
改的只是**每个 NF 进程内部对同一个对端维持几条 TCP**。
两条 TCP 打到的是**同一个 UDM 的同一个 `host:port`**，
UDM 侧只是看到"有个客户端开了两条连接过来"。

```
【现在】1 个 AMF 进程                【改后】还是同一个 AMF 进程
  sharedClient                         sharedClient
    └── clear: 1 个 http2.Transport      └── clear: [2]http2.Transport
          └── 1 条 TCP ──> UDM pod             ├── transport[0] → TCP ──┐
                                                └── transport[1] → TCP ──┴─> 同一个 UDM pod
```

### 2.3 方案 A 为什么必然产生 2 条 TCP

`http2.Transport` 的连接池是**每个 Transport 对象私有的**（`t.connPoolOrDef()` → 该对象自己的 `clientConnPool`）。
两个不同的 `*http2.Transport` 对象之间**不共享任何 ClientConn**。

所以：

```
2 个 http2.Transport 对象  →  对同一个 UDM:8000  →  2 条独立 TCP
```

这是结构性的，不依赖任何 tuning 参数或 server 行为。

### 2.4 "一次性开启"的语义澄清

Go 的 `http2.Transport` 是**懒建连**的：只有第一个请求到达时才拨号。
所以严格意义上的"一次性同时开 2 条"需要区分两种理解：

- **理解一（推荐，默认）**：2 条连接**槽位**在启动时就确定，各自在自己的首个请求到来时建立。
  轮询从第 1 个请求就开始，因此前 2 个请求会分别触发 2 次拨号，之后稳定为 2 条长连接。
  → **无需额外代码**，方案 A 天然满足。

- **理解二**：进程启动时立刻主动拨号 2 条，不等业务请求。
  → 需要额外的预热（warm-up）逻辑，见 4.4 节「可选增强」。
  实验场景下通常**不必要**，因为注册洪水的头两个请求瞬间就会把连接建起来。

### 2.5 与现有日志的兼容性

改造**不影响** `HTTP_log.txt` 的任何字段：

- `req_time` / `resp_time`：仍在 `RoundTrip` 内的同一位置取（:116, :118）
- `wrote_time` / `got_first_byte`：由 `httptrace` 回调产生（:104-113），
  回调闭包捕获的是**本次调用的局部变量**，与走哪条连接无关
- `sniffUEID` / `dstNFFromURL`：纯粹基于 `req`，与连接无关

**唯一建议的新增**：给日志加一个 `conn_idx` 字段，用于事后验证轮询确实生效、
以及分连接对比延迟。见 4.3 节。

### 2.6 风险清单

| 风险 | 评估 | 处理 |
|---|---|---|
| 对端 server 连接数上限 | h2c server（gin + h2c）默认不限制连接数，2 条无压力 | 无需处理 |
| 打乱现有 latency 基线 | 会。这正是实验目的 | 基线组直接用**改造前的镜像**（或把 `connsPerPeer` 改回 1 重新构建），两者行为逐字一致 |
| 响应乱序 / 错配 | **不存在**，见第 3 节 | — |
| 连接不均衡 | 轮询是按**请求数**均衡，不是按**字节数**或**耗时**均衡。若某类请求特别慢，两条连接负载可能不均 | 可接受；`conn_idx` 日志可事后验证 |
| `ReadIdleTimeout` 健康检查开销翻倍 | 每条连接各自 1s ping，N=2 时 ping 翻倍。开销可忽略 | 无需处理 |

---

## 3. 问题 3：请求走连接 1，响应会不会从连接 2 回来？

### 3.1 结论：不会，有三重结构性保证

**绝对不会出现"req 发给连接 1，resp 从连接 2 回来"的情况。** 理由：

**① HTTP/2 协议层**
stream ID 的作用域是**单条连接内部**。连接 1 上的 stream 5 与连接 2 上的 stream 5
是两个毫无关系的东西。server 只能在**收到请求的那条连接**上回复该 stream，
协议本身没有"跨连接回复"的表达能力。

**② Go transport 实现层**
`http2.Transport.RoundTrip` 的实际流程是：

```
RoundTrip(req)
  → 从连接池取得一个具体的 *ClientConn  cc
  → cc.RoundTrip(req)
      → cc.newStream()  在 cc 上分配 stream，登记进 cc.streams[id]
      → 写 HEADERS/DATA 到 cc 的 socket
      → 阻塞等待 cs.resc（这个 stream 私有的 channel）
  ← cc 的读循环 readLoop 收到该 stream 的响应帧后，
    查 cc.streams[id] 找到 cs，写入 cs.resc
```

响应的投递路径是 `cc.readLoop → cc.streams[id] → cs.resc`，
**全程被 `cc` 这一个连接对象闭包住**。连接 2 的 readLoop 根本访问不到连接 1 的 `streams` map。

**③ 本方案的隔离层**
方案 A 里，第 i 个请求交给第 `i % 2` 个 `http2.Transport` 对象，
该对象的 `RoundTrip` 是一个**同步阻塞调用**，返回值就是该次请求的响应。
我们的 `loggingRoundTripper.RoundTrip` 里：

```go
base := pick()               // 选定一个 transport
resp, err := base.RoundTrip(req)   // 同步，resp 必然来自 base
```

`resp` 由被选中的那个 transport 直接返回。**不存在任何跨实例的路由或汇聚**。

### 3.2 一个需要留意的边界情况（不是错配，但值得知道）

`http2.Transport` 在**连接层错误**（如 `GOAWAY`、连接被对端关掉）时，
会对**幂等请求**自动重试 —— 重试可能落到该 transport **新建的另一条连接**上。

这仍然不是"响应跨连接错配"：重试是一次全新的请求-响应对，请求和响应依然在同一条新连接上配对。

但它对日志有一个已知影响，**现有代码已经处理了**：

```go
// httptransport.go:104-109
WroteRequest: func(httptrace.WroteRequestInfo) {
    if wroteTime.IsZero() {      // ← 只保留第一次写出的时间
        wroteTime = time.Now()
    }
},
```

注释 :100-102 明确说明了这一点。改造后若加 `conn_idx`，
需注意重试场景下记录的是**最初选中**的那个 transport 索引（见 4.3 的说明）。

---

## 4. 修改计划

### 4.0 改动范围总览

| 项 | 内容 |
|---|---|
| **需要改的文件** | `NFs/<nf>/internal/accesslog/httptransport.go`，共 **7 个 NF**：`amf` `ausf` `udm` `udr` `pcf` `nrf` `nssf` |
| **可选改的文件** | `NFs/<nf>/internal/accesslog/accesslog.go`（加 `conn_idx` 字段），同样 7 份 |
| **不需要改** | ✅ 任何 consumer/service 代码（`SetHTTPClient(accesslog.Client())` 调用点全部保持原样）<br>✅ openapi 外部模块（**无需 fork、无需 replace**）<br>✅ 任何 `config/*.yaml`<br>✅ 任何现有分析脚本（字段只增不改） |

> **重要前提（已验证）**：7 个 NF 的 `accesslog/httptransport.go` 内容**完全一致**，
> 只有 `accesslog.go:32` 的 `const srcNF` 不同。
> 因此可以在 `amf` 上改好后，把 `httptransport.go` 原样复制到其余 6 个 NF（该文件不含 NF 名）。
> **不要**连 `accesslog.go` 一起复制，会覆盖 `srcNF`。

> **`smf` 说明**：`grep -rl "accesslog.Client()" NFs/smf` 结果为 0，
> SMF 没有接入 accesslog 客户端，因此本次不涉及。若后续要覆盖 SMF，需先给它接入 accesslog。

---

### 4.1 Step 1：改造 `loggingRoundTripper`（核心，唯一必须的改动）

文件：`NFs/amf/internal/accesslog/httptransport.go`

#### 1a. 新增连接数常量（在 :26-30 的 const 块附近）

```go
// connsPerPeer is how many independent HTTP/2 connections this NF keeps to each
// peer NF. Each connection is backed by its own http2.Transport instance, and
// http2.Transport connection pools are per-instance, so connsPerPeer instances
// yield that many separate TCP connections to the same host:port. Requests are
// spread across them round-robin.
//
// This is a compile-time constant on purpose: the deployment always wants two
// connections. Setting it back to 1 reproduces the previous single-connection
// behaviour byte-for-byte, which is the only other value worth building.
const connsPerPeer = 2
```

> 不需要新增 `"os"` / `"strconv"` import —— 常量写死，无需读环境变量。

#### 1b. 把 `loggingRoundTripper` 的两个字段从单实例改为定长数组（:36-39）

```go
// 改造前
type loggingRoundTripper struct {
    tls   http.RoundTripper
    clear http.RoundTripper
}

// 改造后
type loggingRoundTripper struct {
    // One slot per connection to each peer. Each element is a SEPARATE
    // http2.Transport instance with its own connection pool — that is what makes
    // them distinct TCP connections instead of one shared one.
    tls   [connsPerPeer]http.RoundTripper // h2-over-TLS transports (https)
    clear [connsPerPeer]http.RoundTripper // h2c transports (http)
    next  atomic.Uint64                   // round-robin cursor, shared by both schemes
}
```

> 用定长数组而非 slice：`connsPerPeer` 是编译期常量，数组能省掉构造函数里的 `make`，
> 长度也不可能在运行时被改错。
> `atomic.Uint64` 需要 import `"sync/atomic"`。
> 用 `atomic` 而非 mutex：这是每请求都要走的热路径，
> `Add` 是单条 LOCK XADD 指令，比抢锁便宜得多，且与 `accesslog.go` 里
> `dropped atomic.Uint64` 的既有风格一致。

#### 1c. 改造构造函数（:41-58）

把现在**一次性构造一组 transport**（tls + clear 各一个）的写法，
改为 **循环构造 `connsPerPeer` 组**。
每次循环内 `&http2.Transport{...}` 的字段配置**逐字保持不变**
（`TLSClientConfig` / `AllowHTTP` / `DialTLSContext` / `ReadIdleTimeout` / `PingTimeout`），
这样把 `connsPerPeer` 改回 1 时与改造前完全等价。

```go
func newLoggingRoundTripper() *loggingRoundTripper {
    l := &loggingRoundTripper{}
    for i := 0; i < connsPerPeer; i++ {
        // Each iteration builds a SEPARATE http2.Transport. Separate instances
        // mean separate connection pools, which is what produces connsPerPeer
        // distinct TCP connections to the same peer. Field values are identical
        // to the pre-change single-transport config, so setting connsPerPeer
        // back to 1 is behaviourally identical to the old code.
        l.tls[i] = &http2.Transport{
            TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // matches openapi default
            ReadIdleTimeout: readIdleTimeoutPeriod,
            PingTimeout:     pingTimeoutPeriod,
        }
        l.clear[i] = &http2.Transport{
            AllowHTTP: true,
            DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
                d := &net.Dialer{}
                return d.DialContext(ctx, network, addr)
            },
            ReadIdleTimeout: readIdleTimeoutPeriod,
            PingTimeout:     pingTimeoutPeriod,
        }
    }
    return l
}
```

> ⚠️ **Go 版本注意**：`NFs/*/go.mod` 声明 `go 1.25.5`，循环变量按 Go 1.22+ 语义
> 每轮迭代独立，闭包捕获 `i` 无经典陷阱。且此处 `DialTLSContext` 闭包本身
> 并未捕获 `i`，无论如何都是安全的。

#### 1d. 改造 `RoundTrip` 的选路逻辑（:60-64）

```go
// 改造前
base := l.clear
if req.URL != nil && req.URL.Scheme == "https" {
    base = l.tls
}

// 改造后
pool := &l.clear
if req.URL != nil && req.URL.Scheme == "https" {
    pool = &l.tls
}
// Round-robin across the connsPerPeer transports. The cursor is shared between
// the tls and clear pools; since this deployment is http-only (every
// config/*.yaml sets scheme: http) only the clear pool is ever indexed in
// practice, so sharing the cursor costs nothing and keeps the hot path to a
// single atomic op.
idx := int((l.next.Add(1) - 1) % connsPerPeer)
base := pool[idx]
```

> ⚠️ **必须取地址 `&l.clear`,不能写 `pool := l.clear`**。
> 数组在 Go 里是值类型,直接赋值会**整个复制一份**;虽然复制的是接口指针、
> 功能上仍能工作,但每个请求都白白拷贝一次数组。用 `*[connsPerPeer]http.RoundTripper`
> 指针可以避免,且索引语法 `pool[idx]` 不变（Go 会自动解引用）。
> 若嫌指针别扭,也可以把字段改回 slice —— slice 赋值本来就是引用语义。

> `Add(1) - 1` 而不是 `Add(1)`：让第一个请求拿到 idx=0，
> 使 `connsPerPeer=1` 时的行为、以及日志里的索引都从 0 开始，更直观。

> `% connsPerPeer` 直接用常量取模：编译器能把 `% 2` 优化成位运算，
> 比 `% uint64(len(pool))` 更省。

`RoundTrip` 剩余部分（`dstNFFromURL`、`sniffUEID`、`httptrace` 回调、
`reqTime`/`respTime` 取值、`LogHTTP` 调用）**完全不动**。

---

### 4.2 Step 2：同步到其余 6 个 NF

```bash
cd /path/to/Free5gc-TYcustom
for nf in ausf udm udr pcf nrf nssf; do
  cp NFs/amf/internal/accesslog/httptransport.go \
     NFs/$nf/internal/accesslog/httptransport.go
done
```

改完后逐个验证编译：

```bash
for nf in amf ausf udm udr pcf nrf nssf; do
  (cd NFs/$nf && go build ./... ) || echo "BUILD FAIL: $nf"
done
```

> 复制前建议先 `git diff --stat` 确认 7 份 `httptransport.go` 在改动前确实一致：
> ```bash
> for nf in ausf udm udr pcf nrf nssf; do
>   diff -q NFs/amf/internal/accesslog/httptransport.go \
>           NFs/$nf/internal/accesslog/httptransport.go
> done
> ```
> （本计划撰写时已核对为一致；若你在此之前动过某个 NF，请重新确认。）

---

### 4.3 Step 3（推荐但可选）：给 HTTP_log 加 `conn_idx`

没有这个字段，你无法从日志验证轮询是否真的生效、也无法分连接对比延迟。

**改 `accesslog.go` 的 `LogHTTP`**（7 份，`amf` 版在 :318-333）：
新增一个 `connIdx int` 参数，并在 `resp_time` 之后、`latency_us` 之前插入：

```go
b = appendKVInt(b, "conn_idx", connIdx, false)
```

需要新增一个整数版的 helper（放在 `appendKV` 附近）：

```go
// appendKVInt appends an integer-valued JSON field. Unlike appendKV the value is
// emitted unquoted so downstream analysis can read it as a number directly.
func appendKVInt(b []byte, key string, val int, first bool) []byte {
    if !first {
        b = append(b, ',')
    }
    b = appendJSONString(b, key)
    b = append(b, ':')
    return strconv.AppendInt(b, int64(val), 10)
}
```

（`strconv` 在 `accesslog.go:24` 已经 import，无需新增。）

**改 `httptransport.go` 的调用点**（:121）：

```go
LogHTTP(dst, method, uri, ueID, idx, reqTime, wroteTime, gotFirstByte, respTime)
```

> **语义说明（务必写进注释）**：`conn_idx` 记录的是本次 `RoundTrip` **最初选中**的
> transport 索引。若该 transport 内部因连接层错误重试并新建了连接，
> 索引不变 —— 因为重试仍发生在**同一个 transport 实例**内，
> 只是换了它自己池子里的一条新 TCP。所以 `conn_idx` 准确表达的是
> **"走的是哪一路 transport"**，而不是"第几条 TCP 套接字"。
> 在稳态实验中这两者一一对应。

> **`LogHTTPInbound` 不加此字段**：server 端无法得知对端用了哪条连接
> （现有代码里 `src` 已经因同样的原因记为 `"NaN"`，见 :350-352）。
> 保持 inbound 记录不变，分析脚本按 `conn_idx` 字段是否存在即可区分两种视角。

---

### 4.4 Step 4（可选增强）：启动时预热连接

若你要的是 2.3 节的**理解二**（进程启动即刻建满 2 条），加一个预热函数：

```go
// WarmUp eagerly establishes all connsPerPeer connections to peerBaseURL so the
// first real SBI request never pays a dial. Safe to call multiple times and safe
// to ignore errors: a failed warm-up just means the connection is dialled lazily
// later, exactly as it would be without this call.
func WarmUp(peerBaseURL string) { /* 对每个 transport 各发一次轻量请求 */ }
```

**但本计划建议先不做**，理由：

- 注册洪水的头几个请求会在毫秒内把连接全部建起来，稳态实验不受影响
- 预热需要一个"无副作用的 SBI 端点"，各 NF 不统一，实现成本与出错面都不小
- 如果只是想避开冷启动，更简单的做法是**丢弃实验前几秒的数据**

---

### 4.5 Step 5：验证

#### 5a. 连接数验证（最直接）

在 NF pod 内（或宿主机 `nsenter` 进 netns）：

```bash
# 改造前：每个对端 NF 应只有 1 行
# 改造后（connsPerPeer=2）：每个对端 NF 应有 2 行
ss -tnp | grep ESTAB | grep <peer-ip>:8000
```

也可从对端 server 侧看，结论应一致。

> 注意 NF 之间是**双向**的（例如 AMF→UDM 和 UDM→AMF 的回调）。
> 数连接时要按"谁主动拨号"区分，用本地端口是否为临时端口来判断。

#### 5b. 轮询生效验证（需 Step 3 的 `conn_idx`）

```bash
# 两个索引的计数应该接近 1:1
grep '"src":"AMF"' /tmp/HTTP_log.txt | grep -o '"conn_idx":[0-9]*' | sort | uniq -c
```

#### 5c. 对照实验设计

`connsPerPeer` 是编译期常量，所以对照组靠**镜像**区分，不靠运行时配置：

| 组 | 镜像 | 说明 |
|---|---|---|
| 基线 | 改造前的镜像（现有的） | 直接复用现有历史数据，无需重跑 |
| 实验 | `connsPerPeer = 2` 构建的新镜像 | 目标配置 |

> 若之后想看收益是否随连接数继续增长，把常量改成 4 / 8 各构建一个镜像即可；
> 但按你的要求，当前只做 2。

#### 5d. 关键观测指标

对照 `HTTP_WROTE_TIME_PLAN_0804.md` 定义的分段：

| 指标 | 期望变化（若"单连接写锁串行"确为瓶颈） |
|---|---|
| `wrote_time - req_time`（发送端抢写锁） | **应显著下降**（写锁竞争者减半） |
| `got_first_byte - server.resp_time`（含服务端抢写锁） | **应下降**（服务端 serverConn 写锁同样减半） |
| `server.req_time - wrote_time` | 变化不大（内核+对端调度，与连接数关系弱） |
| `resp_time - got_first_byte`（客户端 goroutine 调度） | 变化不大（是 Go 调度器问题，非连接问题） |

> 如果 `wrote_time - req_time` **没有**明显下降，
> 说明瓶颈不在 clientConn 写锁，而在别处（如接收端 goroutine 调度或 CPU），
> 这本身也是一个有价值的负面结论。

---

## 5. 附录：改动前后连接拓扑对比

注意左边始终是**一个** AMF 进程，右边始终是**一个** UDM pod。

```
【改造前】connsPerPeer=1
1 个 AMF 进程 ══════════════════> 1 个 UDM pod   1 条 TCP，全部 UE 的 stream 挤在上面
     (clear: 1 个 http2.Transport)                clientConn 写锁 = 全局串行点

【改造后】connsPerPeer=2
1 个 AMF 进程 ══════════════════> 1 个 UDM pod   transport[0] 的 1 条 TCP
              ══════════════════>  (同一个)       transport[1] 的 1 条 TCP
     (clear: 2 个独立 http2.Transport)            写锁竞争者各减半，两条连接完全独立
                                                  req/resp 在各自连接内严格配对
```

---

## 6. 检查清单

- [ ] 确认 7 份 `httptransport.go` 改动前内容一致（`diff -q`）
- [ ] `amf` 版改造：`const connsPerPeer = 2`、struct 字段改数组、构造函数改循环、`RoundTrip` 选路加轮询
- [ ] 补 import：`sync/atomic`（**不需要** `os` / `strconv`，因为不读环境变量）
- [ ] 确认 `RoundTrip` 里用的是 `&l.clear` / `&l.tls`（取地址，避免每请求拷贝数组）
- [ ] 复制到 `ausf` `udm` `udr` `pcf` `nrf` `nssf`（**不要**动 `accesslog.go` 的 `srcNF`）
- [ ] （可选）`accesslog.go` 加 `appendKVInt` + `LogHTTP` 的 `conn_idx` 参数（7 份）
- [ ] 7 个 NF 逐个 `go build ./...` 通过
- [ ] 重建镜像并部署
- [ ] `ss -tnp` 验证每个 NF-pair 连接数 = 2
- [ ] `conn_idx` 分布验证轮询均衡（两个索引应接近 1:1）
- [ ] 与改造前的历史数据对比 4.5d 的四个指标
