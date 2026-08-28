# HTTP/2 request 发送、服务端 handler 启动与 response 发送分段日志计划（0826，v2 — 已按实际代码校订）

> **版本说明**
>
> v1 是在没有本地 `x/net` 源码的前提下写的设计稿。现在 `xnet/`（`golang.org/x/net v0.47.0`
> 的 pristine fork，见 `xnet/FORK_PROVENANCE.md`）已经落地，本文档已逐条对照
> `xnet/http2/{transport.go,server.go,write.go,http2.go,frame.go,config.go}` 与
> `NFs/<nf>/internal/{accesslog,sbi}/*.go` 的真实代码重写。
>
> 所有行号均对应仓库当前状态（`xnet` = upstream `v0.47.0`；NF 代码 = commit `2fe69be`）。
> 第 20 节集中列出 v1 中与代码不符、或按 v1 写法会拿不到数据/拿到错数据的地方。
>
> **本轮修订推翻的 v1 关键结论（详见第 20 节）**
>
> 1. v1 第 2.1 节的六个原有打点行号**全部错**（偏移 8~11 行）。
> 2. v1 把 W 的取时点定在 `http2/http2.go::writeWithByteTimeout`。该函数是**包级函数、没有
>    连接状态**，而且本部署 `WriteByteTimeout == 0`，函数第一行就 `return conn.Write(p)`，
>    v1 给出的 `for` 循环伪代码**根本不会执行**。正确 hook 是
>    `(*bufferedWriterTimeoutWriter).Write`。
> 3. v1 用 `producedBytes() + 9 + len(frag)` 计算 marker 偏移。该式依赖 HEADERS 帧无
>    padding/priority 的实现细节，且**登记时机相对 `bufio` 自动 flush 有竞态**。改为
>    "在 `writeHeaderBlock` 挂 pending、在 `(*bufferedWriter).Write` 内用 `produced + len(p)`
>    结算"，精确且无帧布局假设。
> 4. v1 要求在 `writeResHeaders` 里运行时判断 `httpResCode >= 200 && trailers == nil`。
>    实际 `writeResHeaders{}` 只有三个构造点，其中最终 response 那个（`server.go:2717`）位于
>    `if !rws.sentHeader` 内，**每个 response 恰好执行一次**。只在该处挂 trace 即可，
>    运行时判断可以完全删掉。
> 5. v1 的 JSON 字段名与现有代码不符：现有 client 行用的是 `conn` / `conn_slot` /
>    `conn_reused` / `latency_us`，没有 `connection_id`、没有 `stream_id`、没有 `retry_count`。
> 6. v1 没有说 server 侧的 connection identity **不需要动 fork**：`c.Request.RemoteAddr`
>    就是 `sc.remoteAddrStr`，`InboundLogger` 直接可读。只有 `stream_id` 必须来自 fork。
> 7. v1 没有给出 `T4 <= W` 的**可判定阈值**。实际阈值是精确的：response body
>    `< handlerChunkWriteSize = 4 KiB` 时 flush 发生在 handler 返回之后（T4 < W）；
>    `>= 4 KiB` 时 `rws.bw` 在 handler 内部自动 flush（W < T4）。

---

## 0. 本计划必须满足的三项总体保证

### 0.1 三个新增时间点统一写入现有 `HTTP_log.txt`

本计划一共新增三个时间点：

```text
M = req_header_mu_start_time
G = server_handler_go_time
W = server_response_headers_flushed_time
```

三者全部进入各 NF Pod 现有的同一个 HTTP access-log sink，即 `HTTP_LOG_PATH`（未配置时为
`/tmp/HTTP_log.txt`，见 `NFs/<nf>/internal/accesslog/accesslog.go` 的 `envHTTPPath` /
`defaultHTTPPath`）：

| 新增点 | 在 `HTTP_log.txt` 中的承载方式 | 是否新增独立行 |
|---|---|---|
| M | 加入现有 client request/response JSON（`LogHTTP`），与 T1/T2/T5/T6 同一行 | 否 |
| G | 加入现有 server request/response JSON（`LogHTTPInbound`），与 T3/T4 同一行 | 否 |
| W | 独立的 `server_response_headers_flushed` JSON event | 是 |

本地 `x/net/http2` fork 只采集时间和传递 request/stream-scoped 状态，不直接打开或写日志文件。
所有最终文件写入必须继续经过 free5GC 现有 access-log 的 `enqueue(kindHTTP, ...)` 和
`writerLoop()` 单 writer goroutine；不得为三个新增点创建第二个直接写 `HTTP_log.txt` 的 writer。

### 0.2 每个完整 HTTP request/response 的九点必须 exact join

一个成功、HTTP/2、无 transport retry 且三类记录都完整的 request/response，由三条逻辑记录承载
九个时间点：

```text
client JSON : T1, M, T2, T5, T6
server JSON : G, T3, T4
W event     : W
```

在当前实验明确保证"同一种 NF 只有一个 Pod、每轮实验排空并清空所有 Pod 的 `HTTP_log.txt`"的
前提下，离线关联必须分两步执行：

```text
第一步  server JSON  <-> W event
        key = (dst, server_request_id)

第二步  client JSON  <-> 已合并的 server 记录
        key = (dst, conn, stream_id)
```

其中 `conn` 的规范化含义（第 17.3 节详述）：

```text
client 行的 conn = 该 TCP 连接在【客户端 Pod】看到的 LocalAddr  = "clientIP:clientPort"
server 行的 conn = 同一条 TCP 连接在【服务端 Pod】看到的 RemoteAddr = "clientIP:clientPort"
```

两者在无 SNAT 的 pod-to-pod 直连下是同一个字符串。这一点**必须在正式实验前实测验证**
（第 17.3 节的关联预检）；如果 CNI 做了源地址改写导致两侧不一致，必须启用第 17.3 节的
`sbi_request_id` 方案，不能回退到 UE/URI/时间顺序近似匹配。

`server_request_id` 只负责同一 server Pod 内的 T3/T4/G 与 W；`conn + stream_id` 负责跨
client/server 连接同一个 HTTP/2 transport attempt。

UE ID、method 和 URI 用于在 exact join 完成后标注"这是哪个 UE 的哪个 SBI request"，不能充当
关联主键。只有能从 client/server 行的 `ue_id` 或 URI 确定 UE 的交易才能声明属于某个 UE；
没有 UE 身份的 NRF discovery/heartbeat 等共享请求应单独归类，不能强行分配给某个 UE。

对连接失败、写失败、timeout、cancel、底层 W 缺失、日志队列 drop 或 transport retry 的样本，
不承诺九个时间值全部存在；必须保留错误/缺失状态并统计，不得伪造时间点。主分析只接受 exact
join 后三类记录各恰好一条、九点均存在且 `retry_count == 0` 的完整样本。

当前实验允许少量日志缺失。缺失记录只会降低完整样本数，不能改变剩余样本的匹配关系：离线分析
必须使用上述强关联 key 做 exact inner join，任何 missing/orphan 记录直接排除，禁止用相邻行、
UE/URI 或最近时间戳补配。实验前需要预先确定可接受的 `missing_rate`/drop-rate 上限；允许该值
非零，但 duplicate key、一个 key 命中多条记录和近似补配必须始终为 0。

### 0.3 打点不得阻塞或改变正常 HTTP/2 流程

"异步写日志"在本计划中的硬性含义是：被测 HTTP/2 热路径不得等待日志 channel、JSON 格式化、
文件写入或 flush。三个新增点遵守以下规则：

- M 热点只执行一次 `time.Now()` 和 request-scoped 原子写；
- G 热点只执行一次 `time.Now()` 和一次普通字段赋值（写在 `go` 语句之前，由 `go` 语句提供
  happens-before，见第 12.1 节 B）；`server_request_id` 只在创建 server request trace 时执行
  一次 atomic increment；
- W socket-write 热点对每次跨过一个或多个 marker 的底层 Write 只执行一次 `time.Now()`、
  小状态更新和非阻塞的固定大小事件投递；同一次 Write 命中的多个 W 共享时间戳，JSON 构造由
  外层长期存活的 collector goroutine 完成；
- M/G 只给现有 client/server JSON 增加字段，不增加新的日志行和额外的每 request 文件写操作；
- W 使用一条独立 JSON event，但不能在 socket writer 中构造 JSON、做文件 I/O、等待 channel 或
  启动 per-request goroutine；
- 所有事件通道必须有界且采用 non-blocking send；队列满时宁可原子计数并丢弃日志，也不能阻塞
  HTTP/2 goroutine；
- 不得为了获得 W 强制 Flush、改变 buffer 大小、frame batching、write scheduler、flow control、
  锁顺序或 handler 生命周期；
- 每轮实验结束时才执行"停止发流量 -> 等待在途 request/W -> drain W collector -> access-log
  Flush -> 采集 -> 清空日志"，不得按 request flush（顺序见第 16.3 节）。

异步写入只能消除日志 I/O 对请求的同步阻塞，不能声称绝对零开销；`time.Now()`、字段复制、
atomic、context node、后台 JSON/文件写入仍会消耗少量 CPU 和 allocation。这部分开销的量化
（尤其在 UE registration 触发间隔小于 1 ms 的最高目标负载下）**由实验负责人自行衡量**，
开销 A/B 不在本计划范围内（见第 19.1 节的范围变更说明）；但第 19.1 节的 clocksource 前置检查
与第 19.2 节的竞态/完整性验收必须完成。

---

## 1. 第一阶段目标：`reqHeaderMu` 竞争起点

第一阶段只增加一个新的时间点：

```text
req_header_mu_start_time
```

它表示：对于每一次向对端 NF 发出的 HTTP/2 request，在该 request **即将开始竞争当前连接的
`reqHeaderMu` 之前**记录时间。

这个时间点保持 request-attempt 粒度。时间戳保存在该 request 对应的 request-scoped trace 上，
之后与该 attempt 的 `stream_id` / connection identity / attempt 序号一起发布，再与现有客户端
日志合并输出。

## 2. 在现有六个时间点中的位置

```text
T1 client req_time
    ↓
req_header_mu_start_time      ← 第一阶段唯一新增的时间点
    ↓
T2 client wrote_time
    ↓
G server_handler_go_time
    ↓
T3 server req_time
    ↓
T4 server resp_time
    ↓
W server_response_headers_flushed_time
    ↓
T5 client got_first_byte_time
    ↓
T6 client resp_time
```

第一阶段的新增时间点位于客户端已经进入该 request 的 HTTP/2 发送流程、即将尝试获取
`reqHeaderMu` 的位置；必须放在实际加锁 `select` 之前，而不是成功取得锁之后。
G 和 W 分别在第 10 节和第 14 节定义。

### 2.1 原有六点在当前代码中的实际取时位置（已核对，v1 行号全部作废）

原有六个点本来就各自执行一次 `time.Now()`。七个已启用 access log 的 NF
（`amf`、`ausf`、`nrf`、`nssf`、`pcf`、`udm`、`udr`）的
`internal/accesslog/httptransport.go` 经 diff 验证**逐字节相同**，因此下表对七个 NF 同时成立：

| 点 | 文件 / 函数 | 实际语句 | 实际行号 |
|---|---|---|---|
| T1 | `NFs/<nf>/internal/accesslog/httptransport.go` `(*loggingRoundTripper).RoundTrip` | `reqTime := time.Now()`，紧接 `base.RoundTrip(req)` 之前 | **261** |
| T2 | 同一函数内 `httptrace.ClientTrace.WroteRequest` 闭包 | `wroteTime = time.Now()`（带 `IsZero()` 保护，只记第一次写） | **250-254** |
| T5 | 同一函数内 `httptrace.ClientTrace.GotFirstResponseByte` 闭包 | `gotFirstByte = time.Now()`（无保护，后到覆盖） | **255-257** |
| T6 | `(*loggingRoundTripper).RoundTrip` | `respTime := time.Now()`，`base.RoundTrip(req)` 返回后 | **263** |
| T3 | **同一文件** `InboundLogger()` 返回的 gin middleware | `reqTime := time.Now()`，在 `sniffInboundUEID` 与 `c.Next()` 之前 | **362** |
| T4 | 同一 middleware | `respTime := time.Now()`，`c.Next()` 返回后 | **370** |

另外两个已核实的细节，v1 没写但影响解释：

- **`GotConn` 闭包（244-249 行）** 已经在每个 request 上执行一次
  `info.Conn.LocalAddr().String()`。这是一次字符串 allocation，位于 `base.RoundTrip` 内部、
  T1 之后，属于 A 组既有成本。第 4.2 节建议把它改成读 fork 预计算好的不可变字符串；
  这属于基础设施改动（会同时让原六点变快），报告时应与 M/G/W 的新增成本分开陈述。
- **T3 之前还有两层 middleware**：`logger_util.NewGinWithLogrus(logger.GinLog)` 装的 gin
  logger + recovery，以及 `metrics.InboundMetrics()`（见各 NF `internal/sbi/server.go`
  的 `newRouter`，例如 `NFs/amf/internal/sbi/server.go:77-78`）。因此 `T3 - G` 天然包含
  这两层的入口开销，第 10.2 节已据此更新。

因此本计划的准确口径是：

```text
A：原有 T1/T2/T3/T4/T5/T6，共 6 次 time.Now()
B：保留原六点，再增加 M/G/W，共 9 个时间戳
```

"M/G/W 新增 `time.Now()`"表示相对于 A 组增加新的取时，不表示原六点不取时。M、G 各为每
request 一次 clock read；W 按每次跨过 marker 的底层 socket Write 取时，同一次 Write 跨过多个
marker 时共用一次 `time.Now()`。因此最常见/上界口径约为每 transaction 从 6 次增加到 9 次
（`+50%`）；**但在本部署中 W 的 clock read 实际会显著少于每 response 一次**——见第 14.5 节：
`writeResHeaders.staysWithinBuffer()` 恒为 `false`，真正的 `conn.Write` 由 `flushFrameWriter`
在 write scheduler 排空后统一触发，高负载下一次 Write 会跨过多个 stream 的 marker。

真实增量不只有 clock read，还包括下表所列状态传递、context node 和日志字段：

| 新点 | 相对原六点新增的同步热路径工作 | 日志输出增量 |
|---|---|---|
| M | 一次 `time.Now()`；一次 `atomic.Int64.Store`；`clientStream` 创建时一次 context 查找 | 在现有 client JSON 中增加 M 及关联字段，不增加日志行 |
| G | serve goroutine 上：一次指针判空 + 一次 `time.Now()` + 一次普通字段赋值；`newWriterAndRequestNoBody` 里一次 `atomic.Uint64.Add`。**无堆分配、无 context 查找**（trace 内联进 `stream`，`stream` 自己实现 `context.Context`，见 12.1 B） | 在现有 server JSON 中增加 G 及关联字段，不增加日志行 |
| W | `writeHeaderBlock` 一次指针赋值；`bufferedWriter.Write` 一次整数加法 + FIFO push；底层 Write 返回后命中时一次 `time.Now()` + 一次固定大小 non-blocking send | 每个完整 response 新增一条 W JSON event |

M/G 的新增 JSON 字段仍由当前 `LogHTTP`/`LogHTTPInbound` 同步构造后再 enqueue；因此它们没有
新增文件 writer 或日志行，但会增加少量时间格式化、append 和可能的 buffer 扩容
（现有初始容量 `LogHTTP` = 304 B、`LogHTTPInbound` = 256 B，必须按第 19.3 节第 8 条上调）。
W 的 JSON 在 collector goroutine 构造，但后台 CPU、allocation、GC 和第三条日志行仍是 B 相对 A
的系统级增量。

## 3. 可以计算的时间

```text
request_send_after_headermu_start_us
    = T2 - req_header_mu_start_time
```

它表示从 request 开始竞争 `reqHeaderMu`，到现有 T2（`WroteRequest`，该 request 已完成本地
写出）之间的总时间。

该区间可能包含（对照 `xnet/http2/transport.go:1424-1507` 的实际顺序）：

- 等待 `reqHeaderMu`（1424-1429 的 `select`）；
- `cc.mu` 加锁、`awaitOpenSlotForStreamLocked` 等待可用 HTTP/2 stream（1431-1440）；
- `cc.addStreamLocked(cs)` 分配 stream ID（1442）；
- `encodeAndWriteHeaders(req)`：HPACK 编码 + 竞争连接写锁 `wmu` + 写入 + flush（1466）；
- 有 body 时 `writeRequestBody`：DATA 组帧与 HTTP/2 flow-control 等待（1497）；
- 这段路径中的 goroutine 调度或抢占时间。

因此，`T2 - req_header_mu_start_time` **不是纯粹的 `reqHeaderMu` 锁等待时间**。它能用一个新增
时间点判断 latency 是否主要产生于"从开始竞争 `reqHeaderMu` 到 request 完成本地发送"的整段
客户端发送路径，但不能单独证明 `reqHeaderMu` 本身就是瓶颈。

还可以使用现有 T1 计算：

```text
before_headermu_start_us
    = req_header_mu_start_time - T1
```

从而把原有 `T2 - T1` 拆为：

```text
T2 - T1
    = (req_header_mu_start_time - T1)
    + (T2 - req_header_mu_start_time)
```

`before_headermu_start_us` 覆盖 `RoundTripOpt` 的 `connPool().GetClientConn`（可能触发 dial）、
`traceGotConn`（含现有那次 `LocalAddr().String()`）、`cc.roundTrip` 建 `clientStream`、
`go cs.doRequest` 的 goroutine 创建与首次调度，以及 `writeRequest` 开头的
extended-CONNECT 分支判断（SBI 流量恒为 false）。

## 4. 具体修改位置（客户端）

### 4.1 HTTP/2 客户端库内部：`xnet/http2/transport.go`

#### A. M 的取时位置（已核实）

```text
xnet/http2/transport.go
func (cs *clientStream) writeRequest(req *http.Request, streamf func(*clientStream)) (err error)
```

真实代码（1424-1429 行）：

```go
	select {
	case cc.reqHeaderMu <- struct{}{}:
	case <-cs.reqCancel:
		return errRequestCanceled
	case <-ctx.Done():
		return ctx.Err()
	}
```

M 必须紧挨该 `select` 之前记录，而不是放到成功进入 `case cc.reqHeaderMu <- ...` 之后：

```go
	// TYcustom M: req_header_mu_start_time
	if cs.instr != nil {
		cs.instr.mUnixNano.Store(time.Now().UnixNano())
	}
	select {
	case cc.reqHeaderMu <- struct{}{}:
	// 其余分支保持不变
	}
```

注意：1414-1423 行还有一个 extended-CONNECT 的 `select`（等待 `cc.seenSettingsChan`）。
它只在 `req.Method == "CONNECT" && req.Header.Get(":protocol") != ""` 时进入，SBI 流量永远
不会命中，因此 M 放在其后、`reqHeaderMu` 的 `select` 之前是正确且无歧义的。

该位置只允许一次 `time.Now()` 和一次原子写；不能做 JSON、字符串转换、channel 等待、
文件 I/O 或创建 goroutine。

#### B. `stream_id` 与 connection identity 的发布位置（已核实）

`cs.ID` 由 `cc.addStreamLocked(cs)`（`transport.go:1442`）赋值：
`cs.ID = cc.nextStreamID; cc.nextStreamID += 2`。紧随其后的 1447-1450 行是现有的：

```go
	if streamf != nil {
		streamf(cs)
	}
```

此时 `cs.ID` 和 `cs.cc` 均已确定，是发布本 attempt 关联信息的准确时机。

**但 `streamf` 在本部署中恒为 nil**：`(*ClientConn).RoundTrip` 的实现就是
`return cc.roundTrip(req, nil)`（`transport.go:1266-1268`），而 `RoundTripOpt` 调用的正是
`cc.RoundTrip(req)`（`transport.go:597`）。所以外层无论如何都拿不到 stream ID，
标准 `httptrace` 也不暴露它——**这正是必须 fork 的第二个理由**（第一个是 M）。

fork 的做法不是去填 `streamf`（那会改变一个公开 API 的语义），而是新增一个 fork-local 的
instrumentation trace，路径为 `request context -> clientStream.instr`。

#### C. `retry_count`（已核实的 retry 语义）

`transport.go:589` 的 `for retry := 0; ; retry++` 是真实的 attempt 循环。关键已核实事实：

- 重试路径是 `req, err = shouldRetryRequest(req, err)`（600 行）。
  `shouldRetryRequest`（665-695 行）在需要重置 body 时执行 `newReq := *req`——**浅拷贝**，
  `ctx` 字段被一并复制。因此 **同一个 trace 对象会被后续 attempt 复用**，不会因为 retry 而
  换成新 trace。
- 因此 attempt 计数必须放在 trace 上，由 `RoundTripOpt` 在每次 `cc.RoundTrip(req)` 之前
  `attempts.Add(1)`；`writeRequest` 记录的 M/stream/conn 属于"当前最新 attempt"。
- 由于 attempt 之间是串行的，且主分析只接受 `retry_count == 0`，这个模型足够。
  **但必须在 client 行输出 `retry_count`，且 `retry_count > 0` 的行一律排除，不得混用**。

#### D. fork 新增文件：`xnet/http2/instrument_client.go`

建议的最小 API（全部为 fork-local 新增，不改动任何既有导出符号的语义）：

```go
package http2

// ClientConnIdentity 是每条 ClientConn 的不可变身份，在 newClientConn 中计算一次。
// 之所以预计算：避免在每个 request 上再做一次 LocalAddr().String() 的 allocation。
type ClientConnIdentity struct {
	LocalAddr  string // "clientIP:clientPort" —— 与 server 侧 RemoteAddr 对齐的规范 key
	RemoteAddr string // 拨号目标，仅用于诊断，不参与 join
}

// ClientRequestTrace 挂在 request context 上，跨 attempt 复用。
// 各字段独立原子发布，零 per-request allocation。
type ClientRequestTrace struct {
	attempts  atomic.Uint32                      // RoundTripOpt 每次尝试前 +1
	mUnixNano atomic.Int64                       // M
	streamID  atomic.Uint32                      // cs.ID
	connID    atomic.Pointer[ClientConnIdentity] // 指向 cc 上的不可变对象，不新分配
}

func WithClientRequestTrace(ctx context.Context, t *ClientRequestTrace) context.Context
func (t *ClientRequestTrace) Attempts() uint32
func (t *ClientRequestTrace) ReqHeaderMuStart() time.Time // 零值表示未记录
func (t *ClientRequestTrace) StreamID() uint32            // 0 表示未分配
func (t *ClientRequestTrace) Conn() *ClientConnIdentity   // nil 表示未确定
```

改动点共五处，全部在 `xnet/http2/`：

1. `transport.go:782` 附近的 `newClientConn`：新增 `cc.identity = &ClientConnIdentity{...}`，
   在 `c != nil` 时由 `c.LocalAddr().String()` / `c.RemoteAddr().String()` 各算一次。
   （`transportTestHooks` 分支 `c` 为 nil，需容错。）
2. `transport.go:1270` 的 `(*ClientConn).roundTrip`：在已有的
   `trace: httptrace.ContextClientTrace(ctx)` 旁边加一行
   `instr: clientRequestTraceFromContext(ctx)`，存进 `clientStream.instr`。
   放这里而不是放在热点，是为了让 `writeRequest` 只做一次指针判空。
3. `transport.go:1424` 之前：记录 M（见上文 A）。
4. `transport.go:1447` 的 `streamf(cs)` 处：
   `if cs.instr != nil { cs.instr.streamID.Store(cs.ID); cs.instr.connID.Store(cc.identity) }`。
5. `transport.go:597` 的 `cc.RoundTrip(req)` 之前：
   `if tr := clientRequestTraceFromContext(req.Context()); tr != nil { tr.attempts.Add(1) }`。

**数据竞争必须走 atomic，不能用普通字段**，原因已核实：`cs.doRequest` 是在
`(*ClientConn).roundTrip` 里用 `go cs.doRequest(req, streamf)` 起的独立 goroutine，
而 `roundTrip` 的 `for/select` 在 `<-ctx.Done()` 与 `<-cs.reqCancel` 两个分支上会**在
`writeRequest` 还在跑的时候就返回**。外层 `loggingRoundTripper.RoundTrip` 随即读取 trace，
构成真实并发读写。`go test -race` 必须覆盖这条路径。

### 4.2 free5GC 客户端 access log

准确接入函数：

```text
NFs/<nf>/internal/accesslog/httptransport.go
func (l *loggingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error)   // 158 行
```

保持当前 T1（261 行）/T6（263 行）的位置不变，改写 259 行现有的那一行。

**必须把两个 trace 合并进同一次 `WithContext`，不能再调一次。**
`(*http.Request).WithContext` 的实现是 `r2 := new(Request); *r2 = *r; r2.ctx = ctx`——
每调用一次就**整体复制一份 `http.Request` 结构体**（含约 20 个字段的 header/URL/body 指针）
并多分配一个 Request 对象。写成两次 `WithContext` 会让每个出站 request 平白多一次
Request 复制 + 一次 Request 分配，这是纯浪费：

```go
	// 原来是一行 req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	// 改成：先把两个 trace 叠进同一条 ctx 链，只做一次 WithContext
	ctx := httptrace.WithClientTrace(req.Context(), trace)

	// TYcustom: fork-local trace, 承载 M / stream_id / conn identity / attempts
	instr := &http2.ClientRequestTrace{}
	ctx = http2.WithClientRequestTrace(ctx, instr)

	req = req.WithContext(ctx)   // 仍然只有一次 Request 复制，与 A 组相同

	reqTime := time.Now()       // 原 T1，不移动
	resp, err := base.RoundTrip(req)
	respTime := time.Now()      // 原 T6，不移动

	// 读取已经发布的快照；全部是 atomic load，不阻塞、不等待
	mStart := instr.ReqHeaderMuStart()
	streamID := instr.StreamID()
	retryCount := 0
	if n := instr.Attempts(); n > 0 {
		retryCount = int(n) - 1
	}

	LogHTTP(dst, method, uri, ueID, connID, connSlot, connReused,
		streamID, retryCount,
		reqTime, mStart, wroteTime, gotFirstByte, respTime)
	return resp, err
```

关于 `connID`：**保持现有 `GotConn` 闭包不变**（244-249 行），继续用
`info.Conn.LocalAddr().String()`。理由：

- 它已经是 A 组既有行为，不改就不引入与新增点无关的差异；
- 它与 fork 的 `instr.Conn().LocalAddr` 应当逐字符相同，正好可以作为**关联预检的自校验**：
  离线脚本比对二者，不一致即说明 attempt 与 GotConn 记录的连接不是同一条（retry 场景），
  该行直接判为 incomplete。

若认为这次 `LocalAddr().String()` 的开销值得省掉，可换成读 `instr.Conn().LocalAddr`
（fork 已预计算，零 allocation）——注意当前实现里 `ClientRequestTrace.Conn()` 尚无任何调用点，
即 fork 每请求 store 了一份 identity 却没人读；改用它可以顺带去掉这次 allocation。
这属于基础设施改动，报告时与 M/G/W 的新增成本分开陈述。

`retryCount` 与 `wroteTime` 的既有语义要一起读：`wroteTime` 有 `IsZero()` 保护，保留的是
**第一次**写完成的时间；而 `connID` 由 `GotConn` **后到覆盖**，保留的是最后一次。二者在
`retry_count > 0` 时指向不同 attempt——这是又一个必须只用 `retry_count == 0` 做主分析的理由。

七个 NF 的 `httptransport.go` 逐字节相同，**改一份、复制七份**即可；不需要修改业务 handler
或 OpenAPI 代码。所有 SBI 出站流量都经过这里，已核实：
`configuration.SetHTTPClient(accesslog.Client())` 在七个 NF 的每个 consumer service 里都调用了
（`grep -rn "accesslog.Client()" NFs/`）。

最终 JSON 的准确构造位置是 `NFs/<nf>/internal/accesslog/accesslog.go` 的 `func LogHTTP(...)`。
它当前在 T6 之后、`RoundTrip` wrapper 真正返回之前同步执行 `formatTime`、JSON append 和
`enqueue(kindHTTP, b)`。增加 M/stream/retry 字段不会增加日志行，但这些新增字段的格式化与
append 是 B 相对 A 的 latency 增量，而且发生在 T6 之后，**不能只用九点内部时间差发现**——
它只会体现在端到端 UE registration latency 上。

## 5. 是否需要 fork HTTP 库

需要，而且已经 fork 完成：`xnet/` = `golang.org/x/net v0.47.0` 的 pristine 副本
（dirhash `h1:Mx+4dIFzqraBXUugkia1OOvlD6LemFo1ALMHjrXDOhY=`，与七个 NF `go.sum` 中已记录的
完全一致，见 `xnet/FORK_PROVENANCE.md`）。

已核实的"为什么是这个模块而不是标准库"：

- **客户端**：`NFs/<nf>/internal/accesslog/httptransport.go:140-153` 直接把
  `&http2.Transport{...}`（本模块）装成 `http.RoundTripper`。`net/http` 自带的 HTTP/2
  从未被配置或触达。
- **服务端**：`NFs/<nf>/internal/sbi/server.go::newHttp2ServerWithIdleTimeout` 用
  `h2c.NewHandler(handler, &http2.Server{IdleTimeout: 500ms})`（本模块）。所有 NF
  `config/*.yaml` 都是 `scheme: http`，走 `ListenAndServe`（非 TLS），标准库的自动 HTTP/2
  不会启用。
- 现有 T2/T5 就是本模块调用的 `traceWroteRequest`（`transport.go:1507`）和
  `traceFirstResponseByte`（`transport.go:2289`），佐证链路正确。

修改范围限制为：

- `xnet/http2/` 下的 `transport.go`、`server.go`、`write.go`、`http2.go`，外加两个新增文件
  `instrument_client.go`、`instrument_server.go`；
- 各相关 NF 的 client/server access-log 包装层及日志字段；
- 七个 NF 的 `go.mod` `replace`（第 12.3 节）。

不需要 fork Go 标准库。三个阶段共用同一份 fork。

## 6. 建议日志形式（已按现有字段校订）

现有 client 行的真实字段集（`accesslog.go::LogHTTP`）是：

```json
{"src":"UDM","dst":"UDR","method":"GET","uri":"http://...","ue_id":"",
 "conn":"10.244.1.7:41236","conn_slot":0,"conn_reused":true,
 "req_time":"...","wrote_time":"...","got_first_byte":"...","resp_time":"...",
 "latency_us":1234}
```

加入 M/stream/retry 之后（**新增三个字段，其余全部保持原名原序，避免影响既有分析脚本**）：

```json
{"src":"UDM","dst":"UDR","method":"GET","uri":"http://...","ue_id":"",
 "conn":"10.244.1.7:41236","conn_slot":0,"conn_reused":true,
 "stream_id":17,"retry_count":0,
 "req_time":"...","req_header_mu_start_time":"...","wrote_time":"...",
 "got_first_byte":"...","resp_time":"...",
 "latency_us":1234}
```

**不要引入 `connection_id` 这个新名字**（v1 的写法）：现有字段就叫 `conn`，含义已经是
"localIP:localPort"，第 17.3 节的规范化 key 直接沿用它。server 行新增的连接字段也必须叫
`conn`，值取 `c.Request.RemoteAddr`，这样两侧同名同值，join 无需字段映射。

`req_header_mu_start_time` 用 `formatTimeOrEmpty`（零值输出 `""`），与 `wrote_time` /
`got_first_byte` 的既有约定一致。`stream_id` / `retry_count` 用 `appendKVInt`
（现有函数，输出为真正的 JSON number）。

这样仍然是一条 request 粒度日志，也避免额外日志行/I/O 扰动被测路径。

## 7. 实现约束

- 对每个 request attempt 只记录一次 M；
- 热路径中只执行取时和原子赋值，不进行同步日志输出；
- HTTP/2 写 goroutine 写入、外层 transport goroutine 读取该时间时，必须使用 atomic
  （原因见第 4.1 节 D 的竞态说明），不得用普通共享字段；
- client 行必须记录可观测的 `retry_count`（无 retry 为 0），第一轮分析只接受成功且
  `retry_count == 0` 的 request；若后续分析 retry，必须对每个 attempt 分别记录并增加
  `request_attempt_id`，该 ID 不是额外时间点；
- 继续使用 monotonic duration 计算内存中的耗时（现有 `latency_us` 用的是
  `respTime.Sub(reqTime)`，保留 monotonic clock）；序列化时间（RFC3339Nano UTC）仅用于离线关联。

## 8. 第一阶段验证与判断方法

实施后先验证每个成功 request 满足：

```text
T1 <= req_header_mu_start_time <= T2
(req_header_mu_start_time - T1) + (T2 - req_header_mu_start_time) ≈ T2 - T1
```

随后在不同 request rate 下比较两个区间的 p50、p95 和 p99：

- 如果 `T2 - req_header_mu_start_time` 随 request rate 明显上升，并解释了大部分 `T2 - T1` 的
  增长，说明主要等待发生在开始竞争 `reqHeaderMu` 之后的客户端发送路径；
- 如果 `req_header_mu_start_time - T1` 上升，而后半段稳定，说明 latency 更可能发生在到达该锁
  之前，例如 `GetClientConn`、`go cs.doRequest` 的 goroutine 创建/首次调度；
- 如果后半段上升，只能先定位到该发送路径，不能仅凭这一个时间点区分 `reqHeaderMu`、`wmu`、
  flow control 或 goroutine 调度。要证明某一个锁，需要下一阶段增加锁取得时间，或结合
  `LOCK_SCHED_PROFILING_GUIDE_0826.md` 里已经启用的 block/mutex profile。

## 9. 第一阶段边界

- 第一阶段不增加 server request 接收端日志；第二阶段只在现有 server request 日志中增加 G 字段；
- 第一、二阶段不增加 server response 发送端日志；第三阶段增加 W，见第 14 节；
- 不增加 client response read-loop 日志；
- 不增加单独的 `reqHeaderMu` 成功加锁时间；
- 不在本计划中实施源代码修改。

---

## 10. 第二阶段目标：服务端 handler 真正启动前的 G 点

第二阶段增加一个服务端时间点：

```text
server_handler_go_time = G
```

G 表示：服务端已经完成当前 request 的 HTTP/2 初始 HEADERS 处理，已经创建好 stream、
`*http.Request` 和 response writer，并且**即将真正执行 `go sc.runHandler(...)`、使新的 handler
goroutine 变为 runnable 之前**的时间。

记录 G 时，新的 handler goroutine 还没有创建。时间戳必须紧挨实际 `go` 语句并位于其前面。
这有两重必要性：

1. **语义**：放在 `go` 之后，新 goroutine 可能已先于父 goroutine 继续执行，测到的就不是
   "提交时刻"；
2. **内存模型**：`go` 语句本身提供 happens-before 边。把 G 写在 `go` 之前，handler goroutine
   里的 `InboundLogger` 读它就是 race-free 的，**因此 G 可以用普通字段赋值，不需要 atomic**。
   写在 `go` 之后则构成真实 data race。

### 10.1 G 在原有六点中的位置

```text
T1 client req_time
    ↓
req_header_mu_start_time
    ↓
T2 client wrote_time
    ↓
客户端内核 Send-Q / veth/CNI / 服务端 Recv-Q
    ↓
readFrames goroutine：顺序读 frame、CONTINUATION、HPACK 解码
    ↓
serve goroutine：processHeaders -> newStream -> newWriterAndRequest(NoBody)
                 创建 stream / *http.Request / responseWriter
    ↓
若 handler 配额不足（curHandlers >= advMaxStreams），进入 unstartedHandlers 等待
    ↓
G server_handler_go_time     ← 第二阶段新增点；实际 go sc.runHandler 之前
    ↓
创建 handler goroutine、等待 Go scheduler、开始运行 runHandler
    ↓
h2c/net-http -> gin logger+recovery middleware -> metrics.InboundMetrics()
    ↓
T3 server req_time            （InboundLogger 内，sniff 之前）
    ↓
T4 server resp_time           （c.Next() 返回后）
    ↓
W server_response_headers_flushed_time
    ↓
T5 client got_first_byte_time
    ↓
T6 client resp_time
```

当前 HTTP/2 server 每条连接实际上有一个 `readFrames` goroutine 和一个 `serve` goroutine。
二者不是同一个 goroutine，但 reader 每交出一帧后必须等待 serve 处理并允许继续
（`sc.readFrameCh` 是无缓冲 channel），因此在一条连接上构成逐帧串行的接收路径。

### 10.2 G 将 `T3 - T2` 拆成的两段

第一段：

```text
server_before_handler_go_us = G - T2
```

覆盖 handler 真正启动之前的路径：客户端内核发送队列、Pod 网络路径、服务端内核接收队列、
`readFrames` goroutine 唤醒与调度、按线序读取本 request 之前的 frame、初始
HEADERS/CONTINUATION 的读取与 HPACK 解码、frame 交给单连接 `serve`、`serve` 处理 HEADERS 并
创建 stream/`http.Request`/responseWriter、header 校验与 canonicalization、allocation/GC，
以及 handler 并发配额已满时在 `unstartedHandlers` 中的排队时间。

因此，`G - T2` 可以判断增长是否主要发生在 handler 创建之前，但它不是纯粹的
`readFrames + serve` 时间；内核/网络和 handler admission 等待也包含在其中。

第二段：

```text
server_after_handler_go_to_t3_us = T3 - G
```

覆盖：`go sc.runHandler(...)` 的 goroutine 创建、新 goroutine 等待 Go scheduler 分配 P/M、
`runHandler` 入口（两层 defer 注册）、进入 h2c/net-http handler、
**`logger_util.NewGinWithLogrus` 装的 gin logger + recovery middleware**、gin 路由匹配与
`*gin.Context` 准备、`metrics.InboundMetrics()` 前置代码，直到 `InboundLogger` 取得
method/URI 并记录 T3。

因此，`T3 - G` 可以判断增长是否发生在"提交 handler goroutine 之后、到达现有 T3 之前"，
但不是纯净的 Go scheduler latency。若以后必须严格得到纯调度时间，还需在新 handler goroutine
的第一条指令记录 E，并计算 `E - G`；本阶段先不增加 E。

### 10.3 handler admission 等待的归属（已核实）

`(*serverConn).scheduleHandler`（`server.go:2354`）只有在 `sc.curHandlers < sc.advMaxStreams`
时才立即 `go sc.runHandler(...)`；否则把 `streamID`/`rw`/`req`/`handler` 存进
`sc.unstartedHandlers`（2365-2370 行），此时 handler goroutine 尚不存在。
`(*serverConn).handlerDone`（`server.go:2374`）在已有 handler 结束后再取出并真正 `go`。

G 必须在两条实际启动路径中都记录：

1. `scheduleHandler` 的立即启动路径：`server.go:2359` 的 `go sc.runHandler(rw, req, handler)` 之前；
2. `handlerDone` 的排队后启动路径：`server.go:2389` 的 `go sc.runHandler(u.rw, u.req, u.handler)` 之前。

这样 `T3 - G` 不会混入 handler admission queue；admission 等待归入 `G - T2`。
`advMaxStreams = conf.MaxConcurrentStreams`（`server.go:449`），而 NF 的
`&http2.Server{IdleTimeout: ...}` 未设 `MaxConcurrentStreams`，`setConfigDefaults` 因此填入
`defaultMaxStreams = 250`（`server.go:60`）。预期正常实验并发远低于 250，但实现必须覆盖该路径。

**G 的覆盖范围是完整的**，已核实：`server.go` 里另外两处 `go sc.runHandler`——
`(*serverConn).upgradeRequest`（2140 行）和 `(*serverConn).startPush`（3235 行）——在本部署中
不可达：

- 客户端用 `AllowHTTP: true` + 自定义 `DialTLSContext` 直接拨明文 TCP
  （`httptransport.go:145-153`），即 prior-knowledge h2c，`ServeConnOpts.UpgradeRequest`
  恒为 nil，`upgradeRequest` 不会被调用；
- 没有任何 NF 调用 `Pusher.Push`，`startPush` 不会被调用。

因此不需要 v1 那句"这些少量请求如果没有 G，只作为 incomplete sample"的兜底——但保留检查：
若离线统计出现"server JSON 有 T3/T4 但 G 为空"，说明上述前提被破坏，必须查明原因而不是
当作正常缺失。

唯一真正可能没有 G 的 inbound 流量，是在 `processHeaders` 阶段就被拒绝、从未创建
`*http.Request` 的请求（malformed header、超出 `MaxHeaderListSize` 等）。这类请求**同样没有
server JSON**（`InboundLogger` 根本不会跑），所以不会产生"有 T3 无 G"的半条记录，对 join
无害。

## 11. G 的 request 粒度与日志形式

G 是严格的 request/HTTP2-stream 粒度。在 G 点已经有完整的 `*http.Request`、method/URI/headers、
当前 HTTP/2 `stream_id` 和 server-connection 状态。

G 不单独输出一条新日志，由该 request 已有的 `InboundLogger` 在现有 server request JSON 中
一起输出。现有 server 行的真实字段集（`accesslog.go::LogHTTPInbound`）是：

```json
{"src":"NaN","dst":"UDM","method":"GET","uri":"http://...","ue_id":"",
 "req_time":"...","resp_time":"...","latency_us":1234}
```

加入 G 与关联字段之后：

```json
{"src":"NaN","dst":"UDM","method":"GET","uri":"http://...","ue_id":"",
 "server_request_id":12345,"conn":"10.244.1.7:41236","stream_id":17,
 "server_handler_go_time":"...","req_time":"...","resp_time":"...",
 "latency_us":1234}
```

`req_time` 仍为现有 T3，`resp_time` 仍为 T4，字段语义不变。

三个新增关联字段的来源，注意**只有两个需要 fork**：

| 字段 | 来源 | 是否需要 fork |
|---|---|---|
| `conn` | `c.Request.RemoteAddr`，其值即 `sc.remoteAddrStr = c.RemoteAddr().String()`（`server.go:438`） | **否**，`InboundLogger` 直接可读 |
| `stream_id` | fork 的 server trace（`*http.Request` 不暴露 HTTP/2 stream ID） | 是 |
| `server_request_id` | fork 的 server trace，进程内 `atomic.Uint64` | 是 |

`server_request_id` 是接收方 NF 为当前 inbound HTTP request 分配的 server-local ID，专门用于把
这条 G/T3/T4 server JSON 与随后独立输出的 W event 严格合并。

G 与 T3 通过同一个 `*http.Request.Context()` 传递，天然一一对应。为了让客户端与服务端在高并发
下也能严格一一配对，必须同时输出 `conn` 与 `stream_id`——这不是额外时间点。只靠 UE、method、
URI 和时间顺序，在相同 request 并发且 handler/response 完成顺序重排时会配错。完整关联要求见
第 17 节。

## 12. 第二阶段需要修改的位置与代码

### 12.1 本地 fork `xnet/http2/server.go`

#### A. 问题：G 会把工作放到 serve goroutine 上（原六点从未碰过这里）

先说清楚**问题是什么**。原有 T3/T4 都在 **handler goroutine**（每请求一个，天然并行）；
G 必须在 `go sc.runHandler(...)` 之前取时，也就是在 **serve goroutine** 上——每条连接**只有一个**。

而 `connsPerPeer = 1` 把这件事放大了：

```text
UDM ──(唯一一条 TCP 连接)──> UDR
                              └─ 唯一一个 serve goroutine
                                 2000 reg/s 时串行处理约 18000 req/s
```

serve goroutine 是串行资源，**宿主机 CPU 没用满并不代表它没满**（一个 goroutine 最多用满
一个核）。这正是此前 `C6525100g_TrueTR_0721_20ms` 的结论——时延爆炸发生在 CPU 有余量时，
原因是序列化与排队，不是算力不够。所以这里加的每一点工作都要斤斤计较。

按本计划的朴素写法，会往 serve goroutine 上加**四**样东西：

| # | 加了什么 | 能否避免 |
|---|---|---|
| 1 | `&ServerRequestTrace{}` 堆分配 | **能**（内联进 `stream`） |
| 2 | `context.WithValue` 节点堆分配 | **能**（让 `stream` 自己实现 `context.Context`） |
| 3 | `scheduleHandler` 里一次 context 链查找 | **能**（沿调用链传指针） |
| 4 | 一次 `time.Now()` | 不能——**这就是被测量本身** |

下面的设计把 1/2/3 全部消掉，serve goroutine 上只剩第 4 项。

#### B. 设计：trace 内联进 `stream`，`stream` 自己当 context（零额外分配）

新增文件 `xnet/http2/instrument_server.go`：

```go
package http2

// serverRequestIDCounter 是 NF 进程级单调计数器。
var serverRequestIDCounter atomic.Uint64

// ServerRequestTrace 的生命周期与一个 inbound HTTP/2 request 一致。
//
// 可变性规约（违反即 data race）：
//   - ID / ConnID / StreamID 创建时写入，之后【只读】；
//   - HandlerGo 由 serve goroutine 在 `go sc.runHandler` 之前写一次，
//     由 handler goroutine 读，happens-before 由 go 语句提供；
//   - W 回调只读前三个字段，【绝不】读 HandlerGo（frame-writer goroutine
//     对它没有 happens-before 保障）。
type ServerRequestTrace struct {
	ID        uint64
	ConnID    string // == sc.remoteAddrStr，每条连接一份不可变字符串
	StreamID  uint32
	HandlerGo time.Time // G
}

type serverTraceKey struct{}

// ServerRequestTraceFromContext 供 free5GC access-log 从 Request.Context() 读取。
func ServerRequestTraceFromContext(ctx context.Context) *ServerRequestTrace {
	tr, _ := ctx.Value(serverTraceKey{}).(*ServerRequestTrace)
	return tr
}
```

**改动 1：`stream` 内联 trace（省掉分配 #1）**

`stream` 本来就在 `newStream`（`server.go:2192`）里分配，把 trace 作为**值字段**塞进去，
不产生任何新对象：

```go
 type stream struct {
 	sc        *serverConn
 	id        uint32
 	...
+	trace     ServerRequestTrace // TYcustom：值字段，不是指针，零额外分配
 }
```

**改动 2：`stream` 自己实现 `context.Context`（省掉分配 #2）**

已核实 `*stream` 现有方法只有 `isPushed / endStream / copyTrailersToHandlerRequest /
onReadTimeout / onWriteTimeout / processTrailerHeaders`，**与 `context.Context` 的四个方法
没有任何冲突**，可以直接实现：

```go
// stream 转发到自己已有的 st.ctx（newStream 里 context.WithCancel 的产物），
// 只在 Value 上多认一个 key。这样 req.WithContext(st) 不需要额外的 valueCtx 节点。
func (st *stream) Deadline() (time.Time, bool) { return st.ctx.Deadline() }
func (st *stream) Done() <-chan struct{}       { return st.ctx.Done() }
func (st *stream) Err() error                  { return st.ctx.Err() }
func (st *stream) Value(k any) any {
	if k == (serverTraceKey{}) {
		return &st.trace
	}
	return st.ctx.Value(k)
}
```

然后 `newWriterAndRequestNoBody`（`server.go:2297`）里：

```go
 	tr := &st.trace                       // 指向内联字段，不分配
 	tr.ID = serverRequestIDCounter.Add(1)
 	tr.ConnID = sc.remoteAddrStr
 	tr.StreamID = st.id
 	req := (&http.Request{ /* 原字段不变 */ }).
-		WithContext(st.ctx)
+		WithContext(st)                   // TYcustom：st 即 context，零额外分配
```

**安全性检查（已核实）**：七个 NF 的服务端代码里**没有任何一处**包装或替换 inbound
request 的 context（`grep -rn "\.WithContext(" NFs/*/internal/sbi/ NFs/*/internal/accesslog/`
只命中 client 侧的 `httptransport.go:259`）。所以 `c.Request.Context()` 就是 `st` 本身，
`Value()` 一步命中。即使将来有人包了一层，`Value()` 沿链向上仍然找得到，只是多走一跳。

**改动 3：沿调用链传指针，不在 serve goroutine 上查 context（省掉查找 #3）**

```go
 type unstartedHandler struct {
 	streamID uint32
 	rw       *responseWriter
 	req      *http.Request
 	handler  func(http.ResponseWriter, *http.Request)
+	trace    *ServerRequestTrace // TYcustom
 }

 func (sc *serverConn) scheduleHandler(streamID uint32, rw *responseWriter,
-	req *http.Request, handler func(http.ResponseWriter, *http.Request)) error {
+	req *http.Request, handler func(http.ResponseWriter, *http.Request),
+	trace *ServerRequestTrace) error {
 	sc.serveG.check()
 	maxHandlers := sc.advMaxStreams
 	if sc.curHandlers < maxHandlers {
 		sc.curHandlers++
+		if trace != nil {
+			trace.HandlerGo = time.Now() // G —— go 语句提供 happens-before，普通赋值即可
+		}
 		go sc.runHandler(rw, req, handler)
 		return nil
 	}
```

调用点在 `processHeaders` 里，手上就有 `st`，直接传 `&st.trace` 即可。

**改动 4：正确性核验——把 `st` 当 context 传下去之后，从它派生子 context 仍然正常**

这是这个设计最容易被质疑的一点，必须先说清楚。Go 的 `context.WithCancel(parent)` 内部会调
`parentCancelCtx(parent)`，逻辑是：

```go
p, ok := parent.Value(&cancelCtxKey).(*cancelCtx)
if !ok { return nil, false }                 // 退化成起一个 goroutine 监听，能用但更贵
pdone, _ := p.done.Load().(chan struct{})
if pdone != parent.Done() { return nil, false }
return p, true                               // 走高效路径，直接挂到父 cancelCtx 上
```

我们的实现天然满足这两个条件：

- `st.Value(&cancelCtxKey)` 会落到 `default` 分支转发给 `st.ctx.Value(...)`，
  而 `st.ctx` 就是 `newStream` 里 `context.WithCancel(sc.baseCtx)` 产生的 `*cancelCtx`，
  所以第一个条件成立；
- `st.Done()` 直接返回 `st.ctx.Done()`，与上一步取到的 `p.done` 是**同一个 channel**，
  第二个条件也成立。

因此任何人（openapi、mongo driver、业务代码）从 `c.Request.Context()` 派生子 context
都会走高效路径，行为与改动前完全一致，不会退化成"额外起一个 goroutine 监听父 Done"。

另外三点也已核对：

- `baseCtx` 里的 `http.LocalAddrContextKey` / `http.ServerContextKey` 仍可通过
  `st.Value -> st.ctx.Value -> baseCtx` 取到，取值路径只多一跳；
- `req.WithContext(ctx)` 只在 `ctx == nil` 时 panic，`st` 永不为 nil；
- `runHandler` 的 `defer` 调的是 `rw.rws.stream.cancelCtx()`，与 `st.ctx` 一一对应，不受影响。

**最终 serve goroutine 上的净增量：一次指针判空 + 一次 `time.Now()` + 一次赋值。**
没有分配、没有 context 查找、没有 atomic（`serverRequestIDCounter.Add` 在
`newWriterAndRequestNoBody` 里，也在 serve goroutine 上——若 profile 显示这个进程级共享
计数器有 cache line 争用，按第 19.5 节 R2 改成 `sc` 上的每连接计数器，serve 单线程连
atomic 都不需要）。

#### C. 在排队后真正启动 handler 的路径记录 G

`server.go:2374` 的 `(*serverConn).handlerDone`，改动在 2388-2389 行之间。
trace 从 `unstartedHandler` 结构体里直接取（B 节已给它加了 `trace` 字段），
**同样不查 context**：

```go
 		sc.curHandlers++
+		if u.trace != nil {
+			u.trace.HandlerGo = time.Now() // G
+		}
 		go sc.runHandler(u.rw, u.req, u.handler)
```

不能在 request 第一次进入 `unstartedHandlers`（2365 行）时写 G，否则 handler admission
的排队等待会被错误算进 `T3 - G`（而它应该属于 `G - T2`）。

#### D. `InboundLogger` 侧（handler goroutine，不是串行资源，无需苛求）

读取走 `ServerRequestTraceFromContext(c.Request.Context())` 即可——它跑在每请求独立的
handler goroutine 上，不占用任何串行资源，一次 `Value()` 命中的成本可以忽略。
不需要为它做 B 节那样的优化。

### 12.2 free5GC server access log

修改所有七个启用了 HTTP access log 的 NF：

```text
NFs/<nf>/internal/accesslog/httptransport.go   （七份逐字节相同，改一份复制七份）
NFs/<nf>/internal/accesslog/accesslog.go       （七份仅 srcNF 与 AMF 专有的 NGAP/Worker sink 不同）
```

`httptransport.go::InboundLogger()`（353 行）的准确记录顺序：

```go
	return func(c *gin.Context) {
		method := c.Request.Method
		uri := inboundURI(c.Request)

		reqTime := time.Now()                    // 原 T3（362 行），不移动
		ueID := sniffInboundUEID(c.Request)

		c.Next()
		respTime := time.Now()                   // 原 T4（370 行），不移动

		// TYcustom：T4 之后再读，保证 T3/T4 的位置与语义完全不变
		connID := c.Request.RemoteAddr           // == sc.remoteAddrStr，无需 fork
		var srvReqID uint64
		var streamID uint32
		var handlerGo time.Time
		if tr := http2.ServerRequestTraceFromContext(c.Request.Context()); tr != nil {
			srvReqID, streamID, handlerGo = tr.ID, tr.StreamID, tr.HandlerGo
		}

		LogHTTPInbound(method, uri, ueID, connID, srvReqID, streamID,
			handlerGo, reqTime, respTime)
	}
```

HTTP/1 或 trace 不存在时使用零值（`server_request_id: 0`、`stream_id: 0`、
`server_handler_go_time: ""`），不改变请求行为；离线分析把这类行判为 incomplete。

`accesslog.go` 中的修改：

1. `LogHTTPInbound` 增加 `connID string, serverRequestID uint64, streamID uint32,
   handlerGo time.Time` 参数；
2. 输出 `conn`（`appendKV`）、`server_request_id`（需要新增一个 `appendKVUint64`，
   现有只有 `appendKVInt`）、`stream_id`（`appendKVInt`）、
   `server_handler_go_time`（`formatTimeOrEmpty`）；
3. 初始 buffer 容量从 256 上调（见第 19.3 节第 8 条）；
4. 继续通过现有 `enqueue(kindHTTP, b)` 只输出一条 server request 日志。

不修改 Gin 业务 handler、processor、OpenAPI model 或数据库代码。
`InboundLogger` 在七个 NF 中都已注册在 `metrics.InboundMetrics()` 之后（已核实：
`grep -rn "accesslog.InboundLogger()" NFs/` 共七处，均在各自 `newRouter` 内）。

因此 G 的取时发生在 `go sc.runHandler` 之前；G 的 JSON 格式化则发生在 T4 之后、gin
middleware 返回及 `rw.handlerDone()` 触发的自动 flush 之前。**这部分新增格式化会直接落进
`W - T4`，也会直接推迟对端看到的 T5/T6**——这是三个点里对端到端 latency 影响最直接的一项，
解释 `W - T4` 时必须记住它包含我们自己的日志构造。

### 12.3 各相关 NF 的 module 配置

在七个 NF 的 `go.mod` 中加入：

```text
replace golang.org/x/net => ../../xnet
```

已核实：`amf`、`ausf`、`nrf`、`nssf`、`pcf`、`udm`、`udr` 七个 module 都**直接** require
`golang.org/x/net v0.47.0`，与 fork 版本一致，因此 build list 不变，A/B 只测到 instrumentation
本身。加完后在各 module 执行 `go mod tidy`；`tidy` 可能给 `udm`/`amf`/`ausf`/`nssf` 的
`go.sum` 补上 `golang.org/x/term` 条目，属预期（filesystem `replace` 会重读 fork 的 `go.mod`）。

**不要**给 `webconsole` 加 replace：它是 `v0.48.0` 且只是间接依赖，也不在 SBI 测量路径上。
其余未插桩的 NF（`smf`/`chf`/`nef`/`bsf`/`n3iwf`/`tngf`/`upf`）保持从 proxy 取 `v0.47.0`，
互不影响。

仓库内没有 `go.work`，也没有 `vendor/` 目录，`replace` 会直接生效。

### 12.4 离线分析脚本

1. 读取 server 记录中的 `server_handler_go_time`；
2. 将客户端 T2 和服务端 G/T3 按第 17.3 节的 key 关联；
3. 计算 `G - T2` 与 `T3 - G`；
4. 对不同 request rate 分别比较两段的 p50、p95、p99；
5. 验证两段之和约等于原有 `T3 - T2`。

## 13. 第二阶段验证与解释边界

对于成功、无 retry、无 request body 的 HTTP/2 request，验证：

```text
T2 <= G <= T3
(G - T2) + (T3 - G) ≈ T3 - T2
```

解释结果时：

- `G - T2` 随 request rate 上升、`T3 - G` 稳定：增长主要发生在 handler 真正启动之前；
  再结合 Send-Q/Recv-Q、CPU throttling 和 profile 判断是内核网络还是 `readFrames/serve`；
- `T3 - G` 随 request rate 上升：增长发生在 handler goroutine 被提交之后；可能是 Go scheduler，
  也可能是 T3 前的 gin logger/recovery + metrics 开销；
- 两段同时上升：服务端连接级前置路径与进程级 CPU/调度都可能处于饱和状态。

两个重要前提：

1. T2 在调用方 Pod、G/T3 在被调方 Pod。若 Pod 位于同一节点，通常共享同一 host clock；
   跨节点时必须验证时钟偏移，不能直接把偏移解释为 latency。
2. 对带 request body 的请求，T2 `WroteRequest` 在完整 body 写完后发生
   （`transport.go:1507`，位于 `writeRequestBody` 之后），而服务端只需收到 HEADERS 就可能启动
   handler，因此 **G/T3 可能早于 T2**。第二阶段首先分析无 body request；若需要覆盖
   POST/body request，应改用客户端 `WroteHeaders`（fork 已在 `transport.go` 调用
   `traceWroteHeaders`）作为请求发送阶段的起点。
   注意 SBI 路径上 POST 很常见（`/nausf-auth/v1/ue-authentications`、
   `/npcf-am-policy-control/v1/policies`、NRF register 等），**这不是边缘情况**。

---

## 14. 第三阶段目标：response HEADERS 交给服务端内核的 W 点

第三阶段只增加一个 response-level 时间点：

```text
server_response_headers_flushed_time = W
```

W 的严格语义是：

> 对当前 HTTP/2 stream 的第一组最终 response HEADERS（排除 1xx 和 trailer），当完整
> HEADERS/CONTINUATION header block 的最后一个字节已被底层 `net.Conn.Write` 接受时，
> 立即记录时间。

当前 NF 使用 cleartext h2c，所以这个点可以实用地理解为"完整 response header block 已交给
服务端内核 TCP 发送缓冲区"。`Write` 返回是可实现的用户态边界，不表示对端已收到数据。

### 14.1 W 在现有时间线中的具体位置（已按真实调用链校订）

```text
T4 server resp_time                    （InboundLogger 的 c.Next() 返回后）
    ↓
LogHTTPInbound 同步构造 JSON + enqueue（异步队列，不含文件 I/O）
    ↓
gin middleware 链返回，metrics middleware 收尾，handler(rw, req) 返回
    ↓
runHandler 的 defer 调用 rw.handlerDone()            server.go:2422 / 3046
    ↓
rws.bw.Flush()  —— 每 request 的 4 KiB responseWriter buffer   server.go:3049 -> 2883
    ↓
chunkWriter.Write -> (*responseWriterState).writeChunk         server.go:2660
    ↓
构造 writeResHeaders 并调用 rws.conn.writeHeaders(...)          server.go:2717
    ↓
writeFrameFromHandler -> wantWriteFrameCh (cap 8)              server.go:2439-2452
    ↓
serve goroutine：writeSched.Pop() -> startFrameWrite            server.go:1445-1447 / 1291
    ↓
writeResHeaders.staysWithinBuffer() == false  =>  go sc.writeFrameAsync(wr, nil)
    ↓
writeResHeaders.writeFrame -> HPACK 编码 -> splitHeaderBlock -> writeHeaderBlock
    ↓
Framer.WriteHeaders/WriteContinuation -> endWrite -> sc.bw.Write(wbuf)
    （此时仍在 4 KiB 连接级 bufio 中，尚未触达内核）
    ↓
serve 排空 writeSched 后：needsFrameFlush -> flushFrameWriter   server.go:1448-1451
    ↓
ctx.Flush() -> sc.bw.Flush() -> bufio.Flush
    -> (*bufferedWriterTimeoutWriter).Write -> writeWithByteTimeout -> conn.Write
    ↓
W server_response_headers_flushed_time        ← 在这里取时
    ↓
服务端 Send-Q / veth/CNI / TCP / 客户端 Recv-Q
    ↓
客户端单 readLoop 调度、前序 frame、完整 HEADERS/CONTINUATION 读取与 HPACK 解码
    ↓
T5 client got_first_byte_time                 transport.go:2289
```

对普通、小型、非 streaming 且没有显式 Flush 的 SBI response，预期 `T4 <= W <= T5`。
精确条件见第 14.6 节。

### 14.2 `W - T4` 代表什么

```text
server_response_send_path_us = W - T4
```

按上面的真实链路，它包含：

- **`LogHTTPInbound` 的 JSON 构造与 enqueue**（这是我们自己的 instrumentation 成本，B 组会比
  A 组更大——解释 `W - T4` 时必须报告这一项，不能当成被测系统的行为）；
- 外层 metrics middleware 与 gin 链的收尾、`handler` 返回、`runHandler` 两层 defer；
- 小 response 从 request-local 4 KiB buffer 提交出来（`rws.bw.Flush()`）；
- 向 `wantWriteFrameCh`（容量 8）提交和可能的通道等待；
- 单连接 `serve` 及 round-robin write scheduler 中的前序/control/其他 stream frame 等待；
- HPACK 编码、HEADERS/CONTINUATION 组帧；
- `writeFrameAsync` goroutine 的创建与调度；
- **等待 write scheduler 完全排空后才发生的 `flushFrameWriter`**；
- 用户态连接 buffer 以及底层 socket write/Send-Q backpressure。

因此，`W - T4` 上升可以先定位到服务端 response 发送路径，但不能仅凭 W 证明一定是 `serve`
goroutine 本身。第 14.5 节说明这里最可能出现的具体机制。

### 14.3 `T5 - W` 代表什么

```text
response_after_server_write_us = T5 - W
```

代表 response header block 完成服务端底层写入之后，到现有 T5 的广义网络+客户端接收路径：
服务端内核 Send-Q、veth/CNI/TCP 路径与客户端内核 Recv-Q、客户端单 `readLoop` 被唤醒并获得
调度、按线序处理当前 response 之前的 frame、当前 HEADERS/CONTINUATION 的完整读取与合并与
HPACK 解码，直到 `processHeaders` 中调用 `traceFirstResponseByte`。

已核实：T5 确实在**完整 `MetaHeadersFrame` 组装并 HPACK 解码之后**才触发
（`transport.go:2283-2291`，上游注释里那个 "TODO: move first response byte earlier" 就是这个
意思）。所以 `T5 - W` **不是"一个字节的纯 TCP 传输时间"，也不是纯 `readLoop` 排队时间**。
若该段上升，需结合 Send-Q/Recv-Q、eBPF/抓包和 runtime trace 进一步区分网络与 client `readLoop`。

### 14.4 W 是 response HEADERS 边界，不是整个 body 边界

W 只保证第一组最终 response HEADERS/CONTINUATION 已交给底层连接，不保证整个 response
DATA/body 已写入内核。小 response 的 HEADERS 和 DATA 通常确实被合并在同一次底层 Write 中
（二者都在同一轮 `flushFrameWriter` 之前进入 `sc.bw`），但不应依赖这一实现细节来改变 W 的定义。

如果要研究整个 response body 发送完成，需要另外定义 final DATA/END_STREAM 时间点；它不能与
表示首个 response header 就绪的 T5 直接配对，所以本计划不增加该点。

### 14.5 【新增，v1 缺失】W 在本部署中一定会看到批量写

这不是边缘情况，而是本部署的**常态**，必须在设计和解释里正面处理：

1. `writeResHeaders.staysWithinBuffer(max)` **恒返回 `false`**（`write.go:209-218`，上游有
   注释说明"算长度比省下的 ~2µs 还贵"）。因此每个 response 的 HEADERS 帧**总是**走
   `go sc.writeFrameAsync(wr, nil)` 异步分支（`server.go:1338-1339`），不会走
   serve goroutine 的同步 fast path。
2. `flushFrameWriter` 的 `staysWithinBuffer` 也恒为 `false`（`write.go:72`），且它只在
   `sc.writeSched.Pop()` **取不出任何 frame 之后**才被调度（`server.go:1444-1451`）。
   也就是说：**write scheduler 里还有别的 stream 的 frame 时，本 stream 的 HEADERS 只会停在
   4 KiB 的 `sc.bw` 里，不会触达内核。**
3. 因此高负载下一次 `conn.Write` 会一次性把多个 stream 的 HEADERS+DATA 推给内核，
   **一次 Write 跨过多个 W marker**，这些 marker 共享同一个时间戳。

这正是本实验想看的现象之一（与 open5gs SCP 的 poll-cycle batching 同构），所以：

- marker 设计必须原生支持"一次 Write 跨多个 marker"（第 15.1 节 C 已如此设计）；
- 离线分析应额外输出**每次 Write 命中的 marker 数分布**，作为 batching 强度的直接度量。
  建议 W event 增加一个 `batch_size` 字段（同一次 Write 结算出的 marker 总数），
  它不是时间点，成本是 collector 侧一个整数。

### 14.6 【新增，v1 缺失】`T4 <= W` 的精确判定阈值

已核实的机制：`rws.bw` 是 `bufio.NewWriterSize(chunkWriter{rws}, handlerChunkWriteSize)`，
`handlerChunkWriteSize = 4 << 10`（`server.go:59, 78`）。

- **response body < 4 KiB**：handler 内部的写全部留在 `rws.bw`，第一次 `chunkWriter.Write`
  发生在 `rw.handlerDone()`（`server.go:3046-3049`）——**在 `handler(rw, req)` 返回之后**，
  也就是在 T4 与 `LogHTTPInbound` 之后。此时 `T4 < W` 成立。
- **response body >= 4 KiB**：`rws.bw` 在 handler 执行期间就会自动 flush，触发
  `writeChunk` -> `writeHeaders`，HEADERS 在 `c.Next()` 返回之前就已提交 —— **`W < T4`**。
- handler 显式调用 `Flush()`（`http.Flusher`）也会提前，但 free5GC 的 gin handler 不这么做。

因此第 18 节的分组标准是可判定的：按 response body 长度以 4 KiB 分组，不要用"大 response"
这种模糊说法。建议在 W event 中带上 `resp_bytes`（`rws.sentContentLen` 或已写字节数），
由 collector 侧输出。

### 14.7 【新增，v1 缺失】`sc.writeHeaders` 会阻塞 handler goroutine

已核实：`(*serverConn).writeHeaders`（`server.go:2439`）第一行是 `sc.serveG.checkNotOn()`，
即它运行在 **handler goroutine** 上；当 `headerData.h != nil`（最终 response 恒成立，
`h: rws.snapHeader`）时它会申请 `errc` 并在提交 frame 之后 `select { case err := <-errc: }`
**同步等待该帧写完**。

两个必须知道的推论：

1. handler goroutine 直到 HEADERS 帧写入 `sc.bw` 完成才返回，因此 `runHandler` 的结束时间
   ≈ HEADERS 进入连接 buffer 的时间（但**不是** W，W 还要等 `flushFrameWriter`）。
2. 因为已有这条同步等待，**任何在 W 路径上引入的额外阻塞都会直接反压 handler goroutine**。
   这是第 0.3 节"W socket writer 必须 non-blocking send"的硬性理由，不是保守估计。

---

## 15. W 需要修改的位置与实现约束（已按真实代码重写）

### 15.1 修改同一份本地 fork `xnet/http2/`

#### A. `server.go`：W event sink 的注入路径

1. 给 `http2.Server` 增加一个可选导出字段（nil 表示关闭 instrumentation）：

```go
// ResponseHeadersFlushedEvent 是固定大小、无需分配的事件。
// ConnID 指向每条连接的不可变字符串，不产生新的 allocation。
type ResponseHeadersFlushedEvent struct {
	ServerRequestID uint64
	ConnID          string
	StreamID        uint32
	At              time.Time
	BatchSize       uint16 // 本次底层 Write 一并结算的 marker 数，见 14.5
	Err             bool   // 底层 Write 返回错误
}

type Server struct {
	// ...既有字段...

	// ResponseHeadersFlushed 若非 nil，每个最终 response HEADERS block 的末字节被
	// net.Conn.Write 接受后投递一个事件。发送为 non-blocking，满则丢弃并计数。
	ResponseHeadersFlushed chan<- ResponseHeadersFlushedEvent
}

// ResponseHeadersFlushedDrops 返回因队列满而丢弃的 W 事件数。
// 包级计数器：每个 NF 进程只有一个 http2.Server，所以进程级即 NF 级。
func ResponseHeadersFlushedDrops() uint64
```

上面是**默认实现：每个 marker 一个事件**。第 19.0 节类别二给出了"一次 Write 的多个 marker
合成一个定长事件"的批量变体——那是在 profile 看到 chansend 竞争之后才切换的优化，
不是起手就做。两者的 JSON 输出格式相同。

2. `server.go:439` 的 `bw: newBufferedWriter(c, conf.WriteByteTimeout)` 改为把 sink 和
   连接身份一起传进去（`c.RemoteAddr().String()` 在同一个结构体字面量的 438 行已经算过一次，
   提取成局部变量复用，不要算第二次）：

```go
	remoteAddrStr := c.RemoteAddr().String()
	sc := &serverConn{
		// ...
		remoteAddrStr: remoteAddrStr,
		bw:            newBufferedWriter(c, conf.WriteByteTimeout, s.ResponseHeadersFlushed, remoteAddrStr),
		// ...
	}
```

3. 在七个 NF 的 `internal/sbi/server.go::newHttp2ServerWithIdleTimeout` 中注入 sink：

```go
	h2Server := &http2.Server{
		IdleTimeout:            idleTimeoutPeriod,
		ResponseHeadersFlushed: accesslog.WEventSink(), // TYcustom
	}
```

**这一步不做的话，fork 里没有消费者，最终会得到 0 条 W。** 该构造器目前只设置了
`IdleTimeout`（已核实，七个 NF 相同）。`h2c.NewHandler(handler, h2Server)` 用的就是这同一个
`*http2.Server`，sink 会随之生效。

4. x/net 必须自己执行带 `default` 的 non-blocking send。**用 channel，不要用 callback**：
   唯一的 socket writer 上不能调用语义不受控、可能阻塞或 panic 的任意业务函数。

#### B. `write.go`：在最终 header fragment 写入前挂 pending marker

准确函数是 `write.go:248` 的
`func (w *writeResHeaders) writeHeaderBlock(ctx writeContext, frag []byte, firstFrag, lastFrag bool) error`。
改为：

```go
	if lastFrag && w.trace != nil {
		// 必须在 Framer write 之前挂：bufio.Writer.Write 可能在该调用内部
		// 自动触发底层 conn.Write。
		ctx.armResponseHeaderMarker(w.trace)
	}
	var err error
	if firstFrag {
		err = ctx.Framer().WriteHeaders(...)    // 原逻辑不变
	} else {
		err = ctx.Framer().WriteContinuation(...) // 原逻辑不变
	}
	if err != nil {
		// 【必须】Framer 可能在触达 bufferedWriter.Write 之前就返回错误
		// （WriteHeaders 的 errStreamID 前置检查、endWrite 的 ErrFrameTooLarge），
		// 此时 pending 没有被消费。不清掉的话，它会挂到【下一帧】上，
		// 导致把别的 stream 的字节偏移当成本 stream 的 W —— 一个静默的错配。
		ctx.armResponseHeaderMarker(nil)
	}
	return err
```

**这条清理是必需的，不是防御性代码。** `armResponseHeaderMarker` 的语义因此定义为
"设置 pending（传 nil 即清除）"，而 `(*bufferedWriter).Write` 是唯一的消费者：
消费后立即置 nil。三者构成的不变式是——**任意时刻至多有一个 pending，且它一定属于
紧接着要写的那一帧**。

配套改动：

- `writeResHeaders` 结构体（`write.go:198-208`）新增一个 `trace *ServerRequestTrace` 字段；
- `writeContext` 接口（`write.go:39-45`）新增一个方法
  `armResponseHeaderMarker(*ServerRequestTrace)`。**已核实非测试代码中该接口只有 `*serverConn`
  一个实现者**（`server.go:699-705`），加方法是安全的。

**只在最终 response 的构造点填 `trace`**，这是 v1 的运行时判断可以完全删掉的原因。
`writeResHeaders{}` 在非测试代码中只有三个构造点，已全部核实：

| 行号 | 场景 | 是否设 trace |
|---|---|---|
| `server.go:2717` | 最终 response HEADERS，位于 `if !rws.sentHeader { rws.sentHeader = true; ... }` 内 | **是**：`trace: &rws.stream.trace`（trace 已内联进 `stream`，见 12.1 B，取地址即可） |
| `server.go:2752` | trailer HEADERS | 否（留 nil） |
| `server.go:2978` | 1xx informational HEADERS，位于 `writeHeader` 的 `code >= 100 && code <= 199` 分支 | 否（留 nil） |

`rws.sentHeader` 保证 2717 处每个 response 恰好执行一次，因此"只记第一组最终 HEADERS"是
**结构性保证**，不需要 `httpResCode >= 200 && trailers == nil` 这类运行时检查，也不需要在
trace 上加"是否已登记"的状态位。

#### C. `http2.go`：在真实 `net.Conn.Write` 成功推进后取 W

**v1 在这里是错的。** 已核实的真实调用链（`http2.go` 末尾）：

```text
(*bufferedWriter).Write(p)
    -> bufio.Writer.Write                        // 进入 4 KiB 用户态 buffer；满时内部触发下一层
    -> (*bufferedWriterTimeoutWriter).Write      // ← 正确的 hook 点
    -> writeWithByteTimeout(conn, timeout, p)
    -> conn.Write                                // 真正底层写入
```

不能 hook `writeWithByteTimeout` 的两个理由：

1. 它是**包级函数**，签名 `func writeWithByteTimeout(conn net.Conn, timeout time.Duration,
   p []byte)`，拿不到 marker FIFO / 累计字节数这些 connection-local 状态；
2. **本部署 `WriteByteTimeout == 0`**：NF 只设了 `IdleTimeout`，`configFromServer` 把
   `h2.WriteByteTimeout`（零值）填进 `conf`，而 `setConfigDefaults`（`config.go:97-111`）
   **没有给 `WriteByteTimeout` 设默认值**。于是 `writeWithByteTimeout` 第一行
   `if timeout <= 0 { return conn.Write(p) }` 直接返回，v1 写的那个 `for { ... n += nn ... }`
   循环**一次都不会执行**。

正确实现——`bufferedWriter` 承载全部 connection-local 状态：

```go
type bufferedWriter struct {
	_           incomparable
	conn        net.Conn
	bw          *bufio.Writer
	byteTimeout time.Duration

	// --- TYcustom: W instrumentation（全部 connection-local）---
	wSink    chan<- ResponseHeadersFlushedEvent
	connID   string
	produced uint64              // 已交给 bufio 的累计字节
	accepted uint64              // conn.Write 已接受的累计字节
	pending  *ServerRequestTrace // writeHeaderBlock 刚挂上、尚未换算偏移的 trace
	markers  []wMarker           // FIFO，按 endOffset 单调递增
}

type wMarker struct {
	endOffset uint64
	trace     *ServerRequestTrace
}

func (w *bufferedWriter) armResponseHeaderMarker(tr *ServerRequestTrace) { w.pending = tr }

func (w *bufferedWriter) Write(p []byte) (n int, err error) {
	if w.bw == nil { /* 原逻辑：从 pool 取 bufio 并 Reset */ }
	// 关键：在调用 w.bw.Write(p) 之【前】把 pending 换算成精确偏移。
	// 因为 w.bw.Write 内部可能立刻触发底层 conn.Write，届时 marker 必须已经在 FIFO 里。
	if w.pending != nil {
		w.markers = append(w.markers, wMarker{endOffset: w.produced + uint64(len(p)), trace: w.pending})
		w.pending = nil
	}
	n, err = w.bw.Write(p)
	w.produced += uint64(n)
	return n, err
}

func (w *bufferedWriterTimeoutWriter) Write(p []byte) (n int, err error) {
	bw := (*bufferedWriter)(w)
	n, err = writeWithByteTimeout(bw.conn, bw.byteTimeout, p)   // 原逻辑不变
	bw.accepted += uint64(n)
	if len(bw.markers) > 0 && bw.accepted >= bw.markers[0].endOffset {
		now := time.Now()          // 本次底层 Write 只取一次
		bw.popCrossedMarkers(now, err != nil)
	}
	return n, err
}
```

要点：

- **偏移用 `produced + len(p)` 结算，不用 `9 + len(frag)`。** `(*bufferedWriter).Write` 由
  `Framer.endWrite` 以"一帧一次 `f.w.Write(f.wbuf)`"的方式调用（`frame.go::endWrite`），
  `p` 就是完整帧（frame header + payload），所以 `produced + len(p)` 精确等于该帧末字节的
  连接内偏移，且完全不依赖 HEADERS 帧是否带 padding/priority。v1 的 `9 + len(frag)` 虽然在
  当前 `writeHeaderBlock` 的调用形态下数值恰好正确（无 padding、无 priority，已核实
  `frame.go::WriteHeaders`/`WriteContinuation`），但它是对帧布局的硬编码假设，不必要地脆弱。
- **partial write / 错误**：`accepted` 只按真实返回的 `n` 推进；短写时 marker 不会被误判为
  已跨过。若 `bufio.Flush` 出错后 `Flush()` 走 `bw.Reset(nil)` 丢弃残留字节，`produced` 与
  `accepted` 会永久错位——但那种情况连接已经完蛋，剩余 marker 在连接关闭时统一释放即可。
- **一次 Write 跨多个 marker**：`popCrossedMarkers` 循环弹出所有 `endOffset <= accepted` 的
  marker，共享同一个 `now`，并把弹出总数填进 `BatchSize`（第 14.5 节）。
- **连接关闭**：在 `serveConn` 的收尾路径释放剩余 marker（可选地投递 `Err: true` 的事件，
  便于统计），不伪造时间。
- **不需要新的锁。** 已核实并发模型：`sc.writingFrame` 保证同一时刻只有一个 frame 在写；
  写要么发生在 serve goroutine（`staysWithinBuffer` 的同步分支），要么发生在
  `writeFrameAsync` goroutine。两者之间的 happens-before 由 `go sc.writeFrameAsync(...)`
  语句和 `sc.wroteFrameCh` 这个 channel 提供。因此 `produced`/`accepted`/`markers`/`pending`
  可以是普通字段。**但这一点必须用 `go test -race` 在真实 h2c 负载下验证，不能只靠推理。**
- `bufferedWriter` **只用于服务端**（已核实：`server.go:439` 是唯一的 `newBufferedWriter`
  调用点；客户端 `cc.bw` 是 `bufio.NewWriter(stickyErrWriter{...})`，`transport.go:814`）。
  所以这些改动不会污染 client 路径，也不会给 client 带来任何额外开销。

#### D. trace 复用

W 复用第二阶段挂在 `st.trace` 上的同一个 `*ServerRequestTrace`。W 回调**只读**创建时写入的
`ID` / `ConnID` / `StreamID` 三个字段，**绝不读 `HandlerGo`**——`HandlerGo` 由 serve goroutine
写、handler goroutine 读，frame-writer goroutine 去读它没有 happens-before 保障，会是真 race。
这条规约必须写在 `ServerRequestTrace` 的注释里（第 12.1 节 A 已包含）。

x/net 内部 trace 不解析 UE ID 或任何 free5GC 业务字段；`run_id`、NF instance 等部署元数据
（若将来需要）由 free5GC 外层 event collector 补入。

即使 stream 已从 `sc.streams` 移除，pending marker 也必须保留到 W 已记录或写失败——
marker FIFO 持有 `*ServerRequestTrace` 的强引用，天然满足。

### 15.2 不能选择的假 W 位置

- 不能在业务 `WriteHeader` 后记录：此时可能只 snapshot header map（`server.go:2989-2994`
  的 `rws.snapHeader = cloneHeader(...)`）；
- 不能在 response HEADERS 提交 `wantWriteFrameCh` 后记录：此时还没有经过 serve/write scheduler；
- 不能仅在 `writeResHeaders.writeFrame` 组帧完成后记录：字节仍在连接级 4 KiB `sc.bw` 中，
  **而且按第 14.5 节，这一步与真正的 `conn.Write` 之间往往还隔着整个 write scheduler 的排空**；
- 不能在 `(*bufferedWriter).Write` 或 `Flush` 的入口/返回处记录：`Write` 只是进入 bufio，
  `Flush` 的返回晚于真实 `conn.Write` 且一次可能覆盖多个 stream；
- 不能简单在某次 `flushFrameWriter` 后把所有 pending stream 记为 W：连接 buffer 满时可能早已
  自动底层 Write；
- 不能为了打点强制每个 response 单独 Flush：这会改变批量写、排队和被测 latency，
  而按第 14.5 节，批量写恰恰是本实验要观测的对象。

### 15.3 热路径约束

- W 只对每个 response 的第一组最终 HEADERS 记录一次（由 `rws.sentHeader` 结构性保证）；
- 排除 1xx informational HEADERS 和 trailer HEADERS（由"只在 2717 处填 trace"结构性保证）；
- 底层写路径只执行取时、整数加法、slice pop 和非阻塞事件投递；
- 不在 socket writer 中做 JSON 格式化、文件 I/O 或可能阻塞的日志调用
  （第 14.7 节说明了为什么这条是硬约束）；
- 连接关闭或写失败时，清理 pending marker 并记录空 W/错误状态，不伪造成功时间。

---

## 16. W 的日志形式与 T4 生命周期问题

### 16.1 为什么 W 必须是独立行

现有 T4 在 `InboundLogger` 的 `c.Next()` 返回后立即记录并输出，但按第 14.6 节，普通小
response 的 W 只能在 `handler` 返回、`rw.handlerDone()` 触发自动 Flush、再等 write scheduler
排空之后发生。因此：

- 不能让 `InboundLogger` 同步等待 W；否则 handler 不返回，`rw.handlerDone()` 不会被调用，
  小 response 根本进不了产生 W 的 flush 路径 —— **这是确定性死锁，不是"可能"**；
- W 不能直接回填已经输出的 server T3/T4 JSON 行（`enqueue` 之后就交给 writer goroutine 了）。

所以 W 单独形成一条极轻量的异步 event，再通过 `(dst, server_request_id)` 与现有 server
T3/T4 行离线合并。

### 16.2 free5GC 侧接线

W event 必须写入该 NF 现有的同一个 HTTP access-log 文件，而不是另建日志文件：

```text
NFs/<nf>/internal/accesslog/accesslog.go
    Init()           在现有 initOne.Do 内追加：创建有界 wQueue，启动一个长期存活的 wCollectorLoop
    WEventSink()     返回 chan<- http2.ResponseHeadersFlushedEvent，供 sbi/server.go 注入
    wCollectorLoop() 读取固定大小 event，在该 goroutine 中 formatTime + 构造 W JSON，
                     然后调用现有 enqueue(kindHTTP, b)
    WDropped()       暴露 W 队列的 drop 计数（与现有 Dropped() 并列）

NFs/<nf>/internal/sbi/server.go
    newHttp2ServerWithIdleTimeout  构造 http2.Server 时注入 accesslog.WEventSink()
```

socket writer 只做 marker 状态更新和第一次 non-blocking send；`wCollectorLoop` 才执行
`formatTime`/JSON append 和第二次现有 `enqueue(kindHTTP, ...)`。**禁止为每个 W 新建 goroutine。**
这样每个 NF 仍然只有 `writerLoop()` 一个文件 writer。

W 队列容量建议与现有 `queueCapacity = 1 << 21` 同量级或略小（每 response 至多一条），
类型是定长 struct 而非 `[]byte`，内存占用远小于现有队列。

W event JSON（`dst` 取 `srcNF`，与 server 行一致）：

```json
{"event":"server_response_headers_flushed","src":"NaN","dst":"UDM",
 "server_request_id":12345,"conn":"10.244.1.7:41236","stream_id":17,
 "server_response_headers_flushed_time":"...","batch_size":3,"outcome":"ok"}
```

注意 `conn` 与 server T3/T4 行同名同值（都是 `sc.remoteAddrStr`），`dst` 也同值，便于校验。

### 16.3 采集顺序（v1 只说了原则，这里给出可执行顺序）

每轮实验结束时按此顺序执行，任何一步提前都会把本轮的迟到 W 写进下一轮：

```text
1. 停止发流量（PacketRusher / UE 触发器）
2. 等待在途 request 结束（观察 client/server 行不再增长）
3. 等待在途 W：给出固定静默期（建议 >= 2 * IdleTimeout = 1s），
   或轮询 accesslog 的 W 队列长度归零
4. 触发 accesslog 的 drain：Flush() 必须先排空 wQueue（即 wCollectorLoop 已把所有 event
   enqueue 到 kindHTTP 队列），再走现有 flushReq 路径
5. 采集各 Pod 的 HTTP_log.txt 到本轮唯一的 OUTDIR
6. 清空各 Pod 的 HTTP_log.txt
7. 记录本轮的 Dropped()、WDropped() 与 http2.ResponseHeadersFlushedDrops()
```

第 4 步是对现有 `Flush()` 的**必要**扩展：当前实现（`accesslog.go` 的 `Flush()` -> `flushReq`
-> `drainAll` + `flush`）只认识 `queue`，不知道 `wQueue` 的存在，不改的话最后一批 W 会丢。

### 16.4 备选方案（不作为首选）

若后续必须保持"一个 request 只有一行 server JSON"，可改为异步 collector：T4 之后保存 record，
W 到达后由 collector 合并输出。这比独立 W event 改动更大（要给每个 in-flight request 保留
record、要处理 W 永不到达的超时回收），不作为第三阶段首选。

---

## 17. 三个新增时间点能否与 request/response 一一匹配

### 17.1 同一端、同一条日志内的关系

**M = `req_header_mu_start_time`**

- request-attempt 粒度；在 `clientStream` 内部取时，最终与该 outgoing request 的
  T1/T2/T5/T6、UE ID、method、URI 写在同一条 client JSON 中；
- 与这条 client request 天然一一对应，**前提是 `retry_count == 0`**（第 4.1 节 C）。

**G = `server_handler_go_time`**

- request/HTTP2-stream 粒度；通过同一个 `*http.Request.Context()` 传到 `InboundLogger`，
  与该 inbound request 的 T3/T4、UE ID、method、URI 写在同一条 server JSON 中；
- 因此 G 与这条 server JSON 天然一一对应，无需任何 key。

**W = `server_response_headers_flushed_time`**

- response/HTTP2-stream 粒度，而 HTTP/2 的最终 response 与该 stream 上的 request 一一对应；
- 由于发生在 T4 行输出之后，输出为独立 event；
- 用 `(dst, server_request_id)` 与 server T3/T4 行严格合并；再用两侧相同的 `conn + stream_id`
  做 transport 一致性校验，合并后即可获得该 response 的 UE ID、method 和 URI。

### 17.2 只靠 `ue_id + method + URI + 时间顺序` 不能严格一一匹配

在低并发、同一 UE 的同一 URI 不重叠时可以近似配对，但这不是严格保证：

- 同一 UE 可以并发多个相同 method/URI 的 request；
- HTTP/2 多路复用允许 handler 开始、业务完成和 response 返回顺序与 request 发送顺序不同；
- **本部署里 `ue_id` 大部分时候是空的**：`LogHTTP` 的 `ueID` 只来自 `sniffUEID`，
  而 `bodyUEIDField`（`httptransport.go:313-321`）只覆盖两个路径——
  `/nausf-auth/v1/ue-authentications`（`supiOrSuci`）和
  `/npcf-am-policy-control/v1/policies`（`supi`）。其余请求（`nudm-sdm`、`nudm-ueau`、
  `nudr-dr`、`namf-comm` 等）的 UE 身份**只在 URI 里**，必须离线正则提取。
  NRF discovery/heartbeat 则根本没有 UE 身份。
- retry/重连会产生新 stream，单纯按时间排序会错位。

因此，UE ID 和 URI 用来回答"这是哪个 UE 的哪种 SBI request"，不能代替 transport-level 唯一
关联键。

### 17.3 当前实验的 exact-join 字段与作用域

当前实验固定满足：同一种 NF 只有一个 Pod；每轮实验结束后按第 16.3 节排空、flush 并采集，
然后清空所有 Pod 的 `HTTP_log.txt`；离线脚本一次只处理一轮数据。因此 `run_id`、
`src_nf_instance` 和 `dst_nf_instance` 不要求作为每条 JSON 的主 join 字段，实验目录/采集
manifest 本身承担 run scope。

"实验目录承担 run scope"必须在采集脚本中真实成立：每轮必须使用唯一 `OUTDIR`，或在本轮开始前
清空本地 merged 输出，不能继续把多轮 Pod 日志 append 到同一个分析文件。少量 missing/orphan
可以排除，但多轮混合或进程重启后的 ID 重用会造成**错误匹配**，不属于可接受的"少量缺失"。
如果不能保证物理隔离，就必须把 `run_id`（以及会重启时的 process instance）加入三类记录和
join key。

**当前每条记录的 mandatory correlation fields：**

```text
client JSON:
    src, dst, conn, stream_id, retry_count
    （conn 已存在；stream_id / retry_count 为新增）

server T3/T4/G JSON:
    dst, server_request_id, conn, stream_id
    （三个都是新增；conn 来自 c.Request.RemoteAddr，不需要 fork）

W event:
    dst, server_request_id, conn, stream_id, outcome
    （全部来自 fork 的 ServerRequestTrace + bufferedWriter.connID）

仅在 retry_count > 0 时：
    request_attempt_id
```

**关联规则：**

1. server JSON 与 W event 用 `(dst, server_request_id)` exact join；
2. client JSON 与已合并的 server/W record 用 `(dst, conn, stream_id)` exact join；
3. `conn` 的规范化：client 侧是 `info.Conn.LocalAddr().String()`，server 侧是
   `c.Request.RemoteAddr`（= `sc.remoteAddrStr` = `net.Conn.RemoteAddr().String()`）。
   在无 SNAT 的 pod-to-pod 直连下这是同一条 TCP 连接的同一个 `clientIP:clientPort`；
4. `stream_id` 不能单独使用：每条新 HTTP/2 连接都会从低值重新分配 stream ID
   （`cc.nextStreamID` 从 1 开始，`addStreamLocked` 每次 +2）。现有 `conn_slot` 只是
   round-robin 槽位（`connsPerPeer == 1` 时恒为 0），更不能代替 `conn`；
5. 第一轮主分析只接受 `retry_count == 0`。若 `retry_count > 0`，必须按 attempt 分别生成/记录
   `request_attempt_id`，不得把不同 attempt 的 M/T2、conn/stream、T5/T6 混到同一条九点样本中。

**【新增，v1 缺失】本地端口复用的唯一性风险。**
`(conn, stream_id)` 在一条连接的生命周期内唯一，但客户端本地端口在 socket 关闭后可能被内核
复用。本实验的连接策略（`connsPerPeer = 1`、server `IdleTimeout = 500ms`、client
`PingTimeout = 3s`）下连接是长寿的，但 `conn_reused == false` 的记录说明池确实会增长/重建。
因此：

- 离线脚本**必须**统计 `(dst, conn, stream_id)` 的 duplicate 数，要求为 0；
- 若出现 duplicate，最小修复是在 client/server 两侧各加一个"连接诞生时刻"字段
  （client 用 `GotConn` 时刻，server 用 `serveConn` 入口时刻），把 key 扩成
  `(dst, conn, conn_epoch, stream_id)` 并按时间窗归并——而不是用近似匹配。

**关联预检（正式实验前必须执行）：**
对每个成功、无 retry 且两侧记录均存在的 request，`(dst, conn, stream_id)` 必须恰好一对一，
duplicate/ambiguous match 必须为 0；missing/unmatched 允许低于预设上限并作为 incomplete 排除。
额外自校验：client 行的 `conn`（来自 `GotConn`）应与 fork 快照的 `ClientConnIdentity.LocalAddr`
逐字符相同（第 4.2 节）。

如果 unmatched 呈系统性或显示两侧 connection identity 根本不同（说明 CNI/NAT 改写了源地址），
不能回退到时间/UE/URI 近似配对，必须改用客户端生成并随 request header 传递的
`sbi_request_id`（例如 `x-ty-req-id`），同时记录在 client 行、server 行和 W trace 中。
该 header 会改变 HPACK 编码内容和帧长度，属于对被测系统的真实扰动，启用后必须在结论里
明确声明。

### 17.4 实施后能否离线找到"哪个 UE 的哪个 request/response"

按本节的必须字段实施后，可以：

1. 用 transport correlation key 严格连接 client request 行、server request 行和 W response event；
2. 从 client/server 行读取 `ue_id`；**若为空，则从 `uri` 正则提取 UE 身份**
   （`imsi-*` / `suci-*`，见第 17.2 节的说明）；
3. 将 T1、M、T2、G、T3、T4、W、T5、T6 九个点归到同一个 HTTP/2 request/response stream；
4. 回答该 latency 样本具体属于哪个 UE、哪个 SBI method/URI 和哪次 transport attempt。

第 4 项只适用于能确定 UE ID 的 UE-associated SBI request；没有 UE 身份的共享/基础设施请求
（NRF register/heartbeat/discovery）仍可按 transport key 获得完整九点，但必须标记为
`ue_id=""`/unattributed，不能强行归给某个 UE。

### 17.5 【新增】"按 ue_id / uri / id 提取"到底能做到什么程度（已按真实路由核对）

这一节回答一个具体问题：**离线合并九点时，是否需要用到时间？**

**答：主链路完全不需要。** 三类记录的合并全部走确定性 ID，与时间戳无关：

```text
server JSON  --(dst, server_request_id)--> W event         纯整数 ID，进程内分配，无歧义
client JSON  --(dst, conn, stream_id)---->  server JSON     纯传输层身份，无歧义
```

时间只在两处出现，都不是主链路：
(a) 第 17.3 节 `conn_epoch` 那个**兜底**方案（只有在 duplicate != 0 时才启用）；
(b) 验证阶段用来检查 `T1 <= M <= T2 <= G <= T3 <= T4 <= W <= T5 <= T6` 是否成立——
那是**校验**，不是**匹配**。

**但必须说清楚：`ue_id + uri` 本身不是 join key，也不能当 join key。**
它们的角色是"join 完成之后给这条九点样本贴标签"。原因见第 17.2 节：同一 UE 可以并发多个
相同 method/URI 的请求，HTTP/2 多路复用又允许乱序完成，靠 `ue_id + uri` 会一对多。
真正保证一一对应的是 `conn + stream_id` 和 `server_request_id`。

#### UE 归属的实际覆盖率（本仓库路由已逐条核对）

好消息是：在**这个 free5gc build** 上，注册链路的每一跳几乎都能确定 UE 身份，而且不需要
额外打点。分三类：

| 来源 | 覆盖的请求 | 说明 |
|---|---|---|
| **`ue_id` 字段（已有 body sniff）** | `POST /nausf-auth/v1/ue-authentications`（`supiOrSuci`）、`POST /npcf-am-policy-control/v1/policies`（`supi`） | `bodyUEIDField`（`httptransport.go:313-321`）只覆盖这两条，client 与 server 两侧都有 |
| **URI 路径段（需离线正则提取，`ue_id` 为空）** | 见下表，注册链路的绝大多数请求 | 这是主要来源 |
| **无 UE 身份** | NRF register / heartbeat / discovery；`GET /nnssf-nsselection/v2/network-slice-information?...`（query 里只有 nf-id 和 slice-info，**没有任何 UE 标识**） | 只能标 `ue_id=""`/unattributed |

URI 里带 UE 身份的路由（已核对本仓库源码）：

```text
UDM  ueau  /nudm-ueau/v1/:supiOrSuci/security-information/generate-auth-data
UDM  ueau  /nudm-ueau/v1/:supi/auth-events
UDM  sdm   /nudm-sdm/v2/:supi/am-data | /:supi/smf-select-data | /:supi/nssai | ...
UDM  sdm   /nudm-sdm/v2/:ueId/sdm-subscriptions
UDM  uecm  /nudm-uecm/v1/:ueId/registrations/amf-3gpp-access
UDR  dr    /nudr-dr/v2/subscription-data/:ueId/...
PCF  am    /npcf-am-policy-control/v1/policies/:polAssoId
```

两个**原本以为会缺、实际不缺**的关键点（这是核对源码才发现的，值得单独记）：

1. **AMF -> AUSF 的 5G-AKA 确认**
   `PUT /nausf-auth/v1/ue-authentications/{authCtxId}/5g-aka-confirmation`
   这条请求 `bodyUEIDField` 匹配不到（它用的是 `HasSuffix(path, ".../ue-authentications")`），
   body 里也只有 `resStar`。**但 `authCtxId` 就是 UE 身份本身**：AUSF 在
   `NFs/ausf/internal/sbi/processor/ue_authentication.go:310` 生成 Location 时写的是
   `locationURI = self.Url + AusfAuthResUriPrefix + "/ue-authentications/" + supiOrSuci`，
   AMF 原样拿去做后续 PUT。所以 URI 里直接是 `suci-0-...` / `imsi-...`。

2. **PCF 策略关联 ID**
   `polAssoId` 在 `NFs/pcf/internal/sbi/processor/ampolicy.go:227` 是
   `fmt.Sprintf("%s-%d", ue.Supi, ue.PolAssociationIDGenerator)`，形如
   `imsi-999700000000001-1`，UE 身份同样在 URI 里。

#### 结论

- **九点的一一匹配：确定性的，只用 ID，不用时间。** 前提是第 17.3 节的三个条件成立
  （`conn` 两侧规范化一致、`(dst, conn, stream_id)` duplicate == 0、`retry_count == 0`），
  这三条都必须在正式实验前用预检实测，不能假定。
- **UE 归属：注册链路上除 NRF 与 NSSF 之外全部可确定**，来源是 `ue_id`（两条）+ URI 正则
  （其余）。离线脚本需要实现一个 UE 提取函数，正则大致是
  `(imsi-\d+|suci-[0-9-]+|nai-[^/?]+)`，对 client 行和 server 行的 `uri` 各跑一次并交叉校验。
- **NSSF 那一跳是真的无法按请求归属到 UE**。若必须归属，只能靠"同一 AMF 在同一时刻为哪个 UE
  做切片选择"——那就退回到时间推断了，与本计划的纪律冲突。**建议直接把 NSSF 请求单列统计，
  不要为它引入时间匹配。** 若确实需要，正确做法是让 AMF 在该请求上带一个
  `sbi_request_id` header（第 17.3 节的方案），而不是离线猜。

#### 与"跨 NF 跳"的区别（容易混淆，先说清）

本计划保证的是：**一次 HTTP request/response 的九个点**能一一对应。
它**不**保证：把 `AMF->AUSF` 这一跳和它引发的 `AUSF->UDM` 那一跳串成一条 UE 级因果链。
后者是另一个问题——`conn + stream_id` 是逐跳的传输身份，跨不过 NF 边界。

在当前手段下跨跳只能靠 `ue_id`/URI 里的同一个 UE 身份来归组；同一 UE 在一次注册中每种
method/URI 通常只出现一次，所以按 (UE, method, URI) 归组在**低并发**下够用，但这已经不是
严格一一对应了。如果实验目标包含"UE 级端到端因果链"，那就必须上第 17.3 节的
`sbi_request_id`，并让 NF 在下游请求上透传上游的 ID——**这属于本计划范围之外的扩展，
现在不做，但要知道现有设计到不了那一步。**

### 17.6 【新增】现有分析脚本的时间依赖，以及"每 UE 只注册一次"把它缩到多小

现有六点分析脚本（`cloudlab/Ty_log/Free5gc/<run>/HTTP_per_UE_transport_latency_2way.py`）
按 `ue_id + method + 归一化 uri` 分桶，然后在桶内用时间收尾。下面先给实测结论，再说明
九点方案改变了什么。

#### 实测：在"每 UE 只注册一次"的数据上，唯一的冲突是 am-data

对 `C6525100g_NF1HTTP_500ms_runtime_0826` 的真实日志，按脚本同款的
`(view, ue_id, dst, method, norm_path)` 分桶统计（RQ200 与 RQ2000 结果**完全一致**）：

```text
总记录数            42000  = 1000 UE x 21 client + 1000 UE x 21 server
桶大小分布          {1: 38000 个桶, 2: 2000 个桶}
桶内出现 2 条的      只有一种：
                    udr GET /nudr-dr/v2/subscription-data/{ueId}/99970/provisioned-data/am-data
                    （2000 = 1000 UE x client/server 两个视图）
完全没有 UE 身份的   0 条
```

三条可以直接得出的结论：

1. **21 个 hop 里有 20 个，每个 `(ue, dst, method, path)` 桶恰好是 1 条 client + 1 条 server。**
   这种桶里的 `sorted(...)` + 下标配对是**恒等操作**——只有一种配法，不可能配错。
   也就是说：**对这 20 个 hop，`ue_id + uri` 今天就已经足够，不依赖时间。**
2. **唯一真正需要时间的是 UDM->UDR 的 am-data 读**，它每 UE 出现两次
   （substage 3 的 nssai 和 substage 5 的 am-data 各触发一次），`(ue, dst, method, path)`
   完全相同。脚本为此写了父窗口 + 最近时间戳的特例（`_shared_legs`）。
3. **本轮数据里没有 NRF/NSSF 记录**（`by dst` 只有 udr/udm/ausf/pcf），
   所以第 17.5 节提到的"NSSF 无 UE 身份"在当前采集窗口内不构成问题；
   NF register/discovery 发生在测量窗口之前，日志已被清空。

#### 需要收回的一句话

上一版这里写过"`min(len(c), len(s))` 会在日志缺一条时静默错位"。**在 1+1 的桶里这句话不成立**：
少一条时 `min(1,0)=0`，该样本直接被跳过，不会污染其它样本。静默错位只发生在桶内有 >= 2 条
的情况——本数据集里只有 am-data 那一种。而且本轮 42000 = 1000 x 42 的记录数**恰好对齐**，
说明这批数据既没有 drop 也没有多余记录，配对是可证明正确的。

#### 那么 1+1 的桶还剩下什么风险

不是"现在有错"，而是"现在没法知道有没有错"。四项，都不会被 `ue_id + uri` 发现：

| 风险 | 会变成什么 | 现在能否发现 |
|---|---|---|
| transport retry（`RoundTripOpt` 内部重试） | `LogHTTP` 每次 `RoundTrip` 只输出一行，但 server 可能收到两次 -> 桶变成 1 client + 2 server，`min(1,2)=1` 会配到**较早的那条**，可能正是失败的那次 | 否。本轮记录数恰好对齐说明没发生，但这是事后观察，不是保证 |
| UE 注册失败后重试 | 该 UE 所有桶都变成 2 条，退化成时间序配对 | 只能靠记录数对不上间接推断 |
| body sniff 失败（body > 8 KiB 或 JSON 解析失败） | `ue_id` 变空，该记录掉进 `ue=""` 的公共桶，跨 UE 混配 | 否。本轮为 0 条，但没有计数器 |
| 日志队列 drop | 样本丢失（不是错配） | `accesslog.Dropped()` 有计数，但脚本没读 |

#### 九点方案改变的是什么

| | 六点 + 1 reg/UE | 九点 |
|---|---|---|
| 20 个 1+1 hop 的 client<->server 配对 | 已经正确（恒等配对） | 仍然正确，但改为**结构性保证**而非"这批数据碰巧干净" |
| am-data 的 client<->server 配对 | 靠父窗口 + `min(abs(Δt))` | **`stream_id` 天然不同 -> exact join，特例代码整段删除** |
| retry 是否发生 | 不可见 | `retry_count` 字段直接可见，`> 0` 一律排除 |
| drop / 不完整 | 隐式减少样本 | 显式计数（duplicate / missing / orphan），并要求 duplicate == 0 |

**所以对第 1、2 问的直接回答：**

- 第 1 问：**是的，除 am-data 外这个现象不存在**。你的 20 个 hop 今天就可以只靠 `ue_id + uri`
  配对，脚本里的排序+下标对它们是恒等操作。am-data 是唯一例外。
- 第 2 问：**是的，加上 `stream_id` / `server_request_id` 之后，am-data 也不再有这个问题**。
  两次 am-data 读走的是同一条 UDM->UDR 连接（`connsPerPeer = 1`），`stream_id` 必然不同；
  `server_request_id` 也必然不同。`_shared_legs` 里的
  `min(s, key=lambda r: abs(r.req_ns - cr.req_ns))` 可以直接删掉。

#### 但有一件事 stream_id 解决不了，必须分清

`stream_id` 解决的是"**这条 client 行和哪条 server 行是同一次请求**"。
它**不**解决"**这次 UDM->UDR 的 am-data 读是由哪一次 AMF->UDM 请求引发的**"——
后者是跨 NF 跳的因果关系，`conn + stream_id` 跨不过 NF 边界。

也就是说 `_shared_legs` 里的两段代码要区别对待：

```python
# (1) 父窗口筛选 —— 用来区分 nssai 触发 vs am-data 触发（跨跳归因）
c_in = [r for r in c if any(wr <= r.req_ns <= we for (wr, we) in windows)]
#     -> 九点方案【不能】替代它。如果你需要按 substage 拆分，这段仍然要保留。

# (2) 最近时间戳配 server 记录 —— 用来配对 client/server（同跳配对）
sr = min(s, key=lambda r: abs(r.req_ns - cr.req_ns))
#     -> 九点方案【可以】完全替代，改成按 (dst, conn, stream_id) exact join。
```

(1) 是"父请求的时间窗包含子请求"，这是一个**因果窗口**判据，比 (2) 的"最近时间戳"可靠得多：
nssai（substage 3）和 am-data（substage 5）对同一个 UE 是**串行**发生的，两个窗口不重叠，
所以窗口归属在结构上就是确定的。如果只需要 `udm<->udr` am-data 这条腿的总体时延而不需要
按 substage 拆，那连 (1) 都可以不要，两次读直接合并统计。

若将来一定要严格的跨跳因果链（而不是靠串行假设），只能上第 17.3 节的 `sbi_request_id`
并让 NF 在下游请求上透传上游 ID——**本计划不做**。

#### 顺带：现有脚本 `_ID_RE` 对 PCF `polAssoId` 的贪婪匹配 bug

`_ID_RE = re.compile(r"(imsi-|suci-)[A-Za-z0-9\-]+")` 会多吃一段：

```text
/npcf-am-policy-control/v1/policies/imsi-999700000000001-1
    -> _extract_ueid 得到 'imsi-999700000000001-1'   （多了 "-1"）
```

因为 `polAssoId = fmt.Sprintf("%s-%d", ue.Supi, ...)`
（`NFs/pcf/internal/sbi/processor/ampolicy.go:227`），UE id 后直接跟序号。
当前 `HOPS` 表没有 `/policies/{polAssoId}` 这一跳所以尚未暴露；一旦纳入 PCF 的 GET/DELETE
就会与其它记录的 `imsi-999700000000001` 对不上。建议改成
`(imsi-\d{5,}|suci-[0-9-]+|nai-[^/?]+)`。

已验证**没有**问题的两条：`5g-aka-confirmation` 路径里的
`suci-0-999-70-0-0-0-0000000001` 在 `/` 处正确截断；`am-data` 路径里的 `imsi-` 也正确。

## 18. 第三阶段离线分析与验证

1. 先按 `(dst, server_request_id)` 对 server T3/T4/G 行与 W event 执行 exact join，
   并用 `conn + stream_id` 校验一致性；再用第 17.3 节的 key 连接 client 行。
   两个阶段都必须显式统计 duplicate、missing 和 unmatched 记录；禁止在主分析中仅按
   UE/URI/index 配对或静默回退到时间顺序配对；
2. 读取 T4、W 和 T5，计算 `W - T4` 与 `T5 - W`；
3. 验证对于普通小 response：

```text
T4 <= W <= T5
(W - T4) + (T5 - W) ≈ T5 - T4
```

4. **按第 14.6 节的精确阈值分组**：response body `< 4 KiB`（预期 `T4 < W`）与 `>= 4 KiB`
   （预期 `W < T4`，不能按 `T4 -> W -> T5` 模型解释）分开统计；W 缺失、底层写失败、1xx、
   显式 Flush、streaming 各自单独标记；
5. **输出 `batch_size` 分布**（第 14.5 节）：每次底层 Write 结算的 marker 数随 RQ 的变化，
   是"服务端写路径批量化"的直接证据；
6. 对不同 request rate 比较 `W - T4` / `T5 - W` 的 p50、p95 和 p99；
7. `W - T4` 上升、`T5 - W` 稳定：优先检查 server response 发送路径。注意 `W - T4` 里
   **包含我们自己的 `LogHTTPInbound` JSON 构造**，解释时必须先扣除或至少明确报告；
8. `T5 - W` 上升、`W - T4` 稳定：优先检查内核/网络/client readLoop 路径；
9. 两段同时上升：检查两端 CPU throttling、GC、goroutine 调度及共享连接饱和；
10. 对每轮实验输出 `client_count`、`server_count`、`w_count`、`complete_nine_point_count`、
    `duplicate_count`、`missing_server_count`、`missing_w_count`、`orphan_w_count`、
    `retry_gt0_count`，以及 `accesslog.Dropped()`、`accesslog.WDropped()`、
    `http2.ResponseHeadersFlushedDrops()` 三个 drop 计数。只有三类记录各恰好一条且九点完整的
    样本进入主 latency 分析；
11. 对可识别 UE 的记录按 UE ID 统计每种 method/URI 的完整率；共享/无 UE 请求单独统计，
    并检查缺失是否集中在特定 NF、URI、UE 或高负载时间段，避免用有偏的完整子集得出总体结论。

T4/W 位于被调方 Pod，T5 位于调用方 Pod。`W - T4` 在同一进程内；`T5 - W` 跨 Pod。跨节点测量时
必须审计时钟偏移/漂移，否则不能将微秒级差值全部解释为 transport latency。

## 19. 日志扰动的专项验证与验收

### 19.0 相对原六点，M/G/W 到底多做了什么（逐项清单）

`time.Now()` 从 6 次变 9 次是**最小**的一项，不是主要的一项。下面按"是否落在被测关键路径上"
分三类列出全部增量。这份清单就是第 19.3 节代码审查的检查表。

#### 类别一：落在**串行/关键路径**上的增量（最需要盯）

| 位置 | 每次做什么 | 为什么关键 |
|---|---|---|
| `scheduleHandler` / `handlerDone`（G） | 1 次指针判空 + 1 次 `time.Now()` + 1 次赋值 | 跑在**每条连接唯一的 serve goroutine** 上，按该连接的 stream 数放大。若用 context 查找还要多一次链表walk——见第 12.1 节 B，推荐改成传指针 |
| `newWriterAndRequestNoBody`（G） | 1 次 `atomic.Uint64.Add` + 3 次字段赋值。**按 12.1 B 的设计没有任何堆分配**（trace 是 `stream` 的内联值字段，`req.WithContext(st)` 不需要 valueCtx 节点） | 在 serve goroutine 上。剩下的 atomic 是进程级共享计数器，若 profile 显示 cache line 争用，按 19.5 R2 改成每连接计数器 |
| `InboundLogger` T4 之后（G） | 多格式化 1 个时间戳（`formatTime` 内部 `Format()` **会分配一个 string**）、多 append 4 个字段（约 150 B）、`uint64` 十进制转换、`conn` 字符串转义 | **这段代码位于 T4 与 `rw.handlerDone()` 之间**，它直接推迟 response HEADERS 的提交，因此**真实地增大 `W - T4`，也真实地增大对端看到的 T5/T6**。这是三个点里对端到端 latency 影响最直接的一项 |
| `(*bufferedWriter).Write`（W） | 1 次指针判空 + 1 次 `uint64` 加法；命中时 1 次 slice append（可能触发扩容分配） | **每一帧都会执行**（DATA / WINDOW_UPDATE / PING / SETTINGS 全都经过），是三个点里频率最高的 hook，不是每 response 一次 |
| `(*bufferedWriterTimeoutWriter).Write`（W） | 1 次 `uint64` 加法 + 1 次 slice 长度比较；命中时 1 次 `time.Now()` + N 次 non-blocking channel send | 跑在真正的 socket 写路径上，且这条路径已经被 `sc.writeHeaders` 的 `<-errc` 同步阻塞着 handler goroutine（第 14.7 节） |
| `writeRequest`（M） | 1 次指针判空 + 1 次 `atomic.Int64.Store` | 在 `reqHeaderMu` 竞争之前，属于被测区间的起点，开销可忽略但要保证不做别的事 |
| `LogHTTP`（M） | 多格式化 1 个时间戳（1 次 string 分配）+ 3 个字段 append（约 95 B） | 在 T6 之后、`RoundTrip` 返回之前，直接算进调用方业务的耗时 |

#### 类别二：新引入的**跨连接共享点**（v1 完全没提，必须单独评估）

marker FIFO、`produced`、`accepted` 都是 connection-local，**但 W 的事件 channel 是每 NF 一条、
所有连接共用的**。Go 的 `select { case ch <- v: default: }` 即使是非阻塞形式，
`runtime.chansend` 仍然要**获取该 channel 的互斥锁**再判断是否有空位。

也就是说：这是本计划在 HTTP/2 写路径上**新引入的唯一一个进程级共享同步对象**。

- 当前部署 `connsPerPeer = 1`，一个 NF 作为 server 时的并发连接数等于"有多少个 NF 调它"
  （UDR 大约 1-2 条，AMF/UDM 略多），竞争者很少，预计影响可忽略；
- 但如果将来 `connsPerPeer` 调回 4/8/16，或 NF 扩成多 Pod，这个 channel 会变成
  socket-writer 路径上的一个真实竞争点——**恰好是本实验想排除的那类效应**。

因此：

1. 第 19.2 节的 mutex/block profile **必须专门看这个 channel**，不能只看整体；
2. 建议实现时就做**批量投递**：一次底层 Write 结算出的 N 个 marker 合成**一个**事件，
   把 chansend 次数从 N 降到 1。这与第 14.5 节的 `batch_size` 字段天然一致。
   **但必须用定长数组，不能用 slice**——slice 会在 socket writer 里分配，直接违反
   第 15.3 节"底层写路径不做分配"：

   ```go
   const wBatchMax = 8   // 超过则拆成多个事件

   type ResponseHeadersFlushedEvent struct {
       At        time.Time
       ConnID    string              // 指向连接上的不可变字符串，无分配
       IDs       [wBatchMax]uint64   // server_request_id
       Streams   [wBatchMax]uint32
       N         uint8               // 本事件实际携带几条
       BatchSize uint16              // 本次底层 Write 一共跨过几个 marker（可 > N）
       Err       bool
   }
   ```

   代价是每个事件按值拷贝约 130 B 进 channel（换来 chansend 从 N 次降到 ceil(N/8) 次）。
   `wBatchMax` 取 8 是因为第 14.5 节预期的批量规模在个位数；若实测 `batch_size` 经常更大，
   再调。**先按每 marker 一个事件实现，profile 看到 chansend 才上批量**——不要一上来就做，
   定长数组会让事件结构体变大，在批量规模本来就是 1 的低负载下反而更慢；
3. 若 profile 显示有竞争，退路是按连接分片（每 K 条连接一个 channel，collector 多路 select），
   而不是加大容量——容量解决不了锁竞争。

#### 类别三：背景 CPU / GC 增量（不阻塞请求，但抢同一批核）

| 项 | A 组（原六点） | B 组（+M/G/W） | 增量 |
|---|---|---|---|
| 每 transaction 的日志行数 | 2 | 3 | **+50%** |
| 每 transaction 的时间戳格式化次数（每次一个 string 分配） | 6 | 9 | **+50%** |
| 每 transaction 经过共享 `queue` 的 `enqueue`（chansend + 锁） | 2 | 3 | **+50%** |
| `writerLoop` 的 `bufio` 写入与 file I/O 字节数 | 约 2×(304+256) | 约 3×(416+416+320) 量级 | **约 +2 倍**（行数 +50% 且每行更宽） |
| 每 transaction 的新增对象分配 | — | `ClientRequestTrace` + `ServerRequestTrace` + ctx node + 第三条行的 `[]byte` + 3 个时间 string | **约 +7 个对象** |
| 常驻 goroutine | 每 NF 1 个 `writerLoop` | 每 NF 2 个（+ `wCollectorLoop`） | +1 |

**GC 才是这里最容易被低估的一项。** 三个新点本身的 CPU 指令数可以忽略，但每 transaction
多约 7 个堆对象、日志字节数翻倍，会抬高 GC 频率与 assist 时间，而 GC assist 是**随机落在
正在跑的业务 goroutine 上**的——它不会出现在九点的任何一段里，只会表现为 p99 抬升。
这就是"GC 尾延迟只能用端到端 registration latency 看出来、不能只看九点内部差值"的原因——
无论是否做正式 A/B，解释 p99 时都要记住这一项不会出现在任何一段九点差值里。

#### 已经可以确定"零增量"的地方

- **客户端 socket 写路径完全没被碰**：`bufferedWriter` 只在 server 侧使用
  （`server.go:439` 是唯一构造点；client 用的是 `bufio.NewWriter(stickyErrWriter{...})`，
  `transport.go:814`），所以 W 的全部改动对 client 是零成本；
- **`req.WithContext` 不增加次数**：按第 4.2 节把两个 trace 合并进同一条 ctx 链，
  `WithContext` 仍然只调用一次，不多复制一份 `http.Request`；
- **不新增每 request 的 goroutine**：M/G/W 都没有 `go` 语句，W 由长期存活的 collector 消费；
- **不改变任何 flush/batching/flow-control 行为**，因此不会通过"改变 HTTP/2 行为"这条隐蔽
  途径影响 latency（第 19.3 节第 6 条逐项验收）。

#### 一句话结论

> 三个新点自身的**同步指令开销**很小（约 3 次 `time.Now()` + 若干 atomic/指针操作）；
> 真正需要留意（并在结论中报告）的是三件事：**(a) `InboundLogger` 变宽的 JSON 直接落在 `W - T4` 上、
> (b) 每 transaction 多约 7 个堆对象带来的 GC 尾延迟、(c) W 事件 channel 这个新的进程级
> 共享同步对象。** 前两项一定存在且可测；第三项在当前 `connsPerPeer = 1` 下预计可忽略，
> 但必须 profile 确认，且连接数一旦调大就要重新评估。

### 19.1 前置检查：宿主机 clocksource 与 GOMAXPROCS

> **范围变更（0827）**：本节与 §19.2 原为"A0/A1/B 三组扰动 A/B"的对照组设计与必测负载/指标
> 清单。按实验负责人的决定，**该开销 A/B 不在本计划范围内**——M/G/W 的 latency 影响由实验
> 负责人自行衡量。这里只保留两项与 A/B 无关、但会直接改变开销量级或结论有效性的检查：本节的
> 宿主机前置检查，以及 §19.2 的竞态/完整性验收。**§19.2 的 `go test -race` 是正确性验收，
> 不随 A/B 一起取消。**
>
> 需要保留的术语：下文其余各节仍用 **A 组** 指"只有原六点的实现"、**B 组** 指"加上 M/G/W
> 的实现"，仅作为描述用语，不再蕴含"必须跑对照实验"。

**在写任何开销结论之前先跑这一条**，因为它能把"9 次取时"的成本放大 10-50 倍：

```bash
cat /sys/devices/system/clocksource/clocksource0/current_clocksource
```

- 结果是 `tsc`：`time.Now()` 走 vDSO，约 20-25 ns。9 次约 200 ns，可忽略，
  本计划第 19.0 节的全部估算成立。
- 结果是 `hpet` / `acpi_pm` / `xen`：vDSO 快路径失效，`time.Now()` 退化到
  **数百 ns 甚至 1 µs**。此时 9 次取时就是 **5-9 µs/transaction**，
  比本计划讨论的所有分配开销加起来还大一个量级——**这时"多三个点"才真的会有影响**。

如果读到的不是 `tsc`，先解决 clocksource（或者接受并在结论里明确扣除）。
这一项不改代码，5 秒就能查，却是所有开销假设里最容易被推翻的那个。

同理，若 Pod 设了 CPU limit 而 `GOMAXPROCS` 没有跟着设，GC worker 会加剧
cgroup throttling；确认 `GOMAXPROCS` 与 limit 匹配（或用 `automaxprocs`）。

### 19.2 竞态与完整性验收（与开销 A/B 无关，不可省略）

- **必须跑 `go test -race`**，且要覆盖两条已知的并发读写路径：
  (a) 客户端 `ctx.Done()`/`reqCancel` 提前返回时外层读 `ClientRequestTrace`（第 4.1 节 D）；
  (b) 服务端 `bufferedWriter` 的 marker 状态在 serve goroutine 与 `writeFrameAsync`
  goroutine 之间交替访问（第 15.1 节 C）。

  特别注意 (b)：`writeResHeaders.staysWithinBuffer()` 恒为 `false`，因此 response HEADERS
  走的是 `startFrameWrite` 的 `go sc.writeFrameAsync(wr, nil)` 分支——**marker 的 arm 与
  `(*bufferedWriter).Write` 并不在 serve goroutine 上执行**。无锁访问的正确性完全依赖
  `sc.writingFrame` 断言与 `sc.wroteFrameCh` 提供的 happens-before，必须用 race detector
  实测坐实，不能只靠推理。

- 每个 NF module 先跑 `go build ./... && go vet ./...`，确认本地 fork 的 `replace` 与
  `writeContext` 接口新增方法、`newBufferedWriter` 签名变更没有破坏编译（含 fork 自带测试）。

- 用 mutex/block profile（`LOCK_SCHED_PROFILING_GUIDE_0826.md` 已启用）确认没有新增会随并发
  增长的锁等待或阻塞 channel send，**特别是 W 事件 channel**（见第 19.0 节类别二）。

- **必须把三个计数器输出到日志**，否则第 19.6 节的完整率/正确性条件无法评估：
  `accesslog.Dropped()`、`http2.ResponseHeadersFlushedDrops()`、`http2.WAccountingErrors()`。
  当前实现里这三个函数都还没有任何调用点，需要补一条周期性（或随 `Flush()` 一起）输出的
  统计行。注意 `WAccountingErrors` 在任何一次连接写错误之后都会合法地 +1，因此报警条件是
  "非零**且**该轮没有连接写错误"。

### 19.3 热路径验收

代码审查和 benchmark 必须确认：

1. M/G 对每个 request 各执行一次 `time.Now()`；W 对每次跨过一个或多个 marker 的底层 socket
   Write 只执行一次，并让同批 marker 共享该 timestamp；
2. M/G 不在 `x/net/http2` 热点做 JSON、文件 I/O、阻塞 send 或 per-request goroutine 创建；
3. W 的 socket writer 不做 JSON、文件 I/O、阻塞 send、强制 Flush 或等待 collector；
4. `server_request_id` 只在 `newWriterAndRequestNoBody` 创建 trace 时做一次 atomic increment；
5. 所有新 channel send 都有 `default`/等价 non-blocking 路径，满队列只增加 atomic drop counter；
6. 新增日志不会改变 request/response 内容、HTTP/2 frame 顺序、buffer/flush 策略、flow control、
   retry 行为、handler 调度条件或错误返回。特别检查：
   - `writeResHeaders` 新增字段不改变 `staysWithinBuffer()` 的返回值（它恒为 false，
     与结构体内容无关）；
   - `armResponseHeaderMarker` 不改变 `writeHeaderBlock` 的返回值和调用次数；
   - `bufferedWriter.Write` 的返回值 `(n, err)` 与原实现逐位相同；
7. 停止/采集阶段能够先排空 W collector 再 flush/清空 `HTTP_log.txt`（第 16.3 节的七步顺序）；
8. `LogHTTP`/`LogHTTPInbound` 的初始 JSON buffer 容量必须按**实测行长**重新预留。
   **这不是优化，是修一个现有 bug。**

   对 `C6525100g_NF1HTTP_500ms_runtime_0826/HTTP_log_RQ2000_UE1000.txt` 实测（42000 行）：

   ```text
   client 行  平均 437 B, 最长 479 B   <-  现有 cap = 304   ← 每一行都溢出！
   server 行  平均 277 B, 最长 320 B   <-  现有 cap = 256   ← 每一行都溢出！
   ```

   也就是说**今天（A 组）每条日志行都会触发一次 `growslice`**：304 扩到 608、256 扩到 512，
   多一次分配 + 一次整行 memcpy。按 2000 reg/s（42000 transaction/s）估算，
   这一项每秒白白分配约 (912+768) x 42000 ≈ **70 MB/s**，而按实际长度预留只需约 30 MB/s。

   **修正后的建议容量**（含 M/G/W 新增字段）：

   | 行 | 实测现长(avg/max) | 新增字段 | 建议 cap |
   |---|---|---|---|
   | client（`LogHTTP`） | 437 / 479 | M 约 +62、`stream_id` +18、`retry_count` +15 = **+95** | 304 -> **640** |
   | server（`LogHTTPInbound`） | 277 / 320 | `server_request_id` +42、`conn` +30、`stream_id` +18、G +60 = **+150** | 256 -> **512** |
   | W（新建） | — | 约 230 | **320** |

   注意 RFC3339Nano 会去掉纳秒尾部的零，所以行长有波动；上表按 max 留余量。
   注意这一项同时让原六点变快，所以它不是 M/G/W 的成本，报告时要与新增点的成本分开陈述。

### 19.4 【新增】高 RQ（0.5 ms 一个 UE reg）下的必做项与优先级

第 19.0 与 19.5 节是按"打点位置"组织的。这一节按"在 2000 reg/s 下真正会影响 UE 注册时延的
东西"重新排序，因为实测数据把优先级完全改变了。

#### 前提：内存大不是这里的约束条件

内存够大只解决一件事——日志队列不会满、不会 drop。它**不解决**任何一项时延问题，
而且现有代码里有一处正是"因为内存大"才设成这样、反而成了 GC 负担：

```go
queueCapacity = 1 << 21   // 2097152
type record struct {
    kind recKind   // uint8
    line []byte    // 24 B
}                  // -> 32 B/entry
```

`2^21 x 32 B = ` **64 MiB**，而且 `record` 含指针，所以 Go 在 `makechan` 时走的是
"元素含指针"分支，`hchan.buf` 是一个 **64 MiB 的含指针对象，每个 GC cycle 都要完整扫描**，
每个 NF 进程一份。队列是不是满的都要扫。

**这一项与新增日志无关，今天就存在**，但加上 W 之后 GC 变频繁，扫描代价会被乘上去。

#### 各 NF 受 W 影响的程度差异极大（实测）

W 只由 **server 端**产生，所以纯 client 的 NF 完全不受影响：

| NF | 现有行数/1000UE | 新增 W 行 | 增幅 | 2000 reg/s 时 |
|---|---|---|---|---|
| **AMF** | 9000 | **0** | **+0%** | 18000 行/s 不变 |
| UDM | 17000 | 8000 | +47% | 34000 -> 50000 行/s |
| **UDR** | 10000 | 10000 | **+100%** | 20000 -> 40000 行/s |
| AUSF | 4000 | 2000 | +50% | 8000 -> 12000 行/s |
| PCF | 2000 | 1000 | +50% | 4000 -> 6000 行/s |
| 合计 | 42000 | 21000 | +50% | — |

两条重要推论：

1. **AMF——已知的瓶颈 NF——新增日志行数为 0。** 在本注册流程里 AMF 是纯 client
   （实测 `by dst` 里没有 amf），不接收任何被测 SBI 请求，因此不产生 W。
   AMF 只承担 M 带来的"client 行变宽"，以及每出站请求 3 次 atomic + 1~2 次分配。
   **这对本实验是个很有利的结构性事实，应当写进结论。**
2. **UDR 受影响最大（+100%）**，因为它是纯 server，现在只写 server 行。
   但 UDR 此前已被排除为瓶颈（48 核宿主机平均约 12% CPU），且它不在 AMF 的
   goroutine 排队路径上。

#### 必做项（按收益排序；这些都同时让原六点变快，成本要与 M/G/W 分开报告）

| 优先级 | 项目 | 消除的开销（2000 reg/s，全系统） | 说明 |
|---|---|---|---|
| **P1** | **修正 buffer 预留容量**（见 19.3 第 8 条） | 约 **-40 MB/s** 分配 + 每行一次整行 memcpy | 修现有 bug。**分配量上收益最大，但要说清楚它的直接同步开销只有约 40-80 ns/transaction**——真正的价值是降 GC 压力。在 CPU 有余量时 GC 后台 worker 能吸收大部分，所以这是"白捡的收益"，不是"不修就会出事" |
| **P2** | **行缓冲复用**（`sync.Pool`，writer 写完归还） | 约 **-45 MB/s** 分配 | 这是剩下的最大来源。生产者取、writer 还；drop 的那条不归还即可 |
| **P3** | **`queueCapacity` 按实测峰值下调** | 每 GC cycle 少扫 **62 MiB**（64 -> 2 MiB） | 先测峰值占用；若从未超过几千，`1<<16` 足够。或改成不含指针的元素类型 |
| **P4** | **`AppendFormat`**（见 19.5 手段 1） | 约 **-12 MB/s** 分配 + 约 270 字节/tx 的转义扫描 | 顺带让原六点也变快 |
| **P5** | **R1/R3**（trace 内联进 stream、trace 兼作 ctx 节点） | 约 -2 次分配/tx | 其中 R1 在 serve goroutine 上，价值高于其字节数 |
| **P6** | **`scheduleHandler` 传指针而非查 context** | serve goroutine 上少一次链表 walk | 见 12.1 节 B |
| **P7** | key 用常量片段替代逐字节转义 | 约 -120 次 switch 迭代/行 | 不省分配，只省指令 |

把 P1+P2+P4 做完之后，日志路径的分配速率大致是：

```text
今天（A 组，6 点）          约 70 MB/s   ← 含每行一次 growslice
只加 M/G/W 不做优化（B）    约 105 MB/s
做完 P1                     约 45 MB/s
做完 P1+P2                  约 12 MB/s
做完 P1+P2+P4               约 5 MB/s
```

**也就是说：做完 P1/P2/P4 之后，带 9 个点的 B 组分配速率比今天 6 个点的 A 组低一个数量级。**
这不是因为新增点免费，而是因为顺手修掉了现有的浪费。

（上面每个数字都是从实测行长 + 已知 transaction 速率推算的估算，
不是 benchmark 结果。必须用 19.5 节那个 `bench_test.go` 和 `GODEBUG=gctrace=1` 实测替换。）

#### P1 的具体改法（可直接照抄，七个 NF 各一份）

文件：`NFs/<nf>/internal/accesslog/accesslog.go`。实测各日志行长度与现有预留容量：

| 函数 | 实测 avg / max | 现有 cap | 结论 | 改成 |
|---|---|---|---|---|
| `LogHTTP`（client 行） | 437 / 479 | 304 | **每行都溢出** | **640** |
| `LogHTTPInbound`（server 行） | 277 / 320 | 256 | **多数行溢出** | **512** |
| `LogDB` | 253 / 269 | 256 | 约半数溢出 | **320** |
| `LogWorker` | 519 / 963 | `256 + n*128` | 基数偏小 | **`384 + n*160`** |
| `LogNGAP` | 138 / 145 | 160 | **没有溢出，不要动** | 160（不变） |

上表是**加 M/G/W 之前**的实测。加上新字段后 client 行约 +95 B、server 行约 +150 B，
所以 640 / 512 已经把新字段算进去了。

具体的三处 diff：

```go
// LogHTTP：304 -> 640
// 实测 client 行 avg 437 / max 479（尚未含 M/stream_id/retry_count 的约 +95 B）。
// 原值 304 导致每一行都触发一次 growslice + 整行 memcpy。
- b := make([]byte, 0, 304)
+ b := make([]byte, 0, 640)

// LogHTTPInbound：256 -> 512
// 实测 server 行 avg 277 / max 320（尚未含 server_request_id/conn/stream_id/G 的约 +150 B）。
- b := make([]byte, 0, 256)
+ b := make([]byte, 0, 512)

// LogDB：256 -> 320   实测 avg 253 / max 269，卡在边界上
- b := make([]byte, 0, 256)
+ b := make([]byte, 0, 320)

// LogWorker：基数与每 SBI 增量都偏小（实测 avg 519 / max 963）
- b := make([]byte, 0, 256+len(sbi)*128)
+ b := make([]byte, 0, 384+len(sbi)*160)
```

W 行是新增的，初始容量直接给 **320**（实测同类字段约 230 B）。

`LogNGAP` 的 160 是唯一预留正确的一个（avg 138 / max 145），**不要改**——
改大它只会白占内存。

**这属于基础设施级改动**（它同时让原六点变快），报告时必须与 M/G/W 的新增成本分开陈述，
否则会把新增点的成本掩盖掉。

**关于它到底有多重要，要说实话：** 直接的同步开销只有约 40-80 ns/transaction
（一次多余分配 + 一次整行 memcpy）。真正的价值是把分配速率从约 70 MB/s 降到约 45 MB/s，
降低 GC 频率。在 CPU 有余量时 GC 后台 worker 能吸收大部分，所以这是
**"改两个常量白捡的收益"，不是"不改就会出事"**。之所以排在 P1，是因为它收益/改动比最高，
而不是因为它最危险。

#### 与分配无关、必须单独盯的三项

P1-P7 全是"少分配"。下面三项无论怎么优化都还在，而且恰好落在最敏感的位置：

1. **serve goroutine 上 G 的取时与赋值**（每连接串行点）——这是被测量本身，不能省，
   但必须保证那里除了 `time.Now()` 和一次赋值没有别的（P5/P6 就是为此）。
2. **W 事件的 chansend**（进程级共享 channel 锁）——见 19.0 类别二。
   2000 reg/s 时 UDR 上是 20000 次/s，`connsPerPeer = 1` 下竞争者只有个位数条连接，
   预计可忽略；但必须用 mutex profile 确认，且 `connsPerPeer` 调大后要重测。
3. **`enqueue` 从 2 次/tx 变 3 次/tx**——共享 `queue` 的 chansend。这是 W 的必然代价。

### 19.5 【新增】如何降低 T4 -> response 这一段的新增成本

第 19.0 节类别一里最需要处理的是这一项：`InboundLogger` 在 T4 之后、`rw.handlerDone()`
之前同步构造 JSON，它不只是让 `W - T4` 变大（那还只是测量偏差），而是**真的让对端晚收到
response**。三种缓解手段，按"改动量 / 收益"排序。

#### 手段 1（强烈推荐，收益大改动小）：用 `AppendFormat` 消灭时间戳的 string 分配

现状（`accesslog.go`）：

```go
func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)     // 每次调用【分配一个 string】
}
...
b = appendKV(b, "req_time", formatTime(reqTime), false)
//              ^ appendJSONString 再逐字节扫一遍做转义
```

每个时间戳因此付出：**1 次 string 分配 + 1 次逐字节转义扫描**。client 行 4 个、server 行
2 个，加上 M/G/W 之后是 5 / 3 / 1。

改成直接 append，零分配、零转义：

```go
// RFC3339Nano 的输出只含 0-9 - : . T Z，没有任何需要 JSON 转义的字符，
// 因此可以直接 append，不必走 appendJSONString。
func appendKVTime(b []byte, key string, t time.Time, first bool) []byte {
	if !first {
		b = append(b, ',')
	}
	b = appendJSONString(b, key)
	b = append(b, ':', '"')
	if !t.IsZero() {
		b = t.UTC().AppendFormat(b, time.RFC3339Nano)   // 直接写进 b，无分配
	}
	return append(b, '"')
}
```

（`t.UTC()` 返回值类型，不分配；`AppendFormat` 自 Go 1.5 起就有。零值仍然输出 `""`，
与现有 `formatTimeOrEmpty` 的约定一致。）

**为什么 `Format` 一定会分配（Go 源码语义，非猜测）：**

```go
func (t Time) Format(layout string) string {
	const bufSize = 64
	var b []byte
	max := len(layout) + 10
	if max < bufSize {
		var buf [bufSize]byte      // RFC3339Nano len=35, 35+10=45 < 64 -> 走栈
		b = buf[:0:bufSize]
	} else {
		b = make([]byte, 0, max)
	}
	b = t.appendFormat(b, layout)
	return string(b)               // <<< 这一步把栈上的字节【拷进堆】，1 次分配
}
```

所以每次 `formatTime` 恰好 1 次堆分配，大小是格式化后的真实长度（约 30 B，落在 Go 的
32 B size class）。`AppendFormat` 直接写进调用者的 `b`，只要容量够就是 **0 分配**。

**收益的量级（估算，必须实测确认）：**

| | A 组（原六点） | B 组（+M/G/W） | 用 AppendFormat 后 |
|---|---|---|---|
| 每 transaction 时间戳分配次数 | 6 | 9 | **0** |
| 每 transaction 时间戳字节 | 约 192 B | 约 288 B | 0 |
| 每 transaction 转义扫描字节 | 约 180 | 约 270 | 0 |

按"小对象分配约 20-30 ns、逐字节 switch 约 1-2 ns/byte"的常识量级估，
**直接 CPU 节省大约 0.5-1 µs / transaction**。

**必须诚实地说清楚：这个量级相对本实验要研究的现象是可以忽略的。**
已有测量显示 AMF-local 时延随 RQ 从 4 ms 涨到 44 ms（见
`Ty_log/Free5gc/C6525100g_TrueTR_0721_20ms` 的结论），1 µs 是它的 0.002%。
**所以 AppendFormat 不会"大幅降低" p50 或均值。**

它真正值得做的理由是另外两个：

1. **分配速率**。按每 transaction 约 288 B、全核心 42000 transaction/s 估算，
   仅时间戳字符串就是约 **12 MB/s 的垃圾**（分摊到 7 个 NF 进程）。消掉它降低 GC 频率，
   而 GC assist 是随机落在业务 goroutine 上的——**这部分只影响 p99，且不会出现在九点的
   任何一段里**——它只会体现在端到端 registration latency 的 p99 上。
2. **它让 B 组的日志路径比 A 组更便宜**，从而把 W 那条新增日志行的成本部分抵掉。
   问题从"打点会不会拖慢系统"变成"我们既让日志更便宜、又多了一个点"。

**这个改法的缺点/风险（四条，都不大但要知道）：**

1. **缓冲区预留变得更重要，而不是更不重要。** `Format` 用的是自己的 64 B 栈缓冲，
   写不满不会影响你的 `b`；`AppendFormat` 直接写进 `b`，容量不够就触发 `growslice`
   **把整行已写内容拷贝一遍**——比原来更糟。所以第 19.3 节第 8 条的容量上调
   （304 -> 416、256 -> 416）从"优化"升级为"前置条件"。
2. **"不需要转义"这个前提有隐含依赖。** 它成立是因为 (a) 先调了 `.UTC()`，时区恒为 `Z`；
   (b) layout 是 RFC3339Nano，只含 `0-9 - : . T Z`。若以后有人去掉 `.UTC()` 或换 layout，
   前提可能不再成立。**必须在函数上写明这个不变式**，否则是个隐藏地雷。
   （不过即使换 layout，Go 的标准 layout 里也不含 `"` 或 `\`，不会破坏 JSON 语法，
   最坏是输出不合预期。）
3. **输出长度是可变的**，20-30 字符不等：RFC3339Nano 的 `.999999999` 会**去掉尾部的零**，
   纳秒恰为 0 时连小数点都没有。这是**现有行为**，不是 AppendFormat 引入的；
   离线脚本的 `_TS_RE` 已经用 `(?:\.(\d+))?` + `(frac+"000000000")[:9]` 正确处理了。
   但它意味着上面的字节估算是上界。
4. **不要在重构时误碰 monotonic clock。** 现有 `latency_us` 用的是
   `respTime.Sub(reqTime)`，两个操作数都还带 monotonic reading，减法走的是单调时钟。
   `t.UTC()` 会**剥掉** monotonic reading，所以重构时**绝不能**把
   `reqTime = reqTime.UTC()` 这种赋值提到 `Sub` 之前——只在 append 的那一刻取 `.UTC()`。

**输出字节完全不变**，所以现有分析脚本一行都不用改。这是它相对手段 2 最大的优势。

#### 建议先跑的 benchmark（本机没有 Go，请在编译节点上跑）

放到 `NFs/udr/internal/accesslog/bench_test.go`：

```go
package accesslog

import (
	"testing"
	"time"
)

var sink []byte

func BenchmarkLineFormatOld(b *testing.B) {
	t0 := time.Now()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 0, 304)
		buf = append(buf, '{')
		buf = appendKV(buf, "req_time", formatTime(t0), true)
		buf = appendKV(buf, "wrote_time", formatTime(t0), false)
		buf = appendKV(buf, "got_first_byte", formatTime(t0), false)
		buf = appendKV(buf, "resp_time", formatTime(t0), false)
		buf = append(buf, '}')
		sink = buf
	}
}

func BenchmarkLineFormatAppend(b *testing.B) {
	t0 := time.Now()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 0, 416)
		buf = append(buf, '{')
		buf = appendKVTime(buf, "req_time", t0, true)
		buf = appendKVTime(buf, "wrote_time", t0, false)
		buf = appendKVTime(buf, "got_first_byte", t0, false)
		buf = appendKVTime(buf, "resp_time", t0, false)
		buf = append(buf, '}')
		sink = buf
	}
}
```

`go test -bench=LineFormat -benchmem ./internal/accesslog/` 应当看到
`allocs/op` 从 5（4 个时间戳 + 1 个 buffer）降到 1（只剩 buffer），
`ns/op` 下降的绝对值就是每行真实节省。**用这个数字而不是上面的估算去谈收益。**

#### 顺带：keys 也可以省一遍逐字节扫描

`appendKV` 对**每个 key** 都跑一遍 `appendJSONString` 的逐字节 switch，
而 key 全是编译期常量、不含任何需要转义的字符。整行 12 个 key、平均 10 字符，
就是约 120 次无谓的 switch 迭代。可以直接写成常量片段：

```go
b = append(b, `,"req_time":`...)   // 一次 memmove，替代 append+逐字节扫描+append
```

指令数上这一项甚至比时间戳那项更大（虽然不省分配）。属于同一批「顺带修掉现有浪费」的改动，
要做就一起做，并与 M/G/W 的成本分开报告。

#### 手段 2（收益最大，改动中等）：把 server 行的 JSON 构造整体移出 T4 路径

与 W 用同一个模式：`InboundLogger` 只把一个**定长 struct** 推进 collector channel，
JSON 由 collector goroutine 构造。

```go
type inboundEvent struct {
	Method   string      // string header 拷贝，无分配
	URI      string
	UeID     string
	ConnID   string
	SrvReqID uint64
	StreamID uint32
	HandlerGo, ReqTime, RespTime time.Time
}
```

T4 之后只剩：读 trace（几次字段读）+ 一次结构体赋值 + 一次 non-blocking chansend。
**整段 JSON 格式化、append、buffer 扩容全部离开关键路径。**

三点提醒：

1. `method` / `uri` / `ue_id` 都是已经存在的 string，放进 struct 只拷贝 header，不分配。
   但 **`uri` 来自 `inboundURI(c.Request)`，它内部 `u.String()` 本来就会分配一个 string**——
   这笔开销在 T3 之前，与是否新增 M/G/W 无关，不用动。
2. 结构体较大（约 130 B），按值进 channel 会拷贝。相比省下的格式化，仍然划算。
3. 这同样是**同时让原六点变快的基础设施改动**，而且它把 client 行也一并搬走才公平
   （否则 client/server 两侧的日志路径不对称，两侧的数字不再可比）。

#### 手段 3（不推荐，仅记录）：让 W collector 合并输出一条 server 行

即第 16.4 节的备选方案：T4 之后不输出，等 W 到达再由 collector 合并成一行。
它能让 T4 路径上只剩一次 chansend，但要为每个在途 request 保留 record、还要处理
W 永不到达时的超时回收，复杂度明显更高，且会引入一个新的按 request 的 map。不作为首选。

#### 还有一个不能动的部分

`c.Next()` 返回到 `handler` 返回之间，除了我们的 access log，还有 gin 的 middleware 链
和 `metrics.InboundMetrics()` 的收尾。那部分 A/B 两组相同，不在本计划范围内。

#### 做完手段 1 之后，M/G/W 还剩下哪些额外开销

这是一份"扣掉可优化项之后的剩余清单"。分成"还能再省"和"省不掉"两栏。

**还能再省的三项（建议一并做掉）**

| # | 剩余开销 | 怎么省 |
|---|---|---|
| R1 | ~~服务端每 request 一个 `&ServerRequestTrace{}` 分配~~ | **已提升为默认设计，见 12.1 B**：trace 内联进 `stream`（值字段），`stream` 自己实现 `context.Context`。同时消掉 trace 分配与 `context.WithValue` 节点两次分配。唯一代价：marker FIFO 的指针会让整个 `stream`（约 200 B）多存活到 W 触发，微秒级，可接受 |
| R2 | `server_request_id` 的进程级 `atomic.Uint64.Add`。它是**全进程共享的一个 cache line**，UDR 在高 RQ 下每请求都要抢一次 | **它其实是冗余的**：server JSON 与 W event 的 join 完全可以用 `(dst, conn, stream_id)`——两边本来就都有这两个字段，而且这个 join 是**同一个 Pod 内部**的，`conn` 不需要跨 Pod 规范化，比 client<->server 那个 join 更安全。去掉后省：1 次共享 atomic、trace 里 8 B、server 行约 42 B、W 行约 42 B。**若保留，也应改成 `sc` 上的每连接计数器**（serve goroutine 单线程，连 atomic 都不需要），join key 用 `(dst, conn, server_request_id)` |
| R3 | 客户端 2 次分配：`&ClientRequestTrace{}` + `WithClientRequestTrace` 的 context 节点 | 让 `ClientRequestTrace` **自己实现 `context.Context`**（内嵌 parent ctx，`Value()` 命中自己的 key 时返回 self），一个对象同时当 trace 和 ctx 节点，**2 次分配降到 1 次**。约 20 行，无行为变化 |

> 注意 R2 会改动第 17.3 节的 mandatory correlation fields。如果选择去掉
> `server_request_id`，第 0.2 / 16.2 / 17.3 / 18 节里所有
> `(dst, server_request_id)` 都要同步改成 `(dst, conn, stream_id)`。
> **本计划暂时保留 `server_request_id` 作为默认**（多一个独立 ID 便于交叉校验，
> 且不受端口复用影响），R2 作为 profile 显示该 atomic 有竞争时的第一优化项。

**省不掉的（这些就是"要拿到这三个点必须付的价"）**

| # | 剩余开销 | 为什么省不掉 |
|---|---|---|
| K1 | **第三条日志行本身**：多 1 次 `enqueue`（共享 `queue` 的 chansend + 锁）、多 1 条记录进 `writerLoop`、文件字节约 +2 倍 | 这就是"有 W"的定义。唯一能调的是采样率，但那会破坏第 0.2 节的完整性纪律 |
| K2 | **W 事件 channel 的 chansend**（进程级共享锁，见第 19.0 节类别二） | 可以靠定长批量把次数从 N 降到 ceil(N/8)，但降不到 0 |
| K3 | serve goroutine 上 G 的 `time.Now()` + 赋值 + context 节点分配 | context 节点是把 trace 暴露给 `InboundLogger` 的唯一途径（`*http.Request` 没有别的挂载点）。`time.Now()` 就是被测量本身 |
| K4 | `(*bufferedWriter).Write` 里**每帧**一次 nil 检查 + `uint64` 加法 | 要按字节偏移精确定位 header block 末字节，就必须逐帧记账 |
| K5 | 客户端 3 次 atomic store + 3 次 atomic load | 第 4.1 节 D 的竞态是真实的，不能退回普通字段 |
| K6 | W collector goroutine 构造 W 行 JSON 时的 1 次 `make([]byte,0,320)` | 在 collector 上，**不在热路径**；只算背景 CPU |

**汇总：热路径上的每 transaction 新增堆分配**

```text
原始方案（本计划 v2 初稿）        约 7 次
+ 手段 1（AppendFormat）          -3   （M/G/W 三个时间戳字符串）
+ 预留容量到位                    -0~2 （消掉 realloc，取决于原来是否溢出）
--------------------------------------------------
小计                              约 4 次   ← 这是"照现在的 plan 实现"的结果
+ R1（trace 内联进 stream）       -1
+ R3（trace 兼作 ctx 节点）       -1
--------------------------------------------------
下限                              约 2 次   （server 的 ctx 节点 + client 的合并对象）
```

也就是说：**做完手段 1 加 R1/R3，三个新点在热路径上的净分配增量可以压到约 2 次/transaction**，
其余全是 atomic、整数加法和 chansend。相对而言，`enqueue` 从 2 次变 3 次（K1）反而成了
最大的一项——而它是 W 存在的必然代价，不是可以优化掉的实现细节。

#### 结论

**先做手段 1**——它几乎没有风险，收益直接，而且顺便让原六点也变快。
做完之后再测 `W - T4`；如果新增点在这一段上的净增量仍然超出扰动预算，再上手段 2。

### 19.6 正式实验准入条件

- （开销 A/B 已按实验负责人决定移出本计划范围。）使用九点数据解释系统 latency 时，必须显式
  报告已知的 instrumentation effect：`InboundLogger` 变宽的 JSON 落在 `W - T4` 与对端 T5/T6 上、
  每 transaction 多一条 W 日志行、以及 GC 尾延迟不出现在任何一段九点差值里；
- HTTP/W 日志队列的 drop/missing rate 低于实验前确定的允许上限；完整率必须随 NF、URI 和
  request rate 一起报告，不能隐藏选择性缺失；
- exact-join 预检无 duplicate/ambiguous match，且 client `conn` 与 fork 快照 `LocalAddr`
  100% 一致；允许少量 missing/unmatched，但必须作为 incomplete sample 排除，禁止近似补配；
- `go test -race` 在两条已知并发路径上通过；没有新增 mutex/block 热点、连接 churn 或强制 Flush；
- 跨 Pod 时间段分析所需的时钟同步/偏移审计已经通过。

---

## 20. v1 → v2 修订清单（逐条对照实际代码）

| # | v1 的说法 | 实际代码 | 影响 | v2 处理 |
|---|---|---|---|---|
| 1 | 六个原有打点行号 T1≈252 / T2≈241-244 / T5≈246-248 / T6≈253-254 / T3≈353 / T4≈360-361 | 真实为 261 / 250-254 / 255-257 / 263 / 362 / 370 | 照 v1 找不到位置 | 第 2.1 节全部改正，并补上 `GotConn` 与 T3 之前两层 middleware |
| 2 | W 取时点在 `writeWithByteTimeout` | 包级函数无连接状态；且本部署 `WriteByteTimeout == 0`，第一行就 `return conn.Write(p)`，v1 的 `for` 循环不执行 | **按 v1 写会拿不到 W** | 第 15.1 节 C 改为 hook `(*bufferedWriterTimeoutWriter).Write`，状态放 `bufferedWriter` |
| 3 | marker 偏移 = `producedBytes() + 9 + len(frag)` | 数值在当前形态下恰好对，但硬编码了"HEADERS 无 padding/priority"；且 v1 未说明登记必须早于 `bufio` 内部自动 flush | 脆弱 + 潜在漏记 | 改为 `writeHeaderBlock` 挂 pending、`(*bufferedWriter).Write` 内以 `produced + len(p)` 结算 |
| 4 | 在 `writeResHeaders` 运行时判断 `httpResCode >= 200 && trailers == nil` 且"尚未登记" | `writeResHeaders{}` 只有三个构造点；最终 response 那个在 `if !rws.sentHeader` 内，每 response 恰好一次 | 多余的运行时状态 | 只在 `server.go:2717` 填 `trace`，判断与状态位全删 |
| 5 | JSON 用 `connection_id` | 现有 client 行字段是 `conn`（还有 `conn_slot`/`conn_reused`/`latency_us`），无 `stream_id`/`retry_count` | 字段名对不上既有脚本 | 第 6 节给出真实字段集，统一用 `conn` |
| 6 | server connection identity 需从 fork trace 取 | `c.Request.RemoteAddr` 即 `sc.remoteAddrStr`，`InboundLogger` 直接可读 | 白做一份 fork 改动 | 第 11 节表格区分"需要 fork"与"不需要" |
| 7 | `T4 <= W` 只说"普通小 response" | 阈值精确为 `handlerChunkWriteSize = 4 KiB`（`server.go:59`） | 分组标准不可判定 | 新增第 14.6 节 |
| 8 | 未提批量写 | `writeResHeaders.staysWithinBuffer()` 恒 false；`flushFrameWriter` 只在 writeSched 排空后触发 | 漏掉本实验最可能的核心现象 | 新增第 14.5 节 + `batch_size` 字段 |
| 9 | 未提 `sc.writeHeaders` 阻塞 handler | `serveG.checkNotOn()` + `<-errc` 同步等待帧写完 | 低估了 W 路径阻塞的后果 | 新增第 14.7 节 |
| 10 | "M 用 atomic.Pointer 快照" | 方向对，但未指出 `roundTrip` 在 `ctx.Done()`/`reqCancel` 分支会**在 writeRequest 仍在跑时返回** | 竞态理由说不清，易被简化成普通字段 | 第 4.1 节 D 给出具体竞态路径 + 零 alloc 的多 atomic 方案 |
| 11 | 未提 retry 时 request 是浅拷贝 | `shouldRetryRequest` 用 `newReq := *req`，ctx 被复制，trace 跨 attempt 复用 | retry 语义会被误设计 | 第 4.1 节 C 说明；`wroteTime` 保留首次、`connID` 保留末次的既有不对称也一并指出 |
| 12 | 未提 `streamf` 恒为 nil | `(*ClientConn).RoundTrip` 就是 `cc.roundTrip(req, nil)` | 会误以为填 `streamf` 就够 | 第 4.1 节 B |
| 13 | G 只从语义角度要求"在 `go` 之前" | `go` 语句同时提供 happens-before | 不知道可以用普通赋值 | 第 10 节开头补内存模型理由 |
| 14 | 未定义 `writeContext` 接口改动的风险 | 非测试代码中只有 `*serverConn` 实现 | 无风险 | 第 15.1 节 B 明确可安全加方法 |
| 15 | 未提 `accesslog.Flush()` 不认识 W 队列 | 现有 `Flush()` 只 drain `queue` | 最后一批 W 会丢 | 第 16.3 节给出七步采集顺序 |
| 16 | `ue_id` 当作可用标注 | `sniffUEID` 只覆盖两个 POST 路径，其余请求 `ue_id` 为空 | 高估了 UE 归属能力 | 第 17.2 / 17.4 节要求从 `uri` 正则提取 |
| 17 | 未提本地端口复用 | 长连接下罕见，但 `conn_reused == false` 说明池会重建 | duplicate key 隐患 | 第 17.3 节新增端口复用风险与 `conn_epoch` 兜底 |
| 18 | h2c upgrade / server push 作为"可能缺 G 的样本" | 本部署两条路径均不可达（prior-knowledge h2c + 无 Push 调用） | 会误报"正常缺失" | 第 10.3 节改为"若出现必须查因" |
| 19 | 未给 buffer 容量的具体数字 | 现值 304 / 256 | 每 request 一次 realloc | 第 19.3 节第 8 条给出估算与建议值 |
| 20 | 未说明七个 NF 代码的重复度 | `httptransport.go` 七份逐字节相同；`accesslog.go` 仅 `srcNF` 与 AMF 专有 sink 不同 | 影响工作量估计 | 第 4.2 / 12.2 节注明"改一份复制七份" |
| 21 | 客户端只说"把 trace 挂进 request" | 若写成第二次 `req.WithContext`，每个出站 request 会多复制一整个 `http.Request` 结构体并多一次分配 | 白白的 per-request 开销 | 第 4.2 节要求两个 trace 合并进同一条 ctx 链，只调一次 `WithContext` |
| 22 | G 的 context 查找"成本可忽略" | `scheduleHandler` 跑在**每条连接唯一的 serve goroutine** 上，是该连接所有 stream 的串行瓶颈 | 会按 stream 数放大，污染 `G - T2` 本身 | 第 12.1 节 B 改为推荐沿调用链传 `*ServerRequestTrace`；context 查找降级为备选 |
| 23 | "marker 状态 connection-local，不新增共享 mutex" | 状态确实是 connection-local，**但 W 事件 channel 是每 NF 一条、所有连接共用**；非阻塞 chansend 仍要拿 channel 锁 | 这是本计划在 HTTP/2 写路径上引入的**唯一**进程级共享同步对象 | 新增第 19.0 节类别二：要求专门 profile，并建议按 Write 批量投递把 chansend 从 N 次降到 1 次 |
| 24 | UE 归属只笼统说"从 URI 提取" | 已逐条核对路由：`authCtxId` 就是 `supiOrSuci`（`ue_authentication.go:310`）、`polAssoId` 是 `supi-N`（`ampolicy.go:227`）；而 NSSF 的 `network-slice-information` 查询**完全没有 UE 标识** | 高估或低估都会误导分析 | 新增第 17.5 节给出逐路由覆盖表与 NSSF 这个真实缺口 |
| 25 | 只笼统说"现有脚本按 UE/URI 近似配对" | 核对 `HTTP_per_UE_transport_latency_2way.py` 后确认有**三处**具体的时间依赖：排序+下标配对、父窗口包含、最近时间戳；且 `min(len(c),len(s))` 会在日志缺一条时**静默错位** | 六点时代 `udm<->udr`（每 UE 9 次）的配对本身就带不确定性 | 新增第 17.6 节逐条对照，并说明九点方案如何把这三处全部消除；同时指出 `_ID_RE` 对 `polAssoId` 的贪婪匹配 bug |
| 26 | §17.6 v2 初稿说 `min(len(c),len(s))` 会在日志缺一条时静默错位 | **在 1+1 的桶里不成立**：少一条时 `min(1,0)=0`，样本被跳过而非错配。实测 42000 = 1000x42 恰好对齐，38000 个桶大小为 1，只有 am-data 的 2000 个桶为 2 | 上一版把风险说重了 | §17.6 已按实测数据重写：20/21 个 hop 今天就只靠 `ue_id + uri` 即可，am-data 是唯一例外 |
| 27 | §15.1 B 只说"在 Framer write 之前挂 pending" | `WriteHeaders` 的 `errStreamID` 前置检查与 `endWrite` 的 `ErrFrameTooLarge` 会在**触达 `bufferedWriter.Write` 之前**返回错误，pending 不被消费 | **会挂到下一帧上，把别的 stream 的偏移当成本 stream 的 W——静默错配** | §15.1 B 增加错误路径 `ctx.armResponseHeaderMarker(nil)` 清理，并明确 pending 的不变式 |
| 28 | §19.0 建议 W 事件"携带定长数组或 slice"做批量 | slice 会在 socket writer 里分配，直接违反 §15.3"底层写路径不做分配" | 自相矛盾 | §19.0 改为强制定长数组 `[8]uint64`/`[8]uint32` + 计数，并注明先按单 marker 实现、profile 后再上批量 |
| 29 | 未给出 T4 -> response 段的缓解手段 | `formatTime` 每个时间戳都 `Format()` 分配一个 string 再逐字节转义 | 这段直接推迟对端收到 response | 新增 §19.5：`AppendFormat` 零分配写法（每 transaction 省 9 次分配 + 9 次转义扫描），以及把 server 行 JSON 整体移出 T4 路径的方案 |
| 30 | §19.5 手段 1 只说"零分配"，没说代价 | `Format` 走 64 B 栈缓冲后 `string(b)` 分配；`AppendFormat` 直接写进调用者的 `b`，**容量不够会 growslice 拷贝整行**，比原来更糟 | 缓冲区预留从"优化"变成"前置条件" | §19.5 补四条风险：预留必须到位、不转义的不变式要写注释、RFC3339Nano 长度可变（20-30 字符）、重构时不能把 `.UTC()` 提到 `Sub` 之前（会剥掉 monotonic clock） |
| 31 | 未评估"做完优化后还剩什么" | 热路径分配可从约 7 次压到约 2 次；剩下最大的一项反而是 `enqueue` 从 2 次变 3 次 | 缺少收敛目标 | §19.5 新增剩余开销清单，分"还能再省(R1-R3)"与"省不掉(K1-K6)" |
| 32 | `server_request_id` 用进程级 `atomic.Uint64` | 它是**全进程共享的一个 cache line**，UDR 高 RQ 下每请求抢一次；而 server<->W 的 join 用 `(dst, conn, stream_id)` 就够——这个 join 在**同一 Pod 内**，`conn` 无需跨 Pod 规范化，比 client<->server 的 join 更安全 | 该字段是冗余的 | §19.5 R2 记录：默认保留（便于交叉校验、免疫端口复用），但列为 profile 见到竞争时的第一优化项；若去掉需同步改 §0.2/16.2/17.3/18 的 key |
| 33 | 无法在本机 benchmark | 开发机没有 Go | 收益只能估算 | §19.5 给出可直接在编译节点运行的 `bench_test.go`，要求用实测 `allocs/op` 与 `ns/op` 替代估算值 |
| 34 | §19.3 第 8 条建议 client cap 304->416、server 256->416 | **实测行长：client 平均 437 B / 最长 479 B，server 平均 277 B / 最长 320 B**（42000 行）。即现有 304/256 **今天每一行都溢出**，每行都触发一次 growslice + 整行 memcpy | 建议值本身仍然太小；且这是一个**现有 bug**，A 组今天就在付约 70 MB/s 的分配 | §19.3 第 8 条按实测重写：client 304 -> **640**、server 256 -> **512**、W **320**，并注明这是修 bug 不是优化 |
| 35 | 把"内存大"当作可以放心加大队列的理由 | `queueCapacity = 1<<21` x `record`(32 B) = **64 MiB 含指针的 hchan.buf**，每个 GC cycle 全量扫描，每 NF 一份；队列空也要扫 | 内存大反而变成 GC 负担 | 新增 §19.4：把 queueCapacity 按实测峰值下调列为 P3（64 -> 2 MiB） |
| 36 | 只说 W 让日志行数 +50% | **实测各 NF 差异极大**：AMF **+0%**（纯 client，不产生 W）、UDR **+100%**（纯 server）、UDM +47%、AUSF/PCF +50% | +50% 只是系统平均，掩盖了 AMF 完全不受影响这个有利事实 | 新增 §19.4 per-NF 表；结论应写明**已知瓶颈 AMF 的新增日志行为 0** |
| 37 | 优化项没有按高 RQ 收益排序 | 实测推算：修容量约 -40 MB/s、行缓冲复用约 -45 MB/s、AppendFormat 约 -12 MB/s | 之前把 AppendFormat 排在第一，实际它只排第三 | 新增 §19.4 P1-P7 优先级表；做完 P1+P2+P4 后 9 点的 B 组分配速率比今天 6 点的 A 组低一个数量级 |
| 38 | §12.1 原写法把 trace 与 context 节点都堆分配，并在 `scheduleHandler` 里查 context | 这三样全落在**每条连接唯一的 serve goroutine** 上；`connsPerPeer=1` 时 UDM->UDR 全部约 18000 req/s 串行经过它。宿主机 CPU 有余量**不代表**这个 goroutine 有余量 | 会污染 `G - T2` 这个被测量本身 | §12.1 A/B/C/D 整节重写为零分配设计：trace 内联进 `stream`、`stream` 自己实现 `context.Context`（已核实与其 6 个现有方法无冲突）、`scheduleHandler`/`unstartedHandler` 传指针。serve goroutine 上只剩 `time.Now()`。并核验了从 `st` 派生子 context 仍走 `parentCancelCtx` 高效路径 |

### 20.1 仍然成立、无需修改的 v1 结论

以下 v1 的关键判断经代码核对**完全正确**，v2 原样保留：

- M 必须紧挨 `transport.go` 的 `select { case cc.reqHeaderMu <- struct{}{}: ... }` 之前
  （真实位置 1424-1429 行）；
- G 的两个位置 `server.go:2359`（`scheduleHandler`）与 `server.go:2389`（`handlerDone`）
  **行号完全准确**，且"不能在进入 `unstartedHandlers` 时写 G"的理由成立；
- `newWriterAndRequestNoBody` 确实以 `.WithContext(st.ctx)` 收尾（2328 行），是创建 server
  trace 的正确位置；（v2 在此基础上改成 `.WithContext(st)`，见 12.1 B——位置不变，只是不再额外分配 context 节点）
- `advMaxStreams` 默认 250（`defaultMaxStreams`）；
- T5 确实等到完整 header block 读取并 HPACK 解码后才触发；
- 必须 fork `x/net/http2`、不需要 fork 标准库；三阶段共用一份 fork；
- "只用 UE/URI/时间顺序不能严格配对"、"exact join 优先、缺失只排除不补配"、
  "duplicate 必须为 0"这一整套关联纪律。

### 20.2 实施顺序建议

```text
步骤 0  在 xnet/ 上打一个 pristine baseline commit（已完成：FORK_PROVENANCE.md 那一版）
        —— 之后每次 git diff 就是 instrumentation 的精确清单

步骤 1  七个 NF go.mod 加 replace + go mod tidy，不改任何代码，构建并跑一轮基线
        —— 确认 replace 本身零影响（build list 不变）

步骤 2  第一阶段 M：instrument_client.go + transport.go 五处改动 + LogHTTP 字段
        验证 T1 <= M <= T2

步骤 3  第二阶段 G：instrument_server.go + server.go 改动 + LogHTTPInbound 字段
        必须按 12.1 B 的零分配设计做：trace 内联进 stream、stream 实现 context.Context、
        scheduleHandler/unstartedHandler 传 *ServerRequestTrace 指针。
        验收标准：serve goroutine 上只多一次 time.Now()，benchmark 显示 0 allocs
        —— 此时 conn / stream_id / server_request_id 齐了，先做第 17.3 节的关联预检
        （client<->server 的 (dst, conn, stream_id) 一对一）
        这一步是整个计划的风险闸门：预检不过就不要继续做 W

步骤 4  第三阶段 W：write.go + http2.go + Server sink + accesslog W collector
        + Flush() 扩展 + 七个 sbi/server.go 注入
        验证 T4 <= W <= T5（按 4 KiB 分组），输出 batch_size 分布

步骤 5  go build/vet + go test -race（第 19.2 节两条并发路径）+ clocksource 前置检查
        + 三个 drop/error 计数器接出来，通过第 19.6 节准入条件后才采正式数据
```
