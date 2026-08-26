# HTTP/2 request 发送、服务端 handler 启动与 response 发送分段日志计划（0826）

## 0. 本计划必须满足的三项总体保证

### 0.1 三个新增时间点统一写入现有 `HTTP_log.txt`

本计划一共新增三个时间点：

```text
M = req_header_mu_start_time
G = server_handler_go_time
W = server_response_headers_flushed_time
```

三者全部进入各 NF Pod 现有的同一个 HTTP access-log sink，即 `HTTP_LOG_PATH`（未配置时为 `/tmp/HTTP_log.txt`）：

| 新增点 | 在 `HTTP_log.txt` 中的承载方式 | 是否新增独立行 |
|---|---|---|
| M | 加入现有 client request/response JSON，与 T1/T2/T5/T6 同一行 | 否 |
| G | 加入现有 server request/response JSON，与 T3/T4 同一行 | 否 |
| W | 独立的 `server_response_headers_flushed` JSON event | 是 |

本地 `x/net/http2` fork 只采集时间和传递 request/stream-scoped 状态，不直接打开或写日志文件。所有最终文件写入必须继续经过 free5GC 现有 access-log 的 `enqueue(kindHTTP, ...)` 和单 writer goroutine；不得为三个新增点创建第二个直接写 `HTTP_log.txt` 的 writer。

### 0.2 每个完整 HTTP request/response 的九点必须 exact join

一个成功、HTTP/2、无 transport retry 且三类记录都完整的 request/response，由三条逻辑记录承载九个时间点：

```text
client JSON : T1, M, T2, T5, T6
server JSON : G, T3, T4
W event     : W
```

在当前实验明确保证“同一种 NF 只有一个 Pod、每轮实验排空并清空所有 Pod 的 `HTTP_log.txt`”的前提下，离线关联必须分两步执行：

```text
server JSON  <-> W event
key = (dst, server_request_id)

client JSON  <-> server JSON
key = (dst, canonical_connection_id, stream_id)
```

`server_request_id` 只负责同一 server Pod 内的 T3/T4/G 与 W；`connection_id + stream_id` 负责跨 client/server 连接同一个 HTTP/2 transport attempt。实现后必须先在实际 Kubernetes/CNI 环境验证 client/server 两侧的 canonical connection identity 能 100% 对齐；如果不能，则必须启用第 17.3 节的 `sbi_request_id` 方案，不能回退到 UE/URI/时间顺序近似匹配。

UE ID、method 和 URI 用于在 exact join 完成后标注“这是哪个 UE 的哪个 SBI request”，不能充当关联主键。只有能从 client/server 行的 `ue_id` 或 URI 确定 UE 的交易才能声明属于某个 UE；没有 UE 身份的 NRF discovery/heartbeat 等共享请求应单独归类，不能强行分配给某个 UE。

对连接失败、写失败、timeout、cancel、底层 W 缺失、日志队列 drop 或 transport retry 的样本，不承诺九个时间值全部存在；必须保留错误/缺失状态并统计，不得伪造时间点。主分析只接受 exact join 后三类记录各恰好一条、九点均存在且 `retry_count == 0` 的完整样本。

当前实验允许少量日志缺失。缺失记录只会降低完整样本数，不能改变剩余样本的匹配关系：离线分析必须使用上述强关联 key 做 exact inner join，任何 missing/orphan 记录直接排除，禁止用相邻行、UE/URI 或最近时间戳补配。实验前需要预先确定可接受的 `missing_rate`/drop-rate 上限；允许该值非零，但 duplicate key、一个 key 命中多条记录和近似补配必须始终为 0。

### 0.3 打点不得阻塞或改变正常 HTTP/2 流程

“异步写日志”在本计划中的硬性含义是：被测 HTTP/2 热路径不得等待日志 channel、JSON 格式化、文件写入或 flush。三个新增点遵守以下规则：

- M 热点只执行一次 `time.Now()` 和 request-scoped 赋值；
- G 热点只执行一次 `time.Now()` 和 request-scoped 赋值；`server_request_id` 只在创建 server request trace 时执行一次 atomic increment；
- W socket-write 热点对每次跨过一个或多个 marker 的底层 Write 只执行一次 `time.Now()`、小状态更新和非阻塞的固定大小事件投递；同一次 Write 命中的多个 W 共享时间戳，JSON 构造由外层长期存活的 collector goroutine 完成；
- M/G 只给现有 client/server JSON 增加字段，不增加新的日志行和额外的每 request 文件写操作；
- W 使用一条独立 JSON event，但不能在 socket writer 中构造 JSON、做文件 I/O、等待 channel 或启动 per-request goroutine；
- 所有事件通道必须有界且采用 non-blocking send；队列满时宁可原子计数并丢弃日志，也不能阻塞 HTTP/2 goroutine；
- 不得为了获得 W 强制 Flush、改变 buffer 大小、frame batching、write scheduler、flow control、锁顺序或 handler 生命周期；
- 每轮实验结束时才执行“停止发流量 -> 等待在途 request/W -> drain collector -> access-log Flush -> 采集 -> 清空日志”，不得按 request flush。

异步写入只能消除日志 I/O 对请求的同步阻塞，不能声称绝对零开销；`time.Now()`、字段复制、atomic、后台 JSON/文件写入仍会消耗少量 CPU。实现必须通过第 19 节的开销 A/B 测试后才能用于正式 latency 结论，特别要覆盖 UE registration 触发间隔小于 1 ms 的最高目标负载。

## 1. 第一阶段目标：`reqHeaderMu` 竞争起点

第一阶段只增加一个新的时间点：

```text
req_header_mu_start_time
```

它表示：对于每一次向对端 NF 发出的 HTTP/2 request，在该 request **即将开始竞争当前连接的 `reqHeaderMu` 之前**记录时间。

这个时间点保持 request 粒度。时间戳应保存在该 request 对应的 request/client-stream 上，之后与现有 request 标识及现有客户端日志合并输出。

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

第一阶段的新增时间点位于客户端已经进入该 request 的 HTTP/2 发送流程、即将尝试获取 `reqHeaderMu` 的位置；必须放在实际加锁语句之前，而不是成功取得锁之后。G 和 W 分别在第 10 节和第 14 节定义。

### 2.1 原有六点在当前代码中的实际取时位置

原有六个点本来就各自执行一次 `time.Now()`，不是没有取时。以 AMF 副本为例，其他启用 HTTP access log 的 NF 使用相同结构：

| 点 | 当前文件/函数 | 当前实际语句和位置 |
|---|---|---|
| T1 | `NFs/amf/internal/accesslog/httptransport.go`，`(*loggingRoundTripper).RoundTrip` | 调用 `base.RoundTrip(req)` 前的 `reqTime := time.Now()`，当前约第 252 行 |
| T2 | 同一函数内的 `httptrace.ClientTrace.WroteRequest` | request 写完后的 `wroteTime = time.Now()`，当前约第 241-244 行 |
| T5 | 同一函数内的 `httptrace.ClientTrace.GotFirstResponseByte` | response 首个 header block 到达 client read loop 后的 `gotFirstByte = time.Now()`，当前约第 246-248 行 |
| T6 | `(*loggingRoundTripper).RoundTrip` | `base.RoundTrip(req)` 返回后的 `respTime := time.Now()`，当前约第 253-254 行 |
| T3 | `NFs/amf/internal/accesslog/httptransport.go`，`InboundLogger` | `c.Next()` 前的 `reqTime := time.Now()`，当前约第 353 行 |
| T4 | 同一 `InboundLogger` | `c.Next()` 返回后的 `respTime := time.Now()`，当前约第 360-361 行 |

因此本计划的准确 A/B 口径是：

```text
A：原有 T1/T2/T3/T4/T5/T6，共 6 次 time.Now()
B：保留原六点，再增加 M/G/W，共 9 个时间戳
```

“M/G/W 新增 `time.Now()`”表示相对于 A 组增加新的取时，不表示原六点不取时。M、G 各为每 request 一次 clock read；W 按每次跨过 marker 的底层 socket Write 取时，同一次 Write 跨过多个 marker 时共用一次 `time.Now()`。因此最常见/上界口径约为每 transaction 从 6 次增加到 9 次（`+50%`），实际 W clock-read 次数可能因批量写而更少；但真实增量不只有 clock read，还包括下表所列状态传递和日志字段：

| 新点 | 相对原六点新增的同步热路径工作 | 日志输出增量 |
|---|---|---|
| M | 一次 `time.Now()`、一次 request-attempt trace 赋值/发布 | 在现有 client JSON 中增加 M 及关联字段，不增加日志行 |
| G | 一次 `time.Now()`、一次 server trace 赋值；按当前方案还包含创建 trace 时的一次 `server_request_id` atomic | 在现有 server JSON 中增加 G 及关联字段，不增加日志行 |
| W | 登记/检查 connection-local byte marker、命中时一次 `time.Now()`、一次固定大小 non-blocking event 投递 | 每个完整 response 新增一条 W JSON event |

M/G 的新增 JSON 字段仍由当前 `LogHTTP`/`LogHTTPInbound` 同步构造后再 enqueue；因此它们没有新增文件 writer 或日志行，但会增加少量时间格式化、append 和可能的 buffer 扩容。W 的 JSON 在 collector goroutine 构造，但后台 CPU、allocation、GC 和第三条日志行仍是 B 相对 A 的系统级增量。

## 3. 可以计算的时间

```text
request_send_after_headermu_start_us
    = T2 - req_header_mu_start_time
```

它表示从 request 开始竞争 `reqHeaderMu`，到现有 T2（`WroteRequest`，该 request 已完成本地写出）之间的总时间。

该区间可能包含：

- 等待 `reqHeaderMu`；
- 等待可用 HTTP/2 stream、分配 stream ID；
- 等待连接写锁 `wmu`；
- HPACK 编码以及 HEADERS/DATA/trailer 的组织、写入和 flush；
- request body 和 HTTP/2 flow-control 等待；
- 这段路径中的 goroutine 调度或抢占时间。

因此，`T2 - req_header_mu_start_time` **不是纯粹的 `reqHeaderMu` 锁等待时间**。它能用一个新增时间点判断 latency 是否主要产生于“从开始竞争 `reqHeaderMu` 到 request 完成本地发送”的整段客户端发送路径，但不能单独证明 `reqHeaderMu` 本身就是瓶颈。

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

## 4. 具体修改位置

### 4.1 HTTP/2 客户端库内部

准确修改文件和函数为本地 fork 的：

```text
golang.org/x/net/http2/transport.go
func (cs *clientStream) writeRequest(req *http.Request, streamf func(*clientStream))
```

在 `v0.47.0` 中，`(*clientStream).doRequest` 调用 `cs.writeRequest(...)`；`writeRequest` 约第 1424-1430 行通过下面的 `select` 开始竞争 `reqHeaderMu`：

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
// 伪代码：具体 API 名称可在实现时确定。
if tr := instrumentationTrace(cs); tr != nil {
    tr.recordReqHeaderMuStart(time.Now()) // 每个 request attempt 只成功记录一次
}
select {
case cc.reqHeaderMu <- struct{}{}:
    // 原逻辑保持不变
// cancel 分支保持不变
}
```

该位置只允许一次 `time.Now()` 和对已经存在的 request/client-stream trace 赋值；不能做 JSON、字符串转换、channel 等待、文件 I/O 或创建 goroutine。M 之后的原路径保持不变：取得 `reqHeaderMu`、等待 stream 配额、`cc.addStreamLocked(cs)` 分配 stream ID、`cs.encodeAndWriteHeaders(req)`，最后释放 `reqHeaderMu`。

`v0.47.0` 中 stream ID 在 `cc.addStreamLocked(cs)` 返回后才写入 `cs.ID`；紧随其后的现有 `streamf(cs)` 时机已经能看到实际 connection 和 stream ID。fork 应从 `Transport.RoundTripOpt` 的真实 attempt 循环把最小 hook 传入 `ClientConn.roundTrip`/`clientStream`，并在该时机一次性发布本 attempt 的不可变关联 snapshot：

```text
M
canonical connection identity
stream_id = cs.ID
attempt_index/retry_count
```

公开实现当前通常把 `streamf` 传为 nil，标准 `httptrace` 也不暴露 HTTP/2 stream ID，所以不能只修改 free5GC 外层后猜测该值。snapshot 必须通过具备 happens-before 的方式发布，例如 `atomic.Pointer` 指向不可变结构；外层不得从普通共享字段无同步读取，也不得为了等待 snapshot 阻塞 `RoundTrip`。若 snapshot 未就绪，该记录按本计划允许的 incomplete sample 处理并排除。

`req_header_mu_start_time` 取时时 stream ID 可能尚未分配，但同一 request-scoped/client-stream trace 必须在后续 stream ID 和实际连接确定后补入 `stream_id`/`connection_id`，最终与该时间戳一起输出。

同一 trace 还必须在 HTTP/2 transport 内部对实际发生的 request attempt 计数；第一次 attempt 后 `retry_count=0`，每发生一次透明 retry 就递增。该值必须来自真实 transport attempt 路径，不能由外层根据时间戳或错误结果猜测。

### 4.2 free5GC 客户端 access log

在各 NF 已有的 HTTP client transport/access-log 包装层中：

1. 为 outgoing request 附加用于接收该时间戳的 request-scoped trace 状态；
2. request 完成后，在现有客户端 request 日志中增加字段 `req_header_mu_start_time`；
3. 继续使用现有 T2 `wrote_time`，第一阶段不再增加第二个新时间点；
4. 从同一 HTTP/2 client stream 回传并输出必须的 `connection_id` 和 `stream_id`，使该 client 行能与 server/W 记录严格关联；
5. 从同一 request trace 输出 transport 实际记录的 `retry_count`；
6. 第一阶段不修改服务端日志；第二阶段的服务端 G 点见第 10 节。

如果当前各 NF 分别维护同一份 access-log 代码，需要同步修改实际发送 NF 间 SBI request 的这些副本；不需要修改业务 handler 或 OpenAPI 代码。

free5GC 侧的准确接入函数是：

```text
NFs/<nf>/internal/accesslog/httptransport.go
func (l *loggingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error)
```

保持当前 T1/T6 的位置不变，在调用 `base.RoundTrip(req)` 前把 request-local instrumentation trace 挂入 request；`base.RoundTrip` 返回后读取已经发布的 M/connection/stream/retry snapshot，再传入现有 `LogHTTP`：

```go
reqTime := time.Now()       // 原 T1，不移动
resp, err := base.RoundTrip(req)
respTime := time.Now()      // 原 T6，不移动

snapshot := requestTrace.snapshot() // 必须 race-free；不得阻塞等待未完成事件
LogHTTP(..., reqTime, snapshot.M, wroteTime, gotFirstByte, respTime, ...)
return resp, err
```

最终 JSON 的准确构造位置是：

```text
NFs/<nf>/internal/accesslog/accesslog.go
func LogHTTP(...)
```

`LogHTTP` 当前在 T6 之后、`RoundTrip` wrapper 真正返回之前同步执行 `formatTime`、JSON append 和 `enqueue(kindHTTP, b)`。增加 M/stream/retry 字段不会增加日志行，但这些新增字段的格式化与 append 是 B 相对 A 的 latency 增量，而且发生在 T6 之后，不能只用九点内部时间差发现；必须同时用端到端 UE registration A/B 检查。

## 5. 是否需要 fork HTTP 库

需要 fork/本地替换当前使用的 `golang.org/x/net/http2`，因为“尝试获取 `reqHeaderMu` 之前”是 HTTP/2 transport 的内部位置，公开的 `net/http/httptrace` 没有对应 hook。

修改范围应限制为：

- 本地固定版本的 `golang.org/x/net/http2`；
- 一个最小的可选 trace/callback，用于把时间传回 request-scoped 状态；
- 各相关 NF 的 client access-log 包装层及日志字段；
- 相关 module 的本地 `replace` 配置。

不需要 fork Go 标准库。第一阶段修改 HTTP/2 客户端；第二阶段与第三阶段在同一个本地 `x/net/http2` fork 中修改服务端，分别见第 10 节和第 14 节。

## 6. 建议日志形式

在现有同一条客户端 request 日志中增加字段，而不是额外输出一行：

```json
{
  "src": "UDM",
  "dst": "UDR",
  "method": "GET",
  "uri": "...",
  "ue_id": "...",
  "connection_id": "...",
  "stream_id": 17,
  "retry_count": 0,
  "req_time": "...",
  "req_header_mu_start_time": "...",
  "wrote_time": "...",
  "got_first_byte": "...",
  "resp_time": "..."
}
```

这样仍然是一条 request 粒度日志，也避免额外日志行/I/O 扰动被测路径。该行必须通过现有 `enqueue(kindHTTP, ...)` 写入同一个 `HTTP_LOG_PATH`；这里的 JSON 构造仍发生在现有外层 access-log 路径，M 所在的 `x/net/http2` 热点本身不构造 JSON。

## 7. 实现约束

- 对每个 request attempt 只记录一次该时间点；
- 热路径中只执行取时和赋值，不进行同步日志输出；
- HTTP/2 写 goroutine 写入、外层 transport goroutine 读取该时间时，需要使用无数据竞争的 request-scoped 传递方式；
- client 行必须记录可观测的 `retry_count`（无 retry 为 0），第一轮分析只接受成功且 `retry_count == 0` 的 request，不能仅凭“每个 UE 只 registration 一次”推断 transport 没有 retry；若后续分析 retry，必须对每个 attempt 分别记录并增加 `request_attempt_id`，该 ID 不是额外时间点；
- 继续使用 monotonic duration 计算内存中的耗时；序列化时间仅用于离线关联。

## 8. 验证与判断方法

实施后先验证每个成功 request 满足：

```text
T1 <= req_header_mu_start_time <= T2
```

并验证：

```text
(req_header_mu_start_time - T1)
+ (T2 - req_header_mu_start_time)
≈ T2 - T1
```

随后在不同 request rate 下比较两个区间的 p50、p95 和 p99：

- 如果 `T2 - req_header_mu_start_time` 随 request rate 明显上升，并解释了大部分 `T2 - T1` 的增长，说明主要等待发生在开始竞争 `reqHeaderMu` 之后的客户端发送路径；
- 如果 `req_header_mu_start_time - T1` 上升，而后半段稳定，说明 latency 更可能发生在到达该锁之前，例如连接选择或 writer goroutine 获得运行机会之前；
- 如果后半段上升，只能先定位到该发送路径，不能仅凭这一个时间点区分 `reqHeaderMu`、`wmu`、flow control 或 goroutine 调度。要证明某一个锁，需要下一阶段增加锁取得时间，或结合 block/mutex profile。

## 9. 第一阶段边界

- 第一阶段不增加 server request 接收端日志；第二阶段只在现有 server request 日志中增加 G 字段；
- 第一、二阶段不增加 server response 发送端日志；第三阶段增加 W，见第 14 节；
- 不增加 client response read-loop 日志；
- 不增加单独的 `reqHeaderMu` 成功加锁时间；
- 不在本计划中实施源代码修改。

## 10. 第二阶段目标：服务端 handler 真正启动前的 G 点

第二阶段增加一个服务端时间点：

```text
server_handler_go_time = G
```

G 表示：服务端已经完成当前 request 的 HTTP/2 初始 HEADERS 处理，已经创建好 stream、`*http.Request` 和 response writer，并且**即将真正执行 `go runHandler(...)`、使新的 handler goroutine 变为 runnable 之前**的时间。

记录 G 时，新的 handler goroutine 还没有创建。时间戳必须紧挨实际 `go` 语句并位于其前面；不能放在 `go` 之后，因为新的 goroutine 可能先于父 goroutine继续执行。

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
readFrames：顺序读 frame、CONTINUATION、HPACK 解码
    ↓
serve：处理 HEADERS、创建 stream/request/response writer
    ↓
若 handler 配额不足，在 unstartedHandlers 中等待
    ↓
G server_handler_go_time     ← 第二阶段新增点；实际 go handler 前
    ↓
创建 handler goroutine、等待 Go scheduler、开始运行
    ↓
h2c/Gin 路由、metrics middleware、access-log 前置代码
    ↓
T3 server req_time
    ↓
T4 server resp_time
    ↓
T5 client got_first_byte_time
    ↓
T6 client resp_time
```

当前 HTTP/2 server 每条连接实际上有一个 `readFrames` goroutine 和一个 `serve` goroutine。二者不是同一个 goroutine，但 reader 每交出一帧后必须等待 serve 处理并允许继续，因此在一条连接上构成逐帧串行的接收路径。

### 10.2 G 将 `T3 - T2` 拆成的两段

第一段：

```text
server_before_handler_go_us
    = G - T2
```

这一段主要覆盖 handler 真正启动之前的路径，包括：

- 客户端内核发送队列、Pod 网络路径和服务端内核接收队列；
- 服务端 `readFrames` goroutine 被唤醒和获得调度；
- 按 TCP/HTTP2 线序读取当前 request 之前的 frame；
- 初始 HEADERS/CONTINUATION 的读取与 HPACK 解码；
- frame 等待交给单连接 `serve`；
- `serve` 处理 HEADERS、创建 stream、`http.Request` 和 response writer；
- header 校验、canonicalization、allocation/GC；
- 如果 handler 并发配额已满，request 在 `unstartedHandlers` 中等待到真正可以执行 `go` 的时间。

因此，`G - T2` 可以判断增长是否主要发生在 handler 创建之前，但它不是纯粹的 `readFrames + serve` 时间；内核/网络和 handler admission 等待也包含在其中。

第二段：

```text
server_after_handler_go_to_t3_us
    = T3 - G
```

这一段主要覆盖：

- 执行 `go runHandler(...)` 和创建 handler goroutine；
- 新 goroutine 处于 runnable 状态、等待 Go scheduler 分配 P/M；
- `runHandler` 的少量入口代码；
- 进入 h2c/net/http handler、Gin 路由和 context 准备；
- 注册在 access log 之前的 `metrics.InboundMetrics()` 前置代码；
- 现有 `InboundLogger` 取得 method/URI，直到记录 T3。

因此，`T3 - G` 可以判断增长是否发生在“提交 handler goroutine 之后、到达现有 T3 之前”，但不是完全纯净的 Go scheduler latency。若以后必须严格得到纯调度时间，还需在新 handler goroutine 的第一条指令记录 E，并计算 `E - G`；本阶段先不增加 E。

### 10.3 handler admission 等待的归属

HTTP/2 server 只有在当前运行中的 handler 数量低于每连接上限时才立即执行 `go runHandler(...)`。达到上限时，代码只把 `streamID`、request、response writer 和 handler 保存到 `unstartedHandlers`；此时 handler goroutine 尚不存在。等已有 handler 结束后，serve 再取出该 request 并真正执行 `go`。

G 必须在两条实际启动路径中都记录：

1. request 可立即启动时，在直接执行 `go runHandler(...)` 前记录；
2. request 曾进入 `unstartedHandlers` 时，在以后真正取出并执行 `go runHandler(...)` 前记录。

这样 `T3 - G` 不会混入 handler admission queue；admission 等待归入 `G - T2`。当前配置未显式设置 `MaxConcurrentStreams`，通常使用每连接默认上限 250；预期正常实验并发远低于该值，但实现必须覆盖该路径。

当前实验的 G 覆盖范围明确限定为 `scheduleHandler` 的“立即启动”和 `handlerDone` 的“排队后启动”两条普通 h2c request 路径。h2c Upgrade 首请求以及 HTTP/2 server push 中绕过 `scheduleHandler` 的直接 `go runHandler` 不在本阶段覆盖范围内；这些少量请求如果没有 G，只作为 incomplete sample 统计并排除，不使用时间顺序或 UE/URI 补配，也不影响其余具有唯一关联 key 的完整样本。

## 11. G 的 request 粒度与日志形式

G 是严格的 request/HTTP2-stream 粒度。在 G 点已经有：

- 完整的 `*http.Request`；
- HTTP method、URI 和 headers；
- 当前 HTTP/2 `stream_id`；
- 当前连接的 server-connection 状态。

G 不单独输出一条新日志。HTTP/2 库只在热路径执行 `time.Now()` 和 request-scoped 赋值，然后由该 request 已有的 `InboundLogger` 在现有 server request JSON 中一起输出：

```json
{
  "src": "NaN",
  "dst": "UDM",
  "server_request_id": 12345,
  "connection_id": "...",
  "stream_id": 17,
  "method": "GET",
  "uri": "...",
  "ue_id": "...",
  "server_handler_go_time": "...",
  "req_time": "...",
  "resp_time": "..."
}
```

其中 `req_time` 仍为现有 T3，字段语义不变。`server_request_id` 是接收方 NF 为当前 inbound HTTP request 分配的 server-local ID，专门用于把这条 G/T3/T4 server JSON 与随后独立输出的 W event 严格合并。该 server JSON 继续通过现有 `enqueue(kindHTTP, ...)` 写入同一个 `HTTP_LOG_PATH`，G 不新增独立日志行。

G 与 T3 通过同一个 `*http.Request.Context()` 传递，天然一一对应。为了让客户端 T2 与服务端 G 在高并发下也能严格一一配对，必须同时输出非时间关联字段：

```text
(connection_id, stream_id)
```

这不是额外时间点。只靠 UE、method、URI 和时间顺序，在相同 request 并发且 handler/response 完成顺序重排时可能配错。三个新增时间点的完整关联要求见第 17 节。

## 12. 第二阶段需要修改的位置与代码

### 12.1 本地 `golang.org/x/net/http2 v0.47.0` fork

修改本地固定版本的 `http2/server.go`，不修改 Go 标准库。

准确涉及的普通 request 函数为：

```text
http2/server.go
func (sc *serverConn) newWriterAndRequestNoBody(...)
func (sc *serverConn) scheduleHandler(...)
func (sc *serverConn) handlerDone()
```

`newWriterAndRequestNoBody` 当前使用 `.WithContext(st.ctx)` 创建 `*http.Request`。对正常 inbound request，应在 request 校验成功后、构造 `*http.Request` 之前创建一次 server trace，同时把指针保存在 `stream` 上供 response/W 使用，并挂入 `st.ctx`，使随后创建的 request 自动继承同一个 trace；不要到 free5GC `InboundLogger` 才临时创建另一份 ID/trace。若采用 `context.WithValue`，它可能增加一次 per-request allocation，必须单独进入第 19 节的 alloc/latency benchmark；实现应优先复用/扩展已有 stream context wrapper，避免额外 map、锁或 goroutine。

#### A. 增加 request-scoped server trace 状态

在每个 server stream/request 的内部状态中增加最小 trace 信息：

```text
server_request_id
connection_id
stream_id
server_handler_go_time
```

每个 server NF 进程维护一个并发安全的 `atomic.Uint64` 计数器。在创建当前 HTTP/2 server stream/request trace 时执行一次 `Add(1)`，把结果写入 `trace.server_request_id`；同一个 request 的 G/T3/T4 与 W 必须始终读取这个已经分配好的值。不能分别在 `InboundLogger` 和 W 回调中临时生成 ID，否则同一 request 会得到两个不同 ID。使用 `uint64` 单调递增即可，不需要为了限制位数而循环复用。

在创建 `*http.Request` 时，把该 trace 状态挂入 request context；同时提供一个最小的只读 accessor，使 free5GC access-log 代码可以从 `Request.Context()` 读取它。如果未来实验需要 `run_id` 和 NF instance 等部署元数据，由外层 access-log 在生成 JSON 时补入；当前单 Pod、每轮清空日志的实验不要求这些字段逐行输出。HTTP/2 库不应知道这些 free5GC 字段，也不引入日志包或业务代码依赖。

#### B. 在直接启动 handler 的路径记录 G

准确位置是 `http2/server.go` 的 `(*serverConn).scheduleHandler`。在 `v0.47.0` 中，它位于 `sc.curHandlers++` 之后、约第 2359 行 `go sc.runHandler(rw, req, handler)` 之前：

修改内容：

```go
if sc.curHandlers < maxHandlers {
    sc.curHandlers++
    traceFromRequest(req).serverHandlerGoTime = time.Now() // G
    go sc.runHandler(rw, req, handler)
    return nil
}
```

这里只取时和赋值，不执行 JSON 格式化、channel 投递、文件 I/O 或同步日志调用。

#### C. 在排队后真正启动 handler 的路径记录 G

准确位置是 `http2/server.go` 的 `(*serverConn).handlerDone`。在 `v0.47.0` 中，它从 `unstartedHandlers` 取出 `u`，确认 stream 仍存在且 handler 配额可用后，在 `sc.curHandlers++` 之后、约第 2389 行 `go sc.runHandler(u.rw, u.req, u.handler)` 之前记录：

```go
sc.curHandlers++
traceFromRequest(u.req).serverHandlerGoTime = time.Now() // G
go sc.runHandler(u.rw, u.req, u.handler)
```

修改内容与直接启动路径相同。不能在 request 第一次进入 `unstartedHandlers` 时写 G，否则 admission 等待会被错误算入 `T3 - G`。

### 12.2 free5GC server access log

修改所有启用了当前 HTTP access log 的 NF 副本：

```text
NFs/<nf>/internal/accesslog/httptransport.go
NFs/<nf>/internal/accesslog/accesslog.go
```

`httptransport.go` 中的修改：

1. 在现有 `InboundLogger` 处理同一个 `*http.Request` 时，从 context 取得 HTTP/2 server trace；为保持 T3/T4 原位置不变，建议在现有 `respTime := time.Now()` 之后读取 trace；
2. 保持现有 T3 `req_time` 的记录位置和语义不变；
3. 把 `server_request_id`、G、必须的 `stream_id` 和 server connection identity 传给现有 `LogHTTPInbound`；
4. HTTP/1 或 trace 不存在时使用空值，不改变请求行为。

`accesslog.go` 中的修改：

1. 为 server HTTP record 增加 `server_request_id` 和 `server_handler_go_time`；
2. 必须增加 `stream_id` 和可与 client 侧对齐的 server connection identity；
3. 扩展 `LogHTTPInbound` 参数/record 构造；
4. 继续通过现有异步日志通道只输出一条 server request 日志。

不修改 Gin 业务 handler、processor、OpenAPI model 或数据库代码。

free5GC 的准确记录/输出顺序是：

```text
NFs/<nf>/internal/accesslog/httptransport.go
func InboundLogger() gin.HandlerFunc

reqTime := time.Now()   // 原 T3，不移动
c.Next()
respTime := time.Now()  // 原 T4，不移动
trace := HTTP2ServerTraceFromContext(c.Request.Context())
LogHTTPInbound(..., trace.serverHandlerGoTime, ...)
```

最终 JSON 仍在 `NFs/<nf>/internal/accesslog/accesslog.go` 的 `LogHTTPInbound` 中同步构造并调用 `enqueue(kindHTTP, b)`。因此 G 的取时发生在 `go runHandler` 之前；G 的 JSON 格式化则发生在 T4 之后、Gin middleware 返回及普通小 response 自动 flush 之前。这部分新增格式化会进入 B 相对 A 的端到端 latency，并可能增加 `T4 -> W/T5`，必须纳入 A/B。

### 12.3 各相关 NF 的 module 配置

在所有使用该 HTTP/2 client/server 和 access log 的 NF module 中，用 `go.mod replace` 指向同一份本地固定版本 `x/net`：

```text
replace golang.org/x/net => <repository-local-xnet-path>
```

第一阶段的客户端 `reqHeaderMu` 点、第二阶段的服务端 G 点与第三阶段的服务端 W 点必须使用同一个本地 fork，避免维护多套不同的 `x/net/http2`。

### 12.4 离线分析脚本

更新当前拆分 HTTP transport latency 的脚本：

1. 读取 server 记录中的 `server_handler_go_time`；
2. 将客户端 T2 和服务端 G/T3 按同一 request 关联；
3. 计算 `G - T2` 与 `T3 - G`；
4. 对不同 request rate 分别比较两段的 p50、p95、p99；
5. 验证两段之和约等于原有 `T3 - T2`。

## 13. 第二阶段验证与解释边界

对于成功、无 retry、无 request body 的 HTTP/2 request，验证：

```text
T2 <= G <= T3
```

并验证：

```text
(G - T2) + (T3 - G) ≈ T3 - T2
```

解释结果时：

- `G - T2` 随 request rate 上升、`T3 - G` 稳定：增长主要发生在 handler 真正启动之前；再结合 Send-Q/Recv-Q、CPU throttling 和 profile 判断是内核网络还是 `readFrames/serve`；
- `T3 - G` 随 request rate 上升：增长发生在 handler goroutine 被提交之后；可能是 Go scheduler，也可能是 T3 前的 Gin/metrics 开销；
- 两段同时上升：服务端连接级前置路径与进程级 CPU/调度都可能处于饱和状态。

两个重要前提：

1. T2 在调用方 Pod、G/T3 在被调方 Pod。若 Pod 位于同一节点，通常共享同一 host clock；跨节点时必须验证时钟偏移，不能直接把偏移解释为 latency。
2. 对带 request body 的请求，T2 `WroteRequest` 在完整 body 写完后发生，而服务端只需收到 HEADERS 就可能启动 handler，因此 G/T3 可能早于 T2。第二阶段首先分析无 body request；若需要覆盖 POST/body request，应使用客户端 `WroteHeaders` 作为请求接收阶段的起点。

## 14. 第三阶段目标：response HEADERS 交给服务端内核的 W 点

第三阶段只增加一个 response-level 时间点：

```text
server_response_headers_flushed_time = W
```

W 的严格语义是：

> 对当前 HTTP/2 stream 的第一组最终 response HEADERS（排除 1xx 和 trailer），当完整 HEADERS/CONTINUATION header block 的最后一个字节已被底层 `net.Conn.Write` 接受时，立即记录时间。

当前 NF 使用 cleartext h2c，所以这个点可以实用地理解为“完整 response header block 已交给服务端内核 TCP 发送缓冲区”。`Write` 返回是可实现的用户态边界，不表示对端已收到数据。

### 14.1 W 在现有时间线中的具体位置

```text
T4 server resp_time
    ↓
access-log/metrics middleware 收尾，Gin/net-http handler 真正返回
    ↓
每 request 的 responseWriter 4 KiB buffer 自动 Flush
    ↓
第一组最终 response HEADERS 提交到 wantWriteFrameCh
    ↓
每连接唯一 serve + write scheduler 处理/排队
    ↓
HPACK 编码、HEADERS/CONTINUATION 组帧、每连接至多一个 frame writer
    ↓
连接级用户态 buffer 的实际底层 Write 覆盖 header block 末字节
    ↓
W server_response_headers_flushed_time
    ↓
服务端 Send-Q / veth/CNI / TCP / 客户端 Recv-Q
    ↓
客户端单 readLoop 调度、前序 frame、完整 HEADERS/CONTINUATION 读取与 HPACK 解码
    ↓
T5 client got_first_byte_time
```

对普通、小型、非 streaming 且没有显式 Flush 的 SBI response，预期：

```text
T4 <= W <= T5
```

### 14.2 `W - T4` 代表什么

```text
server_response_send_path_us
    = W - T4
```

该区间代表从现有 T4 到完整 response header block 交给服务端内核的广义 server response 发送路径，包括：

- T4 之后的 access-log JSON 构造/异步投递、外层 metrics middleware 和 handler 收尾；
- 小 response 从 request-local 4 KiB buffer 提交出来；
- 向 `wantWriteFrameCh` 提交和可能的通道等待；
- 单连接 `serve` 及 write scheduler 中的前序/control/其他 stream frame 等待；
- HPACK 编码、HEADERS/CONTINUATION 组帧；
- 每连接至多一个 frame writer 的调度与执行；
- 用户态连接 buffer 以及底层 socket write/Send-Q backpressure。

因此，`W - T4` 上升可以先定位到服务端 response 发送路径，但不能仅凭 W 证明一定是 `serve` goroutine 本身。如果该段确认上升，再决定是否增加“首个最终 HEADERS 提交发送队列前”与“被 write scheduler 选中”的更细时间点。

### 14.3 `T5 - W` 代表什么

```text
response_after_server_write_us
    = T5 - W
```

该区间代表 response header block 完成服务端底层写入之后，到现有 T5 的广义网络+客户端接收路径，包括：

- 服务端内核 Send-Q；
- veth/CNI/TCP 路径与客户端内核 Recv-Q；
- 客户端单 `readLoop` 被唤醒并获得调度；
- 按 TCP/HTTP2 线序处理当前 response 之前的 frame；
- 当前 HEADERS/CONTINUATION 的完整读取、合并和 HPACK 解码；
- 通过 `stream_id` 找到对应 client stream，直到调用 `GotFirstResponseByte`。

`T5 - W` **不是“一个字节的纯 TCP 传输时间”，也不是纯 `readLoop` 排队时间**。当前 T5 在 HTTP/2 实现中等到完整 response header block 读取和 HPACK 解码后才触发。若该段上升，需结合 Send-Q/Recv-Q、eBPF/抓包和 runtime trace 进一步区分网络与 client `readLoop`。

### 14.4 W 是 response HEADERS 边界，不是整个 body 边界

W 只保证第一组最终 response HEADERS/CONTINUATION 已交给底层连接，不保证整个 response DATA/body 已写入内核。小 response 的 HEADERS 和 DATA 可能被合并在同一次底层 Write 中，但不应依赖这一实现细节来改变 W 的定义。

如果要研究整个 response body 发送完成，需要另外定义 final DATA/END_STREAM 时间点；它不能与表示首个 response header 就绪的 T5 直接配对，所以本计划不增加该点。

## 15. W 需要修改的位置与实现约束

### 15.1 修改同一份本地 `golang.org/x/net/http2 v0.47.0` fork

第三阶段继续使用第一、二阶段的同一份 fork。准确修改文件、函数和打点顺序如下。

#### A. `http2/server.go`：trace 与 W event sink

1. 在每个普通 server stream/request 上保留与 G 共用的 response trace；
2. 给 `http2.Server` 增加一个可选、固定事件类型的 W channel/sink；nil 表示关闭 instrumentation；
3. 在 AMF、AUSF、NRF、NSSF、PCF、UDM、UDR 的 `NFs/<nf>/internal/sbi/server.go::newHttp2ServerWithIdleTimeout` 中创建/注入同一个 NF-local W channel；当前构造器只设置 `IdleTimeout`，如果不补这一步，fork 中没有消费者，最终会得到 0 条 W；
4. x/net 必须自己执行带 `default` 的 non-blocking send。不要在唯一 socket writer 中调用语义不受控、可能阻塞或 panic 的任意业务 callback。

#### B. `http2/write.go`：在最终 header fragment 写入前登记 marker

准确函数是：

```text
type writeResHeaders
func (w *writeResHeaders) writeFrame(ctx writeContext) error
```

`v0.47.0` 当前顺序是：HPACK 编码 → `headerBlock := buf.Bytes()` → `splitHeaderBlock(...)` → 对每个 fragment 调用 `writeHeaderBlock(...)` → `WriteHeaders`/`WriteContinuation`。`writeResHeaders` 需要携带同一个 server trace；只有 `httpResCode >= 200`、`trailers == nil` 且该 trace 尚未登记最终 headers 时才创建一次 W marker。`server.go` 中创建普通最终 response `writeResHeaders`、trailer 和 1xx 的路径必须分别识别，不能把 1xx/trailer 当成 W。

最直接且无需预先计算全部 fragment 数量的位置，是 `http2/write.go::(*writeResHeaders).writeHeaderBlock`：当 `lastFrag == true` 时，在调用本次最终 `WriteHeaders` 或 `WriteContinuation` **之前**登记 marker。当前 Framer 对该 frame 向 connection writer 写入的逻辑长度是 `9-byte frame header + len(frag)`，因此：

```go
func (w *writeResHeaders) writeHeaderBlock(ctx writeContext, frag []byte, firstFrag, lastFrag bool) error {
    if lastFrag && w.isFirstFinalResponseHeaders() {
        endOffset := ctx.producedBytes() + 9 + uint64(len(frag))
        ctx.registerResponseHeaderMarker(endOffset, w.trace)
    }
    if firstFrag {
        return ctx.Framer().WriteHeaders(...)
    }
    return ctx.Framer().WriteContinuation(...)
}
```

登记必须发生在最终 fragment 的 Framer write 之前：`bufio.Writer.Write` 可能在该调用尚未返回时自动触发底层 Write。不能等 `writeHeaderBlock`、`writeFrame` 或 `splitHeaderBlock` 返回后才登记，否则会造成 W 漏记或晚记。

#### C. `http2/http2.go`：只在真实 `net.Conn.Write` 成功推进后取 W

准确调用链是：

```text
(*bufferedWriter).Write
    -> bufio.Writer.Write                       // 这里只是进入/触发 4 KiB 用户态 buffer
    -> (*bufferedWriterTimeoutWriter).Write
    -> writeWithByteTimeout
    -> net.Conn.Write                           // 真正底层写入
```

W 不能记录在 `(*bufferedWriter).Write` 或 `Flush` 的入口/返回处。必须在 `writeWithByteTimeout` 内每次真实 `conn.Write` 返回后，用实际成功的 `n` 推进该连接的 `wireAcceptedBytes`；当累计值首次跨过一个或多个 pending marker 时：

```go
n, err := conn.Write(p)
wireAcceptedBytes += uint64(n)
if crossedAnyMarker(wireAcceptedBytes) {
    now := time.Now() // 本次底层 Write 只取一次
    popAllCrossedMarkersAndTrySend(now)
}
```

一次 Write 可以跨多个 marker，所有 marker 共用该次 Write-return timestamp；partial write 按实际 `n` 推进。marker FIFO 与累计计数是 connection-local 状态，不增加跨连接共享 mutex。连接关闭时只需释放剩余 marker；当前实验允许这类少量 W 缺失，不能为它们伪造时间，也不能回退到时间近似匹配。

为计算 marker end offset，`(*bufferedWriter).Write` 还需维护 connection-local `producedBytes`：每个 frame byte 成功进入/经过该 4 KiB writer 后按返回的 `n` 单调增加。它只做整数加法，不在此处取时。由于每条连接至多一个 frame writer，`producedBytes`、`wireAcceptedBytes` 和 marker FIFO 均由同一连接写路径串行维护，不需要新增共享锁。

W 复用第二阶段已挂入 server stream/request context 的 trace。x/net 内部 trace 只需携带 `server_request_id`、`connection_id`、`stream_id` 和一个最小非阻塞 W callback/event sink；如果未来需要 `run_id` 与 NF instance，则由 free5GC 外层 event logger 补入。x/net 不解析 UE ID 或任何 free5GC 业务字段。

在 W 点已经能确定：

```text
server_request_id
server connection identity
stream_id
response trace
```

因此 W 可以保持严格的 response/HTTP2-stream 粒度。即使 stream 已从活跃 map 移除，也必须让 pending write marker 保留到 W 已记录或写失败。

### 15.2 不能选择的假 W 位置

- 不能在业务 `WriteHeader` 后记录：此时可能只 snapshot header map；
- 不能在 response HEADERS 提交 `wantWriteFrameCh` 后记录：此时还没有经过 serve/write scheduler；
- 不能仅在 `writeResHeaders` 组帧完成后记录：字节可能仍在 connection-level 4 KiB 用户态 buffer；
- 不能简单在某次最终 `flushFrameWriter` 后把所有 pending stream 记为 W：连接 buffer 满时可能早已自动底层 Write，且一次 Flush 可能批量覆盖多个 stream；
- 不能为了打点强制每个 response 单独 Flush：这会改变批量写、排队和被测 latency。

### 15.3 热路径约束

- W 只对每个 response attempt 的第一组最终 HEADERS 记录一次；
- 排除 1xx informational HEADERS 和 trailer HEADERS；
- 底层写路径只执行取时、小状态更新和非阻塞事件投递；
- 不在 socket writer 中做 JSON 格式化、文件 I/O 或可能阻塞的日志调用；
- 连接关闭或写失败时，清理 pending marker 并记录空 W/错误状态，不伪造成功时间。

## 16. W 的日志形式与 T4 生命周期问题

现有 T4 在 Gin `InboundLogger` 的 `c.Next()` 返回后立即记录并输出，但普通小 response 的 W 通常只能在整个 handler 返回、HTTP/2 自动 Flush 之后发生。因此：

- 不能让 `InboundLogger` 同步等待 W；否则 handler 不返回，小 response 就无法进入产生 W 的自动 Flush 路径，可能形成死锁；
- W 不能直接回填已经输出的 server T3/T4 JSON 行。

本计划选择最小、安全的输出方式：W 单独形成一条极轻量的异步 event，再通过 transport correlation key 与现有 server T3/T4 行离线合并。

W event 必须写入该 NF 现有的同一个 HTTP access-log 文件，而不是另建日志文件：

- 继续使用 `HTTP_LOG_PATH`，未配置时即当前默认的 `/tmp/HTTP_log.txt`；
- W 在 `HTTP_log.txt` 中占一条独立 JSON Lines 记录，不能同步回填或覆盖已经输出的 server T3/T4 JSON 行；
- 本地 `x/net/http2` fork 只负责在底层写路径取时并非阻塞地投递最小事件，不直接打开或写入 `HTTP_log.txt`；
- free5GC 外层 access-log event collector 负责补充部署/关联字段、构造下述 JSON，并通过现有 HTTP log sink（`enqueue(kindHTTP, ...)`）落盘，从而继续保持每个 NF 只有一个 HTTP log writer。

free5GC 中建议的准确接线为：

```text
NFs/<nf>/internal/accesslog/accesslog.go
    Init()                         创建有界 wEventQueue，并启动一个长期存活的 wCollectorLoop
    WEventSink()                   只返回供 http2.Server 注入的 send-only channel/sink
    wCollectorLoop()               读取固定大小 event，在该 goroutine 中格式化 W JSON
    enqueue(kindHTTP, jsonBytes)   复用现有 HTTP writer queue

NFs/<nf>/internal/sbi/server.go
    newHttp2ServerWithIdleTimeout  构造 http2.Server 时注入 accesslog.WEventSink()
```

socket writer 只做 marker 状态更新和第一次 non-blocking send；`wCollectorLoop` 才执行 `formatTime`/JSON append 和第二次现有 `enqueue(kindHTTP, ...)`。禁止为每个 W 新建 goroutine。

```json
{
  "event": "server_response_headers_flushed",
  "src": "NaN",
  "dst": "UDM",
  "server_request_id": 12345,
  "connection_id": "...",
  "stream_id": 17,
  "server_response_headers_flushed_time": "...",
  "outcome": "ok"
}
```

当前实验明确保证同一种 NF 只有一个 Pod，并且每轮实验在所有在途 request/W event 排空和 access log flush 后清空各 Pod 的 `HTTP_log.txt`。在这个实验约束下，server T3/T4/G 行与 W event 的离线 exact join key 定义为：

```text
(dst, server_request_id)
```

其中 `dst` 是现有 server HTTP record 已记录的接收方 NF 类型。T3/T4/G 行和 W event 必须各自输出完全相同的 `dst` 与 `server_request_id`。离线分析不得依赖 JSON 行相邻、输出顺序或最近时间戳；必须按上述 key 做一对一 join，并统计 duplicate、missing W 和 orphan W。`connection_id + stream_id` 继续保留，作为该 server-local join 的 HTTP/2 transport 一致性校验，并用于后续连接 client 行；`server_request_id` 本身不替代第 17.3 节用于 client/server 全九点合并的 transport correlation key。

若后续必须保持“一个 request 只有一行 server JSON”，可改为异步 collector：T4 之后保存 record，W 到达后由 collector 合并输出。这比独立 W event 改动更大，不作为第三阶段首选。

## 17. 三个新增时间点能否与 request/response 一一匹配

### 17.1 同一端、同一条日志内的关系

```text
req_header_mu_start_time
```

- 是 request/request-attempt 粒度；
- 在 HTTP/2 client stream 内部取时，最终与该 outgoing request 的 T1/T2/T5/T6、UE ID、method 和 URI 写在同一条 client JSON 中；
- 因此它与这条 client request 天然一一对应。

```text
G = server_handler_go_time
```

- 是 request/HTTP2-stream 粒度；
- 通过同一个 `*http.Request.Context()` 传到 `InboundLogger`，与该 inbound request 的 T3/T4、UE ID、method 和 URI 写在同一条 server JSON 中；
- 因此 G 与这条 server request 天然一一对应。

```text
W = server_response_headers_flushed_time
```

- 是 response/HTTP2-stream 粒度，而且 HTTP/2 的最终 response 与该 stream 上的 request 一一对应；
- W 由于发生在 T4 行输出之后，本计划把它输出为独立 event；
- 在当前“同类 NF 单 Pod、每轮清空日志”的实验中，必须用相同的 `(dst, server_request_id)` 把 W event 与 server T3/T4 行严格合并；再用两侧相同的 `connection_id + stream_id` 做 transport 校验，合并后即可获得该 response 的 UE ID、method 和 URI。

### 17.2 只靠 `ue_id + method + URI + 时间顺序` 不能严格一一匹配

在低并发、同一 UE 的同一 URI 不重叠时，可以用 UE ID、method、URI 和时间近似配对。但这不是严格保证，因为：

- 同一 UE 可以并发多个相同 method/URI 的 request；
- HTTP/2 多路复用允许 handler 开始、业务完成和 response 返回顺序与 request 发送顺序不同；
- 不同 UE 或共享 NF request 可能无法从 URI 唯一提取 UE ID；
- retry/重连可以产生新 stream，单纯按时间排序会错位。

因此，当实验目标是验证 HTTP/2 内部排队时，UE ID 和 URI 用来回答“这是哪个 UE 的哪种 SBI request”，但不能用来代替 transport-level 唯一关联键。

### 17.3 当前实验的 exact-join 字段与作用域

当前实验固定满足：同一种 NF 只有一个 Pod；每轮实验结束后先排空在途 request/W event、flush 并采集，然后清空所有 Pod 的 `HTTP_log.txt`；离线脚本一次只处理一轮数据。因此 `run_id`、`src_nf_instance` 和 `dst_nf_instance` 不要求作为每条 JSON 的主 join 字段，实验目录/采集 manifest 本身承担 run scope。若以后同类 NF 扩为多个 Pod、Pod 在一轮中重启或多轮日志混合，必须恢复这些实例/轮次字段。

“实验目录承担 run scope”必须在采集脚本中真实成立：每轮必须使用唯一 `OUTDIR`，或在本轮开始前清空本地 merged 输出，不能继续把多轮 Pod 日志 append 到同一个分析文件。少量 missing/orphan 可以排除，但多轮混合或进程重启后的 ID 重用可能造成错误匹配，不属于可接受的“少量缺失”。如果不能保证物理隔离，就必须把 `run_id`（以及会重启时的 process instance）加入 client/server/W 三类记录和 join key。

当前每条记录的 mandatory correlation fields 为：

```text
client JSON:
    src, dst, connection_id, stream_id, retry_count

server T3/T4/G JSON:
    dst, server_request_id, connection_id, stream_id

W event:
    dst, server_request_id, connection_id, stream_id, outcome

仅在 retry_count > 0 时：
    request_attempt_id
```

关联规则为：

1. server JSON 与 W event 用 `(dst, server_request_id)` exact join；
2. client JSON 与已合并的 server/W record 用 `(dst, connection_id, stream_id)` exact join；
3. `connection_id` 必须在 client/server 两侧规范化为同一条实际 TCP/HTTP2 连接，`stream_id` 必须由同一 HTTP/2 stream trace 同时传给 client 和 server；
4. `stream_id` 不能单独使用，因为每条新 HTTP/2 连接都会重新从低值分配 stream ID；现有 `conn_slot` 只是连接池位置，也不能代替 `connection_id`；
5. 第一轮主分析只接受 `retry_count == 0`。若 `retry_count > 0`，必须按 attempt 分别生成/记录 `request_attempt_id`，不得把不同 attempt 的 M/T2、connection/stream、T5/T6 混到同一条九点样本中。

实现后必须在正式实验前对实际 Kubernetes/CNI 网络执行关联预检：对每个成功、无 retry 且两侧记录均存在的 request，`(dst, connection_id, stream_id)` 必须恰好一对一，duplicate/ambiguous match 必须为 0；missing/unmatched 允许低于预设上限并作为 incomplete 排除。如果 unmatched 呈系统性或显示两侧 connection identity 根本不同，说明 NAT、CNI 或代理使该 key 无法规范化对齐；此时不能回退到时间/UE/URI 近似配对，必须改用客户端生成并随 request header 传递的 `sbi_request_id`，同时记录在 client 行、server 行和 W trace 中。启用该 header 后也必须纳入第 19 节的扰动 A/B 测试。

### 17.4 实施后能否离线找到“哪个 UE 的哪个 request/response”

按本节的必须字段实施后，可以：

1. 用 transport correlation key 严格连接 client request 行、server request 行和 W response event；
2. 从 client/server request 行读取 UE ID、method 和 URI；
3. 将 T1、M=`req_header_mu_start_time`、T2、G、T3、T4、W、T5、T6 九个点归到同一个 HTTP/2 request/response stream；
4. 回答该 latency 样本具体属于哪个 UE、哪个 SBI method/URI 和哪次 transport attempt。

第 4 项只适用于能确定 UE ID 的 UE-associated SBI request；没有 UE 身份的共享/基础设施请求仍可按 transport key 获得完整九点，但必须标记为 `ue_id=""`/unattributed，不能强行归给某个 UE。如果只保留现有 UE ID、URI 和时间排序算法，则只能做近似配对，不能声称三个新增点已严格一一对应。

## 18. 第三阶段离线分析与验证

离线脚本需要：

1. 先按 `(dst, server_request_id)` 对 server T3/T4/G 行与 W event 执行 exact join，并用 `connection_id + stream_id` 校验一致性；再使用第 17.3 节的 transport correlation key 连接 client 行。两个阶段都必须显式统计 duplicate、missing 和 unmatched 记录；禁止在主分析中仅按 UE/URI/index 配对或静默回退到时间顺序配对；
2. 读取 T4、W 和 T5，计算 `W - T4` 与 `T5 - W`；
3. 验证对于普通小 response：

```text
T4 <= W <= T5
(W - T4) + (T5 - W) ≈ T5 - T4
```

4. 将 W 缺失、底层写失败、1xx、显式 Flush、streaming 和大 response 单独标记/分组；
5. 对不同 request rate 比较两段的 p50、p95 和 p99；
6. `W - T4` 上升、`T5 - W` 稳定：优先检查 server response 发送路径；
7. `T5 - W` 上升、`W - T4` 稳定：优先检查内核/网络/client readLoop 路径；
8. 两段同时上升：检查两端 CPU throttling、GC、goroutine 调度及共享连接饱和；
9. 对每轮实验输出 `client_count`、`server_count`、`w_count`、`complete_nine_point_count`、`duplicate_count`、`missing_server_count`、`missing_w_count`、`orphan_w_count`、`retry_count` 和各日志队列的 drop count；只有三类记录各恰好一条且九点完整的样本进入主 latency 分析；
10. 对可识别 UE 的记录按 UE ID 统计每种 method/URI 的完整率；允许少量 request 缺少九点记录，但进入主分析的每个完整样本必须唯一。共享/无 UE 请求单独统计，并检查缺失是否集中在特定 NF、URI、UE 或高负载时间段，避免用有偏的完整子集得出总体结论。

T4/W 位于被调方 Pod，T5 位于调用方 Pod。`W - T4` 在同一进程内；`T5 - W` 跨 Pod。跨节点测量时必须审计时钟偏移/漂移，否则不能将微秒级差值全部解释为 transport latency。

对大 response、streaming response 或 handler 显式调用 Flush 的样本，首个 response HEADERS 可能在 T4 之前已发送，出现 `W < T4` 乃至 `T5 < T4`。这类样本不能按普通 `T4 → W → T5` 模型解释，必须单独分组。

## 19. 日志扰动的专项验证与验收

本计划的目标是“最小、可测量且不阻塞”的 instrumentation，不宣称打点绝对零开销。正式采集 latency 前必须完成以下 A/B 验证。

### 19.1 对照组

在完全相同的镜像依赖、Pod CPU/内存限制、NF 数量、连接数、UE 数、registration 触发序列和日志采集介质下，至少比较：

```text
A: 当前已有 HTTP access log，不包含 M/G/W
B: 增加 M/G/W 后的实现
```

如果需要单独识别后台文件 I/O 的影响，可增加关闭整个 HTTP access log 的参考组，但 A/B 主结论必须比较“现有日志”与“只增加三个点”，避免把原有日志开销误算成新增点开销。

当前每个完整 HTTP transaction 通常已有一条 client JSON 和一条 server JSON。M/G 只让这两行变宽；W 增加第三行，因此记录行数从 `2` 变为 `3`，增量为 `+50%`。此外 W 还多一次 socket-writer → W collector 的 non-blocking send，再由 collector 调用一次现有 access-log enqueue；这些都是 B 相对 A 的新增成本，不能只 benchmark 三次 `time.Now()`。

如果实现过程中同时调整现有 logger（例如扩大 JSON buffer、改用 `time.Time.AppendFormat` 或把 client/server JSON 也迁到 collector），应使用三组以隔离基础设施重构与三个新点：

```text
A0: 当前原六点实现
A1: 与 B 相同的 logger/容量/collector 基础设施，但关闭 M/G/W
B : 在 A1 基础上只打开 M/G/W
```

`B - A1` 才是 M/G/W 的纯增量；`A1 - A0` 单独报告。若不重构 logger，则直接使用 A/B。无论选择哪一种，实验前必须给端到端 registration p50/p95/p99、throughput 和 success rate 定义实际可接受的等价阈值，不能在看到结果后再用“属于重复波动”解释。

### 19.2 必测负载与指标

- 覆盖正式实验的全部 request rate，并专门覆盖 UE registration 触发间隔小于 1 ms 的最高目标负载；
- 使用相同的预热时间、采样时间、UE 数和重复轮数；不得只比较单轮；
- 比较端到端 registration latency 的 p50、p95、p99、最大值、成功率和实际吞吐；
- 比较各 NF 的 CPU、memory、GC、goroutine 数、allocations、context switch、throttling、HTTP/2 connection 数和 reconnect 数；
- 比较现有 access-log queue 与 W event queue 的最大占用和 drop counter；允许少量 drop/missing，但必须低于实验前预先确定的上限，且所有不完整记录只排除、不补配；
- 使用 race test 验证 request/stream trace 无数据竞争，并使用 mutex/block profile 确认没有新增会随并发增长的锁等待或阻塞 channel send。

### 19.3 热路径验收

代码审查和 benchmark 必须确认：

1. M/G 对每个 request 各执行一次 `time.Now()`；W 对每次跨过一个或多个 marker 的底层 socket Write 只执行一次，并让同批 marker 共享该 timestamp；
2. M/G 不在 `x/net/http2` 热点做 JSON、文件 I/O、阻塞 send 或 per-request goroutine 创建；
3. W 的 socket writer 不做 JSON、文件 I/O、阻塞 send、强制 Flush 或等待 collector；
4. `server_request_id` 只在 server request trace 创建时做一次 atomic increment；
5. 所有新 channel send 都有 `default`/等价 non-blocking 路径，满队列只增加 atomic drop counter；
6. 新增日志不会改变 request/response 内容、HTTP/2 frame 顺序、buffer/flush 策略、flow control、retry 行为、handler 调度条件或错误返回；
7. 停止/采集阶段能够先排空 W collector 再 flush/清空 `HTTP_log.txt`，不会把上一轮的迟到 W event 写入下一轮。
8. `LogHTTP`/`LogHTTPInbound` 的初始 JSON buffer 容量按加入新字段后的实际长度预留，避免每个 request 因容量已知不足而额外 realloc；如果只在 B 中改变容量，必须使用上述 A0/A1/B 三组避免容量优化掩盖新增点成本。

### 19.4 正式实验准入条件

只有同时满足以下条件，新增日志产生的数据才可用于 latency 结论：

- A/B 结果表明新增 M/G/W 的端到端 registration latency、throughput 和成功率变化不超过实验前预先定义的扰动预算。允许 `time.Now()` 等带来可测但低于预算的固定小开销；超过预算时必须先定位/优化，并在使用九点数据解释系统 latency 前扣除或明确报告 instrumentation effect；
- HTTP/W 日志队列的 drop/missing rate 低于实验前确定的允许上限；完整率必须随 NF、URI 和 request rate 一起报告，不能隐藏选择性缺失；
- exact-join 预检无 duplicate/ambiguous match；允许少量 missing/unmatched，但它们必须作为 incomplete sample 排除，禁止近似补配；
- 没有新增 mutex/block 热点、连接 churn、强制 Flush 或数据竞争；
- 跨 Pod 时间段分析所需的时钟同步/偏移审计已经通过。
