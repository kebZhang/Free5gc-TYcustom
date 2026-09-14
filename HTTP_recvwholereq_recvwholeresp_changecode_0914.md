# 第 11 / 12 个时间点：完整 req / 完整 resp 到达时刻 —— 代码修改计划（0914）

本文是 `HTTP_3detailLog_PLAN_0826.md`（九点）与 `HTTP_10thlog_0903.md`（M2、M3）的续篇，
**只描述代码怎么改**。

**本次要做的事，一句话：** 加两个时间点 ——

| log 名 | 含义 | 打点侧 |
|---|---|---|
| `recvwholereq` | **请求的接收端**收齐整个 request（HEADERS + 全部 DATA）的时刻 | 被调 NF（server） |
| `recvwholeresp` | **响应的接收端**收齐整个 response（HEADERS + 全部 DATA）的时刻 | 主调 NF（client） |

**明确不做：** 不在进程内计算任何时长；不记录「handler goroutine 把 body 读完」的时刻（那是另一个点，
见 §10）；不改任何业务代码。

---

## 0. 两条硬性约束（先于一切实现细节）

### 约束一：**只增不删。这是两个新点，原有的点一个都不动。**

现有十个点 —— `T1` / `T2`(M) / `T3`(M2) / M3 / `T4` / `T5`(G) / `T6` / `T7` / `T8`(W) / `T9` / `T10`
—— **字段名、取时位置、在日志行内的顺序一律保持原样**。本次只做**追加**：

- client 行（第 1 种记录）：**一个字节都不改**
- server 行（第 2 种记录）：在 `latency_us` 之前**追加**两个 key，已有 11 个 key 的名字、顺序、取值全部不变
- W event 行（第 3 种记录）：**一个字节都不改**
- 新增第 4 种记录：`recvwholeresp` event 行

判据：改完之后，**现有的离线脚本在不改任何一行的情况下必须照常跑出与改动前一致的数字**。
（新增的第 4 种 event 行会被 `_load_stage_rows` 现有的 `else: continue` 忽略 —— 对旧脚本无害；
想用上它则需要按 §7.3 加一个判据。）

### 约束二：**打点必须异步，不得给正常请求增加 latency。**

沿用 0826 / 0903 计划已经确立的三层结构，**不得为了省事而破例**：

```text
热路径（业务/协议 goroutine）
    只允许：1 次 time.Now()  +  1 次原子存或结构体赋值  +  1 次非阻塞 channel 发送
    禁止：  JSON 序列化 / 加锁 / 内存分配 / 任何可能阻塞的调用 / 任何日志输出
        ↓  非阻塞 channel（满则丢弃并计数，宁可丢日志也绝不阻塞业务）
collector goroutine
    在这里做全部 JSON 格式化，缓冲区取自 linePool（getLine），capHint 一次给够
        ↓  enqueue()，同样是 select + default 丢弃
writer goroutine（全进程唯一一条写文件的 goroutine）
```

对应到本次两个点：

| 点 | 热路径上做什么 | 会不会拖慢请求 |
|---|---|---|
| `recvwholeresp` | readLoop G 上：1 次 `time.Now()` + 1 次 `select{sink<-ev; default:}` | **零序列化、零分配、零阻塞**。readLoop 服务该连接全部 stream，这是最硬的一条红线 |
| `recvwholereq` | serve G 上：1 次 `time.Now()` + 1 次 `atomic.Int64.Store` | 与现有 `mUnixNano` / `mAcqUnixNano` 的开销完全同量级 |

**两处必须诚实记录的增量成本（非零，但可控）：**

1. `LogHTTPInbound` 会多两次 append（`appendKVTime` + `appendKVBool`）。它跑在 handler G 上，
   是同步的 —— 但发生在 `respTime`（T7）**取时之后**，因此**不会污染任何已有时间点的测量值**，
   只是让 handler G 多占用约一个字段的序列化时间（现有 11 个字段，增量约 1/11 的量级）。
2. `recvwholeresp` 事件结构体含 string 字段（`ConnID` / `peer`），与 W 事件同构。
   「channel 元素含指针 → 整个缓冲区被 GC 扫描」这一取舍在
   `accesslog.go:118-124`（`wQueueCapacity` 的注释）里已经评估过，`rQueue` 沿用同样的容量结论
   （`1 << 16`），**不要照抄 `queueCapacity` 的 `1 << 21`**。

**绝对禁止的三种写法**（出现任何一种，本次改动即为失败）：

- 在 `rl.endStream` / `st.endStream` 里调用 `LogXxx()`、`fmt.Sprintf`、`append` 拼 JSON
- 用阻塞的 `sink <- ev`（不带 `default`）
- 为了"保证不丢"而把 `LogHTTP` 推迟到 body 读完之后再写 —— 那会把日志写入拉进请求的关键路径，
  而且调用方一旦漏掉 `Body.Close()` 就永久丢记录

---

## 1. 为什么需要这两个点

现有十个点里，**T6 和 T10 都只保证「头到了」，不保证「body 到了」**。

### 1.1 客户端：T10 的语义错位

`(*ClientConn).roundTrip` 的唯一正常返回路径是 `case <-cs.respHeaderRecv`
（`xnet/http2/transport.go:1372`），而 `cs.respHeaderRecv` 由 readLoop 在处理完 HEADERS 帧后关闭
（`transport.go:2401`）。**DATA 帧到没到与 RoundTrip 返回无关。**

但 free5gc 的调用方拿不到 body 就干不了任何事 —— openapi 生成的 client 在 `CallAPI` 返回后
**无条件** `ioutil.ReadAll(resp.Body)`，然后才看 status code：

```go
// openapi/udr/DataRepository/api_authentication_data_document.go:101-127
localVarHTTPResponse, err := openapi.CallAPI(a.client.cfg, r)   // T10 在这里面返回
localVarBody, err := ioutil.ReadAll(localVarHTTPResponse.Body)   // ← 真正阻塞的地方
localVarHTTPResponse.Body.Close()
switch localVarHTTPResponse.StatusCode {                          // ← 到这里才开始"处理"
```

于是 `T1~T10`（日志里的 `latency_us`）对**带响应 body 的调用系统性低估**，低估量随负载增长。

### 1.2 服务端：T6 的镜像问题

服务端完全对称：serve G 处理完 HEADERS 就起 handler G（`server.go:2411`，即 T5），
handler G 跑到 `InboundLogger` 第一行（即 T6）时 body 同样可能还在路上。

后果是 `T6~T7` 这一段 —— 被离线公式
`T_transport = (T6 - T1) + (T10 - T7)` **整段当作「被调 NF 的服务时间」扣掉** —— 里面
藏着请求 body 的传输时间。对带请求 body 的调用，这部分被误记为服务时间而从 transport 里减掉了。

### 1.3 还能顺带解释一个已知现象

`T4` 取自 `WroteRequest` 回调，而该回调在 HTTP/2 下于 `cs.writeRequestBody(req)` **之后**触发
（`transport.go:1590`）；`T5` 只依赖 HEADERS。**对带请求 body 的调用，T4 与 T5 之间没有因果关系，
T5 早于 T4 是结构性的，不是时钟竞态。** 现行
`HTTP_conn_count_compare.py` 的 `DROP_INTERACTION_ON_NONMONOTONIC` 只丢 `T4 > T5` 的样本，
对一个本就可正可负的差值做单边截断，会把 `T4~T5` 的分布系统性抬高，且被丢弃的样本非随机地
集中在 PUT/PATCH/POST 上。加上 `recvwholereq` 之后这一点可以被直接验证。

### 1.4 UDM<->UDR 九次交互的 body 分布（已逐个核对 UDR handler）

| # | 交互 | 请求 body | 响应 | 响应 body |
|---|---|---|---|---|
| 1 | GET auth-subscription | 无 | `c.JSON(200, data)` | 有 |
| 2 | PATCH auth-subscription | 有 | `c.Status(204)` | 无 |
| 3 | PUT authentication-status | 有 | `c.Status(204)` | 无 |
| 4 | GET am-data (nssai) | 无 | `c.JSON(200, sets)` | 有 |
| 5 | PUT amf-3gpp-access | 有 | `c.Status(204)` | 无 |
| 6 | GET am-data | 无 | `c.JSON(200, sets)` | 有 |
| 7 | GET smf-selection-subscription-data | 无 | `c.JSON(200, sets)` | 有 |
| 8 | GET smf-registrations | 无 | `c.JSON(200, nil)` | 有（字面量 `null`） |
| 9 | POST sdm-subscriptions | 有 | `c.JSON(201, sub)` | 有 |

**请求侧 4/9 带 body，响应侧 6/9 带 body。** 两侧都有无 body 的情况，见 §3。

---

## 2. 取时位置

### 2.1 `recvwholeresp`（客户端）—— 一个挂钩点覆盖全部情况

`(*clientConnReadLoop).endStream` 是唯一汇聚点（`xnet/http2/transport.go:2794`），三条调用路径
全部收敛到它：

```text
transport.go:2403   processHeaders，HEADERS 带 END_STREAM（无 body 响应，如 204）
transport.go:2576   1xx / 特殊路径
transport.go:2789   processData，DATA 带 END_STREAM（有 body 响应）
```

且函数体自带 `if !cs.readClosed` 幂等保护，**只会触发一次**：

```go
func (rl *clientConnReadLoop) endStream(cs *clientStream) {
	if !cs.readClosed {
		cs.readClosed = true
		// ★ recvwholeresp 打在这里
		rl.cc.mu.Lock()
		defer rl.cc.mu.Unlock()
		cs.bufPipe.closeWithErrorAndCode(io.EOF, cs.copyTrailers)
		close(cs.peerClosed)
	}
}
```

实现时打在了比「`closeWithErrorAndCode` 之前」更早的位置 —— **`rl.cc.mu.Lock()` 之前**。
两者都满足下面的 happens-before 要求，但前移一步顺带把连接级锁的等待也排除在时间戳之外。
读到的 `cs.ID` / `cc.identity` / `cc.t` 都是创建后不再变的字段，不需要该锁保护。

**为什么必须在 `closeWithErrorAndCode` 之前：** 该调用会唤醒阻塞在 `bufPipe.Read` 上的调用方
goroutine。放在它之后，调用方可能先读到 EOF、先走完后续逻辑，而 readLoop 这一行还没执行 ——
人为造出一个倒序。放在前面则有真正的 happens-before。

### 2.2 `recvwholereq`（服务端）—— 需要两个挂钩点

服务端**不对称**，这是本计划最容易写错的地方。

`(*stream).endStream` 只有两个调用点（`xnet/http2/server.go`）：

```text
server.go:1963   processData，DATA 带 END_STREAM
server.go:2205   processTrailerHeaders
```

**无 body 的请求（GET）根本不经过 `st.endStream()`。** 它在 `processHeaders` 里直接把流建成
半关闭态：

```go
// server.go:2099-2101
initialState := stateOpen
if f.StreamEnded() {
	initialState = stateHalfClosedRemote
}
st := sc.newStream(id, 0, initialState)
```

并且 `newWriterAndRequest` 里 `bodyOpen` 为 false，`req.Body.(*requestBody).pipe` 保持 nil
（`server.go:2315-2329`）。

因此需要 **两个**挂钩点：

| 挂钩点 | 文件 / 位置 | 覆盖 |
|---|---|---|
| A | `server.go` `(*stream).endStream`，`CloseWithError` 之前（**1989-1998**） | 带 body 的请求（PUT/PATCH/POST） |
| B | `server.go` `newWriterAndRequest`，`bodyOpen` 判断的 else 分支（**2315**） | 无 body 的请求（GET） |

挂钩点 B 的精确位置（`st.initTrace()` 已在 `newWriterAndRequestNoBody` 内于 **2336** 执行完，
所以此处 `st.trace` 可用）：

```go
bodyOpen := !f.StreamEnded()
if bodyOpen {
	// ...existing...
} else {
	// TYcustom recvwholereq（无 body 分支）：HEADERS 带 END_STREAM，永远不会有 DATA
	// 帧，st.endStream() 也永远不会被调用。请求在这一刻就已经"完整到达"。
	st.trace.recvWholeReqUnixNano.Store(time.Now().UnixNano())
}
```

> **注意** `server.go:2166-2176` 有一条 `upgradeRequest` 路径绕过
> `newWriterAndRequestNoBody`（注释明写 "this path bypasses..."）。该路径是 HTTP/1 Upgrade，
> 本部署走 h2c prior knowledge，**不会命中**。为保险起见在那里也补一次 B 型打点，代价是一次
> 原子存。

---

## 3. 没有 body 的情况（本计划的核心约束）

需求明确要求：**没有 body 时，记录的就是「收到完整 req / 完整 resp」的时刻。**

按上面的挂钩点，这一点天然成立，但两侧的路径不同，必须在日志里能区分开：

| 情况 | 打点位置 | 语义 |
|---|---|---|
| 响应无 body（204） | `rl.endStream`，由 `transport.go:2403` 经 HEADERS END_STREAM 触发 | HEADERS 帧处理完 = 响应完整 |
| 响应有 body | `rl.endStream`，由 `transport.go:2789` 经 DATA END_STREAM 触发 | 最后一个 DATA 帧落盘 |
| 请求无 body（GET） | 挂钩点 B，`newWriterAndRequest` | HEADERS 帧处理完 = 请求完整 |
| 请求有 body | 挂钩点 A，`st.endStream` | 最后一个 DATA 帧落盘 |

**因此每条记录必须带一个布尔字段，说明这一笔有没有 body。** 否则离线分析无法区分
「204 所以 recvwholeresp ≈ T9」和「有 body 且传输极快」，两者的含义完全不同：

- `resp_had_body: false` → `recvwholeresp` 与 T9 之差只是同一段 readLoop 代码的执行时间，
  **不应计入任何传输统计**
- `resp_had_body: true` → `recvwholeresp - T9` 是真正的响应 body 传输 + readLoop 排队

布尔值的来源：

- 客户端：给 `rl.endStream` 加一个 `hadBody bool` 参数，三个调用点分别传
  `false`（2403，HEADERS END_STREAM）/ `false`（2576）/ `true`（2789，DATA END_STREAM）。
- 服务端：挂钩点 A 恒为 `true`，挂钩点 B 恒为 `false`。

---

## 4. 两个点的落盘方式**不同** —— 原因与选择

### 4.1 `recvwholereq` → 加在现有 server 行上（不新增记录）

server 行由 `InboundLogger` 在 `c.Next()` **返回之后**写出
（`NFs/<nf>/internal/accesslog/httptransport.go:495-519`）。此时：

- 无 body 请求：挂钩点 B 在 handler 启动**之前**就已执行 → 值一定在
- 有 body 请求：handler 必须读完 body 才能 `ShouldBindJSON` 成功并返回 → 挂钩点 A 已执行 → 值一定在

所以直接在 `LogHTTPInbound` 加参数、加字段即可，**不需要新的 event 行**。

> **例外**：group 级中间件 `NewRouterAuthorizationCheck` 若 `c.Abort()`，handler 从不读 body，
> 挂钩点 A 可能尚未执行 → 字段为零值。约定：**零值 = 未收齐 / 未发生**，离线分析丢弃。
> 这与现有 `server_handler_go_time` 等字段的零值约定一致。

### 4.2 `recvwholeresp` → 独立 event 行（新增记录种类）

client 行由 `LogHTTP` 在 T10 处写出（`httptransport.go:386`），**此时 body 还没读**。
而 `rl.endStream` 与 T10 之间**没有固定先后**：

```text
低负载：HEADERS + DATA 同一轮 readLoop 处理完 → recvwholeresp < T10
高负载：readLoop 被同连接其他 stream 挤住   → recvwholeresp > T10
```

「在 T10 处读一下原子变量，有就写」这种做法**恰好在我们最关心的高负载场景下拿不到值**，
不可接受。

因此走**独立 event 行**，这与现有 `server_response_headers_flushed`（W 点）的设计完全同构 ——
`accesslog.go:208-231` 的注释已经把理由写清楚了（"a separate line rather than a field on the
server request line because W happens strictly after that line has been emitted"）。

现成可复用的完整模板：

```text
xnet/http2/instrument_server.go:127-175   ResponseHeadersFlushedEvent + 丢弃计数器
xnet/http2/server.go:175-186              Server.ResponseHeadersFlushed chan<- 字段
xnet/http2/http2.go:381-402               热路径非阻塞发送 + default 丢弃
NFs/<nf>/internal/accesslog/accesslog.go:135-231
                                          wQueue / Init / WEventSink / wCollectorLoop / logWFlushed
NFs/udr/internal/sbi/server.go:209        ResponseHeadersFlushed: accesslog.WEventSink()
```

客户端侧对应地需要在 `http2.Transport` 上新增一个 `chan<-` 字段，并在
`newLoggingRoundTripper()`（`httptransport.go:178-219`）为每个 slot 的 Transport 赋值
（`l.tls[i]` 和 `l.clear[i]` 两处，共 `2 * connsPerPeer` 个实例）。

**join key**：`(conn, stream_id)`，与现有三种 schema 一致。
`cc.identity.LocalAddr`（`transport.go:818-821`）与 client 行的 `conn` 是**同一个字符串**
（都来自 `net.Conn.LocalAddr().String()`），所以 join 成立。

---

## 5. 逐文件改动清单

```text
xnet/http2/instrument_client.go     加 1 个事件结构体 + 1 个丢弃计数器 + 1 个导出 getter
xnet/http2/transport.go             Transport 加 1 个 chan<- 字段；
                                    rl.endStream 加 hadBody 参数 + 打点 + 非阻塞发送；
                                    3 个调用点各传 1 个实参
xnet/http2/instrument_server.go     ServerRequestTrace 加 1 个 atomic.Int64 + 1 个 getter
xnet/http2/server.go                st.endStream 打点（A）；newWriterAndRequest else 分支打点（B）；
                                    upgradeRequest 路径补 B
NFs/<nf>/internal/accesslog/httptransport.go   x7（逐字节相同，md5 68d5d6d6...）
                                    newLoggingRoundTripper 为 2*connsPerPeer 个 Transport 赋 sink；
                                    InboundLogger 多读 1 个字段并多传 2 个实参
NFs/<nf>/internal/accesslog/accesslog.go       x7（各不同）
                                    新增 rQueue + rCollectorLoop + logRecvWholeResp；
                                    Init 里起第二个 collector；
                                    LogHTTPInbound 加 2 个参数 + 2 行 append
```

### 5.1 `accesslog.go` 七份行号不同

六个 NF（`ausf` / `nrf` / `nssf` / `pcf` / `udm` / `udr`）行号相同，**AMF 比它们大 31**
（`httpLineCap` 一项例外，AMF 大 3）：

| 位置 | 六 NF | AMF |
|---|---|---|
| `httpLineCap = 768` | 109 | **112** |
| `func Init()` | 142 | **173** |
| `wQueue = make(...)` | 146 | **177** |
| `func wCollectorLoop()` | 178 | **209** |
| `func logWFlushed(...)` | 208 | **239** |
| `func LogHTTPInbound(...)` | 645 | **676** |

### 5.2 容量

- `LogHTTPInbound` 当前 `getLine(512)`，注释称实测均值 277B / 峰值 320B。新增
  `recvwholereq`（RFC3339Nano，约 40B）+ `req_had_body`（约 20B）后**仍然够，不改**。
- 新的 `logRecvWholeResp` 行比 W 行少两个字段，`getLine(320)` 照抄即可。

---

## 6. 异步落盘与并发竞态（§0 约束二的实现细节）

### 6.1 `recvwholeresp`：readLoop 单写，无需原子 —— 但事件发送必须非阻塞

`rl.endStream` 跑在 readLoop goroutine 上，是该连接唯一的读侧 goroutine。值不经共享变量传递，
而是**直接进事件结构体**，所以没有共享写。

但**发送必须非阻塞**，与 W 点同一条铁律：readLoop 服务该连接上所有 stream，一旦阻塞就是
全连接停摆。照抄 `http2.go:397-402`：

```go
select {
case sink <- ev:
default:
	recvWholeRespDrops.Add(1)
}
```

**readLoop 上不允许做 JSON 序列化**，只能发结构体；格式化在 NF 侧的 collector goroutine 里做。

### 6.2 `recvwholereq`：serve G 写、handler G 读 —— **必须用原子**

这是与现有 `HandlerGo` 字段的**关键区别，不能照抄**：

- `st.trace.HandlerGo = time.Now()` 是裸字段赋值，安全，因为它写在 `go sc.runHandler(...)`
  **之前**，靠 goroutine 启动建立 happens-before。
- `recvwholereq` 的挂钩点 A（`st.endStream`）在 **handler G 已经在跑的时候**由 serve G 执行，
  两者并发。裸 `time.Time` 赋值是真数据竞争。

所以 `ServerRequestTrace` 新增字段必须是 `atomic.Int64`（存 UnixNano），读侧走 getter，
与客户端 `ClientRequestTrace` 的 `mUnixNano` / `mAcqUnixNano` / `mRelUnixNano`
（`instrument_client.go:68-81`）完全同构。

### 6.3 `atomic.Int64` 使 `ServerRequestTrace` 不可按值拷贝

`atomic.Int64` 内含 `noCopy`。当前仓库中 `ServerRequestTrace` 无任何复合字面量构造、
只经 `&st.trace` 以指针传递（`server.go:2142`），所以安全；但改完必须跑
`go vet ./xnet/...` 确认 copylocks 干净。

### 6.4 `st.endStream` 里的打点位置

与客户端同理，**必须在 `st.body.closeWithErrorAndCode(io.EOF, ...)` 之前** ——
该调用会唤醒阻塞在 body 上的 handler G。

---

## 7. 日志 schema

### 7.1 server 行（现有 schema 扩展，第 2 种记录）

```json
{"src":"NaN","dst":"UDR","method":"PUT","uri":"...","ue_id":"...",
 "server_request_id":6002,"conn":"192.168.88.253:39628","stream_id":3,
 "server_handler_go_time":"...","req_time":"...","resp_time":"...",
 "recvwholereq":"2026-09-14T...Z","req_had_body":true,
 "latency_us":11953}
```

新增两个 key：`recvwholereq`、`req_had_body`。

时间戳的 key **就叫 `recvwholereq`，不加 `_time` 后缀** —— 这偏离了
`server_response_headers_flushed_time` 的既有惯例，是刻意的：这两个点的名字由需求直接指定。
零值（`""`）表示请求从未被完整接收，见 §4.1 的 `c.Abort()` 例外。

### 7.2 `recvwholeresp` event 行（**新增的第 4 种记录**）

```json
{"event":"recvwholeresp","src":"UDM","dst":"NaN",
 "conn":"192.168.88.253:39628","stream_id":3,
 "recvwholeresp":"2026-09-14T...Z","resp_had_body":true,
 "peer":"10.244.1.7:8000"}
```

**没有 `outcome` 字段**，与 W event 行不同。W 要区分 `write_error`，而 `rl.endStream` 只在响应
干净地结束时才被调用 —— 被 RST_STREAM 或连接错误中断的流走的是 `rl.endStreamError` →
`abortStream`，根本不到这里。所以"这一行存在"本身就等于"响应完整收到了"，没有第二种取值。
反过来说：**一次交互的 client 行存在而 recvwholeresp 行缺失，就是响应没有完整到达**，
离线据此判定，不需要额外字段。

- `src` = `srcNF`（本 NF，即响应的接收端）；`dst` 无法在 readLoop 得知 → 恒为 `"NaN"`，
  离线经 `(conn, stream_id)` 从 client 行取。
- `peer` = `cc.identity.RemoteAddr`，**仅作交叉校验，永远不做 join key**
  （与 `ClientConnIdentity.RemoteAddr` 的既有注释一致）。

### 7.3 对离线脚本的影响

`HTTP_conn_count_compare.py` 的 `_load_stage_rows` 按「哪个时间戳字段存在」分类行
（`FLUSH_TS_FIELD` → `SERVER_TS_FIELD` → `CLIENT_TS_FIELD`）。新增第 4 种 schema 后
**必须在该链最前面加一个判据**，否则 `recvwholeresp` 行会因为没有这三个字段而被
`else: continue` 静默丢掉（不会报错，但也不会被用上）。

---

## 8. 离线分析：新的时序规则（**不要套用现有单调性检查**）

加完之后的时间点集合：

```text
T1 T2 T3 (M3) T4 | T5 T6 T7 T8 | T9 T10
                 ↑ recvwholereq 落在 T5..T7 之间某处，与 T4 无序
                                        ↑ recvwholeresp 与 T10 无序
```

**三条必须写进分析脚本的规则：**

1. **`recvwholeresp` 与 `T10` 没有固定先后。** `recvwholeresp - T10` 是一个可正可负的量。
   把它接在 `STAGE_ORDER` 后面当第 9 段，会让低负载下的正常样本全部被
   `DROP_INTERACTION_ON_NONMONOTONIC` 判为倒序而丢弃。**应作为独立分布统计。**

2. **第一个该看的量是符号，不是均值。** `sign(recvwholeresp - T10)` 随 RQ 的翻转比例，
   直接就是 readLoop 批处理程度的读数：
   - 负 → handler G 醒来时 body 已就绪，一次唤醒办完事
   - 正 → handler G 醒来什么也做不了，`ReadAll` 里再 park 一次，白付一次调度往返

3. **无 body 的样本必须先按 `*_had_body` 过滤掉**，再做任何传输统计（见 §3）。

**修正后的 transport 口径**（可选，作为与现有口径的对照）：

```text
现有： T = (T6 - T1) + (T10 - T7)
修正： T = (recvwholereq - T1) + (recvwholeresp - T7)
```

两者之差就是本计划要量的东西。注意修正口径在 `resp_had_body=false` 时退化为
「`recvwholeresp` 紧贴 T9 之后」，仍然合理。

---

## 9. 验证清单

0. **约束一回归**：拿改动前的一份 `HTTP_log_RQ*.txt` 与改动后同配置的一份对比 ——
   client 行与 W event 行的 key 集合必须**逐字节同构**，server 行必须只多出
   `recvwholereq` / `req_had_body` 两个 key 且其余 key 顺序不变。
   用**未修改的**旧版 `HTTP_conn_count_compare.py` 跑一遍新日志，`T1~T10` 各段统计量应与
   改动前同量级（同一 RQ 下均值差异应在实验噪声内）。
0b. **约束二回归**：同一 RQ 下对比改动前后的 `latency_RQ*_UE1000.txt` 端到端注册时延。
   若出现系统性抬升，说明某处打点跑进了同步路径，回查 §0 的「绝对禁止的三种写法」。
1. `go build ./...` 全仓过；`go vet ./xnet/...` copylocks 干净（§6.3）。
2. `go test -race ./xnet/http2/...` 通过 —— 这是 §6.2 那个原子变量的直接检验。
3. 单 UE 跑一次注册，人工核对 9 条 UDM<->UDR 交互：
   - 4 条 GET：`req_had_body=false`，且 `recvwholereq` 早于 `server_handler_go_time`
   - 3 条 PUT/PATCH + 1 条 POST：`req_had_body=true`，`recvwholereq` 晚于 `req_time`(T6)
   - 6 条有响应 body：`resp_had_body=true`
   - 3 条 204：`resp_had_body=false`，`recvwholeresp` 紧贴 `got_first_byte`(T9)
4. `recvwholeresp` 行数 == client 行数（无重试时）；丢弃计数器为 0。
   非 0 说明 `rQueue` 容量不够，**该次实验的 recvwholeresp 数据作废**。
5. 每条 `recvwholeresp` 行的 `(conn, stream_id)` 都能 join 到唯一一条 client 行；
   `peer` 与该 client 行的 `uri` host 解析结果一致。
6. 低 RQ（200）下 `recvwholeresp < T10` 应占多数；高 RQ（2000）下应出现明显翻转。
   **看不到翻转 = 打点位置写错了**，回头查 §2.1 的「必须在 close 之前」。

---

## 10. 明确不做（与第 11/12 点相邻、但本次不动）

| 不做的事 | 理由 |
|---|---|
| handler G 把 **response** body 读完的时刻 | 需要包装 `resp.Body`；它与本次的 `recvwholeresp` 之差是纯 Go 调度延迟。等本次数据出来再判断是否值得加 |
| handler G 把 **request** body 读完的时刻 | 服务端 gin 走 `json.NewDecoder(body).Decode()`，**不保证读到 EOF**，无法靠 `io.EOF` 判据打点，需改用 Content-Length 计数；而该时刻与业务开始时刻几乎重合，已被 `T6~T7` 包含。性价比低 |
| 进程内计算任何 duration 字段 | 沿袭 0826 / 0903 计划的既定原则：相减一律离线做 |
| 修 `DROP_INTERACTION_ON_NONMONOTONIC` 的单边截断偏差（§1.3） | 属于分析脚本改动，不属于本计划。但本计划产出的数据是修它的前提 |

---

## 11. 建议的实施顺序

1. **`recvwholeresp` 先做**（客户端）。挂钩点只有一个、幂等、无需原子、模板现成，
   且修正的是 6/9 次交互的低估 —— 信息量最大。
2. **`recvwholereq` 后做**（服务端）。两个挂钩点、需要原子、要处理 `c.Abort()` 的零值，
   复杂度更高；修正的是 4/9 次交互的错误归属，同时能验证 §1.3 的结构性倒序猜想。

两步各自可独立编译、独立跑实验、独立验证，不必一次做完。
