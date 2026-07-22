# AMF Worker-Log Instrumentation Plan

> STATUS: IMPLEMENTED (code written; not yet compiled here — `go` is not
> installed on this Windows box, build in the Docker/cloudlab pipeline).
> Files touched:
>   - NEW  internal/msgtrace/msgtrace.go, internal/msgtrace/goroutineid.go
>   - EDIT internal/accesslog/accesslog.go   (kindWorker + 4th file + LogWorker + SBIView)
>   - EDIT internal/ngap/scheduler.go        (Begin/End around handler + flushWorkerTrace)
>   - EDIT internal/nas/handler.go           (msgtrace.SetID in logUplinkNAS)
>   - EDIT internal/sbi/consumer/{ausf,udm,pcf,nrf,nssf}_service.go (defer msgtrace.Track)
> New log file: /tmp/AMF_worker_log.txt (override with WORKER_LOG_PATH).


目标:在 free5gc AMF 里新增一份 **`AMF_worker_log.txt`**,记录一条上行 NAS 消息在
AMF worker 内部的处理时间线(**T2 / T3 / T6 / T8**),用来把"AMF local latency"这段
黑盒拆成可归因的子段。要求:

- **完全异步写入**,和现有 HTTP_log / DB_log / AMF_log 同一套机制(内存 channel +
  单 writer goroutine + drop-on-full),**绝不在业务 goroutine 上做同步文件 I/O**,
  不影响正常 registration 的 latency。
- **三个现有 log 一律不改**:`AMF_log.txt`(T1 收 / T7 发 SCTP)、`HTTP_log.txt`
  (T4 req / T5 resp)、`DB_log.txt`。
- 所有现有分析脚本零改动。

---

## 0. 时间点定义(与本次讨论一致)

| 点 | 含义 | 现在有吗 | 记在哪 |
|----|------|---------|--------|
| **T1 / T0** | AMF 收到上行 NAS(SCTPRead)的时刻 | 有 | `AMF_log.txt` UL `sctp_time`(不动) |
| **T2** | worker 从 taskChan 取出、开始处理这条 NAS | **新增** | `AMF_worker_log.txt` `t_start` |
| **T3** | worker 进入某次 `Send*Request`(发 HTTP 之前) | **新增** | `AMF_worker_log.txt` `sbi[].before` |
| **T4** | HTTP 请求真正发出(RoundTrip 前) | 有 | `HTTP_log.txt` `req_time`(不动) |
| **T5** | 收到 HTTP 响应(RoundTrip 后) | 有 | `HTTP_log.txt` `resp_time`(不动) |
| **T6** | 该次 `Send*Request` 返回(反序列化完) | **新增** | `AMF_worker_log.txt` `sbi[].after` |
| **T7** | AMF 发出下行 NAS(SCTPWrite)的时刻 | 有 | `AMF_log.txt` DL `sctp_time`(不动) |
| **T8** | worker 处理完这条 NAS、handler 返回 | **新增** | `AMF_worker_log.txt` `t_end` |

一条 NAS 可能触发 **多次** `Send*Request`(如 SecurityModeComplete 连续调
UeCmRegistration / SDMGetAmData / ... / AMPolicyControlCreate)。已确认这些调用
**全部在同一个 worker goroutine、同一次 handler 调用内同步顺序执行**(GMM 包无
`go func`,FSM `SendEvent` 同步递归),因此它们的 (T3,T6) 对**都归入同一行**的
`sbi[]` 数组。跨上行 NAS 的调用天然分到不同行(一次 registration = 3 行)。

---

## 1. `AMF_worker_log.txt` 的行格式

每处理一条**上行** NAS 消息写 1 行 JSON Lines。字段:

```json
{"nf":"AMF",
 "ue_id":"imsi-999700000000001",
 "nas_type":"SecurityModeComplete",
 "t_recv":"2026-07-21T19:29:07.114838613Z",   // T1/T0，冗余，便于自包含算队列等待
 "t_start":"2026-07-21T19:29:07.115200000Z",  // T2
 "t_end":"2026-07-21T19:29:07.123500000Z",    // T8
 "sbi":[
   {"call":"UeCmRegistration","before":"...T3...","after":"...T6..."},
   {"call":"SDMGetAmData",    "before":"...T3...","after":"...T6..."}
 ]}
```

- 时间戳格式与现有 log **完全一致**:RFC3339 纳秒 UTC(复用 accesslog 的
  `formatTime()`),分析脚本的 `_ts_to_ns()` 直接可解析。
- `sbi` 可为空数组(该 NAS 未触发下游调用,合法)。
- `call` 用一个稳定短名(见 §4 表),用于按 `(ue_id, call)` 与 `HTTP_log` join。

**可算出的分段**(配合 HTTP_log / AMF_log,按 ue_id + call join):

| 分段 | 公式 | 语义 |
|------|------|------|
| 队列等待 | `t_start − t_recv` | T0→T2,channel 排队 + Go 调度 |
| 本地准备 | `sbi[0].before − t_start` | T2→T3,NAS 解码 + 状态机 |
| 序列化+建连 | `HTTP_log.req_time − sbi[].before` | T3→T4,HTTP_log 看不到 |
| 网络往返 | `HTTP_log.resp_time − req_time` | T4→T5,现有 |
| 反序列化 | `sbi[].after − HTTP_log.resp_time` | T5→T6,HTTP_log 看不到 |
| SBI 总阻塞 | `sbi[].after − sbi[].before` | T3→T6 |
| 收尾+发下行 | `t_end − sbi[last].after` | T6→T8 |
| worker 总处理 | `t_end − t_start` | T2→T8 |

---

## 2. 改动总览(4 处新增 + 0 处删除)

| # | 文件 | 改什么 | 风险 |
|---|------|--------|------|
| A | **新建** `internal/msgtrace/msgtrace.go` | goroutine-local 追踪器,累积 T2/T3/T6/T8 + sbi[] | 无(叶子包,仿 recvtime) |
| B | `internal/accesslog/accesslog.go` | 加 `kindWorker` + 第 4 个文件 + `LogWorker()` | 低(照现有多文件模式扩展) |
| C | `internal/ngap/scheduler.go` | worker 循环里 `Begin(T2)` / `End(T8)` 并写出 | 低(仅包住现有 handler 调用) |
| D | `internal/nas/handler.go` | NAS 解码后补 `ue_id` / `nas_type` 到当前 trace | 低(复用现有 logUplinkNAS 位置) |
| E | 各 `internal/sbi/consumer/*.go` 的 `Send*` 函数 | 入口 `time.Now()` + `defer AddSBI(T3,T6)` | 中(点多但机械) |

**全部写路径都只调 `enqueue`(非阻塞、drop-on-full),不做同步 I/O。**

---

## 3. 各文件改动细节

### A. 新建 `internal/msgtrace/msgtrace.go`(仿 `internal/recvtime`)

goroutine-local 追踪器。为什么用 goroutine-local:一条 NAS 的处理(含多次 SBI)
全程在**同一个 worker goroutine 同步**跑完,无 channel hop,和 `recvtime` 同理,
避免把时间戳参数穿过生成代码 `dispatcher_generated.go`(DO NOT EDIT)。

```go
package msgtrace

import ("sync"; "time")

type sbiCall struct { Call string; Before, After time.Time }

type Trace struct {
    UeID, NasType   string
    Recv, Start, End time.Time
    SBI             []sbiCall
}

var byG sync.Map // goroutine id -> *Trace  (goroutineID() 复用 recvtime 里的实现或抽公共)

// Begin: worker 取出 task 时调,记 T0(recv)与 T2(start)。
func Begin(recv, start time.Time) { byG.Store(gid(), &Trace{Recv: recv, Start: start}) }

// SetID: NAS 解码后补 ue_id / nas_type(此时才知道)。
func SetID(ueID, nasType string) {
    if v, ok := byG.Load(gid()); ok { t := v.(*Trace); t.UeID = ueID; t.NasType = nasType }
}

// AddSBI: 每次 Send*Request 前后调,append 一条 (T3,T6)。
func AddSBI(call string, before, after time.Time) {
    if v, ok := byG.Load(gid()); ok { t := v.(*Trace); t.SBI = append(t.SBI, sbiCall{call, before, after}) }
}

// End: handler 返回时调,记 T8,取出并删除当前 goroutine 的 trace。
func End(end time.Time) *Trace {
    if v, ok := byG.LoadAndDelete(gid()); ok { t := v.(*Trace); t.End = end; return t }
    return nil
}
```

- `gid()` = 现有 `recvtime.goroutineID()` 的同款实现(可把它抽到一个公共叶子包,
  或在 msgtrace 里复制一份;复制成本极低,和 recvtime 已有先例一致)。
- `Begin`/`End` 必须成对(和 `recvtime.Set/Clear` 一样),避免 worker 复用时泄漏。

### B. `internal/accesslog/accesslog.go` 加第 4 个流

1. 常量区([:37-63](NFs/amf/internal/accesslog/accesslog.go)):
   - `kindWorker recKind = iota` 追加到 `kindHTTP/kindDB/kindNGAP` 之后。
   - `envWorkerPath = "WORKER_LOG_PATH"`,`defaultWorkerPath = "/tmp/AMF_worker_log.txt"`。
2. `writerLoop()`([:110-159](NFs/amf/internal/accesslog/accesslog.go)):
   - 多开一个 `workerFile, workerW := openLog(envOr(envWorkerPath, defaultWorkerPath))`。
   - `defer` 里补 flush/close;`flush()` / `drainAll()` / `writeRec()` 的签名各加一个
     `workerW` 参数,`writeRec` 的 `switch` 加 `case kindWorker: w = workerW`。
3. 新增 `LogWorker(t *msgtrace.Trace)`(仿 `LogNGAP` 手工拼 JSON,末尾 `enqueue(kindWorker, b)`):
   - 拼 `nf/ue_id/nas_type/t_recv/t_start/t_end`,再拼 `"sbi":[...]` 数组
     (遍历 `t.SBI`,每个元素 `{"call":..,"before":..,"after":..}`,时间用 `formatTime`)。
   - **注意**:为避免在 accesslog 里 import msgtrace 造成的耦合,`LogWorker` 的参数可
     改为普通值/切片(如 `LogWorker(ueID, nasType string, recv, start, end time.Time, sbi []SBIView)`),
     由 scheduler 从 `*Trace` 拆出后传入;`SBIView` 定义在 accesslog 里。二选一,推荐后者
     (accesslog 不依赖 msgtrace,依赖方向单一)。

### C. `internal/ngap/scheduler.go` worker 循环(现有 [:71-86](NFs/amf/internal/ngap/scheduler.go))

在 `case task := <-w.taskChan:` 分支包住现有 handler 调用:

```go
case task := <-w.taskChan:
    start := time.Now()                       // T2
    recvtime.Set(task.RecvTime)               // 现有,保持
    msgtrace.Begin(task.RecvTime, start)       // 新增:T0 + T2
    w.handler(task.Conn, task.Message)         // 现有
    if tr := msgtrace.End(time.Now()); tr != nil {   // 新增:T8 + 取出
        accesslog.LogWorker(tr.UeID, tr.NasType, tr.Recv, tr.Start, tr.End, toViews(tr.SBI))
    }
    recvtime.Clear()                           // 现有,保持
```

- `drainAndExit()`([:89-103](NFs/amf/internal/ngap/scheduler.go))里的 handler 调用同样
  包一层(可选,残余消息也想记录时才加)。
- `LogWorker` 只 `enqueue`,不阻塞 worker。

### D. `internal/nas/handler.go` 补 ue_id / nas_type

现有 `logUplinkNAS`([:94-138](NFs/amf/internal/nas/handler.go))在 NAS 解码后已经算出
`nasType` 和 `ueID`。在它末尾(现有 `accesslog.LogNGAP(...)` 之后)加一行:

```go
msgtrace.SetID(ueID, nasType)   // 把 worker trace 的 ue_id / nas_type 补上
```

这样 worker 行的 `ue_id`/`nas_type` 与 AMF_log 的 UL 记录同源、一致。

### E. 各 `internal/sbi/consumer/*.go` 的 SBI 发送函数打 T3/T6

对每个真正向下游 NF 发请求的 `Send*Request` / `SDMGet*` / `*Registration` /
`AMPolicyControl*` 等函数(约 26 个,分布在 ausf/udm/pcf/nrf/nssf/amf/smf_service.go),
在函数体最前面加:

```go
before := time.Now()                                            // T3
defer func() { msgtrace.AddSBI("<CALL_NAME>", before, time.Now()) }()  // T6
```

- `<CALL_NAME>` 用稳定短名(见 §4),便于与 HTTP_log 的 uri join。
- `defer` 保证任何 return 路径(含错误分支)都记 T6。
- 若某函数内部**不发 HTTP**(如纯缓存命中的 discovery),它仍会记一条 before≈after 的
  sbi,可在分析时按 `sbi[].after-before ≈ 0` 过滤,或该函数不打点——二选一,推荐都打点
  (数据更完整,分析端过滤即可)。

---

## 4. `call` 命名表(建议,稳定短名,便于 join HTTP_log)

| consumer 函数 | call 名 | 对应 HTTP_log uri 关键片段 |
|---------------|---------|---------------------------|
| SendUEAuthenticationAuthenticateRequest | `AUSF_ue-authentications` | `/nausf-auth/v1/ue-authentications` |
| SendAuth5gAkaConfirmRequest | `AUSF_5g-aka-confirmation` | `.../5g-aka-confirmation` |
| UeCmRegistration | `UDM_uecm-registration` | `/nudm-uecm/v1/.../registrations/amf-3gpp-access` |
| SDMGetAmData | `UDM_sdm-am-data` | `/nudm-sdm/v2/.../am-data` |
| SDMGetSmfSelectData | `UDM_sdm-smf-select` | `.../smf-select-data` |
| SDMGetUeContextInSmfData | `UDM_sdm-ue-context-in-smf` | `.../ue-context-in-smf-data` |
| SDMSubscribe | `UDM_sdm-subscribe` | `/sdm-subscriptions` |
| SDMGetSliceSelectionSubscriptionData | `UDM_sdm-nssai` | `.../nssai` |
| AMPolicyControlCreate | `PCF_am-policy-create` | `/npcf-am-policy-control/v1/policies` |
| SearchAmfCommunicationInstance / SendSearchNFInstances | `NRF_disc` | `/nnrf-disc/...` |

(其余按同样规则补;分析时以此列做 group / join 键。)

---

## 5. 不做的事(已查证)

- **不记锁 log**:reg 主路径上无应用层锁(OAuth 关闭 → 无 token 锁;`ue.Lock` 只在
  PDU/SMF 路径;`AllocateAmfUeNgapID` 每 UE 一次、非 per-SBI)。故删去原方案的 T3a/T3b。
- **不改** AMF_log / HTTP_log / DB_log 及任何分析脚本。
- **不在 RoundTripper 记 T3/T6**:RoundTrip 内只能拿到 T4/T5;T3/T6 早于/晚于 RoundTrip,
  必须在 consumer 层记。

---

## 6. 验证步骤

1. `cd NFs/amf && go build ./...` 编译通过。
2. 单 UE 跑一次 registration,检查 `/tmp/AMF_worker_log.txt`:
   - 恰好 3 行(RegistrationRequest / AuthenticationResponse / SecurityModeComplete)。
   - SecurityModeComplete 那行 `sbi[]` 含多次 UDM/PCF 调用。
   - 每行 `t_recv ≤ t_start ≤ sbi[0].before ≤ sbi[last].after ≤ t_end` 单调。
   - `sbi[].before/after` 能与同 UE 的 HTTP_log `req_time/resp_time` 对齐
     (before ≤ req_time ≤ resp_time ≤ after)。
3. 压测确认 `accesslog.Dropped()` 为 0(或极小),说明异步写入没丢数据、没背压。
4. 对照:worker 行的 `t_end − t_start` 应 ≈ 现有分析里"整条 NAS 处理墙钟"。

---

## 7. 落地后能回答的问题

- **队列等待 (`t_start − t_recv`)** 是否随 RQ 上升 → 验证 Go 调度/排队假设。
- **序列化+建连 (`req_time − before`) / 反序列化 (`after − resp_time`)** 是否随 RQ
  上升 → 验证 http2 连接层(建连/等流)这一 HTTP_log 照不到的隐藏开销。
- 三者与现有 HTTP_log 的 T4→T5 相加 = 每次 SBI 的完整可归因分解,最终定位
  "AMF local latency 随 req rate 上升"到底落在哪一段。
```
