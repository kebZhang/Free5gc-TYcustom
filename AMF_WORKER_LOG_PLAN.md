# AMF Worker-Log Instrumentation Plan

> STATUS: REVISED — zero-overhead redesign (single plan). The first implementation
> used a goroutine-local tracker keyed by goroutine id (`internal/msgtrace` +
> `byG sync.Map`, id via `runtime.Stack`). Measurement showed that at high
> request rates this **inflates** AMF-local latency: an A/B on the same host
> (0722 no worker_log vs 0723 with worker_log) showed RQ800 median 63ms → 453ms
> (~7×), and AMF process CPU ~1200% → ~2500%. Root cause is the **per-message,
> per-SBI** cost paid ON the business goroutine:
>   - `runtime.Stack(buf, false)` in `goroutineID()` — called ~30×/registration
>     (Begin/SetID/AddSBI×N/End), each call briefly stops the goroutine to read
>     its stack header; and
>   - a single global `sync.Map` (`byG`) read/written by hundreds of concurrent
>     goroutines — cache-line contention that grows super-linearly with load.
> These land exactly on the AMF's hottest, most schedule-sensitive path, so the
> instrumentation itself became a load-dependent latency source.
>
> **本方案:全部在 handler 侧完成,worker 不改,零抓栈、零共享容器、零锁。**
> 关键突破(已核对代码):worker_log 的 `t_recv` 字段与 AMF_log 的 UL `sctp_time`
> **本来就是同一个值**(分析脚本正是用 `t_recv == sctp_time` join,实测 3000/3000
> 逐字节相同),所以 **worker_log 根本不需要自己携带 T_recv** —— 分析时用 AMF_log 的
> `sctp_time` 补即可。T_recv 一旦不需要跨层传递,worker→handler 之间就没有任何东西
> 要传,于是:
>   - **T2** = `HandleNAS` 第一行 `time.Now()`(与 worker 取出仅差一次
>     `dispatchMain` 调用,~µs);
>   - **T8** = `HandleNAS` 末尾 `time.Now()`;
>   - **ue_id / nas_type / sbi[]** 本来就都在 handler 侧;
>   - **T3/T6** 经 `ue.WorkerTrace`(consumer 都持有 `ue`)。
>
> 这样彻底绕开了那条硬约束:`Dispatch(手写) → dispatchMain(**生成代码
> dispatcher_generated.go, DO NOT EDIT**) → handler → HandleNAS` —— 时间戳无法作为
> 参数穿过生成代码(这正是旧方案不得不用 goroutine-local 的原因)。本方案不再需要穿过
> 它,因此**不需要 goroutineID / runtime.Stack,也不需要任何按 conn/gid 索引的共享
> map**(后者在非 dGNB 模式下还有并发写风险:`hashUEID` 会把同 conn 的不同 UE 分到不同
> worker,已核对 scheduler.go:283-290)。
>
> 每消息抓栈次数:**旧方案 ~21 次 → 本方案 0 次**;全局 `sync.Map` 争用:**有 → 无**。
> 高 RQ 下写 log 不再引入可测 latency,且进一步提高 RQ 也成立(见 §7 负载敏感性分析)。
>
> 改动文件:
>   - EDIT   internal/msgtrace/msgtrace.go   (改为显式 `*Trace`,删 byG/gid)
>   - DELETE internal/msgtrace/goroutineid.go
>   - EDIT   internal/context/amf_ue.go      (加 `WorkerTrace *msgtrace.Trace` 字段)
>   - EDIT   internal/accesslog/accesslog.go (kindWorker + 第4文件 + LogWorker + SBIView)
>   - EDIT   internal/nas/handler.go         (HandleNAS 建 trace / 绑定 / flush)
>   - EDIT   internal/sbi/consumer/{ausf,udm,pcf,nssf}_service.go (经 ue.WorkerTrace 打点)
>   - **internal/ngap/scheduler.go 不改**
> 新日志文件:/tmp/AMF_worker_log.txt(可用 WORKER_LOG_PATH 覆盖)。
>
> 背景(为什么要改):最初实现用 goroutine-local(`byG sync.Map` + `runtime.Stack` 取
> goroutine id),实测在高 RQ 下**自身成为 latency 来源**:同机器 A/B(0722 无
> worker_log vs 0723 有 worker_log)RQ800 中位数 63ms → 453ms(~7×),AMF 进程 CPU
> ~1200% → ~2500%。根因是每消息 ~21 次 `runtime.Stack`(其中 18 次在 SBI 循环里)
> 加上一个被数百 goroutine 争抢的全局 `sync.Map`,两者都随负载超线性放大。本方案将
> 这两项彻底消除。


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
| **T1 / T0** | AMF 收到上行 NAS(SCTPRead)的时刻 | 有 | `AMF_log.txt` UL `sctp_time`(不动;**worker_log 不再重复记**) |
| **T2** | 开始处理这条 NAS(`HandleNAS` 入口) | **新增** | `AMF_worker_log.txt` `t_start` |
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
 "t_start":"2026-07-21T19:29:07.115200000Z",  // T2 (HandleNAS 入口)
 "t_end":"2026-07-21T19:29:07.123500000Z",    // T8 (HandleNAS 末尾)
 "sbi":[
   {"call":"UDM_uecm-registration","before":"...T3...","after":"...T6..."},
   {"call":"UDM_sdm-am-data",      "before":"...T3...","after":"...T6..."}
 ]}
```

- **不再有 `t_recv` 字段**。T1/T_recv 由 `AMF_log.txt` 的 UL `sctp_time` 提供 ——
  两者本来就是同一个值(旧实现里 `t_recv == sctp_time`,实测 3000/3000 逐字节相同),
  记两遍是冗余。去掉它,worker→handler 之间就无需传递任何东西,这是本方案零开销的
  前提。
- **join 键改为 `(ue_id, nas_type)` + 时间窗**:同一 UE 同一 NAS 类型在一次注册里只出现
  一次,worker 行的 `t_start` 必落在 AMF_log 对应 UL `sctp_time` 之后、且是最近的一条。
  分析脚本按此配对即可(见 §6.6 的脚本适配说明)。
- 时间戳格式与现有 log 完全一致:RFC3339 纳秒 UTC(复用 `formatTime()`)。
- `sbi` 可为空数组(该 NAS 未触发下游调用,合法)。
- `call` 用稳定短名(见 §4 表),用于与 `HTTP_log` join。

**可算出的分段**(配合 HTTP_log / AMF_log join):

| 分段 | 公式 | 语义 |
|------|------|------|
| 队列等待 | `t_start − AMF_log.UL.sctp_time` | T1→T2,channel 排队 + Go 调度 |
| 本地准备 | `sbi[0].before − t_start` | T2→T3,NAS 解码 + 状态机 |
| 序列化+建连 | `HTTP_log.req_time − sbi[].before` | T3→T4,HTTP_log 看不到 |
| 网络往返 | `HTTP_log.resp_time − req_time` | T4→T5,现有 |
| 反序列化 | `sbi[].after − HTTP_log.resp_time` | T5→T6,HTTP_log 看不到 |
| SBI 总阻塞 | `sbi[].after − sbi[].before` | T3→T6 |
| SBI 间本地处理 | `Σ(sbi[i+1].before − sbi[i].after)` | 多 SBI 之间的状态机推进 |
| 组装+发下行 | `AMF_log.DL.sctp_time − sbi[last].after` | T6→T7 |
| 发下行后收尾 | `t_end − AMF_log.DL.sctp_time` | T7→T8 |
| handler 总处理 | `t_end − t_start` | T2→T8 |

---

## 2. 改动总览

| # | 文件 | 改什么 | 风险 |
|---|------|--------|------|
| A | `internal/msgtrace/msgtrace.go` | 改写为**显式 `*Trace`**(方法 New/SetID/AddSBI/Track/End),删 `byG`/`goroutineID` | 低(叶子包,只 import time) |
| A' | `internal/context/amf_ue.go` | 加字段 `WorkerTrace *msgtrace.Trace` | 低(仅加字段) |
| B | `internal/accesslog/accesslog.go` | `kindWorker` + 第 4 个文件 + `LogWorker()` + `SBIView` | 低(写入端机制不变) |
| C | `internal/nas/handler.go` | `HandleNAS` 里建 trace(T2)、绑定到 AmfUe、末尾 flush(T8) | 低(单个函数内闭环) |
| D | 各 `internal/sbi/consumer/*.go` 的 `Send*` | `defer ue.WorkerTrace.Track("<CALL>")()` | 中(点多但机械) |
| — | **删除** `internal/msgtrace/goroutineid.go` | 不再需要 | 无 |
| — | `internal/ngap/scheduler.go` | **不改** | — |

**关键:业务 goroutine 上的 per-message 开销从 ~21 次 `runtime.Stack` + 全局
`sync.Map` 读写,降为 0 次抓栈 + 几次指针解引用 + slice append。写入端(单 writer
goroutine + `enqueue` 非阻塞 drop-on-full)完全不变。**

---

## 3. 各文件改动细节

### A. 改写 `internal/msgtrace/msgtrace.go` — 显式携带 `*Trace`,无 goroutine-local

不再有 `byG sync.Map`、不再有 `goroutineID()`/`runtime.Stack`。`Trace` 是一个普通
结构体,由 `HandleNAS` 创建、按指针显式传递(→ `logUplinkNAS` → 绑到 `AmfUe` →
consumer)。一条 NAS 的处理全程在同一 goroutine 同步跑完(GMM 无 `go func`,FSM 同步
递归),且 scheduler 保证**同一 UE 的消息串行**(hashUEID / dGNB per-association),
所以同一个 `*Trace` 不会被并发写 —— 无需任何锁。

```go
package msgtrace

import "time"

type SBICall struct { Call string; Before, After time.Time }

type Trace struct {
    UeID, NasType string
    Start         time.Time   // T2;T8 由 flush 时现取,无需存
    SBI           []SBICall
}

// New: HandleNAS 入口创建,记 T2。
func New(start time.Time) *Trace { return &Trace{Start: start} }

// SetID: NAS 解码后补 ue_id / nas_type。nil-safe。
func (t *Trace) SetID(ueID, nasType string) {
    if t == nil { return }
    t.UeID = ueID; t.NasType = nasType
}

// AddSBI: 每次 Send*Request 前后调,append 一条 (T3,T6)。nil-safe。
func (t *Trace) AddSBI(call string, before, after time.Time) {
    if t == nil { return }
    t.SBI = append(t.SBI, SBICall{Call: call, Before: before, After: after})
}

// Track: consumer 便捷封装,入口记 T3,返回的闭包在 return 时记 T6。nil-safe。
// 用法: defer ue.WorkerTrace.Track("UDM_sdm-am-data")()
func (t *Trace) Track(call string) func() {
    before := time.Now()
    return func() { t.AddSBI(call, before, time.Now()) }
}
```

- **删除** `internal/msgtrace/goroutineid.go`(不再需要)。
- 没有 `Recv` 字段(T1 由 AMF_log 提供)、没有 `End` 方法(T8 在 flush 时 `time.Now()`
  现取),结构更小,每消息只分配 1 个 `Trace` + 1 个 `SBI` slice。
- 所有方法都是 nil-safe:未绑定 `*Trace` 的调用路径(如启动期 NF 注册、NRF discovery
  这类没有 AmfUe 的 SBI)自然记不到,不会 panic、不做任何工作 → 零开销。
- **如何从 consumer 拿到 `*Trace`**:consumer 里 SBI 打点处都能拿到
  `ue *amf_context.AmfUe`(见 §E 验证),`ue.WorkerTrace`(§A' 加的字段)即当前 NAS
  的 trace。因此 `Track` 的用法是 `defer ue.WorkerTrace.Track("...")()` —— 一次指针
  解引用,无 `runtime.Stack`、无 map。

### A'. `internal/context/amf_ue.go` — 加一个承载字段

在 `type AmfUe struct` 里加一个字段,作为"当前正在处理这个 UE 的这条 NAS 的 worker
trace"的挂载点:

```go
// WorkerTrace is the msgtrace.Trace of the uplink NAS currently being handled
// for this UE, bound by the NGAP worker right after NAS decode. It lets the SBI
// consumers append their (T3,T6) without any goroutine-id lookup. It is only
// ever touched on the single worker goroutine that owns this UE's messages
// (scheduler serialises per-UE), so no lock is needed. May be nil.
WorkerTrace *msgtrace.Trace
```

- 依赖方向:context → msgtrace(msgtrace 是叶子包,只 import `time`),无环。
- 每条上行 NAS 处理开始时由 worker/handler **重新绑定**(覆盖上一条的),handler 返回
  后可留着不清(下一条会覆盖);为干净起见 handler 末尾可置 nil,非必需。

### B. `internal/accesslog/accesslog.go` 加第 4 个流

1. 常量区([:37-63](NFs/amf/internal/accesslog/accesslog.go)):
   - `kindWorker recKind = iota` 追加到 `kindHTTP/kindDB/kindNGAP` 之后。
   - `envWorkerPath = "WORKER_LOG_PATH"`,`defaultWorkerPath = "/tmp/AMF_worker_log.txt"`。
2. `writerLoop()`([:110-159](NFs/amf/internal/accesslog/accesslog.go)):
   - 多开一个 `workerFile, workerW := openLog(envOr(envWorkerPath, defaultWorkerPath))`。
   - `defer` 里补 flush/close;`flush()` / `drainAll()` / `writeRec()` 的签名各加一个
     `workerW` 参数,`writeRec` 的 `switch` 加 `case kindWorker: w = workerW`。
3. 新增 `LogWorker(t *msgtrace.Trace)`(仿 `LogNGAP` 手工拼 JSON,末尾 `enqueue(kindWorker, b)`):
   - 拼 `nf/ue_id/nas_type/t_start/t_end`(**无 `t_recv`**),再拼 `"sbi":[...]` 数组
     (遍历,每个元素 `{"call":..,"before":..,"after":..}`,时间用 `formatTime`)。
   - 签名(accesslog **不** import msgtrace,依赖方向单一;`SBIView` 定义在 accesslog):
     ```go
     func LogWorker(ueID, nasType string, start, end time.Time, sbi []SBIView)
     ```
     由 `HandleNAS` 的 defer 从 `*Trace` 拆出后传入。

### C. `internal/nas/handler.go` — 在 `HandleNAS` 内闭环:建 trace(T2)、绑定、flush(T8)

**为什么全部放在 `HandleNAS` 里**:已核实的硬约束是
`ngap.Dispatch(conn,msg)`(手写)→ `dispatchMain(ran,pdu)`(**生成代码
dispatcher_generated.go,DO NOT EDIT**)→ handler → `amf_nas.HandleNAS`,时间戳无法
作为参数穿过生成代码。旧方案为此用了 goroutine-local(抓栈)。本方案**不再需要跨这
一层传任何东西**,因为 `t_recv` 已由 AMF_log 提供(见 §1),而 T2/T8/ue_id/nas_type/
sbi[] **全都能在 `HandleNAS` 内部拿到**。`HandleNAS` 正是这条上行 NAS 在 AMF 侧同步
处理的最外层函数:它返回即本条消息处理完毕。

改 `HandleNAS`([:19](NFs/amf/internal/nas/handler.go#L19)):

```go
func HandleNAS(ranUe *amf_context.RanUe, procedureCode int64, nasPdu []byte, initialMessage bool) {
    tStart := time.Now()                       // T2:本条 NAS 开始处理
    tr := msgtrace.New(tStart)                 // 建 trace(纯结构体,无查表/无抓栈)
    defer func() {                             // T8:handler 返回即处理完
        if tr.NasType != "" {                  // 只记我们关心的 3 种上行 NAS
            accesslog.LogWorker(tr.UeID, tr.NasType, tr.Start,
                time.Now(), toViews(tr.SBI))   // 只 enqueue,不阻塞
        }
        if ranUe != nil && ranUe.AmfUe != nil {
            ranUe.AmfUe.WorkerTrace = nil      // 解绑,避免跨消息串用
        }
    }()

    ... 现有逻辑(含 ranUe==nil 等早退分支;早退时 tr.NasType=="" 自然不记) ...

    logUplinkNAS(ranUe, msg)                   // 现有:内部补 ue_id/nas_type 并绑定(见下)
    // ↑ 绑定发生在这里,而 Dispatch(→FSM→触发 SBI) 在其后(现有 :81),顺序天然正确

    ... 现有 Dispatch(ranUe.AmfUe, ...) ...
}
```

在 `logUplinkNAS`([:95-138](NFs/amf/internal/nas/handler.go#L95))末尾(现有
`accesslog.LogNGAP(...)` 之后)加两行,把 ue_id/nas_type 填进 trace 并绑定到 AmfUe:

```go
tr.SetID(ueID, nasType)                 // 补 ue_id / nas_type
if ranUe.AmfUe != nil {
    ranUe.AmfUe.WorkerTrace = tr        // 绑定:之后 consumer 经 ue.WorkerTrace 打 SBI
}
```

- `logUplinkNAS` 需要能拿到 `tr`:把它作为参数传入即可(`logUplinkNAS(ranUe, msg, tr)`)
  —— 这是同一文件内的手写函数,改签名无障碍,**不涉及生成代码**。
- 已核实:`logUplinkNAS` 在 [:69](NFs/amf/internal/nas/handler.go#L69) 被调,`Dispatch`
  (进 FSM、触发所有 SBI)在 [:81](NFs/amf/internal/nas/handler.go#L81),**绑定早于任何
  SBI**;且此处 `ranUe.AmfUe` 必已创建(:42-53)。
- `defer` 保证任何 return 路径(含早退)都不漏 flush;`tr.NasType == ""` 时(非我们关心
  的 NAS、或早退)不写日志,与旧行为一致。
- **`internal/ngap/scheduler.go` 完全不改。**

- 绑定后,同一条 NAS 触发的所有 SBI(全在同一 goroutine 同步跑)都通过
  `ue.WorkerTrace` 找到同一个 trace;`HandleNAS` 的 defer 在返回前解绑(置 nil),
  下一条 NAS 重新绑定,不会跨消息串用。

### D. 各 `internal/sbi/consumer/*.go` 的 SBI 发送函数打 T3/T6

打点函数都能拿到 `ue *amf_context.AmfUe`(已核对:ausf 的
`SendUEAuthenticationAuthenticateRequest`/`SendAuth5gAkaConfirmRequest`、udm 的
6 个 `SDMGet*`/`UeCmRegistration`、pcf 的 `AMPolicyControlCreate`、nssf 的
`NSSelectionGetForRegistration` —— 共 11 个,首参或函数体首行即用 `ue.XxxUri`)。
每个函数体最前面把原来的 `msgtrace.Track(...)` 改成经 `ue.WorkerTrace`:

```go
defer ue.WorkerTrace.Track("<CALL_NAME>")()   // T3 入口 / T6 return;nil-safe
```

- `<CALL_NAME>` 用稳定短名(见 §4)。`Track` 是 `*Trace` 的方法,nil-safe:UE 尚未
  绑定 trace(理论上不会发生在这些函数)时静默不记,不 panic。
- `defer` 保证任何 return 路径(含错误分支)都记 T6。
- **NRF discovery(`SendSearchNFInstances`)不打点**:它没有 `ue` 参数、且被分析端排除。
  这也顺带去掉了原方案里那 ~4000 条缓存命中的 NRF 噪声记录(见分析:NRF 99.9% 命中缓存,
  before≈after)。若仍想记 NRF,需另想承载(不推荐)。
- 相比原方案,SBI 打点从"每次 2 次 `goroutineID()`(=`runtime.Stack`)"降为
  **"0 次抓栈、1 次指针解引用(`ue.WorkerTrace`)+ 1 次 slice append"**。这是开销的
  大头(9 个 SBI × 2 = 18 次抓栈)被彻底消除,是高 RQ 下 latency 不再上升的关键。

**每消息抓栈次数账本(为什么高 RQ 下不再增 latency):**

| 阶段 | 旧实现(goroutine-local) | 本方案 |
|------|--------------------------|--------|
| 建 trace (T2) | 1 次抓栈 | 0(`msgtrace.New`,纯结构体) |
| SetID / 绑定 | 1 次抓栈 | 0(`tr` 由参数传入 `logUplinkNAS`) |
| 每个 SBI ×9 | 2×9 = 18 次抓栈 | 0(经 `ue.WorkerTrace` 指针) |
| flush (T8) | 1 次抓栈 | 0(`HandleNAS` 的 defer 直接持有 `tr`) |
| **合计 / 消息** | **~21 次 `runtime.Stack`** | **0 次** |
| 全局 `sync.Map` 争用 | 有(随 UE 数爆炸) | **无** |

旧实现 ~21 次抓栈 + 全局 map 争用 → 本方案 **0 次抓栈、无任何共享容器**。高 RQ 下
真正非线性放大的两项(SBI 循环里的 18 次抓栈、全局 map 争用)都被彻底消除,因此写
log 不再引入可测的 latency 上升。

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
| NSSelectionGetForRegistration | `NSSF_ns-selection-registration` | `/nnssf-nsselection/...` |
| ~~SendSearchNFInstances~~ | ~~`NRF_disc`~~ | **本方案不打点**(无 `ue`,分析端排除) |

(其余按同样规则补;分析时以此列做 group / join 键。本方案下 NRF 不再产生
worker 记录,原分析脚本里"排除 NRF"的过滤仍保留即可,无害。)

---

## 5. 不做的事(已查证)

- **不记锁 log**:reg 主路径上无应用层锁(OAuth 关闭 → 无 token 锁;`ue.Lock` 只在
  PDU/SMF 路径;`AllocateAmfUeNgapID` 每 UE 一次、非 per-SBI)。故删去原方案的 T3a/T3b。
- **不改** AMF_log / HTTP_log / DB_log 及任何分析脚本。
- **不在 RoundTripper 记 T3/T6**:RoundTrip 内只能拿到 T4/T5;T3/T6 早于/晚于 RoundTrip,
  必须在 consumer 层记。

---

## 6. 验证步骤

1. `cd NFs/amf && go build ./...` 编译通过。`grep -r "runtime.Stack\|goroutineID" internal/msgtrace`
   应**无结果**(本方案已移除);`goroutineid.go` 已删除。
2. 单 UE 跑一次 registration,检查 `/tmp/AMF_worker_log.txt`:
   - 恰好 3 行(RegistrationRequest / AuthenticationResponse / SecurityModeComplete)。
   - SecurityModeComplete 那行 `sbi[]` 含多次 UDM/PCF 调用(本方案下**不含 NRF**)。
   - 每行 `t_start ≤ sbi[0].before ≤ sbi[last].after ≤ t_end` 单调;且
     `AMF_log.UL.sctp_time ≤ t_start`(跨文件 join 后验证)。
   - `sbi[].before/after` 能与同 UE 的 HTTP_log `req_time/resp_time` 对齐
     (before ≤ req_time ≤ resp_time ≤ after)。
3. 压测确认 `accesslog.Dropped()` 为 0(或极小),说明异步写入没丢数据、没背压。
4. 对照:worker 行的 `t_end − t_start` 应 ≈ 现有分析里"整条 NAS 处理墙钟"。
5. **(核心)开销回归**:同机器、同配置,跑 **有 worker_log(本方案)** 与
   **无 worker_log**(或 `MSGTRACE` 编译关掉)两组 RQ200→1000,比 PacketRusher 的
   e2e latency(latency_RQ*.txt 的 t4−t2)与 AMF 进程 CPU(Cpu_Mem_*.txt):
   - 目标:两组的 p50/p90 在噪声内**重合**(旧 goroutine-local 方案 RQ800 曾 63→453ms、
     CPU 1200%→2500%;本方案应把这个差距抹平)。
   - 若仍有系统性差距 → 说明还有残余同步开销,回查是否漏改某处成 `runtime.Stack` 路径。
6. **正确性不回归**:用 `AMF_detail_analyz.py`(或等价脚本)确认 join 命中率不降
   (complete_UEs、no_http=0),分段自洽(ΣP ≡ total,残差≈0)。

**6.6 分析脚本需要的唯一适配**(因为去掉了 `t_recv` 字段):
原来 T1 join 用 `worker.t_recv == AMF_log.UL.sctp_time`(精确相等)。现在改为按
`(ue_key, nas_type)` 找 AMF_log 的 UL 记录,并要求 `sctp_time ≤ t_start` 且取**最近的
一条**(同一 UE 同一 NAS 类型一次注册只出现一次,无歧义)。其余(SBI↔HTTP_log 的
`before ≤ req ≤ resp ≤ after` 时间窗 join、T7 取 DL `sctp_time`)完全不变。

---

## 7. 负载敏感性分析(为什么进一步提高 RQ 仍然成立)

逐项列出本方案在业务 goroutine 上的每消息开销,以及它们随 RQ / UE 数如何变化:

| 操作 | 每消息次数 | 随 RQ 放大? | 说明 |
|------|-----------|-------------|------|
| `runtime.Stack` | **0** | — | 已彻底移除 |
| 全局 `sync.Map` 读写 | **0** | — | 已彻底移除 |
| `ue.WorkerTrace` 指针解引用 | 9(每 SBI 1 次) | 否 | O(1),与并发无关 |
| `t.SBI` slice append | 9 | 否 | 每 NAS 固定 1 或 7 个,最多扩容 2–3 次 |
| `time.Now()` | ~20 | 否 | vDSO 调用,常数,不加锁 |
| `&Trace` + slice 分配 | 2 个小对象 | 线性 | GC 压力线性增长,但相对 free5gc 每次注册本身
  分配的成百上千个对象(NAS 解码、HTTP 编解码)占比极小 |
| `enqueue` channel send | 1 | 轻微 | `hchan.lock` 争用随并发上升,但**此 channel 现有
  HTTP/DB/AMF_log 已在共用**,且 0722(无 worker_log 但有其余三个 log)RQ800 仅 63ms
  → 证明它不是瓶颈;且为非阻塞 `select/default`,满则 drop,永不背压业务 |

**结论**:高 RQ 下真正会超线性放大的两项(抓栈、全局 map)已归零;其余全是 O(1) 常数
或已被证明非瓶颈的项。因此**进一步提高 RQ 时,写 log 仍不会造成 latency 上升**。

若将来把 RQ 推到极端(比现在再高数倍)且实测发现 channel/GC 成为可见开销,可追加两个
**可选**加固(非本方案必需):
1. **per-worker 分片 channel**:`queue` 改为按 worker 取模的 N 个 channel,单 writer
   goroutine 轮询 drain,消除 `hchan.lock` 争用;
2. **`sync.Pool` 复用 `Trace`**:flush 后归还,消除每消息 2 个对象的 GC 压力。

---

## 8. 落地后能回答的问题

- **队列等待 (`t_start − AMF_log.UL.sctp_time`)** 是否随 RQ 上升 → 验证 Go 调度/排队假设。
- **序列化+建连 (`req_time − before`) / 反序列化 (`after − resp_time`)** 是否随 RQ
  上升 → 验证 http2 连接层(建连/等流)这一 HTTP_log 照不到的隐藏开销。
- 三者与现有 HTTP_log 的 T4→T5 相加 = 每次 SBI 的完整可归因分解,最终定位
  "AMF local latency 随 req rate 上升"到底落在哪一段。
```
