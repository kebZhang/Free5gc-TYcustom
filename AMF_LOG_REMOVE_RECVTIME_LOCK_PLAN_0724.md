# AMF_log 保留、去除 recvtime 全局锁竞争 —— 修改 Plan (2026-07-24)

## 目标
保留 AMF_log 中「上行 SCTP 收到时间(UL T1)」和「下行 SCTP 发出时间 + NAS 类型/UE (DL)」这两条记录，
但**彻底去掉 `internal/recvtime` 包造成的锁竞争**，且方案对**任意拓扑通用安全**（含未来「1 个 gNB 带多个 UE」）。

---

## 一、为什么 recvtime 是锁热点（基于代码 + mutex profile 实测）

`internal/recvtime/recvtime.go` 现状：
- 两个**全局 `sync.Map`**：`byG`（goroutine id → 收到时间）、`dlByG`（goroutine id → {nasType,ueID}）。
- key 由 `goroutineID()` 得到，而 `goroutineID()` **每次都调 `runtime.Stack()`**（recvtime.go:116-129）。

每处理一条上行 NAS，worker goroutine 触发（实测调用点）：
- 上行：`recvtime.Set`+`Clear`（ngap/scheduler.go:76,78,94,96；ngap/service/service.go:290,292）、
  `recvtime.Current`（nas/handler.go:141）。
- 下行：每次 `Send*` 都 `recvtime.SetDLNas`+`ClearDLNas`（gmm/message/send.go 多处），底层 `CurrentDLNas`（ngap/message/send.go:61）。

即**每条 NAS 对全局 map 做 6+ 次 Store/Load/Delete，每次还 `runtime.Stack()`**，高并发下成为全局串行点。

mutex profile 实测（`C6525100g_TrueTR_mutexprofile_0724`，RQ1000 净值）：
- 锁总等待从 RQ600 的 20.35s → RQ1000 的 225.37s（≈11×）。
- 按调用来源 cum：`recvtime.CurrentDLNas` 71.56s、`SetDLNas` 46.65s、`ClearDLNas` 45.96s、`Current` 20.92s、`Set`/`Clear` ~30s
  —— **recvtime 一个包占 ~95% 锁等待**；榜首 `runtime.unlock` 96.9%。

**这是观察者效应**：为喂 AMF_log 而加的 recvtime，本身制造了它要测的大部分 AMF-local 延迟。
（`internal/msgtrace` 的包头注释亦证实：同款旧实现「~21 runtime.Stack calls per NAS message，RQ800 使 AMF-local 中位数膨胀约 7×」。）

---

## 二、方案选型（已复核，选定「选项 2」）

| 方案 | 通用安全(任意拓扑) | 是否碰生成器 | 改动量 | 结论 |
|---|---|---|---|---|
| 1 全量参数透传 | ✅ | ✅ 大改(~90 handler) | 大 | 过度工程，否决 |
| **2 只给 3 个上行 handler 传参** | ✅ | ✅ 小改(有先例) | 中 | **选定** |
| 3 挂 AmfRan/WorkerTrace | ❌ 仅 1UE-1gNB 安全 | ❌ 不碰 | 小 | 未来 1gNB 多 UE 会数据竞争，否决 |

**为什么选 2**：函数参数放在 goroutine 栈上、不共享、天然零锁，对任意拓扑安全；且改动被「只碰 3 个上行
handler」和「生成器已有 `message` 参数同款先例」压到可控。选项 3 会在「1 个 gNB 多 UE + 非 dGNB 模式」下让
`AmfRan` 被多 worker 并发访问而竞争，不满足前瞻要求。

### 关键背景：什么是「生成器」，以及它如何编译

`internal/ngap/` 下的三类文件：
```
ngap_generator.go        ← 生成器：一个会打印 Go 代码的程序。带 //go:build ignore，普通 go build 会跳过它
        │ 由 //go:generate go run ngap_generator.go 触发（声明在 ngap_generate.go:3）
        │ 读取 internal/ngap/asn1/38413-fd0.asn，写出到当前目录
        ▼
dispatcher_generated.go  ← 生成产物，顶部标 "DO NOT EDIT"（含 dispatchMain）
handler_generated.go     ← 生成产物，顶部标 "DO NOT EDIT"（~11000 行，含 ~90 个 handler）
```
**要改 dispatchMain / handler 的签名，必须改 `ngap_generator.go` 的打印语句，再重新运行生成器，
不能手改生成文件（会被覆盖）。**

### 现成先例（可直接仿照）
`InitialUEMessage` 已经通过一个叫 `messageAppend` 的机制，把一个额外参数 `message` 从 `dispatchMain`
一路传到了 `handleInitialUEMessageMain`。位置：ngap_generator.go 的 275-276、278、299-300、556-557、640-643。
**`recvTime` 参数照搬这条路即可——`message` 能通，`recvTime` 就能通。**

---

## 三、完整参数链（每一环已验证）

```
worker(有 task.RecvTime)                                     [service.go 手写]
 └ HandleMessage(conn, msg, recvTime)                         改签名+调用
   └ Dispatch(conn, msg, recvTime)          [dispatcher.go 手写] 改签名+调 dispatchMain
     └ dispatchMain(ran, message, recvTime) [生成] 改生成器
       └ handlerUplinkNASTransport(ran, initMsg, recvTime)     [生成] 改生成器
         └ handleUplinkNASTransportMain(ran, ranUe,…, recvTime)[handler.go 手写] 改签名
           └ HandleNAS(ranUe,…, recvTime)   [nas/handler.go 手写] 改签名
             └ logUplinkNAS(ranUe,msg,tr, recvTime) 用 recvTime 取代 recvtime.Current()
```
上行 3 个需要 recvTime 的 handler：`UplinkNASTransport`、`InitialUEMessage`、`NASNonDeliveryIndication`
（其余 ~87 个 handler 不传，保持原状）。

---

## 四、具体改动清单

### A. 手写文件（直接改）
1. **`internal/ngap/service/service.go`**
   - `type NGAPHandler.HandleMessage` 签名 `func(conn, msg)` → `func(conn, msg, recvTime time.Time)`（:22）。
   - worker 调用处 `handler.HandleMessage(conn, msg)` → `(conn, msg, task.RecvTime)`（:291 附近）。
   - fallback 路径（:290-292）里删除 `recvtime.Set/Clear`，改为把 recvTime 直接传入（若走 fallback 的 HandleMessage）。
2. **`internal/ngap/dispatcher.go`**
   - `Dispatch(conn, msg)` → `Dispatch(conn, msg, recvTime time.Time)`（:13）；`dispatchMain(ran, pdu)` → `(ran, pdu, recvTime)`（:55）。
3. **`internal/ngap/handler.go`**（三个 Main 手写）
   - `handleUplinkNASTransportMain`（:116）、`handleInitialUEMessageMain`（:430）、`handleNASNonDeliveryIndicationMain`（:1781）
     签名末尾加 `recvTime time.Time`；调 `amf_nas.HandleNAS(...)` 处（:136/:572/:1791）加实参 `recvTime`。
4. **`internal/nas/handler.go`**
   - `HandleNAS(ranUe, procedureCode, nasPdu, initialMessage)` → 末尾加 `recvTime time.Time`（:20）。
   - `logUplinkNAS(ranUe, msg, tr)` → 加 `recvTime`；函数内把 `t, ok := recvtime.Current()`（:141）
     替换为 `t := recvTime`（recvTime 非零即用；零值则按原逻辑跳过）。
   - 删除本文件 `recvtime` import。

### B. 生成器（改 `ngap_generator.go`，仿照 `messageAppend` 加 `recvTimeAppend`）
5. handler 外层签名：在生成 `func handler%s(ran …)` 处（:278 用到 `messageAppend`），
   为上行 3 个 msgName 追加 `, recvTime time.Time`（新增 `recvTimeAppend`，仅这 3 个非空）。
   → 参考 :275-276 `if msgName == "InitialUEMessage" { messageAppend = ", message *ngapType.NGAPPDU" }`。
6. `...Main` 参数定义：:299-300 同款，为这 3 个 msgName 往 `mainFuncArgDefs`/`mainFuncArgs` 追加 `recvTime`。
7. `dispatchMain` 签名：:620 `func dispatchMain(ran …)` 加 `, recvTime time.Time`。
8. `dispatchMain` 调 handler：:640-643，为这 3 个 msgName 让 `messageAppend`/新 append 带上 `recvTime` 实参。
9. handler 调 `...Main`：:556-557 由 `mainFuncArgs` 自动带出（已含 recvTime，无需额外改）。
10. **重新运行生成器**（见第五节），重生成 dispatcher_generated.go / handler_generated.go 并提交。

### C. 下行（不碰生成器）
11. **`internal/gmm/message/send.go`**：所有 `recvtime.SetDLNas("X", dlUeID(amfUe))` →
    `amfUe.WorkerTrace.SetDLNas("X", dlUeID(amfUe))`；删除配套 `recvtime.ClearDLNas()`
    （WorkerTrace 生命周期由 HandleNAS 的 defer 统一 unbind）。
    > T3560 等**定时器重传在独立 goroutine**（send.go:197-203），无 WorkerTrace 上下文：
    > 该回调本地已知 nasType/ueID，直接 `accesslog.LogNGAP("DL", nasType, ueID, time.Now())`，不经全局 map。
12. **`internal/msgtrace/msgtrace.go`**：给 `Trace` 加 `dlNasType,dlUeID string` 字段 +
    nil-safe 方法 `SetDLNas(nasType,ueID)` / `DLNas()(string,string,bool)`（仿现有 `SetID`/`Track` 风格）。
13. **`internal/ngap/message/send.go`**：DL 日志点从最底层 `SendToRan`（:61 `recvtime.CurrentDLNas()`）
    **上移到 `SendToRanUe(ue *context.RanUe, …)`**（:69，该层有 `ue.AmfUe`）：
    读 `ue.AmfUe.WorkerTrace.DLNas()`，命中则 `accesslog.LogNGAP("DL", nasType, ueID, sentTime)`。
    删除底层对 recvtime 的依赖。

### D. 删除
14. 删除 `internal/recvtime/` 整个包，并移除所有残留 import
    （ngap/scheduler.go、ngap/service/service.go、nas/handler.go、gmm/message/send.go、ngap/message/send.go）。

---

## 五、编译流程注意（重要——当前 Docker build 不会跑生成器）

当前 build 用 `go build`（Dockerfile.custom），**只编译已生成的 `*_generated.go`，不会触发 `go generate`**，
而 `ngap_generator.go` 带 `//go:build ignore` 也不会被 `go build` 当普通源码。因此：

**改完生成器后，必须先在本地重新生成，再 build，否则生成文件与手写签名不匹配 → 编译失败。**

推荐做法（二选一）：
- **方式甲（本地生成后提交，最稳）**：
  ```bash
  cd NFs/amf/internal/ngap
  go generate            # = go run ngap_generator.go，读 asn1/38413-fd0.asn，重写两个 *_generated.go
  cd -                   # 回仓库根，git add 变更的 dispatcher_generated.go / handler_generated.go
  # 然后照原有 Dockerfile.custom 正常 docker build（go build 会用新生成文件）
  ```
- **方式乙（在 Dockerfile 里生成）**：在 `COPY . .` 之后、`go build` 之前插入一步：
  ```dockerfile
  RUN cd NFs/amf/internal/ngap && go run ngap_generator.go
  ```
  注意 builder 用 golang:1.25-bookworm，含完整 go 工具链，可直接 go generate/go run。

无论哪种方式，`asn1/38413-fd0.asn` 与两个 `*_generated.go` 都已在 git 中、会被 `COPY . .` 带入，故可行。

---

## 六、验证
1. 本地先 `go generate` + `go build ./...`（或直接 `go vet ./...`）确保**签名匹配、无编译错误**。
2. build 镜像 → apply（namespace free5gc, deployment free5gc-amf）。
3. 按 `AMF_MUTEX_PROFILING_GUIDE.md` 重抓 mutex profile：
   - 预期 `recvtime.*` 从榜单**消失**，`runtime.unlock` 占比从 ~97% 大幅下降，锁总等待（RQ1000）从 225s 量级骤降。
   - AMF_log 仍有 UL/DL 记录：UL 时间戳非零、DL 有 nasType/ueID。
4. 重跑 `AMF_detail_analyz.py`：观察 AMF-local（P1/P2/P5/P6/P7）是否显著下降。
   若大降，则证实此前「AMF-local 随 RQ 上升」很大部分是 recvtime 观察者效应，需用干净数据重做瓶颈分析。

## 七、影响面与风险
- **不影响注册业务逻辑**：recvtime/WorkerTrace 只喂异步日志，全部 nil-safe，失败仅日志缺字段。
- **通用安全**：recvTime 走 goroutine 栈参数（不共享）；DL 走 WorkerTrace（NGAP 调度器保证每 UE 单 worker 独占）。
  → 对未来「1 gNB 多 UE」「非 dGNB 模式」同样安全。
- **唯一操作性风险**：忘记重新运行生成器 → 编译失败（见第五节，已给强制步骤）。
- 生成器仅对 3 个上行 msgName 增参，其余 87 个 handler 与上游 free5gc 结构保持一致，降低后续合并冲突。

## 八、改动文件汇总
- 改生成器：`internal/ngap/ngap_generator.go`（+`recvTimeAppend`，仿 `messageAppend`）→ 重生成
  `internal/ngap/dispatcher_generated.go`、`internal/ngap/handler_generated.go`。
- 手写：`internal/ngap/service/service.go`、`internal/ngap/dispatcher.go`、`internal/ngap/handler.go`、
  `internal/nas/handler.go`、`internal/gmm/message/send.go`、`internal/ngap/message/send.go`、
  `internal/msgtrace/msgtrace.go`。
- 删除：`internal/recvtime/`（整包）。
