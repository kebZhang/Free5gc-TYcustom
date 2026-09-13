# free5GC NF 间 HTTP 传输过程 —— 源码阅读 Tutorial

> **主线样例：UDM ↔ UDR**（不是 AMF ↔ UDM）
>
> 目标：从"业务层决定要发一个 SBI 请求"开始，到"业务层拿到反序列化后的响应对象"结束，
> 把 **Free5gc-TYcustom** 这一份代码里完整的一条 HTTP/2 请求-响应链路读通，
> 并且读完之后能自己回答：**在 request rate 很大的时候，哪一段会先饱和，为什么。**
>
> 这份文档不是链路的答案，而是**读代码的作业本 + 检查清单**。

---

## 1. 阅读策略：两阶段，先原始路径，后观测层

这个仓库里有两种代码，混在同一批文件里：

- **原始路径**：请求真正走过的代码。upstream free5GC + upstream `golang.org/x/net/http2`，
  加上本 fork 里**真正改变了行为**的少数改动（连接数、超时值）。
- **观测层**：你自己后加的十个时间点（T1/M/M2/T2/G/T3/T4/W/T5/T6）、
  `HTTP_log.txt` / `DB_log` 的写入、以及为了打点而做的状态传递。
  **它们不改变请求的走向，只记录请求走过了哪里。**

**第一阶段只读原始路径，观测层全部跳过。** 理由不只是"少看点东西"：

1. 十个点的意义**取决于它们夹住了什么机制**。不先搞清机制，看到"T4 到 W 之间"
   只是两个名字之间的空白，没法判断它代表什么。
2. 观测层的注释写得非常详细（很多是长篇实验史），容易让人把"我们测了什么"
   误当成"代码做了什么"。第一遍读的目标是后者。
3. 第一阶段结束时你会从代码本身推出一批瓶颈假设。**第二阶段再看现有打点能不能验证它们** ——
   验证不了的那些，就是你缺的打点。这个顺序能直接产出下一份 plan 的内容；
   反过来先看打点，就只会去解释已经有的数据。

| 阶段 | Stage | 内容 |
|---|---|---|
| **第一阶段：原始路径** | 0 – 8 | 请求怎么走、什么数据结构、哪里串行、哪些资源被调度、瓶颈假设（只写机制） |
| **第二阶段：观测层** | 9 – 12 | 十个点分别在哪、每个区间的语义边界、四个免费指标、回填瓶颈表的验证方法 |
| 之后 | 13 | AMF |

### 1.1 第一阶段的跳读清单（精确到行）

**规则一：在 `xnet/http2/` 里，凡是带 `TYcustom` 注释的地方，第一遍全部跳过。**
这条 grep 就是完整清单：

```bash
grep -rn "TYcustom" xnet/http2/*.go
```

它会命中 `transport.go`（M / M2 / `instr` / `identity` / attempts）、
`server.go`（G / `trace` / `ResponseHeadersFlushed` / marker）、
`write.go` + `http2.go`（W 的 marker 与真实 socket write 取时）。
这些行**全部**是观测。跳过它们之后剩下的就是 upstream 的 HTTP/2 实现。

**规则二：这些文件/包第一阶段整个不看。**

| 位置 | 是什么 |
|---|---|
| `xnet/http2/instrument_client.go` | M / M2 / streamID / attempts 的载体，纯观测 |
| `xnet/http2/instrument_server.go` | G / ServerRequestTrace，纯观测 |
| `NFs/<nf>/internal/accesslog/accesslog.go` | 日志队列、writer goroutine、JSON 拼装，纯观测 |
| `NFs/udr/internal/dbtrace/` | mongoapi 的透明包装，纯观测（第一阶段当它不存在，直接看 `mongoapi`） |
| `NFs/amf/internal/msgtrace/` | AMF 专有，纯观测 |
| `ACCESSLOG.md`、`HTTP_*_PLAN_*.md`、`AMF_*_LOG_*.md`、`LOCK_SCHED_PROFILING_GUIDE_0826.md` | 全是打点设计文档 |

**规则三：`httptransport.go` 要读，但只读一半。**

这个文件同时装着"功能"和"观测"。第一阶段的精确读法
（行号以 `NFs/udr/internal/accesslog/httptransport.go` 为准，七个 NF 该文件逐字节相同）：

| 行 | 读 / 跳 | 内容 |
|---|---|---|
| 44-49 | **读** | 三个超时常量（`readIdleTimeoutPeriod` / `pingTimeoutPeriod` / `timeoutPeriod`）—— **真的改变了行为** |
| 118 | **读** | `connsPerPeer = 2` —— **真的改变了连接数** |
| 124-162 | **读** | `loggingRoundTripper` 结构：`tls`/`clear` 两个 Transport 数组 + `tlsNext`/`clearNext` 两个游标 |
| 170-181 | **读** | `nextSlot`：每 peer 一个 round-robin 游标 |
| 182-224 | **读** | `newLoggingRoundTripper`：构造 `connsPerPeer` 个独立 `http2.Transport` |
| 225-252 | **读** | `RoundTrip` 开头：按 scheme 选池、`nextSlot` 选槽位、`base := pool[connSlot]` |
| 254-259 | 跳 | `dst`/`method`/`uri` 提取 —— 只为日志字段 |
| **261** | **读** | `ueID := sniffUEID(req)` —— **观测目的、功能副作用**（见下） |
| 262-337 | 跳 | `wroteTime`/`gotFirstByte` 变量 + `httptrace.ClientTrace` 三个回调（T2 / T5 / connID） |
| 338-358 | 跳 | 挂两个 trace + `req.WithContext` |
| 360 | 跳 | `reqTime := time.Now()`（T1） |
| **361** | **读** | `resp, err := base.RoundTrip(req)` —— **这一行才是主线**，通往 Stage 5 |
| 362-388 | 跳 | `respTime`（T6）、读回 M/M2/streamID、`LogHTTP` |
| 389 | **读** | `return resp, err` |
| **398-435** | **读** | `sniffUEID` + `restoreBody`（见下） |
| 436-475 | 跳 | `bodyUEIDField` / `extractStringField` —— 纯提字段 |
| 476-569 | 跳 | `InboundLogger`（T3/T4 中间件）+ `inboundURI` + `sniffInboundUEID` |
| **570-582** | **读** | `Client()` —— 全进程唯一的 `*http.Client` 单例 |
| 583-638 | 跳 | `dstNFFromURL` / `nfFromServicePrefix` —— 只为日志字段 |

> **为什么 `sniffUEID` 必须在第一阶段读**：它的**目的**是观测（给日志填 `ue_id`），
> 但它的**效果**是功能性的 —— 它把 request body 整个读出来再重建一遍
> （`restoreBody`），也就是每个命中的请求多一次读取、一次分配、一次拷贝，
> 并且换掉了 `req.Body`。任何"改变了请求本身"的代码都属于原始路径，必须读。
> 顺带确认一件事：`bodyUEIDField`(436) 决定了哪些端点才 sniff ——
> **UDM→UDR 的 UE id 在 URI 里，所以这条主线大概率完全不 sniff**，去确认。

**规则四：`InboundLogger` 在第一阶段只需要知道一件事** ——
它是中间件链上的第三项，是个 `c.Next()` 前后各取一次时间的**透明中间件**，
不改变请求。upstream 的中间件链只有前两项（gin logger+recovery、metrics）。
第一阶段读中间件时把它当"一层空转"处理，Stage 9 再回来看它。

### 1.2 Stage 总览

| Stage | 阶段 | 名字 | 目标 |
|---|---|---|---|
| **0** | 一 | 45 行读完整个故事 | 先把 UDM 和 UDR 各一个 processor 函数读到能背。它们加起来不到 60 行，却包含了整条链路的全部骨架，后面每个 Stage 都是在给它们的某一行加放大镜。 |
| **1** | 一 | UDR：一个 HTTP 请求的完整生命周期 | 以 UDR 为例，把"**收到一个 HTTP request → 处理 → 发出 response**"的全过程走通。UDR 在这条路上完全没有出站 SBI，是最干净的样本。起点是"`*http.Request` 已构造好、handler 已经在跑"。 |
| **2** | 一 | UDM：收到请求 → 处理 → 组装出站请求 | 先把 UDM 的入站过程**再过一遍**（确认它和 UDR 同构），然后往下走 UDM 独有的部分：每个请求怎么被处理、出站请求怎么被组装、目标 URI 从哪来。 |
| **3** | 一 | openapi：请求怎么变成 JSON 和 `*http.Request` | 弄清楚参数结构体在哪一行、用什么方式变成 JSON 字节流和一个 `*http.Request`；响应又在哪一行被反序列化回结构体。**这是"从生成 HTTP req 的 json 开始"的真正起点。** |
| **4** | 一 | 连接槽位与超时：本 fork 唯一的功能性改动 | 这一层做两件**功能性**的事：把 upstream 的 1 个 `http2.Transport` 换成 N 个（每 peer round-robin），以及改了三个超时值。剥掉打点之后它其实很小。 |
| **5** | 一 | HTTP/2 客户端：连接、锁、流、帧 | 协议栈发送侧。取/建 TCP 连接、竞争连接写锁、分配 stream、HPACK 编码、写 socket。**串行点最密集的一层。** |
| **6** | 一 | HTTP/2 服务端：帧 → `*http.Request` → 启动 handler | Stage 1 是从 handler 往上读的；这里补上它下面那层：**serve goroutine** 如何读帧、解 HPACK、构造 `*http.Request`，最后 `go runHandler` 交给 handler goroutine。**这一层不是 handler goroutine，是它之前那个每连接唯一的串行 goroutine。** |
| **7** | 一 | 响应回程的两个反直觉机制 | handler `return` 之后 response 还在用户态；`RoundTrip` 返回时 body 还没读。这两件事都是 **upstream 行为**，和打点无关，但它们决定了后面所有区间的语义。 |
| **8** | 一 | 三个投影：并发 / 资源 / 瓶颈假设 | 把链路投影成三张表：哪里串行/并行、哪些资源被调度、高 RQ 下哪里先饱和。**瓶颈表这一轮只写"机制"，不写"怎么验证"** —— 验证留给 Stage 12。 |
| **9** | 二 | 十个点分别加在哪、为什么加在那 | 现在回头看观测层：每个点的取时位置、为什么必须在那一行、以及它在热路径上的成本。 |
| **10** | 二 | 每个区间的语义边界 | 逐段写清"它代表什么"和"它**不**代表什么"。Stage 7 的两个机制在这里变成两条硬约束。 |
| **11** | 二 | 嵌套区间：免费拿到的四个指标 | UDM 的入站区间**包住**它的出站区间。利用这个关系，不加新打点就能把"UDM 自己慢"和"UDM 在等 UDR"代数地分开。 |
| **12** | 二 | 回填 Stage 8 的瓶颈表 | 给每个假设补上"用哪个字段、算什么、看到什么算证实/证伪"。**补不上的那些，就是你缺的打点** —— 这就是下一份 plan。 |
| **13** | — | 之后再看 AMF | AMF = 这个骨架 + 五样额外的东西（SCTP/NGAP 入口、worker 调度器、GMM 状态机、msgtrace、进程级发现缓存）。 |

**Stage 1 和 Stage 6 是同一件事的两半**，故意分开读：
先从 handler 往上（好懂、有业务含义），再回头补 handler 以下的协议栈（难懂、但瓶颈所在）。

### 1.3 你的四个问题落在哪里

| 你的问题 | 落点 | 产出物 |
|---|---|---|
| ① 从哪开始、什么数据结构、函数怎么传递 | Stage 0 → 7 | `调用链主图` |
| ② 哪里串行 / 并行 / 顺序不确定 | Stage 8.1 | `串行点清单表` |
| ③ 资源调度、上下文获取、不同 NF 差异 | Stage 8.2 | `资源与上下文表` |
| ④ 高 RQ 下的潜在 bottleneck | Stage 8.3（机制）→ Stage 12（验证） | `瓶颈假设表` + `缺失打点清单` |

另外补 5 个你没提但**必须有**的维度，见 [第 15 节](#15-你没提但必须补的五个维度)。

---

## 2. 为什么从 UDM ↔ UDR 开始（而不是 AMF）

AMF 是"HTTP 客户端 + SCTP 服务端 + NAS 状态机 + UE 上下文管理器"四合一，
它的 SBI 调用被埋在 NGAP worker 调度和 GMM 状态机下面。
想学 **HTTP req-resp 本身**，那些都是噪声。

UDM 和 UDR 干净得多，而且**恰好构成一对递进的教学样本**：

| | UDR | UDM |
|---|---|---|
| 入站 HTTP | ✅ | ✅ |
| 出站 SBI | ❌ **完全没有**（这条路径上） | ✅ 每个入站请求触发一个出站请求 |
| 数据来源 | MongoDB（叶子） | UDR |
| 状态机 / 长连接维护 | 无 | 无（只有一个 `UdmUe` 缓存） |
| 角色 | **纯服务端 = 最小完整服务端路径** | **服务端 + 客户端嵌套 = 中间 NF 的通用形态** |

关键洞察：**UDM 的一个 handler goroutine 里同时装着"服务端处理"和"客户端调用"，两者嵌套。**
这个结构是 AUSF→UDM、UDM→UDR、PCF→UDR 全都一样的形态，
学会它等于学会了 free5GC 里**除 AMF/SMF 以外所有 NF** 的 HTTP 行为。

---

## 3. 前置准备

### 3.1 这条链路涉及的代码分布（全部在本仓库内）

| 层（自上而下） | 位置 | 第一阶段 |
|---|---|---|
| **业务处理层**（processor） | `NFs/<nf>/internal/sbi/processor/` | 全读 |
| **路由与 handler 层** | `NFs/<nf>/internal/sbi/router.go`、`api_*.go` | 全读 |
| **中间件层** | gin logger/recovery、metrics、`accesslog.InboundLogger()` | 前两层读，第三层当空转 |
| **消费者层**（consumer） | `NFs/<nf>/internal/sbi/consumer/` | 全读 |
| **SBI 客户端生成层** | `openapi/`（模块 `github.com/free5gc/openapi`） | 全读 |
| **连接槽位层** | `NFs/<nf>/internal/accesslog/httptransport.go` | **只读功能部分**（见 1.1 规则三） |
| **应用层 HTTP/2 协议栈** | `xnet/http2/` | 读，但跳过所有 `TYcustom` |
| **传输层 TCP / 内核 socket** | Go runtime / 内核 | 不在本仓库 |
| **数据库访问层**（仅 UDR） | `NFs/udr/internal/database/` → `util/mongoapi` → MongoDB | 读；`dbtrace` 当透明 |

两条 `replace` 是本仓库最重要的结构事实：

```
# NFs/udm/go.mod:22, NFs/udr/go.mod:24
replace golang.org/x/net => ../../xnet
```

它意味着 HTTP/2 协议栈是本地可读的。**第一阶段把它当成"就是标准的 x/net/http2"来读**
（跳过 `TYcustom` 之后它确实就是），第二阶段再看 fork 在里面加了什么。

### 3.2 openapi 源码：已就位，版本已核对 ✅

`./openapi/` 是**正确的版本**：

- `git describe --tags` → **`v1.2.3`**，HEAD = `1548c66`
- 所有 NF 的 `go.mod` 都 require `github.com/free5gc/openapi v1.2.3` —— 完全一致
- 目录结构是新版风格（`udm/`、`udr/`、`models/`、`oauth/`），与 NF 代码里
  `openapi/udr/DataRepository` 这种导入路径吻合

> ⚠️ **一个必须知道的限制**：`NFs/*/go.mod` 里**没有**
> `replace github.com/free5gc/openapi => ../openapi`（只 replace 了 `golang.org/x/net`）。
> 所以这份本地代码是**只读参考** —— 改它不会影响任何 NF 的编译产物。
> 如果哪天要在 openapi 层加打点，**必须先给每个 NF 的 go.mod 加上 replace**，
> 否则会出现"改了代码但镜像里没有"的静默失败。
> （这个坑在 Aether 那边踩过：编辑了 `shared-libs/openapi` 但没加 replace，构建脚本静默忽略。）

另有一份旧版在 `../Free5gc_TYcustom_v3.4.3/NFs/openapi/`（老代码生成风格）。**不要用它。**

### 3.3 建立两个笔记文件

- `notes/http_path_facts.md` —— 只记**代码里核对过的事实**，每条必须带 `文件:行号`
- `notes/http_path_hypotheses.md` —— 所有推测、直觉、"我觉得"

两者严格分开。Stage 8.3 的瓶颈表只允许引用 facts；Stage 12 负责把能验证的搬过去。

---

# 第一阶段：原始路径

## 4. Stage 0 —— 45 行读完整个故事

**这是全篇最重要的一节。先把这两个函数读到能背。**

### 4.1 UDM 侧：`GetAmDataProcedure`

`NFs/udm/internal/sbi/processor/subscriber_data_management.go:20-65`

```go
func (p *Processor) GetAmDataProcedure(c *gin.Context, supi string, plmnID string, supportedFeatures string) {
    // ① 取出站调用需要的 context（token / 目标 NF 类型）
    ctx, pd, err := p.Context().GetTokenCtx(models.ServiceName_NUDR_DR, models.NrfNfManagementNfType_UDR)

    // ② 组装出站请求的参数结构体（还不是 JSON，还不是 http.Request）
    var queryAmDataRequest Nudr_DataRepository.QueryAmDataRequest
    queryAmDataRequest.SupportedFeatures = &supportedFeatures
    queryAmDataRequest.UeId            = &supi
    queryAmDataRequest.ServingPlmnId   = &plmnID

    // ③ 拿到（或创建）指向 UDR 的 APIClient —— 可能触发一次 NRF discovery！
    clientAPI, err := p.Consumer().CreateUDMClientToUDR(supi)

    // ④ 【同步阻塞】整条 openapi→协议栈→网络→UDR→网络→协议栈→openapi 都在这一行里
    accessAndMobilitySubscriptionDataResp, err := clientAPI.
        AccessAndMobilitySubscriptionDataDocumentApi.QueryAmData(ctx, &queryAmDataRequest)

    // ⑤ 错误映射：GenericOpenAPIError → 透传 RawBody / ProblemDetails
    if err != nil { ... }

    // ⑥ 把结果写进 UDM 自己的 UE 缓存
    udmUe, ok := p.Context().UdmUeFindBySupi(supi)
    if !ok { udmUe = p.Context().NewUdmUe(supi) }
    udmUe.SetAMSubsriptionData(&accessAndMobilitySubscriptionDataResp.AccessAndMobilitySubscriptionData)

    // ⑦ 写入站响应
    c.JSON(http.StatusOK, accessAndMobilitySubscriptionDataResp.AccessAndMobilitySubscriptionData)
}
```

**第 ④ 行是整个 tutorial 的主角。** 它一行代码里藏着：
JSON 序列化 → `*http.Request` 构造 → 连接槽位选择 → 取/建 TCP 连接 →
`reqHeaderMu` 竞争 → HPACK 编码 → socket write → 网络 → UDR 的 serve goroutine →
handler goroutine → 中间件 → 路由 → MongoDB → 回程 → 读 body → 反序列化。

**并且它是同步阻塞的** —— UDM 的这个 handler goroutine 在这一行上原地等着。
记住这一点，Stage 8.3 的 2 号假设全靠它。

### 4.2 UDR 侧：`QueryAmDataProcedure`

`NFs/udr/internal/sbi/processor/access_and_mobility_subscription_data_document.go:22-33`

```go
func (p *Processor) QueryAmDataProcedure(c *gin.Context, collName string, ueId string, servingPlmnId string) {
    filter := bson.M{"ueId": ueId, "servingPlmnId": servingPlmnId}   // ① path 参数 → mongo filter
    data, pd := p.GetDataFromDB(collName, filter)                     // ② 【同步阻塞】唯一的外部依赖
    if pd != nil { c.JSON(int(pd.Status), pd); return }
    c.JSON(http.StatusOK, data)                                       // ③ map[string]interface{} → JSON
}
```

**11 行。这就是 free5GC 里一个完整的 SBI 服务端业务逻辑的全部。**
`collName` 是上一层硬编码的字符串常量（`"subscriptionData.provisionedData.amData"`，
见 `NFs/udr/internal/sbi/api_datarepository.go:966`），
`ueId` / `servingPlmnId` 从 URL path 里 `c.Params.ByName(...)` 取。

注意 UDR 返回 `map[string]interface{}` —— **不是** typed model。
这对序列化开销意味着什么？（Stage 1 会问）

### 4.3 把两个函数拼起来：一次 UDM→UDR 事务的完整地图

```text
┌───────────────────────── UDM 进程 ─────────────────────────┐
│  [入站]  HTTP/2 协议栈 serve goroutine ──► handler go      │  ← Stage 6
│            │                                               │
│            ├─ 中间件：gin logger/recovery → metrics         │  ← Stage 1/2
│            │           (→ accesslog：第一阶段视为空转)      │
│            ├─ router.go → api_subscriberdatamanagement.go:30│
│            │              HandleGetAmData                  │
│            └─► processor: GetAmDataProcedure   ◄── Stage 0.1│
│                  │                                         │
│                  │  ┌──── [出站] 同一个 goroutine ───────┐  │
│                  ├─►│ consumer/udr_service.go:30         │  │  ← Stage 2
│                  │  │   CreateUDMClientToUDR (缓存/NRF)  │  │
│                  │  │ openapi: struct → JSON → *http.Req │  │  ← Stage 3
│                  │  │ httptransport.go: 选连接槽位        │  │  ← Stage 4
│                  │  │ xnet/http2/transport.go            │  │  ← Stage 5
│                  │  └────────────┬───────────────────────┘  │
└──────────────────────────────────┼─────────────────────────┘
                                   │ 传输层 TCP / 网络
┌──────────────────────────────────┼─────────────────────────┐
│  [入站]  serve goroutine ──► handler goroutine             │  ← Stage 6
│            ├─ 中间件                                        │
│            ├─ api_datarepository.go:82-85 路由             │  ← Stage 1
│            │   :963 HandleQueryAmData（硬编码 collName）   │
│            └─► processor: QueryAmDataProcedure ◄── Stage 0.2│
│                  └─► database/mongodb/mongo_db_inplement.go │
│                        └─► mongoapi → MongoDB               │
│  [出站响应] c.JSON → frame writer → socket   UDR 进程       │  ← Stage 7
└──────────────────────────────────┼─────────────────────────┘
                                   │ 传输层 TCP / 网络
┌──────────────────────────────────┼─────────────────────────┐
│  UDM: RoundTrip 返回(仅 header) → ReadAll(body)            │  ← Stage 7
│       → Deserialize → SetAMSubsriptionData → c.JSON        │
└────────────────────────────────────────────────────────────┘
```

**三个进程边界**（每个都可能排队）：
1. UDM 用户态 → 内核
2. 网络（内核 → 内核）
3. UDR serve goroutine → handler goroutine（**纯用户态排队，最容易被忽略**）

---

## 5. Stage 1 —— UDR：一个 HTTP 请求的完整生命周期

**目标**：把"**收到一个 HTTP request → 处理 → 发出 response**"的全过程走通，
且不被任何出站 SBI 调用干扰。

**边界**：起点是"HTTP/2 协议栈已经把帧解成一个 `*http.Request`，并已启动 handler goroutine"；
终点是"handler `return`，`c.JSON` 已调用完"。
更下面的协议栈留给 Stage 6 和 Stage 7。

### 5.1 读什么 —— 按请求实际经过的顺序

| 顺序 | 文件 | 看什么 |
|---|---|---|
| ① 服务器怎么起来的 | `NFs/udr/internal/sbi/server.go` | `NewServer`、`newRouter`、`newHttp2ServerWithIdleTimeout`、`ListenAndServe`。**先看清中间件注册顺序**（`router.Use(...)`） |
| ② 中间件链 | gin logger+recovery → `metrics.InboundMetrics()` → （accesslog：第一阶段视为空转） | 重点理解 **`c.Next()` 的语义** —— 中间件是"洋葱"，`c.Next()` 之后的代码在整条链返回后才跑 |
| ③ 路由表 | `NFs/udr/internal/sbi/router.go` + `api_datarepository.go:82-85` | `GET /subscription-data/:ueId/:servingPlmnId/provisioned-data/am-data` → `HandleQueryAmData` |
| ④ handler：**无 body 的 GET** | `api_datarepository.go:963-975` | `c.Params.ByName` 取参数、`collName` 硬编码常量、空值检查 |
| ④' handler：**有 body 的 PUT** | `api_datarepository.go:800-840` | `c.GetRawData()` + `openapi.Deserialize` ← **服务端反序列化点**，与 GET 对照读 |
| ⑤ 业务处理 | `processor/access_and_mobility_subscription_data_document.go:22` | GET 的 processor（11 行） |
| ⑤' 业务处理 | `processor/amf3_gpp_access_registration_document.go:43-54` | PUT 的 processor：`util.ToBsonM` + `RestfulAPIPutOne`（第一阶段把 `dbtrace.` 读成 `mongoapi.`） |
| ⑥ 数据库抽象 | `NFs/udr/internal/database/database.go` | `DbConnector` interface —— 一层接口，为什么存在？ |
| ⑦ 数据库实现 | `NFs/udr/internal/database/mongodb/mongo_db_inplement.go:51-62` | `GetDataFromDB` → `RestfulAPIGetOne` → **直接看 `util/mongoapi` 的实现** |
| ⑧ 响应出去 | `c.JSON(...)` → gin 的 `Render` → `http.ResponseWriter` | 这一步之后交给协议栈（Stage 7） |

### 5.2 必须能回答

1. **`c.Next()` 到底做了什么？** 它是整个中间件链的枢纽。
   `c.Next()` 之后的代码，在"路由匹配 + handler + processor + DB"全部返回之后才执行。
   所以任何"在中间件里量整个请求处理时间"的做法，量到的都是 `c.Next()` 那一段。
2. GET 和 PUT 两条路径的差别有多大？
   - GET：参数全在 path，**零反序列化**
   - PUT：`c.GetRawData()` 把整个 body 读进内存 → `openapi.Deserialize` → `util.ToBsonM`
     **三次完整的数据结构转换**，每次都分配内存。在你的 RQ 下这是多少次/秒？
3. `collName` 是**硬编码常量**，`filter` 是 `bson.M{...}` 现场构造 —— 每请求一次 map 分配。
   把 UDR 所有 handler 的 `collName` 列出来（附录 A 有命令），对照 MongoDB 里的集合。
4. UDR 返回 `map[string]interface{}`（不是 typed struct）。
   `c.JSON` 序列化一个 map 和序列化一个 struct，开销差别在哪？
   （提示：反射 + map 遍历 + 无法复用字段元信息）
5. `DbConnector` 是 interface，`NewDbConnector` 按配置选实现 ——
   一次无法内联的接口方法调用，在热路径上。记录事实，不下结论。
6. **`mongoapi.RestfulAPIGetOne` 内部做了什么？** 它是同步阻塞的吗？
   它有没有自己的超时？mongo driver 的连接池在哪初始化、多大？
   （`NFs/udr/pkg/service/init.go` + `NFs/udr/pkg/factory/` + `config/udrcfg.yaml`）
7. **`c.JSON` 返回之后，response 到内核了吗？**（答案是"没有"，Stage 7 解释）

### 5.3 产出

- **UDR 请求生命周期逐层清单**：从"handler goroutine 启动"到"`c.JSON` 返回"，
  每层一行，标注 `文件:行号` + `这一层新增/转换了什么数据结构` + `有没有分配/拷贝/等待`。
- **GET vs PUT 对比表**：把数据结构转换次数和内存分配次数数清楚。

### 5.4 卡住时让 AI 做什么

> "把 `NFs/udr/internal/sbi/` 下 `HandleQueryAmData` 到 `mongoapi.RestfulAPIGetOne`
> 的完整调用链列出来，每一跳标 文件:行号，并标注该跳是否有内存分配、
> 是否有无法内联的接口方法调用、是否有锁、是否有阻塞等待。跳过所有只用于日志的代码。"

---

## 6. Stage 2 —— UDM：收到请求 → 处理 → 组装出站请求

**目标**：分三步走。

### 6.1 第一步：UDM 的入站过程（快速复核，不要跳过）

**目的是验证"入站路径对所有 NF 是同构的"这个假设，而不是重新学一遍。**

| 文件 | 和 UDR 比对什么 |
|---|---|
| `NFs/udm/internal/sbi/server.go` | `newRouter` 的中间件顺序和 UDR **一样吗**？`idleTimeoutPeriod` 一样吗？ |
| `NFs/udm/internal/sbi/router.go`、`api_subscriberdatamanagement.go:18` | 路由注册方式一样吗？ |
| `NFs/udm/internal/sbi/api_subscriberdatamanagement.go:30` `HandleGetAmData` | 和 UDR 的 handler 结构一样吗？（取 path 参数 → 调 processor） |

**做完这一步你应该能写下一句话**：
"free5GC 所有 NF 的入站路径结构完全相同，差异只在 ____ 和 ____。"
（附录 A 的 `diff` / `grep` 命令就是为此准备的。）

### 6.2 第二步：UDM 拿到请求之后怎么处理

读 `api_subscriberdatamanagement.go:30-68` 和 `processor/subscriber_data_management.go:20`。

1. `HandleGetAmData` 从 `*gin.Context` 里取了哪些东西？
   （path 参数 `supi`、query 参数 `plmn-id` / `supported-features`）
   `getPlmnIDStruct`（:70）在干什么？为什么 PLMN ID 需要单独解析？
   **提示：它是一个 JSON 对象被塞进了 query string** —— 3GPP SBI 的常见做法，
   Stage 3 会在客户端侧看到它的另一半。
2. `GetAmDataProcedure` 里，**在发出出站请求之前**做了哪些事？
   （`GetTokenCtx` → 建 Request struct → `CreateUDMClientToUDR`）
   这三件事哪一件可能**阻塞**？（答案：第三件，如果触发 NRF discovery）
3. **在拿到出站响应之后**做了哪些事？
   （错误映射 → `UdmUeFindBySupi` / `NewUdmUe` → `SetAMSubsriptionData` → `c.JSON`）
4. `p.Context().UdmUeFindBySupi(supi)` / `NewUdmUe(supi)` —— **去 `NFs/udm/internal/context/`
   查它们有哪些锁**。这是每请求都要走的进程级共享结构。
5. 对比 UDR：UDR 的 processor 11 行、一个外部依赖（Mongo）；
   UDM 45 行、外部依赖是**另一个 NF**。**结构上多出来的是什么？**
   （答案：一整套"我也是个客户端"的逻辑）

### 6.3 第三步：出站请求怎么被组装

| 文件 | 看什么 |
|---|---|
| `NFs/udm/internal/sbi/consumer/consumer.go` | `Consumer` 结构、单例 |
| `consumer/udr_service.go:30` `CreateUDMClientToUDR` | **重点**：`map[string]*APIClient` + `sync.RWMutex` 缓存 |
| `consumer/udr_service.go:56` `getUdrURI` | **重点**：URI 从哪来 —— `UdmUe.UdrUri`，空则 `SendNFInstancesUDR`（**一次 NRF 往返**） |
| `consumer/nrf_service.go` | `SendNFInstancesUDR` |
| `processor/ue_context_management.go:88,118-122` | **有 body 的出站调用**：`CreateAmfContext3gppRequest` 带 `models.Amf3GppAccessRegistration` |

1. `QueryAmDataRequest` 的字段都是**指针**（`UeId *string`）。生成代码为什么这么写？
   （对照 `openapi/udr/DataRepository/api_access_and_mobility_subscription_data_document.go:55-70`
   那一串 `SetXxx` 方法 —— 指针 + setter 是"可选参数"的表达方式）
   对照 UDR 侧路由 `/:ueId/:servingPlmnId/...`：哪个字段变成 path，哪个变成 query？
2. `CreateUDMClientToUDR` 的锁：`RUnlock()` 之后到 `Lock()` 之前有窗口，
   两个 goroutine 可能都建一个 client 然后互相覆盖。功能上无害，但：
   **被覆盖掉的那个 APIClient 里的 `*http.Client` 是共享的还是独立的？**
   （去看 `httptransport.go:570` 的 `Client()` 是不是单例 —— 这决定泄漏的是对象还是连接）
3. `getUdrURI` 的**第一次调用**会触发 `SendNFInstancesUDR`，即**一次完整的 UDM→NRF HTTP 往返**，
   之后缓存在 `UdmUe.UdrUri` 里。
   - 缓存粒度是**每 UE** 的（不是每进程）。1000 个 UE 就是 1000 次 NRF 查询？
   - 对比 AMF 侧有 `NFs/amf/internal/disccache/`（**每进程**缓存）。
     **UDM 有没有类似的东西？** `ls NFs/udm/internal/` 确认。
     如果没有，这是真实的架构差异，记进 facts，成为 Stage 8.3 的 9 号假设。
4. `p.Context().GetTokenCtx(...)` 做了什么？本部署是 direct communication（无 SCP、无 OAuth）时
   它是不是空转？去 `config/udmcfg.yaml` 确认开关。
   （Stage 3 会在 `openapi/client.go:562` 看到这个 ctx 值被消费的地方）
5. `cfg.SetHTTPClient(...)` + `cfg.SetMetrics(...)`：**逐个 service 核对哪些注入了、哪些漏了**。
   ```bash
   grep -rc "NewConfiguration()" NFs/*/internal/sbi/consumer/*.go
   grep -rc "SetHTTPClient"      NFs/*/internal/sbi/consumer/*.go
   ```
   **两个数字不等就是功能性差异**（不只是打点问题）—— Stage 3.2 会告诉你漏掉的请求走去哪。
6. 出站有 body 的情况：`registerRequest models.Amf3GppAccessRegistration`
   是**按值传递**进 `RegistrationAmf3gppAccessProcedure` 的。这个 struct 有多大？
   （`grep -A 40 "type Amf3GppAccessRegistration struct" openapi/models/`）

### 6.4 产出

- 一句话结论：入站路径的 NF 间差异是什么（6.1 那两个空）
- 表：`UDM 每个出站调用` → `目标 NF` → `HTTP method + URI 模板` → `参数在 path/query/body 的分布`
  → `是否需要先做 NRF discovery`

---

## 7. Stage 3 —— openapi：请求怎么变成 JSON 和 `*http.Request`

**目标**：这是"从生成这个 HTTP req 的 json 格式开始"的**真正起点**。
源码已在 `./openapi/`，下面全部是**核对过的真实行号**。

### 7.1 出站：从结构体到 `*http.Request`

| 顺序 | 位置 | 做什么 |
|---|---|---|
| ① | `openapi/udr/DataRepository/api_access_and_mobility_subscription_data_document.go:84` `QueryAmData` | 生成代码入口，`ctx` + `*QueryAmDataRequest` 进来 |
| ② | 同文件 `:96-97` | **URL path 拼接**：`strings.Replace(localVarPath, "{ueId}", openapi.StringOfValue(*request.UeId), -1)`。**注意没有 URL escape** —— `imsi-208930000000001` 恰好不需要，但这是个值得记的事实 |
| ③ | 同文件 `:100-101` | `localVarQueryParams := url.Values{}` —— query 参数在这里累积 |
| ④ | 同文件 `:134` | `openapi.PrepareRequest(ctx, cfg, path, method, postBody, headerParams, queryParams, ...)` |
| ⑤ | `openapi/client.go:414` `PrepareRequest` | 真正构造 `*http.Request` 的地方 |
| ⑥ | `openapi/client.go:428` | `ctx = httptrace.WithClientTrace(ctx, otelhttptrace.NewClientTrace(ctx))` ← **upstream 就有的**，每个请求无条件挂一个 OpenTelemetry ClientTrace：一次分配 + 一堆闭包 |
| ⑦ | `openapi/client.go:446` | `body, err = setBody(postBody, contentType)` ← **有 body 时的 JSON 生成入口** |
| ⑧ | `openapi/client.go:721` `setBody` → `Serialize` | |
| ⑨ | `openapi/serialize.go:10` `Serialize` | **`json.Marshal(v)`** ← **JSON 就是在这一行诞生的** |
| ⑩ | `openapi/client.go:512` | `url.Parse(path)` |
| ⑪ | `openapi/client.go:530 / 532` | `http.NewRequest(method, url.String(), body)`（有 body）/ `..., nil`（无 body）。**用的是 `http.NewRequest` 而非 `NewRequestWithContext`** |
| ⑫ | `openapi/client.go:553` | `Header.Add("User-Agent", cfg.UserAgent())` |
| ⑬ | `openapi/client.go:557` | `localVarRequest = localVarRequest.WithContext(ctx)` ← ctx 在这里才装上（**一次 Request 结构体拷贝**） |
| ⑭ | `openapi/client.go:562-579` | OAuth2 token / BasicAuth / Bearer 从 ctx 取出写进 header ← **Stage 2 里 `GetTokenCtx` 的消费点** |
| ⑮ | `openapi/client.go:161` `CallAPI` | **选 client**（见下） |

### 7.2 `CallAPI` 的三个分支 —— 一个真实的功能性分叉

`openapi/client.go:161-190`：

```go
func CallAPI(cfg Configuration, request *http.Request) (*http.Response, error) {
    start := time.Now()
    metricHook := cfg.Metrics()

    if cfg.HTTPClient() != nil {                      // ← 我们的 NF 走这一支
        resp, err := cfg.HTTPClient().Do(request)     //   = httptransport.go 的 Client()
        ...
    }
    if request.URL.Scheme == "https" {
        resp, err := innerHTTP2Client.Do(request)     // ← upstream 默认：openapi 自带的 client
    } else if request.URL.Scheme == "http" {
        resp, err := innerHTTP2CleartextClient.Do(request)
    }
}
```

**这不只是打点覆盖率问题，是真实的功能分叉**：走 `innerHTTP2CleartextClient` 的请求
**只有一个 `http2.Transport`、一个连接池、upstream 的默认超时**，
完全不经过 Stage 4 的连接槽位 round-robin。
所以 Stage 2 第 5 问的两个数字不等，意味着**一部分流量的连接行为和另一部分不一样**。
必须逐个 service 核对。

### 7.3 入站：响应怎么变回结构体

回到生成代码 `api_access_and_mobility_subscription_data_document.go`：

| 行 | 做什么 | **重要** |
|---|---|---|
| `:139` | `openapi.CallAPI(...)` 返回 | 此时**只有 response header 到了**，body 还是一个流 |
| `:144` | `ioutil.ReadAll(localVarHTTPResponse.Body)` | **body 是在这里才被读完的** —— 见 Stage 7 |
| `:148` | `localVarHTTPResponse.Body.Close()` | |
| `:154-157` | 构造 `GenericOpenAPIError{RawBody, ErrorStatus}` | **每个请求都构造，无论成功失败** |
| `:160` | `openapi.Deserialize(&localVarReturnValue..., localVarBody, Content-Type)` | **反序列化点** |
| `openapi/client.go:652` `Deserialize` | `json.Unmarshal(b, v)` | |
| `:165-167` | 读 `Cache-Control` / `ETag` / `Last-Modified` header | |

### 7.4 必须能回答

1. **JSON 是什么时候变成 `[]byte` 的？** → `serialize.go:10` 的 `json.Marshal`。
   **有没有复用 buffer？** 看 `setBody`：`Serialize` 先分配一个 `[]byte`，
   然后 `bodyBuf.Write(b)` 又拷进一个新的 `bytes.Buffer`。
   **每个有 body 的请求两次分配 + 一次拷贝，没有 `sync.Pool`。**
2. `QueryAmData` 是 GET，`localVarPostBody` 为 nil ⇒ 走 `:532` 的 `http.NewRequest(..., nil)`。
   所以 **GET 路径上完全没有 `json.Marshal`**。那 GET 的开销主要在哪？
3. **`Content-Length` 有没有被设？** `http.NewRequest` 对 `*bytes.Buffer` 会自动设 `ContentLength`。
   这决定了 HTTP/2 发送 DATA 帧时要不要等 flow control（Stage 5 用到）。
4. `client.go:428` 那个 `otelhttptrace.NewClientTrace(ctx)`：
   没配 TracerProvider 时它是 noop 还是仍然分配？去看它的实现。
5. `client.go:557` 的 `WithContext` 会**拷贝整个 `http.Request` 结构体**。
   加上 Stage 4 里还有一次（挂 trace 用），合计几次？
6. 响应反序列化在 `RoundTrip` **返回之后**（见 7.3 和 Stage 7）。这一点在 Stage 11 算指标时至关重要。

### 7.5 产出

- 事实笔记里写死：
  **"JSON 生成点 = `openapi/serialize.go:10` (`json.Marshal`)；
  反序列化点 = `openapi/client.go:652` (`json.Unmarshal`)；buffer 复用 = 无"**
- 一张 **数据结构变换链**（以有 body 的 `CreateAmfContext3gpp` 为例）：
  ```
  models.Amf3GppAccessRegistration (Go struct，按值传进 processor = 拷贝1)
    → json.Marshal → []byte (分配1)
    → bytes.Buffer.Write → *bytes.Buffer (分配2 + 拷贝2)
    → *http.Request.Body → HTTP/2 DATA frames → 网络
  UDR 侧:
    → c.GetRawData() → []byte (分配3)
    → openapi.Deserialize → models.Amf3GppAccessRegistration (分配4)
    → util.ToBsonM → bson.M (分配5)
    → mongo driver → BSON (分配6) → MongoDB
  ```
  **把总转换次数和分配次数数清楚。** 这个数字在讨论 GC 压力时很有说服力。

---

## 8. Stage 4 —— 连接槽位与超时：本 fork 唯一的功能性改动

**目标**：剥掉打点之后，这一层只做两件事，都是**功能性**的：

1. 把 upstream 的**一个** `http2.Transport` 换成 **`connsPerPeer` 个独立的 Transport**，
   并让每个 peer 按自己的游标 round-robin 挑一个。
   一个 Transport = 一个独立连接池 ⇒ **N 个槽位 = 对每个 peer N 条 TCP 连接**。
2. 改了三个超时值：`readIdleTimeoutPeriod` / `pingTimeoutPeriod` / `timeoutPeriod`。

> **和 upstream 的差别有多大**：如果没有这一层，`openapi.CallAPI` 会走
> `innerHTTP2CleartextClient`（Stage 3.2），那是**一个** `http2.Transport`、
> 对每个 peer **一条**连接、upstream 默认超时。
> 所以这一层的功能贡献可以一句话概括：**1 条连接 → N 条连接，加三个超时值。**

### 8.1 读什么（严格按 1.1 规则三的行号取舍）

| 行 | 内容 | 要点 |
|---|---|---|
| 44-49 | 三个超时常量 | `readIdleTimeout=1s` + `pingTimeout=3s`：连接空闲 1s 后发 PING，3s 内无 PONG 就拆连接。`timeoutPeriod=10s` 是 `http.Client.Timeout`。**注释里记录了 pingTimeout=1s 时健康检查误杀繁忙连接的实例** |
| 118 | `connsPerPeer = 2` | 顶部长注释是实验史（1/2/4/8/16 的序列）。第一阶段只需拿到"现在是 2"和"它意味着什么" |
| 124-162 | `loggingRoundTripper` 结构 | `tls[N]` / `clear[N]` 两个数组 + `tlsNext` / `clearNext` 两个 `sync.Map` 游标。**为什么游标要 per-peer 而不是 per-process** —— 注释里的 `gcd(L, connsPerPeer) != 1` 那段是个真实的退化案例，值得读懂 |
| 170-181 | `nextSlot` | `sync.Map` + `atomic.Uint64.Add`，无锁热路径；`LoadOrStore` 而不是 `Store` 的理由 |
| 182-224 | `newLoggingRoundTripper` | 每个槽位一个**独立实例**（字段值完全相同，只有实例身份不同 —— 这正是"一槽一连接"的来源）。`StrictMaxConcurrentStreams` **故意不设**；`clear` 那支用 `AllowHTTP` + 自定义 `DialTLSContext` 实现 h2c |
| 225-252 | `RoundTrip` 开头 | `pool := &l.clear`（**取地址而不是拷贝数组**）、按 scheme 选池、`host = req.URL.Host`、`connSlot := nextSlot(...)`、`base := pool[connSlot]` |
| 261 + 398-435 | `sniffUEID` / `restoreBody` | **观测目的、功能副作用**：读掉 body 再重建。`bodyUEIDField`(436) 决定哪些端点会被 sniff |
| 361 | `base.RoundTrip(req)` | 通往 Stage 5 |
| 570-582 | `Client()` | 全进程唯一的 `*http.Client`，`Timeout: timeoutPeriod`，`Transport: newLoggingRoundTripper()` |

### 8.2 必须能回答

1. **"2 条连接"到底是什么粒度？** 把这三件事分清（24-118 那段长注释讲了）：
   - 2 个**槽位**（`http2.Transport` 实例）是**每进程**的
   - 但每个槽位有自己按 `host:port` 索引的连接池
   - 所以是"**每 NF 对 2 条连接**"，一个有 4 个 peer 的 NF 持有 8 条连接
2. 为什么 round-robin 游标必须 **per-peer**？
   一个 per-process 游标在什么情况下会导致某些连接**永远不被建立**？
   （注释给了条件：每 UE 发 L 个请求且 `gcd(L, connsPerPeer) != 1`）
3. `StrictMaxConcurrentStreams` 故意保持 false 意味着什么？
   当一个槽位的 in-flight stream 数达到对端上限时会发生什么？
   （答案：transport 自己再 dial 一条 ⇒ **第三条连接是溢出信号，不是配置失效**）
4. `readIdleTimeout` + `pingTimeout` 这个健康检查机制：
   它在什么条件下会**误杀一条健康但繁忙的连接**？
   （提示：对端在 `pingTimeout` 内没能把 PONG 转回来 —— 高负载下这很可能）
   这会不会形成正反馈？（Stage 15.1 会再问）
5. `Client()` 是单例 ⇒ 全进程所有 consumer、所有 peer 共享这 2N 个 Transport。
   这回答了 Stage 2 第 2 问：重复创建的 `APIClient` 泄漏的是**对象**，不是连接。
6. `sniffUEID` 只对 POST 且在已知端点列表里的请求生效。
   **UDM→UDR 主线上有没有命中？** 去 `bodyUEIDField`(436) 对照 URI 确认。
   没命中就意味着这条主线上它是一次 `req.Method != POST` 的早退，成本可忽略。

### 8.3 产出

- 一句话：**这一层相对 upstream 的功能差异是什么**（连接数 + 三个超时）
- 一个数字：本次运行下，UDM 对 UDR 持有几条连接？对 NRF 几条？总共几条？

---

## 9. Stage 5 —— HTTP/2 客户端：连接、锁、流、帧

**目标**：**串行点最密集的一层**，也是问题②③④的核心战场。

> **第一阶段读法**：`grep -n TYcustom xnet/http2/transport.go` 命中的 8 处全部跳过
> （316 / 406 / 599 / 814 / 1298 / 1443 / 1452 / 1490）。
> 跳过之后，`transport.go` 就是标准的 `golang.org/x/net/http2`。
> `instrument_client.go` 整个不看。

### 9.1 读什么

沿这条路径读 `xnet/http2/transport.go` + `client_conn_pool.go`：

```text
(*Transport).RoundTrip
  └─ RoundTripOpt
       ├─ connPool().GetClientConn(req, addr)     ← client_conn_pool.go：真正取/建 TCP 连接
       └─ cc.roundTrip(req)                       ← transport.go
            ├─ 建 clientStream
            ├─ go cs.doRequest(...)               ← ★ 这里分叉出第二个 goroutine
            │    └─ cs.writeRequest(req)
            │         ├─ select { case cc.reqHeaderMu <- struct{}{} ... }  ← ★ 每连接一把写锁
            │         ├─ cc.mu.Lock() + awaitOpenSlotForStreamLocked       ← ★ MaxConcurrentStreams 等待
            │         ├─ cc.addStreamLocked(cs)                            ← 分配 stream id
            │         ├─ encodeAndWriteHeaders(req)                        ← HPACK + cc.wmu + 写 + flush
            │         └─ writeRequestBody(req)                             ← DATA 帧 + flow control 等待
            └─ select { <-cs.respHeaderRecv / <-ctx.Done() / <-cs.reqCancel }
```

另一侧：`(*ClientConn).readLoop` —— **每连接一个 goroutine**，
所有响应帧都从这一个 goroutine 上读出并分发。

### 9.2 必须能回答

1. `reqHeaderMu` 是 **per-ClientConn** 的，用 `chan struct{}` 实现。
   为什么用 channel 而不是 `sync.Mutex`？（提示：它要能被 ctx / reqCancel 打断）
2. `cc.mu`、`cc.wmu`、`reqHeaderMu` 三者有什么区别？各保护什么？持有时长量级？
3. HPACK 编码为什么必须在锁内做？（提示：HPACK 有跨帧的动态表状态，顺序不能乱）
4. **`RoundTrip` 什么时候返回？** 看最后那个 `select { <-cs.respHeaderRecv ... }` ——
   它等的是 **respHeaderRecv**，不是 body 读完。**这是 Stage 7 反直觉事实二的根源。**
5. 一条连接上能同时有多少个 in-flight stream？谁定的？超了会怎样？
   （和 Stage 4 第 3 问对照）
6. `readLoop` 每连接一个 goroutine —— 一条连接上同时回来 200 个响应，
   它们是并行处理的还是串行分发的？分发之后呢？
7. GET（无 body）会不会执行 `writeRequestBody`？两条路径的时间线差别在哪？
8. `connsPerPeer = 2` 意味着 UDM 对 UDR 有 2 条连接、2 把 `reqHeaderMu`、2 个 `readLoop`。
   改成 1 / 4 / 8，上面每条的答案怎么变？
9. **UDM→UDR 这一对特别值得算一算**：如果 UDM 有 N 个 handler goroutine 同时卡在
   Stage 0.1 的第 ④ 行，这 N 个请求要挤过 2 条连接。
   N 的上限是多少？（= UDM **入站**的 `MaxConcurrentStreams`，Stage 6 会确认）

### 9.3 产出

**串行资源清单（客户端）**：`reqHeaderMu`(每连接) / `cc.mu`(每连接) / `cc.wmu`(每连接) /
`readLoop` goroutine(每连接) / `connPool.mu`(每 Transport) / `bufio.Writer`(每连接)。
每条标注：**保护什么、持有时间量级、并发量大时排队长度如何增长**。
（"用哪个时间戳能看到它"这一列留空，Stage 12 来填。）

---

## 10. Stage 6 —— HTTP/2 服务端：帧 → `*http.Request` → 启动 handler

**目标**：补上 Stage 1 下面那一层。
**这一层不是 handler goroutine** —— 它是 handler 之前那个**每连接唯一的 serve goroutine**。

**为什么这一层最容易被忽略却最要紧**：
一条 TCP 连接上**所有请求的解析严格串行**（只有一个 serve goroutine），"处理"才并行。
宿主机有空闲 CPU **完全不能说明** serve goroutine 有余量。

> **第一阶段读法**：`grep -n TYcustom xnet/http2/server.go` 命中的 13 处全部跳过
> （175 / 446 / 715 / 724 / 2142 / 2166 / 2336 / 2366 / 2391 / 2397 / 2407 / 2447 / 2785 / 3300）。
> `instrument_server.go` 整个不看。

### 10.1 读什么

| 文件 | 看什么 |
|---|---|
| `xnet/http2/server.go` `serverConn.serve()` | 主循环：读帧、分发。**每连接一个 goroutine** |
| 同文件 `processHeaders` → `newWriterAndRequestNoBody` | HEADERS 帧 → `*http.Request` + `http.ResponseWriter` |
| 同文件 `scheduleHandler` / `unstartedHandler` / `go sc.runHandler(...)` | **handler goroutine 在这里才被启动** |
| `NFs/udr/internal/sbi/server.go` `newHttp2ServerWithIdleTimeout` | `IdleTimeout = 500ms`（**上游硬编码 1ms**，那段注释解释了它曾导致"每请求一条新连接"）。**注意它只设了 `IdleTimeout`，没设 `MaxConcurrentStreams`** |
| 同文件 `h2c.NewHandler(handler, h2Server)` | 本部署全走 h2c（`scheme: http`）；`ListenAndServeTLS` 路径会绕过 h2Server —— 注释说明了这个陷阱 |

### 10.2 必须能回答

1. **serve goroutine 做了什么、没做什么？** 列一个清单。
   它做完 `go runHandler` 之后立刻回去读下一帧 —— 所以它**不等待** handler。
2. 从"HEADERS 帧读完"到"handler goroutine 真正开始跑"之间发生了什么？
   （构造 Request/ResponseWriter → `scheduleHandler` → `go` → runtime 调度 → 中间件入口）
   **这一段是纯粹的用户态排队，和 CPU 空闲程度无关。**
3. `scheduleHandler` 什么时候会真的"排队"而不是立刻 `go`？（grep `unstartedHandler`）
4. **UDR / UDM 的入站 `MaxConcurrentStreams` 是多少？** 在哪设的？
   `newHttp2ServerWithIdleTimeout` 没设 ⇒ 走 `xnet/http2` 的默认值。**这个默认值是多少？**
   → **这是 UDM 作为"中间 NF"最重要的一个数字**：它同时是
   "UDM 能同时处理多少个入站请求"和"UDM 能同时有多少个 handler 卡在出站调用上"的上限。
5. `IdleTimeout = 500ms`（服务端）和 `readIdleTimeout = 1s` / `pingTimeout = 3s`（客户端）
   是**两端各自的超时**。它们会不会互相打架？
   服务端 1ms 的原始值为什么会导致"每请求一条新连接"？
6. UDR 的 handler goroutine 在等 MongoDB 时也是阻塞的。
   UDR 的并发上限和 mongo 连接池大小哪个先到？

### 10.3 产出

**串行资源清单（服务端）**：`serve` goroutine(每连接) / frame writer goroutine + `sc.wmu`(每连接) /
`bufio.Writer` + `flushFrameWriter` / gin router / mongo 连接池。
以及一个数字：**入站 `MaxConcurrentStreams` 的实际生效值**。

---

## 11. Stage 7 —— 响应回程的两个反直觉机制

**这两件事都是 upstream 行为，和你的打点完全无关**，
但它们决定了第二阶段每一个区间的语义。读懂它们，Stage 10 就只是填表。

> 第一阶段读法：`write.go`(47 / 208 / 263) 和 `http2.go`(260 / 320 / 344 / 408)
> 的 `TYcustom` 全部跳过；只看**机制**。

### 11.1 反直觉一：handler `return` 不等于"响应发出去了"

读：`xnet/http2/server.go` 的 `writeHeaders` / `writeFrameFromHandler` 路径、
`xnet/http2/write.go` 的 `writeResHeaders` + `staysWithinBuffer()`、
`xnet/http2/http2.go` 的 `bufferedWriter.Write` 与真实 `net.Conn.Write` 的关系。

机制：handler 调 `c.JSON` → gin 写进 `http.ResponseWriter` → `sc.writeHeaders` 把一个
写请求**投递给 frame writer goroutine** → handler 返回。
真正的 `net.Conn.Write` 由 **frame writer goroutine** 在 write scheduler 排空后触发。

要回答：
1. `writeResHeaders.staysWithinBuffer()` 在什么条件下为 true / false？
   在本部署（响应带 body）恒为哪个？
2. 真正的 `net.Conn.Write` 由谁、在什么时机触发？
3. **高负载下一次 `conn.Write` 会打包多少个 stream 的响应？**
   这意味着"响应进内核"这件事对不同请求**可能是同一个瞬间**。
4. `sc.writeHeaders` 会不会**阻塞 handler goroutine**？什么条件下会？
   （这直接影响 Stage 6 第 4 问那个并发上限的真实含义）

### 11.2 反直觉二：`RoundTrip` 返回不等于"响应收完了"

读：Stage 5 第 4 问那个 `select { <-cs.respHeaderRecv ... }`，
配 `openapi/udr/DataRepository/api_...document.go:139-160`。

机制：`http2.Transport.RoundTrip` 等的是 **response HEADERS**，
返回时 `resp.Body` **还是一个流**。真正读完 body 是在生成代码 `:144` 的
`ioutil.ReadAll(resp.Body)`，反序列化在 `:160`，**两者都在 `RoundTrip` 返回之后**。

要回答：
1. 从 `RoundTrip` 返回到 `Deserialize` 完成，中间做了哪些事？在哪个 goroutine 上？
2. 如果响应 body 很大（跨多个 DATA 帧），剩余帧是什么时候到的？
   由谁读的？（提示：Stage 5 的 `readLoop`）
3. 所以 **`RoundTrip` 的耗时不包含 body 传输** ——
   对小响应影响可忽略，对大响应就不是。
   **`am-data` 的响应有多大？** 去看 MongoDB 里那条文档，
   以及 HTTP/2 的初始 window 和默认帧大小。**这是个必须验证的假设。**

### 11.3 UDM 特有的双重回程

UDM 拿到 UDR 的响应之后还要：
`ReadAll(body)` → `Deserialize` → `SetAMSubsriptionData` → `c.JSON`（序列化）→
交给自己的 frame writer。

所以 UDM 的一次处理里有**两次 JSON 反序列化/序列化**和**两次响应回程机制**。
Stage 11 会把这一段变成一个可算的指标。

---

## 12. Stage 8 —— 三个投影：并发 / 资源 / 瓶颈假设

主线读完了。做三张表。**这三张表是第一阶段的最终产出。**

### 12.1 表一：串行 / 并行 / 顺序不确定（回答问题②）

模板（自己填满，每行必须有 文件:行号）：

| # | 位置 | 粒度 | 类型 | 并发量大时的行为 |
|---|---|---|---|---|
| 1 | UDM handler goroutine 卡在出站调用 | 每入站请求 | **串行（对单个请求）** | in-flight handler 数堆积 |
| 2 | UDM 不同入站请求之间 | — | **并行** | 受入站 `MaxConcurrentStreams` 限制 |
| 3 | `transport.go` `reqHeaderMu` | 每 ClientConn | **串行** | 排队 |
| 4 | `cc.mu` + `awaitOpenSlotForStreamLocked` | 每 ClientConn | 串行 + 条件等待 | 达到上限时阻塞 |
| 5 | client `readLoop` | 每 ClientConn | **串行** | 响应分发串行化 |
| 6 | UDR/UDM `serve` goroutine | 每连接 | **串行** | 请求解析串行化 |
| 7 | `go runHandler` 之后 | 每 stream | **并行** | goroutine 数量膨胀 → 调度延迟 |
| 8 | frame writer + `bufio.Writer` | 每连接 | **串行 + 批量** | 一次 Write 打包多个响应 |
| 9 | mongo driver 连接池 | 每 UDR 进程 | 有限并行 | 池满后排队 |
| 10 | `CreateUDMClientToUDR` 的 `nfDRMu` | 每 UDM 进程 | 读多写少 | 命中后只有 RLock |
| 11 | `UdmUeFindBySupi` / `NewUdmUe` | 每 UDM 进程 | ？ | **去查它的锁！** |
| 12 | `nextSlot` 的 `sync.Map` + atomic | 每 peer | 无锁 | 一次 atomic add |

**「顺序不确定」单独列一节** —— 第一阶段能从代码本身推出来的：
- 同一连接上多个响应的"进内核"时刻可能是**同一个瞬间**（11.1 第 3 问）
- 一个请求的"handler 返回"和它的响应"进内核"之间**没有确定的先后间隔**
- `map[string]interface{}` 序列化出来的 JSON 字段顺序（Go 的 `json.Marshal` 对 map 会排序，
  但**验证一下**）
- 跨两个进程的事件先后（这一条第一阶段**无法回答**，因为它依赖时钟 —— 记进 hypotheses）

### 12.2 表二：资源与上下文（回答问题③）

| 资源 | 谁分配 / 谁回收 | 生命周期 | 上限由谁决定 | 上下文如何获取 |
|---|---|---|---|---|
| goroutine（UDM/UDR 每 stream handler） | `sc.runHandler` | 单请求 | 入站 `MaxConcurrentStreams` | `stream` 的 ctx |
| goroutine（每出站请求 `doRequest`） | `cc.roundTrip` | 单请求 | 无显式上限 | — |
| goroutine（每连接 `serve` / `readLoop` / frame writer） | 建连时 | 连接生命周期 | 连接数 | — |
| TCP 连接（UDM→UDR） | `client_conn_pool.go` | 长连接（服务端 `IdleTimeout=500ms` 后 GOAWAY） | `connsPerPeer=2` + 自动扩张 | 按 `host:port` |
| HTTP/2 stream id | `cc.addStreamLocked` | 单请求 | 连接内单调递增 | — |
| `*http.Client` / Transport 槽位 | `httptransport.go:570` `Client()` | **进程（单例）** | `connsPerPeer` | 全进程共享 |
| `*APIClient`（UDM→UDR） | `consumer/udr_service.go:30` 的 map | 进程 | 每 URI 一个 | `nfDRMu sync.RWMutex` |
| **UDR URI** | `UdmUe.UdrUri` | **每 UE** | — | 空则触发 `SendNFInstancesUDR`（NRF 往返） |
| `*UdmUe` | `NFs/udm/internal/context/` | UE 生命周期 | — | **去查它有哪些锁！** |
| OAuth token ctx | `GetTokenCtx` → `openapi/client.go:562` 消费 | 单请求 | 配置开关 | 本部署是否启用？ |
| Mongo 连接 | mongo driver pool | 进程 | driver 配置（**在哪配？**） | — |
| otel ClientTrace | `openapi/client.go:428` | 单请求 | — | **每请求无条件分配** |

**不同 NF 对 HTTP 的差异** —— 用 `diff` 而不是肉眼，见附录 A。

### 12.3 表三：瓶颈假设（回答问题④ —— 只写机制）

**这一轮的规则：只写"机制"和"预期现象"，不写"怎么验证"。**
验证方法留到 Stage 12 —— 那时才允许看打点。
**这样做的意义是：先有一批从代码推出来的假设，再去看现有观测能覆盖哪些。**

| # | 假设 | 机制（从代码推出，带 文件:行号） | 高 RQ 下预期看到什么现象 |
|---|---|---|---|
| 1 | UDM 是"透传放大器"，自身不慢 | handler 全程阻塞在 `QueryAmData` 一行上 | UDM 的 server 侧延迟 ≈ UDR 往返延迟，差值平坦 |
| 2 | UDM in-flight handler 堆积 | 入站并发 × 下游延迟，上限 = 入站 `MaxConcurrentStreams` | 同时存活的 handler 数线性涨，CPU 不饱和 |
| 3 | `reqHeaderMu` 争用 | 每 ClientConn 一把写锁，HPACK 编码在锁内 | 发送侧排队；把 `connsPerPeer` 翻倍应该缓解一半 |
| 4 | serve goroutine 串行 | 每连接一个读帧 goroutine，请求解析不并行 | handler 启动被延后，与 CPU 空闲无关 |
| 5 | frame writer 批量发送 | `staysWithinBuffer()` 恒 false，真实 Write 由 frame writer 统一触发 | 一次 Write 打包越来越多响应 |
| 6 | 少连接 head-of-line | 2 条连接承载 UDM→UDR 全部流量 | 同连接上并发 stream 越多，单请求越慢 |
| 7 | MongoDB / driver 池 | 池满排队 + 磁盘 | 长尾 |
| 8 | UDR 序列化 `map[string]interface{}` 贵 | 反射 + map 遍历，无法缓存字段元信息 | 随响应大小/RQ 上升 |
| 9 | 每 UE 一次 NRF discovery | `UdmUe.UdrUri` 是**每 UE** 缓存，UDM 无进程级 disccache | UDM→NRF 请求数 ≈ UE 数 |
| 10 | 每请求固定分配开销 | otel trace + 两次 `WithContext` + 多次 JSON 转换 + `bson.M` + map | GC 时间与调度延迟随 RQ 涨，**所有区间一起变慢** |
| 11 | 连接健康检查误杀 | `pingTimeout=3s`：高负载下对端可能来不及回 PONG | 出现非预期的重连；正反馈风险 |

> **方法论警告（第一阶段就要立好的规矩）**：
> 「某一段变慢」≠「这一段是瓶颈」。必须区分三种情况：
> **(a)** 这一段自己在干活且变慢；
> **(b)** 这一段在等前面的串行资源（等待被算进了它的区间）；
> **(c)** 这一段只是被批量化了 —— 等待其实发生在它开始**之前**。
>
> Stage 7 的两个反直觉机制正好各自制造一种混淆：
> 11.1 制造 (c)（响应批量进内核），11.2 制造 (b)（body 传输被算进"之后的处理"）。
> **所以第一阶段必须先把机制读清楚，否则第二阶段拿到数据一定会归错因。**
> 在 open5gs SCP 那边 (c) 曾被误判成 (a) —— 这边同样适用。

### 12.4 第一阶段完成的标志

- [ ] 三张表填完，每行都有 `文件:行号`
- [ ] `notes/http_path_facts.md` 里**没有一条**引用时间戳或日志字段
- [ ] 表三每个假设都能只用代码解释清楚机制
- [ ] 能画出 Stage 0.3 那张全景图，且**图上一个时间点名字都没有**

---

# 第二阶段：观测层

> 只有在第一阶段三张表填完之后才进入这里。

## 13. Stage 9 —— 十个点分别加在哪、为什么加在那

现在回头看那些跳过的代码。**先读设计文档，再读实现**：

| 顺序 | 读什么 | 内容 |
|---|---|---|
| ① | `ACCESSLOG.md` | `HTTP_log.txt` / `DB_log` 的字段定义与 client/server 双视角 |
| ② | `HTTP_3detailLog_PLAN_0826.md` 第 2 节 | 十个点的完整位置表（**已按实际代码校订过**） |
| ③ | `httptransport.go` 那些跳过的行 | T1(360) / T2(331) / T5(336) / T6(362) / T3(485) / T4(493) |
| ④ | `xnet/http2/instrument_client.go` | M / M2 的载体；`ClientConnIdentity`；atomic 契约；trace 跨 retry 存活 |
| ⑤ | `xnet/http2/transport.go` 的 `TYcustom` 行 | M(1443) / M2(1452) 的取时位置 —— **为什么必须在加锁 `select` 之前** |
| ⑥ | `xnet/http2/instrument_server.go` | G 的载体；三条 mutability 契约；为什么 trace 内联进 `stream` |
| ⑦ | `xnet/http2/server.go` 的 `TYcustom` 行 | G(2407 / 2447) 的位置 |
| ⑧ | `write.go` + `http2.go` 的 `TYcustom` 行 + 计划第 14-15 节 | W：marker 机制 + 真实 socket write 取时 |
| ⑨ | `accesslog.go` | 异步队列、单 writer goroutine、`linePool`、`Dropped()` |
| ⑩ | `dbtrace.go` | DB 段打点 |

**每个点要回答的同一组问题**：
1. 它取在哪一行？**为什么必须是那一行而不是前后一行？**
2. 它在什么 goroutine 上执行？那个 goroutine 是不是串行资源？
3. 它的热路径成本是多少？（几次 clock read、几次 atomic、几次分配）
4. 它夹住的是 Stage 1-7 里的**哪个机制**？

第 4 问是这一 Stage 的核心：**把十个点一一映射回第一阶段读过的机制**。
映射不上的点说明你第一阶段漏了东西，回去补。

**这一 Stage 的产出**：那张十点时间线图。

```text
     UDM (client)                         UDR (server)                      UDM (client)
T1 ──► M ──► M2 ──► T2 ══网络══► G ──► T3 ──► [handler+Mongo] ──► T4 ──► W ══网络══► T5 ──► T6
```

---

## 14. Stage 10 —— 每个区间的语义边界

逐段写清"它代表什么"和"它**不**代表什么"。
**Stage 7 的两个反直觉机制在这里变成两条硬约束**：

| 约束 | 来自 | 后果 |
|---|---|---|
| `T4` 不是"响应发出去了" | 11.1（handler 返回 ≠ 进内核） | `W - T4` 是一段真实的、可批量化的等待；`T4` 不能当发送时刻用 |
| `T6` 不是"响应收完了" | 11.2（RoundTrip 只等 header） | `T6 - T1` **不含** body 传输；`T6` 之后还有 ReadAll + Deserialize |

必须逐段写完的清单：

| 区间 | 代表 | 不代表 |
|---|---|---|
| `T1 → M` | | |
| `M → M2` | | |
| `M2 → T2` | | |
| `T2 → G` | | （跨进程，依赖 NTP） |
| `G → T3` | | |
| `T3 → T4` | | |
| `T4 → W` | | |
| `W → T5` | | （跨进程） |
| `T5 → T6` | | |
| `T6 → 业务拿到对象` | **没有打点** | |

最后一行是重点：**openapi 的 `ReadAll` + `Deserialize` 完全没有被观测**。
把它记进 Stage 12 的缺失清单。

配套还要读：
- 计划第 17 节：join 的可行性（`(conn, stream_id)` 是唯一可靠的跨 pod 精确 key；
  `ue_id + method + uri + 时间序`**不够**）
- 计划第 2.1 节末尾那张**打点成本表**：A 组（6 点）和 B 组（9/10 点）的数据不能混着比
- 失败记录是 `stream_id=0` + 空 M（`httptransport.go:381-385`）—— **离线分析必须先丢掉**
- 重试记录：`wroteTime` 留第一次而 `connID` 留最后一次 ⇒ **只接受 `retry_count == 0`**

---

## 15. Stage 11 —— 嵌套区间：免费拿到的四个指标

**这是选 UDM↔UDR 做主线的最大回报，AMF 给不了。**

UDM 处理一个 `GetAmData` 时，在**同一个 handler goroutine** 上产生两条 `HTTP_log.txt` 记录：

- **server 行**（`src="NaN"`, `dst="UDM"`）：`T3` 入站到达，`T4` 入站响应
- **client 行**（`src="UDM"`, `dst="UDR"`）：`T1` 出站发出，`T6` 出站响应头收到

区间**严格嵌套**：`T3 < T1 < T6 < T4`。于是**不加任何新打点**就能算出：

| 指标 | 公式 | 代表什么 | 对应 Stage 0.1 的哪几步 |
|---|---|---|---|
| **前段** | `T1 - T3` | 入站中间件 + 路由 + 取参数 + `GetTokenCtx` + 建 Request struct + `CreateUDMClientToUDR` + openapi 序列化 + 选槽位 | ①②③ + Stage 3 出站 |
| **中段** | `T6 - T1` | UDR 往返（网络 + UDR 处理 + Mongo + 响应头回来） | ④ |
| **后段** | `T4 - T6` | body 读取 + 反序列化 + 更新 `UdmUe` + `c.JSON` 序列化 | ⑤⑥⑦ + Stage 3 入站 |
| **UDM 自身计算** | `(T4 - T3) - (T6 - T1)` | = 前段 + 后段，**UDM 真正花在自己身上的时间** | 全部除 ④ |

同理在 UDR 侧，`(T4 - T3)` 减去 `DB_log` 里那条 `GetOne` 的 latency，
就是 **UDR 自身计算时间**。

### 15.1 但是有两个坑必须先解决

**坑一：怎么把这两行 join 起来。**
server 行的 `src` 恒为 `"NaN"`。跨 pod 的精确 key 是 `(conn, stream_id)`，
但 UDM 内部这两行属于**不同的连接**（一条入站、一条出站），`conn` 配不上。
**「区间嵌套 + ue_id」够不够？什么情况下会歧义？**
（先读计划第 17.2 节。结论若是"不够"，写下**需要加什么最小打点** —— 那就是下一份 plan。）

**坑二：`T6` 的语义（Stage 10 的第二条硬约束）。**
`T6` 是"响应头到了"，不是"响应收完了"。所以：
- 中段 `T6−T1` **低估**了等 UDR 的时间（缺 body 传输）
- 后段 `T4−T6` **高估**了 UDM 自己的时间（多算了 body 传输）

必须先验证 Stage 7 第 11.2 节第 3 问（响应体大小）再声称"UDM 自身计算 = X ms"。

---

## 16. Stage 12 —— 回填 Stage 8 的瓶颈表

把 12.3 那张表拿出来，给每个假设补上第三列：
**用哪个字段、算什么、看到什么算证实 / 证伪。**

| # | 假设 | 验证：算什么 | 证实的样子 | 证伪的样子 |
|---|---|---|---|---|
| 1 | UDM 透传放大器 | `(T4−T3) − (T6−T1)`（Stage 11） | 随 RQ 平坦 | 上升 ⇒ UDM 自身成瓶颈 |
| 2 | in-flight handler 堆积 | 同时刻 in-flight 请求数 vs RQ；对照 `MaxConcurrentStreams` | 线性涨且 CPU 不饱和 | 平坦 |
| 3 | `reqHeaderMu` 争用 | `M2 − M` 的 P50/P99，按 `conn_slot` 分组 | 随 RQ 抬升；`connsPerPeer` 翻倍后≈减半 | 恒定≈0 |
| 4 | serve goroutine 串行 | `T3 − G` 分布 | 随 RQ 上升 | 平坦 |
| 5 | frame writer 批量 | `W − T4`；一次 Write 跨过的 marker 数 | marker 数随 RQ 上升 | 恒为 1 |
| 6 | head-of-line | 同 `conn` 上并发 stream 数 vs 延迟 | 强正相关 | 无关 |
| 7 | MongoDB | `DB_log` latency 分布 vs 并发 | 长尾 | 平坦 |
| 8 | map 序列化贵 | `(T4−T3) − DB_log latency` | 随响应大小/RQ 上升 | 平坦 |
| 9 | 每 UE 一次 NRF discovery | `HTTP_log` 里 UDM→NRF 行数 vs UE 数 | ≈ UE 数 | ≈ 常数 |
| 10 | 每请求固定分配开销 | ？ | | |
| 11 | 连接健康检查误杀 | `conn_reused == false` 记录的时间分布 | 运行中出现新连接 | 只在开头出现 |

**注意 10 号是空的** —— 这就是这一 Stage 的真正产出：

### 16.1 缺失打点清单（本 tutorial 的最终交付物之一）

把所有"验证方法填不上"或"只能间接验证"的假设列出来。目前已知至少三条：

| 缺什么 | 为什么现有打点不够 | 影响哪个假设 |
|---|---|---|
| **序列化 / 反序列化耗时** | `T1..T6` 只覆盖 RoundTripper 进出；openapi 的 `json.Marshal` 在 T1 之前，`ReadAll`+`json.Unmarshal` 在 T6 之后（Stage 10 最后一行） | 8、10 |
| **分配 / GC 归因** | 没有任何按请求的分配计数 | 10 |
| **UDM 内部 server 行 ↔ client 行的精确 join** | `conn` 配不上（15.1 坑一） | 1、2、8 |

每条写清：**加在哪一行、在哪个 goroutine 上、热路径成本、会不会自己变成瓶颈**
（`msgtrace.go` 的包注释和 `AMF_LOG_REMOVE_RECVTIME_LOCK_PLAN_0724.md`
各记录了一次"观测本身成了瓶颈"的实例，先读它们再动手）。

---

## 17. 你没提但必须补的五个维度

### 17.1 错误、超时与重试路径（第一阶段就要读）
- `transport.go` 的 `shouldRetryRequest`：什么请求可重试、重试几次、换不换连接
- `timeoutPeriod = 10s`（`http.Client.Timeout`）、`readIdleTimeoutPeriod = 1s` +
  `pingTimeoutPeriod = 3s`（连接健康检查）、服务端 `IdleTimeout = 500ms` 的相互作用
- 连接被 GOAWAY / PING 超时杀掉后，正在飞的请求怎么办
- UDM 侧的错误映射：`GenericOpenAPIError` → `apiError.RawBody` 透传 / `ProblemDetails`
  （Stage 0.1 第 ⑤ 步；`GetIdTranslationResultProcedure` 里还有 `apiErr.Model()` 分支）。
  注意 `api_...:154` **每个请求都构造 `GenericOpenAPIError`**，成功路径也构造
- **高 RQ 下延迟恶化时，这些超时会不会开始触发？会不会形成正反馈？**
  （连接被杀 → 重连 → 流量重分布 → 更慢 → 再被杀。这是 12.3 表的 11 号假设）

### 17.2 背压与队列容量（第一阶段读功能部分）
每个 channel / buffer 都问三件事：**容量多大、满了怎么办、谁在等**。
- HTTP/2 connection-level / stream-level flow control window → 发送方阻塞
- `bufio.Writer` → 何时强制 flush
- 服务端 `unstartedHandler` 队列 → 达到 `MaxConcurrentStreams` 之后
- mongo driver 连接池 → 池满时是排队还是报错？
- （第二阶段追加）accesslog 的 `queue`/`wQueue` 满了 → **静默丢日志**，
  `Dropped()`/`WDropped()` 非零就说明数据不完整，**先修再分析**

### 17.3 内存分配与 GC（第一阶段）
UDM↔UDR 这条路上的每请求分配点（Stage 3.5 数了一部分，补齐）：
`otelhttptrace.NewClientTrace`、`Request.WithContext` 拷贝、
`json.Marshal` 的 `[]byte` + `bytes.Buffer`、`url.Values`、`GenericOpenAPIError`、
`bson.M` filter、`map[string]interface{}` 结果、`c.GetRawData()` 的 body 拷贝、`util.ToBsonM`。
**GC 压力会同时抬高所有区间，看起来像"哪里都慢一点"** —— 最难归因的一类现象。

### 17.4 时钟与跨进程比较（第二阶段）
- 所有 `time.Now()` 都是本进程时钟；跨 pod 区间（`T2→G`、`W→T5`）依赖 NTP，
  **不能算绝对延迟**，只能看趋势
- 同进程内的区间才是硬数据
- 客户端记的 `conn` 是 `LocalAddr`，服务端记的是 `RemoteAddr`，
  相同的前提是路径上没有 NAT / 源地址改写 —— **在 K8s 里要单独确认**
- **区间的语义边界比区间的数值更重要**（Stage 10 的两条硬约束）

### 17.5 观测本身的开销（第二阶段）
从 6 个 `time.Now()` 到 9/10 个是 +50% 以上，真实增量还包括 atomic、context node、
JSON 字段格式化、buffer 扩容、额外的 W 日志行、`ueIDFromFilter` 的 filter 遍历。
计划第 2.1 节末尾那张成本表要抄进笔记。**A 组和 B 组的数据不能混着比。**

---

## 18. Stage 13 —— 之后再看 AMF

AMF = 这个骨架 + 五样额外的东西：

| 额外的东西 | 文件 | 它改变了什么 |
|---|---|---|
| SCTP / NGAP 入口 | `NFs/amf/internal/ngap/service/`、`dispatcher.go:17 Dispatch` | 请求的**触发源**不再是 HTTP ⇒ **Stage 11 那四个嵌套指标在 AMF 上不成立** |
| worker 调度器 | `NFs/amf/internal/ngap/scheduler.go` | `Task`(29) / `Worker.run()`(61) / `hashUEID`(228) vs `workerForConn`(235)。**dGNB 模式下按 SCTP association 钉住 worker** ⇒ 同一 gNB 的所有 UE 串行 |
| GMM 状态机 | `NFs/amf/internal/gmm/handler.go` | 一条 NAS 消息触发**一串**顺序 SBI 调用。看 `communicateWithUDM`(1004)：1 次 NRF discover + UECM 注册 + 3 次 SDM Get + 1 次 Subscribe，**全部串行** |
| `msgtrace`（观测，第二阶段） | `NFs/amf/internal/msgtrace/msgtrace.go` | 因为没有入站 HTTP 区间可用，AMF 只能**另加**打点来得到 Stage 11 在 UDM 上免费拿到的那个量。**读它的包注释** —— 它解释了为什么第一版（goroutine-id 查表）自己成了延迟源 |
| 进程级发现缓存 | `NFs/amf/internal/disccache/disccache.go` | 和 UDM 的"每 UE 缓存 `UdrUri`"形成对比 ⇒ 12.3 表的 9 号假设 |

**读 AMF 时的核心问题**：AMF 的"串行单元"是 worker goroutine（每 association 或每 UE-hash），
UDM 的"串行单元"是 handler goroutine（每请求）。这两种模型在高 RQ 下的失效方式**不一样** ——
差别在哪？

---

## 19. 最终产出物

1. **`notes/http_path_facts.md`** —— 逐条带 `文件:行号`（第一阶段部分不含任何时间戳）
2. **一张主图** —— Stage 0.3 全景图（第一阶段版无时间点，第二阶段版加上十个点）
3. **三张表** —— 12.1 串行点 / 12.2 资源 / 12.3+16 瓶颈假设与验证
4. **Stage 11 四个指标的计算脚本** —— 先解决 15.1 的两个坑
5. **缺失打点清单**（16.1）—— 这就是下一份 plan 的内容
6. **一份"下一步实验设计"** —— 从瓶颈表挑 2-3 个假设，写出：
   改什么参数（`connsPerPeer`？入站 `MaxConcurrentStreams`？`IdleTimeout`？mongo 池大小？）、
   预期哪个指标怎么变、**如果不变说明什么**

---

## 20. 分阶段 checklist

**第一阶段：原始路径（不看任何时间戳/日志）**

- [ ] **D1** 3.3 建两个笔记文件；**Stage 0 读到能背**；画出全景图（**图上不许有时间点**）
- [ ] **D2** Stage 1（UDR 完整生命周期，GET + PUT 两条路径）；产出逐层清单
- [ ] **D3** Stage 2 第一、二步（UDM 入站复核 + 请求处理）；填上 6.1 那两个空
- [ ] **D4** Stage 2 第三步（出站组装）；产出出站调用字典表；核对 `SetHTTPClient` 覆盖率
- [ ] **D5** Stage 3（openapi）；确认 JSON 生成/反序列化点；画数据结构变换链
- [ ] **D6** Stage 4（连接槽位与超时）；说清相对 upstream 的功能差异；数出总连接数
- [ ] **D7** Stage 5（HTTP/2 客户端，跳过 8 处 TYcustom）；产出客户端串行资源清单
- [ ] **D8** Stage 6（HTTP/2 服务端，跳过 13 处 TYcustom）；**查出入站 `MaxConcurrentStreams` 实际值**
- [ ] **D9** Stage 7（两个反直觉机制）；验证 `am-data` 响应体大小
- [ ] **D10** Stage 8：三张表；补 17.1/17.2/17.3；**过 12.4 的完成检查**

**第二阶段：观测层**

- [ ] **D11** Stage 9：十个点的位置与成本；把每个点映射回第一阶段的机制；画十点时间线
- [ ] **D12** Stage 10：填完区间语义表；读计划第 17 节（join）
- [ ] **D13** Stage 11：解决 join 歧义 → 用真实 `HTTP_log.txt` 算出四个指标
- [ ] **D14** Stage 12：回填瓶颈表；**产出缺失打点清单**
- [ ] **D15+** Stage 13：AMF

---

## 附录 A：常用检索命令

```bash
cd /d/UB/Nemo/5GC_project/code/Free5gc-TYcustom

### ★ 第一阶段跳读清单（这就是要跳过的全部内容）★
grep -rn "TYcustom" xnet/http2/*.go            # 协议栈里的观测钩子，全部跳过
grep -rn "TYcustom" NFs/udm/internal/ NFs/udr/internal/ --include=*.go

### 结构核对 ###
# 七个 NF 的 httptransport.go 是否仍逐字节相同
for nf in amf ausf nrf nssf pcf udr; do
  diff -q NFs/udm/internal/accesslog/httptransport.go NFs/$nf/internal/accesslog/httptransport.go
done

# 各 NF 的 server 配置差异（IdleTimeout / 中间件 / h2c / MaxConcurrentStreams）
grep -n "idleTimeoutPeriod\|router.Use\|h2c.NewHandler\|MaxConcurrentStreams" \
     NFs/*/internal/sbi/server.go

# 各 NF 是否都 replace 了 xnet（没 replace 的用官方 x/net，行为可能不同）
grep -n "replace" NFs/*/go.mod

# NewConfiguration 次数 vs SetHTTPClient 次数（不等 = 一部分流量的连接行为不同，见 Stage 3.2）
grep -rc "NewConfiguration()" NFs/*/internal/sbi/consumer/*.go
grep -rc "SetHTTPClient"      NFs/*/internal/sbi/consumer/*.go

### UDR 主线 ###
grep -n '"/subscription-data' NFs/udr/internal/sbi/api_datarepository.go | head -40
grep -rhon 'collName := "[^"]*"' NFs/udr/internal/sbi/api_*.go | sort -u
grep -rn "Mongodb\|mongodb" NFs/udr/pkg/factory/*.go config/udrcfg.yaml 2>/dev/null | head

### UDM 主线 ###
grep -rn "clientAPI\..*Api\." NFs/udm/internal/sbi/processor/ | head -40
grep -rn "sync.Mutex\|sync.RWMutex\|\.Lock()\|\.RLock()" NFs/udm/internal/context/ --include=*.go

### openapi 层（./openapi，v1.2.3）###
grep -n "func PrepareRequest\|func CallAPI\|func setBody\|func Deserialize" openapi/client.go
grep -n "func Serialize" openapi/serialize.go
grep -n "localVarPath = strings.Replace\|url.Values{}\|PrepareRequest\|CallAPI\|ReadAll\|Deserialize" \
     openapi/udr/DataRepository/api_access_and_mobility_subscription_data_document.go

### 通用（找并发点 / 串行点 / 队列）###
grep -rn "go func\|^\s*go [a-zA-Z_]" NFs/udm/internal/ NFs/udr/internal/ --include=*.go | grep -v _test
grep -rn "sync.Mutex\|sync.RWMutex\|\.Lock()\|\.RLock()" NFs/udm/internal/ NFs/udr/internal/ --include=*.go | grep -v _test
grep -rn "make(chan" NFs/udm/internal/ NFs/udr/internal/ xnet/http2/ --include=*.go | grep -v _test
```

## 附录 B：仓库里已有的文档索引

**⚠️ 这些文档全部是打点设计文档 —— 第一阶段一份都不要看。**

| 文档 | 讲什么 | 什么时候看 |
|---|---|---|
| `ACCESSLOG.md` | `HTTP_log.txt` / `DB_log` 的字段定义与双视角 | **Stage 9 第一份** |
| `HTTP_3detailLog_PLAN_0826.md` (2613 行) | M/G/W 的完整设计；第 2 节十点位置表；第 14 节 W；第 17 节 join | Stage 9-11 主参考 |
| `HTTP_10thlog_0903.md` | 第 10 个点 M2 | Stage 9 |
| `HTTP_WROTE_TIME_PLAN_0804.md` | T2 的引入 | Stage 9 |
| `HTTP_CONN_IDENTITY_LOG_PLAN_0806.md` | conn 身份如何做跨 pod join key | Stage 10 |
| `HTTP_SINGLE_CONN_PLAN_0806-1.md`、`HTTP_MULTI_CONN_ROUNDROBIN_PLAN_0806.md`、`HTTP_4CONN_ROUNDROBIN_PLAN_0807.md`、`HTTP_8CONN_PLAN_0809.md`、`HTTP_16CONN_PLAN_0809v1.md` | 连接数实验序列，**多处以 UDM→UDR 为主样本** | Stage 12 之后设计新实验时 |
| `HTTP2_IDLETIMEOUT_FIX_PLAN_0807.md` | 1ms IdleTimeout 导致"每请求一条新连接"的 bug | **Stage 6 可以看**（它讲的是一个功能性 bug，不是打点） |
| `UDR_MONGO_DRIVER_LATENCY_PLAN_0809.md` | UDR→Mongo 段 | Stage 12 |
| `DISCCACHE.md` | NRF 发现缓存（AMF 的做法） | Stage 2 第三步（这个是功能，可以看） |
| `LOCK_SCHED_PROFILING_GUIDE_0826.md`、`AMF_MUTEX_PROFILING_GUIDE.md` | mutex / sched profile | Stage 12 之后 |
| `AMF_WORKER_LOG_PLAN.md`、`AMF_LOG_REMOVE_RECVTIME_LOCK_PLAN_0724.md` | AMF 专有；后者是"观测成瓶颈"的实例 | Stage 13 / 16.1 |

## 附录 C：读每个函数时反复问自己的六个问题

1. **谁在调它？在哪个 goroutine 上？**
2. **它拿到的数据结构里，哪些字段是这一层新填的？**
3. **它有没有分配内存？有没有拷贝？**
4. **它有没有等待？等锁、等 channel、等网络、等 flow control、等 DB？**
5. **它把数据交给下一层时，交的是值还是指针？所有权归谁？谁负责释放？**
6. **如果同时有 1000 个它在跑，什么东西会先撑不住？**

第 6 问就是你的问题④。前 5 问答扎实了，第 6 问是自然结果。

**第一阶段还要加一问**：**这一行是功能还是观测？** 观测就跳过。
判定标准：**去掉它，请求的走向、内容、时序会不会变？** 不变的就是观测。
（唯一的例外是 `sniffUEID` —— 目的是观测，但它换掉了 `req.Body`，所以算功能。）
