# NF↔NF 双 HTTP/2 连接 + Round-Robin 改造计划 (0806)

> **状态：已实施**（在 commit `0848dba` 之上）。
> 本文件已根据 `C6525100g_HTTPconnum_0806` 的实测数据重写；
> 初版中「每个 NF 对恰好 1 条 TCP」的假设**已被实测推翻**，见第 1 节。
>
> **修订（本次）**：`PingTimeout` 1s → 3s，缓解高负载下健康检查误杀连接
> 导致的重建。这是相对 `2fa0755` 的**唯一行为差异**，见第 2.3 节。

---

## 0. TL;DR

| # | 问题 | 结论 |
|---|---|---|
| 1 | 改造前两个 NF 之间只有一条 TCP 吗？ | **不是**。低负载下是 1 条，但高负载下实测多达 **10 条**（UDM→UDR @1500 req/s）。初版计划的核心假设是错的。 |
| 2 | 强制开 2 条 + round-robin 可行吗？ | **可行，已实施**。每个 NF 只改 `internal/accesslog/` 下两个文件。 |
| 3 | 请求走连接 1，响应会从连接 2 回来吗？ | **不会，100% 保证**。HTTP/2 协议 + Go transport 的结构性保证。 |
| 4 | 日志能验证 round-robin 效果吗？ | **能**。新增 `conn_slot` 字段（本次），配合已有的 `conn` / `conn_reused`。见第 5 节。 |
| 5 | 连接断了会自动重建吗？ | **会**。每个槽独立重建，互不影响，轮询逻辑不受干扰。见第 6 节。 |
| 6 | 健康检查误杀连接怎么办？ | **`PingTimeout` 1s → 3s**（本次）。1s 会把「忙」误判成「死」，见第 2.3 节。 |

> **术语**：下文的「实例 / instance」一律指**进程内的 `http2.Transport` 对象**，
> 不是 NF 的 pod / 副本。**NF 部署拓扑一个都不变。**

---

## 1. 前提修正：改造前并非「只有一条连接」

### 1.1 初版假设与实测的冲突

初版计划断言「每个 NF 对恰好 1 条 TCP」，理由是 `http2.Transport` 的连接池按
`(scheme, host:port)` 复用。**这个推理在低负载下成立，在高负载下不成立。**

用 `conn` 字段（commit `2fa0755` 引入）实测 UDM→UDR：

| RQ | 连接数 | 峰值并发 stream | 主连接占比 |
|---|---|---|---|
| 1000 | 1 | 48 | 100% |
| 1500 | **10** | **371** | 86.4% |
| 2000 | 5 | 297 | 91.4% |

### 1.2 连接增长的两种机制（实测确认）

RQ1500 各连接诞生瞬间的并发 stream 数：

```
#1  @   4.5ms  streams_at_birth=  1   reqs=6986   ← 起始连接
#2  @ 434.7ms  streams_at_birth=250   reqs=4      ┐
#3  @ 449.9ms  streams_at_birth=250   reqs=28     │
#4  @ 527.0ms  streams_at_birth=251   reqs=33     │
#5  @ 550.4ms  streams_at_birth=251   reqs=55     ├ 机制①：撞 250 上限
#6  @ 603.9ms  streams_at_birth=251   reqs=215    │
#7  @ 673.4ms  streams_at_birth=249   reqs=188    │
#8  @ 753.2ms  streams_at_birth=249   reqs=10     │
#9  @ 763.3ms  streams_at_birth=247   reqs=6      ┘
#10 @ 820.9ms  streams_at_birth=  1   reqs=563    ← 机制②：主连接刚死
```

**机制① — `MaxConcurrentStreams` 溢出**。Go 文档（`StrictMaxConcurrentStreams`）：

> If false, **new TCP connections are created to the server as needed** to keep
> each under the per-connection SETTINGS_MAX_CONCURRENT_STREAMS limit.

**机制② — 健康检查误杀后重建**。`PingTimeout = 1s`（Go 默认 15s）在高负载下
误判连接失效。实测主连接 @813.6ms 死亡，#10 @816.4ms 接管后续 563 个请求。

> 机制②已由本次的 `PingTimeout` 1s → 3s **针对性缓解**（2.3 节）。
> 机制①（撞 250 上限溢出）**未处理也不打算处理**——那是真实的并发压力信号。

### 1.3 对改造的影响

初版计划的价值主张是「1 条 → 2 条」。实测表明改造前就是「**浮动的 1~10 条**」，
且**没有**可靠手段把它锁成固定值（Strict 的尝试见 2.1，失败）。

因此本次的对照设计改为：

| 组 | 配置 | 连接数 | 关键指标 |
|---|---|---|---|
| 对照（`2fa0755`） | 默认 | 1 条起步，高负载溢出 | 主连接占 86~100% |
| 实验（**本次**） | 2 个 transport 轮询 | 2 条起步，高负载溢出 | `conn_slot` 应接近 50:50 |

对照的重点**不是连接总数相等**，而是**负载分布**：改造前无论有几条连接，
主连接始终扛 86~100%；改造后轮询强制按请求数均分。

---

## 2. 需求

| # | 要求 | 实现方式 |
|---|---|---|
| **R1** | 解除「只能 1 条连接」的限制，回到 `0848dba` 之前的状态 | **移除** `StrictMaxConcurrentStreams: true`；transport 字段与 `2fa0755` 一致，**唯一例外是 R5** |
| **R2** | 强制每对 NF 开 **2** 条连接 | 2 个独立 `http2.Transport` 实例 |
| **R3** | round-robin 分流 | 原子计数器 `next.Add(1) % 2` |
| **R4** | 连接断了允许重建 | Go 默认行为，本就如此（第 6 节） |
| **R5** | 减少健康检查误杀导致的连接重建 | `pingTimeoutPeriod` **1s → 3s**（见 2.3） |

### 2.1 为什么移除 `StrictMaxConcurrentStreams: true`（实测依据）

初稿曾建议保留 Strict 以锁死连接数。**实测数据否定了这个建议。**

`C6525100g_NFHTTPonly1conn_0806v1`（Strict=true 的那次实验）UDM→UDR 结果：

| RQ | 800 | 1000 | 1200 | 1400 | 1600 | 1800 | 2000 |
|---|---|---|---|---|---|---|---|
| 连接数 | 2 | 4 | 2 | 3 | **7** | 4 | 3 |

**Strict 并没有把连接数降到 1**，反而制造了大量短命连接。RQ1600 的连接生命周期：

```
192.168.88.209:38012  born=+  0.0ms  died=+ 25.8ms  reqs=161
192.168.88.209:38026  born=+ 24.5ms  died=+122.6ms  reqs=915
192.168.88.209:38042  born=+120.1ms  died=+241.8ms  reqs=1001
192.168.88.209:38046  born=+229.6ms  died=+546.3ms  reqs=2474
192.168.88.209:38054  born=+549.7ms  died=+753.6ms  reqs=1554
192.168.88.209:38068  born=+756.0ms  died=+758.0ms  reqs=2
192.168.88.209:38084  born=+763.6ms  died=+765.4ms  reqs=1
```

连接每 25~120ms 就被销毁重建一次，**几乎是纯粹的先死后生**（重叠仅 3 对）。

**推测原因**：Strict 让超限请求阻塞在 `RoundTrip`，连接上没有新帧流动；
`ReadIdleTimeout=1s` 触发 PING 健康检查，而对端在高负载下 1 秒内回不上，
`PingTimeout=1s` 随即判定连接失效并销毁。**Strict 与激进的健康检查超时互相踩踏。**

结论：Strict 不但没达到锁死连接的目的，还严重干扰日志分析。**本次移除。**

### 2.2 移除 Strict 的代价：连接总数不再受控

必须明说：去掉 Strict 后，两个 transport 各自仍可能在撞 250 时溢出建连，
所以实际连接数是「**2 条起步，可能更多**」，不是严格的 2 条。

这是**有意接受的取舍**：

- 保留 Strict → 连接数名义受控，但实测反而churn 出更多短命连接，日志不可读
- 移除 Strict → 连接数可能 >2，但每条连接生命周期长、日志干净可分析

判读时用 `conn_slot`（本次新增）区分：**轮询是否均衡看 `conn_slot`，
实际 socket 数看 `conn`**。两者分开记录正是为了应对这种情况。

### 2.3 `PingTimeout` 1s → 3s（本次新增）

**改动**：`NFs/<nf>/internal/accesslog/httptransport.go`

```go
readIdleTimeoutPeriod = 1 * time.Second   // 不变
pingTimeoutPeriod     = 3 * time.Second   // 原 1s
timeoutPeriod         = 10 * time.Second  // 不变
```

**机制**：连接上 `ReadIdleTimeout`（1s）内没有任何帧到达 → transport 主动发 PING
→ `PingTimeout` 内收不到 PONG → **判定连接失效并销毁**。

**为什么 1s 太短**：这个检查区分不了「对端死了」和「对端很忙」。高负载下 UDR
的 event loop 被塞满，1 秒内回不上一个 PING 是**正常现象**，不是故障。结果是
健康的连接被反复误杀：

- `C6525100g_HTTPconnum_0806`：主连接 @813.6ms 被杀，@816.4ms 新连接接管
  （6.3 节原本把这段当作「重建无缝」的正面佐证，**实际它是误杀的证据**）
- `C6525100g_NFHTTPonly1conn_0806v1`：Strict 让请求阻塞在 `RoundTrip`，
  连接上更没有帧流动，误杀被进一步放大 → 每 25~120ms 重建一次（2.1 节）

**为什么改 3s 而不是 15s**：

| 值 | 影响 |
|---|---|
| 1s（原） | 忙 = 死，误杀频繁，`conn` 生命周期被切碎，per-connection 指标不可读 |
| **3s（本次）** | 留出足够余量吸收负载抖动，仍远低于 Go 默认，真死也能及时发现 |
| 15s（Go 默认） | 误杀基本消除，但真正的连接故障要 15s 才被发现，实验中途出问题难以察觉 |

**这是本次相对 `2fa0755` 的唯一行为差异**，必须在对比时明说。第 6.4 节
原本写「若后续发现误杀频繁干扰实验，可放宽到 15s，**本次不改**」——
现在改了，理由是误杀已经被实测确认（而非「若发现」），且它直接污染
本次要测的 per-slot 连接寿命。取 3s 而非 15s 是为了把行为改动压到最小。

> **对实验解读的影响**：对照组（`0848dba` / `2fa0755`）跑的是 1s。
> 若要严格归因「双连接」的效果，理想做法是**对照组也用 3s 重跑一次**；
> 否则两组之间存在「连接数」和「ping 超时」两个变量。
> 若时间不允许，至少在结论中注明这一点——见第 8 节验证清单。

### 3.1 改动范围

| 项 | 内容 |
|---|---|
| **已改文件** | 7 个 NF × 2 个文件 = **14 个** |
| | `NFs/<nf>/internal/accesslog/httptransport.go`（连接策略 + `PingTimeout` 3s） |
| | `NFs/<nf>/internal/accesslog/accesslog.go`（`conn_slot` 日志字段） |
| **NF 列表** | `amf` `ausf` `udm` `udr` `pcf` `nrf` `nssf` |
| **未改动** | ✅ consumer/service 代码 ✅ openapi 外部模块 ✅ `config/*.yaml` |

### 3.2 `httptransport.go`

**a. 新增常量**

```go
const connsPerPeer = 2
```

**b. struct 改为定长数组 + 原子游标**

```go
type loggingRoundTripper struct {
	tls   [connsPerPeer]http.RoundTripper // h2 over TLS  (https)
	clear [connsPerPeer]http.RoundTripper // h2c cleartext (http)
	next  atomic.Uint64                   // round-robin cursor
}
```

**c. 构造函数改为循环**，每轮建立**独立**的 `http2.Transport`。
字段**集合**与 `2fa0755`（Strict 引入前）一致——只有
`AllowHTTP` / `DialTLSContext` / `ReadIdleTimeout` / `PingTimeout`，
**不设** `StrictMaxConcurrentStreams`；
其中 `PingTimeout` 的**取值**由 1s 改为 3s（2.3 节）。
连接池是每实例私有的，所以 2 个实例 = 2 条 TCP。

**d. `RoundTrip` 选路**

```go
pool := &l.clear                 // 取地址：数组是值类型，直接赋值会整份拷贝
if req.URL != nil && req.URL.Scheme == "https" {
	pool = &l.tls
}
connSlot := int((l.next.Add(1) - 1) % connsPerPeer)
base := pool[connSlot]
```

> `Add` 返回自增**后**的值，减 1 使首个请求落在 slot 0，日志索引 0-based。

**e. import** 新增 `sync/atomic`。

### 3.3 `accesslog.go`

新增 `appendKVInt` helper，`LogHTTP` 签名加 `connSlot int` 参数，
JSON 行在 `conn` 之后插入 `"conn_slot":N`（无引号整数）。
预分配 `288 → 304`。

### 3.4 建连时机：懒建连

Go 的 `http2.Transport` 懒建连。两个 slot 各自在**首个请求到达时**拨号。
轮询从第 1 个请求就开始，因此头两个请求会分别触发两次拨号，之后稳定为 2 条。

**不做启动预热**：注册洪水的头几个请求毫秒内就把连接建满；
预热需要一个无副作用的 SBI 端点，各 NF 不统一，成本高于收益。
若要排除冷启动影响，丢弃实验前几十毫秒的数据即可。

---

## 4. 请求/响应配对保证（问题 3）

**绝对不会出现「req 走连接 1、resp 从连接 2 回来」。** 三重保证：

**① 协议层**：HTTP/2 的 stream ID 作用域是单条连接内部。连接 1 的 stream 5
与连接 2 的 stream 5 毫无关系，server 只能在**收到请求的那条连接**上回复。

**② Go 实现层**：响应投递路径是 `cc.readLoop → cc.streams[id] → cs.resc`，
全程被 `cc`（单个连接对象）闭包住。连接 2 的读循环访问不到连接 1 的 `streams` map。

**③ 本实现**：`base.RoundTrip(req)` 是同步调用，`resp` 由被选中的 transport
直接返回，不存在跨实例的路由或汇聚。

> **边界情况**（不是错配）：连接层错误时 h2 会对幂等请求自动重试，
> 可能落到新连接上。那是一次全新的请求-响应对，仍在同一条新连接内配对。
> 现有代码用 `if wroteTime.IsZero()` 保留首次写出时间来处理这种情况。

---

## 5. 如何从日志验证 round-robin 效果（问题 4）

日志现在有**三个**相关字段：

| 字段 | 含义 |
|---|---|
| `conn_slot` | **本次新增**。请求被分配到哪个轮询槽（0 或 1） |
| `conn` | 该槽当时持有的实际 socket（`localIP:localPort`） |
| `conn_reused` | 是复用还是新建 |

### 5.1 验证轮询是否均衡

```bash
grep '"src":"UDM"' HTTP_log.txt | grep '"dst":"UDR"' \
  | grep -o '"conn_slot":[0-9]*' | sort | uniq -c
```

**期望：两个槽计数接近 1:1**（轮询按请求数强制均分，误差 ≤1）。

对比改造前实测的 86.4% : 13.6%，这是最直接的效果证据。

### 5.2 验证确实开了 2 条连接

```bash
grep '"dst":"UDR"' HTTP_log.txt | grep -o '"conn":"[^"]*"' | sort -u | wc -l
```

**期望：2**（若某槽的连接中途死亡重建，会 > 2，属正常，见第 6 节）。

### 5.3 验证槽与连接的对应关系

```bash
grep '"dst":"UDR"' HTTP_log.txt \
  | grep -o '"conn":"[^"]*","conn_slot":[0-9]*' | sort | uniq -c
```

**期望：每个 slot 对应一个 conn**。若某 slot 对应多个 conn，
说明该槽的连接被重建过——这正是 `conn_slot` 与 `conn` 分开记录的价值：
**`conn_slot` 反映轮询是否均衡，`conn` 反映实际 socket 生命周期。**

### 5.4 验证负载是否真的并行

仅看请求数均衡还不够——改造前 RQ1000 曾出现「两条连接总数接近但实际是时间上接力」
的假象。用并发 stream 的时间序列确认两个槽**同时**在工作：

`cloudlab/Ty_log/Free5gc/C6525100g_HTTPconnum_0806/analyze_http_conns.py`
的第 6 张图（按时间分箱的占比堆叠图）可直接看出是并行还是接力。

---

## 6. 连接断开会自动重建吗（问题 5）

**会。** 分两个层面：

### 6.1 重建是 Go 的默认行为

`http2.Transport` 在连接失效（GOAWAY、读写错误、PING 超时）后，
下一个请求会自动拨号补上。这是默认行为，**本改造没有做任何抑制**
（`StrictMaxConcurrentStreams` 已按 2.1 移除）。

已查证 `http2.Transport` 的连接池相关字段只有
`ConnPool` / `StrictMaxConcurrentStreams` / `IdleConnTimeout` 三个，
**没有** `MaxConnsPerHost`（那是 `net/http.Transport` 的字段），
所以也没有任何设置会阻止重建。

### 6.2 每个槽独立重建

两个 slot 是独立的 `http2.Transport`，**各自维护自己的连接池**。
slot 0 的连接死亡不影响 slot 1，slot 0 会自己拨号补上，
轮询逻辑完全不受影响（它只按索引取 transport，不关心底层 socket 状态）。

### 6.3 实测佐证

改造前 RQ1500 的数据显示重建确实会自动发生且无缝：

```
主连接最后一个响应 @813.6ms
后继连接首个请求   @816.4ms   ← 间隔 2.8ms，之后 563 个请求全部由它承担
```

> 注意：这段数据有**两层**含义。它证明了「重建是自动且无缝的」（本节主题），
> 但那次断开本身是 `PingTimeout=1s` 的**误杀**，并非真实故障——
> 这正是 2.3 节把 `PingTimeout` 提到 3s 的直接依据。

### 6.4 因此连接总数仍可能 > 2

即使 `PingTimeout` 提到 3s，连接总数**依然不是**严格的 2 条，原因有三：

1. **溢出建连**：移除 Strict 后，任一 transport 的 in-flight stream 撞到对端
   250 上限时会自行再拨号（2.2 节）。实测单 transport 峰值达 371 streams，
   所以这条路径在高 RQ 下**必然**触发。
2. **健康检查重建**：3s 只是把误杀**变少**，没有消除。极端拥塞下仍可能发生。
3. **冷启动**：懒建连，头两个请求之前连接数是 0 → 1 → 2（3.4 节）。

因此准确表述是：**轮询槽恒为 2 个，实际 socket 数是「2 条起步、浮动 ≥2」**。

判读时用 5.3 区分：**同一 slot 出现多个 conn = 该槽重建过**，属正常。
**轮询是否均衡看 `conn_slot`，实际 socket 数看 `conn`**——两者分开记录
正是为了应对这种情况。

---

## 7. 风险

| 风险 | 评估 | 处理 |
|---|---|---|
| 延迟未改善 | 有可能。若瓶颈不在连接层（而在 Go 调度、CPU、对端处理），2 条连接无收益 | 这本身是有价值的负面结论 |
| 连接数 > 2 | **预期会发生**。移除 Strict 后仍有溢出建连 + 健康检查重建 | 用 5.3 区分；轮询均衡看 `conn_slot` 而非 `conn` |
| 两槽的连接寿命不同 | 某槽连接死亡重建时，该槽短暂无连接 | 轮询仍按索引分派，Go 会自动补建，不影响正确性 |
| 两槽负载不均 | 轮询按**请求数**均衡，非按**耗时**。慢请求集中在一槽时仍可能不均 | 用 5.1 + 5.4 观察 |
| 漏改某 NF | 改了 `LogHTTP` 签名，漏改会**编译报错** | 编译器兜底 |
| `srcNF` 被 sed 写坏 | **不报编译错**，是真实风险 | 已验证 7 个全对 |
| **`PingTimeout` 引入第二个变量** | 对照组跑 1s、实验组跑 3s，连接数与 ping 超时同时变了 | 理想做法：对照组用 3s 重跑。否则结论中必须注明（2.3 节末） |
| **3s 掩盖真实故障** | 真连接故障的发现从 1s 延后到 3s | 仍远快于 Go 默认 15s；实验时长为分钟级，影响可忽略 |

---

## 8. 验证清单

- [x] 7 份 `httptransport.go` 改动后仍完全一致（`diff -q`）
- [x] 每个 NF：`connsPerPeer=2`、轮询、`connSlot` 传参
- [x] 每个 NF：`StrictMaxConcurrentStreams` **计数为 0**（已移除）
- [x] 每个 NF：`pingTimeoutPeriod = 3 * time.Second`（7/7 已确认）
- [x] 每个 NF：`appendKVInt`、`LogHTTP` 新签名
- [x] 7 个 `srcNF` 正确（AMF/AUSF/UDM/UDR/PCF/NRF/NSSF）
- [x] AMF 的 `LogWorker`/`SBIView` 未被覆盖，且未泄漏到其余 6 个
- [ ] **7 个 NF `go build ./...` 通过**（本机无 Go 工具链，需在 node-0 执行）
- [ ] 重建镜像并部署
- [ ] 跑 RQ1000 / 1500 / 2000
- [ ] 5.1：`conn_slot` 分布接近 1:1
- [ ] 5.2：`conn` 去重计数 = 2（或 >2 但可由 5.3 解释为重建）
- [ ] 5.3：每个 slot 对应的 conn 数量合理
- [ ] 5.4：确认两槽**并行**工作而非时间接力
- [ ] **连接重建次数显著下降**（3s 生效的直接证据）：
      同一 slot 对应的 `conn` 去重数应明显少于 1s 那几次实验
- [ ] **各 NF `dropped` 计数为 0**——否则 `conn_slot` 分布统计本身有采样偏差
      （每行新增 ~15 字节，高 RQ 下写盘压力上升）
- [ ] 与对照组（`0848dba`）对比 t2→t4 延迟，并在结论中注明
      对照组是 **1 条连接 + ping 1s**、实验组是 **2 槽 + ping 3s**（两个变量）

---

## 9. 不在覆盖范围内的 HTTP 路径

以下路径不走 `accesslog.Client()`，因此**既不受本改造影响，也不记日志**：

- `NFs/pcf/internal/sbi/consumer/bsf_service.go:30` — 裸 `&http.Client{}`（PCF→BSF）
- `NFs/smf/internal/sbi/consumer/bsf_service.go:28` — 裸 `&http.Client{}`（SMF→BSF）
- `NFs/nef/internal/sbi/consumer/*.go` — `http.DefaultClient`（4 处）
- SMF 出向：SMF 无 `internal/accesslog/` 目录，未接入

用户关注的 **AMF / AUSF / UDM / UDR / PCF** 之间的链路**全部覆盖**。
（AMF→SMF 方向走 accesslog，会被改造；SMF 出向不会。）
