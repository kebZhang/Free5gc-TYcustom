# NF↔NF HTTP/2 连接数 8 → 16 修改计划 (0809v1)

> **状态：计划待实施**（基于 `ef8cef9` "add NF-NF HTTP conn to 8 -- 0809 6PM"）。
>
> 本文档**重新核查了当前代码**（不是依据记忆或既往计划），并且**风险结论全部来自
> 你自己的实测数据**——包括刚跑完的 8 连接 run
> `cloudlab/Ty_log/Free5gc/R6525_NF8HTTP_idle500ms_0809/`。原始数字见第 3、4 节。
>
> **先说结论**：改动本身是 **7 个文件各 1 行**，零风险、行为可精确预测。
> 但 **16 是否值得跑，数据给出的答案是"边际收益在缩小，但尚未消失"**——
> 第 4 节给出 1→4→8 的完整趋势，以及 16 的预期收益区间。

---

## 0. TL;DR

| 项 | 结论 |
|---|---|
| 改什么 | `connsPerPeer`：**8 → 16** |
| 改哪里 | `NFs/<nf>/internal/accesslog/httptransport.go` 第 **84** 行 × **7 个 NF** |
| 改动量 | **7 个文件，每个 1 行常量 + 1 段注释**。无结构性改动 |
| 轮询要改吗 | ❌ 不用。`% connsPerPeer` 自动适配 16 |
| 日志要改吗 | ❌ 不用。`conn_slot` 字段、`LogHTTP` 签名全部已就位 |
| 分析脚本要改吗 | ❌ 不用。slot 列表由数据推导，无硬编码 |
| 行为可预测吗 | ✅ **可以**。8 连接 run 的实测与 4 连接 run 的预测**完全吻合**（见 3.1） |
| 预期结果 | 每对 NF **恰好 16 条连接、16 次拨号、0 次重拨** |
| 唯一实质风险 | **每槽样本量降到 62**（AMF→PCF / PCF→UDR），尾延迟统计开始不可靠 |
| 值不值得做 | ⚠️ **边际收益递减但未归零**：1→4 省 128ms，4→8 省 41ms（RQ2500）。见第 4 节 |

---

## 1. 当前代码现状（重新核查）

### 1.1 核查命令与结果

```bash
cd Free5gc-TYcustom
git log --oneline -3
#   ef8cef9 add NF-NF HTTP conn to 8 -- 0809 6PM      ← 当前 HEAD
#   e5b47ba NF HTTP 4 conn at start - 0806v4
#   d2f0cfc Fix HTTP idletime=1ms to idltime=500ms -- 0806v3

grep -n "const connsPerPeer" NFs/*/internal/accesslog/httptransport.go
#   7 个 NF 全部为 :84: const connsPerPeer = 8

md5sum NFs/*/internal/accesslog/httptransport.go | awk '{print $1}' | sort -u
#   392e5836e80e4c4164610cc424e1a368     （唯一值 → 7 份字节一致）

git status --short
#   （空，工作区干净）
```

### 1.2 逐项现状

| 项 | 现状 | 位置 | 本次是否改 |
|---|---|---|---|
| `connsPerPeer` | **8** | `httptransport.go:84` | ✅ **改为 16** |
| 常量上方注释 | 论证"为什么是 8"，含 `8 is a floor, not a cap` | `:50-83` | ✅ **重写** |
| 定长数组 | `tls`/`clear` 各 `[connsPerPeer]http.RoundTripper` | `:94-95` | ❌ 自动变 `[16]` |
| 构造循环 | `for i := 0; i < connsPerPeer; i++` | `:106` | ❌ 自动循环 16 次 |
| 轮询游标 | `next atomic.Uint64`，两 scheme 共用 | `:101` | ❌ 无需改 |
| 轮询选路 | `int((l.next.Add(1) - 1) % connsPerPeer)` | `:159` | ❌ `% 16` 自动成立 |
| `StrictMaxConcurrentStreams` | **未设**（grep 命中处全在注释 `:110-126`） | — | ❌ 保持不设 |
| `MaxConcurrentStreams`（客户端） | **从未设置** | — | ❌ 保持 |
| `ReadIdleTimeout` / `PingTimeout` | 1s / **3s** | `:45-46` | ❌ 保持 |
| 客户端 `Timeout` | 10s | `:47` | ❌ 保持 |
| 服务端 `IdleTimeout` | **500ms** | 10 个 `internal/sbi/server.go` | ❌ 保持 |
| `conn_slot` 日志字段 | 已就位 | `accesslog.go:367` | ❌ 无需改 |
| `sharedClient` | **进程级单例** | `:402` 附近 | ❌ 无需改 |

### 1.3 关键架构事实（决定第 5 节的连接数估算）

`sharedClient` 是**包级单例**，`loggingRoundTripper` 持有
`[connsPerPeer]http.RoundTripper`。**这些 slot 是全进程共享的，与对端无关**；
每个 `http2.Transport` 自己按 `host:port` 维护连接池。因此：

```
单 NF 进程持有的连接数 = connsPerPeer × 该进程实际访问过的对端数
```

即"每一对 NF 之间 N 条"，`connsPerPeer` 命名准确；但**进程总连接数是 N × 对端数**。

---

## 2. 改动内容

### 2.1 范围

| 项 | 内容 |
|---|---|
| 文件 | `NFs/<nf>/internal/accesslog/httptransport.go` × **7** |
| NF | `amf` `ausf` `nrf` `nssf` `pcf` `udm` `udr` |
| 每文件 | 第 **84** 行常量 + 第 **50-83** 行注释块 |

> 只有这 7 个 NF 有 `internal/accesslog/` 目录。
> `smf` `chf` `nef` `bsf` `upf` `n3iwf` `tngf` 未接入该客户端。
> 特别注意 **`nef` 用的是 `http.DefaultClient`**（`nef/internal/sbi/consumer/*.go`
> 与 `notifier/pfd_notifier.go`），其流量不受本改动影响，也不进 `HTTP_log.txt`。

### 2.2 常量改动

```go
const connsPerPeer = 8      // 改为
const connsPerPeer = 16
```

### 2.3 注释改动

当前 `:50-83` 的注释整段在论证"为什么是 8"，含这些与新值直接矛盾的句子：

- `... round-robin slots requests are dealt across. It is 8.`
- `A NF with 6 peers therefore holds 8*6 = 48 connections.`
- `8 is this step (HTTP_8CONN_PLAN_0809.md).`
- `The 4-slot run it doubles held exactly 4 sockets per pair ... drops from ~250 to ~125 per slot.`
- `Growth beyond these 8 is still permitted ... 8 is a floor, not a cap`

**必须整段重写**，否则注释会与代码直接冲突。建议替换为：

```go
// connsPerPeer is how many HTTP/2 connections this NF opens to each peer NF up
// front, and how many round-robin slots requests are dealt across. It is 16.
//
// Each slot is a separate http2.Transport with its own private pool, so N slots
// mean N connections held from the start, with requests handed to them one after
// another in turn. The slots are per PROCESS, not per peer: these same N
// transports serve every peer this NF talks to, and each one keeps its own pool
// keyed by address. A NF with 4 peers therefore holds 16*4 = 64 connections.
//
// The history matters for reading this number. It was 2 for the original
// round-robin experiment (HTTP_MULTI_CONN_ROUNDROBIN_PLAN_0806.md), then went
// back to 1 (HTTP2_IDLETIMEOUT_FIX_PLAN_0807.md) once that comparison turned out
// never to have run as designed: the server's 1ms IdleTimeout tore every
// connection down between requests, so each slot handed out a freshly dialled
// socket every time instead of holding one. With the server-side IdleTimeout now
// at 500ms, connections survive the gaps between requests, so N slots finally
// mean N concurrent long-lived sockets. From that fixed baseline of 1 the
// measured series is 4 (HTTP_4CONN_ROUNDROBIN_PLAN_0807.md), 8
// (HTTP_8CONN_PLAN_0809.md), and 16 here (HTTP_16CONN_PLAN_0809v1.md).
//
// The returns are diminishing and that is the point of measuring 16. On median
// end-to-end registration at RQ2500/UE1000, 1->4 saved 126ms and 4->8 saved a
// further 41ms; each doubling has bought less than the one before it, so 16 is
// the step that shows whether the curve has flattened or still has slope.
//
// 16 also halves the per-slot sample again. The thinnest pairs measured,
// AMF->PCF and PCF->UDR at 1000 requests, fall to ~62 per slot. That is still
// enough to see whether the round-robin split is even, but too thin to read a
// per-slot P95/P99 from: tail statistics should come from AMF->UDM (~375) and
// UDM->UDR (~562) instead. A materially lower request rate or UE count would
// make even the split unreadable at 16 slots.
//
// Growth beyond these 16 is still permitted: StrictMaxConcurrentStreams is
// deliberately left unset (see below), so when a slot's in-flight streams reach
// the peer's 250-stream limit the transport dials an additional connection by
// itself. In practice that has never triggered -- every run from 4 to 8 held
// exactly connsPerPeer sockets per pair with zero redials -- so 16 is expected
// to be the actual count, not merely a floor.
const connsPerPeer = 16
```

### 2.4 一并更新 `newLoggingRoundTripper` 内的注释（可选但建议）

`:124-126` 有一句：

```go
// connsPerPeer sets how many connections are held from the start, so
// that the number in use can be compared against the single-connection
// baseline; it is not meant to cap the total.
```

这句仍然成立，**无需改**。

---

## 3. 实测基线：8 连接 run 的真实数字

数据源：`cloudlab/Ty_log/Free5gc/R6525_NF8HTTP_idle500ms_0809/HTTP_log_RQ*_UE1000.txt`
（客户端视角记录，即 `src != "NaN"` 且带 `conn` 字段的行）。

### 3.1 连接数与拨号次数：**预测被完全验证**

**RQ800 / 1000 / 1500 / 2000 / 2500 五个速率，结果完全一致**：

| NF 对 | distinct conns | new dials | 重拨次数 |
|---|---|---|---|
| AMF→AUSF | **8** | **8** | **0** |
| AMF→PCF | **8** | **8** | **0** |
| AMF→UDM | **8** | **8** | **0** |
| AUSF→UDM | **8** | **8** | **0** |
| PCF→UDR | **8** | **8** | **0** |
| UDM→UDR | **8** | **8** | **0** |

**这一条是本计划最重要的依据。** `HTTP_8CONN_PLAN_0809.md` 当初根据 4 连接 run
预测"8 连接下每对恰好 8 条、8 次拨号、0 次重拨"——**实测一字不差**。

因此对 16 的预测有了经过验证的外推基础：

> **预测：16 连接下每对恰好 16 条、16 次拨号、0 次重拨。**

三个支撑：

1. **从未扩容**。`conns == connsPerPeer` 在 4 和 8 两个值上都成立，说明即使
   RQ2500 下单 slot 的 in-flight stream 也远够不到对端 250 上限。
   16 条只会让每 slot 的 stream 更少 → **更不可能扩容**。
2. **从未重拨**。`dials == conns` 说明连接建立后活到窗口结束，
   没有被 PING 判死或被 `IdleTimeout` 收掉。
3. **与负载无关**。五个速率结果完全相同 → 连接数纯由常量决定。

### 3.2 每槽负载分布（8 槽实测 → 16 槽外推）

以 RQ2500 为例（其余速率几乎相同）：

| NF 对 | 总请求 | 8 槽实测分布 | **16 槽后每槽约** |
|---|---|---|---|
| AMF→AUSF | 2000 | 241/253/249/230/255/255/273/244 | **125** |
| **AMF→PCF** | **1000** | 125/126/127/136/132/112/121/121 | **62** ← 最稀 |
| AMF→UDM | 6000 | 759/746/749/759/738/758/731/760 | **375** |
| AUSF→UDM | 2000 | 250 × 8（完全均匀） | **125** |
| **PCF→UDR** | **1000** | 125 × 8（完全均匀） | **62** ← 最稀 |
| UDM→UDR | 9000 | 1125 × 8（完全均匀） | **562** |

> 注意 AUSF→UDM / PCF→UDR / UDM→UDR 三对是**完全均匀**（每槽数值相同），
> 而 AMF→AUSF / AMF→PCF / AMF→UDM 有 ±5% 波动。这不是 bug：
> AMF 的请求由多个 UE 处理 goroutine 并发发起，原子游标的取值顺序与
> goroutine 调度交织；而 AUSF/PCF/UDM 的下游调用路径更接近串行。

---

## 4. **16 值得做吗**：1→4→8 的实测趋势

这是本计划真正需要你决策的部分。改动本身没有风险，问题是**收益还剩多少**。

### 4.1 端到端注册延迟（t2_gnb_send_amf → t4_complete，中位数）

数据源：三个 run 的 `latency_RQ*_UE1000.txt`，各 1000 个 UE。

| RQ | 1 conn | 4 conn | 8 conn | 1→4 收益 | **4→8 收益** |
|---|---|---|---|---|---|
| 800 | 28.9 ms | 28.0 | 27.2 | 0.9 ms | **0.8 ms** |
| 1000 | 38.1 | 30.5 | 29.0 | 7.5 | **1.5** |
| 1500 | 176.1 | 47.7 | 35.7 | **128.4** | **12.0** |
| 2000 | 222.9 | 91.2 | 77.0 | **131.7** | **14.2** |
| 2500 | 281.2 | 155.7 | 114.8 | **125.5** | **40.9** |

### 4.2 HTTP 单请求延迟（客户端视角，全部 NF 对）

| RQ | 4 conn p50/p95/p99 (ms) | 8 conn p50/p95/p99 (ms) |
|---|---|---|
| 800 | 1.53 / 6.71 / 9.74 | 1.47 / 6.82 / 10.33 |
| 1000 | 1.54 / 7.72 / 12.53 | 1.52 / 7.16 / 11.83 |
| 1500 | 2.17 / 10.92 / 17.62 | **1.84 / 7.87 / 12.69** |
| 2000 | 4.17 / 18.11 / 26.31 | **3.53 / 15.45 / 24.15** |
| 2500 | 5.65 / 26.12 / 40.18 | **4.39 / 19.57 / 30.62** |

### 4.3 怎么读这两张表

**三个事实**：

1. **低 RQ（800/1000）下多连接几乎无用。** 1→4→8 全程只差 1~2ms。
   合理：低负载下单连接的写锁和读循环根本不是瓶颈。
2. **1→4 是决定性的一步。** RQ1500 从 176ms 降到 48ms（省 128ms，-73%）。
   这一步把"单连接串行"这个主瓶颈拆掉了。
3. **4→8 收益小得多，但在高 RQ 下仍然显著且在增长**：
   RQ1500 省 12ms，RQ2000 省 14ms，**RQ2500 省 41ms（-26%）**。
   **注意 4→8 的收益随 RQ 单调上升**，说明在 RQ2500 时 8 条连接**仍未饱和**。

**对 16 的判断**：

| 观点 | 依据 |
|---|---|
| ✅ **值得做** | 4→8 的收益在 RQ2500 达到 41ms 且**仍在随 RQ 增长**，说明 8 条尚未把并发瓶颈拆完。若曲线还有斜率，16 应该还有收益 |
| ⚠️ **收益大概率有限** | 每次翻倍的收益在递减（128 → 41）。按此趋势，**8→16 在 RQ2500 的预期收益约 10~20ms**，比 4→8 更小 |
| ⚠️ **低 RQ 下必然无收益** | RQ800/1000 已经贴地（27~29ms），16 不可能再降 |

**结论：值得跑一次，但应把它当作"确认曲线是否已经拉平"的收尾实验，
而不是期待又一次大幅改善。** 且**重点看 RQ2000/2500**，低 RQ 的数据只用于确认无回退。

> **若 16 的结果相比 8 只有个位数 ms 差异**，那就说明连接数这条路已经走到头，
> 瓶颈转移到了别处（AMF 内部 goroutine 排队 / MongoDB / SCTP），
> 应该停止加连接，转去测别的维度。**这本身就是有价值的结论。**

---

## 5. 风险评估

| # | 风险 | 严重度 | 依据 |
|---|---|---|---|
| R1 | **每槽样本量降到 62** | ⚠️ **唯一实质风险** | 见 5.1 |
| R2 | 漏改某个 NF 不报错 | ⚠️ 高危但可兜住 | 见 5.2 |
| R3 | 连接数 / FD / 内存 | ✅ 可忽略 | 见 5.3 |
| R4 | 连接 churn 加剧 | ✅ **实测排除** | 3.1：4 和 8 都是 0 次重拨 |
| R5 | 自行扩容超过 16 | ✅ **实测排除** | 3.1：`conns == connsPerPeer` 恒成立 |
| R6 | HTTP/2 多路复用被削弱 | ⚠️ 是实验自变量，非缺陷 | 见 5.4 |
| R7 | 日志量增加 | ✅ 无影响 | `LogHTTP` 每请求一行，与连接数无关 |
| R8 | 与历史 run 可比性 | ⚠️ 注意读百分比 | 见 5.5 |

### 5.1 R1：每槽样本量（**唯一实质风险**）

16 槽下每槽只拿 1/16 流量。从 3.2 的实测外推：

| NF 对 | 16 槽后每槽 | 能做什么分析 |
|---|---|---|
| UDM→UDR | ~562 | ✅ 全部，含 P95/P99 |
| AMF→UDM | ~375 | ✅ 全部，含 P95/P99 |
| AMF→AUSF / AUSF→UDM | ~125 | ✅ 分布均匀性、中位数；⚠️ P99 勉强 |
| **AMF→PCF / PCF→UDR** | **~62** | ✅ 只能看分布均匀性；❌ **P95/P99 不可用** |

62 个样本的 P99 由**不到 1 个点**决定，完全是噪声。

**建议**：
- 尾延迟分析**只用 AMF→UDM 和 UDM→UDR**
- AMF→PCF / PCF→UDR 只用于确认 round-robin 分布均匀（每槽应 ~62±少量）
- **不要在这次 run 上跑低 RQ**。若以后要跑 RQ5/UE10 那种（每对总共 84 个请求），
  16 槽下每槽 5 个，图表将完全不可读——届时必须改回小值

### 5.2 R2：漏改某个 NF 不会编译报错

改的是常量**值**不是签名。漏掉一个 NF 照样编译、照样跑，只是它悄悄还用 8 条，
**而数据看起来完全正常**。

**唯一兜底**（改完立即执行）：

```bash
md5sum NFs/*/internal/accesslog/httptransport.go | awk '{print $1}' | sort -u | wc -l
# 必须输出 1

grep -c "connsPerPeer = 16" NFs/*/internal/accesslog/httptransport.go
# 7 行必须全部为 1
```

### 5.3 R3：连接数、FD、内存

按 1.3 的公式和 3.1 实测的对端关系：

| NF | 对端数（含 NRF） | 8 连接时 | **16 连接时** |
|---|---|---|---|
| AMF | ~4（AUSF/PCF/UDM/NRF） | 32 | **64** |
| AUSF | ~2（UDM/NRF） | 16 | **32** |
| UDM | ~2（UDR/NRF） | 16 | **32** |
| PCF | ~2（UDR/NRF） | 16 | **32** |
| **UDR（服务端侧）** | 被 UDM + PCF 呼叫 | 入 16 | **入 32** |

**✅ 可忽略。** 64 条对比 Linux 默认 `ulimit -n = 1024` 仍差一个数量级以上。
每条 HTTP/2 连接固有开销主要是收发缓冲（默认几十 KB 量级），64 条约 MB 级。

> 唯一值得确认的：pod 内 `ulimit -n` 是否被设成异常低的值。
> 到 64 这个量级仍然安全，但这是最后一次"可忽略"——**如果以后还要翻倍到 32/64，
> 就必须真正评估 FD 与内存了**。

### 5.4 R6：多连接削弱 HTTP/2 多路复用

16 条连接 = 16 个独立拥塞窗口 + 16 个独立 HPACK 动态表（头部压缩率下降，
因为动态表不共享）+ 16 个独立 flow-control 窗口。

**这在协议设计上是反模式，但正是本实验的自变量**：目的就是把单连接的
写锁（`clientConn.wmu`）与单个读循环这个串行点拆开。
解读结果时记得：收益来自"拆串行"，代价是"压缩率与拥塞窗口效率下降",
测出来的就是两者净效果。

> **在 16 这个量级，代价侧开始变得不可忽略**：HPACK 动态表分散到 16 份，
> 每份看到的头部重复模式更少，压缩率下降。这可能正是收益曲线拉平的原因之一。

### 5.5 R8：与历史 run 的可比性

`analyze_http_conns.py` 从数据推导 slot 列表（`sorted(self.per_slot)`），
**不会报错**。但跨 run 对比注意：

- 图 2（slot balance）从 8 根柱变 16 根柱，**柱高不可直接比**（每槽天然只有一半）。
  比就比**百分比**（脚本的 `slot_balance()` 已输出百分比）
- 图 1（connections per pair）从 8 跳到 16，是预期的
- 脚本里 `show_labels = max_conns <= 6`，**16 条连接时柱上数字会自动关闭**，
  这是脚本已有的保护，不是 bug
- **不要覆盖 `R6525_NF8HTTP_idle500ms_0809`**，它是本次的对照组。
  新 run 建议命名 `R6525_NF16HTTP_idle500ms_0809v1`

---

## 6. 实施步骤

### 6.1 改代码（7 个文件同一处）

可用脚本一次性替换，避免手工漏改：

```bash
cd Free5gc-TYcustom
for nf in amf ausf nrf nssf pcf udm udr; do
  f="NFs/$nf/internal/accesslog/httptransport.go"
  sed -i 's/^const connsPerPeer = 8$/const connsPerPeer = 16/' "$f"
done
```

> ⚠️ 这条 `sed` **只改常量，不改注释**。注释块（2.3 节）必须另外替换，
> 否则会留下 `It is 8.` / `8 is a floor` 等与代码矛盾的句子。
> 建议直接用 2.3 节的整段替换 `:50-83`。

### 6.2 验证一致性（**必做**）

```bash
md5sum NFs/*/internal/accesslog/httptransport.go | awk '{print $1}' | sort -u | wc -l   # 必须 1
grep -c "connsPerPeer = 16" NFs/*/internal/accesslog/httptransport.go                    # 7 行全为 1
```

### 6.3 编译（**必须在有 Go 的机器上做**）

```bash
for nf in amf ausf nrf nssf pcf udm udr; do
  (cd NFs/$nf && go build ./...) && echo "OK $nf" || echo "FAIL $nf"
done
```

> Windows 开发机上没有 Go 工具链（`go` 不在 PATH），
> 所以这一步只能在 cloudlab 节点上执行。

### 6.4 重建镜像

按 `cloudlab/K8s/UPDATE_free5gc_custom_image.md`。
**注意该文档记录的 3 个坑**（NRF 重启顺序、sequenceNumber 格式、下游 NF 注册），
否则 UE 注册会失败。

### 6.5 跑实验

沿用与 8 连接 run 相同的参数（RQ800/1000/1500/2000/2500，UE1000），
否则无法与 `R6525_NF8HTTP_idle500ms_0809` 对比。

---

## 7. 结果判读

### 7.1 先确认改动生效

```bash
python - <<'PY'
import json, collections, glob
for f in sorted(glob.glob("HTTP_log_RQ*_UE*.txt")):
    conn = collections.defaultdict(set)
    slot = collections.defaultdict(collections.Counter)
    dials = collections.Counter()
    for line in open(f, errors="ignore"):
        try: r = json.loads(line)
        except: continue
        if r.get("src") == "NaN" or not r.get("conn"): continue
        k = (r["src"], r["dst"])
        conn[k].add(r["conn"]); slot[k][r.get("conn_slot")] += 1
        if r.get("conn_reused") is False: dials[k] += 1
    print("==", f)
    for k in sorted(conn):
        c = slot[k]
        print(f"   {k} conns={len(conn[k])} dials={dials[k]} "
              f"per_slot={[c[i] for i in range(16)]}")
PY
```

| 观察 | 含义 |
|---|---|
| `conns == 16` 且 `dials == 16` | ✅ 完全符合预期（与 4/8 两次 run 的行为一致） |
| `conns == 8` | ❌ **该 NF 漏改了**，或跑的是旧镜像 |
| `per_slot` 里有槽为 0 | ❌ 同上 |
| `conns > 16` 且 `dials > 16` | ⚠️ 发生重拨/扩容。`dials - 16` 即重拨次数。**这将是首次出现，需单独调查** |
| `per_slot` 偏差 > 10% | ⚠️ 异常（AMF 发起的三对有 ±5% 属正常，见 3.2 注） |

### 7.2 再判断收益（**这才是实验目的**）

把第 4 节的表补上 16 列。**关键是看 RQ2500 的 4→8→16 三个数**：

| 结果形态 | 结论 |
|---|---|
| 8→16 收益 **> 20ms** | 曲线仍有斜率，连接数还没到头，可考虑 32 |
| 8→16 收益 **5~20ms** | 收益递减符合预期，**16 大致是合理终点** |
| 8→16 收益 **< 5ms 或为负** | ✅ **曲线已拉平**。瓶颈已不在 HTTP 连接数，应转向 AMF 内部排队 / MongoDB / SCTP。**这是个有价值的阴性结果，不是失败** |

---

## 8. 回滚

单个 commit，`git revert` 即可，或把 7 个文件的 `connsPerPeer` 改回 8
（注释块一并还原）。无数据迁移、无配置依赖、无 helm values 变更。

---

## 9. 关于"无论连接有多少，都是 16 个"

你的原话包含"硬上限"的意思。**这一点无需额外改动，因为实测已经证明它自动成立**：

| 需求 | 现状 | 是否需要改 |
|---|---|---|
| "起始就是 16 个" | ⚠️ 严格说是**懒建连**：16 个 `http2.Transport` 对象在进程启动时就有，但 TCP 连接要等第一个请求路由到该 slot 才拨号 | ❌ 不改。影响面仅前 16 个请求，且分析脚本按 t2~t4 开窗已过滤启动流量 |
| "**无论多少都是** 16 个"（不多不少） | ✅ **实测已成立**：4 连接 run 和 8 连接 run 在五个速率下 `conns` 恒等于 `connsPerPeer`，从未扩容、从未重拨 | ❌ 不改 |

**特别说明为什么不加 `StrictMaxConcurrentStreams = true`**：

它是唯一能"硬性"钉死连接数的开关，但：

1. **它要治的问题不存在**。3.1 实测证明扩容从未发生——单 slot 的 in-flight
   stream 远够不到 250 上限，16 条只会让每 slot 更空闲。
2. **它有实测的负面结论**。`0806v1` 试过（见 `httptransport.go:110-120` 的注释
   与 `Ty_log/Free5gc/C6525100g_NFHTTPonly1conn_0806v1`）：请求阻塞在
   `RoundTrip` 里 → 连接无帧流动 → PING 健康检查判死 → 拆除重建。
   结果 churn 出 2~7 条只活 25~120ms 的短命连接，**连接数不但没钉死反而更乱**。
3. **它会污染实验**。`Strict=true` 在客户端引入一个新的排队点（stream 满时阻塞），
   测出的延迟变化将无法归因于"16 条连接"本身。

**所以：什么都不用做，16 就已经是精确的 16 条。**
