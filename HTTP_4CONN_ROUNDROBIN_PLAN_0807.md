# NF↔NF HTTP/2 连接数 1 → 4 + Round-Robin 负载均衡 修改计划 (0807)

> **状态：计划待实施**（基于 `d2f0cfc` 之上）。
> 本计划把客户端 `connsPerPeer` 由 **1 改为 4**，7 个 NF 各改 1 个常量 + 1 段注释。
>
> **前置依赖**：本计划成立的前提是 `HTTP2_IDLETIMEOUT_FIX_PLAN_0807.md`
> 已把服务端 `IdleTimeout` 由 1ms 提到 **500ms**（commit `d2f0cfc`）。
> 若那一步未生效，本计划的 4 条连接会重蹈 `0806v2` 的覆辙——
> 4 个 slot 每次拿到的都是刚建好的新连接，"4 条并存"从未真正成立。见第 2 节 R1 注。
>
> **本计划不改**：并发 stream 限制（`MaxConcurrentStreams` 从未设置）、
> `StrictMaxConcurrentStreams`（保持不设，以允许自行扩容）——
> 这两项正是用户要求的"不改并发 stream 限制、连接数可自行扩容"，**当前代码已满足**。

---

## 0. TL;DR

| # | 项 | 结论 |
|---|---|---|
| 1 | 改什么 | 客户端 `connsPerPeer`：**1 → 4** |
| 2 | 改哪里 | `NFs/<nf>/internal/accesslog/httptransport.go` 第 **69** 行 × **7 个 NF** |
| 3 | 改动量 | **7 个文件，每个 1 行常量 + 1 段注释**。无结构性改动 |
| 4 | 轮询要写吗 | **不用**。原子游标轮询在 `1d10253` 已建好，`% connsPerPeer` 自动适配 4 |
| 5 | 日志要改吗 | **不用**。`conn_slot` 字段、`appendKVInt`、`LogHTTP` 签名全部已就位 |
| 6 | 并发 stream 限制 | **不动**。`MaxConcurrentStreams` 从未设置过 |
| 7 | 连接能自行扩容吗 | **能，且是刻意保留的**。`StrictMaxConcurrentStreams` 未设置（默认 false） |
| 8 | 最大风险 | **漏改某个 NF 不会编译报错**（改的是常量值，非签名），只能靠 grep + 哈希兜底 |

> **术语**：下文的「slot / 槽」指**进程内的 `http2.Transport` 对象**，
> 不是 NF 的 pod / 副本。**NF 部署拓扑一个都不变。**

---

## 1. 当前状态（已逐项核实，非推测）

改动前必须确认基础设施已就位——这决定了本次是「改一个数字」还是「重写一个模块」。

### 1.1 代码现状

| 项 | 现状 | 位置 | 本次是否要改 |
|---|---|---|---|
| `connsPerPeer` | **1**（7 个 NF 全部一致） | `httptransport.go:69` | ✅ **改为 4** |
| 常量上方注释 | 论证「为什么回到 1 条」 | `httptransport.go:50-68` | ✅ **重写** |
| 定长数组 | `tls`/`clear` 各 `[connsPerPeer]http.RoundTripper` | `httptransport.go:79-80` | ❌ 自动变 `[4]` |
| 构造循环 | `for i := 0; i < connsPerPeer; i++` | `httptransport.go:91` | ❌ 自动循环 4 次 |
| 轮询游标 | `next atomic.Uint64` | `httptransport.go:86` | ❌ 无需改 |
| 轮询选路 | `int((l.next.Add(1) - 1) % connsPerPeer)` | `httptransport.go:143` | ❌ `% 4` 自动成立 |
| `conn_slot` 日志字段 | `appendKVInt(b, "conn_slot", connSlot, false)` | `accesslog.go:336` | ❌ 无需改 |
| `LogHTTP` 签名 | 已含 `connSlot int` 参数 | `accesslog.go:325` | ❌ 无需改 |
| `StrictMaxConcurrentStreams` | **未设置**（grep 命中 2 处**全在注释里**） | — | ❌ 保持不设 |
| `MaxConcurrentStreams` | **从未设置** | — | ❌ 保持不设 |
| 服务端 `IdleTimeout` | **500ms**（`d2f0cfc` 已修） | 10 个 `server.go` | ❌ 保持 |
| `PingTimeout` | **3s** | `httptransport.go:46` | ❌ 保持 |

### 1.2 7 份文件字节一致（关键前提）

```
0e74c5ced1df48ddb2048f3ee35d01be  NFs/amf/internal/accesslog/httptransport.go
0e74c5ced1df48ddb2048f3ee35d01be  NFs/ausf/internal/accesslog/httptransport.go
0e74c5ced1df48ddb2048f3ee35d01be  NFs/nrf/internal/accesslog/httptransport.go
0e74c5ced1df48ddb2048f3ee35d01be  NFs/nssf/internal/accesslog/httptransport.go
0e74c5ced1df48ddb2048f3ee35d01be  NFs/pcf/internal/accesslog/httptransport.go
0e74c5ced1df48ddb2048f3ee35d01be  NFs/udm/internal/accesslog/httptransport.go
0e74c5ced1df48ddb2048f3ee35d01be  NFs/udr/internal/accesslog/httptransport.go
```

**7 份完全一致**，所以这是**同一处改动复制 7 次**。改完必须再次比对哈希——
这也是唯一能兜住「漏改某个 NF」的手段（见第 6.1 节）。

> 只有这 7 个 NF 有 `internal/accesslog/` 目录。`smf`/`chf`/`nef`/`bsf`/`upf`
> 等未接入该客户端，不在本次范围。

### 1.3 历史沿革（三次改动的因果链）

| commit | `connsPerPeer` | 服务端 `IdleTimeout` | 实际效果 |
|---|---|---|---|
| `2fa0755` 及以前 | 无此常量（单 transport） | 1ms | 每请求一条新 socket，复用率 0% |
| `1d10253`（0806） | **2** | 1ms | **2 条从未真正并存**——每个 slot 每次都拿到刚建的新连接 |
| `d2f0cfc`（0807） | **1** | **500ms** | 连接第一次能活过请求间隔；回归单连接基线 |
| **本计划** | **4** | 500ms | 4 条**第一次**真正成为并存的长命 socket |

**这条因果链是本计划的核心依据**：`0806` 的「2 连接实验」之所以要作废，
不是因为双连接思路错，而是因为服务端 1ms 的 `IdleTimeout` 让它根本没跑起来。
`d2f0cfc` 修好那个 bug 之后，多连接实验**才第一次具备可行性**。

---

## 2. 需求

| # | 要求 | 实现方式 | 工作量 |
|---|---|---|---|
| **R1** | NF 之间默认开 **4** 条 HTTP/2 连接 | `connsPerPeer` **1 → 4**（7 个 NF） | 7 行 |
| **R2** | 4 条连接**负载均衡**处理所有请求 | 复用已有的原子游标 round-robin，`% 4` 自然成立 | **0** |
| **R3** | **不修改**并发 stream 的限制 | `MaxConcurrentStreams` 从未设置，本次也不设 | **0** |
| **R4** | HTTP 连接数**可以自己扩容** | `StrictMaxConcurrentStreams` 保持不设（默认 false） | **0** |

> **R1 为什么现在才能做**：见 1.3 节。`0806` 的双连接实验在 `IdleTimeout=1ms` 下
> **从未真正成立**——两个 slot 每次拿到的都是刚建好的新连接，因为上一条早被
> GOAWAY 掉了。`d2f0cfc` 把服务端 `IdleTimeout` 提到 500ms 之后，
> N 个 slot 才**第一次**意味着 N 条稳定并存的 socket。

> **R3 / R4 为什么零改动**：`StrictMaxConcurrentStreams` 在 `0848dba` 之后已移除
> （实测它制造大量 25~120ms 短命连接，见 `HTTP_MULTI_CONN_ROUNDROBIN_PLAN_0806.md` 2.1）。
> 保持不设正是"允许自行扩容"的前提：Go 文档明确说明
> —— *"If false, new TCP connections are created to the server as needed to keep
> each under the per-connection SETTINGS_MAX_CONCURRENT_STREAMS limit."*
> 即撞到对端 250 stream 上限时，transport 会**自行再拨号**。
> 因此 **4 是地板不是天花板**。

### 2.1 为什么取 4

| 值 | 评估 |
|---|---|
| 1（现状） | 单连接基线。所有请求串在一条 HTTP/2 连接的写锁与读循环上 |
| 2（`0806` 尝试） | 因 `IdleTimeout=1ms` 而作废，从未真正跑成 |
| **4（本次）** | 对 1 的 **4 倍**，跨度足以在延迟上产生可测差异；4 个槽的分布统计（每槽 25%）在常见 RQ 下样本量仍充足 |
| 8 / 16 | 每槽样本量被稀释，低 RQ 下 `conn_slot` 分布的统计噪声上升；服务端并存连接数 ×8~16，FD/内存开销开始需要单独评估 |

4 是「跨度够大能看出效果」与「每槽样本量够、开销可控」之间的取值。
若本次结果显示 4 仍不够，再往 8 走；届时建议先看第 9 节的参数化方案。

---

## 3. 方案选择：改常量，不改结构

有两种改法，本计划采用 A。

| | **方案 A：改常量（采用）** | 方案 B：改成 slice + 环境变量 |
|---|---|---|
| 改动 | 7 个文件 × (1 行常量 + 1 段注释) | `[connsPerPeer]T` → `[]T`，构造改 `make`，加 `os.Getenv` 解析 + 兜底，约 15 行 × 7 |
| 编译期检查 | 数组长度编译期确定 | slice 长度运行期确定，越界风险靠代码保证 |
| 换值成本 | **需重新 build 镜像** | 改 helm values 重启即可 |
| 风险 | 低（只改字面量） | 中（动了数据结构与生命周期） |
| 适用 | 验证「4 vs 1」这一个对比 | 扫描 1/2/4/8/16 找最优值 |

**采用方案 A**：当前目标是验证一个具体配置，不是参数扫描。
方案 B 保留在第 9 节，若后续确定要扫参数再实施。

---

## 4. 具体改动

### 4.1 改动范围

| 项 | 内容 |
|---|---|
| **待改文件** | `NFs/<nf>/internal/accesslog/httptransport.go` × **7** |
| **NF 列表** | `amf` `ausf` `nrf` `nssf` `pcf` `udm` `udr` |
| **每文件改动** | 第 **69** 行常量值 + 第 **50-68** 行注释块 |
| **不改** | ✅ `internal/accesslog/accesslog.go`（7 个）✅ `internal/sbi/server.go`（10 个）✅ 所有 `go.mod` ✅ `config/*.yaml` ✅ `Dockerfile.custom` ✅ consumer / service 代码 ✅ openapi 外部模块 |

**本次不新增任何 import，不改任何 `go.mod`，因此编译全程无需联网。**

### 4.2 常量改动

```go
// 改前（httptransport.go:69）
const connsPerPeer = 1
// 改后
const connsPerPeer = 4
```

### 4.3 注释块替换（httptransport.go:50-68）

现有注释整段在论证「为什么回到 1 条」（`d2f0cfc` 写的），与 `= 4` **直接矛盾**，
必须一并替换，否则代码与注释互相打架，下一个读者无从判断哪个是意图。

```go
// connsPerPeer is how many HTTP/2 connections this NF opens to each peer NF up
// front, and how many round-robin slots requests are dealt across. It is 4.
//
// Each slot is a separate http2.Transport with its own private pool, so N slots
// mean N connections held from the start, with requests handed to them one after
// another in turn.
//
// The history matters for reading this number. It was 2 for the original
// round-robin experiment (HTTP_MULTI_CONN_ROUNDROBIN_PLAN_0806.md), then went
// back to 1 (HTTP2_IDLETIMEOUT_FIX_PLAN_0807.md) once that comparison turned out
// never to have run as designed: the server's 1ms IdleTimeout tore every
// connection down between requests, so each slot handed out a freshly dialled
// socket every time instead of holding one -- measured at RQ5/UE10 as exactly
// 1.0 requests per socket, 84 requests over UDM->UDR opening 84 connections.
// With the server-side IdleTimeout now at 500ms, connections survive the gaps
// between requests, so N slots finally mean N concurrent long-lived sockets.
// 4 is what this experiment measures against that fixed baseline of 1.
//
// Growth beyond these 4 is still permitted and expected:
// StrictMaxConcurrentStreams is deliberately left unset (see below), so when a
// slot's in-flight streams reach the peer's 250-stream limit the transport dials
// an additional connection by itself. 4 is a floor, not a cap -- which is why
// conn_slot rather than conn is the field that shows whether the split is even.
const connsPerPeer = 4
```

### 4.4 为什么其余部分一行都不用动

| 代码 | 为什么自动适配 |
|---|---|
| `tls [connsPerPeer]http.RoundTripper` | 数组长度是常量表达式，自动变 `[4]` |
| `clear [connsPerPeer]http.RoundTripper` | 同上 |
| `for i := 0; i < connsPerPeer; i++` | 自动循环 4 次，建 4 个独立 `http2.Transport` |
| `connSlot := int((l.next.Add(1) - 1) % connsPerPeer)` | `% 4`，取值 0/1/2/3 循环 |
| `base := pool[connSlot]` | `connSlot ∈ [0,4)`，数组长度 4，**编译期即可保证不越界** |
| `appendKVInt(b, "conn_slot", connSlot, false)` | `connSlot` 是 `int`，写 0~3 与写 0 无差别 |
| `LogHTTP(..., connSlot int, ...)` | 签名不变 |

**日志缓冲区预分配 `304` 字节也不用改**：`conn_slot` 的值从 `0` 变成 `0..3`，
**仍然是 1 位数字**，每行字节数不变。

### 4.5 建连时机：懒建连，4 个槽逐个拨号

Go 的 `http2.Transport` 是懒建连的。4 个 slot 各自在**首个落到自己身上的请求**
到达时才拨号。轮询从第 1 个请求就开始，所以前 4 个请求会分别触发 4 次拨号，
之后稳定为 4 条。

**不做启动预热**（与 `0806` 计划的判断一致）：注册洪水的头几个请求毫秒内
就把连接建满；预热需要一个无副作用的 SBI 端点，各 NF 不统一，成本高于收益。
若要排除冷启动影响，丢弃实验前几十毫秒的数据即可。

---

## 5. 请求/响应配对保证

**绝对不会出现「req 走连接 1、resp 从连接 3 回来」。** 这一点在
`HTTP_MULTI_CONN_ROUNDROBIN_PLAN_0806.md` 第 4 节已论证，连接数从 2 变 4
不改变任何一条理由，此处复述结论：

**① 协议层**：HTTP/2 的 stream ID 作用域是单条连接内部。连接 1 的 stream 5
与连接 3 的 stream 5 毫无关系，server 只能在**收到请求的那条连接**上回复。

**② Go 实现层**：响应投递路径是 `cc.readLoop → cc.streams[id] → cs.resc`，
全程被 `cc`（单个连接对象）闭包住。连接 3 的读循环访问不到连接 1 的 `streams` map。

**③ 本实现**：`base.RoundTrip(req)` 是同步调用，`resp` 由被选中的 transport
直接返回，不存在跨实例的路由或汇聚。

> **边界情况**（不是错配）：连接层错误时 h2 会对幂等请求自动重试，
> 可能落到新连接上。那是一次全新的请求-响应对，仍在同一条新连接内配对。
> 现有代码用 `if wroteTime.IsZero()` 保留首次写出时间来处理这种情况。

---

## 6. 风险

### 6.1 编译期风险

| 风险 | 评估 | 处理 |
|---|---|---|
| **漏改某个 NF** | ⚠️ **本次最高风险**。改的是常量**值**不是签名，编译器**完全兜不住**，会静默跑成 1 条 | 第 7.1 节 grep 计数 + **哈希 7 份一致**双重校验 |
| 注释改了但常量没改（或反之） | 不报错，但代码与文档矛盾，误导后续判读 | 哈希一致性可兜住（7 份必须完全相同） |
| 误改到 `smf`/`chf`/`nef` | 这几个 NF 没有 `internal/accesslog/`，改不到 | 不适用 |

> 本次**不新增 import、不改 `go.mod`**，所以 `0807` 计划里那些
> `// indirect` / unused import 类的编译错误**本次不会出现**。

### 6.2 运行期风险

| 风险 | 评估 | 处理 |
|---|---|---|
| **镜像 tag 对不上 → 跑的是旧代码** | **最危险**：不报错、有数据、数据全错。历史上 build tag 与 `free5gc-all-custom.yaml` 的 repository/tag **两边都对不上**过 | 第 8.2 节；部署后**立即**核对 pod 内二进制时间戳，别等数据异常才回头查 |
| 4 条连接实际未并存 | 低。`IdleTimeout` 已是 500ms，覆盖实测全部间隔中位数（2.3~199.6ms） | 看 `conn` 去重数是否达到 4；未达到则查 7.2(d) 的复用率 |
| 低 RQ 下用不满 4 条 | **预期行为**，非失败。懒建连（4.5 节） | 丢弃前几十毫秒数据 |
| 高 RQ 下 socket 数 > 4 | **预期行为，是 R4 要的**。撞 250 stream 上限时自行扩容 | 用 `conn_slot` 判轮询、用 `conn` 判 socket 数（7.2(c)） |
| 轮询按**请求数**均衡而非**耗时** | 慢请求集中在某槽时仍会不均。这是 round-robin 的固有性质 | 用 7.2(a) 分布 + 7.2(e) 时间分箱堆叠图判读 |
| 服务端 FD / 内存上升 | 连接数 ×4 且活得久。每连接几十 KB，量级仍小 | 观察 pod 内存；仍远小于原先每秒数百次建连销毁的开销 |
| 延迟未改善 | 有可能。若瓶颈不在连接层（而在 Go 调度、CPU、对端处理），4 条连接无收益 | **这本身是有价值的负面结论**——与 AMF 侧已知的 goroutine 排队结论互为佐证 |
| `dropped` 上升 | 本次每行日志字节数不变，写盘压力不增 | 仍需在 7.2(g) 确认为 0 |

### 6.3 本次生效后的实际配置（四份 plan 叠加）

| 层 | 设置 | 来源 | 效果 |
|---|---|---|---|
| 客户端连接数 | `connsPerPeer = 4` | **本次**（1→4） | 开局 4 条并存 + 轮询均分 |
| 客户端并发 stream | `MaxConcurrentStreams` 未设 | 从未设置 | 用对端通告值（250） |
| 客户端扩容 | `StrictMaxConcurrentStreams` 未设 | `0806` 移除 | 撞 250 时自行扩容 |
| 客户端健康检查 | `PingTimeout = 3s` | `0806` | 已确认该路径从未触发 |
| 服务端空闲回收 | `IdleTimeout = 500ms` | `0807`(`d2f0cfc`) | 连接不再每请求重建 |

### 6.4 ✅ 对照实验的变量干净度

**这是本次相对 `0806` 系列实验的重要改进，值得单独说明。**

| 对比组 | 配置 | 变量差异 |
|---|---|---|
| 对照（`d2f0cfc`） | 1 slot + IdleTimeout 500ms | — |
| 实验（**本次**） | **4 slot** + IdleTimeout 500ms | **仅「连接数 1 vs 4」一项** |

`0806` 的双连接实验背负两个问题：`PingTimeout` 1s→3s 引入了第二个变量
（后经 `0807` 1.4 节确认该路径从未触发，实际未污染），以及更致命的
——服务端 1ms `IdleTimeout` 让「2 条连接」根本没成立。

**本次两组只差一个变量，因果归因干净。** 结论可以直接归因到连接数。

---

## 7. 验证清单

### 7.1 代码验证（编译前）

```bash
cd Free5gc-TYcustom

# ① 7 个 NF 全部为 4
grep -rn "connsPerPeer = " --include=*.go NFs/ | grep -c "= 4"      # 期望 7
grep -rn "connsPerPeer = 1" --include=*.go NFs/ | wc -l             # 期望 0

# ② 7 份文件改后仍字节一致 —— 唯一能兜住「漏改」的判据
md5sum NFs/*/internal/accesslog/httptransport.go \
  | awk '{print $1}' | sort -u | wc -l                              # 期望 1

# ③ 扩容能力未被误关（R4）
grep -rn "StrictMaxConcurrentStreams:" --include=*.go NFs/ | wc -l  # 期望 0（仅注释提及）

# ④ 并发 stream 限制未被误加（R3）
grep -rn "MaxConcurrentStreams" --include=*.go NFs/ \
  | grep -v "^.*//" | grep -v Strict | wc -l                        # 期望 0

# ⑤ 前提未被破坏：服务端 IdleTimeout 仍是 500ms
grep -rn "idleTimeoutPeriod = " --include=*.go NFs/ \
  | grep -c "500 \* time.Millisecond"                               # 期望 10

# ⑥ 本次不该动的文件确实没动
git diff --stat -- 'NFs/*/internal/accesslog/accesslog.go'          # 期望空
git diff --stat -- 'NFs/*/internal/sbi/server.go'                   # 期望空
git diff --stat -- 'NFs/*/go.mod'                                   # 期望空

# ⑦ 改动总量确认
git diff --stat                                    # 期望仅 7 个 httptransport.go
```

**编译**（本机无 Go 工具链，需在编译机 / node-0 执行）：

```bash
for nf in amf ausf nrf nssf pcf udm udr; do
  (cd NFs/$nf && GOFLAGS=-mod=mod go build ./...) || echo "FAIL $nf"
done
```

- [ ] 7 个 NF `go build ./...` 通过
- [ ] 编译**全程未联网拉取新模块**（本次无新增 import、无 `go.mod` 改动）

### 7.2 实验验证

**(a) 轮询是否均衡 —— 核心判据**

```bash
grep '"src":"UDM"' HTTP_log.txt | grep '"dst":"UDR"' \
  | grep -o '"conn_slot":[0-9]*' | sort | uniq -c
```

**期望：4 个槽计数接近 1:1:1:1，误差 ≤1**（轮询按请求数强制均分）。

判读：
- 只出现 `conn_slot:0` → 该 NF **漏改**，或 pod 跑的是**旧二进制**（查 8.3-①）
- 出现 `conn_slot:4` 或更大 → 不可能，若出现说明日志或二进制不一致
- 4 个槽但严重不均 → 轮询本身不会不均（原子自增取模），检查是否有多个
  `sharedClient` 实例（不应该，它是包级单例）

**(b) 确实开了 4 条连接**

```bash
grep '"dst":"UDR"' HTTP_log.txt | grep -o '"conn":"[^"]*"' | sort -u | wc -l
```

| 负载 | `conn` 去重数预期 | 说明 |
|---|---|---|
| 低 RQ（RQ5 UE10） | **4** | 每槽一条，稳定持有，不触发扩容 |
| 高 RQ | **>4** | 撞 250 stream 上限时自行扩容 —— **属预期，不是失败**（R4） |

**(c) 槽与连接的对应关系**

```bash
grep '"dst":"UDR"' HTTP_log.txt \
  | grep -o '"conn":"[^"]*","conn_slot":[0-9]*' | sort | uniq -c
```

**期望：每个 slot 对应 1 个（低 RQ）或少数几个（高 RQ 扩容 / 重建）`conn`。**

判读原则不变：**轮询是否均衡看 `conn_slot`，实际 socket 数看 `conn`。**
两者分开记录正是为了应对「一个槽的连接中途被替换」这种情况。

**(d) 连接复用率 —— 确认 500ms 前提仍成立**

```bash
grep '"dst":"UDR"' HTTP_log.txt | grep -o '"conn_reused":[a-z]*' | sort | uniq -c
```

**期望：`true` 占绝大多数。**

⚠️ 若仍大量 `false`（尤其低 RQ 下每 socket 请求数 ≈1.0），说明服务端
`IdleTimeout` 那一层出了问题 —— **本次改动的结论不可信**，
先回到 `HTTP2_IDLETIMEOUT_FIX_PLAN_0807.md` 8.3 排查，再谈连接数。

**(e) 并行 vs 接力**

仅看请求数均衡不够。`0806` 实测出现过「两条连接总数接近、实际是时间上接力」
的假象。用 `cloudlab/Ty_log/Free5gc/C6525100g_HTTPconnum_0806/analyze_http_conns.py`
的时间分箱堆叠图，确认 4 个槽**同时**在工作。

> 该脚本已 slot-aware（`0806` 引入 `conn_slot` 时适配）。
> 若图例硬编码了 2 个 slot，需扩到 4。

**(f) 最终目的：延迟对比**

与对照组（`d2f0cfc`，1 slot + IdleTimeout 500ms）**同 RQ** 对比 t2→t4
UE 注册延迟。如 6.4 节所述，**两组只差「连接数 1 vs 4」一个变量**。

建议至少跑 RQ5(UE10) / RQ1000 / RQ1500 / RQ2000 四档，因为连接数的收益
预期随并发上升才显现——低 RQ 下 4 条与 1 条可能无差异，那是正常的。

**(g) 采样完整性**

- [ ] 各 NF `dropped` 计数为 **0**，否则 `conn_slot` 分布统计本身有采样偏差

### 7.3 完整勾选清单

**代码**
- [ ] 7 个 NF `connsPerPeer = 4`（7.1-①）
- [ ] 7 份 `httptransport.go` 哈希一致（7.1-②）
- [ ] `StrictMaxConcurrentStreams` 计数为 0（7.1-③，R4）
- [ ] `MaxConcurrentStreams` 未设置（7.1-④，R3）
- [ ] 服务端 `idleTimeoutPeriod` 仍 10 处 500ms（7.1-⑤）
- [ ] `accesslog.go` / `server.go` / `go.mod` 均未改动（7.1-⑥）
- [ ] 7 个 NF `go build ./...` 通过

**部署**
- [ ] `docker images` 输出与 `free5gc-all-custom.yaml` 的 repository/tag 完全一致
- [ ] **部署后立即**核对 pod 内二进制时间戳（8.3-①）
- [ ] NF 注册数正常

**数据**
- [ ] (a) `conn_slot` 四槽分布接近 1:1:1:1
- [ ] (b) 低 RQ `conn` 去重数 = 4
- [ ] (c) 每 slot 对应的 conn 数量可解释
- [ ] (d) `conn_reused` 以 `true` 为主
- [ ] (e) 四槽**并行**工作而非时间接力
- [ ] (f) 与 `d2f0cfc` 同 RQ 对比 t2→t4 延迟
- [ ] (g) 各 NF `dropped` 为 0

---

## 8. 部署流程

沿用 `HTTP2_IDLETIMEOUT_FIX_PLAN_0807.md` 第 7 节流程，只换 tag。
代码改动后 **`Dockerfile.custom` 无需修改**。

### 8.1 机器 A：编译

```bash
cd /local/5GC/Free5gc-TYcustom
git pull        # 或同步改动

docker build -f Dockerfile.custom \
  -t free5gc-custom-v4.2.2:4conn-0807v1 .

docker save free5gc-custom-v4.2.2:4conn-0807v1 \
  -o free5gc-custom-v4.2.2-4conn-0807v1.tar
```

### 8.2 ⚠️ 机器 B：tag 一致性

> 历史上 build 出的 tag 与 `free5gc-all-custom.yaml` 里写的
> `repository: free5gc-custom, tag: v4.2.2-custom` **两边都对不上**。
> 若无额外处理，helm 会拉到旧镜像或拉取失败。**本次务必先核对。**

```bash
docker load -i free5gc-custom-v4.2.2-4conn-0807v1.tar
docker images | grep free5gc-custom      # 以此输出为准填 yaml
```

`free5gc-all-custom.yaml` 中 10 个 NF 的 `repository`/`tag`
必须与 `docker images` 完全一致。

### 8.3 重启顺序（NRF 必须先起）

```bash
kubectl -n free5gc rollout restart deploy/free5gc-nrf
kubectl -n free5gc rollout status  deploy/free5gc-nrf
for nf in ausf udm udr nssf pcf smf amf chf webui; do
  kubectl -n free5gc rollout restart deploy/free5gc-$nf
done
kubectl -n free5gc rollout status deploy/free5gc-amf
```

**① 部署后立即核对二进制时间戳**（不要等到数据异常才回头查）：

```bash
kubectl -n free5gc exec deploy/free5gc-udr -- ls -l /free5gc/udr
```

随后按原流程验证 NF 注册数、跑 PacketRusher、收集
`/local/free5gcLog` 下的 HTTP_log / DB_log。

### 8.4 排错：若 `conn_slot` 不是四槽均分

**按顺序排查，每排除一层再往后推：**

| # | 检查 | 命令 / 判据 |
|---|---|---|
| 1 | **镜像真的生效了吗**（最常见） | 8.3-① 核对二进制时间戳 |
| 2 | 该 NF 漏改了吗 | 回到 7.1-① / ②，重跑 grep 与哈希比对 |
| 3 | 只有 `conn_slot:0` 但镜像是新的 | 该 NF 的 `httptransport.go` 没改到；确认改的是 `NFs/<nf>/internal/accesslog/` 而非别处 |
| 4 | 四槽均分但 `conn` 只有 1 条 | 服务端 `IdleTimeout` 未生效或 4 条被复用成 1 条；查 7.2(d) 复用率 |

---

## 9. 后续待办

### 9.1 若要做连接数参数扫描（方案 B）

本次是定值 4。若后续要扫 1/2/4/8/16 找最优值，每个值重新 build 镜像成本太高，
届时改成运行期可配：

| 改动 | 内容 |
|---|---|
| 数据结构 | `tls`/`clear` 由 `[connsPerPeer]http.RoundTripper` 改为 `[]http.RoundTripper` |
| 构造 | `make([]http.RoundTripper, n)`，`n` 来自 `os.Getenv("FREE5GC_CONNS_PER_PEER")` |
| 兜底 | 解析失败或 ≤0 时回落到默认值 4；**必须有兜底**，否则 n=0 会导致 `% 0` panic |
| 选路 | `pool[connSlot]` 改为对 slice 取模，`len(pool)` 替代常量 |
| 部署 | helm values 加环境变量，改值只需重启 pod |

**代价**：数组长度的编译期越界保证变成运行期保证（约 15 行 × 7 个 NF），
风险高于改常量。**只在确定要扫参数时才做。**

### 9.2 需回头修正的既有文档

本计划实施后，`HTTP2_IDLETIMEOUT_FIX_PLAN_0807.md` 需补注：

- **4.5 节**：`connsPerPeer` 已由本计划改为 **4**，该文件描述的 `=1` **已不是当前状态**
- **8.2 节**：`conn_slot` 恒为 0 的验证条目**已失效**，由本计划 7.2(a) 的四槽分布取代
- **6.3 节**：配置叠加表需加入本次的 `connsPerPeer = 4`

同时 `HTTP_MULTI_CONN_ROUNDROBIN_PLAN_0806.md` 的 9 节待办仍未完成，
本次可一并处理（它记录的双连接配置已被两次改动覆盖）。

### 9.3 沿用的既有待办

| 项 | 说明 |
|---|---|
| `Dockerfile.custom` 未纳入版本控制 | 靠编译机上的 heredoc 现场生成，`git pull` 拉不到，属「改动静默不生效」的同构风险 |
| https 下 `IdleTimeout` 不生效 | 本部署 `config/` 全部 `scheme: http`，路径不触发 |
| 不在覆盖范围的 HTTP 路径 | `pcf`/`smf` 的 `bsf_service.go` 裸 `&http.Client{}`、`nef` 的 `http.DefaultClient`、SMF 出向 —— 均不走 `accesslog.Client()`，**不受本改造影响，也不记日志** |
