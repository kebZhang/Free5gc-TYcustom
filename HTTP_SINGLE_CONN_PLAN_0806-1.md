# 强制 NF 间单 TCP 连接改造计划 (0806-1)

> 基于 commit `98903eb` + 已合入的 conn-identity 打点（`conn` / `conn_reused` 字段）。
> 所有行号对应当前工作区代码。本计划**只改连接策略**，不动日志、不动 round-robin。

---

## 1. 需求

### 1.1 用户要求（原文）

> 限制两个 NF 之间的 TCP 连接只有一条，但是如果没有通过健康检查除外，
> 如果是因为健康检查，导致例如一条 TCP 连接没了要建立另外一条这是可以被允许的。

### 1.2 拆解为可验证的规格

| # | 要求 | 判定标准 |
|---|---|---|
| **R1** | 同一时刻，一对 NF 之间**最多只有 1 条**活跃 TCP 连接 | 任意时刻并发连接数 = 1 |
| **R2** | 并发请求超过 `MaxConcurrentStreams` 时**不得**新开连接，应排队等待 | 不再出现 `streams_at_birth ≈ 250` 的新连接 |
| **R3** | 健康检查失败导致连接关闭后，**允许**建立新连接接替 | 允许连接总数 > 1，但必须是**先死后生**，不能并存 |

> **R1 与 R3 的关系**：R1 约束的是「同时」，R3 允许的是「先后」。
> 即整个实验期间 `conn` 的**去重计数**可以 > 1，但任一时刻只有一条在服务。

---

## 2. 现状与根因（实测结论）

### 2.1 当前代码没有任何连接数约束

`NFs/<nf>/internal/accesslog/httptransport.go:41-58` 的 `http2.Transport` 只设了 4 个字段：

```go
clear: &http2.Transport{
    AllowHTTP:       true,
    DialTLSContext:  func(...) {...},
    ReadIdleTimeout: readIdleTimeoutPeriod,   // 1s
    PingTimeout:     pingTimeoutPeriod,       // 1s
},
```

全仓库 grep 验证（零命中）：

```
StrictMaxConcurrentStreams | MaxConcurrentStreams | MaxConnsPerHost
MaxIdleConnsPerHost | DisableKeepAlives | ConnPool
→ NONE FOUND
```

**连接数完全由 Go 默认行为决定。**

### 2.2 实测：连接是怎么涨起来的

`C6525100g_HTTPconnum_0806` 的 UDM→UDR 数据（用新增的 `conn` 字段统计）：

| RQ | 连接数 | 峰值并发 stream | 主连接占比 |
|---|---|---|---|
| 1000 | 1 | 48 | 100% |
| 1500 | **10** | **371** | 86.4% |
| 2000 | 5 | 297 | 91.4% |

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

### 2.3 两种建连机制

**机制① — `MaxConcurrentStreams` 溢出（#2~#9）**

Go 官方文档（`StrictMaxConcurrentStreams` 字段）：

> If false, **new TCP connections are created to the server as needed** to keep each
> under the per-connection SETTINGS_MAX_CONCURRENT_STREAMS limit. If true, the
> server's SETTINGS_MAX_CONCURRENT_STREAMS is interpreted as a global limit and
> **callers of RoundTrip block when needed**, waiting for their turn.

Go h2 server 默认通告 `MaxConcurrentStreams = 250`。当前 `StrictMaxConcurrentStreams`
未设置（= false），所以并发撞 250 时 Go 自动新开连接。

**机制② — 健康检查误杀后重建（#10）**

实测主连接在实验中途死亡：

```
RQ1500: 主连接最后一个响应 @813.6ms，之后 563 个请求全部由 #10 承担（#10 生于 816.4ms）
RQ2000: 主连接最后一个响应 @671.0ms，之后 316 个请求由 #4/#5 承担
```

根因是这两个超时值（`httptransport.go:26-30`）：

```go
readIdleTimeoutPeriod = 1 * time.Second   // 1s 没收到帧就发 PING
pingTimeoutPeriod     = 1 * time.Second   // PING 1s 无响应就关闭连接
```

Go 文档：*"PingTimeout is the timeout after which the connection will be closed if
a response to Ping is not received. **Defaults to 15s**."*

当前值是默认值的 1/15。高负载下 UDR 忙不过来，PING 响应超过 1 秒即被误判为连接失效。

---

## 3. 方案

### 3.1 核心：只改一个字段

**`StrictMaxConcurrentStreams: true`** 恰好精确对应用户需求：

| 机制 | Strict=true 后的行为 | 是否符合要求 |
|---|---|---|
| ① 溢出建连 | **被禁止**，RoundTrip 阻塞排队 | ✅ 满足 R2 |
| ② 健康检查后重建 | **不受影响**，连接死了照样重建 | ✅ 满足 R3 |

这正是用户要的语义边界：**堵住"因为忙而多开"，保留"因为死而重建"。**

> **为什么不用 `MaxConnsPerHost`**：`http2.Transport` **没有**这个字段
> （已查证 Go 官方文档的 Transport 字段列表，只有 `ConnPool` /
> `StrictMaxConcurrentStreams` / `IdleConnTimeout` 三个与连接池相关）。
> `MaxConnsPerHost` 是 `net/http.Transport` 的字段，此处用不上。

### 3.2 健康检查超时：本计划**不改**

这是一个需要明确说明的决策。

用户要求「因为健康检查导致重建是可以被允许的」，所以机制② **本就在允许范围内**，
不需要为了满足需求而去动它。

但要认识到它的影响：**保留 1s/1s 会让"连接总数"在高 RQ 下仍然 > 1**
（虽然任一时刻只有 1 条，符合 R1）。

如果后续发现健康检查误杀太频繁、干扰实验判读，可以再单独放宽——
但那是另一个决策，不属于本次「限制单连接」的范围。**本计划保持 1s/1s 不变。**

| 选项 | 效果 | 本计划取舍 |
|---|---|---|
| 保持 1s/1s（**本计划**） | 严格满足用户需求；连接会因误杀而重建 | ✅ 采用 |
| `PingTimeout: 15s`（Go 默认） | 大幅减少误杀，连接总数更接近 1 | ❌ 超出本次需求，留作后续选项 |
| `ReadIdleTimeout: 0`（关闭健康检查） | 完全不重建，但连接真断了也发现不了 | ❌ 违背 R3 的保留意图 |

### 3.3 ⚠️ 必须预期的副作用：延迟会变差

**这是本计划最重要的提醒。**

实测峰值并发 stream：RQ1500 = **371**，RQ2000 = **297**，均**远超 250**。

改造后，超出 250 的那 47~121 个并发请求**不再溢出到新连接，而是阻塞排队**。
因此 **UE 注册延迟大概率会上升**。

这不是 bug，而是本改造的**预期语义**：

- 改造前：并发超限 → 偷偷多开连接 → 延迟被掩盖
- 改造后：并发超限 → 显式排队 → **单连接的真实上限被暴露出来**

若实验目的是**证明单连接是瓶颈**，延迟变差正是有力证据。
若期望"控制变量后延迟不变"，则会失望。

---

## 4. 具体改动

### 4.1 改动范围

| 项 | 内容 |
|---|---|
| **必改文件** | `NFs/<nf>/internal/accesslog/httptransport.go`，共 **7 个 NF** |
| | `amf` `ausf` `udm` `udr` `pcf` `nrf` `nssf` |
| **改动内容** | 每个文件加 **2 行**（`tls` 和 `clear` 各一行） |
| **不需要改** | ✅ consumer/service 代码 ✅ openapi 外部模块 ✅ `config/*.yaml` ✅ `accesslog.go` |

**已验证前提**（`diff -q` 实测）：7 份 `httptransport.go` 当前**完全一致**，
且该文件不含 NF 名，因此改好 `amf` 后可直接 `cp` 到其余 6 个。

### 4.2 Step 1：改 `amf` 版

文件：`NFs/amf/internal/accesslog/httptransport.go`

在 `newLoggingRoundTripper()`（:41-58）的**两个** `http2.Transport` 字面量里各加一行：

```go
func newLoggingRoundTripper() *loggingRoundTripper {
	return &loggingRoundTripper{
		tls: &http2.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // matches openapi default
			// Treat the peer's SETTINGS_MAX_CONCURRENT_STREAMS as a limit on
			// this NF as a whole rather than per connection. With the default
			// (false) the transport silently dials extra connections whenever
			// the in-flight stream count reaches the peer's limit, which is how
			// a single NF pair was observed holding up to 10 sockets at 1500
			// req/s. With true, requests past the limit block in RoundTrip and
			// wait their turn, so the pair keeps exactly one connection.
			//
			// This does NOT pin the connection's identity: if the connection
			// dies (GOAWAY, or the ping health check below failing) the
			// transport still dials a replacement. That is intended -- what is
			// being suppressed is "open another because we are busy", not
			// "open another because the old one is gone".
			StrictMaxConcurrentStreams: true,
			ReadIdleTimeout:            readIdleTimeoutPeriod,
			PingTimeout:                pingTimeoutPeriod,
		},
		clear: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				d := &net.Dialer{}
				return d.DialContext(ctx, network, addr)
			},
			// Same rationale as the tls transport above. This is the one that
			// actually carries traffic in this deployment: every config/*.yaml
			// sets `scheme: http`, so all SBI calls take the h2c path.
			StrictMaxConcurrentStreams: true,
			ReadIdleTimeout:            readIdleTimeoutPeriod,
			PingTimeout:                pingTimeoutPeriod,
		},
	}
}
```

> **两个 transport 都要改**：虽然当前部署全是 `scheme: http`（只有 `clear` 在用），
> 但两处保持一致才不会在将来切到 https 时出现行为差异。

> **`readIdleTimeoutPeriod` / `pingTimeoutPeriod` 保持 1s 不变**，理由见 3.2。
> 同时建议更新 :24-25 的注释，因为它现在说「behaviour is unchanged apart from
> the added logging」，改造后已不再成立：

```go
// These mirror the timeouts used by free5gc/openapi's internal HTTP/2 clients.
// The 1s ping timeout is aggressive (Go's default is 15s): under load a peer
// that is slow to answer a PING gets its connection torn down and replaced.
// That replacement is deliberately left in place -- see the
// StrictMaxConcurrentStreams comment below for which kind of extra connection
// is suppressed and which is not.
const (
	readIdleTimeoutPeriod = 1 * time.Second
	pingTimeoutPeriod     = 1 * time.Second
	timeoutPeriod         = 10 * time.Second
)
```

### 4.3 Step 2：同步到其余 6 个 NF

```bash
cd /path/to/Free5gc-TYcustom

# 改动前先确认 7 份仍然一致
for nf in ausf udm udr pcf nrf nssf; do
  diff -q NFs/amf/internal/accesslog/httptransport.go \
          NFs/$nf/internal/accesslog/httptransport.go
done

# 改好 amf 后复制
for nf in ausf udm udr pcf nrf nssf; do
  cp NFs/amf/internal/accesslog/httptransport.go \
     NFs/$nf/internal/accesslog/httptransport.go
done
```

> `httptransport.go` **不含** NF 名（`srcNF` 在 `accesslog.go` 里），
> 所以直接复制是安全的。**本计划不碰 `accesslog.go`。**

### 4.4 Step 3：编译验证

```bash
for nf in amf ausf udm udr pcf nrf nssf; do
  (cd NFs/$nf && go build ./... ) || echo "BUILD FAIL: $nf"
done
```

验证改动确实落到 7 个文件：

```bash
grep -c "StrictMaxConcurrentStreams: true" NFs/*/internal/accesslog/httptransport.go
# 期望：每个文件都是 2
```

---

## 5. 验证方法

复用已合入的 `conn` / `conn_reused` 字段，配合
`cloudlab/Ty_log/Free5gc/C6525100g_HTTPconnum_0806/analyze_http_conns.py`。

### 5.1 R2 验证：不再有溢出建连

改造前的特征是大量新连接诞生在 `streams_at_birth ≈ 250`。改造后应**全部消失**。

用 `analyze_http_conns.py` 的第 7/8 张图（并发 stream + 新连接时刻）直接看：

| 观察 | 结论 |
|---|---|
| 曲线在 250 处被**削平**，且该处无新连接竖线 | ✅ R2 达成，排队生效 |
| 曲线仍越过 250 且伴随新连接 | ❌ Strict 未生效，检查是否漏改 |

### 5.2 R1 验证：任一时刻只有一条活跃连接

去重连接数（可能 > 1，因为允许重建）：

```bash
grep '"src":"UDM"' HTTP_log.txt | grep '"dst":"UDR"' \
  | grep -o '"conn":"[^"]*"' | sort -u | wc -l
```

**关键是验证它们不并存**——每条连接的活跃区间应当首尾相接、互不重叠：

```bash
# 每条连接的 [首次请求, 末次响应] 区间
# 改造后应看到：区间之间不重叠（先死后生）
# 改造前：多条区间大幅重叠（并存）
```

> `analyze_http_conns.py` 里已有 `first_use` / `intervals`，
> 可直接扩展一个「连接活跃区间是否重叠」的检查。

### 5.3 R3 验证：健康检查重建仍然可用

若日志中出现 `conn_reused:false` 且当时 `streams_at_birth` 很低（1~2），
同时前一条连接的最后响应就在这之前——说明是**重建**而非溢出，**属于允许行为**。

### 5.4 延迟对比（预期会变差，见 3.3）

对照 `latency_RQ*.txt` 的 t2→t4，与改造前同 RQ 的数据比较。
**延迟上升是预期结果**，应如实记录，不要当作改造失败。

---

## 6. 风险

| 风险 | 评估 | 处理 |
|---|---|---|
| **UE 注册延迟显著上升** | **高概率发生**。峰值并发 371 vs 上限 250，超出部分要排队 | 这是预期语义（3.3）。若延迟高到 UE 超时失败，需降低 RQ 重测 |
| 请求超时 / 注册失败率上升 | 排队时间可能触及 `timeoutPeriod = 10s` 的客户端超时 | **`timeoutPeriod` 保持 10s 不变**（见 6.1）。不改代码，改为在分析阶段统计并剔除被截断的样本 |
| 连接总数仍 > 1 | 健康检查误杀导致重建，**这是用户明确允许的** | 按 R3 属正常；用 5.2 确认不并存即可 |
| Strict 模式的已知上游问题 | Go issue #70809 报告 `ReserveNewRequest` 在 Strict 下可能造成 stall | 属上游行为；若观察到异常停顿，记录并对照该 issue |
| 漏改某个 NF | 不影响编译，静默失效 | 4.4 的 `grep -c` 必须执行 |

### 6.1 `timeoutPeriod` 保持 10s 不变（已决策）

本次改动**只关闭「NF 间 HTTP 连接自动扩容」这一项**，不动任何其他字段。
`timeoutPeriod = 10s` 与改造前完全一致。

理由：`timeoutPeriod` 是本实验的**受控变量**。同时改连接策略和超时值会让
「延迟上升」无法归因——分不清是排队造成的，还是超时放宽后长尾被保留下来造成的。
保持 10s，改造前后唯一的差异就是连接扩容行为本身。

代价是：排队超过 10s 的请求会被 `http.Client` 取消。这**不需要改代码**，
因为它是一个可以在分析阶段处理的测量问题：

- 被取消的请求**仍然会记录到 `HTTP_log.txt`**——`httptransport.go:151-152`
  在返回 error 之前无条件调用 `LogHTTP`（注释原文："Always log, even on
  transport error, so failed attempts are visible"）。样本不会凭空消失。
- 但这些行的 `latency_us` 是被 10s 截断的值，不是真实服务时间，
  且 `got_first_byte` 为零值（`0001-01-01`）。

因此 5.4 的延迟对比**必须先统计截断样本数**：

```bash
# 统计撞到 10s 上限的请求（got_first_byte 从未被赋值）
grep '"src":"UDM"' HTTP_log.txt | grep '"dst":"UDR"' \
  | grep -c '"got_first_byte":"0001-01-01'
```

| 结果 | 处理 |
|---|---|
| = 0 | 延迟对比干净，无需额外处理 |
| > 0 | 与延迟数据一并报告。均值会**偏低**（截断样本被截在 10s，而非其真实时长），属右删失数据，不可直接与改造前均值比较 |

---

## 7. 检查清单

- [x] 确认 7 份 `httptransport.go` 改动前一致（`diff -q`）
- [x] `amf`：两个 `http2.Transport` 各加 `StrictMaxConcurrentStreams: true`
- [x] `amf`：更新 :24-25 的常量注释（说明 1s ping 的取舍）
- [x] 复制到 `ausf udm udr pcf nrf nssf`（改后 7 份仍 `diff -q` 一致）
- [x] `grep -c "StrictMaxConcurrentStreams: true"` 每个文件 = 2
- [ ] 7 个 NF 逐个 `go build ./...` 通过
      ⚠️ **本机（Windows）未安装 Go 工具链，此步未执行**，需在 node-0 上补跑
- [ ] 重建镜像并部署
- [ ] 跑 RQ1000 / 1500 / 2000 三档
- [ ] 5.1：并发 stream 曲线在 250 处削平，且无溢出新连接
- [ ] 5.2：连接活跃区间互不重叠（任一时刻只有 1 条）
- [ ] 5.3：确认残余的新连接都是健康检查重建（低 streams_at_birth + 前连接刚死）
- [ ] 5.4：记录延迟变化，与改造前同 RQ 对比
- [ ] 6.1：统计 `got_first_byte` 为零值的截断样本数；若 > 0，随延迟数据一并报告
- [x] 确认**只**改了 `StrictMaxConcurrentStreams` 一项：`timeoutPeriod`、
      `readIdleTimeoutPeriod`、`pingTimeoutPeriod` 三个常量值均与改造前一致
      （7 份文件均为 1s / 1s / 10s；`git diff` 非注释行仅 2 处新增 + gofmt 对齐）

---

## 8. 后续

本计划把连接数锁定为「同时只有 1 条」，这正是
`HTTP_MULTI_CONN_ROUNDROBIN_PLAN_0806.md` 所缺的**干净对照基线**：

| 组 | 配置 | 连接数 |
|---|---|---|
| A（本计划） | Strict=true | 确定的 1 条 |
| B（round-robin） | Strict=true + 2 个 transport 轮询 | 确定的 2 条 |
| 现状 | 什么都不加 | 1~10 浮动，**无法对照** |

只有 A、B 都把连接数锁死，「1 条 vs 2 条」的对比才成立。
因此本计划应当**先于** round-robin 改造实施。
