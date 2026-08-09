# NF↔NF HTTP/2 连接数 4 → 8 修改计划 (0809)

> **状态：已实施**（基于 `e5b47ba` "NF HTTP 4 conn at start - 0806v4"）。
> 改动 7 个文件，每个文件 1 行常量 + 1 段注释。
>
> 本文档的风险结论**不是理论推演**，而是从你自己的 4 连接实测数据
> `cloudlab/Ty_log/Free5gc/R6525_NF4HTTP_idle500ms_0807v1/` 里算出来的。
> 原始数字见第 3 节。

---

## 1. 改了什么

**文件**：`NFs/<nf>/internal/accesslog/httptransport.go` × 7
**NF**：`amf` `ausf` `nrf` `nssf` `pcf` `udm` `udr`

```go
const connsPerPeer = 4   // 改为
const connsPerPeer = 8
```

同时重写了常量上方的注释块（原注释整段在论证"为什么是 4"，
含 `4 is what this experiment measures against that fixed baseline of 1`
和 `4 is a floor, not a cap`，不改会与代码矛盾）。

**为什么只需要改这一行**：

| 依赖 `connsPerPeer` 的地方 | 是否需要手改 |
|---|---|
| `tls`/`clear [connsPerPeer]http.RoundTripper`（`:83-84`） | ❌ 数组长度是编译期常量，自动变 `[8]` |
| `for i := 0; i < connsPerPeer; i++`（`:95`） | ❌ 自动循环 8 次 |
| `int((l.next.Add(1) - 1) % connsPerPeer)`（`:148`） | ❌ 自动 `% 8` |
| `conn_slot` 日志字段（`accesslog.go:367`） | ❌ 已就位，写的是 `connSlot` 变量值 |
| `LogHTTP` 签名（`accesslog.go:356`） | ❌ 已含 `connSlot int` |
| `analyze_http_conns.py` | ❌ slot 列表由数据推导（`sorted(self.per_slot)`），无硬编码 4 |

**只有 7 个 NF 在范围内**：只有这 7 个有 `internal/accesslog/` 目录。
`smf` `chf` `nef` `bsf` `upf` `n3iwf` `tngf` 未接入该客户端。
特别注意 `nef` 用的是 `http.DefaultClient`（`nef/internal/sbi/consumer/*.go`），
其流量不受本改动影响，也不进 `HTTP_log.txt`。

### 1.1 已完成的验证

```
md5sum NFs/*/internal/accesslog/httptransport.go | awk '{print $1}' | sort -u
→ 392e5836e80e4c4164610cc424e1a368     （唯一值，7 份仍字节一致 ✅）

grep -h "const connsPerPeer" NFs/*/internal/accesslog/httptransport.go | sort | uniq -c
→ 7 const connsPerPeer = 8              （7 个 NF 全部改到 ✅）
```

### 1.2 尚未验证：编译

**本机没有 Go 工具链**（`go` 不在 PATH，也不在 `C:\Program Files\Go`），
所以**没有跑过编译**。改动只是一个 `const` 字面量 `4`→`8`，无新语法，
但这是事实陈述而非"已验证通过"。上 cloudlab 节点后执行：

```bash
for nf in amf ausf nrf nssf pcf udm udr; do
  (cd NFs/$nf && go build ./... ) && echo "OK $nf" || echo "FAIL $nf"
done
```

---

## 2. 风险总览

| # | 风险 | 严重度 | 判断依据 |
|---|---|---|---|
| R1 | **低 RQ 下每槽样本量稀释** | ⚠️ **真实存在，是本次唯一实质风险** | 实测：AMF→PCF 每槽从 250 降到 125 |
| R2 | 漏改某个 NF 不会编译报错 | ⚠️ 高危但**已用 md5 兜住** | 改的是常量值不是签名 |
| R3 | 连接数翻倍 → FD/内存 | ✅ 可忽略 | 48 条 vs 默认 ulimit 1024 |
| R4 | 服务端并发连接压力 | ✅ 可忽略 | 见 4.2 实测 |
| R5 | 连接churn 加剧 | ✅ **实测排除** | 4 连接run 全程 0 次重拨 |
| R6 | HTTP/2 多连接削弱多路复用 | ⚠️ 这是**实验目的**，不是缺陷 | 见 4.4 |
| R7 | 日志量翻倍 | ✅ 可忽略 | 日志量与请求数相关，与连接数无关 |
| R8 | 破坏与历史 run 的可比性 | ⚠️ 需注意 | 见 4.6 |

---

## 3. 实测基线：4 连接 run 的真实数字

从 `R6525_NF4HTTP_idle500ms_0807v1/HTTP_log_RQ*_UE1000.txt` 直接统计
（客户端视角记录，即带 `conn` 字段、`src != "NaN"` 的行）：

### 3.1 每对 NF 的实际连接数与拨号次数

**RQ800 / RQ1000 / RQ1500 / RQ2000 / RQ2500 五个速率，结果完全一致**：

| NF 对 | distinct conns | new dials (`conn_reused=false`) |
|---|---|---|
| AMF→AUSF | **4** | **4** |
| AMF→PCF | **4** | **4** |
| AMF→UDM | **4** | **4** |
| AUSF→UDM | **4** | **4** |
| PCF→UDR | **4** | **4** |
| UDM→UDR | **4** | **4** |

**这三个数字（4 / 4 / 五个速率全一致）一次性排除了三个风险**：

1. **`distinct conns == 4`** → 从未发生"自行扩容"。
   即使在 RQ2500，单个 slot 的 in-flight stream 也**远未**触及对端 250 上限。
   → **8 条同样不会扩容**，8 就是实际值，不是地板。
2. **`new dials == distinct conns == 4`** → **全程 0 次重拨**。
   每条连接建立一次就活到窗口结束，没有任何一条被 PING 判死或被
   `IdleTimeout` 收掉。→ **R5（churn）实测排除**。
3. **五个速率结果相同** → 连接数不随负载变化，是纯粹由 `connsPerPeer` 决定的。
   → 改成 8 的结果**可预测**：每对恰好 8 条，8 次拨号，0 次重拨。

### 3.2 每槽负载分布（RQ800 与 RQ2500）

| NF 对 | 总请求 | 4 槽分布 | 8 槽后每槽约 |
|---|---|---|---|
| AMF→AUSF | 2000 | 527/502/508/463 | **250** |
| **AMF→PCF** | **1000** | 237/247/244/272 | **125** ← 最稀 |
| AMF→UDM | 6000 | 1486/1501/1498/1515 | **750** |
| AUSF→UDM | 2000 | 500/500/500/500 | **250** |
| PCF→UDR | 1000 | 250/250/250/250 | **125** ← 最稀 |
| UDM→UDR | 9000 | 2250/2250/2250/2250 | **1125** |

（RQ2500 数字几乎相同，round-robin 是严格轮询，偏差只来自窗口截断。）

---

## 4. 风险逐项论证

### 4.1 R1：每槽样本量稀释（**唯一实质风险**）

**这是本次改动真正的代价，其余风险都被数据排除了。**

8 个槽意味着每槽只拿到 1/8 的流量。从 3.2 的实测：

- **最稀的两对是 AMF→PCF 和 PCF→UDR：每槽从 ~250 降到 ~125。**
- 最厚的 UDM→UDR 每槽仍有 ~1125，完全充足。

125 个样本够不够，取决于你要从 `conn_slot` 里读什么：

| 分析目的 | 125/槽 是否够 |
|---|---|
| 看 round-robin 分布是否均匀 | ✅ 够。严格轮询下 8 槽应各 125±少量 |
| 每槽的延迟中位数 / P50 | ✅ 够 |
| 每槽的 **P95 / P99** 尾延迟 | ⚠️ **勉强**。125 个样本的 P99 只由 1~2 个点决定，噪声大 |
| 每槽的延迟分布形状（直方图/CDF） | ⚠️ 偏稀 |

**结论与建议**：
- 当前 RQ800~2500 / UE1000 的量级下，8 槽**可用**，但 AMF→PCF 和 PCF→UDR
  这两对的**尾延迟统计要谨慎**——优先看 AMF→UDM（750/槽）和
  UDM→UDR（1125/槽）。
- **若以后要跑低 RQ（如早期的 RQ5/UE10），8 槽会彻底不可读**
  ——那种 run 每对总共才 84 个请求，8 槽下每槽 10 个。届时应改回 4 或 1。
- 若需要 8 槽 + 充足样本，提高 UE 数比提高 RQ 更有效：
  总请求数正比于 UE 数（3.2 里 UDM→UDR 的 9000 = 1000 UE × 9 次调用），
  而 RQ 只改变速率不改变总量——这也解释了为什么五个速率的总请求数完全相同。

### 4.2 R3/R4：连接数、FD、服务端压力

**关键事实：slot 是每进程的，不是每对端的**（`httptransport.go:402` 的
`sharedClient` 是包级单例）。所以：

```
单 NF 进程持有的连接数 = connsPerPeer × 该进程访问过的对端数
```

从 3.1 的实测，各 NF 的对端数：AMF→{AUSF,PCF,UDM}=3（加 NRF 共 4），
AUSF→{UDM}（加 NRF 共 2），UDM→{UDR}（加 NRF 共 2），PCF→{UDR}（加 NRF 共 2）。

| NF | 对端数 | 4 连接时 | **8 连接时** |
|---|---|---|---|
| AMF | ~4 | 16 | **32** |
| AUSF / UDM / PCF | ~2 | 8 | **16** |
| UDR（被动接收，几乎不外呼） | ~1 | 4 | **8** |

**服务端侧**：UDR 同时被 UDM 和 PCF 呼叫，入连接从 8 涨到 **16**。

**结论：✅ 完全可忽略。** 32 / 16 条连接对比 Linux 默认 `ulimit -n = 1024`
差两个数量级。每条 HTTP/2 连接的固有开销主要是收发缓冲区（默认几十 KB 量级），
32 条也就 MB 级别。

> 唯一需要确认的：K8s pod 若设了异常低的 `ulimit`。用 `ulimit -n` 在 pod 内查一下即可。
> 从 4 到 8 这个量级不可能是问题。

### 4.3 R5：连接 churn / 重拨（**实测排除**）

历史上 `0806v1`（`StrictMaxConcurrentStreams=true` + `PingTimeout=1s`）
曾出现严重 churn：2~7 条连接，每条只活 25~120ms。

**但当前配置下 4 连接 run 的实测是 `new_dials == 4`，即全程零重拨**
（3.1）。原因是当前三个参数已经互相配合好了：

| 参数 | 值 | 作用 |
|---|---|---|
| 服务端 `IdleTimeout` | 500ms | 连接能活过请求间隔（`server.go:218`） |
| `PingTimeout` | 3s | 高负载下 PONG 迟到不会被误判为死连接（`:46`） |
| `StrictMaxConcurrentStreams` | **不设** | 请求不会阻塞在 `RoundTrip` 里导致连接静默 |

**8 条连接不会改变这个平衡，反而更宽松**：每条连接分到的请求更少
（1/8 而非 1/4）。

> ⚠️ 唯一的理论隐患：请求更分散 → 单条连接上的帧间隔变长 →
> 更容易触发 `ReadIdleTimeout = 1s` 的 PING 健康检查。
> **量化一下：** UDM→UDR 每槽 1125 个请求。即使窗口长达 10s，
> 每槽平均帧间隔也只有 ~9ms，距离 1s 差两个数量级。
> 最稀的 AMF→PCF 每槽 125 个请求，间隔 ~80ms，同样远小于 1s。
> → **不会触发。** 而且就算触发了 PING，3s 的 `PingTimeout` 也足够回 PONG。
>
> **验证方式**：改完后检查 `new_dials` 是否恰好 = 8。若 > 8，就是发生了重拨。

### 4.4 R6：多连接削弱 HTTP/2 多路复用（**这是实验目的**）

HTTP/2 的设计初衷是单连接多路复用。开 8 条连接在协议设计上是"反模式"：

- 8 条连接 = 8 个独立的 TCP 拥塞窗口、8 个 HPACK 压缩上下文
  （头部压缩效率下降，因为动态表不共享）
- 8 条连接 = 8 个独立的 flow-control 窗口

**但这正是你要测的东西**：单连接下所有请求串在一条连接的
写锁（`clientConn.wmu`）和单个读循环上，这正是怀疑的瓶颈。
8 条连接就是为了把这个串行点拆开。

**所以 R6 不是缺陷，是这次实验的自变量。** 只需在解读结果时记得：
延迟若下降，收益来自"拆开写锁/读循环的串行"，代价是"HPACK 压缩率和
拥塞窗口效率下降"——两者的净效果就是测量结果本身。

### 4.5 R7：日志量

✅ **无影响。** `LogHTTP` 是**每请求一行**，与连接数无关。
`conn_slot` 字段的值从 `0-3` 变成 `0-7`，仍是单字符，行长不变。
`queueCapacity = 1<<21`（约 210 万条）不受影响。

### 4.6 R8：与历史 run 的可比性

`analyze_http_conns.py` 从数据推导 slot 列表，**不会报错**，
但跨 run 对比时要注意：

- 图 2（slot balance）在 4-conn run 里是 4 根柱，8-conn run 里是 8 根柱，
  **柱高不可直接比**（8 槽每槽天然只有一半流量）。要比就比**百分比**
  （脚本的 `slot_balance()` 已经输出百分比）。
- 图 1（connections per pair）会从 4 跳到 8，这是预期的。
- **`R6525_NF4HTTP_idle500ms_0807v1` 是本次的对照组**，不要覆盖。
  新 run 建议命名 `R6525_NF8HTTP_idle500ms_0809`。

---

## 5. 改完后的验证清单

### 5.1 编译（本机无 Go，必须在 cloudlab 节点做）

```bash
cd Free5gc-TYcustom
for nf in amf ausf nrf nssf pcf udm udr; do
  (cd NFs/$nf && go build ./... ) && echo "OK $nf" || echo "FAIL $nf"
done
```

### 5.2 一致性（已在本机通过，重建镜像前再确认一次）

```bash
md5sum NFs/*/internal/accesslog/httptransport.go | awk '{print $1}' | sort -u | wc -l
# 必须输出 1

grep -c "connsPerPeer = 8" NFs/*/internal/accesslog/httptransport.go
# 7 行必须全部为 1
```

> **这是唯一能兜住"漏改某个 NF"的手段。** 改的是常量值不是函数签名，
> 漏掉一个 NF 照样编译通过、照样运行，只是那个 NF 悄悄还在用 4 条连接，
> 而数据看起来完全正常。

### 5.3 运行时：真的是 8 条吗

pod 内（以 UDM→UDR 为例，UDR SBI 端口 8000）：

```bash
ss -tn state established '( dport = :8000 )' | tail -n +2 | wc -l   # 期望 8
```

### 5.4 日志判读（**最重要**，直接套用第 3 节的基线）

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
        conn[k].add(r["conn"])
        slot[k][r.get("conn_slot")] += 1
        if r.get("conn_reused") is False: dials[k] += 1
    print("==", f)
    for k in sorted(conn):
        c = slot[k]
        print(f"   {k} conns={len(conn[k])} dials={dials[k]} "
              f"per_slot={[c[i] for i in range(8)]}")
PY
```

**判读标准（对照第 3 节的 4-conn 基线）**：

| 观察 | 含义 |
|---|---|
| `conns == 8` 且 `dials == 8` | ✅ 完全符合预期，与 4-conn run 行为一致 |
| `conns == 4` | ❌ **该 NF 漏改了**，或跑的是旧镜像 |
| `per_slot` 里有槽为 0 | ❌ 同上 |
| `conns > 8` 且 `dials > 8` | ⚠️ 发生了重拨或扩容。对比两者差值：`dials - 8` 就是重拨次数 |
| `per_slot` 8 个值不均（偏差 >10%） | ⚠️ 异常。round-robin 是严格轮询，除窗口截断外不应有大偏差 |

---

## 6. 回滚

单个 commit，`git revert` 即可。或直接把 7 个文件的 `connsPerPeer` 改回 4
（注释块也要一并还原）。无数据迁移、无配置依赖、无 helm values 变更。

---

## 7. 附：本次**没有**做的两件事

你最初的描述里还有另外两层意思，本次**刻意没做**，记录在此以免混淆：

| 需求 | 现状 | 为何没做 |
|---|---|---|
| "**起始的时候**就建好 8 条" | 当前是**懒建连**：8 个 `http2.Transport` 对象在进程启动时就有了，但 TCP 连接要等第一个请求路由到该 slot 才拨号 | 需要预热逻辑（约 60 行 × 7），且 `newLoggingRoundTripper()` 执行时进程还不知道任何对端地址。**且实测显示这最多只影响前 8 个请求**，你的分析脚本已经用"按 t2~t4 开窗"过滤掉了启动流量 |
| "无论多少**都是** 8 条"（硬上限） | 8 是地板不是天花板，`StrictMaxConcurrentStreams` 不设 | **3.1 的实测证明这个上限根本没被触及**：4 连接 run 在 RQ2500 下 distinct conns 恒等于 4，从未扩容。加 `Strict=true` 治的是一个不存在的问题，反而会引入 `0806v1` 那种 churn |

**简言之：第二件事被数据证明没必要做；第一件事影响面只有前 8 个请求，
且已被分析脚本的开窗逻辑覆盖。**
