# HTTP_log 新增「连接身份」打点计划 (0806)

> 基于 commit `98903eb` 实际代码阅读得出。所有行号对应该 commit。
> 本计划**只做测量**，不改变任何连接行为。

---

## 1. 目的：回答一个目前无法回答的问题

### 1.1 我们现在不知道什么

两个 NF 之间**到底有几条 TCP 连接**，以及**高 RQ 下会不会自动扩容**——
目前**没有可信数据**。

### 1.2 为什么 `ss` 外部采样不可信

0806 用 `ss` 在 UDR pod 内每 20ms 采样一次，3 秒实验得到：

```
UDM→UDR:  udm=1(78次)  udm=2(8)  udm=3(5)  udm=4(24)     峰值 4
PCF→UDR:  pcf=1(21次)  ...       pcf=29(7)                峰值 29
```

**这组数据自相矛盾，不能采信**：

UDM→UDR 的请求量**远大于** PCF→UDR（每个 UE 注册要多次查 SDM/认证数据，
PCF 只有一次 AM policy）。负载更小的 PCF 反而连接数是 UDM 的 7 倍，讲不通。

矛盾的根源是采样方法本身有两个缺陷：

| 缺陷 | 说明 |
|---|---|
| **`grep -c` 计数未经验证** | 脚本用 `grep -c "192.168.88.218"` 数 PCF 连接，但 `ss` 输出里 Local 和 Peer 两列都会被扫到，且从未核对过匹配行的原始内容。29 这个数字**可能根本不是 PCF 的连接数** |
| **20ms 采样有盲区** | HTTP/2 连接可能建立几十毫秒后就关闭，两个采样点之间的连接完全不可见 |

### 1.3 为什么改用 `httptrace.GotConn`

`GotConn` 是 Go transport **自己报告**的连接信息，不是外部观测：

| | `ss` 外部采样 | `httptrace.GotConn` |
|---|---|---|
| 采样盲区 | 有，短命连接会漏 | **无**，每个请求都记录 |
| 归属判断 | 靠 IP 猜，易错（正是 1.2 的问题） | 发起方自己记录，准确 |
| 能否定位到具体请求 | 否 | **能**，可看出哪些请求挤在同一条连接上 |
| 能否区分新建/复用 | 否 | **能**（`info.Reused`） |

### 1.4 这个测量要回答的三个问题

1. **基线连接数**：一对 NF 之间实际用了几条 TCP？
2. **是否扩容**：连接是随负载陆续新增的（→ 自动扩容），
   还是全部在最初几毫秒建立、之后数量不变（→ 只是建连期并发突发）？
   **这两者性质完全不同**，是本次测量最关键的判别点。
3. **`http2.Transport` 是否真的"每 host 一条连接"**：
   这是 `HTTP_MULTI_CONN_ROUNDROBIN_PLAN_0806.md` 的核心假设，**至今未经证实**。

> **依赖关系**：本计划的结论决定 round-robin 改造方案该怎么设计
> （见第 6 节）。**应先做本计划，再定主改造。**

---

## 2. 方案选择：记什么作为「连接身份」

### 2.1 结论：记 `LocalAddr`（本地 IP:端口）

TCP 连接由四元组唯一标识。同一进程内，**本地端口就足以唯一标识一条连接**：

- 同一个 socket 在其整个生命周期内本地端口不变
- 不同的并发连接必然有不同的本地端口（内核保证）

因此 `info.Conn.LocalAddr().String()`（形如 `192.168.88.239:41234`）
就是一个完备的连接标识。

### 2.2 备选方案对比

| 方案 | 评价 |
|---|---|
| **`LocalAddr` 字符串（推荐）** | ✅ 完备、可读、零额外状态<br>✅ 能与 `ss` / `tcpdump` 的输出直接对照<br>❌ 每条日志多 ~20 字节（可接受） |
| `RemoteAddr` | ❌ 同一对端的多条连接 RemoteAddr 完全相同，**无法区分** |
| 自建连接序号（map[Conn]int） | ❌ 需要全局 map + 锁，热路径加锁，且 `net.Conn` 作 key 有生命周期问题 |
| `fmt.Sprintf("%p", info.Conn)` | ❌ 指针值可能被复用，且不可读、无法与外部工具对照 |

### 2.3 同时记录 `Reused` 标志

`httptrace.GotConnInfo.Reused` 直接告诉你这次请求是**复用**了已有连接
还是**触发了新建**。这个字段让"是否扩容"的判断从"事后推断"变成"直接读出"：

```
reused=false 的记录 = 一次新连接的诞生
```

统计 `reused=false` 的出现时刻分布，就能直接看出扩容发生在何时。

### 2.4 服务端（inbound）为什么不加

`InboundLogger`（[httptransport.go:208-229](NFs/amf/internal/accesslog/httptransport.go)）
是 gin 中间件，理论上可以通过 `c.Request.RemoteAddr` 拿到对端地址。

**但本计划不做**，理由：

- 客户端 `LocalAddr` 与服务端 `RemoteAddr` 是**同一个四元组的两端**，信息等价，记一次即可
- 服务端无法识别对端是哪个 NF（现有代码 `src` 已因此记为 `"NaN"`，见 `accesslog.go` 的 `LogHTTPInbound`）
- 保持 inbound 记录不变，分析脚本按 `conn` 字段是否存在即可区分两种视角

---

## 3. 改动范围

| 项 | 内容 |
|---|---|
| **必改文件** | 每个 NF 的 `internal/accesslog/httptransport.go` + `accesslog.go`，共 **7 个 NF × 2 = 14 个文件** |
| **不需要改** | ✅ 任何 consumer/service 代码<br>✅ openapi 外部模块（无需 fork / replace）<br>✅ 任何 `config/*.yaml`<br>✅ `InboundLogger` 及其 7 个注册点 |

### 3.1 已验证的前提（`diff` 实测，非记忆）

| 前提 | 验证结果 |
|---|---|
| 7 份 `httptransport.go` 完全一致 | ✅ 全部 IDENTICAL |
| `LogHTTP` 只有 7 个调用点，都在 `httptransport.go:121` | ✅ `grep -rn "LogHTTP("` 确认 |
| 非 AMF 的 6 份 `accesslog.go` 除 `srcNF` 外一致 | ✅ ausf/udm/udr/pcf/nrf/nssf 相互等价 |
| **AMF 的 `accesslog.go` 与其他 6 个不同** | ⚠️ AMF 多了 `LogWorker`/`SBIView`，`LogHTTP` 在 :318 而非 :287 |

> **因此复制策略是不对称的**：
> - `httptransport.go` → 7 份完全相同，**可直接复制**
> - `accesslog.go` → **不能整份复制**，必须逐份改（或只复制非 AMF 的 6 份，AMF 单独改）

### 3.2 覆盖范围说明

本改动覆盖所有走 `accesslog.Client()` 的出向 HTTP，即
**AMF / AUSF / UDM / UDR / PCF / NRF / NSSF** 七个 NF 之间的 SBI 调用。

以下路径**不在覆盖范围**（它们不走 accesslog，本次不改）：

- `NFs/pcf/internal/sbi/consumer/bsf_service.go:30` — 裸 `&http.Client{}`（PCF→BSF）
- `NFs/smf/internal/sbi/consumer/bsf_service.go:28` — 裸 `&http.Client{}`（SMF→BSF）
- `NFs/nef/internal/sbi/consumer/*.go` — `http.DefaultClient`（4 处）
- SMF 出向：SMF 没有 `internal/accesslog/` 目录，未接入

> 用户已确认只关心 **AMF / AUSF / UDM / UDR / PCF**，上述路径均不涉及这五者之间的通信。
> （AMF→SMF 方向走 accesslog，**会**被记录；SMF 出向不会。）

---

## 4. 具体改动

### 4.1 Step 1：`httptransport.go` — 新增 `GotConn` 回调

文件：`NFs/amf/internal/accesslog/httptransport.go`

#### 1a. 扩展局部变量声明（:103）

```go
// 改造前
var wroteTime, gotFirstByte time.Time

// 改造后
var wroteTime, gotFirstByte time.Time
// connID identifies the TCP connection this request was sent on, as
// "localIP:localPort". The local port uniquely identifies a socket within this
// process for the socket's whole lifetime, so grouping log lines by connID
// recovers exactly which requests shared a connection.
//
// connReused reports whether the transport handed us an existing connection
// (true) or had to establish a new one (false). Every "false" is the birth of a
// new connection, so the timestamps of the false records show WHEN the pool grew
// — which is what distinguishes genuine load-driven expansion from a burst of
// dials at start-up.
var connID string
var connReused bool
```

#### 1b. 在 `ClientTrace` 中新增回调（:104-113）

```go
trace := &httptrace.ClientTrace{
    // GotConn fires once, after the transport has picked (or dialled) the
    // connection for this request and before the request is written. Like the
    // other two callbacks it must not log or block: it runs on the calling
    // goroutine here, but keeping all three uniform (stamp a local, nothing
    // else) is what makes them safe. The record is enqueued after RoundTrip
    // returns, on the normal asynchronous path.
    GotConn: func(info httptrace.GotConnInfo) {
        if info.Conn != nil {
            connID = info.Conn.LocalAddr().String()
        }
        connReused = info.Reused
    },
    WroteRequest: func(httptrace.WroteRequestInfo) {
        if wroteTime.IsZero() {
            wroteTime = time.Now()
        }
    },
    GotFirstResponseByte: func() {
        gotFirstByte = time.Now()
    },
}
```

> **为什么不像 `wroteTime` 那样做 `IsZero()` 保护**：
> `WroteRequest` 的保护是为了在**重试**时保留第一次的值（见 :100-102 的注释）。
> 对 `GotConn` 恰恰相反——重试意味着换了连接，我们**想要**最后一次的值，
> 因为那才是真正送达请求的那条连接。所以这里直接覆盖是正确的。

#### 1c. 传给 `LogHTTP`（:121）

```go
// 改造前
LogHTTP(dst, method, uri, ueID, reqTime, wroteTime, gotFirstByte, respTime)

// 改造后
LogHTTP(dst, method, uri, ueID, connID, connReused, reqTime, wroteTime, gotFirstByte, respTime)
```

> 参数位置：把 `connID`/`connReused` 放在 `ueID` 之后、时间戳之前，
> 让"标识类"参数聚在一起，与现有签名风格一致。

---

### 4.2 Step 2：`accesslog.go` — 扩展 `LogHTTP`

文件：`NFs/amf/internal/accesslog/accesslog.go`（AMF 在 :318，其余 6 个在 :287）

#### 2a. 新增一个 bool 版的 JSON helper

放在 `appendKV` 附近：

```go
// appendKVBool appends a boolean-valued JSON field. The value is emitted
// unquoted so downstream analysis reads it as a real boolean, not a string.
func appendKVBool(b []byte, key string, val bool, first bool) []byte {
    if !first {
        b = append(b, ',')
    }
    b = appendJSONString(b, key)
    b = append(b, ':')
    if val {
        return append(b, "true"...)
    }
    return append(b, "false"...)
}
```

> 不需要新增任何 import。

#### 2b. 改 `LogHTTP` 签名与函数体

```go
// 签名：新增 connID / connReused 两个参数
func LogHTTP(dstNF, method, uri, ueID, connID string, connReused bool,
    reqTime, wroteTime, gotFirstByte, respTime time.Time,
) {
    b := make([]byte, 0, 288)          // 256 → 288，容纳新增的两个字段
    b = append(b, '{')
    b = appendKV(b, "src", srcNF, true)
    b = appendKV(b, "dst", dstNF, false)
    b = appendKV(b, "method", method, false)
    b = appendKV(b, "uri", uri, false)
    b = appendKV(b, "ue_id", ueID, false)
    b = appendKV(b, "conn", connID, false)              // ← 新增
    b = appendKVBool(b, "conn_reused", connReused, false) // ← 新增
    b = appendKV(b, "req_time", formatTime(reqTime), false)
    b = appendKV(b, "wrote_time", formatTimeOrEmpty(wroteTime), false)
    b = appendKV(b, "got_first_byte", formatTimeOrEmpty(gotFirstByte), false)
    b = appendKV(b, "resp_time", formatTime(respTime), false)
    b = appendDurUs(b, "latency_us", respTime.Sub(reqTime))
    b = append(b, '}')
    enqueue(kindHTTP, b)
}
```

#### 2c. 同步更新函数上方的文档注释

现有注释逐个说明了每个参数（AMF 版在 :302-317）。新增两行：

```
//   - connID:       the TCP connection this request went out on, as
//     "localIP:localPort". Empty if the transport failed before obtaining one.
//   - connReused:   true if an existing connection was reused, false if this
//     request caused a new connection to be established.
```

> **注意 `connID` 可能为空**：如果拨号失败，`GotConn` 根本不会触发，
> `connID` 保持零值 `""`。这与 `wrote_time` 的空值语义一致，
> 分析脚本应把空 `conn` 视为"连接建立失败"。

---

### 4.3 Step 3：同步到其余 6 个 NF

**`httptransport.go` 可以直接复制**（7 份完全一致，且不含 NF 名）：

```bash
cd /path/to/Free5gc-TYcustom
for nf in ausf udm udr pcf nrf nssf; do
  cp NFs/amf/internal/accesslog/httptransport.go \
     NFs/$nf/internal/accesslog/httptransport.go
done
```

**`accesslog.go` 不能整份复制**（AMF 版含 `LogWorker`，且 `srcNF` 各不相同）。
两种做法二选一：

**做法 A（推荐）**：先改 `ausf`，再复制到其余 5 个，最后 `sed` 修正 `srcNF`：

```bash
# ausf 改好后
for nf in udm udr pcf nrf nssf; do
  cp NFs/ausf/internal/accesslog/accesslog.go NFs/$nf/internal/accesslog/accesslog.go
  # 修正 srcNF（大写 NF 名）
  UP=$(echo $nf | tr 'a-z' 'A-Z')
  sed -i "s/^const srcNF = \"AUSF\"$/const srcNF = \"$UP\"/" NFs/$nf/internal/accesslog/accesslog.go
done
# AMF 单独手改（它多了 LogWorker）
```

**做法 B**：7 份全部手改（只有 3 处小改动，也不算慢，且不会误伤 AMF）。

**改完必须验证 `srcNF` 没被写坏**：

```bash
for nf in amf ausf udm udr pcf nrf nssf; do
  printf "%-6s " "$nf"; grep 'const srcNF' NFs/$nf/internal/accesslog/accesslog.go
done
```

期望输出 7 行，各自是 `"AMF"` `"AUSF"` `"UDM"` `"UDR"` `"PCF"` `"NRF"` `"NSSF"`。
**这一步不能省**——`srcNF` 写错会让整份日志的 src 字段失效，且不会报编译错误。

---

### 4.4 Step 4：编译验证

```bash
for nf in amf ausf udm udr pcf nrf nssf; do
  (cd NFs/$nf && go build ./... ) || echo "BUILD FAIL: $nf"
done
```

> 改了 `LogHTTP` 签名，如果某个 NF 漏改了调用点，**编译会直接报错**——
> 这是好事，编译器帮你兜底。

---

## 5. 分析方法

跑一次实验后，用以下命令回答第 1.4 节的三个问题。
（假设日志已从各 pod 收集，UDM 的日志为 `UDM_HTTP_log.txt`）

### 5.1 问题一：一对 NF 之间用了几条 TCP？

```bash
# UDM→UDR 全程用过的不同连接数
grep '"src":"UDM"' UDM_HTTP_log.txt | grep '"dst":"UDR"' \
  | grep -o '"conn":"[^"]*"' | sort -u | wc -l
```

### 5.2 问题二：每条连接承载了多少请求？

```bash
grep '"src":"UDM"' UDM_HTTP_log.txt | grep '"dst":"UDR"' \
  | grep -o '"conn":"[^"]*"' | sort | uniq -c | sort -rn
```

**判读**：
- 一条连接吃掉绝大部分请求，其余几条只有零星几次 → 那几条是**瞬时抖动**，不是稳定扩容
- 请求较均匀地分布在 N 条上 → 是**真正的稳定多连接**

### 5.3 问题三（最关键）：扩容发生在什么时刻？

```bash
# 每条新连接诞生的时刻（conn_reused=false 即新建）
grep '"src":"UDM"' UDM_HTTP_log.txt | grep '"dst":"UDR"' \
  | grep '"conn_reused":false' \
  | grep -o '"conn":"[^"]*"\|"req_time":"[^"]*"' \
  | paste - - | sort -k2
```

**判读**（这是本次测量的核心判别）：

| 观察 | 结论 | 对主改造的影响 |
|---|---|---|
| 所有 `reused=false` 都集中在实验最初几十毫秒 | 只是**建连期并发突发**，稳态是固定连接数 | 主改造前提基本成立 |
| `reused=false` 随实验进行**持续出现** | 是**负载驱动的自动扩容** | **主改造必须加 `StrictMaxConcurrentStreams: true`**，否则连接数不可控 |

### 5.4 交叉验证：与请求量的关系

之前 `ss` 数据自相矛盾的地方（UDM 负载大却连接少、PCF 负载小却连接多），
用日志重新核对：

```bash
# 各 NF-pair 的请求总量
for f in *_HTTP_log.txt; do
  echo "=== $f ==="
  grep -o '"dst":"[^"]*"' $f | sort | uniq -c | sort -rn
done
```

请求量与连接数应当呈**正相关**。若仍然反常，说明还有未知因素，需进一步排查。

---

## 6. 与 round-robin 主改造的关系

`HTTP_MULTI_CONN_ROUNDROBIN_PLAN_0806.md` 的第 1 节断言
**"每个 NF-pair 恰好 1 条 TCP"**，该断言**目前没有证据支持**——
它依赖 `x/net/http2` 的内部行为，而本机没有 Go module cache，无法读源码核实。

本计划的结果将直接决定主改造的设计：

| 本计划结论 | 主改造应如何调整 |
|---|---|
| 稳态确实只有 1 条 | 主改造前提成立，按原计划实施（round-robin 到 2 个 transport） |
| 存在自动扩容 | 光加 round-robin **不够**——每个 transport 仍会各自扩容，结果是"2×浮动值"，实验组和对照组的连接数都不确定，**失去对照意义**。必须同时设 `StrictMaxConcurrentStreams: true` 把连接数锁死为 2 |

> **建议**：先合入本计划、跑一次实验、拿到结论，再回头定主改造。
> 这两件事有先后依赖，不应并行。

---

## 7. 风险评估

| 风险 | 评估 | 处理 |
|---|---|---|
| `GotConn` 回调影响热路径性能 | 极低。只做一次字符串取值和一次 bool 赋值，无锁无 I/O | 无需处理 |
| `LocalAddr()` 调用开销 | 极低。`net.TCPConn.LocalAddr()` 返回已缓存的地址对象，`String()` 有一次小分配 | 可接受；若实测有影响可改为只在首次记录 |
| 日志体积增大 | 每行 +~40 字节（`conn` ~25B + `conn_reused` ~20B）。原每行约 250B，增幅 ~16% | 队列容量 `1<<21` 有充足余量，无需调整 |
| 改签名漏改某个 NF | **编译期即报错**，不会静默失败 | Step 4 逐个 build |
| `srcNF` 被 `sed` 写坏 | **不会报编译错**，是真实风险 | Step 3 末尾的验证命令**必须执行** |
| 端口复用导致 connID 重复 | 理论上连接关闭后端口可被复用。实验时长仅数秒，且内核有 TIME_WAIT 保护，实际不会发生 | 长实验时可结合 `req_time` 区分 |

---

## 8. 检查清单

- [ ] `amf/httptransport.go`：加 `connID`/`connReused` 变量 + `GotConn` 回调 + 改 `LogHTTP` 调用
- [ ] `amf/accesslog.go`：加 `appendKVBool` + 改 `LogHTTP` 签名/函数体/文档注释
- [ ] `ausf/accesslog.go`：同上（作为其余 5 个的复制源）
- [ ] `httptransport.go` 复制到 `ausf udm udr pcf nrf nssf`
- [ ] `accesslog.go` 复制到 `udm udr pcf nrf nssf` 并 `sed` 修正 `srcNF`
- [ ] **验证 7 个 `srcNF` 全部正确**（Step 3 末尾命令）
- [ ] 7 个 NF 逐个 `go build ./...` 通过
- [ ] 重建镜像并部署
- [ ] 跑一次实验（3 秒即可，日志是逐请求记录的，不依赖采样密度）
- [ ] 按 5.1 / 5.2 / 5.3 分析，得出「是否自动扩容」的结论
- [ ] 按 5.4 交叉验证请求量与连接数是否正相关
- [ ] 据结论更新 `HTTP_MULTI_CONN_ROUNDROBIN_PLAN_0806.md` 第 1 节与方案设计
