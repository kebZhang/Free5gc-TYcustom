# 五个 NF 的 block profile + sched profile 埋点计划（0826）

> **本文档的重点是「加代码」**：给 **AMF / AUSF / UDM / UDR / PCF** 五个 NF
> 补齐两套可用于 profiling 的代码：
>
> | 要加的东西 | 用来量什么 | 靠什么实现 |
> |---|---|---|
> | **block profile**（+ mutex profile 作旁证） | `cc.reqHeaderMu` / `cc.wmu` / 等 stream 配额 三处阻塞的总耗时 | `runtime.SetBlockProfileRate` + `net/http/pprof`，**不改任何库** |
> | **sched profile**（goroutine 调度延迟） | goroutine 从 `_Grunnable` 到 `_Grunning` 的总等待 | `runtime/metrics` 的 `/sched/latencies:seconds`，自己挂一个 `/debug/schedstat` |
>
> 两套都是**零 fork、零热路径改动**：只在 `cmd/main.go` 开采样 + 起 pprof 端口，
> 外加一个只读的 HTTP handler。**不涉及** `HTTP_REQHEADERMU_START_LOG_PLAN_0826.md`
> 里的 M / G / W 打点，也不需要 fork `golang.org/x/net/http2`。
> 那份计划要不要做、做哪几个点，正是靠本文档量出来的数据决定。
>
> **要回答的问题**：`HTTP_latency_pipeline.html` 里 transport T 随 RQ 涨 22×（0.92 → 20.41 ms），
> 其中 ①+② 占 86%。这 86% 到底是**等锁**还是**等调度**？
>
> - **第一部分（block profile）** → 等锁的 goroutine·秒
> - **第二部分（sched profile）** → 等调度的 goroutine·秒
>
> 两者单位都是 **goroutine·秒**，测的状态不重叠（`_Gwaiting` vs `_Grunnable`），**可以直接相加**。
>
> 环境沿用 `AMF_MUTEX_PROFILING_GUIDE.md`：namespace `free5gc`，deployment 名 `free5gc-<nf>`。

---

## 0. 前提事实：一个 NF 对，默认只有 **一条** TCP 连接

这条事实是整个分析的地基，先记牢再往下看。

代码依据 `NFs/<nf>/internal/accesslog/httptransport.go`（amf / ausf / nrf / nssf / pcf / udm / udr
七份**逐字节相同**）：

```go
const connsPerPeer = 1          // L95，commit 9ebf444「update NF-NF 1 TCP conn -- 0826 4PM」
```

- `tls` / `clear` 是 `[connsPerPeer]http.RoundTripper` 数组，每个槽是一个**独立的**
  `http2.Transport`，各自持有私有 connPool、按 `host:port` 分键。
- 槽位是 **per-process 而非 per-peer**：这一个 transport 服务本 NF 的所有 peer，
  所以 **每个 (本 NF 进程 → peer 地址) = 1 条 h2c 连接**。
- 有方向性：UDM→UDR 是 UDM 进程开的 1 条；UDR→UDM 的 callback 是 UDR 进程自己另开的 1 条。
- `connsPerPeer = 1` 时 round-robin 退化，日志里 `conn_slot` 恒为 0。
- 全部走 h2c（`config/*cfg.yaml` 均为 `scheme: http`），`tls` 池在本部署中是死代码。

**对本次 profiling 的三个直接后果：**

1. **`cc.reqHeaderMu` 是 per-ClientConn 的**。一个 NF 对只有一条连接 ⇒
   该 NF 对的**全部**请求排在**同一把** `reqHeaderMu` 上。
   这正是 `HTTP_latency_pipeline.html` §3.2「一条连接上的 4 个串行点」画的那张图，
   也是 block profile 在这里特别有意义的原因 —— 争用被最大程度集中，信号最强。
2. **250 stream 上限基本不会成为阻塞点**（详见 §5.3），别把它当默认嫌疑人。
3. **block profile 的栈里没有对端地址**。UDM 进程的 `reqHeaderMu` 数字是
   UDM→UDR 和 UDM→NRF **两条连接的和**，拆不开。这是它的硬边界，处理办法见 §5.4。

> 实验开始前，用 `HTTP_log.txt` 复核一次这个前提（比 pprof 便宜得多）。
> 字段名以 `accesslog.go` 的 `LogHTTP` 为准：`src` / `dst` / `conn` / `conn_slot` / `conn_reused`。
>
> ```bash
> # 每个 NF 对实际用了几条 socket（期望 1）
> jq -r 'select(.src != "NaN") | (.src + "->" + .dst) + "\t" + .conn' \
>   HTTP_log_RQ1500_UE1000.txt | sort -u | cut -f1 | uniq -c
>
> # 连接池是否扩容过（期望：每个 NF 对恰好 1 条 conn_reused=false，即首次建连）
> jq -r 'select(.src != "NaN" and .conn_reused == false) | .src + "->" + .dst' \
>   HTTP_log_RQ1500_UE1000.txt | sort | uniq -c
> ```
>
> 若某个 NF 对出现 >1 条 socket，说明该对撞到了 250 in-flight stream 上限、
> 连接池自行扩容了（`StrictMaxConcurrentStreams` 未设置，见 §5.3）。
> **这会改变 §5.3 的判读，必须先记下来再分析 profile。**

---

## 1. 要加的代码（这一节是本计划的主体）

### 1.1 现状盘点

| NF | `cmd/main.go` 里的 pprof | `internal/accesslog/schedstat.go` | 本次要做 |
|---|---|---|---|
| AMF  | ✅ 已有（L33–L45） | ❌ 无 | 加 schedstat.go + 1 行注册 |
| AUSF | ❌ 无 | ❌ 无 | 全套 |
| UDM  | ❌ 无 | ❌ 无 | 全套 |
| UDR  | ❌ 无 | ❌ 无 | 全套 |
| PCF  | ❌ 无 | ❌ 无 | 全套 |

五个 NF 都已存在 `internal/accesslog` 包（`accesslog.go` + `httptransport.go`），
新文件直接放进去即可，沿用该包「每个 NF 一份完全相同的拷贝」的惯例。

### 1.2 新增文件：`NFs/<nf>/internal/accesslog/schedstat.go`

**五份完全相同**（文件里没有任何 NF 相关的常量，可以直接 `cp`）。
路径：`NFs/{amf,ausf,udm,udr,pcf}/internal/accesslog/schedstat.go`

```go
package accesslog

import (
	"encoding/json"
	"math"
	"net/http"
	"runtime/metrics"
	"sync"
	"time"
)

// SchedTrackingPeriod: Go runtime 只对每个 goroutine 第 8 次转换出 _Grunning
// 之后的那次记录 runnable 时长（runtime/proc.go: gTrackingPeriod = 8），所以
// 直方图里的 count 约为真实调度次数的 1/8，总时长要 x8 才是实际值。
const SchedTrackingPeriod = 8

var (
	schedMu      sync.Mutex
	schedSamples = []metrics.Sample{
		{Name: "/sched/latencies:seconds"},
		{Name: "/sched/goroutines:goroutines"},
	}
)

// SchedSnap 是某一时刻的累计量（进程启动以来，只增不减）。
type SchedSnap struct {
	Wall       string  `json:"wall"`
	Count      uint64  `json:"count"`      // 被采样到的 runnable->running 次数
	TotalSec   float64 `json:"total_sec"`  // 被采样到的 goroutine·秒（未 x8）
	Goroutines uint64  `json:"goroutines"` // 当前存活 goroutine 数
}

// SchedSnapshot 读一次 runtime/metrics 并把直方图折成标量。
// 只在被 curl 时调用，不在任何热路径上。
func SchedSnapshot() SchedSnap {
	schedMu.Lock()
	defer schedMu.Unlock()

	metrics.Read(schedSamples)
	h := schedSamples[0].Value.Float64Histogram()

	s := SchedSnap{Wall: time.Now().UTC().Format(time.RFC3339Nano)}
	for i, c := range h.Counts {
		if c == 0 {
			continue
		}
		lo, hi := h.Buckets[i], h.Buckets[i+1]
		if math.IsInf(lo, -1) {
			lo = hi // 第 0 桶下界是 -Inf
		}
		if math.IsInf(hi, +1) {
			hi = lo // 末桶上界是 +Inf
		}
		s.Count += c
		s.TotalSec += float64(c) * (lo + hi) / 2
	}
	s.Goroutines = schedSamples[1].Value.Uint64()
	return s
}

// schedOnce 保证 handler 只注册一次：http.HandleFunc 对同一 pattern 注册两次
// 会 panic，加锁比依赖调用方自律安全。
var schedOnce sync.Once

// RegisterSchedStat 把 /debug/schedstat 注册到 pprof 用的同一个 default mux 上。
// 必须在 main.go 起 :6060 的 ListenAndServe 之前调用。
func RegisterSchedStat() {
	schedOnce.Do(func() {
		http.HandleFunc("/debug/schedstat", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(SchedSnapshot())
		})
	})
}
```

> 注意：`accesslog` 包有 `func init() { Init() }`（`accesslog.go` L93），
> 从 `cmd/main.go` import 它会启动 access-log 的 writer goroutine。
> 但五个 NF 的 consumer 代码本来就已经 import 了 accesslog，**没有新增副作用**。

### 1.3 `cmd/main.go` 的改动

#### (a) AUSF / UDM / UDR / PCF —— 需要全套

**import 增加 4 行**（stdlib 组按字典序插入，free5gc 组把 `accesslog` 放在 `logger` 前）：

```go
import (
	"context"
	"net/http"                 // + 新增
	_ "net/http/pprof"         // + 新增：注册 /debug/pprof 到 default mux
	"os"
	"os/signal"
	"path/filepath"
	"runtime"                  // + 新增
	"runtime/debug"
	"syscall"

	"github.com/urfave/cli/v2"

	"github.com/free5gc/<nf>/internal/accesslog" // + 新增
	"github.com/free5gc/<nf>/internal/logger"
	"github.com/free5gc/<nf>/pkg/factory"
	"github.com/free5gc/<nf>/pkg/service"
	logger_util "github.com/free5gc/util/logger"
	"github.com/free5gc/util/version"
)
```

`<nf>` 取值与插入点（`main()` 里 `defer func(){ recover() ... }()` 之后、
`app := cli.NewApp()` 之前）：

| NF | module 路径 | `}()` 结束行 | `app := cli.NewApp()` 行 |
|---|---|---|---|
| AUSF | `github.com/free5gc/ausf` | L36 | L38 |
| UDM  | `github.com/free5gc/udm`  | L26 | L28 |
| UDR  | `github.com/free5gc/udr`  | L28 | L30 |
| PCF  | `github.com/free5gc/pcf`  | L40 | L42 |

在该处插入（**四个 NF 逐字一致**，和 AMF 那份也一致，方便对照）：

```go
	// --- Lock-contention + scheduler profiling (TYcustom, 0826) ---
	// 开启 mutex + block 采样，并在 :6060 暴露 pprof。配合 RegisterSchedStat
	// 提供的 /debug/schedstat，实验前后各抓一次快照做差值。
	// 采样很便宜，端点是只读 pull-only；生产环境如不想多开端口再摘掉。
	runtime.SetMutexProfileFraction(5) // sample ~1/5 of mutex contention events
	runtime.SetBlockProfileRate(10000) // sample a blocking event ~every 10us blocked
	accesslog.RegisterSchedStat()      // 必须在 ListenAndServe 之前
	go func() {
		if err := http.ListenAndServe("0.0.0.0:6060", nil); err != nil {
			logger.MainLog.Warnf("pprof server on :6060 exited: %v", err)
		}
	}()
```

> PCF 的 `main()` 用 `fmt.Printf` 报 `app.Run` 的错，但 `logger` 包已经 import
> 并在 `action()` 里用了，所以上面的 `logger.MainLog.Warnf` 在 PCF 里同样能编译，
> 不要为它单独换成 `fmt`。

#### (b) AMF —— 只补两处

`NFs/amf/cmd/main.go` 已有采样和 `:6060`（L33–L45），只需要：

1. import 组加一行 `"github.com/free5gc/amf/internal/accesslog"`（放在 `internal/logger` 前）；
2. 在 L41 的 `go func() {` **之前**插入一行：

```go
	accesslog.RegisterSchedStat()      // 必须在 ListenAndServe 之前
```

改完后 AMF 的 profiling 块与另外四个 NF 逐字相同。

### 1.4 编译自查

```bash
for nf in amf ausf udm udr pcf; do
  ( cd NFs/$nf && go build ./... ) || echo "BUILD FAIL: $nf"
done
```

`go vet ./internal/accesslog/` 也顺手跑一遍，主要看 import 分组有没有被 gofmt 打回。

### 1.5 重新 build 镜像

按 `cloudlab/K8s/UPDATE_free5gc_custom_image.md` 走。
注意 NRF 必须先于其它 NF 起来（该文档记录的三个 gotcha 之一）。

---

## 2. 前置验证（别跳过）

五个 NF 各开一个终端做 port-forward，本地端口错开：

```bash
kubectl port-forward -n free5gc deployment/free5gc-amf  6061:6060
kubectl port-forward -n free5gc deployment/free5gc-ausf 6062:6060
kubectl port-forward -n free5gc deployment/free5gc-udm  6063:6060
kubectl port-forward -n free5gc deployment/free5gc-udr  6064:6060
kubectl port-forward -n free5gc deployment/free5gc-pcf  6065:6060
```

验证两套端点都活着：

```bash
for p in 6061 6062 6063 6064 6065; do
  echo "== $p =="
  curl -s http://localhost:$p/debug/pprof/ | grep -c block   # 期望 >0
  curl -s http://localhost:$p/debug/schedstat                # 期望一行 JSON
done
# schedstat 期望： {"wall":"2026-...","count":12345,"total_sec":0.0234,"goroutines":87}
```

再确认采样真的开了（rate=0 会静默返回空 profile，最容易踩的坑）：

```bash
for p in 6061 6062 6063 6064 6065; do
  echo -n "$p block: "
  curl -s "http://localhost:$p/debug/pprof/block?debug=1" | sed -n '2p'
done
# 期望看到非零的 "contentions/delay" 行；全 0 说明 SetBlockProfileRate 没生效
```

任何一个不通 → apply 的是旧镜像，重新编译。

> 另外在**分析机**（装了 Go 工具链的那台）上还有一步必做的准备：
> 把 `x/net/http2` 里两个阻塞点的行号 grep 出来。见 **§5.2 步骤 1**。
> 没有这个行号，block profile 的结果无法区分“等锁”和“等响应”。

---

## 3. 实验时序（两部分共用）

**核心原则：block profile 和 sched profile 都是「进程启动以来的累计值，只增不减」，
所以必须在实验前后各抓一次快照，用差值才是这一次实验的量。**

```
① 跑一次 warm-up：200 UE                    <- 避开进程冷启动（首次 GC、线程池扩张、建连）
② 等 2 秒
③ 抓 PRE 快照
④ 启动 PacketRusher：1000 UE @ 0.67ms       <- 正式实验，约 0.9 秒
⑤ 等 2 秒（让在途请求收尾）
⑥ 抓 POST 快照
⑦ dereg
⑧ 再抓一次快照，作为下一组的 PRE 基线        <- dereg 本身也产生锁等待，不能混进下一组
```

> 实验只有 0.9 秒（实测：RQ800 = 1286 ms，RQ1500 = 900 ms，RQ2500 = 798 ms），
> 所以 **① 的 warm-up 不能省** —— 否则测到的一大半是 Go 进程的冷启动，不是负载本身。
>
> warm-up 还有第二个作用：`connsPerPeer = 1` 的那条连接必须在 PRE 之前就建好。
> 否则建连（TCP 握手 + SETTINGS 交换）会被算进这次实验的阻塞里。

抓取脚本（五个 NF 全量抓 block / mutex / sched）：

```bash
cat > ~/snap.sh <<'SNAPEOF'
#!/usr/bin/env bash
# 用法: ~/snap.sh <标签>     例: ~/snap.sh RQ1500_PRE
LABEL="$1"; [ -z "$LABEL" ] && { echo "需要标签"; exit 1; }
OUT="$HOME/prof_0826"; mkdir -p "$OUT"
for np in amf:6061 ausf:6062 udm:6063 udr:6064 pcf:6065; do
  nf=${np%%:*}; port=${np##*:}
  curl -s "http://localhost:$port/debug/pprof/block" > "$OUT/block_${nf}_${LABEL}.pb.gz"
  curl -s "http://localhost:$port/debug/pprof/mutex" > "$OUT/mutex_${nf}_${LABEL}.pb.gz"
  curl -s "http://localhost:$port/debug/schedstat"   > "$OUT/sched_${nf}_${LABEL}.json"
done
echo "snap $LABEL -> $OUT"
SNAPEOF
chmod +x ~/snap.sh
```

对每个 RQ 点（800 / 1000 / 1500 / 2000 / 2500）重复一遍上面的时序，
标签用 `RQ1500_PRE` / `RQ1500_POST`。

---

## 4. 先算「总预算」——所有分析都拿它做分母

从本次实验的 `HTTP_log_RQ<rate>_UE1000.txt` 里取某个 NF 对：

```
总预算 (goroutine·秒) = 该 NF 对的请求数 x 平均 transport T
```

例：RQ1500 的 UDM→UDR = 9000 次 × 10.76 ms = **96.8 goroutine·秒**。

五个 NF 涉及的主要 NF 对（每个方向各 1 条连接）：

| 调用方 | 被调方 | 备注 |
|---|---|---|
| AMF | AUSF / UDM / PCF / NSSF / NRF | AMF 是扇出最大的一个 |
| AUSF | UDM / NRF | |
| UDM | **UDR** / NRF | 重点观察对象 |
| PCF | UDR / AMF(notification) / NRF | PCF 这次新纳入 |
| UDR | NRF | UDR 主要是被调方 |

下面两部分量出来的每一项，除以总预算就是**该项在 T 里的占比**。

> 单位是 goroutine·秒，不是墙钟秒。0.9 秒的实验里出现 96.8 goroutine·秒是正常的 ——
> 意味着平均约 107 个 goroutine 同时卡在 RoundTrip 里。

---

# 第一部分：block profile —— 两把锁消耗了多少时间

## 5.1 测什么

`golang.org/x/net/http2` 客户端发送路径上的三个阻塞点：

| 阻塞点 | 底层类型 | mutex profile | block profile |
|---|---|---|---|
| `cc.reqHeaderMu` | **`chan struct{}`**（容量 1 的信号量 channel） | ❌ **看不到** | ✅ |
| 等 stream 配额 `awaitOpenSlotForStreamLocked` | `cc.cond.Wait()` | ❌ | ✅ |
| `cc.wmu` | `sync.Mutex` | ✅ | ✅ |

> **关键**：`reqHeaderMu` 是 channel 不是 Mutex（要能被 ctx 取消，而 `sync.Mutex.Lock()` 没法 select），
> 所以 **mutex profile 完全看不到它**，必须用 block profile。mutex profile 只作为 `wmu` 的旁证。
>
> 上机前确认 v0.47.0 的实现：
> ```
> grep -n "reqHeaderMu\|wmu \|cc.cond\|awaitOpenSlotForStream" \
>   $(go env GOMODCACHE)/golang.org/x/net@v0.47.0/http2/transport.go
> ```

## 5.2 拿到什么结果

### ⚠️ 必须加 `-lines`：`writeRequest` 一个函数里有两个阻塞点

`(*clientStream).writeRequest` 这**同一个函数**里，有两处会让 goroutine 停住：

| 位置 | 卡在等什么 | 量级 |
|---|---|---|
| 函数**开头**的 `select` | 抢 `cc.reqHeaderMu` —— **本节唯一要量的东西** | 待测 |
| 函数**结尾**的 `for { select { case <-cs.respHeaderRecv … } }` | 等对端 NF 把响应发回来（正常往返） | ≈ 整个 transport T |

`go tool pprof` **默认按函数聚合**，会把这两处加成一行 `…writeRequest` 报出来，
而第二项在数值上压倒性地大。**不加 `-lines`，等于把整条链路的正常往返时间
当成了锁竞争**，本节的结论直接作废。

`-lines` 让 pprof 改成**按行号聚合**，两处才会分成两行。

### 步骤 1：查出 v0.47.0 两个阻塞点的行号（做一次，记下来）

在**分析机**（装了 Go 工具链的那台）上：

```bash
grep -n "reqHeaderMu <- struct{}{}\|respHeaderRecv\|awaitOpenSlotForStreamLocked\|cc.wmu.Lock()" \
  $(go env GOMODCACHE)/golang.org/x/net@v0.47.0/http2/transport.go
```

- `case cc.reqHeaderMu <- struct{}{}:` 所在行号记为 **L_lock**
  （`HTTP_REQHEADERMU_START_LOG_PLAN_0826.md` §4.1 记录约 L1424–1430，以实际 grep 为准）
- 函数结尾那个等 `cs.respHeaderRecv` 的 select 行号记为 **L_resp**

> pprof 报的是 `select` 语句本身那一行，可能比 `case` 行小 1 —— 两行都记下，
> 对不上时取靠近的那个。

### 步骤 2：按行号出报表（五个 NF 各跑一遍）

```bash
cd ~/prof_0826
go tool pprof -lines -top -sample_index=delay -nodecount=40 \
  -base block_udm_RQ1500_PRE.pb.gz block_udm_RQ1500_POST.pb.gz
```

输出会从「函数名」变成「函数名 + 文件:行号」：

```
      flat  flat%   sum%        cum   cum%
    43.9s  45.3%  45.3%      43.9s  45.3%  x/net/http2.(*clientStream).writeRequest transport.go:1512    <- L_resp 等响应，忽略
     1.3s   1.3%  46.6%       1.3s   1.3%  x/net/http2.(*clientStream).writeRequest transport.go:1425    <- L_lock reqHeaderMu，要的就是这行
     0.0s   0.0%  46.6%       0.0s   0.0%  x/net/http2.(*ClientConn).awaitOpenSlotForStreamLocked transport.go:1668
     0.3s   0.3%  46.9%       0.3s   0.3%  x/net/http2.(*clientStream).encodeAndWriteHeaders transport.go:1571
```

**只取 `transport.go:L_lock` 那一行的 flat 值**，那才是 `reqHeaderMu` 的排队时间，
填进 §5.3 ② 的表和 §7 的归因表。

### 步骤 3（可选，更直观）：逐行标注源码，不用对行号

分析机上如果有 x/net 源码（`go mod download golang.org/x/net` 过），直接把
`writeRequest` 整个函数带 delay 打出来：

```bash
go tool pprof -sample_index=delay \
  -base block_udm_RQ1500_PRE.pb.gz block_udm_RQ1500_POST.pb.gz \
  -list='writeRequest'
```

左边一列就是每行贡献的 delay，`case cc.reqHeaderMu <- struct{}{}:` 那一行一眼可见。
第一次分析建议先跑这个确认行号，之后再用步骤 2 批量出数。

### 步骤 4：批量导出（五个 NF × 每个 RQ 点）

```bash
cat > ~/blockrep.sh <<'REPEOF'
#!/usr/bin/env bash
# 用法: ~/blockrep.sh RQ1500
RQ="$1"; [ -z "$RQ" ] && { echo "需要 RQ 标签，如 RQ1500"; exit 1; }
OUT="$HOME/prof_0826"
for nf in amf ausf udm udr pcf; do
  echo "########## $nf $RQ ##########"
  go tool pprof -lines -top -sample_index=delay -nodecount=40 \
    -base "$OUT/block_${nf}_${RQ}_PRE.pb.gz" "$OUT/block_${nf}_${RQ}_POST.pb.gz" \
    2>/dev/null | grep -E "http2|flat%"
done
REPEOF
chmod +x ~/blockrep.sh
~/blockrep.sh RQ1500 | tee ~/prof_0826/blockrep_RQ1500.txt
```

> **行号信息会不会被 strip 掉？不会。** Go 的 block profile 是 **runtime 自己在生成
> `pb.gz` 时就把函数名 / 文件 / 行号写进 proto 的**，依据是 pclntab 而不是 DWARF，
> 所以镜像即使用 `-ldflags="-s -w"` 编译也照样有行号。

### 两个不用管的干扰项（都不影响锁的数字）

**(a) 同一次请求的「等响应」被记了两遍。**
`cc.roundTrip` 起了 `go cs.doRequest(req)` 之后，它自己也 select 等同一个响应，
两个 goroutine 各记一次。**这不影响锁** —— `reqHeaderMu` 只有写请求那个 goroutine
会去抢，另一个从头到尾不碰它。唯一后果是：**别拿 `-top` 的总计去除以 §4 的总预算**，
那个百分比会虚高一倍多。分子只能用步骤 2 里 `transport.go:L_lock` 那一行的值。

**(b) 空闲 goroutine 停在 channel 上等活干也算 block。**
`accesslog` 的 `writerLoop`、http2 的 `(*serverConn).serve`、mongo driver 连接池、
各种 ticker，两次干活之间的空转会被整段计入 block profile。它们是**完全不同的
函数和行号**，不污染锁那一行，只污染总计 —— 所以 §7 那张表的「剩余未知」列
按「总预算 − 已归因」是算不出有意义的值的，只填各项的绝对值和占总预算的比例。

## 5.3 怎么分析

**① 按调用栈 + 行号对号入座**（block profile 按栈聚合；加 `-lines` 后细到行）：

| 栈里出现的函数（+ 行号） | 对应哪一项 |
|---|---|
| `(*clientStream).writeRequest` 且 **行号 = L_lock**（见 §5.2 步骤1） | **reqHeaderMu 排队** —— 不加 `-lines` 拿不到这一行 |
| `(*clientStream).writeRequest` 且 **行号 = L_resp** | 等对端回包的正常往返 —— **不计入锁，直接忽略** |
| `awaitOpenSlotForStreamLocked` / `cc.cond.Wait` | 等 stream 配额 —— **正常情况下应接近 0，见下方警告** |
| `(*ClientConn).writeHeaders` / `writeFrame` 附近的 `sync.Mutex` | **wmu** |
| `(*serverConn).serve` / `wantWriteFrameCh` | 服务端响应侧的 channel 排队（对应 ④） |

> ⚠️ **关于「等 stream 配额」这一项，本文档旧版的判读是错的，已更正：**
>
> 当前代码 **没有** 设置 `StrictMaxConcurrentStreams`（保持默认 `false`，
> 见 `httptransport.go` L121–L139 的长注释）。在 non-strict 模式下，
> `clientConnPool.getClientConn` → `cc.ReserveNewRequest()` → `idleStateLocked()`
> 会先做 `streamsReserved + len(streams) + 1 <= maxConcurrentStreams` 判断；
> **撞到 250 的请求在"预约"阶段就被挡下，连接池直接 dial 一条新连接**，
> 根本走不到 `awaitOpenSlotForStreamLocked` 去 `cond.Wait()`。
>
> 所以：
> - **这一项接近 0 = 符合预期**，不是采样失败。
> - **这一项占比大 = §0 的前提被打破了，先去查 `conn_reused`**，
>   而不是直接下结论说"撞了 250 上限"。可能的原因：
>   (a) 服务端 SETTINGS 帧尚未到达，客户端临时用的是
>       `initialMaxConcurrentStreams = 100` 而非 250（冷启动窗口，warm-up 应已覆盖）；
>   (b) 预约与实际建流之间的竞态窗口；
>   (c) 有人把 `StrictMaxConcurrentStreams` 改成了 true。
> - 真要提高 250 这个上限，只能显式设置服务端的 `http2.Server.MaxConcurrentStreams`
>   （`NFs/<nf>/internal/sbi/server.go` 的 `newHttp2ServerWithIdleTimeout` 目前
>   只设了 `IdleTimeout`，`MaxConcurrentStreams` 全仓库无一处设置，故取
>   `x/net/http2` 默认的 `defaultMaxStreams = 250`）。客户端无法自行协商更大。
>
> 另外一个**放大效应**值得记住（`HTTP_latency_pipeline.html` §3.1）：
> `awaitOpenSlotForStreamLocked` 内部的 `cc.cond.Wait()` 会释放 `cc.mu`，
> 但 **不释放 `reqHeaderMu`**。所以万一它真的阻塞了，整条连接的队头会一起被堵住，
> `reqHeaderMu` 那一项也会同步暴涨 —— 两项同时变大是同一个病因，不要重复计数。

**② 填表**（UDM，RQ1500）：

```
UDM→UDR 总预算                     =  96.8  goroutine·秒   (100%)
├─ reqHeaderMu 排队                =    ?                    ?%
├─ 等 stream 配额                  =    ?                    ?%   <- 期望 ≈ 0
└─ wmu                             =    ?                    ?%
```

**③ 判读**：

- `reqHeaderMu` 占比大（>30%）→ 坐实 `HTTP_latency_pipeline.html` §3.1 的假设：
  队头阻塞在发送端大锁。**在 `connsPerPeer = 1` 下这是最可能的结果** ——
  整个 NF 对的全部请求排在同一把锁上。
  这时 §6 的 A 项（M 打点）非做不可，用来拆出 per-connection 归因。
- `等 stream 配额` 占比大 → 见上方 ⚠️，先复核连接数前提，别急着调参。
- `wmu` 占比小（预期）→ 和 §3.1 的结论一致：「wmu 决定吞吐上限，reqHeaderMu 决定尾延迟」。
- **三项加起来仍远小于总预算** → 大头既不在锁也不在配额，去看第二部分。

**④ 横向对比（最有说服力的一步）**：把同一项在 RQ800 / 1500 / 2500 三点列出来。
如果 `reqHeaderMu` 占比随 RQ 单调上升、而 UDR 侧 handler 相关项保持平坦，
就完全对上了 `HTTP_latency_pipeline.html` §5 那张「按 NF 对流量排序」的图。

**⑤ 五 NF 横向对比（这次加 PCF 的意义）**：
AMF 扇出 5 个 peer、PCF 扇出 3 个、UDR 几乎只被调。
如果 `reqHeaderMu` 的绝对值大致正比于「该 NF 发出的请求数」，
说明瓶颈是**每连接一把锁**这个结构本身，而不是某个 NF 的业务代码。

## 5.4 注意事项

- `delay` 是**所有 goroutine 阻塞时长之和**，会远大于 0.9 秒墙钟时间 —— 正常，
  它和总预算同单位、可直接比。
- rate=10000 的含义是「≥10 µs 的阻塞必记，更短的按概率抽样」。
  **短事件的绝对值不精确，长尾是准的** —— 对找队头阻塞正合适。
- **block profile 的栈里没有对端地址**，分不出 UDM→UDR 还是 UDM→NRF。
  这是它的硬边界。两个办法：
  - **近似拆分**：按 `HTTP_log.txt` 里该 NF 各 peer 的请求数占比加权分摊。
    只在各 peer 的平均 T 量级接近时才成立 —— UDM→NRF 是低频心跳、
    UDM→UDR 是高频热路径，两者差一个量级，**这个近似在 UDM 上并不可靠**，
    只能用来判断「NRF 那条肯定不是大头」。
  - **精确拆分**：只能靠 per-request 打点（`HTTP_REQHEADERMU_START_LOG_PLAN_0826.md`
    的 M 点）。要不要做，正是本表填完后要回答的问题。
- 采样开销：`SetBlockProfileRate(10000)` + `SetMutexProfileFraction(5)` 在 0.9 秒
  实验里的额外开销可忽略，但**对照组仍要做**：至少跑一次「镜像已加代码但
  rate 设为 0」的 A/B，确认端到端 registration 延迟没有系统性偏移。

---

# 第二部分：sched profile —— goroutine 调度消耗了多少时间

## 6.1 测什么

`/sched/latencies:seconds`：goroutine 从**进入 `_Grunnable`（已就绪）**到
**真正在 P 上开始跑（`_Grunning`）**的等待时间。

对应 `HTTP_latency_pipeline.html` 里的：

- 步骤 3　客户端 `go cs.doRequest()` 等调度排上 P
- 步骤 13　服务端 handler goroutine 等调度排上 P
- 步骤 21 / 25　readLoop 唤醒、调用者 goroutine 唤醒（整个 ⑤）

**和第一部分不重叠**：

```
_Grunning --阻塞在锁/channel--> _Gwaiting --被唤醒--> _Grunnable --拿到 P--> _Grunning
                               \________/           \__________/
                               第一部分测这段         第二部分测这段
                               "在等锁"               "已就绪，在排队等 CPU"
```

一个 goroutine 抢 `reqHeaderMu` 等了 5 ms，被唤醒后又等 0.2 ms 才拿到 P
→ 第一部分记 5 ms，第二部分记 0.2 ms，**相加 = 5.2 ms，不重复计算**。

## 6.2 拿到什么结果

`~/snap.sh` 已经在 PRE / POST 各存了一份 JSON：

```json
// sched_udm_RQ1500_PRE.json
{"wall":"2026-08-26T09:12:03.101Z","count":184213,"total_sec":1.8422,"goroutines":91}
// sched_udm_RQ1500_POST.json
{"wall":"2026-08-26T09:12:06.447Z","count":297540,"total_sec":3.1078,"goroutines":94}
```

**换算**（手算即可）：

```
调度次数     = (297540 - 184213) x 8       = 906,616 次
调度总时间   = (3.1078 - 1.8422) x 8       = 10.12  goroutine·秒
平均单次调度 = 10.12 / 906616               = 11.2 µs
```

> `x8` 是因为 Go runtime 只对每个 goroutine 第 8 次转换做记录（`gTrackingPeriod = 8`）。
> 上机确认：`grep -n "gTrackingPeriod" $(go env GOROOT)/src/runtime/proc.go`

五个 NF 各算一遍。

## 6.3 怎么分析

**① 和总预算比（唯一真正要做的判断）**：

| 调度总时间 / 总预算 | 结论 |
|---|---|
| **< 5%** | 调度不是瓶颈。①② 的 17.5 ms 全部归给锁和连接级串行点。**不需要为测调度去 fork x/net/http2**，按 §6 的 A/B 项加 M/G/W 打点即可。 |
| **5% ~ 30%** | 调度有份但不是主因。先把第一部分的锁拆干净，调度留作二阶因素。 |
| **> 30%** | 调度是一等公民。需要给 M / G 补上配对的 goroutine 入口时间戳（`doRequest` 第一行、`runHandler` 第一行），才能把「调度」和「抢锁」分开。 |

**② 看平均单次调度延迟随 RQ 怎么变**：

- 一直在几十 µs、不随 RQ 涨 → 调度器健康，goroutine 一就绪就能跑。
  那 ①② 的涨完全是「等锁」，不是「等 CPU」。
- 从几十 µs 涨到毫秒级 → P 不够用，goroutine 在 runq 里堆积。
  这时配合 `GODEBUG=schedtrace=1000` 看 `idleprocs` 和 `runqueue`
  （`idleprocs=0` 且 `runqueue` 持续 >0 = 真的没 P 了）。

**③ 看调度次数随 RQ 怎么变**：
如果次数涨得比请求数快很多，说明每个请求引发了更多次上下文切换
（典型原因：goroutine 反复在锁上阻塞-唤醒）—— 这本身就是锁竞争的间接证据。
**在 `connsPerPeer = 1` 下这一条特别有指示性**：一条连接、一把 `reqHeaderMu`，
每次锁移交都是一对「唤醒 + 调度」，次数应该与该 NF 对的请求数同阶。

**④ 调用方 vs 被调方对比**：
UDR 的调度总时间明显高于 UDM → 瓶颈偏被调方；反之偏调用方。
配合 §4 里「③ UDR handler 只涨 3.4× 而 transport 涨 22×」一起看。
PCF 这次纳入后可以多一组对照：PCF 既是被调方（AMF→PCF）又是调用方（PCF→UDR），
它自己的调用侧和被调侧数字如果差异很大，方向性结论就更硬。

## 6.4 注意事项

- 这个数是**整个进程所有 goroutine** 的总和，包含 access-log writer、mongo driver、
  prometheus 等不在关键路径上的 goroutine。所以它是**上界**，
  不能直接说成「20.41 ms 里有 X ms 是调度」。
- 但作为**排除性证据**非常强：如果上界就只有 3%，调度可以彻底不用再考虑。
- 要按 goroutine 种类拆开（区分 handler goroutine 和日志 goroutine），
  才需要上 `runtime/trace`（`curl ':6060/debug/pprof/trace?seconds=3'`，
  0.9 秒的实验只有几十 MB，不贵）。**先看上界，多半用不上。**
- `/debug/schedstat` 自己也要跑一次 `metrics.Read`，抓快照那一下会有微秒级停顿。
  只在 PRE / POST 各调一次，不要在实验进行中轮询。

---

## 7. 最终产出：一张归因表

每个 RQ 点填一行，五个 NF 各一张（单位统一 goroutine·秒）：

| RQ | 总预算 | reqHeaderMu | stream 配额 | wmu | 调度 | 已归因合计 | 剩余未知 |
|---|---|---|---|---|---|---|---|
| 800 | | | | | | | |
| 1000 | | | | | | | |
| 1500 | 96.8 | | ≈0 | | | | |
| 2000 | | | | | | | |
| 2500 | | | | | | | |

外加一张贯穿五个 NF 的横表（RQ1500 固定），用来看结构性规律：

| NF | 发出请求数 | peer 数（=连接数） | reqHeaderMu | 调度 | reqHeaderMu / 请求数 |
|---|---|---|---|---|---|
| AMF | | 5 | | | |
| AUSF | | 2 | | | |
| UDM | | 2 | | | |
| PCF | | 3 | | | |
| UDR | | 1 | | | |

这两张表填完，`HTTP_REQHEADERMU_START_LOG_PLAN_0826.md` 里 M / G / W 三个点要不要加、
加在哪、要不要补配对的 goroutine 入口点，答案就是自明的：

- 哪一项占比最大 → 打点优先补在那里
- 「剩余未知」很大 → 瓶颈在还没测的地方（服务端 ④ 的 `writeSched` / `writingFrame` 闸门），
  那就必须 fork x/net/http2 了
- `reqHeaderMu / 请求数` 在五个 NF 之间接近常数 → 是每连接一把锁的结构问题，
  下一步该动的是 `connsPerPeer`，而不是继续加打点
