# 锁竞争 + goroutine 调度 Profiling 运行手册（0826）

> **要回答的问题**：`HTTP_latency_pipeline.html` 里 transport T 随 RQ 涨 22×（0.92 → 20.41 ms），
> 其中 ①+② 占 86%。这 86% 到底是**等锁**还是**等调度**？
>
> - **第一部分（block profile）** → 量出 `cc.reqHeaderMu` / `cc.wmu` / 等 stream 配额 三处阻塞的总耗时
> - **第二部分（sched latency）** → 量出 goroutine 调度的总耗时
>
> 两者单位都是 **goroutine·秒**，测的状态不重叠（`_Gwaiting` vs `_Grunnable`），**可以直接相加**。
>
> 环境沿用 `AMF_MUTEX_PROFILING_GUIDE.md`：namespace `free5gc`，deployment 名 `free5gc-<nf>`。

---

## 0. 代码改动

### 0.1 四个 NF 的 `cmd/main.go`

| NF | 文件 | 状态 |
|---|---|---|
| AMF | `NFs/amf/cmd/main.go` | ✅ **已完成**，无需改动（L39–L45） |
| AUSF | `NFs/ausf/cmd/main.go` | ❌ 待加 |
| UDM | `NFs/udm/cmd/main.go` | ❌ 待加 |
| UDR | `NFs/udr/cmd/main.go` | ❌ 待加 |

AUSF / UDM / UDR 三个文件，import 增加：

```go
import (
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof handlers on the default mux
	"runtime"
	...
)
```

`main()` 里 `defer func(){ recover() ... }()` 之后、`app := cli.NewApp()` 之前，插入：

```go
	// --- Lock-contention + scheduler profiling (TYcustom, 0826) ---
	runtime.SetMutexProfileFraction(5) // sample ~1/5 of mutex contention events
	runtime.SetBlockProfileRate(10000) // sample a blocking event ~every 10us blocked
	go func() {
		if err := http.ListenAndServe("0.0.0.0:6060", nil); err != nil {
			logger.MainLog.Warnf("pprof server on :6060 exited: %v", err)
		}
	}()
```

和 AMF 那份逐字一致，方便四个 NF 对照。

### 0.2 UDM / UDR 额外加一个文件（第二部分才需要）

新建 `NFs/udm/internal/accesslog/schedstat.go` 和 `NFs/udr/internal/accesslog/schedstat.go`，
内容完全相同（沿用 accesslog 包「每个 NF 一份相同拷贝」的惯例）：

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

// Go runtime 只对每个 goroutine 第 8 次转换出 _Grunning 之后的那次记录 runnable
// 时长（runtime/proc.go: gTrackingPeriod = 8），所以直方图里的 count 约为真实调度
// 次数的 1/8，总时长要 ×8 才是实际值。
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

// RegisterSchedStat 把 /debug/schedstat 注册到 pprof 用的同一个 default mux 上。
// 必须在 main.go 启动 :6060 之前调用。
func RegisterSchedStat() {
	http.HandleFunc("/debug/schedstat", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(SchedSnapshot())
	})
}
```

然后在 UDM / UDR 的 `main.go` 里，`ListenAndServe` **之前**加一行
（import `"github.com/free5gc/udm/internal/accesslog"` / `".../udr/internal/accesslog"`）：

```go
	accesslog.RegisterSchedStat()
```

### 0.3 重新 build 镜像

按 `cloudlab/K8s/UPDATE_free5gc_custom_image.md` 走。

---

## 1. 前置验证（别跳过）

四个 NF 各开一个终端做 port-forward，本地端口错开：

```bash
kubectl port-forward -n free5gc deployment/free5gc-amf  6061:6060
kubectl port-forward -n free5gc deployment/free5gc-ausf 6062:6060
kubectl port-forward -n free5gc deployment/free5gc-udm  6063:6060
kubectl port-forward -n free5gc deployment/free5gc-udr  6064:6060
```

验证：

```bash
for p in 6061 6062 6063 6064; do
  echo "== $p =="
  curl -s http://localhost:$p/debug/pprof/ | grep -c block   # 期望 >0
done

# UDM / UDR 还要验证第二部分的端点
curl -s http://localhost:6063/debug/schedstat
curl -s http://localhost:6064/debug/schedstat
# 期望： {"wall":"2026-...","count":12345,"total_sec":0.0234,"goroutines":87}
```

任何一个不通 → apply 的是旧镜像，重新编译。

---

## 2. 实验时序（两部分共用）

**核心原则：block profile 和 sched latency 都是「进程启动以来的累计值，只增不减」，
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

抓取脚本：

```bash
cat > ~/snap.sh <<'SNAPEOF'
#!/usr/bin/env bash
# 用法: ~/snap.sh <标签>     例: ~/snap.sh RQ1500_PRE
LABEL="$1"; [ -z "$LABEL" ] && { echo "需要标签"; exit 1; }
OUT="$HOME/prof_0826"; mkdir -p "$OUT"
for np in amf:6061 ausf:6062 udm:6063 udr:6064; do
  nf=${np%%:*}; port=${np##*:}
  curl -s "http://localhost:$port/debug/pprof/block" > "$OUT/block_${nf}_${LABEL}.pb.gz"
  curl -s "http://localhost:$port/debug/pprof/mutex" > "$OUT/mutex_${nf}_${LABEL}.pb.gz"
done
for np in udm:6063 udr:6064; do
  nf=${np%%:*}; port=${np##*:}
  curl -s "http://localhost:$port/debug/schedstat" > "$OUT/sched_${nf}_${LABEL}.json"
done
echo "snap $LABEL -> $OUT"
SNAPEOF
chmod +x ~/snap.sh
```

对每个 RQ 点（800 / 1000 / 1500 / 2000 / 2500）重复一遍上面的时序，
标签用 `RQ1500_PRE` / `RQ1500_POST`。

---

## 3. 先算「总预算」——所有分析都拿它做分母

从本次实验的 `HTTP_log_RQ<rate>_UE1000.txt` 里取某个 NF 对（重点是 UDM→UDR）：

```
总预算 (goroutine·秒) = 该 NF 对的请求数 x 平均 transport T
```

例：RQ1500 的 UDM→UDR = 9000 次 × 10.76 ms = **96.8 goroutine·秒**。

下面两部分量出来的每一项，除以它就是**该项在 T 里的占比**。

> 单位是 goroutine·秒，不是墙钟秒。0.9 秒的实验里出现 96.8 goroutine·秒是正常的 ——
> 意味着平均约 107 个 goroutine 同时卡在 RoundTrip 里。

---

# 第一部分：block profile —— 两把锁消耗了多少时间

## 1.1 测什么

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

## 1.2 拿到什么结果

对每个 NF 做差：

```bash
cd ~/prof_0826
go tool pprof -top -sample_index=delay -nodecount=30 \
  -base block_udm_RQ1500_PRE.pb.gz block_udm_RQ1500_POST.pb.gz
```

输出形如（`delay` 单位纳秒，加起来就是 goroutine·秒）：

```
      flat  flat%   sum%        cum   cum%
    45.2s  46.7%  46.7%      45.2s  46.7%  x/net/http2.(*ClientConn).writeRequest
    12.1s  12.5%  59.2%      12.1s  12.5%  x/net/http2.(*ClientConn).awaitOpenSlotForStreamLocked
     3.4s   3.5%  62.7%       3.4s   3.5%  x/net/http2.(*ClientConn).writeHeaders
```

看具体栈用 `-peek='writeRequest'`。

## 1.3 怎么分析

**① 按调用栈对号入座**（block profile 是按栈聚合的）：

| 栈里出现的函数 | 对应哪一项 |
|---|---|
| `(*ClientConn).writeRequest` 里的 select | **reqHeaderMu 排队** |
| `awaitOpenSlotForStreamLocked` / `cc.cond.Wait` | **等 stream 配额**（撞 250 上限） |
| `(*ClientConn).writeHeaders` / `writeFrame` 附近的 `sync.Mutex` | **wmu** |
| `(*serverConn).serve` / `wantWriteFrameCh` | 服务端响应侧的 channel 排队（对应 ④） |

**② 填表**（UDM，RQ1500）：

```
UDM→UDR 总预算                     =  96.8  goroutine·秒   (100%)
├─ reqHeaderMu 排队                =    ?                    ?%
├─ 等 stream 配额                  =    ?                    ?%
└─ wmu                             =    ?                    ?%
```

**③ 判读**：

- `reqHeaderMu` 占比大（>30%）→ 坐实文档 §3.1 的假设：队头阻塞在发送端大锁。
  这时 §6 的 A 项（M 打点）非做不可，用来拆出 per-connection 归因。
- `等 stream 配额` 占比大 → 撞的是 250 stream 上限，不是锁本身。
  对策完全不同：调 `MaxConcurrentStreams` 或加 `connsPerPeer`，而不是拆锁。
- `wmu` 占比小（预期）→ 和 §3.1 的结论一致：「wmu 决定吞吐上限，reqHeaderMu 决定尾延迟」。
- **三项加起来仍远小于总预算** → 大头既不在锁也不在配额，去看第二部分。

**④ 横向对比（最有说服力的一步）**：把同一项在 RQ800 / 1500 / 2500 三点列出来。
如果 `reqHeaderMu` 占比随 RQ 单调上升、而 UDR 侧 handler 相关项保持平坦，
就完全对上了 §5 那张「按 NF 对流量排序」的图。

## 1.4 注意事项

- `delay` 是**所有 goroutine 阻塞时长之和**，会远大于 0.9 秒墙钟时间 —— 正常，
  它和总预算同单位、可直接比。
- rate=10000 的含义是「≥10 µs 的阻塞必记，更短的按概率抽样」。
  **短事件的绝对值不精确，长尾是准的** —— 对找队头阻塞正合适。
- block profile 的栈里**没有对端地址**，分不出 UDM→UDR 还是 UDM→NRF。
  要分 peer 只能靠 per-request 打点（M 点）。这是它的边界。

---

# 第二部分：sched latency —— goroutine 调度消耗了多少时间

## 2.1 测什么

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

## 2.2 拿到什么结果

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

UDM 和 UDR 各算一遍。

## 2.3 怎么分析

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

**④ UDM vs UDR 对比**：
UDR 的调度总时间明显高于 UDM → 瓶颈偏被调方；反之偏调用方。
配合 §4 里「③ UDR handler 只涨 3.4× 而 transport 涨 22×」一起看。

## 2.4 注意事项

- 这个数是**整个进程所有 goroutine** 的总和，包含日志 writer、mongo driver、
  prometheus 等不在关键路径上的 goroutine。所以它是**上界**，
  不能直接说成「20.41 ms 里有 X ms 是调度」。
- 但作为**排除性证据**非常强：如果上界就只有 3%，调度可以彻底不用再考虑。
- 要按 goroutine 种类拆开（区分 handler goroutine 和日志 goroutine），
  才需要上 `runtime/trace`（`curl ':6060/debug/pprof/trace?seconds=3'`，
  0.9 秒的实验只有几十 MB，不贵）。**先看上界，多半用不上。**

---

## 4. 最终产出：一张归因表

每个 RQ 点填一行，UDM / UDR 各一张（单位统一 goroutine·秒）：

| RQ | 总预算 | reqHeaderMu | stream 配额 | wmu | 调度 | 已归因合计 | 剩余未知 |
|---|---|---|---|---|---|---|---|
| 800 | | | | | | | |
| 1000 | | | | | | | |
| 1500 | 96.8 | | | | | | |
| 2000 | | | | | | | |
| 2500 | | | | | | | |

这张表填完，`HTTP_REQHEADERMU_START_LOG_PLAN_0826.md` 里 M / G / W 三个点要不要加、
加在哪、要不要补配对的 goroutine 入口点，答案就是自明的：

- 哪一项占比最大 → 打点优先补在那里
- 「剩余未知」很大 → 瓶颈在还没测的地方（服务端 ④ 的 `writeSched` / `writingFrame` 闸门），
  那就必须 fork x/net/http2 了
