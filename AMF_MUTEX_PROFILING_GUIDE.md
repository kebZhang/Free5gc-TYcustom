# AMF 锁竞争 Profiling 运行手册

> 目的：在 CPU 未饱和的情况下，定位「AMF-local latency 随 req rate 上升」的真正原因。
> 用 Go 自带的 mutex / block profiling，直接看出是**哪一把锁（头号嫌疑：全 UE 共享表 `UePool`/`RanUePool`）在排队**，
> 还是根本没有热锁（→ 真凶是 goroutine 集中唤醒的调度开销）。
>
> 本手册全程**纯命令行**，不需要任何图形界面，适合服务器 / SSH 环境。

---

## 0. 前置：代码改动（已完成）

改动位于 [`NFs/amf/cmd/main.go`](NFs/amf/cmd/main.go)：

- import 增加 `net/http`、`_ "net/http/pprof"`、`runtime`
- `main()` 开头增加三行，开启锁/阻塞采样并在 `:6060` 暴露 pprof：

```go
runtime.SetMutexProfileFraction(5) // 采样 ~1/5 的锁竞争事件
runtime.SetBlockProfileRate(10000) // 采样阻塞事件（~每阻塞 10us 记一次）
go func() {
    if err := http.ListenAndServe("0.0.0.0:6060", nil); err != nil {
        logger.MainLog.Warnf("pprof server on :6060 exited: %v", err)
    }
}()
```

采样开销很低、端口只读，可长期开着。生产环境如不想暴露端口，事后移除或加开关即可。

---

## 关键原理（务必理解，否则数据会不干净）

**mutex/block profile 是「从进程启动到此刻的累计值，只增不减，不会自动清零」。**

因此要看「某一段（某个 RQ 的 reg）单独造成了多少锁等待」，必须：
**在这一段的前后各打一个快照，然后用 `go tool pprof -base 前快照 后快照` 相减。**

dereg 本身也是信令、也碰 `UePool`、也产生锁等待。所以 **dereg 后必须补一个快照**，作为下一组 reg 的干净基线，否则下一组 reg 会混入上一组的 dereg。

---

## 1. 验证镜像里 profiling 真的生效（最重要，别跳过）

pod 起来后先做这个，不通则后面全部白跑。

```bash
# ① 找到 AMF pod
kubectl get pods -n <namespace> | grep amf

# ② 端口转发（开一个终端，全程不要关）
kubectl port-forward -n <namespace> <amf-pod-name> 6060:6060
#    显示 "Forwarding from 127.0.0.1:6060 -> 6060" 即成功

# ③ 另开一个终端，验证 pprof 通不通
curl -s http://localhost:6060/debug/pprof/ | head -20
```

- 看到 `mutex / block / goroutine / heap` 等条目 → ✅ 生效，继续。
- 连接被拒 / 空白 / 404 → ❌ apply 的是旧镜像（没含改动）。
  **重新编译带改动的 AMF 镜像 → 重新 apply → 回到 ①。**

> `kubectl port-forward` 直连 pod，不需要 k8s Service 暴露 6060。

---

## 2. 准备抓取脚本（在执行 curl 的机器上）

```bash
mkdir -p ~/amf_mutex && cd ~/amf_mutex

cat > snap.sh <<'EOF'
#!/usr/bin/env bash
# 用法: ./snap.sh <标签>    例: ./snap.sh S0_baseline
LABEL="$1"
if [ -z "$LABEL" ]; then echo "需要一个标签,如 ./snap.sh S0_baseline"; exit 1; fi
TS=$(date +%H%M%S)
curl -s http://localhost:6060/debug/pprof/mutex > "mutex_${LABEL}.pb.gz"
curl -s http://localhost:6060/debug/pprof/block > "block_${LABEL}.pb.gz"
echo "[$TS] 已抓取: mutex_${LABEL}.pb.gz  block_${LABEL}.pb.gz"
ls -l "mutex_${LABEL}.pb.gz"
EOF
chmod +x snap.sh
```

---

## 3. 跑实验 + 打 5 个快照（严格按顺序）

每一步括号说明「在什么时刻执行」。

```bash
# ===== S0：pod 已起来，还没跑任何 UE reg 时 =====
./snap.sh S0_baseline

# ===== 跑 RQ200 的 1000 UE reg（你平时的 PacketRusher 命令）=====
# ...启动 RQ200 UE1000 reg...
# 等所有 UE 注册完成、进入注册态停留后：
./snap.sh S1_afterRQ200reg

# ===== 对 RQ200 这组做 dereg =====
# ...执行 RQ200 dereg...  等 dereg 全部完成后：
./snap.sh S2_afterRQ200dereg

# ===== 等一会，跑 RQ1000 的 1000 UE reg =====
# ...启动 RQ1000 UE1000 reg...  等注册完成后：
./snap.sh S3_afterRQ1000reg

# ===== （可选）RQ1000 dereg，如果也想分析 dereg =====
# ...执行 RQ1000 dereg...
./snap.sh S4_afterRQ1000dereg
```

产物：`mutex_S0_baseline.pb.gz` … `mutex_S3_afterRQ1000reg.pb.gz`（及对应 `block_`）。

**提醒：S2（dereg 后）必须打**，它是 RQ1000 的干净基线。

> 更省心的替代方案：**每组 RQ 单独重启一次 AMF pod**，则每组从零累计，
> 连 dereg 快照都不用管：`重启 → S0基线 → RQ200 reg → S1 → 对比 S1-S0`，
> 再 `重启 → S0'基线 → RQ1000 reg → S1' → 对比 S1'-S0'`。代价是多重启一次。

---

## 4. 分析：相邻快照相减（纯命令行，无需图形界面）

需要一台装了 Go 的机器（`go tool pprof` 随 Go 自带）。本机没 Go 就把 `.pb.gz` 拷到有 Go 的机器。

### 4.1 交互方式（SSH 终端即可）

```bash
cd ~/amf_mutex

# RQ200 reg 净贡献 = S1 - S0
go tool pprof -base mutex_S0_baseline.pb.gz mutex_S1_afterRQ200reg.pb.gz
```
进入后依次输入：
```
top20          # 等待时间最长的前 20 个锁点
list AmfUe     # (可选) 看 UePool/AmfUe 相关的具体代码行
quit
```

```bash
# RQ1000 reg 净贡献 = S3 - S2
go tool pprof -base mutex_S2_afterRQ200dereg.pb.gz mutex_S3_afterRQ1000reg.pb.gz
# 同样输入 top20
```

### 4.2 一条命令直接出文本（不进交互，可存文件、可贴给他人判读）

```bash
# 直接打印 top40 并退出，重定向到文本文件
go tool pprof -top -nodecount=40 \
  -base mutex_S2_afterRQ200dereg.pb.gz mutex_S3_afterRQ1000reg.pb.gz \
  > rq1000_mutex_top.txt

cat rq1000_mutex_top.txt
```
`rq1000_mutex_top.txt` 是**纯文本**，可直接阅读，也可贴给他人/AI 判读（无需传二进制）。

> `.pb.gz` 是 gzip 压缩的 protobuf 二进制，肉眼读不了；
> 但 `go tool pprof -top ... > x.txt` 会把它转成纯文本。
> **图形界面（`web`/`svg`）只是可选的调用图，`top`/`list` 已给出全部结论，服务器环境完全够用。**

### 4.3 判读表

对比两组 `top20`：**同一把锁在 RQ1000 里的累计等待，是否远大于 RQ200（几倍～十几倍）。**

| top 第一名指向 | 结论 | 下一步 |
|---|---|---|
| `sync.Map` / `UePool` / `RanUePool` / `AmfRanPool`，且 RQ1000 >> RQ200 | ✅ **头号嫌疑证实：全 UE 共享表在排队** | 对 UePool 做分片(sharded map) |
| `accesslog` / channel 相关 | 是自加日志系统在污染测量 | 优化日志(分片/异步/采样) |
| `scheduler` 的 `s.mu` | dispatch 全局锁 | connToWorker 改 sync.Map |
| **所有锁等待都很小、RQ1000≈RQ200** | ❌ **不是锁的问题** | → 见第 5 节（调度排队） |

---

## 5. 若第 4 节显示「没有热锁」→ 补测调度排队（覆盖嫌疑2）

mutex profile 只记锁。没有热锁，说明真凶是「goroutine 集中唤醒的调度开销」，用两个手段确认：

### 5.1 block profile（已抓）——看 goroutine 卡在哪（含 channel/网络等待）
```bash
go tool pprof -top -nodecount=30 \
  -base block_S2_afterRQ200dereg.pb.gz block_S3_afterRQ1000reg.pb.gz \
  > rq1000_block_top.txt
cat rq1000_block_top.txt
```

### 5.2 schedtrace（不改代码，重启 AMF 时加环境变量）
在 AMF 容器 env 增加：
```yaml
env:
  - name: GODEBUG
    value: "schedtrace=1000"
```
看 AMF 日志里每秒一行 `SCHED ...ms: ... runqueue=N [n1 n2 ...]`。
**高 RQ 时 `runqueue` 与各 P 本地队列变大 → 证实调度排队（goroutine 等着被恢复执行）。**

---

## 6. 已确认 / 待确认（结论边界）

**现有实验日志已证明（硬结论）：**
- 不是 CPU 饱和（RQ1000 最忙 3s 内 ≥80% 利用率的采样仅占 1%，max 88.5%）
- 不是 GC/STW 为主因（慢事件遍布 90% 时间桶，非周期性聚集）
- 不是纯计算变慢（纯计算段 P3 仅涨 1.2×）
- 是「跨 goroutine 挂起/唤醒边界」的段在涨，且随瞬时并发度单调上升（并发 20→240，local 1.5→6.8ms）

**本手册要确认的（现有日志无法区分，必须靠 profile）：**
- 是**锁竞争**（头号嫌疑 `UePool`/`RanUePool` 共享表）→ 第 4 节 mutex profile 点名
- 还是**调度/唤醒开销**（无热锁）→ 第 5 节 block + schedtrace

做完本手册，结论从「推断」变为「实测点名」。

---

## 7. 常见问题速查

- **port-forward 中途断**：重跑该命令即可，不影响 AMF 内累计数据。
- **.pb.gz 只有几百字节**：正常，mutex profile 本就不大；能被 `go tool pprof` 打开即可。
- **抓下来是 0 字节**：那一刻端口没通，重抓（并回第 1 节确认 6060 通）。
- **确认正在采样**：`curl -s "http://localhost:6060/debug/pprof/mutex?debug=1" | head` 有可读文本即在采。
- **本机无 Go**：把 `.pb.gz` 拷到任意有 Go 的机器分析；或用 `-top > x.txt` 导出文本再判读。
