# UDR↔MongoDB 归因埋点方案：新增 UDR_DB_log.txt — 0809 (rev2)

## 目的

DB_log 的 `latency_us` 是**一个总数**，无法区分时间花在 UDR 的 Go 侧还是 mongod 侧。
本方案在 UDR 新增 `UDR_DB_log.txt`，**每条 mongo driver 命令一行**，记录 driver 贴着
socket 量到的耗时，并通过 `call_id` 与 DB_log **严格一一对应**（不依赖时间）：

```
latency_us (DB_log)  −  Σ duration_ns (同 call_id 的所有命令)  =  纯 Go 侧开销
```

### 背景（R6525_NF8HTTP_idle500ms_0809 + ...0809v1 两批数据已确立）

- Little's Law 精确吻合；latency 是并发度 N 的单一函数，**与 RQ 无关**（排除外部干扰）
- 吞吐在 N>30 后饱和于 ~24-30k op/s → 有效并发度 **c ≈ 6-8**
- 纯服务时间未退化（p1: 0.275→0.292 ms）；增长 **100% 来自排队**
- **0809v1 新增 mongod 采集，推翻了"mongod 很闲"的推测**：
  mongod CPU 552%→1400%（5.5→14 核），是 UDR 的两倍；
  但**单位成本恒定**（每 1000 op/s 耗 0.64-0.76 核，五档几乎不变）、
  峰值 4843%/12800% 远未触顶、线程数 366→405 全程远超并发需求 140
  → **mongod 未饱和，但也不闲**
- UDR CPU 337%→732%，同样线性、未饱和
- **两侧都不是"资源不足"** → c≈8 是结构性的
- **剩余问题**：1.02→2.00 ms 的增量中，多少在 socket-to-socket（mongod 段），
  多少在 Go 侧（BSON 编解码 + 连接获取 + goroutine 调度）← 本方案要回答

---

## 一、原理：一次调用，两把尺子

```
callID := atomic.AddInt64(&counter, 1)
ctx    := context.WithValue(context.Background(), callIDKey, callID)
start  := time.Now()                   ← 尺子A 起点
   │
   │  ① BSON 序列化                    ┐
   │  ② 从连接池拿连接                 │  Go 侧
   │
   │  ③ ┌─ (CommandStarted) ──────┐    ← 尺子B（driver 提供 Duration）
   │    │   写 socket              │
   │    │   mongod 排队 + 执行      │    这段 = MongoDB
   │    │   读回响应                │
   │    └─ CommandSucceeded(ctx) ─┘      ← ctx 里带着 callID
   │
   │  ④ BSON 反序列化                  │  Go 侧
   │  ⑤ goroutine 等调度上 CPU         ┘
   │
logDB(..., callID) → time.Now()        ← 尺子A 终点 = latency_us
```

- **尺子A**（DB_log）`latency_us` = ①+②+③+④+⑤
- **尺子B**（UDR_DB_log）`duration_ns` = ③（同 callID 可能有多条，需求和）
- **相减** = ①+②+④+⑤ = **纯 Go 侧开销**

### `duration_ns` 精确包含什么

**包含**：socket 写 + mongod 接收/排队/执行 + mongod 写回 + socket 读
（+ driver wire 层组包/解包的亚微秒级开销）

**不包含**（落在差值里，正是怀疑的 Go 侧瓶颈）：
BSON 序列化/反序列化、连接池 checkout、**响应到达后 goroutine 等 scheduler 调度**

### 判读表

| 现象（RQ 800→2500） | 结论 |
|---|---|
| `Σduration_ns` 平坦，差值暴涨 | 瓶颈在 **UDR 的 Go 侧** |
| `Σduration_ns` 自己暴涨 | 瓶颈在 **MongoDB** |
| 两边都涨 | 按比例定量分摊 |

---

## 二、严格一一对应的可行性核查（全部基于实际源码，逐环节验证）

**结论：可行，且完全不依赖时间。** 依据是下面 5 个环节全部验证通过。

### 环节 1 ✅ `mongoapi.Client` 可从外部替换

[free5gc/util v1.3.1 mongoapi.go](https://github.com/free5gc/util/blob/v1.3.1/mongoapi/mongoapi.go)：
```go
var (
	Client *mongo.Client = nil   // ← 导出，可赋值
	dbName string                // ← 未导出
)
func RestfulAPIGetOne(collName string, filter bson.M, argOpt ...interface{}) (...) {
	collection := Client.Database(dbName).Collection(collName)   // ← 每次现取，不缓存 handle
	...
}
```
→ 替换 `mongoapi.Client` 后立即全局生效。
`SetMongoDB` 有幂等保护（`if Client != nil { return nil }`）且 `dbName` 未导出，
故顺序必须是**先 SetMongoDB（设好 dbName），再替换 Client**。

### 环节 2 ✅ driver 把**调用方的 ctx** 原样交给回调

[x/mongo/driver/operation.go](https://github.com/mongodb/mongo-go-driver/blob/v1.17.1/x/mongo/driver/operation.go)：
```go
func (op Operation) publishFinishedEvent(ctx context.Context, info finishedInformation) {
	...
	op.CommandMonitor.Succeeded(ctx, successEvent)   // ← 直接透传 ctx
}
```
`ctx` 就是 `op.Execute(ctx)` 收到的那个。唯一变形是：若 `op.Timeout` 已设，
Execute 会派生一个 timeout context —— **`context.WithTimeout` 保留父 ctx 的所有
Value**，故 `callID` 依然可读。

### 环节 3 ✅ 回调同步、同 goroutine（无 goroutine 派发）

同上文件：`publishStartedEvent` / `publishFinishedEvent` 均为 inline 调用，
**无 `go func()`、无 channel 派发**。
[mongo/collection.go](https://github.com/mongodb/mongo-go-driver/blob/v1.17.1/mongo/collection.go)：
`FindOne`→`find`→`op.Execute(ctx)`；`UpdateOne`→`updateOrReplace`→`op.Execute(ctx)`，
全程无 goroutine spawn。

> 注：本方案用 `call_id` 精确匹配，**已不依赖**此性质。它只是额外的一致性保证
> （回调不会与业务 goroutine 并发写同一累加器）。

### 环节 4 ⚠️ 必须改 mongoapi 让 ctx 传进去 —— 这是唯一的堵点

mongoapi 内部**全部硬编码 `context.TODO()`**：
```go
err = collection.FindOne(context.TODO(), filter, opts).Decode(&result)
collection.UpdateOne(context.TODO(), filter, bson.M{"$set": putData}, opts)
collection.InsertOne(context.TODO(), putData)
```
`context.TODO()` 是空 context，**不携带值、不接受调用方传入**。
→ **必须 vendor util 并加 `replace`**（详见第四节第 5 项）。

### 环节 5 ⚠️ 不能简单地把 ctx 追加进 `argOpt` —— 会破坏 collation

**这是本次核查发现的关键陷阱。** mongoapi 解析 `argOpt` 的方式是：
```go
func getCollation(argOpt ...interface{}) *options.Collation {
	if len(argOpt) == 0 { return nil }
	strength, ok := argOpt[0].(int)     // ← 只看 argOpt[0]！
	if !ok { return nil }
	return &options.Collation{Locale: "en_US", Strength: strength}
}
```

**只检查 `argOpt[0]`**，且类型不符时**静默返回 nil**（不报错、不 panic）。

后果：如果把 ctx 作为**第一个**可变参数塞进去，
`mongoapi.COLLATION_STRENGTH_IGNORE_CASE` 就会被挤到 `argOpt[1]`，
`getCollation` 读到 ctx（不是 int）→ **静默丢弃 collation**。

而 UDR 确实在用 collation（3 处）：
- [provisioned_data_document.go:103](NFs/udr/internal/sbi/processor/provisioned_data_document.go#L103)
- [session_management_subscription_data.go:48](NFs/udr/internal/sbi/processor/session_management_subscription_data.go#L48)
- [smf_registrations_collection.go:25](NFs/udr/internal/sbi/processor/smf_registrations_collection.go#L25)（**注册流程用到，GetMany×1000**）

→ **必须改 `getCollation` 为类型扫描**（遍历 argOpt 找 int），而不是只看 `[0]`；
或改用显式 ctx 形参。见第四节的两个选项。

---

## 三、匹配方案（`call_id` 精确匹配，不依赖时间）

### 匹配规则

**DB_log 的 `call_id` == UDR_DB_log 的 `call_id`，整数相等，一对多。**

```python
# 伪代码
by_call = defaultdict(list)
for cmd in udr_db_log:
    by_call[cmd['call_id']].append(cmd)

for row in db_log:
    cmds = by_call[row['call_id']]          # 精确取出，无歧义
    driver_ns = sum(c['duration_ns'] for c in cmds)
    go_side_ns = row['latency_us']*1000 - driver_ns
```

**不用时间区间、不用 conn_id、不用 coll。** 并发重叠、连接复用、同集合访问
全都不再造成歧义。

> **rev1 的三条件匹配（时间区间 + conn_id + coll）已废弃**，因为它不成立：
> 连接池复用意味着两次不同调用可以先后用到同一条连接；若二者区间重叠且访问
> 同一集合（`GetOne authenticationSubscription` 有 3000 次全是同一集合），
> 则该连接上的命令**同时满足两行的全部三个条件**，归属不唯一。
> 且 `PutOne`/`JSONPatch` 各有 2 条命令，混入他人命令时可能"凑够 2 条"
> 看似匹配成功 —— **静默错误**，校验发现不了。

### 预期命令数（正确性自检，非匹配依据）

| DB_log operation | 预期命令数 | 构成 | 本轮次数 |
|---|---|---|---|
| `GetOne` | 1 | FindOne | 7000 |
| `GetMany` | 1（或 ≥2） | Find (+ 可能的 getMore) | 1000 |
| `PutOne` | **2** | FindOne(查存在性) + UpdateOne/InsertOne | 2000 |
| `JSONPatch` | **2** | FindOne(getOrigData) + UpdateOne | 1000 |

预期 UDR_DB_log 总行数 = 7000×1 + 1000×1 + 2000×2 + 1000×2 = **14 000**

匹配脚本必须输出：
- `call_id` 匹配率（**目标 100%**；<100% 说明有 record 被 drop 或埋点漏了路径）
- 每种 operation 的实际命令数分布 vs 预期（偏差需能解释，如 GetMany 的 getMore）
- 匹配失败的行 → **从统计剔除，不得猜测填补**

> **顺带发现**：`PutOne`(1.78ms) / `JSONPatch`(1.84ms) 基线是 `GetOne`(0.89ms) 的
> 两倍，正因为它们是**两次串行 socket 往返**（mongoapi 内部 read-then-write），
> 不是写操作本身慢。本埋点落地后可直接量化验证。

---

## 四、要改的文件（5 处）

### 1. vendor util + UDR go.mod 加 replace 【新增，必需】

#### 1.1 版本必须精确 —— 已核验

**UDR 用的是 `github.com/free5gc/util v1.3.1`**（[NFs/udr/go.mod:9](NFs/udr/go.mod#L9)），
go.sum 中：
```
github.com/free5gc/util v1.3.1 h1:5j5Exvp42Ow3zNP2aaAp68MSne6BcaCF4/cekXYUS6w=
github.com/free5gc/util v1.3.1/go.mod h1:qsv/ez8YhI+pO8bjNiZWXc2xmRE3XuEIa0EDTCPkSy0=
```

**v1.3.1 对应 commit `13d2e77a01f98044d4243825f9007a6db7fb8166`**（已通过 GitHub
tags API 核验）。本 plan 的全部代码论断均基于**该 commit**（非 main 分支）复核通过：

| 论断 | 状态 |
|---|---|
| `var ( Client *mongo.Client = nil` — 导出的包级变量 | ✅ |
| `getCollation` 只查 `argOpt[0].(int)` | ✅ |
| 驱动调用使用 `context.TODO()` | ✅ |
| `SetMongoDB` 有 `if Client != nil { return nil }` 早返回 | ✅ |
| module path == `github.com/free5gc/util` | ✅ |
| util 自身 `go 1.25.5`（与 [udr/go.mod:3](NFs/udr/go.mod#L3) 的 1.25.5 一致） | ✅ |
| util 依赖 `go.mongodb.org/mongo-driver v1.17.1`（与 UDR 一致） | ✅ |

> ⚠️ **各 NF 的 util 版本并不统一**，vendor 时切勿混用：
> | 版本 | NF |
> |---|---|
> | **v1.3.1** | **udr** ← 本方案目标、ausf, bsf, n3iwf, nef, nssf, pcf, tngf, webconsole |
> | v1.3.2-0.20260204030658-79d56f347175 | amf, nrf |
> | v1.3.2-0.20260331093717-98c25b027c49 | chf |
> | v1.3.2-0.20260107090449-c09baaf75b11 | smf |
> | v1.3.2-0.20260319090834-b2a2938f37b4 | udm |
> | v1.3.2-0.20260228091348-fb7d1127055f | upf |
>
> 必须 vendor **v1.3.1（commit 13d2e77）**，否则 UDR 的 go.sum 校验或 API 会不匹配。

#### 1.2 必须 vendor **完整的 util module**，不能只取 mongoapi 子目录

`replace` 是 **module 级别**的，一旦替换，`github.com/free5gc/util/**` 的所有子包
都从本地目录解析。UDR 实际引用了 4 个子包：

| 子包 | UDR 引用次数 |
|---|---|
| `util/metrics` | **41** |
| `util/mongoapi` | 8 |
| `util/logger` | 4 |
| `util/version` | 1 |

只 vendor `mongoapi/` 会导致：
```
no required module provides package github.com/free5gc/util/metrics
```

**正确做法**：
```bash
cd Free5gc-TYcustom
mkdir -p shared
git clone --depth 1 --branch v1.3.1 https://github.com/free5gc/util.git shared/util
rm -rf shared/util/.git          # 避免嵌套 git 仓库/submodule 混淆
# 确认 module path 与 go 版本
head -3 shared/util/go.mod       # 应为 module github.com/free5gc/util / go 1.25.5
```
`shared/util/` **必须包含原仓库的 `go.mod` 和 `go.sum`**，否则报
`replacement directory ... does not exist` 或 module path 不匹配。

#### 1.3 replace 只加在 UDR

`NFs/udr/go.mod` 追加：
```
replace github.com/free5gc/util => ../../shared/util
```
相对路径基准是 go.mod 所在目录（`NFs/udr/`）→ 解析为 `<repo>/shared/util` ✅

**其他 NF 一行都不动。** 本仓库**每个 NF 有独立 go.mod**（已确认 14 个，无 root
go.mod、无 submodule），Dockerfile 逐个 `cd NFs/$nf/cmd && go build` 各自解析自己的
go.mod，故 chf/nrf/pcf/webconsole 继续用它们各自的上游版本，**零影响**。

#### 1.4 防静默失效（Aether 教训 [[aether-tycustom-openapi-replace]]）

那次的症状是：镜像打好、部署成功、tag 正确，**但改动根本不在镜像里**，且不报错。
本方案必须加同样的防护：
- 构建前 `cd NFs/udr && go mod tidy`（replace 会让 go.sum 漂移，
  `-mod=readonly` 默认会因此失败）
- 本地先 `cd NFs/udr && go build ./...` 通过
- **构建后 grep 二进制确认符号进了镜像**（见第六节第 4 步）

### 2. 改 vendor 后的 `shared/util/mongoapi/mongoapi.go`

**两个选项，需你选一个：**

**选项 A（推荐）：修 `getCollation` 为类型扫描 + ctx 也走 argOpt**
```go
// 遍历找 int，不再只看 argOpt[0] —— 顺带修掉上游"参数顺序敏感"的脆弱设计
func getCollation(argOpt ...interface{}) *options.Collation {
	for _, a := range argOpt {
		if strength, ok := a.(int); ok {
			return &options.Collation{Locale: "en_US", Strength: strength}
		}
	}
	return nil
}

// 新增：从 argOpt 里挑 ctx，没有则退回 context.TODO()
func ctxFrom(argOpt ...interface{}) context.Context {
	for _, a := range argOpt {
		if c, ok := a.(context.Context); ok {
			return c
		}
	}
	return context.TODO()
}
```
然后把每处 `context.TODO()` 换成 `ctxFrom(argOpt...)`。
- 优点：**签名完全不变**，向后兼容，其他 NF 零影响
- 缺点：`interface{}` 传 ctx 不够优雅；每次调用多一次小遍历（~5 ns）

**选项 B：加显式 ctx 形参的新函数**
```go
func RestfulAPIGetOneCtx(ctx context.Context, collName string, filter bson.M,
	argOpt ...interface{}) (map[string]interface{}, error)
```
保留原函数（内部转调 `...Ctx(context.TODO(), ...)`）。
- 优点：类型安全、语义清晰
- 缺点：要新增 ~13 个函数；dbtrace 侧改调用名

> 无论选哪个，**`getCollation` 的类型扫描修复都必须做**，否则 collation 会被
> 静默丢弃（影响 `smf_registrations_collection.go:25` 的注册路径 GetMany×1000）。

### 3. 新建 `NFs/udr/internal/dbtrace/monitor.go`

```
callIDKey        — context key（私有类型，避免碰撞）
callCounter      — atomic.Int64，全局唯一 call_id 发号器
NextCallID()     — atomic.AddInt64
WithCallID(ctx, id)

Monitor() *event.CommandMonitor
    只注册 Succeeded 和 Failed；**不注册 Started**
      → CommandSucceededEvent 自带 Duration，start 可由 end-Duration 反推
      → 省一次回调 + 一次 time.Now()
    回调体：从 ctx 取 call_id（取不到则记 0，用于排查）→ 拼短 JSON → enqueue()

AttachMonitor(url string) error
    用同一 URI（含 maxPoolSize=1000&minPoolSize=200&maxConnecting=10）新建带
    monitor 的 client，赋给 mongoapi.Client
```

回调必须极轻：**不取 gid、不遍历 filter、不做时间格式化**（见第五节）。
`Failed` 也要记（正常实验应为 0，可作健康检查）。

### 4. 改 `NFs/udr/internal/dbtrace/dbtrace.go`（13 个 wrapper）

> ⚠️ 现有 import 块（[dbtrace.go:13-20](NFs/udr/internal/dbtrace/dbtrace.go#L13-L20)）
> 只有 `time` / `bson` / `accesslog` / `mongoapi`，**必须补 `"context"`**。
> 漏了会编译期立刻报错（不会静默），风险低但别忘。

每个从：
```go
func RestfulAPIGetOne(collName string, filter bson.M, argOpt ...interface{}) (...) {
	start := time.Now()
	res, err := mongoapi.RestfulAPIGetOne(collName, filter, argOpt...)
	logDB(collName, "GetOne", filter, start)
	return res, err
}
```
改为（选项 A 写法）：
```go
func RestfulAPIGetOne(collName string, filter bson.M, argOpt ...interface{}) (...) {
	callID := NextCallID()
	ctx := WithCallID(context.Background(), callID)
	start := time.Now()
	res, err := mongoapi.RestfulAPIGetOne(collName, filter, append(argOpt, ctx)...)
	logDB(collName, "GetOne", filter, start, callID)
	return res, err
}
```
注意 `append(argOpt, ctx)` 把 ctx 放**末尾**，配合选项 A 的类型扫描，
collation 无论在 `[0]` 还是别处都能被正确识别。

> `append(argOpt, ctx)` 可能触发一次底层数组扩容（argOpt 通常 len 0-1，cap 可能为
> len）→ 一次小分配。可用 `make([]interface{}, 0, len(argOpt)+1)` 显式构造避免
> 意外修改调用方切片。

### 5. 改 `NFs/udr/internal/accesslog/accesslog.go`

新增第三个日志流 `kindUDRDB`：
- `envUDRDBPath = "UDR_DB_LOG_PATH"`，`defaultUDRDBPath = "/tmp/UDR_DB_log.txt"`
- `writerLoop` 开第三个文件；`writeRec`/`flush`/`drainAll` 各加一路
  （**共享同一 writer goroutine，不新增 goroutine、不新增锁**）
- 新函数：
  `LogMongoCmd(cmd, coll, connID string, callID, reqID, endNs, durationNs int64)`
- `LogDB` 加 `callID int64` 参数，多写 `"call_id"` 字段

**UDR_DB_log 每行**（字段为最低开销而定）：
```json
{"nf":"UDR","cmd":"find","coll":"subscriptionData.authenticationData.authenticationSubscription",
 "call_id":123456,"conn_id":"free5gc-mongodb:27017[-42]","req_id":98765,
 "end_ns":1786323806275123456,"duration_ns":310221}
```
- `end_ns`/`duration_ns` 写**纳秒整数**（`strconv.AppendInt`），**不做 RFC3339
  格式化** → 省 ~200 ns/条（本是新 log 里最贵的一项）
- `start_ns = end_ns − duration_ns`，Python 侧算
- `conn_id`/`req_id` 仅供人工排查与交叉校验，**不参与匹配**

**DB_log 每行**（仅新增 `call_id`，其余不变，现有脚本零改动）：
```json
{"nf":"UDR","mongo":"mongodb","resource":"...","operation":"GetOne","ue_id":"imsi-...",
 "req_time":"...","resp_time":"...","latency_us":679,"call_id":123456}
```

### 6. 改 `NFs/udr/pkg/service/init.go`（+4 行）

[init.go:202](NFs/udr/pkg/service/init.go#L202) 保持不动，其后追加：
```go
if err := mongoapi.SetMongoDB(mongodb.Name, mongodb.Url); err != nil {   // 不动
	logger.InitLog.Errorf("UDR start set MongoDB error: %+v", err)
	return
}
// 新增：把 mongoapi.Client 换成带 CommandMonitor 的 client。
// 必须在 SetMongoDB 之后 —— dbName 是 mongoapi 未导出变量，只有它能设。
if err := dbtrace.AttachMonitor(mongodb.Url); err != nil {
	logger.InitLog.Errorf("UDR attach mongo monitor error: %+v", err)
	return
}
```

---

## 五、开销分析（硬约束：不得超过 DB_log / HTTP_log）

### 现有 DB_log 每条 ≈ 1000 ns

| 环节 | 开销 |
|---|---|
| `time.Now()` × 2 | ~50 ns |
| `ueIDFromFilter` 遍历 13 个 key 查 bson.M | ~100-200 ns |
| `make([]byte, 0, 256)` | ~50 ns |
| 7 字段 appendKV + 逐字符 JSON 转义 | ~300 ns |
| **`formatTime` × 2**（RFC3339Nano） | **~400 ns** |
| `enqueue()` | ~50 ns |

### 现有 HTTP_log 每条 ≈ 1500-2000 ns
12 字段、**4 次 formatTime**、长 URI 转义。

### 新增开销

**dbtrace wrapper 侧（每次 mongoapi 调用一次）**

| 环节 | 开销 |
|---|---|
| `atomic.AddInt64` 发号 | ~5 ns |
| `context.WithValue` | ~30 ns（一次小分配） |
| 构造 argOpt 切片（+ctx） | ~30 ns |
| DB_log 多写 `call_id` 整数字段 | ~20 ns |
| **小计** | **~85 ns** |

**回调侧（每条 driver 命令一次）**

| 环节 | 开销 | 优化 |
|---|---|---|
| ~~`Started` 回调~~ | **0** | 不注册；用 `end − Duration` 反推 |
| ~~取 gid~~ | **0** | 不需要（call_id 精确匹配） |
| ~~遍历 filter~~ | **0** | driver 直接给 coll |
| ~~`formatTime`~~ | **0** | 写纳秒整数 |
| `ctx.Value(callIDKey)` | ~10 ns | |
| `time.Now()` × 1 | ~25 ns | |
| `make([]byte, 0, 208)` | ~40 ns | |
| 5 短字段 + 3 整数 append | ~200 ns | cmd/coll/conn_id 都是短串 |
| `enqueue()` | ~50 ns | |
| **小计** | **~325 ns** | |

**mongoapi 侧**：`ctxFrom` + `getCollation` 各一次小遍历 → **~10 ns**

### 汇总

| 项 | 每 UE |
|---|---|
| wrapper 侧 85 ns × 11 次调用 | ~0.9 μs |
| 回调侧 325 ns × 14 条命令 | ~4.6 μs |
| mongoapi 侧 10 ns × 14 | ~0.1 μs |
| **合计** | **≈ 5.6 μs/UE** |

对照 per-UE DB 总时间 **11 200 μs**（RQ800）→ **0.05%**
单条命令 ~325 ns **仍低于 DB_log 的 ~1000 ns** ✓ 满足约束

### 写文件 = 0（复用现有异步机制）

[accesslog.go:1-11](NFs/udr/internal/accesslog/accesslog.go#L1-L11)：
- 热路径只 marshal 小 record 推 buffered channel，**从不碰文件**
- `queueCapacity = 1<<21` (2 097 152)；单轮新增 14 000 条，**余量 150 倍**
- `enqueue()` 用 `select/default`：**队列满则丢弃计数，永不阻塞数据面**
  ([accesslog.go:197-203](NFs/udr/internal/accesslog/accesslog.go#L197-L203))
- 单 writer + 1 MiB bufio + 200 ms tick flush

### 完全不碰 mongod
不改配置、不开 profiler、不轮询 serverStatus → 无观测者效应。埋点全在 UDR 进程内。

---

## 六、实施与验证顺序

1. vendor util → `shared/util/`（**完整 module，tag v1.3.1 / commit 13d2e77**）；
   UDR go.mod 加 replace；修 `getCollation` 类型扫描
2. 实现 monitor.go；改 dbtrace 13 个 wrapper（**补 `"context"` import**）；
   accesslog 加第三流 + call_id；init.go +4 行
3. 本地 `cd NFs/udr && go mod tidy && go build ./...` 通过
4. **改 Dockerfile.custom（见下）**，重建镜像；**grep 二进制确认符号在镜像里**
5. **副作用验证（先只跑 RQ800）**
   - `latency_us` 的 **p1 必须仍在 0.275 ms** 附近（p1 是最干净的纯服务时间基线；
     上移 >2% 则回退优化）
   - `accesslog.Dropped()` 必须为 **0**
   - UDR_DB_log 行数 ≈ **14 000**
   - **`call_id` 匹配率 100%**；`call_id == 0` 的行数为 0（否则有路径未传 ctx）
   - collation 未被丢弃：`GetMany smfRegistrations` 结果与改动前一致
6. 跑全五档 RQ800/1000/1500/2000/2500
7. 匹配脚本先输出匹配率与命令数分布，再算 driver/Go 拆分
8. 画图：x = Request/s，两条线 `Σduration_ns` 与 `latency_us − Σduration_ns`

---

## 六之二、构建流程改动（对现有 Dockerfile.custom 的最小修改）

现有流程（机器 A 编译 → tar → 机器 B `docker load` → helm upgrade → rollout restart）
**主体不变**，但 Dockerfile 有两处必须改，否则**构建会失败**。

### 改动 1：build 前加 `go mod tidy`（必需）

现有 Dockerfile 是 `COPY . .` 后直接 `go build`，没有 vendor、没有 `-mod=mod`。
加了 replace 之后 `go.sum` 会漂移，Go 默认 `-mod=readonly` 会报：
```
go: updates to go.sum needed, disabled by -mod=readonly
```

```dockerfile
FROM golang:1.25-bookworm AS builder
WORKDIR /src
COPY . .
RUN cd NFs/amf/internal/ngap && go run ngap_generator.go
# ---- 新增：replace 使 go.sum 漂移，必须先 tidy（只影响 udr）----
RUN cd NFs/udr && go mod tidy
RUN set -e; \
    for nf in amf ausf bsf nrf nssf pcf smf udm udr chf nef n3iwf tngf; do \
      echo "==== building $nf ===="; \
      (cd NFs/$nf/cmd && CGO_ENABLED=0 go build -o /out/$nf main.go); \
    done
```

> `golang:1.25-bookworm` 与 udr/util 声明的 `go 1.25.5` 兼容 ✅
> （若基础镜像的 patch 版本低于 1.25.5，Go 1.21+ 的 toolchain 机制会自动下载
> 匹配的 toolchain；构建机需能联网，本流程已联网拉依赖，故无新增要求。）

### 改动 2：确认 `shared/util/` 被 COPY 进镜像

`COPY . .` 会带上，但需确认没被排除：
```bash
grep -nE 'shared|util' .gitignore .dockerignore 2>/dev/null   # 应无匹配
```
若 `shared/util/.git` 未删除，会造成嵌套 git 目录（不影响构建但会显著增大镜像层）。

### 改动 3：构建后验证符号真的进了镜像（强烈建议，勿省）

这是 Aether 那次踩坑的直接防护 —— 当时镜像正常、部署正常、tag 正常，
**但改动不在镜像里且不报错**：

```bash
docker run --rm --entrypoint sh free5gc-custom-v4.2.2:<tag> -c '
  grep -ac call_id /free5gc/udr >/dev/null \
    && echo "OK: call_id instrumentation present in udr binary" \
    || echo "FAIL: replace was ignored -- upstream util was compiled in"
'
```
（`CGO_ENABLED=0` 且未加 `-s -w` strip，故字符串字面量可被 grep 到。）

### K8s 侧（机器 B）：流程不变，仅两点注意

1. helm upgrade / rollout restart 步骤**完全不用改**，只换镜像 tag
2. **日志收集要加新文件** `/tmp/UDR_DB_log.txt`
   （现有脚本只收 `DB_log.txt` 与 `HTTP_log.txt`）
3. 每次 pod 重启后 PID 会变 → `Cpu_Mem` 采集脚本的 PID 列表需用 `crictl` 循环重取
   （mongodb 容器名为 `mongodb`，无 `free5gc-` 前缀）

---

## 七、预期产出

> 在 RQ 800→2500 区间，UDR 观测的 DB 往返时间从 1.02 ms 增至 2.00 ms。
> 逐操作拆分显示 driver 侧（socket 往返 + mongod 排队与执行）为 X→Y ms，
> Go 侧（BSON 编解码 + 连接获取 + goroutine 调度延迟）为 Z→W ms，
> 即增量的 P% 来自 [UDR Go 侧 / mongod]。

### 附带的命名修正

`HTTP_per_UE_transport_latency.py` 中名为 `MongoDB` 的线
([:634](../cloudlab/Ty_log/Free5gc/R6525_NF8HTTP_idle500ms_0809/HTTP_per_UE_transport_latency.py#L634))
实际语义是 **"UDR 内的 DB-wait"**。落地后应拆成两条线。
p1 分位数已证明数据库**查询性能**未退化（0.275→0.292 ms）；0809v1 进一步证明
mongod 单位成本恒定、未饱和。即便增量主要落在 driver 段，也是**并发调度**问题，
不是查询能力问题。
