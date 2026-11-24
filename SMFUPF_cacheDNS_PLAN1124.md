# SMF / UPF 域名→IP 缓存：让会话建立不再查 DNS —— 代码修改计划（1124）

> **状态：已实施（2026-11-24），未提交。** 基于 HEAD `c159bda`（"CHF add HTTP log & go profiling & GC port -- 1124 3PM"）。
> - **改动**：新增 `pfcp/dnscache/{dnscache.go,dnscache_test.go}`、`NFs/upf/internal/dnscache/{dnscache.go,dnscache_test.go}`；修改 8 个文件：`pfcp/pfcpType/NodeID.go`，SMF 的 `internal/context/{context.go,upf.go}`、`internal/pfcp/udp/udp.go`、`internal/sbi/processor/association.go`、`pkg/utils/pfcp_util.go`，UPF 的 `internal/pfcp/{pfcp.go,session.go}`。所有 go.mod/go.sum 都没动。
> - **和第 4 节代码片段的两处差别**（以实际代码为准）：
>   - `RefreshNow` 记录上次触发的时间改用单调时钟（`time.Since(base)`），这样系统时间被往回调时不会把触发全部丢掉；
>   - `Lookup` 遇到 IPv4 字面量时返回 4 字节形式，和原来的 `net.ResolveIPAddr` 一致。
> - **本机验证**：用 go1.25.5 测试，目标 linux/amd64、`CGO_ENABLED=0`：
>   - SMF（`./cmd`）、UPF（`./cmd`）都能编译通过，SMF 二进制里已链接 `pfcp/dnscache`；
>   - 改动的包 `go vet` 通过，`gofmt` 无差异；
>   - 两份 dnscache 各 7 个单测通过（Windows 无 gcc，没有跑 `-race`）；
>   - pfcp 库原有的测试、SMF 的 `internal/context`、`internal/pfcp`、`pfcp/message`、`pfcp/udp` 测试都通过。
> - **原本就失败的测试**：SMF 的 `internal/pfcp/handler`（3 个）和 `internal/sbi/processor`（`TestHandlePDUSessionSMContextCreate`）在改动前的 HEAD `c159bda` 上同样失败（用临时 worktree 对照过）。原因分别是 PFCP_log 代码在测试里遇到 `Server` 为 nil、TYcustom 的 HTTP 客户端绕过了测试的 mock，和本改动无关。
> - **UPF 的 Go 版本**：镜像里 UPF 用 go1.21.8 编译，本机用 1.25.5 编；dnscache 只用到 Go 1.19 及以前就有的标准库 API，所以在 1.21.8 下也能编。
> 缓存规则按用户指定，与 Aether 6.1.2 的 SMF 一致（`Aether_6.1.2_TYcustom/NFs/smf/context/nodeid.go`）：**每 1 min 全量刷新一次；刷新时查询失败就删掉该条；IP 变了就更新。**
> **1124 审阅后更新**：
> - 刷新失败的处理确定沿用 Aether 的做法（删除该条）；
> - 新增两个立即刷新的触发点：SMF 的 PFCP 请求失败（含重传用完）时、心跳判定 UPF 丢失时（3.6、4.10、4.11 节）；
> - 第 7 节的风险按审阅结论重写。
> 对照基线：`cloudlab/Ty_log/Free5gc/PDUSessionEst/R6525_NF1HTTP_HTTPDBAMFPFCPlog_ch0_max128_1123`。
> 本机没有 Go 工具链，文中 `go` 命令都在编译机（node-0 / `golang:1.25-bookworm` 容器）执行。

---

## 0. TL;DR

| # | 项 | 结论 |
|---|---|---|
| 1 | 改什么 | SMF、UPF 各加一张进程内的"域名 → IP"表。所有把域名解析成 IP 的入口都先查这张表 |
| 2 | 改多少 | 新增 2 个文件（每个 NF 一份缓存包），改 9 处（7 处解析入口和预热、2 处立即刷新的触发）。改动点见第 4 节 |
| 3 | 效果 | 每个会话的 DNS 查询：**SMF 25 → 0，UPF 1 → 0**。整个运行期只剩"启动时每个域名查 1 次"加上"每分钟刷新一次" |
| 4 | 缓存规则 | 命中直接返回。未命中同步查一次，成功才写表。查询失败的结果不写表。后台每 1 min 把表里每个域名重查一遍：失败删除，IP 变了更新。另外，SMF 的 PFCP 请求失败或心跳判定 UPF 丢失时，在后台立即刷新一次（多个触发合并成一次，两次触发刷新至少间隔 5 s） |
| 5 | 并发 | 读写锁 + 每条目一个版本号，照搬 Aether（3.3 节） |
| 6 | 开关 / 回退 | 缓存包里的常量 `Enabled`，改成 `false` 就恢复"每次都查"。原代码注释保留，统一标 `[DNSCACHE 1124]` |
| 7 | 怎么判定生效 | 跑完后在 block profile 里，SMF/UPF 会话路径上的 DNS 调用栈消失，只剩 `dnscache.refresh`（6.2 节） |
| 8 | 不做 | 不按 DNS 记录的 TTL 失效、不缓存失败结果、不重建 SMF 里以 IP 为键的表、不动 SBI(HTTP) 和 7 处只为写日志的解析（第 8 节） |

---

## 1. 术语（本文的准确定义）

- **一次解析**：代码调用一次 Go 解析器（`LookupHost` / `ResolveIPAddr` / `LookupIPAddr` / `LookupIP`），把一个域名换成 IP。镜像用 `CGO_ENABLED=0` 编译，走纯 Go 解析器。**它不缓存结果**，每次解析都会向 CoreDNS 发 UDP 查询。查哪些记录取决于网络参数：`"ip"` 同时查 A 和 AAAA，`"ip4"` 只查 A。
- **缓存表**：进程内全局的 `map[域名]net.IP`，每个域名只存 1 个 IP。
- **命中 / 未命中**：表里有这个域名 / 没有。
- **刷新**：后台 goroutine 每 1 min 对表里每个域名重新解析一次，然后按结果删除或更新这一条。
- **版本号（gen）**：每个域名一个计数器，这一条每被写一次（插入、刷新更新、刷新删除）就加 1。作用是防止一次慢的刷新，把别人在这期间写进去的新结果覆盖或删掉（3.3 节）。

---

## 2. 现状

### 2.1 每个会话在哪里解析、解析什么

| 入口 | 位置 | 每会话次数 | 解析的域名 |
|---|---|---|---|
| `pfcpType.NodeID.ResolveNodeIdToIp` | `pfcp/pfcpType/NodeID.go:108`（`LookupHost` 在 :113） | 19 | SMF 配置里 UPF 的 N4 NodeID（18 次）；UPF 在 Establishment Response 里自报的 NodeID（1 次，`sbi/processor/datapath.go:167`） |
| SMF `context.ResolveIP`（经 `ExternalIP()` 调用） | `NFs/smf/internal/context/context.go:93-99` | 3 | SMF 自己的 N4 地址（`pfcp.externalAddr`，填 CP F-SEID） |
| SMF `UPFInterfaceInfo.IP` | `NFs/smf/internal/context/upf.go:157` | 3 | UPF 的 N3 endpoint（填 PDR 的 F-TEID、N2 SM info 的上行隧道地址） |
| UPF `net.ResolveIPAddr("ip4", s.nodeID)` | `NFs/upf/internal/pfcp/session.go:87` | 1 | UPF 自己配置的 nodeID（填 UP F-SEID） |

- SMF 通过 `go.mod` 里的 `replace github.com/free5gc/pfcp => ../../pfcp` 使用本地这份 pfcp 库，所以改 `NodeID.go` 这一处，就能覆盖 SMF 里约 60 处 `ResolveNodeIdToIp` 调用。
- UPF（go-upf）用的是 `wmnsk/go-pfcp`，**不依赖**本地 pfcp 库，所以缓存包要单独放一份。
- 每会话 25 次 = Establishment 22 次 + Modification 3 次。按用途分：用 UPF 的 IP 查 SMF 内部的表 10 次，PFCP 发送目的地址 2 次，往消息里填 IP 字段 6 次，只为写日志 7 次。数字来自 1123 RQ200 的 block profile，每个调用点每轮约 1000 次。

### 2.2 1123 实测的代价（来自 block profile，见记忆 free5gc-pdu-1123-upf-dns-knee）

- **SMF**：DNS 等待占 SMF 本地时间的 78%（RQ200）、95%（RQ600）、98–99%（RQ≥800）。
- **UPF**：每个 Establishment 的处理时间是 3.3–4.9 ms，其中 `session.go:87` 的解析占 2.4–3.8 ms。而且它跑在 UPF 唯一的 main goroutine 上，所以 UPF 在约 226 sess/s 就饱和了。
- **CoreDNS 被打满**：高 RQ 时，SMF 的大量查询让 CoreDNS 变慢，连 UPF 自己的那 1 次解析也从约 3 ms 涨到 190–450 ms。

### 2.3 规范允许缓存

TS 29.244 §5.8.1 写的是：建立 PFCP Association 之前查找一次对端（用 DNS、NRF 或本地配置都可以），之后查找时得到的 IP "may then be used"，用来发后续的节点级消息和 Session Establishment Request。规范里没有要求每条消息都重新解析，也没有禁止缓存。

---

## 3. 设计

### 3.1 缓存规则（与 Aether 相同，用户指定）

1. **IP 字面量**：直接返回，不进表。
2. **命中**：直接返回表里的 IP，不发 DNS 查询。
3. **未命中**：同步解析一次。成功则写表，然后返回。**失败不写表**，并按原来的方式返回失败值（`ResolveNodeIdToIp` 返回 `0.0.0.0`，`ResolveIP` 返回 `nil`，`UPFInterfaceInfo.IP` 返回 error，UPF 的 `v4` 保持为 nil）。
4. **刷新**：每 1 min 一次，对表里每个域名重新解析：
   - 失败 → 删除这一条。下次用到时会走未命中路径，同步再查；
   - 成功且 IP 变了 → 更新；
   - 成功且没变 → 不变。
5. **立即刷新**（SMF 侧，用户 1124 指定）：PFCP 请求失败（含重传用完）时、心跳判定 UPF 丢失时，立即在后台跑一次第 4 条的刷新，删除/更新规则完全相同（3.6 节）。

### 3.2 地址选择

- **SMF**：用 `LookupIPAddr`，取第一个 IPv4，没有 IPv4 才取第一个地址。
  - 这和原来 `ResolveIPAddr("ip", host)` 的选择一致。
  - 原来的 `LookupHost` 取的是 `ns[0]`，在纯 IPv4 集群里两者结果相同。
- **UPF**：用 `LookupIP(ctx, "ip4", host)`，只查 A 记录，和原来的 `ResolveIPAddr("ip4", ...)` 一致。
- **`ResolveNodeIdToIp` 的返回形式**：原来返回的是 `net.ParseIP(ns[0])`，IPv4 也是 16 字节形式。改后返回 `ip.To16()` 保持原样。其他入口原来返回的就是 4 字节，不变。

### 3.3 并发：读写锁 + 版本号（照搬 Aether `nodeid.go:28-38、125-189`）

- **读（热路径）**：拿 `RLock` 查表。按每会话 25 次、RQ2000 估算，约 5 万次 `RLock`/s，这个量级对读写锁不构成问题。
- **未命中写入**：拿 `Lock`，写入后版本号加 1。
- **刷新**分三步：
  1. 拿 `RLock`，抄下"域名 → 当前版本号"，然后放锁；
  2. **不持锁**逐个解析，这样一次慢查询不会卡住读者；
  3. 对每个域名拿 `Lock`，**只有当前版本号等于抄下来的值（说明这期间没人写过这一条）**，才应用刷新结果（删除或更新），并把版本号加 1。否则丢弃这次结果。
- **版本号只增不删**：删除条目时版本号不清零。否则以后重新插入时从 1 开始计数，可能撞上某次慢刷新抄下的旧值，导致旧结果覆盖新结果。这一点照抄 Aether 的注释。

### 3.4 预热

- **为什么要预热**：在 2.1 表的几个域名里，只有"SMF 配置里 UPF 的 NodeID"会在 SMF 启动建表时被解析。其他几个（UPF 自报的 NodeID、N3 endpoint、SMF 的 externalAddr）要等**第一个会话**才第一次被解析。开头一波会话如果同时到达，缓存还是空的，就会各自去查。
- **未命中路径的并发合并**：未命中时用 `LookupIPAddr` / `LookupIP` 解析，它们内部有 Go 自带的 singleflight，**同一域名同一时刻只有一次真实查询**，其他调用者共享结果。原来的 `LookupHost` 没有这个合并。所以就算漏了预热，冷启动时的同时查询也会被压到每个域名一次。
- **预热的时间点**：
  - SMF 在 `pfcpStart` 启动 PFCP 之前，预热 externalAddr、listenAddr、每个 UPF 的 NodeID、N3/N9 endpoint（4.5 节）；
  - SMF 在收到 PFCP Association Setup Response 时，预热 UPF 自报的 NodeID（4.6 节）；
  - UPF 在 `PfcpServer.Start` 里预热自己的 nodeID（4.9 节）。

### 3.5 开关与回退

- **开关**：两个缓存包各有 `const Enabled = true`。改成 `false` 后，`Lookup` 不读也不写表、不启动刷新，每次都直接解析，行为等同改动前。用于做 A/B 对照。
- **保留原代码**：所有改动点把原代码注释保留，标 `[DNSCACHE 1124]`。被注释掉的代码里如果有 import 变成不用了，也注释保留，不删。

### 3.6 立即刷新的触发（SMF）

**触发点**：
- **PFCP 请求失败**：SMF 的所有 PFCP 请求（Establishment、Modification、Deletion、Association、Heartbeat）都经过 `NFs/smf/internal/pfcp/udp/udp.go:125` 的 `SendPfcpRequestUE`。只要它返回错误就触发，对应 Aether `HandlePfcpSendError` 里"任何发送失败（含超时）都刷新"（`Aether_6.1.2_TYcustom/NFs/smf/pfcp/message/send.go:464`）。"重传用完"就是其中一种错误：pfcp 库发 3 次、每次等 3 s 都没收到响应，返回 `retry-out`（`pfcp/transaction.go:114-141`）。
- **心跳丢失**：心跳请求同样走 `SendPfcpRequestUE`，心跳超时已经被上一个触发点覆盖。但心跳丢失还有一种判定方式不经过发送失败：UPF 报来的 recovery 时间戳变了。所以在 `keepHeartbeatTo` 返回的地方再加一个触发（4.11 节）。

**与 Aether 的两处不同**：

| 项 | Aether | 本计划 | 原因 |
|---|---|---|---|
| 刷新在哪个 goroutine 里跑 | 在发送方的 goroutine 里同步跑 | **后台异步跑**，发送方不等 | `SendPfcpRequestUE` 在会话路径上。同步刷新会让发送方多等几次 DNS 查询，DNS 出问题时可能要等好几秒 |
| 多个触发同时到达 | 每次都完整刷新一遍 | **合并**：同一时刻只跑一个触发刷新；距上一次触发刷新不到 5 s 的触发直接丢弃 | UPF 挂掉时，在途的几百个 PFCP 请求会在约 9 s 内陆续失败。不合并就会变成几百次完整刷新 |

**正常跑实验时不会触发**：1123 的 PFCP_log 里，RQ200–2000 所有 SMF 请求的结果都是 `ok`，一次重传用完都没有。

**作用有限**：触发刷新只能把"用旧 IP"的窗口从最多 1 min 缩短到几秒。UPF 的 IP 变了以后，SMF 仍然要重启（7.3 节）。

---

## 4. 改动清单

### 4.1 新增 `pfcp/dnscache/dnscache.go`（SMF 用）

- **位置**：放在本地 pfcp 库里，作为新的子包 `github.com/free5gc/pfcp/dnscache`。
- **好处**：
  - `pfcpType` 和 SMF 都能 import 同一份。
  - SMF 的 `go.mod` 已经 `replace` 到 `../../pfcp`，新子包自动可用，**不改 go.mod / go.sum**。
  - 只用标准库，加上 pfcp 库自己的 logger。
- **依赖方向**：`dnscache` 不 import `pfcpType`，没有循环依赖。

```go
// Package dnscache is a process-wide hostname -> IP cache. [DNSCACHE 1124]
// Rules follow Aether 6.1.2 SMF (context/nodeid.go): miss -> resolve once and insert;
// failures are never inserted; every RefreshInterval each cached host is re-resolved,
// deleted on failure, updated when the IP changed.
package dnscache

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/free5gc/pfcp/logger"
)

// Enabled=false: Lookup resolves on every call, nothing is cached, Start/RefreshNow are no-ops.
const Enabled = true

const RefreshInterval = time.Minute

// MinTriggerGap: triggered refreshes (RefreshNow) closer than this to the previous one are dropped.
const MinTriggerGap = 5 * time.Second

var (
	mu        sync.RWMutex
	cache     = map[string]net.IP{}
	gen       = map[string]uint64{} // never deleted, only incremented (see 3.3)
	startOnce sync.Once

	triggering  atomic.Bool  // a triggered refresh is running
	lastTrigger atomic.Int64 // UnixNano of the last accepted trigger

	// package var so tests can stub DNS. LookupIPAddr merges concurrent lookups
	// of the same host (Go's internal singleflight); LookupHost does not.
	lookupFn = func(ctx context.Context, host string) ([]net.IPAddr, error) {
		return net.DefaultResolver.LookupIPAddr(ctx, host)
	}
)

// pick matches net.ResolveIPAddr("ip", host): first IPv4, else first address.
func pick(addrs []net.IPAddr) net.IP {
	for _, a := range addrs {
		if v4 := a.IP.To4(); v4 != nil {
			return v4
		}
	}
	if len(addrs) > 0 {
		return addrs[0].IP
	}
	return nil
}

func resolve(host string) (net.IP, error) {
	addrs, err := lookupFn(context.Background(), host)
	if err != nil {
		return nil, err
	}
	if ip := pick(addrs); ip != nil {
		return ip, nil
	}
	return nil, &net.DNSError{Err: "no address", Name: host, IsNotFound: true}
}

// Lookup returns the IP for host: literal IPs as-is, cached hosts from the table,
// otherwise one synchronous resolve that is inserted only on success.
func Lookup(host string) (net.IP, error) {
	if host == "" {
		return nil, &net.DNSError{Err: "empty host", Name: host, IsNotFound: true}
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip, nil
	}
	if !Enabled {
		return resolve(host)
	}
	mu.RLock()
	ip, ok := cache[host]
	mu.RUnlock()
	if ok {
		return ip, nil
	}
	ip, err := resolve(host)
	if err != nil {
		return nil, err
	}
	mu.Lock()
	cache[host] = ip
	gen[host]++
	mu.Unlock()
	logger.PFCPLog.Infof("[DNSCACHE] miss: %s -> %s", host, ip)
	return ip, nil
}

// Prewarm resolves hosts once so the first sessions do not miss. Failures only warn.
func Prewarm(hosts ...string) {
	for _, h := range hosts {
		if h == "" {
			continue
		}
		if _, err := Lookup(h); err != nil {
			logger.PFCPLog.Warnf("[DNSCACHE] prewarm %s failed: %v", h, err)
		}
	}
}

// Start launches the 1-min refresher once.
func Start() {
	if !Enabled {
		return
	}
	startOnce.Do(func() {
		go func() {
			t := time.NewTicker(RefreshInterval)
			defer t.Stop()
			for range t.C {
				refresh()
			}
		}()
	})
}

// RefreshNow runs one full refresh in the background, like Aether's RefreshDnsHostIpCache on a
// PFCP send failure (send.go:464), but asynchronous and coalesced: at most one triggered refresh
// runs at a time, and a trigger within MinTriggerGap of the last accepted one is dropped, so a
// dead UPF failing hundreds of in-flight requests costs one refresh, not hundreds. Running at the
// same time as the 1-min refresh is safe: the generation check in refresh() handles it.
func RefreshNow() {
	if !Enabled {
		return
	}
	now := time.Now().UnixNano()
	last := lastTrigger.Load()
	if now-last < int64(MinTriggerGap) || !lastTrigger.CompareAndSwap(last, now) {
		return
	}
	if !triggering.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer triggering.Store(false)
		refresh()
	}()
}

func refresh() {
	mu.RLock()
	observed := make(map[string]uint64, len(cache))
	for h := range cache {
		observed[h] = gen[h]
	}
	mu.RUnlock()

	for h, g := range observed {
		ip, err := resolve(h) // no lock held: a slow resolver never blocks readers
		mu.Lock()
		if gen[h] == g { // nobody wrote this entry since the snapshot
			if err != nil {
				delete(cache, h)
				logger.PFCPLog.Warnf("[DNSCACHE] refresh %s failed, entry deleted: %v", h, err)
			} else if !cache[h].Equal(ip) {
				logger.PFCPLog.Infof("[DNSCACHE] refresh %s: %s -> %s", h, cache[h], ip)
				cache[h] = ip
			}
			gen[h]++
		}
		mu.Unlock()
	}
}
```

### 4.2 `pfcp/pfcpType/NodeID.go:108-121`：`ResolveNodeIdToIp`

只改 FQDN 分支，IPv4/IPv6 分支不变。

```go
case NodeIdTypeFqdn:
	// [DNSCACHE 1124] was: net.DefaultResolver.LookupHost(context.Background(), n.FQDN) on every call
	// if ns, err := net.DefaultResolver.LookupHost(context.Background(), n.FQDN); err != nil {
	// 	logger.PFCPLog.Warnf("Host lookup failed: %+v", err)
	// 	return net.IPv4zero
	// } else {
	// 	return net.ParseIP(ns[0])
	// }
	ip, err := dnscache.Lookup(n.FQDN)
	if err != nil {
		logger.PFCPLog.Warnf("Host lookup failed: %+v", err)
		return net.IPv4zero
	}
	return ip.To16() // same 16-byte form as the old net.ParseIP(ns[0])
```

- `import "context"` 如果因此变成不用了，按惯例注释保留。
- 覆盖：表 2.1 第 1 行的 19 次。

### 4.3 `NFs/smf/internal/context/context.go:93-99`：`ResolveIP`

```go
func ResolveIP(host string) net.IP {
	// [DNSCACHE 1124] was: net.ResolveIPAddr("ip", host) on every call
	// if addr, err := net.ResolveIPAddr("ip", host); err != nil {
	// 	return nil
	// } else {
	// 	return addr.IP
	// }
	ip, err := dnscache.Lookup(host)
	if err != nil {
		return nil
	}
	return ip
}
```

覆盖：`ExternalIP()` 每会话 3 次，另有 `ListenIP()`（启动时调用）和 ULCL 的 `ResolveIP(node.UPF.Addr)`。

### 4.4 `NFs/smf/internal/context/upf.go:157`：`UPFInterfaceInfo.IP`

只换掉解析这一行，后面按 PDU 类型做 `To4` / `To16` 的 switch 不动：

```go
if i.EndpointFQDN != "" {
	// [DNSCACHE 1124] was: resolvedAddr, err := net.ResolveIPAddr("ip", i.EndpointFQDN)
	ip, err := dnscache.Lookup(i.EndpointFQDN)
	resolvedAddr := &net.IPAddr{IP: ip}
	if err != nil {
		logger.CtxLog.Errorf("resolve addr [%s] failed", i.EndpointFQDN)
	} else {
		// ... unchanged switch on pduSessType ...
	}
}
```

覆盖：每会话 3 次（`datapath.go` 建 PDR 1 次，`ngap_build.go:53/79` 2 次）。

### 4.5 `NFs/smf/pkg/utils/pfcp_util.go:21` 之前：启动刷新 + 预热

在 `pfcpStart` 里、`udp.Run(pfcp.Dispatch)` 之前插入：

```go
// [DNSCACHE 1124] start the 1-min refresher and resolve every configured name once,
// so the first sessions never miss (see SMFUPF_cacheDNS_PLAN1124.md 3.4)
dnscache.Start()
hosts := []string{smfContext.ExternalAddr, smfContext.ListenAddr}
for _, upNode := range smfContext.UserPlaneInformation.UPFs {
	if upNode.NodeID.NodeIdType == pfcpType.NodeIdTypeFqdn {
		hosts = append(hosts, upNode.NodeID.FQDN)
	}
	for _, ifs := range [][]*smf_context.UPFInterfaceInfo{upNode.UPF.N3Interfaces, upNode.UPF.N9Interfaces} {
		for _, i := range ifs {
			hosts = append(hosts, i.EndpointFQDN)
		}
	}
}
dnscache.Prewarm(hosts...)
```

`Prewarm` 会跳过空串和 IP 字面量（`Lookup` 对它们直接返回），所以不用事先过滤。

### 4.6 `NFs/smf/internal/sbi/processor/association.go:100` 之后：预热 UPF 自报的 NodeID

`nodeID := rsp.NodeID` 和随后的 nil 检查（:100-103）之后插入：

```go
// [DNSCACHE 1124] prewarm the NodeID the UPF reports about itself. It can be a different
// string from the configured one (in 1123 one lookup of it cost ~6x the DNS round trips),
// and is otherwise first resolved by the first session (datapath.go:167).
_ = nodeID.ResolveNodeIdToIp()
```

### 4.7 新增 `NFs/upf/internal/dnscache/dnscache.go`（UPF 用）

与 4.1 逐行相同，只有三处不同：

| 项 | 4.1（SMF） | 4.7（UPF） |
|---|---|---|
| package import 路径 | `github.com/free5gc/pfcp/dnscache` | `github.com/free5gc/go-upf/internal/dnscache` |
| logger | `github.com/free5gc/pfcp/logger` 的 `PFCPLog` | `github.com/free5gc/go-upf/internal/logger` 的 `PfcpLog` |
| 解析函数 | `LookupIPAddr(ctx, host)`，`pick` 优先 IPv4 | `LookupIP(ctx, "ip4", host)`，只查 A，取第一个 `To4()` |

注意 go-upf 的 `go.mod` 声明的是 `go 1.21`，镜像里 UPF 用 go1.21.8 编译。所以不能用 1.22 以后才有的语法（比如对整数用 `range`）。4.1 的代码里没有用到这类语法（`atomic.Bool` / `atomic.Int64` 从 Go 1.19 起就有）。

`RefreshNow` 在 UPF 里没有调用方：UPF 只解析自己的 nodeID，没有"发给对端失败"的触发场景。但代码照样保留，这样两份文件除了上表三处之外逐行相同。

### 4.8 `NFs/upf/internal/pfcp/session.go:87`：每个 Establishment 的解析

```go
var v4 net.IP
// [DNSCACHE 1124] was: one synchronous ResolveIPAddr per Est, on the single main goroutine
// addrv4, err := net.ResolveIPAddr("ip4", s.nodeID)
// if err == nil {
// 	v4 = addrv4.IP.To4()
// }
if ip, err := dnscache.Lookup(s.nodeID); err == nil {
	v4 = ip.To4()
}
```

### 4.9 `NFs/upf/internal/pfcp/pfcp.go:247`：`PfcpServer.Start`

在 `go s.main(wg)` 之前加：

```go
dnscache.Start()          // [DNSCACHE 1124]
dnscache.Prewarm(s.nodeID)
```

UPF 的刷新跑在独立的 goroutine 里，**不占 main goroutine**。只有在刷新失败、条目被删掉之后，下一个 Establishment 才会在 main goroutine 上同步查一次。

`pkg/factory/factory.go:49` 启动时校验 nodeID 那一次解析不动。

### 4.10 `NFs/smf/internal/pfcp/udp/udp.go:125`：PFCP 请求失败时立即刷新

`SendPfcpRequestUE` 有 3 个返回点：:127 目的地址是 `0.0.0.0`、:130 关闭 pfcplog 时的直接返回、:168 打点后返回。把返回值改成**具名返回值**，再加一个 `defer`，就能一次覆盖全部 3 个返回点。调用方不受影响。

```go
// [DNSCACHE 1124] was: (*pfcpUdp.Message, error) -- named so the deferred check below sees the error
func SendPfcpRequestUE(sndMsg *pfcp.Message, addr *net.UDPAddr, ueID string) (rsvMsg *pfcpUdp.Message, err error) {
	// [DNSCACHE 1124] like Aether HandlePfcpSendError (send.go:464): any failed PFCP request --
	// retry-out after NumOfResend x 3 s, write error, or a 0.0.0.0 destination -- triggers one
	// background DNS-cache refresh (coalesced, see SMFUPF_cacheDNS_PLAN1124.md 3.6).
	defer func() {
		if err != nil {
			dnscache.RefreshNow()
		}
	}()
	// ... body unchanged ...
```

函数体里 :139 的 `rsvMsg, tr, err := Server.WriteRequestToTraced(...)` 在函数最外层，会直接赋值给同名的具名返回值，不会遮蔽。而且 Go 的 `return x, y` 会先把值赋给具名返回值，再执行 defer，所以 3 个返回点的错误 defer 都看得到。

### 4.11 `NFs/smf/internal/sbi/processor/association.go:38` 之后：心跳丢失时立即刷新

在 `ToBeAssociatedWithUPF` 的循环里，`keepHeartbeatTo(upf, upfStr)`（:37）返回就表示心跳判定 UPF 丢失，或者 association 被取消。在它后面、`p.releaseAllResourcesOfUPF`（:40）之前插入：

```go
// [DNSCACHE 1124] heartbeat loss (incl. a changed UPF recovery timestamp, which is not a send
// failure): refresh before re-associating, so ensureSetupPfcpAssociation uses a fresh IP.
dnscache.RefreshNow()
```

- 心跳超时引起的丢失，在它之前的心跳请求失败时已经由 4.10 触发过一次刷新。这里的触发如果在 5 s 之内，会按 3.6 的规则被丢掉。这样没问题，因为刚刚已经刷新过了。
- **前提是心跳是开着的**：`PfcpHeartbeatInterval == 0` 时 :34 直接 return，这个触发点永远不会走到。仓库里的 `config/smfcfg.yaml:57` 写的是 `heartbeatInterval: 10s`，但集群用的是 chart 渲染出来的配置，**这个值没核实**。就算心跳没开，4.10 仍然覆盖所有 PFCP 请求失败的情况。

---

## 5. 编译

- **SMF**：`Build/Dockerfile.custom` 已经检查 SMF 的 `github.com/free5gc/pfcp` 解析到 `/src/pfcp`（:79-85、:135）。新子包在同一个被替换的模块里，`COPY . .` 会把它带进镜像，**不需要改 Dockerfile**。
- **UPF**：沿用 Dockerfile 里的 go1.21.8 + `CGO_ENABLED=0` 编译阶段，同样不改。
- **单测（建议）**：
  - 位置：`pfcp/dnscache/dnscache_test.go` 和 `NFs/upf/internal/dnscache/dnscache_test.go`。
  - 方法：把 `lookupFn` 换成桩函数，覆盖 5 种情况：命中不调用解析、未命中写表、失败不写表、刷新失败删除、刷新时 IP 变了更新；另加一个"刷新期间别人写过 → 丢弃刷新结果"（版本号），以及一个 `RefreshNow` 合并（5 s 内连续调用多次，桩函数只被一轮刷新调用）。
  - 命令：在编译机上 `cd pfcp && go test -race ./dnscache/`，`cd NFs/upf && go test -race ./internal/dnscache/`。
- **镜像命名**：沿用现有习惯，例如 `free5gc-custom-v4.2.2-NF1HTTP_..._dnscache_1124.tar`。

---

## 6. 验证

用和 1123 相同的 RQ 列表、collect 脚本（`cloudlab/K8s/collect_HTTPDBAMFPFCPlog_Free5gc_1122.sh`）和 profON 镜像，跑一轮 PDU 实验，然后逐项核对。

### 6.1 功能

- 每轮 1000 个 UE 全部 accept。
- PFCP_log 里没有新增重传，SMF 发 PFCP 的目的地址（`peer`）和 1123 一致。

### 6.2 DNS 次数（主判据）

在编译机上，对每个 RQ 用 block profile 做"跑后减跑前"：

```bash
go tool pprof -sample_index=contentions -top -nodecount=40 \
  -focus 'net\.\(\*Resolver\)\.(lookupIPAddr|LookupHost|LookupIP)' \
  -base block_smf_pre_RQ200_UE1000.pb.gz block_smf_after_RQ200_UE1000.pb.gz
```

UPF 换成 `block_upf_*` 再跑一遍。判据：
- **SMF**：1123 里每个调用点约 1000 次的那些调用栈（`ActivateTunnelAndPDR`、`establishPfcpSession`、`BuildPfcpSessionEstablishmentRequest`、`UPFInterfaceInfo.IP`、`ResolveIP` 等）应当**全部消失**。只剩 `dnscache.refresh` 的栈：每个域名的次数约等于 profile 窗口秒数 ÷ 60，和 UE 数无关。
- **UPF**：`handleSessionEstablishmentRequest:87` 的栈消失。

### 6.3 时延

- `Latency_breakdown.py` 里的 SMF local、UPF local 应当明显下降。
- PFCP_log 里 UPF 的排队时间（`req_recv → req_deq`）应在更高的 RQ 才开始上涨，即拐点右移。
- **按 1123 的 profile 推断（未实测）**：UPF 每个 Establishment 的处理时间从 3.3–4.9 ms 降到约 1 ms，饱和点从约 226 sess/s 提高到约 650 sess/s。实际以这次实测为准。

### 6.4 刷新 / 失效（可选，单独做）

部署里所有 NF 的日志级别都是 error，第 4 节里的 Info/Warn 日志默认看不到。要观察的话，把 SMF、UPF 的日志级别临时调到 info，看以下几点：
- `[DNSCACHE] miss` 只在启动时出现，每个域名 1 次；
- 空闲超过 1 min 后，刷新照常运行；
- 重启 UPF Pod 后，1 min 内出现 `refresh ...: old -> new`。如果在途有 PFCP 请求失败或心跳丢失，会在几秒内出现（4.10、4.11 节）。注意 7.3 节：IP 变了以后，现有代码本身就会出问题。这个检查只用来验证缓存逻辑，做完要按完整顺序重启所有 NF。

---

## 7. 风险与限制（1124 审阅结论）

**先说明你的部署方式对这些风险的影响**：
- `cloudlab/K8s/Deploy_Update_Free5gc/deploy_newtar.sh` 里的 `ordered_restart`（:104-117）按"UPF → NRF → 其余 NF（含 SMF）"的顺序重启。按这个流程重启所有 NF 后，SMF 启动时建的表和缓存都是基于 UPF 的新 IP，**7.3 不会发生**。
- 部署里只有一个 UPF，**7.4 不适用**。
- 剩下的主要是 7.1：它只在实验跑的过程中 CoreDNS 出错时才会触发。

### 7.1 刷新失败 → 删除 → DNS 仍失败 → SMF 进程可能崩溃（用户已选择沿用 Aether 的删除规则）

- **触发过程**：
  1. 刷新（定时或触发）时 DNS 查询失败，条目被删；
  2. 之后的调用走未命中路径，同步去查；如果 DNS 还在失败，`ResolveNodeIdToIp` 返回 `0.0.0.0`；
  3. 同一个会话里，先前用真 IP 建了 `PFCPContext[真IP]`（`sm_context.go:531`），后面用 `"0.0.0.0"` 去查得到 nil；
  4. `build.go:392` 取 `.LocalSEID`，或 `datapath.go:168` 给 `.RemoteSEID` 赋值，触发 nil 指针。
- **后果**：这两处都跑在 `go establishPfcpSession`（`datapath.go:100`）开出来的 goroutine 里，没有 `recover`，panic 会让整个 SMF 进程退出。
- **概率**：要求实验跑的过程中 CoreDNS 出错。加缓存后 SMF/UPF 对 CoreDNS 的查询从每会话 26 次降到每分钟几次，CoreDNS 被打满的情况会消失。
- **和改动前比**：改动前每次调用都查 DNS，暴露得更多。
- **检测**：每轮实验后看 SMF Pod 的重启次数（`kubectl -n free5gc get pod` 的 RESTARTS 列）。
- **可选的更稳做法（未采纳）**：刷新失败时保留旧 IP，只在成功拿到不同 IP 时才更新。

### 7.2 同一个 UPF 的两个名字各自缓存

- **两个名字**：建会话上下文时用"SMF 配置里 UPF 的 NodeID"，收到回包后查上下文用"UPF 回包里自报的 NodeID"（`datapath.go:167`）。根据 1123 的数据推断，这是两个不同的字符串。
- **风险**：它们在缓存里是两条独立的记录。只要某一刻二者对应的 IP 不一致，就会出现 7.1 那样的 nil 指针。不一致来自两种情况：一条刷新失败被删（属于 7.1），或者 IP 正好在两条的刷新之间变化（属于 7.3）。
- **在你的部署里**：按完整顺序重启所有 NF，加上实验中 DNS 不出错，这一条不会发生，**不需要另外处理**。

### 7.3 UPF 的 IP 变化（只单独重启 UPF 时才会发生）

- **SMF 会用旧 IP 一段时间**：最多 1 min（定时刷新）。如果在途有 PFCP 请求失败或心跳丢失，会缩短到几秒（3.6 节）。
- **以 IP 为键的表不会跟着更新（现有问题，不是本改动引入的）**：
  - 涉及的表：`UPFIPToName`（`user_plane_information.go:199`）、`UPFsIPtoID`（:78、:454），以及每个会话的 `PFCPContext`。
  - UPF 的 IP 一变：新会话在 `sortUPFListByName` 里选不到 UPF，会被拒绝；变化之前建好的会话做 Modification 时，`build.go:522` 会触发 nil 指针。
  - 改动前也一样：每次重新解析会立刻拿到新 IP，同样对不上旧表。
- **结论**：不要只重启 UPF。要重启就按 `ordered_restart` 的顺序把所有 NF 一起重启。

### 7.4 一个名字解析出多个 IP（现在不适用）

- 只有将来多个 UPF Pod 挂在同一个名字下时才会出现。K8s 默认的 CoreDNS 会打乱返回的 A 记录顺序（`loadbalance` 插件）。
- 每分钟刷新时取到的"第一个 IP"可能不同，会被当成"IP 变了"去更新，后果同 7.2、7.3。
- 到时的改法：刷新结果里只要还包含当前缓存的 IP，就不更新。

### 7.5 返回的是表里同一块内存（低）

- `ResolveIP` 和 `UPFInterfaceInfo.IP` 直接返回表里那个 `net.IP`。如果有调用方原地修改了它的字节，缓存就被改坏了。我没有逐个检查调用方。
- `ResolveNodeIdToIp` 返回的是 `To16()` 新分配的拷贝，不受影响。
- 让 `Lookup` 返回拷贝就能排除（未纳入本计划）。

### 7.6 触发刷新风暴（已处理）

- UPF 挂掉时，在途的几百个 PFCP 请求会陆续失败。
- 3.6 节的合并规则（同一时刻只跑一个、两次至少间隔 5 s）把这种情况限制为每 5 s 最多一轮刷新。

### 7.7 效果可能不如推断

- 去掉 DNS 之后，瓶颈会转到别处：UPF 单个 goroutine 本身的处理、SMF 其他步骤、GC。
- 6.3 节"约 650 sess/s"是推断值。对比时，镜像的日志和 profile 设置要和 1123 保持一致。

### 7.8 其他

- **地址选择**：规则是"优先 IPv4"。在双栈集群里，这可能和原来 `LookupHost` 的 `ns[0]` 不同；纯 IPv4 集群里两者相同（3.2 节）。
- **日志不可见**：日志级别是 error，缓存是否生效要靠 block profile 判断（6.2 节），不靠日志。

---

## 8. 不做的事

| 不做 | 原因 |
|---|---|
| 按 DNS 记录的 TTL 失效 | 用户指定固定 1 min 刷新，与 Aether 一致 |
| 缓存失败结果 | 与 Aether 一致。缓存了失败，一次 DNS 抖动就会卡在 `0.0.0.0` 上 |
| 刷新失败时保留旧 IP | 审阅时提出（7.1），用户选择沿用 Aether 的"失败删除" |
| `Lookup` 返回拷贝 | 审阅时提出（7.5），未纳入 |
| 重建以 IP 为键的表（7.3） | 现有问题，且需要改 SMF 的 UPF 选择和会话上下文索引，范围大。用"按顺序重启所有 NF"规避 |
| 改 7 处"只为写日志"的解析 | 有缓存后它们只是一次读表，不再发 DNS |
| 改 SBI(HTTP) 的 DNS | 它们只在新建 TCP 连接时查（1123：每轮每个 NF 1–14 次），与会话数无关 |
| 改 NRF 发现缓存 | 那是另一项工作（`de9886c` "add NRF cache in SMF"，`DISCCACHE.md`） |
