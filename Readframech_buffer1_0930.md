# readFrameCh 容量 0 → 1：让 readFrames 不再阻塞在交帧 channel 上 —— 代码修改计划（0930）

> **状态：代码已实施（2026-09-30），分支 `readframech-buf1-0930`（基于 6e8a1ff），未提交。**
> 改动：`xnet/http2/server.go:459` 1 行 + 新增单测 `xnet/http2/readframech_cap_tycustom_test.go`。
> 已核对：`git diff -U0 6e8a1ff` 只有 `@@ -459 +459 @@` 一个 hunk，server.go 总行数 3433 不变。
> **尚未编译验证**——本机没有 go 和 docker，7.3 的 gofmt / vet / test 需在编译机执行。
> 改动对象：`xnet/http2/server.go`（x/net v0.47.0 的本地 fork，Go 1.25.5，h2c prior knowledge）。
> 对照基线：`cloudlab/Ty_log/Free5gc/R6525_NF8HTTP_500ms_HTTP13logs_runtime_0914v8`（每对 NF 8 条连接）。
> 本机没有 Go 工具链，文中所有 `go` / `gofmt` 命令都在编译机或 `golang:1.25-bookworm` 容器里执行。

---

## 0. TL;DR

| # | 项 | 结论 |
|---|---|---|
| 1 | 改什么 | `xnet/http2/server.go:459`：`make(chan readFrameResult)` → `make(chan readFrameResult, 1)` |
| 2 | 改多少 | **1 行，原地改，不增不删行**（为什么行号不能动见 7.4） |
| 3 | 改完的效果 | readFrames 读完一帧后，**880 行的发送一定不会 park，与 RQ 无关**（证明和适用范围见 5.1）：serve 正在主 select 里等就直接交给 serve，serve 不在就放进缓冲。readFrames 不再停在 879，直接走到 884 等 gate |
| 4 | 为什么安全 | gate 一行没动：readFrames 仍要等 serve 处理完上一帧才读下一帧 → channel 里任何时刻最多 1 帧，容量 1 永远不会满；帧内存在 readMore 之前不会被覆盖 |
| 5 | 覆盖哪些 NF | go.mod 里有 `replace golang.org/x/net => ../../xnet` 的 7 个：amf ausf nrf nssf pcf udm udr。实验里的服务端 AUSF/PCF/UDM/UDR 全部覆盖 |
| 6 | 代码基线 | 从 `6e8a1ff`（8 conn）拉分支，新镜像与 0914v8 镜像只差这 1 行 |
| 7 | 怎么判定改动进了二进制 | trace 里 readFrames 停在 `server.go:879` 的次数 = 0（所有 RQ） |
| 8 | 不做 | gate 不改缓冲、不去掉 gate、不改客户端、不在 server.go 加新行（第 10 节） |

---

## 1. 术语（本文的准确定义）

- **readFrames G**：每条服务端 HTTP/2 连接一个，跑 `(*serverConn).readFrames`（server.go:874），只做"从 socket 读一帧、交给 serve、等放行"。
- **serve G**：每条连接一个，跑 `(*serverConn).serve` 的主循环（server.go:1007），处理所有帧和 handler 侧事件。h2c 下就是 net/http 的 `(*conn).serve` 那个 G。
- **readFrameCh**：readFrames → serve 交帧的 channel。459 创建、612 声明、880 发送、1018 接收。现在容量 0。
- **gate**：readFrames 函数里的局部 channel（875，容量 0）。serve 处理完一帧调 `res.readMore()`（1032），即 `gate <- struct{}{}`（876）；readFrames 在 884 收到后才读下一帧。
- **H 帧 / B 帧**：readFrames 走到 880 发送时，serve 正停在主 select 里等 → **H**（handoff）；serve 不在主 select 里（正在处理别的事件，或已就绪但还在排队）→ **B**。

---

## 2. 现状：容量 0 时一帧怎么走

```go
// xnet/http2/server.go:874-893（不改）
func (sc *serverConn) readFrames() {
	gate := make(chan struct{})
	gateDone := func() { gate <- struct{}{} }                       // 876
	for {
		f, err := sc.framer.ReadFrame()                              // 878
		select {                                                     // 879
		case sc.readFrameCh <- readFrameResult{f, err, gateDone}:    // 880
		case <-sc.doneServing:
			return
		}
		select {                                                     // 884
		case <-gate:
		case <-sc.doneServing:
			return
		}
		if terminalReadFrameError(err) {                             // 889
			return
		}
	}
}

// serve 主循环里收帧的分支（不改）
		case res := <-sc.readFrameCh:                                // 1018
			...
			if !sc.processFrameFromReader(res) {                     // 1029
				return
			}
			res.readMore()                                           // 1032
```

- **H 帧**：880 直接把帧拷给等着的 serve 并把 serve 唤醒，readFrames 不阻塞，接着到 884 阻塞等 gate。
- **B 帧**：readFrames **阻塞在 879**。serve 回到主 select 收帧时把 readFrames 唤醒（goready → 放进 serve 当前 P 的 runnext）。serve 处理这一帧时 `go runHandler` 等操作把 readFrames 从 runnext 挤到本地队列尾，所以 serve 处理完调 readMore 时 readFrames 多半还没走到 884 → **serve 阻塞在 876 的 gate 发送上**（栈 `chansend1 <- readFrames.func1 <- serve:1032`）。readFrames 走到 884 收 gate，把 serve 放进**自己 P 的** runnext，然后在同一个 P 上直接读下一帧；如果下一帧已经在 socket 缓冲区里，serve 跑不上，下一帧又是 B。这就是锁步。

0914v8 UDM→UDR 8 条连接的 trace 实测（2026-09-28 分析，RQ200 → 1800）：

- B 帧占比 0.14% → 35.4%。拆开：进入概率 a = P(B | 上一帧 H) 0.12% → 17%；维持概率 stay = P(B | 上一帧 B) 16% → 69%。
- 积压帧（ReadFrame 时数据已在缓冲区、不用等 netpoll 的帧）流水线周期中位数：非锁步 44–52 µs，锁步 66–84 µs；RQ1800 锁步占积压帧 39.9%。
- serve 排队均值 8.1 → 16.5 µs 的增量里，72% 来自锁步中 readFrames@884 唤醒 serve（次数 19 → 4299，单次均值 16.6 → 41 µs）。

一个 B 帧比 H 帧多两个阻塞点：readFrames 停在 879，serve 停在 876。

---

## 3. 改动

```go
// xnet/http2/server.go:459 —— 改前
		readFrameCh:                 make(chan readFrameResult),
// xnet/http2/server.go:459 —— 改后（同一行，说明写在行尾注释里）
		readFrameCh:                 make(chan readFrameResult, 1), // TYcustom: gate keeps <=1 frame in flight, so readFrames never parks on this send
```

改动只有这一处。`grep -rn readFrameCh xnet/` 只命中 459（创建）、612（字段声明）、880（发送）、1018（接收）、1545（注释）五处，channel 只在 459 创建。readFrames 和 serve 的函数体都不动。

---

## 4. 改完以后一帧怎么走

Go runtime 处理带缓冲 channel 的顺序（Go 1.25.5，`Free5gc_official/shared-libs/gostd-net/src/runtime/`）：

- **发送**（select.go:293-298，chan.go:229/236）：先看有没有正在等的接收者，有就直接交接，**与容量 0 完全相同**；没有才写缓冲（`bufsend`），不阻塞。
- **接收**（select.go:276-281）：先看有没有阻塞着的发送者，没有再看缓冲（`bufrecv`）。**从缓冲取数据不唤醒任何 G。**

| 情况 | 容量 0（现在） | 容量 1（改后） |
|---|---|---|
| H：serve 在主 select 等 | 880 直接交接，readFrames 唤醒 serve，自己到 884 阻塞 | **完全相同**，帧不经过缓冲 |
| serve 不在主 select（原 B） | readFrames 停在 879；serve 收帧时唤醒 readFrames；serve 处理完多半停在 876 | readFrames 把帧写进缓冲，**不阻塞**，直接到 884 阻塞。serve 回主 select 从缓冲取帧（不唤醒谁），处理完调 readMore 时 readFrames 已经停在 884 → 直接交接，readFrames 进 serve 当前 P 的 runnext，**serve 不阻塞**，继续回主 select |
| 这种情况下每帧的阻塞点 | readFrames@879 + serve@876 | 只有 readFrames@884 |

（readFrames 在 878 ReadFrame 里等 netpoll 的阻塞两个版本一样，表里没列。）

**锁步为什么会断**：改后 readMore 不会让 serve 阻塞，serve 不会被放进 readFrames 的 runnext。readFrames 要等 serve 让出 P（回主 select 没事可做而阻塞）或被别的 P 偷走才会跑；它读完下一帧时，前一种情况 serve 正在主 select 里等 → 下一帧是 H。下一帧仍是"serve 不在 select"，只可能是 serve 自己还有别的事要处理，而不是被上一帧的锁步拖住。

**残留情况**：serve 已经从缓冲取走帧并处理完，readFrames 却还没从 880 走到 884 → serve 仍会阻塞在 876（与现在同一个栈）。readFrames 在 880 写完缓冲后，到 884 park 之间只有一次 selectgo，通常不到 1 µs；而 serve 要在这段时间里取帧并跑完 processFrameFromReader（HEADERS 帧还要建 stream、`go runHandler`）。只有 readFrames 在两次 select 之间被抢占、或它的 M 丢了 P 才会发生。预期接近 0，但不保证为 0。

---

## 5. 为什么语义安全

### 5.1 不变量：缓冲里任何时刻最多 1 帧，容量 1 永远不会满

- readFrames 只有在 884 收到第 N 帧的 gate 之后，才会调 ReadFrame 读第 N+1 帧；
- serve 只有在 1018 从 readFrameCh 取到第 N 帧之后，才会在 1032 对它调 readMore；
- 所以 readFrames 在 880 发第 N+1 帧时，第 N 帧已经被 serve 取走，缓冲是空的，发送不会 park。

推论：容量 > 1 和容量 1 没有任何区别，gate 不放行，readFrames 就不会读第二帧。

**这个保证与 RQ 无关。** 上面是一条先后顺序链（取走第 N 帧 → readMore → 收 gate → 读第 N+1 帧 → 发送），不依赖任何时间长短；RQ 再高也只是把链上每一步拉长，顺序不变。它成立的前提（已逐条 grep 核对）：

| 前提 | 代码位置 |
|---|---|
| readMore（即 gateDone）只在 serve 收到帧之后调用，每帧一次 | 876 定义，1032 是唯一调用点，紧跟在 1018 收帧之后 |
| readFrameCh 只有一个接收点 | 1018 |
| 每条连接只有一个 readFrames G | 1000 `go sc.readFrames()`，serve 每个 sc 只调一次（582） |
| gate 容量 0、readFrames 读下一帧前必须先收到 gate | 875、884，本次不动 |

以后若改动这四条中的任意一条（例如给 gate 加缓冲、在别处调 readMore），这个保证要重新证明。

**保证的范围只到"不会 park 在 879"，不包括下面三件事**：

1. **readFrames 照样会等，只是换了地方。** 它仍会停在 884 等 gate，也仍会停在 878 等 netpoll。原来 B 帧停在 879 的那段等待，改后挪到 884，长度 = 帧在缓冲里等 serve 的时间 + serve 处理这一帧的时间。一次一帧的串行没有变，高 RQ 下 readFrames 在 884 的等待会变长。
2. **发送内部的 runtime 锁等待还在。** 879 的 select 要同时锁 readFrameCh 和 doneServing 两把 channel 锁；doneServing 也被同一连接的 handler G 在 select 里锁（sendServeMsg 1141、writeDataFromHandler 1203、writeFrameFromHandler 1239、writeHeaders 2545、noteBodyReadFromHandler 2577）。锁被占时 readFrames 所在的 M 会自旋或 futex 睡一会儿。这是 M 级等待，G 不进 sendq，trace 里不算阻塞事件，只算运行时间；临界区很短，旧代码完全一样，不是这次改动引入的。
3. **抢占、GC 栈扫描**可能恰好发生在这一行，trace 里会留下栈含 `readFrames:879` 的事件，但那不是 channel 阻塞（统计口径见 8.2）。

### 5.2 帧内存不会被提前覆盖

- 注释原文：frame.go:259 `Frames are only valid until the next call to Framer.ReadFrame.`；server.go:861 `f Frame // valid until readMore is called`，864-867 `After readMore, f is invalid and more frames can be read.`
- 帧在缓冲里的这段时间，readFrames 停在 884，不会调 ReadFrame，Framer 复用的 readBuf（frame.go:448-452）不会被覆盖。

### 5.3 退出路径没有 goroutine 泄漏

- **对端关连接 / 读出错**：readFrames 把带 err 的结果交出去（缓冲或直接交接），停在 884。EOF 类错误下 processFrameFromReader 返回 false → serve 返回 → doneServing 关闭 → readFrames 从 884 返回；其他错误 serve 调 readMore，readFrames 在 889 `terminalReadFrameError` 返回。与现在相同。
- **serve 先退出**（超时、GOAWAY 结束等）：serve 的 defer 按 LIFO 执行，`close(sc.doneServing)`（951）先于 `sc.conn.Close()`（948）。readFrames 若停在 884 → 走 doneServing 分支返回，缓冲里没取走的帧随 sc 一起回收；若停在 ReadFrame → conn.Close 让它返回错误 → 880 的 select 两个分支同时就绪、随机选一个：选发送就写进空缓冲，然后在 884 遇到已关闭的 doneServing 返回；选 doneServing 就直接返回。
- 现在容量 0 时，后一种情况 readFrames 是停在 879 等 doneServing 退出，结局相同。

### 5.4 其他不受影响的东西

- **帧顺序**：单生产者、单消费者、FIFO，不变。
- **serve 侧逻辑**：`lastFrameTime`（1019）、先收写完成结果（1022-1028）、settingsTimer（1033）都在 serve 拿到帧之后执行，和帧是从缓冲还是直接交接拿到的无关。
- **TYcustom 13 个时间戳**：服务端的 recvwholereq / server_handler_go_time / req_time / resp_time 和 flushed 事件，分别打在 serve G、handler G 和 bufferedWriter 上，readFrames 里没有打点 → 字段定义不变，现有 HTTP 拆段脚本直接可用。

### 5.5 注释原文和推断要分开

- **原文**（873-874）：`It takes care to only read one frame at a time, blocking until the consumer is done with the frame.` 即"一次一帧"是 gate 实现的。
- 上游代码和注释**没有说明** readFrameCh 为什么不带缓冲。**推断**：gate 已经保证了正确性，缓冲对正确性没有作用，所以没加；没有证据表明上游权衡过它的调度代价。

### 5.6 serve G：select 的语义不变，代码不用改

**readFrameCh 这个 case 什么时候就绪，两个版本等价。** 容量 0 时 = readFrames 正阻塞在发送上（sendq 里有它）；容量 1 时 = 缓冲里有 1 帧。两者都等于"有一帧已经读出来、还没交给 serve"。这一帧什么时候出现由 readFrames 调 ReadFrame 的时机决定，两个版本都是"上一帧 readMore 之后"。select 在所有就绪 case 里均匀随机选一个（select.go 的 pollorder），这条规则也没变。serve 收到的 `res` 是同一个值（f、err、readMore），1019-1036 照常执行。

**变的只有两处调度副作用，都不需要 serve 配合：**

1. **收帧时少一次唤醒。** 容量 0 时从阻塞的发送者收，runtime 的 `recv()`（chan.go:702 起）要 goready readFrames：把它放进 serve 当前 P 的 runnext，原来在 runnext 里的 G（常是刚被唤醒的 handler）被挤到本地队列尾，`ready()` 里还会 `wakep()`，可能叫醒一个空闲 P。容量 1 时从缓冲收（select.go:461-470），只拷贝加清槽，不唤醒任何 G。
2. **readMore 通常不再阻塞**（第 4 节）。serve 调完 readMore 直接回主 select，wantWriteFrameCh、bodyReadCh 等其他 case 能更早被处理。

**逐项核对过、确认不用改的地方：**

| 检查点 | 结论 |
|---|---|
| 缓冲会不会一直持有帧的引用 | 不会。serve 取走后槽位被清零（select.go:465 `typedmemclr`） |
| serve 退出时缓冲里剩一帧，要不要排空 | 不用。旧版这一帧同样没处理（readFrames 拿着它停在 879），两版都是 readFrames 走 doneServing 退出，连接要关了，这帧本来就不处理 |
| 1022-1028 的"先收写完成，再处理新帧" | 只在处理帧时做一次非阻塞收，和帧是从缓冲还是直接交接拿到的无关。"帧在异步写完成之前就被读出"在两版里都可能发生，这次改动不引入新的交错 |
| `lastFrameTime`（1019） | 仍是 serve 收到帧的时刻，口径不变（两版都不是从 socket 读到的时刻） |
| 有没有代码依赖 readFrames 阻塞，或读 `len(readFrameCh)` | 没有。readFrameCh 只出现在 459 / 612 / 880 / 1018 |

**trace 上会看到的间接变化**（不是要改代码，是分析时别误读）：serve 不再停在 876、也不再被 readFrames@884 用锁步方式唤醒，所以 serve 的 park 次数会减少，连续运行段可能变长。这是少了阻塞，不代表 serve 每帧干的活变多；比较 serve 开销时请看每帧 CPU 时间，不要看单段运行时长。

---

## 6. 影响范围

- **生效的 NF**：go.mod 有 replace 的 amf / ausf / nrf / nssf / pcf / udm / udr。0914 系列 HTTP 日志里只有 AMF→{AUSF, PCF, UDM}、AUSF→UDM、PCF→UDR、UDM→UDR 六对，服务端 AUSF / PCF / UDM / UDR 全部在内。
- **不生效的 NF**：smf / chf / nef / n3iwf / tngf / upf / bsf 的 go.mod 没有 replace，用的是模块缓存里的上游 x/net v0.47.0，仍是容量 0。注册实验里它们没有服务端流量，不影响本次 A/B；以后要测它们得先补 replace。
- **客户端不涉及**：transport.go 的 readLoop 读完帧就地处理，没有 readFrameCh / gate 这种两个 G 交接的结构。
- **net/http 自带的 h2_bundle 不涉及**：各 NF 用 `h2c.NewHandler(handler, h2Server)`（如 `NFs/udr/internal/sbi/server.go:213`），h2c 和 http2.Server 都来自 x/net，也就是 xnet。

---

## 7. 实施步骤

### 7.1 拉分支

```bash
cd Free5gc-TYcustom
git switch -c readframech-buf1-0930 6e8a1ff
```

理由：main 当前 HEAD `0f03c9f` 是 16 conn。`git diff --stat 6e8a1ff 0f03c9f` 只改了 7 个 `NFs/*/internal/accesslog/httptransport.go`（connsPerPeer 8 → 16），`xnet/` 两者完全相同。从 6e8a1ff 拉分支，新镜像和 0914v8 镜像只差 readFrameCh 这 1 行。

> "0914v8 镜像是用 6e8a1ff 编的"是按 commit message（`NF-NF 8 http log … 0914 4PM`）和镜像名推断的，编译前请确认。

之后如果也要测 16 conn，在 main 上改同一行即可，这个改动和连接数无关。

### 7.2 改代码

只改 server.go:459，见第 3 节。

### 7.3 静态检查与单测（编译机 / 容器）

```bash
# 1) 确认只动了 1 行、行号没移
git diff --stat                                              # 期望: 1 file changed, 1 insertion(+), 1 deletion(-)
git diff -U0 6e8a1ff -- xnet/http2/server.go | grep '^@@'    # 期望: 只有一个 @@ -459 +459 @@

# 2) 格式 / vet / 单测（xnet 本身是一个 module）
docker run --rm -v "$PWD/xnet":/src -w /src golang:1.25-bookworm sh -c '
  gofmt -l http2                                   # 期望无输出；有输出就 gofmt -w 后重做第 1 步
  go vet ./http2/
  go test ./http2/ -count=1
  go test -race ./http2/ -run "TestServer" -count=1
'
```

TYcustom 已经在 xnet 里加了不少打点，上游测试在 6e8a1ff 上未必全绿。**先在未改的 6e8a1ff 上跑一遍记下失败列表**，改后失败列表不应变长。

（已加）可选：加一个只检查容量的单测，防止以后合并上游时被悄悄改回去。放在新文件 `xnet/http2/readframech_cap_tycustom_test.go` 里（新文件不影响 server.go 行号），写法照 server_test.go 现有的 `TestServer → synctestTest(t, testServer)` 模式：

```go
package http2

import "testing"

// TYcustom: see Readframech_buffer1_0930.md.
func TestTYcustomReadFrameChCap(t *testing.T) { synctestTest(t, testTYcustomReadFrameChCap) }
func testTYcustomReadFrameChCap(t testing.TB) {
	st := newServerTester(t, nil)
	if got := cap(st.sc.readFrameCh); got != 1 {
		t.Fatalf("cap(readFrameCh) = %d, want 1", got)
	}
}
```

`st.sc` 由 newServerTester 在 server_test.go:215 赋值；readFrameCh 创建后不再被写，测试 G 读它不会触发 -race。

### 7.4 为什么 server.go 不能加任何新行（包括注释行）

现有分析脚本靠 trace 栈里的行号区分 readFrames / serve 停在哪：readFrames 879（发 readFrameCh）/ 884（收 gate），serve 1018（收帧）/ 1032（readMore → gate 发送）；`ServeG_whysingleschedlatencyincrease.py` 里直接写着 `serve:1032`。459 之后只要多一行，这些行号全部后移，新旧两组 trace 就不能用同一套脚本比。所以说明文字只放行尾注释和本文档。

### 7.5 编译镜像

按 `cloudlab/K8s/Free5gc_docker_images/UPDATE_free5gc_custom_image.md` 的 B-1 ~ B-4。镜像 tar 建议命名：

```
free5gc-custom-v4.2.2-NF8HTTP_RR_500ms_runtimeprofile_HTTP13logs_wholereqresp_readframech1_0930.tar
```

可选：确认 replace 生效（最终判据仍是 8.2 的 trace 检查）：

```bash
cid=$(docker create free5gc-custom:v4.2.2-custom); docker cp $cid:/free5gc/udr ./udr; docker rm $cid
docker run --rm -v "$PWD":/w golang:1.25-bookworm go version -m /w/udr | grep -A1 'golang.org/x/net'
# 期望: dep golang.org/x/net v0.47.0，下一行 => ../../xnet (devel)
```

### 7.6 部署

同一手册 A-1 ~ A-5：导入镜像 → helm upgrade → **先重启 NRF 再重启其他 NF** → 确认所有 NF 都注册到 NRF。

### 7.7 实验

配置与 0914v8 完全一致：R6525、connsPerPeer = 8、IdleTimeout 500 ms、UE1000、RQ200 ~ 2000 共 10 组、每组 21000 事务、15 s prof_trace 窗口。结果目录建议：

```
cloudlab/Ty_log/Free5gc/R6525_NF8HTTP_500ms_HTTP13logs_runtime_readframech1_0930
```

建议同一天、同一节点先用 0914v8 的 tar 重跑 RQ1000、RQ1800 两组作对照，排除节点和时段差异。直接和 0914v8 的旧数据比也可以，但要把这层噪声考虑进去。

---

## 8. 验证与验收

### 8.1 回归（必须全过，否则这组数据作废）

- 每组 21000 事务零丢失、retry 0；HTTP 13 时间戳四路 join 100%，uri 零失配。
- 每对 NF 严格 8 条连接、零溢出。
- 低 RQ（200 / 400）的注册端到端时延（`latency_RQ*_UE1000.txt`）不应系统性上升：低 RQ 时 B 帧只有 0.14%，改动理论上不影响。

### 8.2 改动确实生效（trace，必须）

| 指标 | 0914v8 | 改后期望 |
|---|---|---|
| readFrames 停在 `readFrames:879`（selectgo，发 readFrameCh）的次数 | = B 帧数，RQ1800 约占 35% 帧 | **恒为 0**。非 0 说明二进制里没有这次改动 |
| `Readframe_wakeup_Reason_UDMUDR.py` 的 channel 类（readFrames 被 serve 主 select 唤醒，栈 `selectgo <- serve`） | 有 | **0** |
| serve 停在 gate 发送（`chansend1 <- readFrames.func1 <- serve:1032`） | 锁步时大量 | 接近 0（第 4 节的残留情况） |

第一行的统计口径：只数 **readFrames G 的阻塞事件（GoBlock，栈顶 `runtime.selectgo`，下一帧是 `readFrames:879`）**，不要数"栈里出现 879"的所有事件，抢占、GC 栈扫描的事件也可能带这一行（5.1 第 3 条）。

### 8.3 效果（和 0914v8 对比）

- **缓冲路径占比** = 1 − (serve 被 `readFrames:879` 唤醒的次数) / 帧数。H 帧在两个版本里都会留下"readFrames@879 唤醒 serve"这个事件，所以这个公式在新旧版本上口径相同；在旧版本上它就等于 B 帧占比（0.14% → 35.4%）。
  **预测（推断，未验证）**：锁步的维持项消失后，这个比例应接近旧的进入概率 a（0.12% → 17%），而不是 35%。如果实测仍明显高于 a，说明主因是 serve 自己忙（handler 侧事件），锁步只是放大器。
- **积压帧流水线周期**（相邻两帧 readFrames 离开 884 的时间间隔，只算 ReadFrame 没进 netpoll 的帧）：旧版非锁步 44–52 µs、锁步 66–84 µs。期望改后全部落在非锁步那一档。
- **serve 排队**：旧版增量的 72% 来自锁步里 readFrames@884 唤醒 serve（RQ1800 4299 次、单次均值 41 µs）。期望这类唤醒基本消失。
- **HTTP 拆段**（UDM→UDR，均值 µs，旧版 RQ200 → 1800）：e2e 1980 → 5575；S_in（client wrote → server handler_go）60 → 1187（p90 96 → 4026）；handler 1551 → 2949；S_out（handler 结束 → W）98 → 338。
  期望：S_in 在高 RQ 下降，S_out 小幅下降；**handler 不应变**。handler 变了说明还有别的因素在变，A/B 不干净。

### 8.4 分析脚本里要跟着改的口径

- `Readframe_serveGwake_whysingleschedlatencyincrease.py` 里"每一帧恰好产生一次 readFrames 唤醒，要么 select 要么 gate"：改后变成 **帧数 = readFrames 被 gate 唤醒的次数 + serve 停在 gate、被 readFrames 唤醒的次数**，select（channel）项恒为 0。
- **帧在缓冲里等了多久，trace 里没有直接事件**：serve 从缓冲取帧不产生 GoUnblock。旧版 B 帧可以用"readFrames 在 879 park → 被 serve 唤醒"精确量出等待；新版只能量"readFrames 在 884 park → 被 readMore 唤醒"，这段 = 缓冲等待 + serve 处理这一帧的时间。跨版本请用 8.3 的流水线周期比，不要拿这两段等待直接比。
- 用行号的脚本不用改（7.4），前提是 `git diff 6e8a1ff` 只有 `@@ -459` 一个 hunk。

---

## 9. 风险与边界

- **收益上限**：这次只拆锁步。一次一帧的串行（gate）、每条连接只有一个 serve G、handler 本身的耗时都不动。0914v8 UDM→UDR 的 e2e 增量 +3595 µs 里，handler 占 +1398、S_in 占 +1127；本次只针对 S_in 里的排队，e2e 不会按 S_in 的降幅等比例下降。
- **readFrames 改为在 runnext 里等**：改后 readMore 不让 serve 阻塞，readFrames 被放进 serve 当前 P 的 runnext，要等 serve 让出这个 P 或被别的 P 偷走才能跑。偷 runnext 时如果受害 P 正在运行，源码会先 `usleep(3)`，这台 R6525 上实测约 70 µs。这和现在 H 帧的 readMore 路径是同一个机制，不是新东西；区别是以前锁步里 serve 一阻塞 readFrames 就能马上跑，现在要等 serve 回主 select 没事可做。应在 `Readframe_singlesche` 系列里看 readFrames 被 gate 唤醒后的等待有没有上升。
- 单次运行有噪声，见 7.7 的同日对照建议。

---

## 10. 明确不做

| 不做的事 | 理由 |
|---|---|
| gate 也改成容量 1 | 能顺带消掉第 4 节的残留情况（serve 停在 876），但那是第二个变量。先做单变量 A/B；8.2 里 serve@876 仍明显不为 0 时再单独做 |
| 去掉 gate、让 readFrames 连续读多帧 | Framer 复用 readBuf（frame.go:448-452），帧只在下一次 ReadFrame 之前有效（frame.go:259）；去掉 gate 必须先拷贝帧，改动大，还多出拷贝开销 |
| readFrameCh 容量 > 1 | gate 不放行就不会读下一帧，缓冲里最多 1 帧，> 1 不起作用（5.1） |
| 改客户端 transport.go | 客户端 readLoop 没有这种两个 G 交接的结构 |
| 给 smf / chf / nef / n3iwf / tngf 补 replace | 本实验没有它们的服务端流量，要测时再补 |
| 在 server.go 加任何新行（含注释行） | 行号后移，现有分析脚本失效（7.4） |
| 新加打点测缓冲等待 | 要改 readFrames / serve 函数体，行号会动，还给热路径加开销；先用 8.3 的流水线周期比 |
