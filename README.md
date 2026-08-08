# pollmux

HTTP 长轮询虚拟连接 + yamux 多路复用的共享传输库。

把"字节如何在两台机器之间流动"这一层单独抽出来：客户端、服务端 handler、以及 yamux 配置与重连循环这些容易各写一遍的胶水。

pollmux 只管字节怎么在两台机器之间流动，不管这些字节是什么、也不管两端是什么角色。应用语义（role、endpoint、subdomain、client_id 之类）一律走不透明的 `meta`。

---

## 用之前必须先知道的几件事

### 一、`EnableKeepAlive` 必须为 false

这不是可选优化，是**正确性前提**。请始终用 `pollmux.YamuxConfig()` 构造 yamux 配置，或至少保证这一项为 false。

yamux 的 keepalive 会周期性发 PING 并要求对端在 `ConnectionWriteTimeout`（默认 10s）内回 PONG。但在长轮询传输上，一次 PING-PONG 往返需要：

1. PING 写进本地发送缓冲 → 等下一次 POST 把它送出去；
2. 服务端 yamux 处理后把 PONG 写进它的下行管道；
3. PONG 要等客户端**下一次 poll** 才被取回 —— 而此刻很可能有一个 poll 正挂在服务端等待，最长可挂 `poll_timeout`（默认 30s）。

30s > 10s，于是 yamux 在链路完全健康的情况下断定"连接已死"并关闭会话。**存活性完全由 poll 循环承担**：poll 的 `ResponseHeaderTimeout` 被设为服务端下发的 `poll_timeout` 加一段宽限，超时即判定传输失败；服务端侧则靠 `session_timeout` 与 `pollInFlight` 感知客户端消失。

这个约束很容易在每个建 yamux 会话的地方靠一句注释重复传递。收进 `YamuxConfig()` 就是为了它只存在一处。

`PollMode = "stream"` 下结论不变，但机制更简单：流式响应内部本来就周期性地发心跳帧（见下），这本身就是一条应用层 keepalive，yamux 自己的 PING/PONG 彻底没有存在必要——但仍然必须保持关闭，理由和 batch 模式一样：yamux 不知道底下是 batch 还是 stream，打开 keepalive 唯一的效果就是白白重新触发上面这套超时问题。

### 二、上下行吞吐是不对称的（在 batch 模式下,以及在只开了一半流式的情况下）

两个方向走的是**不同机制**，性能特征因此不同——但这只在你只开了一个方向的流式模式时才成立。两个方向都开(见下一节)之后,两边其实是同一种机制。

| 方向 | 机制 | 受什么限制 |
|---|---|---|
| 上行（客户端 → 服务端，`PreferStreamUpload = false`） | 数据到达即发一个离散 POST，短窗口内合并，同一时刻只有一个请求在途 | **每 RTT 最多发出一个 `max_send_bytes` 分片** |
| 上行（客户端 → 服务端，`PreferStreamUpload = true`） | 一条长驻 POST 的请求体保持打开，数据随到随发 | 链路带宽本身，不再逐 RTT 摊销 |
| 下行（服务端 → 客户端，`PollMode = "batch"`） | 长轮询响应，一次最多带回 `poll_buffer_bytes` | **每 RTT 最多一个缓冲区** |
| 下行（服务端 → 客户端，`PollMode = "stream"`） | 长轮询响应保持打开，数据随到随推 | 链路带宽本身，不再逐 RTT 摊销 |

早期版本这里写的是"上行基本不受 RTT 限制"——**这个结论是错的**，已经用 benchmark 证伪：上行原来的离散 POST 实现和 batch 模式下行是同一种"一个 RTT 一个分片"的结构，只是分片大小换成了 `max_send_bytes` 而不是 `poll_buffer_bytes`。`bench_test.go` 的 `BenchmarkUploadThroughput_Batch_150msRTT` 在这台开发机上量到约 1.9 MB/s，和下面 batch 下行的量级完全一致，不是巧合。

batch 模式下行的含义要算一下：`poll_buffer_bytes` 默认 256KB，在 150ms RTT 下单条隧道下行上限约 **1.7 MB/s**。这个上限来自"离散 POST + 批量响应"这个模型本身，不是实现缺陷。彻底解法是流式化：下行把 `ServerConfig.PollMode` 设为 `"stream"`，上行把 `PreferStreamUpload`/`UploadStreamMode` 也谈成 `"stream"`（两者通常一起开，见下一节），响应/请求体都不再等一整个 RTT 攒够一个缓冲区才收发，而是保持连接打开，数据一到就编帧推送。仓库自带的 `bench_test.go` 在这台开发机上的一次实测（150ms RTT，8MB payload）：

| | batch | stream |
|---|---|---|
| 下行 `BenchmarkThroughput_*` | ~1.0 MB/s | ~28.7 MB/s |
| 上行 `BenchmarkUploadThroughput_*` | ~1.9 MB/s | ~30 MB/s |

两个方向流式化之后都不再随 RTT 线性下降,量级也基本对齐。这是本地回环 + 人为延迟的模拟数据，不是真实链路的数字；生产环境请在自己的部署上重新跑一遍这些 benchmark（`go test -run '^$' -bench Benchmark -benchtime=1x`）。

同一个数字也出现在 yamux 的流窗口上：yamux 强制 `MaxStreamWindowSize ≥ 256KB`（`mux.go:83`，地板值不是默认值），所以两处的 256KB 是对齐的。调小任何一个都会成为新瓶颈。

代价是内存：256KB × 256 并发流 = **64MB/隧道**最坏值，多租户下要乘以隧道数。这个界由 yamux 的流控提供（`BufferedPipe` 自身没有背压），要压低就调并发流上限，不是调窗口。`BufferedPipe.HiWater()` 可以读到实际水位，用它判断离最坏值有多远。

### 三、流式模式（`PollMode = "stream"`）

开启方式：服务端 `ServerConfig.PollMode = pollmux.PollModeStream`，客户端 `Connector.PreferStream = true`。两边都要设——协商在 connect 时一次性谈好，按"客户端请求 && 服务端支持"决定，任一边不满足都静默降级到 `batch`，不报错。旧服务端（不认识 `prefer_stream_mode` 字段）和旧客户端（不发这个字段）完全无感知，`ProtocolVersion` 不需要跟着升级。

新增两个只在流式模式下生效的旋钮，和其余参数一样由服务端权威下发（见"参数下发"一节）：

- `ServerConfig.HeartbeatInterval`（默认 10s）：下行响应空闲多久发一次心跳帧，语义上相当于 batch 模式里 `PollTimeout` 的角色。
- `ServerConfig.StreamMaxDuration`（默认 45s）：一条流式响应最长占用多久，到点服务端主动干净结束、客户端立即重开一条。这不是流控，是为了避开链路上中间代理（nginx、Cloudflare 之类）自己的 idle/read 超时——**这个值必须留在你部署链路里最紧的那个中间层超时之下**，pollmux 自己检测不到外部代理的超时设置，量清楚这件事是使用者的责任（可以参考 `ServerConfig.check()` 里 `StreamMaxDuration >= 2×HeartbeatInterval` 的自检，同样的道理，但那只管本地两个参数是否自洽，管不到外部代理）。

客户端侧多一层存活性检测：流式响应的 HTTP 响应头几乎立刻返回（不像 batch 要等一整个长轮询超时），所以 `ResponseHeaderTimeout` 只覆盖连接建立，不再是"链路是否还活着"的信号；取而代之的是一个读空闲看门狗，每收到一帧（心跳或数据）就重置，超过 `HeartbeatInterval + PollGrace` 没收到任何帧就判定传输失败。

响应体内部有四种帧：`frameData`（数据）、`frameHeartbeat`（心跳）、`frameEnd`（良性结束——`StreamMaxDuration` 到期轮转，会话仍然活着，客户端应立即重开一条新的流式轮询）、`frameGone`（致命结束——会话已经在服务端被关闭，重开轮询只会对着一个不存在的 session id 反复拿到同样的 `frameGone`，客户端必须把它当成 batch 模式里 410 的等价物，触发 `TransportFailed` 走重连，而不是重开轮询）。这两种"结束"语义不同，帧类型也必须不同——`v0.1.0` 曾经两者共用 `frameEnd`，导致会话被关闭后客户端分不清，一直对着死会话重开轮询，`v0.1.1` 起分离为独立帧类型修复。

### 四、上行流式模式

上一节的流式化只覆盖下行。上行（客户端 → 服务端）默认仍然是离散 POST——"二、上下行吞吐是不对称的"里已经说明，这不是"基本不受 RTT 限制"，而是和 batch 下行同构的瓶颈，只是分片大小不同。上行流式化把同一套思路应用到反方向。

开启方式：客户端 `Connector.PreferStream = true` 会同时请求下行和上行两种流式（内部分别置位 `ConnectRequest.PreferStreamMode` 和 `PreferStreamUpload`），服务端只要 `ServerConfig.PollMode = pollmux.PollModeStream` 就同时支持两个方向——**没有单独的"只支持上行"开关**，一个部署要么两个方向都提供，要么都不提供。两个方向仍然在协议层面**独立协商**（`ConnectResponse.PollMode` 和 `UploadStreamMode` 是两个字段），纯粹是为了让新旧版本的客户端/服务端可以任意混搭：一个只认识下行流式的旧服务端，收到不认识的 `prefer_stream_upload` 字段会直接忽略，新客户端读到 `UploadStreamMode` 是空字符串就照常退回离散 POST 上行，不会去发它读不懂的请求。

机制上行方向不是对下行 `pollStream` 简单镜像，因为上行方向读者和写者是反过来的：客户端是写者（把数据编帧写进请求体），服务端是读者（`PollHandler` 收到 `X-Send-Stream: true` 后转给 `pollSendStream`，循环解帧、边收边喂给应用层，不等请求结束）。这个角色反转带来一个关键设计决定：

**轮转（`StreamMaxDuration` 到期后开下一条请求）永远由写者决定，不是读者。** 下行方向服务端是写者，服务端说了算；上行方向客户端是写者，改成客户端说了算——客户端在写完一个完整的数据/心跳帧之后才检查是否到点，从不在帧中途做这个决定。原因是正确性：如果读者单方面决定"够了，我要回应了"，它没法保证自己是在帧边界上做这个决定，一旦在帧中途把响应发回去，请求体会被 net/http 当成没读完直接把连接关掉，此时数据可能已经从本地缓冲区取出、正在传输路上，会真实丢失、造成 yamux 流失步。写者永远知道"刚完成一个完整单元、现在切换安全"这件事，读者不知道。

其他要点：

- `frameGone` 在上行方向用不上——它在下行方向的语义是"服务端主动通知会话已关闭"，但上行方向服务端是读者，没有主动写请求体的机会。会话被服务端关闭这件事,上行方向靠 410 状态码表达（`pollSendStream` 发现 `Session` 已关闭就直接回 410，客户端按老规矩当传输失败处理），不需要新的帧类型。
- 上行方向"会话已死"的检测**不是独立的**，是从下行那条腿"借"来的：两个方向总是一起协商、一起打开，会话被关闭时下行那条腿的 `frameGone` 会先感知到并触发 `c.fail()`，进而通过共享的 `context` 取消掉上行这条腿正在进行的请求。上行自己的 410 检测只是兜底，不是第一道防线——独立触发时最长要等到下一次心跳（写者试图往一个已经断开的管道里写心跳帧时才会发现），不是瞬时的。
- 服务端读上行请求体时用 `http.ResponseController.SetReadDeadline` 做空闲看门狗（每收到一帧就重置），这是 Go 1.20+ 的标准机制，不需要额外的 context/goroutine 拼接。

详见 pollmux 仓库的 `DESIGN.md`（不随库发布，是给下一个改这块的人看的实现笔记）。

---

## 协议与行为约定

### 状态码

| 状态码 | 含义 | 客户端反应 |
|---|---|---|
| 200 | 有数据 / 发送成功 | 继续 |
| 204 | 长轮询超时无数据，**正常心跳** | 立即重新 poll |
| 401 | 鉴权失败 | 致命 → 传输失败 → 退避重连 |
| 404 | 会话不存在 | 致命 → 重连 |
| **410** | **会话已被服务端关闭** | 致命 → 立即重连 |
| **413** | 请求体超限 —— **协议违规** | 致命 → 记录并重连 |
| 426 | `protocol_version` 不支持 | 致命 → 停止重试（`ErrProtocolVersion`）|
| 3xx | 反扫描重定向，通常意味着鉴权失败 | 致命 → 检查 token |

**410 与 204 必须分开**：两者共用一个码的话，服务端关掉会话之后客户端会把它当成正常心跳，继续空转轮询一个死会话，直到自己的 `session_timeout` 到期才重连。分开之后是秒级。

**413 是协议违规而不是可恢复状况**：服务端在 connect 时下发 `max_send_bytes`，客户端取 `min(本地配置, 下发值)`，所以守规矩的客户端不可能发出超限请求。收到 413 说明对端有 bug，正确反应是大声记日志并重连，而不是减半重试 —— 那是一条带状态的重试路径，是 bug 的温床。

**3xx 不会被跟随**：客户端设了 `CheckRedirect: http.ErrUseLastResponse`。否则 Go 默认跟随重定向，`resp.StatusCode` 根本看不到 3xx，一次清晰的鉴权失败会变成别处一个莫名其妙的解析错误。

流式模式不引入新状态码——上面这张表在 `PollMode = "stream"` 下原样适用（410、413 等语义不变），分帧发生在响应体内部，是应用层的事，不需要 HTTP 层再表达一次。

### 参数下发

需要两端一致的数字由服务端做权威并在 connect 时下发（`max_send_bytes`、`poll_timeout`、`session_timeout`、`poll_buffer_bytes`，流式模式下再加 `heartbeat_interval_ms`、`stream_max_duration_ms`），客户端只能更保守，不能更激进。这与 HTTP/2 的 SETTINGS、TCP 的 MSS 协商是同一个道理：**不要两边各配一份**。

客户端在 connect 时还会自检 —— 如果 `poll_timeout + PollInterval >= session_timeout`，说明这个客户端健康时也会被服务端当成掉线扫掉，于是直接返回错误而不是带着这个隐患跑起来。

### `ReconnectLoop` 的退避

- `OutcomeTransportFailed`（链路问题）→ 退避并翻倍，上限 `MaxBackoff`。
- `OutcomePeerClosed`（对端走了，链路健康）→ 只短暂停顿，**不推进退避**。
- **每次连接成功都会重置退避。** 因此"连上就立刻断"的抖动链路会稳定在 `InitialBackoff` 反复重试，不会升级。这是为了真实故障恢复后能立刻恢复速度而做的取舍。

注意 `OutcomePeerClosed` 需要调用方判断：yamux 会话结束本身不区分"链路断了"和"对端走了"，要在 `sess.CloseChan()` 触发后再查一次 `conn.TransportFailed()`。另外 `yamux.Session.Close()` 会连底层 conn 一起关，所以如果底层 conn 就是这条隧道，关 yamux 等于拆隧道，对端只会看到传输失败 —— 想要"对端走了但隧道保留"，需要应用层另设信令。
