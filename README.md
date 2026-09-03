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

#### 连接期自动探测：`Connector.UploadStreamPreference`

生产环境实测过一个前提：并不是每条链路都能如实转发一个长驻、持续写入的 chunked 请求体。Cloudflare 标准（非 Enterprise）套餐对普通 HTTP 代理走的是"攒完整个请求体再转发给源站"的模型，不支持把一个还没结束的请求体实时流式转发——一旦上行方向的 yamux 单流未确认数据超过 `MaxStreamWindowSize`（256KB），发送方等待的窗口更新永远等不到，整条隧道会**永久挂死**，不是变慢。这不是理论风险，是线上复现过的真实故障。

`Connector.UploadStreamPreference` 是三态开关，用来在"能获得流式上行的吞吐"和"不确定链路是否支持时不要挂死生产"之间做选择：

- `PollModeBatch`：永远不用上行流式，哪怕服务端提供。`ConnectRequest.PreferStreamUpload` 直接发 `false`，不协商、不探测。
- `PollModeStream`：只要服务端提供就直接用，不做探测。只在你已经用其他方式确认过这条链路（有没有经过 Cloudflare 之类的缓冲代理）能实时转发长驻请求体时才应该这么配置。
- `""`（默认，自动）：协商成功后，`Connect` 先做一次一次性探测——通过一个真正的 send-stream 请求推送超过 `MaxStreamWindowSize` 的填充数据、正常以 `frameEnd` 结束、等真实响应，整个过程套一个 `Connector.UploadProbeTimeout`（默认 15s，可调）的硬超时。探测在超时内正常收到 200 就采用流式上行；超时或出错（`context deadline exceeded` 是最常见的一种，代表这条链路没在超时内把请求转发到源站）就退回离散 POST，这条连接的生命周期内不会再重试流式。探测是有意做成"和生产真实一条 send-stream 请求完全同形"的——推送、结束、等响应——而不是发明一套"请求体还开着就提前应答"的专用协议：那种设计在本地回环网络下用 Go 标准库实测就不可靠（客户端能否在请求体写完之前先读到响应头，取决于 Go/内核缓冲区的内部实现细节，不是文档承诺的行为），线上环境只会更不可控。探测请求带 `X-Send-Stream-Probe` 头，服务端据此丢弃这些帧而不写入会话，不会污染真实数据。

三态里显式配置的两种（`batch`/`stream`）总是零延迟；只有默认的自动档会在每次 `Connect` 上多付出一次探测的时间成本（成功时通常几毫秒，失败时最多 `UploadProbeTimeout`）。

### 五、WebSocket 传输模式

上面两节解决的是"轮询本身有 RTT 瓶颈"；WebSocket 传输模式解决的是另一个、更根本的问题：**有些中间代理对普通 HTTP 干脆不支持真正的双向流式转发**。

生产环境复现过这个故事的两半：一开始怀疑只有上行（长驻 chunked 请求体）会被 Cloudflare 标准套餐整体缓冲（见上一节），于是给上行加了连接期自动探测。但探测用的是一次性、无间隔的大块灌包，和真实隧道流量"长时间打开、数据一阵一阵地来、中间有大段空档"的模式并不一样——探测通过之后，真实流量在同一条链路上还是会挂死，且没有任何报错，双端日志只看到会话每隔几十秒静默重连一次。用一个独立 endpoint 做对照实验（同一个 broker、同一条 Cloudflare 链路，只切 `upload_stream_preference`）确认了这一点：自动档下 9/9 请求全部超时，强制 `"batch"` 下 6/6 请求全部在一秒内成功。换句话说，探测这道防线本身是不可靠的，问题也不是只出在上行方向的字节数上，而是这条链路对"长时间保持打开、间歇性写入"这种模式本身处理有问题。

WebSocket 是绕开这个问题的正确层：Cloudflare（以及几乎所有反向代理）对 WebSocket 连接是逐帧透传的，不会像对付一个长驻 HTTP 请求体那样整体缓冲——这是 WebSocket 协议本身的地位决定的，不依赖套餐等级或额外配置。

**开启方式**：服务端 `ServerConfig.EnableWebSocket = true`，客户端 `Connector.PreferWebSocket = true`。协商独立于 `PollMode`/`UploadStreamPreference`（各自的 wire 字段是 `ConnectRequest.PreferWebSocket` / `ConnectResponse.Transport`），网关不支持时静默降级到 `PollMode`/`UploadStreamPreference` 已经谈好的结果，旧版本双方完全无感知——这条路径是纯新增的，不修改、不复用批量/流式轮询的任何现有代码路径。

**这不是轮询的第三个模式，是轮询的替代品**：协商到 `Transport = "websocket"` 的会话，客户端不再发 `/connect` 之外的任何 POST 请求，而是对 `{prefix}/{id}/ws` 发起一次 WebSocket 升级，之后两个方向的数据都在这一条连接上收发，直到会话结束。`PollHandler`/`pollLoop`/`pollLoopStream`/`sendLoopStream` 这些既有代码路径完全不参与——一个会话要么走轮询（batch 或 stream 二选一），要么走 WebSocket，不会混用。

**帧格式复用，但只用得到一半**：WebSocket 连接上的每条消息是一个字节的类型标签（复用 `frame.go` 的 `frameType`）加payload，`frameData` 传数据、`frameHeartbeat` 传心跳——WebSocket 本身自带消息边界，所以不需要 `frame.go` 那套用于 HTTP chunked body 的长度前缀。`frameEnd`/`frameGone` 在这条路径上没有对应物：WebSocket 有自己原生的关闭握手，"这个会话结束了"直接体现为把连接关掉（干净关闭还是异常关闭，客户端一律当传输失败处理并重连——和 `frameGone` 今天的语义一致，`OutcomePeerClosed` vs `OutcomeTransportFailed` 的判断权在更上层，不需要靠帧类型区分）。

**心跳与存活检测复用 `HeartbeatInterval`，但没有 `StreamMaxDuration`**：`ServerConfig.HeartbeatInterval` 照旧决定"空闲多久发一次心跳帧"，两端各自的读空闲看门狗都是 `HeartbeatInterval + 宽限`（服务端侧宽限沿用内部的 `defaultStreamReadGrace`，客户端侧沿用 `Connector.PollGrace`，和流式轮询的看门狗是同一套参数、同一个量级）。但 `StreamMaxDuration`（流式轮询里"一条长驻响应最长开多久，到点强制轮转"那个参数）在这条路径上完全用不上——它存在的理由是"避开中间代理自己的 idle/read 超时"，而 WebSocket 连接不需要靠周期性重开来避开这类超时，只要心跳按时发，一条连接可以一直开着。

**依赖**：`go.mod` 新增 `github.com/coder/websocket`。选它是因为 API 是 context-native 的（`Read(ctx)`/`Write(ctx, ...)`），和这个库本身大量用 context 做超时/取消的风格一致；没有引入额外的传递依赖。

### 六、跨重连会话恢复（`PreferResume` / `EnableResume`）

前五节解决的都是"链路健康时怎么跑得快、跑得稳"。这一节解决另一类问题：**链路被外部按存活时间掐断时，隧道上的流为什么必死、以及怎么让它不死**。

默认模型下，底层传输（poll / send-stream / WebSocket）一断，客户端就丢弃整个 yamux 会话，下一轮 `Connect` 拿到的是一个全新 session id，旧会话里所有 stream（比如一条 SSH 的 TCP 流）随之被切断。生产上 consumer 与 provider 各有一条传输经过 Cloudflare/反向代理，任意一条被中间层的最大连接存活时间（观察到约一小时）掐断，SSH 就断一次。心跳、`StreamMaxDuration` 滚动这些秒级机制保护不了这件事——它们不是原因。

开启方式：服务端 `ServerConfig.EnableResume = true` 并**挂载 `ResumeHandler`**（`POST {prefix}/{id}/resume`），客户端 `Connector.PreferResume = true`。协商仍是"客户端请求 && 服务端支持"的纯附加模式，`ConnectRequest.PreferResume` / `ConnectResponse.Resumable`，`ProtocolVersion` 不变，老客户端 × 新服务端、新客户端 × 老服务端都退化为今天的行为。**只有两种传输能恢复**：WebSocket，或上下行都是流式（`PollMode = "stream"` 且上行也谈成 stream）。batch 一个响应即一条消息，接缝语义太弱，`EnableResume` 与 batch 组合时 `Resumable` 直接为 false。

**它做了什么**：在 yamux 与 pollmux 传输之间插了一层可靠续传层（`reliable.go`），两端共用同一份实现——

- 每方向给数据字节编累积序号（和 TCP 一样按字节计），数据帧改用带 8 字节绝对 offset 的 `frameSeqData`，对端收到后用 `frameAck(offset)` 回确认。ACK 搭载在反向数据/心跳帧上，收满半个 yamux 窗口还会主动叫醒发送方立刻发一次，正常运行不多花 RTT。
- 已发未确认的字节留在重放缓冲里。传输断开后，客户端不再新建会话，而是 `POST /{id}/resume` 交换双方各自"已连续收到多少字节"，服务端把下行重放缓冲回退到客户端声明的位置，客户端把上行缓冲回退到服务端声明的位置，新传输附着后先重放缺口、再继续。接收方按 offset 去重：重放过来的、已经收到过的字节丢掉，出现空洞则判定不可恢复。**接缝处无丢失、无重复、严格保序**，否则 yamux 帧流一错位整条会话就废了。
- yamux **只构建一次**。瞬断期间 `Read` 阻塞、`Write` 立即返回（字节编号后进缓冲），yamux 从不卡在底层写上，所以 `ConnectionWriteTimeout` 不会触发；对端的窗口更新也发不过来，yamux 自己的流控让每条 stream 最多再写约 `MaxStreamWindowSize`（256KB）就停手，重放缓冲因此天然封顶在"256KB × 活跃流数"。
- 服务端的 `*Session` 本来就跨请求存活；现在传输脱离后它进入宽限期（`ServerConfig.ResumeGrace`，默认 30s，上限 `MaxResumeGrace` 5 分钟），期间不被 `SessionTimeout` 淘汰，`CloseSessionIfNoPollInFlight` 也会把它当成"仍有传输附着"而拒绝关闭——所以一个周期性调用它的 fast reaper（HttpBroker 那种）**不需要任何改动**，宽限期一过它自然返回 true。`Session.Resumable()` / `Session.ResumeDeadline()` 供状态页和 reaper 观察。
- 应用侧无感：HttpBroker 的 `ServerSession(session)` / `ClientSession(conn)` / `bridgeStream` 的 `io.Copy` 一行不改，只加配置。

**什么时候会放弃恢复、退化成今天的"新建会话"**（客户端 `TransportFailed` 触发、`ReconnectLoop` 照旧转一圈）：宽限期内没能完成 `/resume`（服务端回 404/410，或客户端按下发的 `resume_grace_ms` 重试用尽）；服务端主动关闭了会话（410 / `frameGone`，比如对端离开、被 DELETE）；任一方向重放缓冲超过 `MaxReplayBytes`（默认 16MB，两端各自独立配置，超过后会话继续在当前传输上工作但不再可恢复）；offset 越界或出现空洞（这是 bug 信号，宁可重建也绝不错误重放）；`UploadStreamPreference` 自动探测失败——上行退回离散 POST 就没法续传，客户端会删掉这个会话、不带 `PreferResume` 重连一次，调用方拿到的是一个普通连接。

**要注意的两件事**：一，**两跳都要开**。consumer↔broker 与 provider↔broker 任一跳不可恢复，那一跳断裂时流照样死。二，恢复窗口是内存放大面：脱离的会话在宽限期内持有 `*Session` 加最多 `MaxReplayBytes` 的重放缓冲。`ResumeGrace` 有硬上限，`ServerConfig.MaxDetachedResumable`（默认 1024）封顶同时处于脱离状态的可恢复会话数、超出时 sweeper 先淘汰脱离最久的。三，**`/resume` 必须挂在与 `/poll`、`DELETE`、`/ws` 完全相同的鉴权中间件后面**。库本身对这几个端点都不调用 `Hooks.Authenticate`（它绑定的是 `ConnectRequest`），信任边界是 128 位随机 session id 加应用自己的中间件。一个带着越界 `recv_offset` 的 `/resume` 会让该会话永久失去可恢复性（409）——这是故意的：只拒绝不标记的话，状态错乱的对端可以换几个 offset 反复试到落进可重放区间，那就是"错误重放"，比断流严重得多；诚实的客户端收到 409 本来也会放弃这个会话。能打到这个端点的人同样能直接 `DELETE` 会话，所以它没有引入 session id 本身没有的能力，但前提是鉴权层没有漏掉它。

`/resume` 的状态码：200 恢复成功（响应体带服务端的 `recv_offset`）；404 会话不存在、410 已关闭、409 不可恢复（未协商、已判定不可恢复、offset 超出可重放范围——客户端一律放弃并重建）；426 协议版本；503 稍后重试（旧传输还没脱离干净，或另一个 resume 正在进行）。

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

WebSocket 传输模式用的是升级请求本身的状态码，和上面这张表是两套独立的错误面：404（会话不存在）、400（会话没有协商 WebSocket 传输，例如客户端连错了 endpoint 或服务端 `EnableWebSocket` 中途被关掉）、409（会话已经有一条 WebSocket 挂着，正常运行下不会触发——见下方"这不是轮询的第三个模式"）。协商阶段的握手（`/connect`）仍然走上面那张表。

### 参数下发

需要两端一致的数字由服务端做权威并在 connect 时下发（`max_send_bytes`、`poll_timeout`、`session_timeout`、`poll_buffer_bytes`，流式模式或 WebSocket 传输下再加 `heartbeat_interval_ms`）。WebSocket 传输协商时 `stream_max_duration_ms` 也会跟着一起下发（内部复用了流式轮询同一套"这次 connect 要不要带上 Heartbeat/StreamMaxDuration"的判断），但 WebSocket 的客户端/服务端实现都不读它——上一节已经说明这条路径不需要强制轮转。客户端只能更保守，不能更激进。这与 HTTP/2 的 SETTINGS、TCP 的 MSS 协商是同一个道理：**不要两边各配一份**。

客户端在 connect 时还会自检 —— 如果 `poll_timeout + PollInterval >= session_timeout`，说明这个客户端健康时也会被服务端当成掉线扫掉，于是直接返回错误而不是带着这个隐患跑起来。

### `ReconnectLoop` 的退避

- `OutcomeTransportFailed`（链路问题）→ 退避并翻倍，上限 `MaxBackoff`。
- `OutcomePeerClosed`（对端走了，链路健康）→ 只短暂停顿，**不推进退避**。
- **每次连接成功都会重置退避。** 因此"连上就立刻断"的抖动链路会稳定在 `InitialBackoff` 反复重试，不会升级。这是为了真实故障恢复后能立刻恢复速度而做的取舍。

注意 `OutcomePeerClosed` 需要调用方判断：yamux 会话结束本身不区分"链路断了"和"对端走了"，要在 `sess.CloseChan()` 触发后再查一次 `conn.TransportFailed()`。另外 `yamux.Session.Close()` 会连底层 conn 一起关，所以如果底层 conn 就是这条隧道，关 yamux 等于拆隧道，对端只会看到传输失败 —— 想要"对端走了但隧道保留"，需要应用层另设信令。
