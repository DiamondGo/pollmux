# pollmux

HTTP 长轮询虚拟连接 + yamux 多路复用的共享传输库。

把"字节如何在两台机器之间流动"这一层单独抽出来：客户端、服务端 handler、以及 yamux 配置与重连循环这些容易各写一遍的胶水。

pollmux 只管字节怎么在两台机器之间流动，不管这些字节是什么、也不管两端是什么角色。应用语义（role、endpoint、subdomain、client_id 之类）一律走不透明的 `meta`。

---

## 两条必须先知道的约束

### 一、`EnableKeepAlive` 必须为 false

这不是可选优化，是**正确性前提**。请始终用 `pollmux.YamuxConfig()` 构造 yamux 配置，或至少保证这一项为 false。

yamux 的 keepalive 会周期性发 PING 并要求对端在 `ConnectionWriteTimeout`（默认 10s）内回 PONG。但在长轮询传输上，一次 PING-PONG 往返需要：

1. PING 写进本地发送缓冲 → 等下一次 POST 把它送出去；
2. 服务端 yamux 处理后把 PONG 写进它的下行管道；
3. PONG 要等客户端**下一次 poll** 才被取回 —— 而此刻很可能有一个 poll 正挂在服务端等待，最长可挂 `poll_timeout`（默认 30s）。

30s > 10s，于是 yamux 在链路完全健康的情况下断定"连接已死"并关闭会话。**存活性完全由 poll 循环承担**：poll 的 `ResponseHeaderTimeout` 被设为服务端下发的 `poll_timeout` 加一段宽限，超时即判定传输失败；服务端侧则靠 `session_timeout` 与 `pollInFlight` 感知客户端消失。

这个约束很容易在每个建 yamux 会话的地方靠一句注释重复传递。收进 `YamuxConfig()` 就是为了它只存在一处。

### 二、上下行吞吐是不对称的

两个方向走的是**不同机制**，性能特征因此不同：

| 方向 | 机制 | 受什么限制 |
|---|---|---|
| 上行（客户端 → 服务端） | 数据到达即发 POST，短窗口内合并 | 基本不受 RTT 限制；受 `max_send_bytes` 分片上限约束 |
| 下行（服务端 → 客户端） | 长轮询响应，一次最多带回 `poll_buffer_bytes` | **每 RTT 最多一个缓冲区** |

下行的含义要算一下：`poll_buffer_bytes` 默认 256KB，在 150ms RTT 下单条隧道下行上限约 **1.7 MB/s**。这个上限来自"离散 POST + 批量响应"这个模型本身，不是实现缺陷 —— 彻底解法是上下行流式化，`ServerConfig.PollMode` 为此预留，目前只接受 `batch`。

同一个数字也出现在 yamux 的流窗口上：yamux 强制 `MaxStreamWindowSize ≥ 256KB`（`mux.go:83`，地板值不是默认值），所以两处的 256KB 是对齐的。调小任何一个都会成为新瓶颈。

代价是内存：256KB × 256 并发流 = **64MB/隧道**最坏值，多租户下要乘以隧道数。这个界由 yamux 的流控提供（`BufferedPipe` 自身没有背压），要压低就调并发流上限，不是调窗口。`BufferedPipe.HiWater()` 可以读到实际水位，用它判断离最坏值有多远。

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

### 参数下发

需要两端一致的数字由服务端做权威并在 connect 时下发（`max_send_bytes`、`poll_timeout`、`session_timeout`、`poll_buffer_bytes`），客户端只能更保守，不能更激进。这与 HTTP/2 的 SETTINGS、TCP 的 MSS 协商是同一个道理：**不要两边各配一份**。

客户端在 connect 时还会自检 —— 如果 `poll_timeout + PollInterval >= session_timeout`，说明这个客户端健康时也会被服务端当成掉线扫掉，于是直接返回错误而不是带着这个隐患跑起来。

### `ReconnectLoop` 的退避

- `OutcomeTransportFailed`（链路问题）→ 退避并翻倍，上限 `MaxBackoff`。
- `OutcomePeerClosed`（对端走了，链路健康）→ 只短暂停顿，**不推进退避**。
- **每次连接成功都会重置退避。** 因此"连上就立刻断"的抖动链路会稳定在 `InitialBackoff` 反复重试，不会升级。这是为了真实故障恢复后能立刻恢复速度而做的取舍。

注意 `OutcomePeerClosed` 需要调用方判断：yamux 会话结束本身不区分"链路断了"和"对端走了"，要在 `sess.CloseChan()` 触发后再查一次 `conn.TransportFailed()`。另外 `yamux.Session.Close()` 会连底层 conn 一起关，所以如果底层 conn 就是这条隧道，关 yamux 等于拆隧道，对端只会看到传输失败 —— 想要"对端走了但隧道保留"，需要应用层另设信令。
