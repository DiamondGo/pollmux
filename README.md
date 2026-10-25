# pollmux

An HTTP virtual connection and yamux multiplexing library for Go. It supports finite HTTP batches, streaming HTTP in either direction, and WebSocket transport, with optional session resumption after transport failures.

pollmux handles **how bytes move between two machines**, not what those bytes mean or which application roles the endpoints play. Roles, endpoints, subdomains, and client identifiers belong in the opaque `meta` map. Applications supply authentication, routing, stream handling, and lifecycle policy.

## Installation

Requires Go 1.25 or later, as declared in [go.mod](go.mod).

```sh
go get github.com/DiamondGo/pollmux
```

The library uses HashiCorp yamux and coder/websocket. A client connection implements an ordered byte stream through [`Conn`](client.go#L33); pass it to [`ClientSession()`](yamux.go#L60) on the end that opens yamux streams, and pass the server-side [`Session`](session.go#L24) to [`ServerSession()`](yamux.go#L65) on the end that accepts them. These roles are application choices, not necessarily HTTP client/server roles.

## Before deploying

- **Keep yamux keepalive disabled.** Use the session helpers above, or [`YamuxConfig()`](yamux.go#L40). Enabling yamux PING/PONG can close a healthy polling connection.
- **Check proxy behavior, not just negotiated capabilities.** Streaming HTTP requires live forwarding of request/response bodies. An upload probe can reject some broken paths but cannot prove that real traffic will work. Prefer WebSocket where ordinary HTTP bodies are buffered, or use finite batches.
- **Protect every session endpoint with the same authentication middleware**, including resume. The connect authentication hook does not authenticate later requests.
- **Resume is bounded, in-memory recovery**, not persistence or an unlimited outage guarantee. Preserve replica affinity, budget replay memory, and enable resume on both hops of a relay if streams must survive either hop failing.

## Configuration and routing

All handlers for a deployment should share one store, configuration, and hook set. The default path prefix is `/tunnel`.

| Method | Path | Handler |
|---|---|---|
| POST | `/tunnel/connect` | [`ConnectHandler()`](handler.go#L291) |
| POST | `/tunnel/{id}/poll` | [`PollHandler()`](handler.go#L434), for both HTTP directions |
| DELETE | `/tunnel/{id}` | [`DeleteHandler()`](handler.go#L848) |
| GET | `/tunnel/{id}/ws` | [`WebSocketHandler()`](websocket.go#L62), when WebSocket is offered |
| POST | `/tunnel/{id}/resume` | [`ResumeHandler()`](resume_server.go#L208), when resume is offered |

The following is a **routing helper for an existing Go server**, not a complete application. It requires imports for `net/http` and `github.com/DiamondGo/pollmux`. Supply a non-nil authentication/authorization middleware and application hooks; the connect hook should start application session processing asynchronously rather than wait for streams before returning. Mount the returned handler at the server root, and call the returned stop function during shutdown.

```go
func tunnelRoutes(
	hooks pollmux.Hooks,
	authenticate func(http.Handler) http.Handler,
) (http.Handler, func()) {
	store := pollmux.NewSessionStore()
	cfg := pollmux.ServerConfig{
		PollMode:        pollmux.PollModeStream,
		EnableWebSocket: true,
		EnableResume:    true,
	}

	mux := http.NewServeMux()
	mux.Handle("POST /tunnel/connect", pollmux.ConnectHandler(store, cfg, hooks))
	mux.Handle("POST /tunnel/{id}/poll", pollmux.PollHandler(store, cfg, hooks))
	mux.Handle("DELETE /tunnel/{id}", pollmux.DeleteHandler(store, cfg, hooks))
	mux.Handle("GET /tunnel/{id}/ws", pollmux.WebSocketHandler(store, cfg, hooks))
	mux.Handle("POST /tunnel/{id}/resume", pollmux.ResumeHandler(store, cfg, hooks))

	stop := pollmux.StartSweeper(store, cfg, hooks)
	// One wrapper protects connect AND every subsequent session request.
	return authenticate(mux), stop
}
```

The standard router supplies the `id` path value. With another router, configure [`ServerConfig.SessionIDFunc`](handler.go#L60) to extract it. Middleware must preserve streaming, WebSocket upgrade, and response-controller capabilities; do not wrap these routes in middleware that buffers entire responses.

For an **existing client function**, with `ctx`, `baseURL`, and `token` supplied by the application, configure and connect as follows:

```go
connector := &pollmux.Connector{
	BaseURL:         baseURL,
	AuthToken:       token,
	PreferWebSocket: true,
	PreferStream:    true,
	PreferResume:    true,
	// UploadStreamPreference is left empty: probe HTTP upload streaming
	// if WebSocket is not negotiated and the server offers streamed uploads.
}
conn, err := connector.Connect(ctx)
if err != nil {
	return err
}
defer conn.Close()

muxSession, err := pollmux.ClientSession(conn)
if err != nil {
	return err
}
defer muxSession.Close()
// Open and handle application streams here, before this function returns.
```

This client snippet belongs inside a function returning an error, with imports for `github.com/DiamondGo/pollmux` and any application dependencies. Keep the connector for subsequent reconnects. Closing the connection intentionally deletes its server session; it is not how to trigger resume.

### Selecting HTTP directions

The server offers both HTTP streaming directions with [`ServerConfig.PollMode`](handler.go#L70) set to [`PollModeStream`](handler.go#L25). There is no separate server-side upload-stream switch. The directions are nevertheless negotiated **independently**: request field `prefer_stream_mode` selects response field `poll_mode`, while `prefer_stream_upload` selects `upload_stream_mode`. An absent upload mode means batch, so a server predating upload streaming can ignore the new preference without receiving frames it cannot understand.

To select a specific HTTP combination, leave [`Connector.PreferWebSocket`](client.go#L141) false and use this table. The server must offer streaming for any requested stream direction.

| Download: server → client | Upload: client → server | [`PreferStream`](client.go#L112) | [`UploadStreamPreference`](client.go#L126) |
|---|---|---|---|
| Batch | Batch | false | `"batch"` |
| Stream | Batch | true | `"batch"` |
| Batch | Stream | false | `"stream"` |
| Stream | Stream | true | `"stream"` |

An empty upload preference is the default **auto-probe** setting, not forced streaming. It requests upload streaming even when the download preference is false. To guarantee pure batch, explicitly choose `"batch"` for uploads, or use a batch-only server. Forced `"stream"` skips the probe and should only be used on a verified path.

All four HTTP combinations can resume when both endpoints support the batch-resume capability. WebSocket is negotiated separately and, when selected, replaces both HTTP directions.

## yamux correctness and memory

### Keepalive must stay disabled

[`YamuxConfig()`](yamux.go#L40) sets [`EnableKeepAlive`](yamux.go#L44) to false. This is a correctness requirement, not an optional optimization. yamux's keepalive expects a PING/PONG round trip within its connection write timeout, normally 10 seconds. Batched transport can delay that exchange across polling boundaries; the default long-poll timeout is 30 seconds. A healthy transport can therefore be declared dead by yamux.

Liveness belongs to the underlying transport instead:

- Batch polls wait for headers for the negotiated poll timeout plus the client's grace period.
- Streamed downloads use heartbeats and a read-idle watchdog.
- Streamed uploads have a server-side read-idle watchdog.
- WebSocket has application heartbeats and read-idle detection in both directions.
- The server uses in-flight request tracking, session expiry, and, where negotiated, resume grace to manage session lifetime.

Keep yamux keepalive disabled for streaming and WebSocket too. Switching transport must not accidentally reintroduce a second, incompatible liveness policy.

### Stream windows are not free memory

The configured [`MaxStreamWindowSize`](yamux.go#L27) is 256 KiB, yamux's **minimum accepted window**, not just a tunable default. Lowering it below 256 KiB is rejected by yamux. A smaller window would also lower the amount one stream could have in flight per RTT.

The flow-control buffering budget is roughly window size × concurrent streams: **256 KiB × 256 streams ≈ 64 MiB per tunnel**, multiplied by the number of tunnels in a multi-tenant deployment. This is not a hard bound on total process memory: pipes, replay buffers, HTTP bodies, and application state add overhead. [`BufferedPipe`](pipe.go#L23) itself has no backpressure; normal yamux flow control is what bounds ordinary stream traffic. Applications writing arbitrary bytes directly to a connection cannot assume the same protection.

Limit concurrent streams rather than trying to shrink the window below yamux's floor. Use [`BufferedPipe.HiWater()`](pipe.go#L65), [`Session.HiWater()`](session.go#L328), and [`ServerConfig.HighWaterWarn`](handler.go#L65) to observe actual buffering.

## Transport behavior and caveats

### Finite HTTP batches

HTTP uploads and downloads are independent requests to the poll endpoint:

- `X-Send-Only: true`: one finite upload, coalesced from queued writes, followed by an immediate response. Only one upload is in flight at a time.
- `X-Receive-Only: true`: one long-poll response, containing at most the negotiated `poll_buffer_bytes`. An idle poll returns 204, then the client polls again.

Uploads never wait for the downstream long poll. The default client poll interval is zero: waiting belongs inside the server's long poll, not an extra client sleep. A configured interval is applied after idle responses, not after data.

For **resumable** batch polls, the header timeout is `poll_timeout_ms + PollGrace`. A whole-request timeout of `DialTimeout + poll timeout + PollGrace` also bounds body consumption: receiving headers and then stalling halfway through a response is a transport failure, not an indefinitely healthy poll. Batch uploads use the client's send timeout.

**Size `poll_buffer_bytes` for the slowest link you support.** The whole-request timeout also covers the body transfer. Because yamux keepalive is disabled, it is the client's only stall detector for batch polls, so it deliberately does not stretch for slow bodies. Data that arrives near the end of a long poll leaves about `DialTimeout + PollGrace` to transfer a full batch. Keep `poll_buffer_bytes` within what the link carries in `PollGrace`, for example 256 KiB needs about 26 KB/s with the default 10 seconds. A batch that misses the deadline only costs a resume and a replay. But if a full batch cannot transfer within the whole deadline at all, every replay times out the same way. After three consecutive legs end with a response body timing out and no downstream bytes delivered, the client abandons the session and signals transport failure instead of resuming forever. See [resumable batch wire behavior](#resumable-batch-wire-behavior) for offsets and replay.

### Streamed HTTP downloads

Downloads stream only when the client requests them and the server offers them. Otherwise they silently fall back to batch. Unknown or absent negotiated download modes are treated as batch, supporting older peers without changing [`ProtocolVersion`](protocol.go#L22).

The server flushes response headers immediately, then sends length-prefixed data and control frames. The important controls are:

- [`HeartbeatInterval`](handler.go#L78), default 10 seconds: send a heartbeat when idle.
- [`StreamMaxDuration`](handler.go#L92), default 45 seconds: end a response cleanly and immediately open the next one. Rotation is checked between complete frames, not in the middle of a frame; allow timing margin.

Keep the stream duration **comfortably below the tightest applicable proxy timeout** on the deployed path. Local validation requires the configured stream duration to be at least twice the heartbeat interval, but cannot discover a CDN's or reverse proxy's timeouts. Configure those explicitly and verify actual forwarding. The server sends `X-Accel-Buffering: no` as a best-effort hint, not a guarantee that every intermediary disables buffering.

Since headers arrive immediately, the client's response-header timeout only covers setup. A separate watchdog resets on every complete frame, including heartbeats; no frame within heartbeat interval + [`Connector.PollGrace`](client.go#L89), default 10 seconds, fails the transport.

HTTP stream framing distinguishes:

| Frame | Meaning |
|---|---|
| [`frameData`](frame.go#L15) | Ordinary, non-resumable payload |
| [`frameHeartbeat`](frame.go#L16) | Idle liveness signal |
| [`frameEnd`](frame.go#L20) | Benign request/response rotation; reopen immediately on the same session |
| [`frameGone`](frame.go#L26) | Server session is closed; do not keep polling it or try to revive it |
| [`frameSeqData`](frame.go#L37) / [`frameAck`](frame.go#L36) | Numbered payload / cumulative acknowledgment on resumable sessions |

A clean body end at a frame boundary is also a benign rotation. A partial frame or read error is a transport failure. Session-gone is equivalent to HTTP 410, **not** an idle heartbeat or normal rotation. Confusing these conditions would leave clients polling a dead session forever.

### Streamed HTTP uploads and the auto probe

Streamed uploads use a long-lived POST with `X-Send-Stream: true`. The server consumes complete frames and delivers them as they arrive, rather than waiting for the entire request to finish. The directions share the negotiated heartbeat and stream-duration settings.

**The writer controls rotation.** The server rotates downstream responses; the client rotates upstream requests, checking duration only after finishing a complete frame. A server ending an upload arbitrarily in the middle of a frame could abandon bytes already removed from the client's queue and desynchronize yamux. Upload-side session closure is reported with HTTP 410, not a downstream-only gone frame. On resumable connections, failure of either direction cancels its companion before recovery; upload-side detection is not necessarily instantaneous and may wait until another write or heartbeat. The server refreshes a read deadline after each received frame.

[`Connector.UploadStreamPreference`](client.go#L126) has three settings:

| Setting | Behavior |
|---|---|
| `"batch"` | Do not request upload streaming; no probe |
| `"stream"` | Use it if negotiated; no probe; requires a verified streaming path |
| `""` (default) | Request it, then probe if negotiated; fall back when the probe fails |

The automatic probe is bounded by [`UploadProbeTimeout`](client.go#L129), default 15 seconds. It sends more than one yamux window of filler through a real framed upload, ends it normally, and waits for the actual 200 response. `X-Send-Stream-Probe` tells the server to discard filler rather than deliver it to the application. Explicit batch/stream preferences avoid this probe latency; auto mode can add up to the probe timeout to a connection attempt.

For a session negotiated as resumable, **a failed probe causes a best-effort DELETE of the original session and a fresh connect with batch uploads**. Its upload mode is not changed in place. Resume remains requested only if the original response echoed `batch_resume`; an older server without that capability falls back without resume. The download preference is preserved. For a non-resumable connection, a failed probe simply selects ordinary batch uploads for that connection. Streaming is not periodically re-probed on an established connection.

**A passing probe is not proof of compatibility.** Some proxies, observed with Cloudflare standard/non-Enterprise HTTP proxy paths, buffer long-lived request bodies before forwarding them. With yamux, filling a stream window while the peer cannot receive bytes or return window updates can produce a permanent hang, not merely lower throughput. A short, continuous connect-time probe may pass while a long-lived, intermittently written production body still stalls. This has also been observed in real deployments. Verify the entire path under realistic bidirectional traffic, or choose WebSocket/finite batches instead.

### WebSocket

Enable [`ServerConfig.EnableWebSocket`](handler.go#L101) and request [`Connector.PreferWebSocket`](client.go#L141), wire field `prefer_websocket`. If the server does not offer it, negotiation silently falls back to the separately negotiated HTTP directions. When selected, `transport` is `"websocket"`; the upload probe is skipped.

WebSocket is **an alternative transport, not a third poll mode**. After connect, one upgrade at `/tunnel/{id}/ws` carries both directions; the poll/send-stream loops are not used. DELETE still handles intentional teardown, and resumable connections use the resume endpoint before attaching a replacement WebSocket.

Ordinary WebSocket binary messages contain a one-byte type tag plus payload; native message boundaries replace HTTP's length prefix. Resumable messages add byte offsets and acknowledgments. WebSocket uses its native close mechanism rather than HTTP stream end/gone frames. Without resume, an unexpected close, including a clean peer close, signals transport failure; with resume, transport loss triggers recovery of the same session if possible.

Heartbeats use the negotiated heartbeat interval. Each side has a read-idle watchdog: heartbeat interval + client poll grace, or heartbeat interval + the server's internal 10-second margin. There is **no forced stream-duration rotation** for WebSocket. The connect response currently includes `stream_max_duration_ms`, but the WebSocket path does not use it.

Proxies commonly forward WebSocket messages live even when they buffer ordinary HTTP bodies; this is why WebSocket is useful on those paths. It still requires upgrade support and is still subject to intermediary lifetime limits and network failures. Heartbeats and HTTP stream rotation do not prevent an intermediary from killing a connection at its maximum age.

## Resuming the same session after transport loss

### Opt-in and backward-compatible negotiation

Resume requires all of the following:

1. [`Connector.PreferResume`](client.go#L150) is true.
2. [`ServerConfig.EnableResume`](handler.go#L111) is true.
3. [`ResumeHandler()`](resume_server.go#L208) is mounted at `POST /{prefix}/{id}/resume` behind the **same authentication middleware as poll, DELETE, and WebSocket**.
4. The connect response actually negotiates `resumable: true`.

The API switches remain the same: the client requests `prefer_resume` and the server responds with `resumable`. New clients automatically send [`ConnectRequest.BatchResume`](protocol.go#L192), wire field `batch_resume`, when requesting resume. This capability **does not enable resume on its own**. New servers echo [`ConnectResponse.BatchResume`](protocol.go#L321) only for resumable sessions when the client advertised it.

| Negotiated transport | With batch-resume capability | Without batch-resume capability |
|---|---|---|
| Batch upload + batch download | Resumable | Not resumable |
| Batch upload + stream download | Resumable | Not resumable |
| Stream upload + batch download | Resumable | Not resumable |
| Stream upload + stream download | Resumable | Original stream resume supported |
| WebSocket | Resumable | Original WebSocket resume supported |

Every resumable entry still requires both resume switches. This additive capability protects older resumable clients from receiving a batch wire format they do not understand. Older servers may support only two-way-stream/WebSocket resume, or ignore resume entirely. Absent capability must never be interpreted as permission for resumable batches; absent `resumable` means ordinary non-resumable operation. The protocol version remains 1.

### Reliability and lifecycle

Without resume, transport loss ends the virtual connection, and a reconnect creates a new session ID and a new yamux session. Existing application streams cannot survive that replacement.

With resume, a reliable byte layer sits below yamux. Each direction has absolute byte offsets, cumulative acknowledgments, and a replay buffer. Bytes remain retained until the peer acknowledges receiving them; a successful local HTTP write alone is not an acknowledgment. Stream/WebSocket transports carry numbered data and ACK frames, with ACKs sent alongside reverse traffic/heartbeats and an eager ACK wakeup at half a yamux window. Batch requests and responses carry the cumulative ACK in headers every time, so a direction whose ACKs travel that way skips the eager wakeup and never issues ACK-only requests.

On transport failure, the client stops both directions, then sends `POST /{prefix}/{id}/resume` with its protocol version and `recv_offset`: the number of downstream bytes received contiguously. The server first cancels and waits out old attachments, then answers with its contiguous upstream `recv_offset`. Each side replays from the other side's offset before sending new bytes. Duplicate or overlapping replay is discarded; gaps and impossible acknowledgments are rejected rather than corrupting yamux's byte stream. Only one transport owns each direction at a time, including during a resume handshake.

**The session ID, server session, and yamux session stay the same.** A successful recovery is internal to the connection and does not close its transport-failed signal. Reads can block through the outage. Transport-level writes queue locally without waiting for the network, avoiding yamux's connection write timeout during recovery, but **application/yamux stream writes can still block on flow control** once their window is exhausted. Application contexts, stream deadlines, and business timeouts still apply; resume does not suspend or extend them. This is ordered byte-stream recovery, not a guarantee that an application has processed or committed a business operation.

During a valid detached grace period, the server does not evict the session using ordinary idle timeout. [`CloseSessionIfNoPollInFlight()`](handler.go#L900) also treats it as busy, so a reaper using that helper need not learn a separate resume policy. Applications can inspect [`Session.Resumable()`](session.go#L127) and [`Session.ResumeDeadline()`](session.go#L137) for monitoring. Explicit session close, DELETE, and shutdown still terminate it; resume never resurrects a deliberately closed session. Once the grace has run out, any late attach (poll, upload, WebSocket, or resume) gets 410 even if the sweeper has not evicted the session yet, so the recovery window is exactly `resume_grace_ms`.

### Resumable batch wire behavior

Resumable batches are **raw, bounded, finite HTTP bodies**, not framed streams hidden inside batch requests. Upload payloads remain bounded by `max_send_bytes`; download payloads remain bounded by `poll_buffer_bytes`. A buffering proxy can forward these finite bodies without supporting live request-body streaming.

| Message | Required offset headers |
|---|---|
| Send-only batch request | `X-Send-Offset`: absolute starting upload byte offset; `X-Recv-Offset`: cumulative downstream acknowledgment |
| Receive-only batch request, with an empty body (`Content-Length: 0` or an empty chunked body) | `X-Recv-Offset`: cumulative downstream acknowledgment |
| 200 batch poll response, always with a non-empty body | `X-Send-Offset`: absolute starting download byte offset; `X-Recv-Offset`: cumulative upstream acknowledgment |
| Successful batch upload response or 204 idle poll response | `X-Recv-Offset`: cumulative upstream acknowledgment |

Offsets are unsigned decimal byte counts, present even when zero. Each required header must have exactly one value; missing, malformed, duplicated, or ambiguous values are rejected. Every batch request and successful batch response carries the cumulative receive acknowledgment, **including 204**. Mixed HTTP sessions use these headers on the batch direction and numbered frames/ACKs on the streaming direction.

The server reads an entire upload successfully before accepting its data into the reliable byte stream. An incomplete upload is not partially delivered. The client likewise consumes and validates a complete bounded poll body before delivery; an explicit response length detects truncation. Upload success responses must have an empty body and be consumed successfully before their acknowledgment is accepted.

If an upload was accepted but its 200 response was lost, or a download was truncated after leaving the server queue, the sender still retains the unacknowledged bytes. The client performs the resume handshake **before retransmitting**, and the absolute offsets prevent duplicate delivery while replaying whatever is missing. A raw batch retry without the handshake is not the client's recovery algorithm.

Batch downloads are strictly serial, so the server also starts every poll response at that poll's cumulative acknowledgment. A response lost without any handshake, such as a stale poll that a proxy delivered after the client had already resumed, is therefore resent by the next poll instead of leaving a gap. An idle poll is answered with 204; the client rejects a 200 with an empty body as malformed.

Pure-batch resumable sessions must have [`Connector.PollInterval`](client.go#L86) **strictly below the negotiated resume grace**; connect rejects an interval reaching that grace. Between finite requests there may be no attachment, so a healthy client must poll again before its detached session expires. The default zero interval is recommended.

### Recovery limits and failure cases

| Setting | Default / limit |
|---|---|
| [`ServerConfig.ResumeGrace`](handler.go#L119) | 30 seconds detached; hard maximum 5 minutes; configuration above the maximum panics |
| [`MaxReplayBytes`](handler.go#L126) on the server and [`Connector.MaxReplayBytes`](client.go#L155) on the client | 16 MiB per outgoing direction; independently configured, not negotiated |
| [`ServerConfig.MaxDetachedResumable`](handler.go#L136) | 1,024 detached resumable sessions; the sweeper evicts longest-detached first; negative disables the cap. A pure-batch session counts only after it has been detached longer than the client's declared `poll_interval_ms` (clamped to the grace) plus 10 seconds, because a healthy one has nothing attached between polls |

Start the configured [`StartSweeper()`](handler.go#L914) to enforce grace expiry and the detached-session cap. A detached session retains application state and replay memory, so grace and population limits are important memory controls, not just retry tuning. The replay limit covers retained, transmitted-but-unacknowledged bytes, not all queued data or total session memory. Normal yamux windowing helps bound pending traffic per active stream, but concurrency still matters.

Recovery ends and the client signals transport failure for a fresh-session reconnect when, for example:

- The server session is missing, expired, deleted, or closed, or the resume endpoint definitively refuses it.
- Transient resume failures outlast the advertised `resume_grace_ms`. Detection and retries must fit within the server's actual retention window; a client-side retry budget does not extend a server deadline.
- A replay buffer exceeds its configured limit. The current transport may continue working, but the session loses resumability; its next transport loss cannot be repaired.
- A byte gap, impossible ACK, or resume offset outside the retained range makes replay unsafe.
- Repeated successful handshakes produce unusable replacement transports: five consecutive failures lasting less than two seconds **without peer-visible byte progress** trip a stability guard. Progress or a sufficiently stable transport resets it.

Transient resume retries use jittered exponential waits, starting at 250 milliseconds and capped at two seconds, within the advertised grace budget. A definitive 4xx resume refusal is not retried for that session.

Sessions and replay buffers are **process-local and not persistent**. A server restart loses them. Load-balanced deployments require affinity so connect, poll/upload, resume, DELETE, and WebSocket requests reach the process holding that session. Resume does not migrate sessions across replicas. In a consumer ↔ broker ↔ provider relay, **both hops need resume** for streams to survive a transport failure on either hop.

### Authentication boundary

[`Hooks.Authenticate`](handler.go#L252) authenticates **connect requests only**. It is not called by poll, DELETE, WebSocket, or resume handlers. Session IDs are random 128-bit values, but the application must still apply the same credential and session-authorization policy to **all** those routes. The client's bearer token is sent on subsequent requests as well.

A caller reaching resume with an out-of-range `recv_offset` permanently invalidates that session's resumability and gets 409. This is intentional: letting an inconsistent peer guess offsets until one happens to fit could silently replay the wrong bytes. An authorized caller capable of this could also DELETE the session outright; that security argument only holds if no endpoint bypasses the common middleware. Never expose resume unprotected just because connect has an authentication hook.

Client-declared metadata is not authenticated identity. Metadata returned by the authentication hook overrides matching client-supplied keys in the connect response. Use that hook for connect-specific policy in addition to shared route middleware.

## Status codes

### Connect and HTTP polling/upload

| Status | Meaning | Client behavior |
|---|---|---|
| 200 | Connected, data available, or upload accepted | Continue; honor negotiated framing/offsets |
| 204 | Idle long-poll timeout: **normal heartbeat**, not closure | Poll again, subject to any configured idle interval; resumable batches still process ACKs |
| 400 | Malformed request, invalid batch headers/offsets, or wrong negotiated direction | Error, not a successful empty poll; inconsistent byte state can make the session unrecoverable |
| 401 | Authentication rejected | Connect returns [`ErrUnauthorized`](client.go#L29); reconnect may retry with backoff |
| 404 | Unknown session | That session cannot be recovered; reconnect with a fresh one |
| **410** | **Session closed by the server** | Stop using that session promptly; do not treat it as 204 or normal rotation |
| **413** | Upload body exceeds advertised `max_send_bytes`: **protocol violation** | Server closes the session; diagnose and reconnect, not shrink-and-retry |
| 426 | Unsupported protocol version | Connect returns [`ErrProtocolVersion`](client.go#L24); the reconnect loop stops rather than retry forever |
| 503 | A prior resumable attachment is still detaching, or a resume prevents attachment | Transient transport/attachment failure; recovery may retry within grace |
| 3xx | Often an anti-scanning/authentication redirect | Not followed; inspect credentials and routing |

**410 and 204 must remain distinct.** Otherwise closure looks like a healthy idle session and the client repeatedly polls a dead ID. Streaming HTTP uses the same status meanings before headers are committed; subsequent control frames express rotation or session closure inside the body.

**413 is not normal size negotiation.** A conforming client already clamps its batch upload size to the minimum of its local chunk cap and the server's advertised maximum. Exceeding that maximum indicates a bug or protocol violation, not a reason to introduce a stateful halve-and-retry algorithm.

Clients do not follow redirects. This preserves the actual auth/routing error instead of turning it into a confusing response-parsing failure at another URL. For resumable connections, recoverable transport problems are handled internally first; transport failure reaches the caller only when recovery is no longer possible.

### Resume endpoint

| Status | Meaning |
|---|---|
| 200 | Resumed; JSON includes `resumed: true` and the server's upstream `recv_offset` |
| 400 | Malformed resume request |
| 404 | Unknown session |
| 409 | Not negotiated as resumable, no longer resumable, or offset outside the replayable range; abandon this session |
| 410 | Session is closed |
| 426 | Protocol version mismatch |
| 503 | Previous transport has not detached, or another resume is in progress; retry shortly |

Authentication middleware may also reject a resume request. Resume 4xx responses are definitive for that session; network errors and server-side transient failures may be retried within grace.

### WebSocket upgrade

The upgrade has its own error surface: 404 for an unknown session, 400 for a session not negotiated for WebSocket, 410 for a closed session, and 409 for an already attached non-resumable WebSocket. Resumable sessions can replace stale attachments, but may return 503 if the previous attachment has not let go. Connect negotiation still uses the ordinary connect statuses above.

## Authoritative parameter negotiation

The server sends [`Limits`](protocol.go#L227) at connect time: `max_send_bytes`, `poll_timeout_ms`, `session_timeout_ms`, and `poll_buffer_bytes`; streaming in either direction or WebSocket additionally supplies `heartbeat_interval_ms` and `stream_max_duration_ms`. Resumable sessions also receive `resume_grace_ms`. WebSocket ignores stream duration even though it is included.

**Configure shared transport limits on the server, not independently at both ends.** Clients validate the response and can only choose more conservative behavior, such as a smaller upload chunk. The server's negotiated modes and limits are authoritative; requests do not override a session's negotiated direction later.

Useful defaults are:

| Parameter | Default |
|---|---|
| Server poll timeout / session idle timeout / sweep interval | 30 s / 60 s / 5 s |
| Poll response payload cap / server upload payload cap | 256 KiB / 1 MiB |
| Client upload chunk cap | 512 KiB, clamped to server maximum |
| Client extra idle poll interval / poll grace | 0 / 10 s |
| Client send timeout / dial timeout | 15 s / 10 s |
| Send/receive coalescing window | 2 ms |
| Stream heartbeat / nominal rotation duration | 10 s / 45 s |

Connect rejects `poll timeout + PollInterval >= session timeout`, because that would let a healthy client be evicted. A resumable pure-batch connection additionally requires `PollInterval < resume grace`. Resumable responses require a positive grace; streaming responses require positive heartbeat/duration settings; WebSocket requires a positive heartbeat. Server validation rejects explicitly inconsistent timing settings, including a session timeout below twice the poll timeout. These checks do not validate external proxy deadlines or application deadlines.

## Throughput: batches are RTT-limited in both directions

| Direction | Mechanism | Main transport ceiling |
|---|---|---|
| Batch upload | Coalesced finite POSTs, one in flight | At most one effective upload chunk per request round trip |
| Stream upload | Open framed request body | Avoids the per-batch RTT ceiling, assuming live forwarding |
| Batch download | One finite long-poll response at a time | At most one `poll_buffer_bytes` payload per round trip |
| Stream download | Open framed response body | Avoids the per-batch RTT ceiling, assuming live forwarding |

Batch uploads are **not RTT-independent**: their effective chunk is the smaller of client chunk cap and server upload cap. Mixed-direction HTTP removes the batch ceiling only from the streamed direction. With both directions streamed, the transport mechanisms are similar, but bandwidth, flow control, per-stream windows, proxies, and application behavior still limit actual throughput.

For example, a 256 KiB download batch over 150 ms RTT has an idealized ceiling of about **1.7 MB/s** before other overhead. Historical development-machine measurements for an 8 MiB payload with an artificial 150 ms poll-endpoint delay were:

| Direction | Batch | Stream |
|---|---|---|
| Download | ~1.0 | ~28.7 |
| Upload | ~1.9 | ~30 |

These are the benchmark's reported `MB/s` values (its calculation uses MiB/s). They are **loopback measurements with injected per-request delay, not real WAN results or production guarantees**. The benchmarks transfer raw connection bytes rather than measure a complete application over yamux; in particular, a single yamux stream still has its own window/RTT constraint. The synthetic delay models request overhead, not every network effect. Resume adds retention, acknowledgment, and replay work; these historical figures are not a resumable-transport performance claim.

Rerun the benchmarks in your own environment:

```sh
go test -run '^$' -bench Benchmark -benchtime=1x
```

See [bench_test.go](bench_test.go) for the workload and injected-delay setup.

## Reconnect policy

[`ReconnectLoop`](reconnect.go#L54) obtains a connection, runs the application's serve callback, closes the connection afterwards, and chooses the next action from the reported outcome:

- [`OutcomeTransportFailed`](reconnect.go#L17): wait with exponential backoff, doubling up to the maximum. Connect errors also advance backoff, except a protocol-version error stops the loop.
- [`OutcomePeerClosed`](reconnect.go#L21): wait only the short peer-closed pause; **do not advance or reset accumulated backoff**.
- [`OutcomeSuperseded`](reconnect.go#L25): a newer connect replaced the session; stop reconnecting.
- [`OutcomeShutdown`](reconnect.go#L27): stop on application shutdown.

Defaults are 1 second initial backoff, 3 minutes maximum backoff, and a 500 ms peer-closed pause. **A successful connect alone does not reset backoff.** On a transport-failed outcome, backoff resets only after the connection has served for at least [`MinStableDuration`](reconnect.go#L72), default **5 seconds**. Connections that repeatedly succeed and then fail immediately therefore continue escalating rather than hammering the server at the initial delay. A negative stable-duration setting disables this reset.

The application must distinguish peer departure from transport failure. After the yamux session closes, recheck [`Conn.TransportFailed()`](client.go#L45), and handle supersession separately; yamux close alone does not identify the cause. Closing a yamux session also closes its underlying connection. If the application wants a peer to leave while retaining the tunnel, it needs separate application-level signaling.

With resume enabled, successful internal recovery keeps the same connection and yamux session alive; the outer reconnect loop is only needed after recovery fails or the application otherwise ends that session.

## Tests

Run the test suite with:

```sh
go test ./...
```

[batch_resume_test.go](batch_resume_test.go) covers deterministic lost upload responses and truncated download bodies, raw offset deduplication/replay, malformed or ambiguous offsets, incomplete and oversized uploads, stalled bodies, lifecycle failures, capability/backward compatibility, concurrent streams, and repeated transport kills for pure-batch and both mixed HTTP combinations. Stream and WebSocket recovery tests are in [resume_test.go](resume_test.go); reconnect timing behavior is covered in [reconnect_test.go](reconnect_test.go).
