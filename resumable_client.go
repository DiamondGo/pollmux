package pollmux

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// resumableConn is Conn's implementation for a session that negotiated
// ConnectResponse.Resumable. Where httpConn and wsConn give up on the whole
// session the moment their transport fails, this one keeps the session —
// and the yamux session the caller built on top of it — alive across the
// failure: a supervisor goroutine tears down the dead legs, performs the
// resume handshake (see ResumeHandler), and brings up fresh legs on the same
// session id. yamux above sees a Read that took a while and a Write that
// returned at once, nothing else.
//
// The pieces:
//
//   - rl, the reliable byte layer (reliable.go), numbers every byte yamux
//     writes, retains it until the server acknowledges it, and drops
//     anything the server replays that already arrived. It is the one part
//     that persists across transports; everything below it is disposable.
//   - Legs are the transport goroutines: a stream poll pulling frames down
//     and a send-stream pushing frames up, or a WebSocket read loop and
//     write loop. Each set lives exactly as long as one transport.
//   - The supervisor (supervise) is the only goroutine that ever decides a
//     transport is over, so there is never more than one set of legs, and
//     the reliable layer's one-sender-per-direction rule holds.
//
// TransportFailed is only closed when resuming is impossible: the server
// refused (grace expired, offsets it cannot honour, session gone), the
// replay buffer overflowed, or the reconnect budget ran out. At that point
// the caller's ReconnectLoop does what it does today — a brand new session.
type resumableConn struct {
	sessionGuard
	sessionID string
	limits    Limits
	meta      map[string]string
	logger    *slog.Logger

	rl *reliable // out: yamux → server (writePipe); in: server → yamux (readPipe)

	// transport is TransportWebSocket or PollModeStream — the only two the
	// server will negotiate Resumable for.
	transport string
	pollURL   string
	resumeURL string
	deleteURL string
	wsURL     string
	authToken string

	// pollClient and sendStreamClient serve the stream-mode legs, with the
	// same timeouts httpConn gives them; httpClient serves the short,
	// bounded requests — resume, DELETE, and the WebSocket dial.
	pollClient       *http.Client
	sendStreamClient *http.Client
	httpClient       *http.Client

	localHealth       func() bool
	heartbeat         time.Duration
	idleTimeout       time.Duration
	streamMaxDuration time.Duration
	coalesceWindow    time.Duration
	sendTimeout       time.Duration
	dialTimeout       time.Duration
	maxFrame          int
	resumeGrace       time.Duration

	// ws is the current WebSocket connection when transport is
	// TransportWebSocket. Only the supervisor replaces it, between one set
	// of legs ending and the next starting; Close reads it after waiting
	// the supervisor out. Atomic anyway, so that safety does not hinge on
	// a reader knowing that ordering.
	ws atomic.Pointer[websocket.Conn]

	transportFailed chan struct{}
	failOnce        sync.Once

	closed atomic.Bool
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (c *resumableConn) SessionID() string                { return c.sessionID }
func (c *resumableConn) Limits() Limits                   { return c.limits }
func (c *resumableConn) Meta() map[string]string          { return maps.Clone(c.meta) }
func (c *resumableConn) TransportFailed() <-chan struct{} { return c.transportFailed }

// Read returns data the server sent. It blocks through a transport failure
// and resume — the read pipe is only ever closed by Close or by giving up.
func (c *resumableConn) Read(p []byte) (int, error) { return c.rl.in.Read(p) }

// Write queues data for delivery and returns at once, whether or not a
// transport is currently attached. That is what keeps yamux from ever
// blocking in a write during an outage (its ConnectionWriteTimeout would
// otherwise end the session for us); the bytes wait in the reliable layer,
// bounded by yamux's own flow control.
func (c *resumableConn) Write(p []byte) (int, error) {
	if c.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	return c.rl.out.Write(p)
}

// Close stops the supervisor and its legs and tells the server to drop the
// session. This is the only path that sends a DELETE: a resume never does,
// since that would kill the very session it is trying to keep.
func (c *resumableConn) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}

	c.cancel()
	// Closing out unblocks a sender parked in nextOut with io.EOF; the
	// interrupt covers the case where out already has data queued and the
	// sender would otherwise go on to try writing it to a dead leg.
	c.rl.out.Close()
	c.rl.interruptOut()
	c.wg.Wait()
	c.rl.in.Close()
	if ws := c.ws.Load(); ws != nil {
		ws.CloseNow()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.deleteURL, nil)
	if err == nil {
		c.setAuth(req)
		if resp, err := c.httpClient.Do(req); err == nil {
			io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
			resp.Body.Close()
		}
	}

	c.httpClient.CloseIdleConnections()
	if c.pollClient != nil {
		c.pollClient.CloseIdleConnections()
	}
	if c.sendStreamClient != nil {
		c.sendStreamClient.CloseIdleConnections()
	}
	return nil
}

func (c *resumableConn) setAuth(req *http.Request) {
	if c.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.authToken)
	}
}

// fail gives up on the session: signals transport failure once and unblocks
// anyone reading.
func (c *resumableConn) fail() {
	c.failOnce.Do(func() {
		close(c.transportFailed)
		c.rl.in.Close()
	})
}

func (c *resumableConn) onSessionClosed() {
	if c.supersededByNewerConnect(c.logger) {
		c.rl.in.Close()
		return
	}
	c.fail()
}

const (
	// resumableLegsMinStable is long enough to distinguish a transport that
	// never became usable after resume from an ordinary established leg that
	// later failed. A stable leg resets the consecutive-fast-failure guard.
	resumableLegsMinStable = 2 * time.Second
	// maxConsecutiveFastResumes bounds the otherwise unthrottled case where
	// every resume handshake succeeds but its replacement legs fail at once.
	maxConsecutiveFastResumes = 5
)

type resumeStabilityGuard struct {
	consecutiveFast int
}

// record reports whether the session should be abandoned after a successful
// resume. A leg that moved bytes is useful even if it was short-lived (for
// example, tests and real flaky links can fail repeatedly during a bulk
// transfer), so either duration or progress resets the guard. The inputs make
// its boundary and reset behaviour testable without sleeping.
func (g *resumeStabilityGuard) record(legsDuration time.Duration, progressed bool) bool {
	if legsDuration >= resumableLegsMinStable || progressed {
		g.consecutiveFast = 0
		return false
	}
	g.consecutiveFast++
	return g.consecutiveFast >= maxConsecutiveFastResumes
}

// supervise runs one set of legs after another until the connection is
// closed or the session cannot be kept. Between sets it decides, from why
// the previous set ended, whether resuming is even worth attempting: a
// server that closed the session (410, frameGone) will not have it back,
// and a reliable layer that caught an inconsistency (a gap, an impossible
// ack, an overflowed replay buffer) cannot vouch for the byte stream any
// more. Everything else is a transport problem, and transport problems are
// what resume is for.
func (c *resumableConn) supervise() {
	defer c.wg.Done()
	var stability resumeStabilityGuard
	for {
		legsStart := time.Now()
		progressBefore := c.rl.progress()
		err := c.runLegs()
		legsDuration := time.Since(legsStart)
		if c.ctx.Err() != nil || c.closed.Load() {
			return
		}
		if isSessionClosedErr(err) {
			c.onSessionClosed()
			return
		}
		if errors.Is(err, errReplayGap) || errors.Is(err, errBadAck) || c.rl.isBroken() {
			c.logger.Warn("pollmux: resumable session is no longer consistent, giving up on it", "error", err)
			c.fail()
			return
		}

		c.logger.Warn("pollmux: transport failed, resuming the session", "error", err)
		if err := c.resume(); err != nil {
			if c.ctx.Err() != nil || c.closed.Load() {
				return
			}
			if isSessionClosedErr(err) {
				c.onSessionClosed()
				return
			}
			c.logger.Warn("pollmux: could not resume the session, falling back to a fresh one", "error", err)
			c.fail()
			return
		}
		// Include peer progress learned by resumeOut from the handshake, not
		// merely in-band acks observed before runLegs returned.
		progressed := c.rl.progress() != progressBefore
		if stability.record(legsDuration, progressed) {
			c.logger.Warn("pollmux: replacement legs failed too quickly after consecutive resumes, abandoning session",
				"consecutive_fast_resumes", stability.consecutiveFast,
				"last_legs_duration", legsDuration,
				"error", err,
			)
			c.fail()
			return
		}
		c.logger.Info("pollmux: session resumed",
			"recv_offset", c.rl.recvOffsetNow(), "unacked_bytes", c.rl.unacked())
	}
}

// runLegs runs one transport's legs to completion. Whichever leg fails first
// decides the outcome; the other is cancelled and waited for, so by the time
// this returns nothing is touching the pipes and a resume may safely rewind
// the reliable layer.
func (c *resumableConn) runLegs() error {
	legCtx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	errCh := make(chan error, 2)
	ws := c.ws.Load()
	if c.transport == TransportWebSocket {
		go func() { errCh <- c.wsReadLoop(legCtx, ws) }()
		go func() { errCh <- c.wsWriteLoop(legCtx, ws) }()
	} else {
		go func() { errCh <- c.downLoop(legCtx) }()
		go func() { errCh <- c.upLoop(legCtx) }()
	}

	err := <-errCh
	cancel()
	// A sender parked in nextOut has no context to observe; this is what
	// gets it back to check legCtx.
	c.rl.interruptOut()
	if ws != nil {
		ws.CloseNow()
	}
	<-errCh
	return err
}

// resume performs the handshake with ResumeHandler and, for WebSocket
// transport, dials the replacement connection, retrying transient failures
// with jittered backoff until the server's grace period is spent. A
// definitive refusal (any 4xx, including the 409 for offsets the server
// cannot honour and the 410/404 for a session that is gone) ends it at
// once — retrying cannot change a server's mind about those.
//
// The jitter matters more here than for an ordinary reconnect: the failure
// this exists for — a CDN closing every connection that reached its
// maximum age — hits a whole fleet of clients within the same second, and
// without it they would all retry on the same beat.
func (c *resumableConn) resume() error {
	deadline := time.Now().Add(c.resumeGrace)
	backoff := 250 * time.Millisecond
	var lastErr error

	for attempt := 1; ; attempt++ {
		serverRecv, err := c.doResume()
		switch {
		case err == nil:
			if !c.rl.resumeOut(serverRecv) {
				return fmt.Errorf("pollmux: server's recv_offset %d is outside what this side can replay", serverRecv)
			}
			if c.transport != TransportWebSocket {
				return nil
			}
			ws, err := c.dialWS()
			if err == nil {
				c.ws.Store(ws)
				return nil
			}
			// A dial failure right after a successful handshake: the next
			// attempt repeats the handshake, which is idempotent — nothing
			// moved on either side in between.
			lastErr = err
		default:
			var fatal *fatalPollError
			if errors.As(err, &fatal) && fatal.status < http.StatusInternalServerError {
				return err
			}
			lastErr = err
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("pollmux: resume grace %v exhausted after %d attempts: %w", c.resumeGrace, attempt, lastErr)
		}
		wait := jitter(backoff)
		c.logger.Debug("pollmux: resume attempt failed, retrying", "attempt", attempt, "error", lastErr, "retry_in", wait)
		if !sleepOrDone(c.ctx, wait) {
			return c.ctx.Err()
		}
		backoff = min(backoff*2, 2*time.Second)
	}
}

// jitter spreads d by ±20%, so clients that lost their transport at the
// same instant do not retry in lockstep.
func jitter(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
}

// doResume performs one POST {prefix}/{id}/resume exchange.
func (c *resumableConn) doResume() (uint64, error) {
	body, err := json.Marshal(ResumeRequest{
		ProtocolVersion: ProtocolVersion,
		RecvOffset:      c.rl.recvOffsetNow(),
	})
	if err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(c.ctx, c.sendTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.resumeURL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<10))
		return 0, &fatalPollError{status: resp.StatusCode, detail: strings.TrimSpace(string(detail))}
	}
	var rr ResumeResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&rr); err != nil {
		return 0, fmt.Errorf("pollmux: failed to parse resume response: %w", err)
	}
	if !rr.Resumed {
		return 0, &fatalPollError{status: http.StatusConflict, detail: "server did not resume the session"}
	}
	return rr.RecvOffset, nil
}

// ---- stream-mode legs ----

// downLoop is pollLoopStream's counterpart: one stream poll after another,
// reopening on a clean end, returning on anything else.
func (c *resumableConn) downLoop(ctx context.Context) error {
	for {
		err := c.doStreamPoll(ctx)
		if errors.Is(err, errStreamEnd) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		return err
	}
}

// doStreamPoll is httpConn.doStreamPoll for a resumable session: data
// arrives as frameSeqData and goes through the reliable layer, and frameAck
// releases replay. A plain frameData is a protocol error here — the server
// never sends one on a resumable session.
func (c *resumableConn) doStreamPoll(ctx context.Context) error {
	reqCtx, cancelReq := context.WithCancel(ctx)
	defer cancelReq()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.pollURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set(HeaderReceiveOnly, "true")
	c.setAuth(req)
	if c.localHealth != nil {
		if c.localHealth() {
			req.Header.Set(HeaderLocalHealth, "ok")
		} else {
			req.Header.Set(HeaderLocalHealth, "down")
		}
	}

	resp, err := c.pollClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusGone:
		return &fatalPollError{status: resp.StatusCode, detail: "session closed by server"}
	default:
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<10))
		return &fatalPollError{status: resp.StatusCode, detail: strings.TrimSpace(string(detail))}
	}

	watchdog := time.AfterFunc(c.idleTimeout, cancelReq)
	defer watchdog.Stop()

	fr := newFrameReader(resp.Body, c.maxFrame)
	for {
		typ, payload, err := fr.next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return errStreamEnd
			}
			if reqCtx.Err() != nil && ctx.Err() == nil {
				return fmt.Errorf("pollmux: stream idle watchdog fired after %v with no frame", c.idleTimeout)
			}
			return fmt.Errorf("reading stream frame: %w", err)
		}
		watchdog.Reset(c.idleTimeout)

		switch typ {
		case frameSeqData:
			off, data, err := splitSeq(payload)
			if err == nil {
				err = c.rl.recvIn(off, data)
			}
			if err != nil {
				return err
			}
		case frameAck:
			n, err := decodeAck(payload)
			if err == nil {
				err = c.rl.ack(n)
			}
			if err != nil {
				return err
			}
		case frameHeartbeat:
		case frameEnd:
			return errStreamEnd
		case frameGone:
			return errSessionGone
		case frameData:
			return errors.New("pollmux: server sent an unnumbered data frame on a resumable session")
		default:
			return fmt.Errorf("pollmux: unknown stream frame type %#x", typ)
		}
	}
}

// upLoop is sendLoopStream's counterpart: one send-stream request after
// another, reopening on a clean rollover.
func (c *resumableConn) upLoop(ctx context.Context) error {
	for {
		if err := c.doSendStream(ctx); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// doSendStream is httpConn.doSendStream with the resumable feeder.
func (c *resumableConn) doSendStream(ctx context.Context) error {
	pr, pw := io.Pipe()
	feedErrCh := make(chan error, 1)
	go func() { feedErrCh <- c.feedSendStream(ctx, pw) }()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.pollURL, pr)
	if err != nil {
		pw.CloseWithError(err)
		<-feedErrCh
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(HeaderSendStream, "true")
	req.ContentLength = -1
	c.setAuth(req)

	resp, err := c.sendStreamClient.Do(req)
	feedErr := <-feedErrCh
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))

	switch resp.StatusCode {
	case http.StatusOK:
		return feedErr
	case http.StatusGone:
		return &fatalPollError{status: resp.StatusCode, detail: "session closed by server"}
	default:
		return &fatalPollError{status: resp.StatusCode}
	}
}

// feedSendStream is httpConn.feedSendStream for a resumable session: bytes
// come from the reliable layer (pending replay first) as frameSeqData, each
// write is preceded by any due ack, and an interrupted wait is either the
// leg being torn down (ctx done) or an ack being due. Every leg opens with
// an ack, so a server that missed one on the previous leg catches up.
func (c *resumableConn) feedSendStream(ctx context.Context, pw *io.PipeWriter) error {
	buf := make([]byte, c.maxFrame)
	deadline := time.Now().Add(c.streamMaxDuration)
	c.rl.resetAck()

	for {
		off, n, err := c.rl.nextOut(buf, c.heartbeat, c.coalesceWindow)
		interrupted := false
		switch {
		case errors.Is(err, io.EOF):
			// out closed: Close() was called. End this leg cleanly.
			writeFrame(pw, frameEnd, nil)
			pw.Close()
			return nil
		case errors.Is(err, errPipeInterrupted):
			if ctx.Err() != nil {
				pw.CloseWithError(ctx.Err())
				return ctx.Err()
			}
			// This is a wake-up signal, not a transport error. Clear it so
			// later code cannot accidentally propagate it as a leg failure.
			interrupted = true
			err = nil
		}

		if ack, due := c.rl.takeAck(); due {
			if err := writeFrame(pw, frameAck, encodeOffset(ack)); err != nil {
				pw.CloseWithError(err)
				return err
			}
		}
		switch {
		case n > 0:
			if err := writeSeqFrame(pw, off, buf[:n]); err != nil {
				pw.CloseWithError(err)
				return err
			}
		case !interrupted:
			if err := writeFrame(pw, frameHeartbeat, nil); err != nil {
				pw.CloseWithError(err)
				return err
			}
		}

		if time.Now().After(deadline) {
			writeFrame(pw, frameEnd, nil)
			pw.Close()
			return nil
		}
	}
}

// ---- WebSocket legs ----

func (c *resumableConn) dialWS() (*websocket.Conn, error) {
	header := http.Header{}
	if c.authToken != "" {
		header.Set("Authorization", "Bearer "+c.authToken)
	}
	dialCtx, cancel := context.WithTimeout(c.ctx, c.dialTimeout)
	defer cancel()
	ws, _, err := websocket.Dial(dialCtx, c.wsURL, &websocket.DialOptions{
		HTTPClient: c.httpClient,
		HTTPHeader: header,
	})
	if err != nil {
		return nil, fmt.Errorf("pollmux: websocket dial failed: %w", err)
	}
	ws.SetReadLimit(int64(c.maxFrame) + 1 + seqHeaderLen)
	return ws, nil
}

// wsReadLoop is wsConn.readLoop through the reliable layer.
func (c *resumableConn) wsReadLoop(ctx context.Context, ws *websocket.Conn) error {
	for {
		rctx, cancel := context.WithTimeout(ctx, c.idleTimeout)
		typ, msg, err := ws.Read(rctx)
		cancel()
		if err != nil {
			return err
		}
		if typ != websocket.MessageBinary {
			continue
		}
		ft, payload, err := wsDecode(msg)
		if err != nil {
			return err
		}
		switch ft {
		case frameSeqData:
			off, data, err := splitSeq(payload)
			if err == nil {
				err = c.rl.recvIn(off, data)
			}
			if err != nil {
				return err
			}
		case frameAck:
			n, err := decodeAck(payload)
			if err == nil {
				err = c.rl.ack(n)
			}
			if err != nil {
				return err
			}
		case frameHeartbeat:
		case frameData:
			return errors.New("pollmux: server sent an unnumbered data frame on a resumable session")
		default:
			return fmt.Errorf("pollmux: unexpected websocket frame type %#x", ft)
		}
	}
}

// wsWriteLoop is wsConn.writeLoop through the reliable layer; see
// feedSendStream for the shape.
func (c *resumableConn) wsWriteLoop(ctx context.Context, ws *websocket.Conn) error {
	buf := make([]byte, c.maxFrame)
	c.rl.resetAck()

	for {
		off, n, err := c.rl.nextOut(buf, c.heartbeat, c.coalesceWindow)
		interrupted := false
		switch {
		case errors.Is(err, io.EOF):
			return io.EOF // Close() was called
		case errors.Is(err, errPipeInterrupted):
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// The interrupt only asks this leg to re-check cancellation and
			// send a pending ack. Leaving it in err makes the final err check
			// below tear down a healthy WebSocket when no ack is due.
			interrupted = true
			err = nil
		}

		wctx, cancel := context.WithTimeout(ctx, c.idleTimeout)
		if ack, due := c.rl.takeAck(); due {
			err = ws.Write(wctx, websocket.MessageBinary, wsEncode(frameAck, encodeOffset(ack)))
		}
		if err == nil {
			switch {
			case n > 0:
				err = ws.Write(wctx, websocket.MessageBinary, wsEncodeSeq(off, buf[:n]))
			case !interrupted:
				err = ws.Write(wctx, websocket.MessageBinary, wsEncode(frameHeartbeat, nil))
			}
		}
		cancel()
		if err != nil {
			return err
		}
	}
}

// connectResumable builds the Conn for a connect that negotiated Resumable.
// Called by Connector.connect once every transport-level validation has
// passed and, for stream mode with UploadStreamPreference left on auto,
// once the upload probe has passed too.
func (c *Connector) connectResumable(ctx context.Context, base string, cr *ConnectResponse,
	dialTimeout, sendTimeout, pollGrace, coalesce time.Duration) (Conn, error) {
	maxFrame := orInt(cr.Limits.PollBufferBytes, DefaultPollBufferSize)
	logger := c.logger().With("session_id", cr.SessionID)

	connCtx, cancel := context.WithCancel(context.Background())
	conn := &resumableConn{
		sessionGuard:      newSessionGuard(c, cr.SessionID, cancel),
		sessionID:         cr.SessionID,
		limits:            cr.Limits,
		meta:              cr.Meta,
		logger:            logger,
		rl:                newReliable(NewBufferedPipe(), NewBufferedPipe(), c.MaxReplayBytes),
		transport:         cr.Transport,
		pollURL:           fmt.Sprintf("%s/%s/poll", base, cr.SessionID),
		resumeURL:         fmt.Sprintf("%s/%s/resume", base, cr.SessionID),
		deleteURL:         fmt.Sprintf("%s/%s", base, cr.SessionID),
		wsURL:             fmt.Sprintf("%s/%s/ws", base, cr.SessionID),
		authToken:         c.AuthToken,
		httpClient:        &http.Client{Timeout: sendTimeout, Transport: c.newTransport(dialTimeout, sendTimeout), CheckRedirect: noRedirect},
		localHealth:       c.LocalHealth,
		heartbeat:         cr.Limits.HeartbeatInterval(),
		idleTimeout:       cr.Limits.HeartbeatInterval() + pollGrace,
		streamMaxDuration: cr.Limits.StreamMaxDuration(),
		coalesceWindow:    coalesce,
		sendTimeout:       sendTimeout,
		dialTimeout:       dialTimeout,
		maxFrame:          maxFrame,
		resumeGrace:       cr.Limits.ResumeGrace(),
		transportFailed:   make(chan struct{}),
		ctx:               connCtx,
		cancel:            cancel,
	}

	if cr.Transport == TransportWebSocket {
		// The WebSocket dial reuses httpClient for the upgrade request
		// itself, so its Timeout must not cut a long-lived connection
		// short: coder/websocket only uses the client for the handshake.
		conn.httpClient.Timeout = 0
		ws, err := conn.dialWS()
		if err != nil {
			cancel()
			return nil, err
		}
		conn.ws.Store(ws)
	} else {
		conn.transport = PollModeStream
		conn.pollClient = &http.Client{Transport: c.newTransport(dialTimeout, pollGrace), CheckRedirect: noRedirect}
		conn.sendStreamClient = &http.Client{Transport: c.newTransport(dialTimeout, cr.Limits.StreamMaxDuration()+pollGrace), CheckRedirect: noRedirect}
	}

	c.adoptSession(cr.SessionID)

	logger.Debug("pollmux: connected (resumable)",
		"transport", conn.transport,
		"resume_grace", conn.resumeGrace,
		"poll_buffer_bytes", maxFrame,
	)

	conn.wg.Add(1)
	go conn.supervise()
	return conn, nil
}
