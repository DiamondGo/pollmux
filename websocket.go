package pollmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// wsEncode/wsDecode tag a WebSocket message with one of frame.go's frameType
// values. WebSocket already delivers whole messages with their own
// boundaries, so — unlike frame.go's length-prefixed wire format, needed
// because an HTTP chunked body has no message boundaries of its own — this
// only needs a one-byte type tag ahead of the payload. Only frameData and
// frameHeartbeat ever travel this way: frameEnd/frameGone have no
// WebSocket counterpart, because a WebSocket connection has its own native
// close handshake and "this session is over" is conveyed by actually
// closing the connection (see WebSocketHandler and wsConn.Close) rather
// than by a payload frame.
func wsEncode(typ frameType, payload []byte) []byte {
	msg := make([]byte, 1+len(payload))
	msg[0] = byte(typ)
	copy(msg[1:], payload)
	return msg
}

func wsDecode(msg []byte) (frameType, []byte, error) {
	if len(msg) == 0 {
		return 0, nil, errors.New("pollmux: empty websocket message")
	}
	return frameType(msg[0]), msg[1:], nil
}

// ---- Server side ----

// WebSocketHandler serves GET {prefix}/{id}/ws. It upgrades the request to a
// WebSocket and attaches it to the named session as that session's sole
// transport for as long as the connection lives — a full replacement for
// the poll/send-stream request pair PollHandler otherwise serves, not an
// addition to it. Register it alongside ConnectHandler/PollHandler/
// DeleteHandler; it only ever succeeds for a session ConnectHandler
// negotiated TransportWebSocket for (see ServerConfig.EnableWebSocket).
func WebSocketHandler(st *SessionStore, cfg ServerConfig, h Hooks) http.Handler {
	cfg.check()
	cfg = cfg.withDefaults()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := cfg.SessionIDFunc(r)
		s, ok := st.Get(id)
		if !ok {
			writeError(w, http.StatusNotFound, "session not found")
			return
		}
		if s.transport != TransportWebSocket {
			writeError(w, http.StatusBadRequest, "session was not negotiated for websocket transport")
			return
		}
		if !s.wsAttached.CompareAndSwap(false, true) {
			// In practice unreachable — ReconnectLoop always calls Connect
			// for a fresh session id rather than reattaching to an old one
			// — but a session having two WebSockets fighting over the same
			// toClient/toServer pipes would be a real, confusing bug, so
			// this stays a hard error rather than an assumption.
			writeError(w, http.StatusConflict, "session already has a websocket attached")
			return
		}
		s.touch()

		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			// Our clients are not browsers and never send a same-origin
			// Origin header, so the CSRF-style check InsecureSkipVerify
			// disables does not apply here — the same trust boundary
			// ConnectHandler's own auth (Hooks.Authenticate) already
			// covers this connection.
			InsecureSkipVerify: true,
		})
		if err != nil {
			// Accept already wrote the HTTP error response.
			return
		}
		c.SetReadLimit(int64(cfg.MaxSendBytes) + 1) // +1 for wsEncode's type byte

		s.pollInFlight.Add(1)
		defer s.pollInFlight.Add(-1)

		// Not r.Context(): coder/websocket's Accept doc warns that using the
		// request context after Accept returns "may lead to unexpected
		// behavior" (it has hijacked the connection out from under net/http
		// by that point). The pumps below need no signal from it anyway —
		// clean shutdown already arrives through s.toClient/s.toServer
		// closing (see closeSession), and an abrupt disconnect already
		// surfaces as a Read/Write error on c itself.
		ctx := context.Background()
		idle := cfg.HeartbeatInterval + defaultStreamReadGrace

		// wg tracks both pumps to completion; readErrCh/writeErrCh exist only
		// to let the select below observe whichever one finishes first — the
		// select drains exactly one of them, so waiting on wg afterward
		// (rather than reading both channels unconditionally) is what avoids
		// blocking forever on the channel the select never touched.
		var wg sync.WaitGroup
		wg.Add(2)
		readErrCh := make(chan error, 1)
		writeErrCh := make(chan error, 1)
		go func() { defer wg.Done(); readErrCh <- wsReadPump(ctx, c, s, idle) }()
		go func() { defer wg.Done(); writeErrCh <- wsWritePump(ctx, c, s, cfg, idle) }()

		// Whichever pump ends first decides the outcome and closing c below
		// is what unblocks whichever pump is still running — a read or
		// write in flight on a closed connection errors out immediately.
		var firstErr error
		select {
		case firstErr = <-readErrCh:
		case firstErr = <-writeErrCh:
		}

		if errors.Is(firstErr, io.EOF) {
			// wsWritePump's io.EOF: the session ended server-side (delete,
			// eviction, or shutdown), not a link failure. Close cleanly so
			// the client sees a normal closure — though see wsConn.readLoop,
			// which treats any non-self-initiated close as a transport
			// failure regardless, the same role frameGone plays for the
			// poll/stream transport.
			c.Close(websocket.StatusNormalClosure, "session closed")
		} else {
			c.CloseNow()
		}
		wg.Wait()

		if firstErr != nil && !errors.Is(firstErr, io.EOF) {
			cfg.Logger.Debug("pollmux: websocket attachment ended", "session_id", s.ID, "error", firstErr)
		}
	})
}

// wsReadPump decodes frames off the client's WebSocket messages and writes
// data frames upstream, the WebSocket transport's counterpart to
// pollSendStream. Every Read is bounded by idle so a client that has gone
// silent — no data, no heartbeat — is noticed within idle instead of held
// open forever; per coder/websocket's own contract a context timeout closes
// the whole connection just like any other error, which is exactly the
// outcome wanted here.
func wsReadPump(ctx context.Context, c *websocket.Conn, s *Session, idle time.Duration) error {
	for {
		rctx, cancel := context.WithTimeout(ctx, idle)
		typ, msg, err := c.Read(rctx)
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
		case frameData:
			if len(payload) > 0 {
				if _, err := s.writeUpstream(payload); err != nil {
					return err
				}
			}
		case frameHeartbeat:
			// Liveness only.
		default:
			return fmt.Errorf("pollmux: unexpected websocket frame type %#x", ft)
		}
	}
}

// wsWritePump drains the session's outgoing pipe onto the WebSocket
// connection, the WebSocket transport's counterpart to pollStream: reusing
// toClient.ReadAvailable with HeartbeatInterval as its timeout is what turns
// "nothing to send" into a heartbeat frame on the same cadence pollStream
// already uses, so the peer's read-idle watchdog never has reason to fire
// while this session is merely quiet rather than dead.
func wsWritePump(ctx context.Context, c *websocket.Conn, s *Session, cfg ServerConfig, idle time.Duration) error {
	buf := make([]byte, cfg.PollBufferSize)
	for {
		n, err := s.toClient.ReadAvailable(buf, cfg.HeartbeatInterval, cfg.CoalesceWindow)
		if errors.Is(err, io.EOF) {
			return io.EOF // session closed: the caller distinguishes this from a real failure
		}

		wctx, cancel := context.WithTimeout(ctx, idle)
		if n > 0 {
			err = c.Write(wctx, websocket.MessageBinary, wsEncode(frameData, buf[:n]))
		} else {
			err = c.Write(wctx, websocket.MessageBinary, wsEncode(frameHeartbeat, nil))
		}
		cancel()
		if err != nil {
			return err
		}
	}
}

// ---- Client side ----

// connectWebSocket dials {prefix}/{id}/ws and returns a Conn backed by it.
// Called by Connector.Connect once ConnectResponse.Transport is
// TransportWebSocket; see the comment there for why this replaces the
// poll/send-stream setup entirely rather than layering on top of it.
func (c *Connector) connectWebSocket(ctx context.Context, base string, cr *ConnectResponse, dialTimeout, pollGrace time.Duration) (Conn, error) {
	// websocket.Dial interprets an http(s) URL as ws(s), so base — always
	// http(s) here, since Connector.BaseURL is — needs no scheme rewrite.
	wsURL := fmt.Sprintf("%s/%s/ws", base, cr.SessionID)

	header := http.Header{}
	if c.AuthToken != "" {
		header.Set("Authorization", "Bearer "+c.AuthToken)
	}
	// Reused for the DELETE Close sends below — both are short, bounded
	// requests outside the WebSocket connection itself, the same shape
	// httpConn.deleteURL/sendClient already have.
	httpClient := &http.Client{Transport: c.newTransport(dialTimeout, dialTimeout)}

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	ws, _, err := websocket.Dial(dialCtx, wsURL, &websocket.DialOptions{
		HTTPClient: httpClient,
		HTTPHeader: header,
	})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("pollmux: websocket dial failed: %w", err)
	}
	maxFrame := orInt(cr.Limits.PollBufferBytes, DefaultPollBufferSize)
	ws.SetReadLimit(int64(maxFrame) + 1) // +1 for wsEncode's type byte

	connCtx, cancelConn := context.WithCancel(context.Background())
	conn := &wsConn{
		sessionGuard:    newSessionGuard(c, cr.SessionID, cancelConn),
		sessionID:       cr.SessionID,
		deleteURL:       fmt.Sprintf("%s/%s", base, cr.SessionID),
		authToken:       c.AuthToken,
		ws:              ws,
		httpClient:      httpClient,
		limits:          cr.Limits,
		meta:            cr.Meta,
		logger:          c.logger().With("session_id", cr.SessionID),
		readPipe:        NewBufferedPipe(),
		writePipe:       NewBufferedPipe(),
		transportFailed: make(chan struct{}),
		idleTimeout:     cr.Limits.HeartbeatInterval() + pollGrace,
		coalesceWindow:  orDuration(c.CoalesceWindow, DefaultCoalesceWindow),
		maxFrame:        maxFrame,
		ctx:             connCtx,
		cancel:          cancelConn,
	}

	conn.wg.Add(2)
	go conn.readLoop()
	go conn.writeLoop()

	c.adoptSession(cr.SessionID)

	return conn, nil
}

// wsConn is Conn's WebSocket-transport implementation. One connection
// carries both directions, so — unlike httpConn, which needs a poll loop
// plus either a flush loop or a send-stream loop — this needs exactly one
// read loop and one write loop, each on a BufferedPipe the same way
// httpConn's stream-mode paths already work.
type wsConn struct {
	sessionGuard
	sessionID  string
	deleteURL  string
	authToken  string
	ws         *websocket.Conn
	httpClient *http.Client // for the DELETE Close sends, same shape as httpConn.sendClient

	limits Limits
	meta   map[string]string
	logger *slog.Logger

	readPipe  *BufferedPipe
	writePipe *BufferedPipe

	idleTimeout    time.Duration
	coalesceWindow time.Duration
	maxFrame       int

	transportFailed chan struct{}
	failOnce        sync.Once

	closed atomic.Bool
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (c *wsConn) SessionID() string                { return c.sessionID }
func (c *wsConn) Limits() Limits                   { return c.limits }
func (c *wsConn) Meta() map[string]string          { return maps.Clone(c.meta) }
func (c *wsConn) TransportFailed() <-chan struct{} { return c.transportFailed }

func (c *wsConn) onTransportProblem(err error) {
	if c.supersededByNewerConnect(c.logger) {
		c.readPipe.Close()
		return
	}
	c.logger.Warn("pollmux: websocket transport problem, signalling failure", "error", err)
	c.fail()
}

// fail signals transport failure once and unblocks anyone reading.
func (c *wsConn) fail() {
	c.failOnce.Do(func() {
		close(c.transportFailed)
		c.readPipe.Close()
	})
}

// Read returns data the server sent down the WebSocket connection.
func (c *wsConn) Read(p []byte) (int, error) { return c.readPipe.Read(p) }

// Write queues data for delivery. Like httpConn.Write, it returns once the
// data is queued, not once the peer has it.
func (c *wsConn) Write(p []byte) (int, error) {
	if c.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	return c.writePipe.Write(p)
}

// Close stops both loops and closes the WebSocket connection.
func (c *wsConn) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}

	// Cancelling aborts whichever loop is in flight on a Read/Write call —
	// coder/websocket closes the connection on a context error from either,
	// the same effect c.ws.Close below would have, just immediate rather
	// than waiting on a call that may be blocked for up to idleTimeout.
	c.cancel()
	c.writePipe.Close()
	c.wg.Wait()
	c.readPipe.Close()

	// Already closed by the cancellation above in the common case;
	// CloseNow is a no-op then. Kept for the case neither loop was inside a
	// Read/Write call at the moment of cancellation (e.g. writeLoop parked
	// in ReadAvailable, which c.writePipe.Close already unblocks, but
	// without ever touching c.ws) — a clean close still needs to happen
	// somewhere, and a full close handshake is not worth the wait here.
	c.ws.CloseNow()

	// Same reason httpConn.Close sends one: closing the WebSocket only
	// stops this attachment, it does not by itself tell the server the
	// session is done — WebSocketHandler treats a connection dropping
	// without a DELETE the same as any other abrupt disconnect, leaving the
	// session for the sweeper. A DELETE is what makes a deliberate Close
	// register as ReasonClientDelete immediately instead.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.deleteURL, nil)
	if err == nil {
		if c.authToken != "" {
			req.Header.Set("Authorization", "Bearer "+c.authToken)
		}
		if resp, err := c.httpClient.Do(req); err == nil {
			io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
			resp.Body.Close()
		}
	}
	c.httpClient.CloseIdleConnections()
	return nil
}

// readLoop decodes frames off the WebSocket connection into readPipe, the
// same role doStreamPoll's frame-decoding loop plays for the poll/stream
// transport: data frames are payload, heartbeat frames only prove the link
// is alive. Any error — including the idle watchdog's own context timeout —
// ends the connection; it is a transport failure unless this side is the one
// that asked to shut down.
func (c *wsConn) readLoop() {
	defer c.wg.Done()
	for {
		rctx, cancel := context.WithTimeout(c.ctx, c.idleTimeout)
		typ, msg, err := c.ws.Read(rctx)
		cancel()
		if err != nil {
			if c.ctx.Err() != nil || c.closed.Load() {
				return // deliberate shutdown, not a failure
			}
			c.onTransportProblem(err)
			return
		}
		if typ != websocket.MessageBinary {
			continue
		}
		ft, payload, err := wsDecode(msg)
		if err != nil {
			c.onTransportProblem(err)
			return
		}
		switch ft {
		case frameData:
			if len(payload) > 0 {
				if _, err := c.readPipe.Write(payload); err != nil {
					return // pipe already closed: Close() or fail() got here first
				}
			}
		case frameHeartbeat:
			// Liveness only.
		default:
			c.onTransportProblem(fmt.Errorf("unexpected websocket frame type %v", ft))
			return
		}
	}
}

// writeLoop drains writePipe onto the WebSocket connection, the send-side
// mirror of readLoop — structurally the same as feedSendStream, minus the
// StreamMaxDuration rollover that transport needs and this one does not
// (see ServerConfig.StreamMaxDuration).
func (c *wsConn) writeLoop() {
	defer c.wg.Done()
	buf := make([]byte, c.maxFrame)
	heartbeat := c.limits.HeartbeatInterval()

	for {
		n, err := c.writePipe.ReadAvailable(buf, heartbeat, c.coalesceWindow)
		if errors.Is(err, io.EOF) {
			return // Close() was called; writePipe closing is the signal
		}

		wctx, cancel := context.WithTimeout(c.ctx, c.idleTimeout)
		if n > 0 {
			err = c.ws.Write(wctx, websocket.MessageBinary, wsEncode(frameData, buf[:n]))
		} else {
			err = c.ws.Write(wctx, websocket.MessageBinary, wsEncode(frameHeartbeat, nil))
		}
		cancel()
		if err != nil {
			if c.ctx.Err() != nil || c.closed.Load() {
				return // deliberate shutdown, not a failure
			}
			c.onTransportProblem(err)
			return
		}
	}
}
