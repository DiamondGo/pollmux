package pollmux

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
)

// e2e stands up the whole stack in one process: the three handlers behind a
// real HTTP server, a real Connector on the other side, and yamux on top of
// both. Only the network is local.
type e2e struct {
	ts    *httptest.Server
	store *SessionStore
	cfg   ServerConfig
	hooks Hooks

	// pollMode is "" (batch) or PollModeStream. dial() reads it to decide
	// whether the client asks for stream mode.
	pollMode string

	mu           sync.Mutex
	disconnects  []DisconnectReason
	serverStream func(net.Conn)
}

// newE2E builds the server side in batch mode. handle runs for each yamux
// stream the client opens; nil means echo.
func newE2E(t *testing.T, handle func(net.Conn)) *e2e {
	return newE2EWithMode(t, handle, "")
}

// newE2EStream is newE2E's stream-mode counterpart: same topology, same
// helper methods, negotiated stream instead of batch.
func newE2EStream(t *testing.T, handle func(net.Conn)) *e2e {
	return newE2EWithMode(t, handle, PollModeStream)
}

// newE2EWebSocket is newE2E's WebSocket-transport counterpart: same
// topology, same helper methods, one WebSocket connection instead of
// long-poll requests.
func newE2EWebSocket(t *testing.T, handle func(net.Conn)) *e2e {
	return newE2EWithMode(t, handle, TransportWebSocket)
}

func newE2EWithMode(t *testing.T, handle func(net.Conn), mode string) *e2e {
	t.Helper()

	if handle == nil {
		handle = func(stream net.Conn) {
			defer stream.Close()
			io.Copy(stream, stream)
		}
	}

	e := &e2e{
		store:        NewSessionStore(),
		serverStream: handle,
		pollMode:     mode,
		cfg: ServerConfig{
			PollTimeout:    500 * time.Millisecond,
			SessionTimeout: time.Second,
			SweepInterval:  100 * time.Millisecond,
			CoalesceWindow: 2 * time.Millisecond,
			PollBufferSize: 256 << 10,
			MaxSendBytes:   256 << 10,
		},
	}
	switch mode {
	case PollModeStream:
		e.cfg.PollMode = mode
		e.cfg.HeartbeatInterval = 200 * time.Millisecond
		e.cfg.StreamMaxDuration = 2 * time.Second
	case TransportWebSocket:
		e.cfg.EnableWebSocket = true
		e.cfg.HeartbeatInterval = 200 * time.Millisecond
	}

	e.hooks = Hooks{
		OnConnect: func(s *Session, _ map[string]string) error {
			// The application's job: put yamux on top of the session. The
			// client opens streams, so this end accepts them.
			go func() {
				sess, err := ServerSession(s)
				if err != nil {
					return
				}
				defer sess.Close()
				for {
					stream, err := sess.Accept()
					if err != nil {
						return
					}
					go e.serverStream(stream)
				}
			}()
			return nil
		},
		OnDisconnect: func(_ *Session, reason DisconnectReason) {
			e.mu.Lock()
			e.disconnects = append(e.disconnects, reason)
			e.mu.Unlock()
		},
	}

	mux := http.NewServeMux()
	mux.Handle("POST /tunnel/connect", ConnectHandler(e.store, e.cfg, e.hooks))
	mux.Handle("POST /tunnel/{id}/poll", PollHandler(e.store, e.cfg, e.hooks))
	mux.Handle("DELETE /tunnel/{id}", DeleteHandler(e.store, e.cfg, e.hooks))
	mux.Handle("GET /tunnel/{id}/ws", WebSocketHandler(e.store, e.cfg, e.hooks))

	e.ts = httptest.NewServer(mux)
	t.Cleanup(e.ts.Close)
	return e
}

// dial brings up a client conn plus its yamux session.
func (e *e2e) dial(t *testing.T) (Conn, *yamux.Session) {
	t.Helper()
	c := &Connector{
		BaseURL:         e.ts.URL,
		PollGrace:       2 * time.Second,
		PreferStream:    e.pollMode == PollModeStream,
		PreferWebSocket: e.pollMode == TransportWebSocket,
	}
	conn, err := c.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	sess, err := ClientSession(conn)
	if err != nil {
		conn.Close()
		t.Fatalf("ClientSession: %v", err)
	}
	t.Cleanup(func() {
		sess.Close()
		conn.Close()
	})
	return conn, sess
}

func (e *e2e) reasons() []DisconnectReason {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]DisconnectReason, len(e.disconnects))
	copy(out, e.disconnects)
	return out
}

// Megabytes through the whole stack, compared by hash. Everything else in this
// library is in service of this working.
func TestE2EByteFidelity(t *testing.T)           { testE2EByteFidelity(t, newE2E(t, nil)) }
func TestE2EByteFidelity_Stream(t *testing.T)    { testE2EByteFidelity(t, newE2EStream(t, nil)) }
func TestE2EByteFidelity_WebSocket(t *testing.T) { testE2EByteFidelity(t, newE2EWebSocket(t, nil)) }

func testE2EByteFidelity(t *testing.T, e *e2e) {
	_, sess := e.dial(t)

	payload := make([]byte, 2<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	want := sha256.Sum256(payload)

	stream, err := sess.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer stream.Close()

	// Read a known length rather than to EOF: yamux v0.1.2 has no CloseWrite,
	// so there is no way to half-close and still read the reply.
	readDone := make(chan []byte, 1)
	go func() {
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(stream, got); err != nil {
			readDone <- nil
			return
		}
		readDone <- got
	}()

	if _, err := stream.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	select {
	case got := <-readDone:
		if got == nil {
			t.Fatal("the echo came back short")
		}
		if sha256.Sum256(got) != want {
			t.Fatal("echoed bytes do not match what was sent")
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the echo never came back")
	}
}

// Reads and writes travel on separate requests precisely so one direction
// cannot head-of-line block the other. If they were serialized, N slow streams
// would take N times as long as one.
func TestE2EConcurrentStreamsAreNotSerialized(t *testing.T) {
	testE2EConcurrentStreamsAreNotSerialized(t, func(handle func(net.Conn)) *e2e { return newE2E(t, handle) })
}
func TestE2EConcurrentStreamsAreNotSerialized_Stream(t *testing.T) {
	testE2EConcurrentStreamsAreNotSerialized(t, func(handle func(net.Conn)) *e2e { return newE2EStream(t, handle) })
}
func TestE2EConcurrentStreamsAreNotSerialized_WebSocket(t *testing.T) {
	testE2EConcurrentStreamsAreNotSerialized(t, func(handle func(net.Conn)) *e2e { return newE2EWebSocket(t, handle) })
}

func testE2EConcurrentStreamsAreNotSerialized(t *testing.T, build func(func(net.Conn)) *e2e) {
	const streamDelay = 200 * time.Millisecond
	const streams = 8

	e := build(func(stream net.Conn) {
		defer stream.Close()
		buf := make([]byte, 16)
		n, err := stream.Read(buf)
		if err != nil {
			return
		}
		time.Sleep(streamDelay)
		stream.Write(buf[:n])
	})
	_, sess := e.dial(t)

	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, streams)

	for range streams {
		wg.Go(func() {
			stream, err := sess.Open()
			if err != nil {
				errs <- err
				return
			}
			defer stream.Close()
			if _, err := stream.Write([]byte("ping")); err != nil {
				errs <- err
				return
			}
			buf := make([]byte, 16)
			if _, err := io.ReadFull(stream, buf[:4]); err != nil {
				errs <- err
			}
		})
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("stream failed: %v", err)
	}

	elapsed := time.Since(start)
	serial := streams * streamDelay
	if elapsed > serial/2 {
		t.Fatalf("%d concurrent streams took %v; serial would be %v, so they are not overlapping",
			streams, elapsed, serial)
	}
}

func TestE2EBidirectionalTraffic(t *testing.T) {
	testE2EBidirectionalTraffic(t, func(handle func(net.Conn)) *e2e { return newE2E(t, handle) })
}
func TestE2EBidirectionalTraffic_Stream(t *testing.T) {
	testE2EBidirectionalTraffic(t, func(handle func(net.Conn)) *e2e { return newE2EStream(t, handle) })
}
func TestE2EBidirectionalTraffic_WebSocket(t *testing.T) {
	testE2EBidirectionalTraffic(t, func(handle func(net.Conn)) *e2e { return newE2EWebSocket(t, handle) })
}

func testE2EBidirectionalTraffic(t *testing.T, build func(func(net.Conn)) *e2e) {
	e := build(func(stream net.Conn) {
		defer stream.Close()
		buf := make([]byte, 64)
		n, err := stream.Read(buf)
		if err != nil {
			return
		}
		stream.Write([]byte("reply to " + string(buf[:n])))
	})
	_, sess := e.dial(t)

	stream, err := sess.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer stream.Close()

	if _, err := stream.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	buf := make([]byte, 64)
	stream.SetReadDeadline(time.Now().Add(10 * time.Second))
	n, err := stream.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got := string(buf[:n]); got != "reply to hello" {
		t.Fatalf("got %q, want %q", got, "reply to hello")
	}
}

// ★ A5 end to end. The server drops the session and the client has to find out
// in seconds — not by waiting for its own timeout, which is what sharing 204
// with the idle heartbeat used to cause.
func TestE2EServerCloseIsNoticedPromptly(t *testing.T) {
	testE2EServerCloseIsNoticedPromptly(t, newE2E(t, nil))
}
func TestE2EServerCloseIsNoticedPromptly_Stream(t *testing.T) {
	testE2EServerCloseIsNoticedPromptly(t, newE2EStream(t, nil))
}
func TestE2EServerCloseIsNoticedPromptly_WebSocket(t *testing.T) {
	testE2EServerCloseIsNoticedPromptly(t, newE2EWebSocket(t, nil))
}

func testE2EServerCloseIsNoticedPromptly(t *testing.T, e *e2e) {
	conn, _ := e.dial(t)

	// Let the poll loop park before pulling the session out from under it.
	waitFor(t, 2*time.Second, func() bool {
		s, ok := e.store.Get(conn.SessionID())
		return ok && s.PollInFlight() > 0
	})

	s, _ := e.store.Get(conn.SessionID())
	start := time.Now()
	CloseSession(e.store, e.hooks, s, ReasonServerClose)

	select {
	case <-conn.TransportFailed():
		elapsed := time.Since(start)
		// The session timeout is 1s here; in production it is 60s. Either way
		// the point is that this does not wait for it.
		if elapsed > 500*time.Millisecond {
			t.Fatalf("took %v to notice the server closed the session", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the client never noticed the server had closed its session")
	}

	if got := e.reasons(); len(got) != 1 || got[0] != ReasonServerClose {
		t.Fatalf("OnDisconnect reasons = %v, want exactly [server_close]", got)
	}
}

// Graceful shutdown: close everything in the store and every application gets
// told once, with a reason it can distinguish from an eviction.
//
// Batch-only: this exercises store/sweeper mechanics that do not depend on
// poll mode.
func TestE2EGracefulShutdownClosesEverySession(t *testing.T) {
	e := newE2E(t, nil)

	const clients = 3
	conns := make([]Conn, 0, clients)
	for range clients {
		conn, _ := e.dial(t)
		conns = append(conns, conn)
	}

	waitFor(t, 2*time.Second, func() bool { return e.store.Len() == clients })

	for _, s := range e.store.All() {
		CloseSession(e.store, e.hooks, s, ReasonServerClose)
	}

	if e.store.Len() != 0 {
		t.Fatalf("store still holds %d sessions after shutdown", e.store.Len())
	}

	got := e.reasons()
	if len(got) != clients {
		t.Fatalf("OnDisconnect fired %d times, want %d — once per session", len(got), clients)
	}
	for i, r := range got {
		if r != ReasonServerClose {
			t.Fatalf("reason %d = %v, want server_close", i, r)
		}
	}

	// And every client finds out.
	for i, conn := range conns {
		select {
		case <-conn.TransportFailed():
		case <-time.After(5 * time.Second):
			t.Fatalf("client %d never noticed the shutdown", i)
		}
	}
}

// A client that goes away without a DELETE — killed, network cut — must be
// swept, and the application told, within roughly one session timeout.
//
// Batch-only: sweeper mechanics do not depend on poll mode.
func TestE2EAbandonedSessionIsSwept(t *testing.T) {
	e := newE2E(t, nil)
	conn, _ := e.dial(t)

	stop := StartSweeper(e.store, e.cfg, e.hooks)
	defer stop()

	waitFor(t, 2*time.Second, func() bool { return e.store.Len() == 1 })

	// Stop polling without telling the server, the way a killed process would.
	conn.(*httpConn).cancel()

	// Wait on the callback, not on the store: the sweeper unindexes a session
	// before it notifies, so the store empties fractionally earlier.
	waitFor(t, 10*time.Second, func() bool { return len(e.reasons()) == 1 })

	if got := e.reasons(); got[0] != ReasonEvicted {
		t.Fatalf("OnDisconnect reasons = %v, want exactly [evicted]", got)
	}
	if e.store.Len() != 0 {
		t.Fatalf("store still holds %d sessions after the sweep", e.store.Len())
	}
}

// A clean client exit removes the session immediately rather than leaving it
// for the sweeper.
func TestE2ECleanDisconnect(t *testing.T) {
	testE2ECleanDisconnect(t, newE2E(t, nil))
}
func TestE2ECleanDisconnect_Stream(t *testing.T) {
	testE2ECleanDisconnect(t, newE2EStream(t, nil))
}
func TestE2ECleanDisconnect_WebSocket(t *testing.T) {
	testE2ECleanDisconnect(t, newE2EWebSocket(t, nil))
}

func testE2ECleanDisconnect(t *testing.T, e *e2e) {
	c := &Connector{
		BaseURL:         e.ts.URL,
		PollGrace:       2 * time.Second,
		PreferStream:    e.pollMode == PollModeStream,
		PreferWebSocket: e.pollMode == TransportWebSocket,
	}
	conn, err := c.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return e.store.Len() == 1 })

	conn.Close()

	waitFor(t, 3*time.Second, func() bool { return e.store.Len() == 0 })

	if got := e.reasons(); len(got) != 1 || got[0] != ReasonClientDelete {
		t.Fatalf("OnDisconnect reasons = %v, want exactly [client_delete]", got)
	}
}

// Reconnecting through the real stack, driven by ReconnectLoop: the server
// closes a session and the loop brings a fresh one up on its own.
func TestE2EReconnectLoopRecoversFromServerClose(t *testing.T) {
	testE2EReconnectLoopRecoversFromServerClose(t, newE2E(t, nil))
}
func TestE2EReconnectLoopRecoversFromServerClose_Stream(t *testing.T) {
	testE2EReconnectLoopRecoversFromServerClose(t, newE2EStream(t, nil))
}
func TestE2EReconnectLoopRecoversFromServerClose_WebSocket(t *testing.T) {
	testE2EReconnectLoopRecoversFromServerClose(t, newE2EWebSocket(t, nil))
}

func testE2EReconnectLoopRecoversFromServerClose(t *testing.T, e *e2e) {
	sessionIDs := make(chan string, 4)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	loop := &ReconnectLoop{
		Connect: func(ctx context.Context) (Conn, error) {
			c := &Connector{
				BaseURL:         e.ts.URL,
				PollGrace:       2 * time.Second,
				PreferStream:    e.pollMode == PollModeStream,
				PreferWebSocket: e.pollMode == TransportWebSocket,
			}
			return c.Connect(ctx)
		},
		Serve: func(ctx context.Context, conn Conn) Outcome {
			sessionIDs <- conn.SessionID()
			select {
			case <-ctx.Done():
				return OutcomeShutdown
			case <-conn.TransportFailed():
				return OutcomeTransportFailed
			}
		},
		InitialBackoff:  20 * time.Millisecond,
		MaxBackoff:      200 * time.Millisecond,
		PeerClosedPause: 10 * time.Millisecond,
	}
	go loop.Run(ctx)

	var first string
	select {
	case first = <-sessionIDs:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop never established a first session")
	}

	// Drop it the way a restarting server would.
	if s, ok := e.store.Get(first); ok {
		CloseSession(e.store, e.hooks, s, ReasonServerClose)
	}

	select {
	case second := <-sessionIDs:
		if second == first {
			t.Fatal("the loop reused the dead session id instead of registering afresh")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the loop never reconnected after the server closed the session")
	}
}
