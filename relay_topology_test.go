package pollmux

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
)

// This file runs a miniature relay built entirely on pollmux: a broker in the
// middle, a provider on one side, a consumer on the other, and streams bridged
// between them. application_test.go pins down the API shape; this pins down the
// runtime behaviour that shape has to support.
//
// The three roles:
//
//	consumer --(yamux client)--> broker --(yamux server per consumer)
//	                             broker --(yamux client)--> provider
//
// so the broker is a yamux server toward consumers and a yamux client toward
// providers, as relay.go is today.

// ---------------------------------------------------------------------------
// the broker
// ---------------------------------------------------------------------------

type miniBroker struct {
	ts    *httptest.Server
	store *SessionStore
	cfg   ServerConfig
	hooks Hooks

	mu        sync.Mutex
	providers map[string]*yamux.Session            // endpoint -> provider yamux
	consumers map[string]map[string]*yamux.Session // endpoint -> session id -> consumer yamux

	// authSeen records the Authorization header per request path, so we can
	// prove the client authenticates every request and not just connect.
	authMu   sync.Mutex
	authSeen map[string][]string
}

func newMiniBroker(t *testing.T, token string, tls bool) *miniBroker {
	return newMiniBrokerWithMode(t, token, tls, "")
}

// newMiniBrokerWithMode is newMiniBroker's poll-mode-aware counterpart, used
// by the two topology tests parameterized over batch/stream.
func newMiniBrokerWithMode(t *testing.T, token string, tls bool, mode string) *miniBroker {
	t.Helper()

	b := &miniBroker{
		store:     NewSessionStore(),
		providers: make(map[string]*yamux.Session),
		consumers: make(map[string]map[string]*yamux.Session),
		authSeen:  make(map[string][]string),
		cfg: ServerConfig{
			PollTimeout:    400 * time.Millisecond,
			SessionTimeout: 800 * time.Millisecond,
			SweepInterval:  50 * time.Millisecond,
			CoalesceWindow: 2 * time.Millisecond,
			PollBufferSize: 256 << 10,
			MaxSendBytes:   256 << 10,
			PollMode:       mode,
			// An application on gorilla/mux never gets the id from
			// PathValue. Extract it the way mux.Vars would.
			SessionIDFunc: func(r *http.Request) string {
				parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
				if len(parts) < 2 {
					return ""
				}
				return parts[1]
			},
		},
	}
	if mode == PollModeStream {
		b.cfg.HeartbeatInterval = 100 * time.Millisecond
		b.cfg.StreamMaxDuration = 400 * time.Millisecond
	}

	b.hooks = Hooks{
		Authenticate: func(_ *http.Request, req ConnectRequest) (map[string]string, error) {
			role, endpoint := req.Meta["role"], req.Meta["endpoint"]
			if role != "consumer" && role != "provider" {
				return nil, StatusErrorf(http.StatusBadRequest, "role must be 'consumer' or 'provider'")
			}
			if endpoint == "" {
				return nil, StatusErrorf(http.StatusBadRequest, "missing endpoint")
			}
			return nil, nil
		},
		OnConnect: func(s *Session, meta map[string]string) error {
			bs := &brokerSession{Session: s, Role: meta["role"], Endpoint: meta["endpoint"]}
			if bs.Role == "provider" {
				go b.handleProvider(bs)
			} else {
				go b.handleConsumer(bs)
			}
			return nil
		},
		OnDisconnect: func(*Session, DisconnectReason) {},
	}

	mux := http.NewServeMux()
	// Every route behind auth, which is the realistic arrangement — and why the
	// client has to authenticate polls and deletes too, not only connect.
	wrap := func(h http.Handler) http.Handler {
		return b.recordAuth(authMiddleware(token, h))
	}
	mux.Handle("/tunnel/connect", wrap(ConnectHandler(b.store, b.cfg, b.hooks)))
	mux.Handle("/tunnel/{id}/poll", wrap(PollHandler(b.store, b.cfg, b.hooks)))
	mux.Handle("/tunnel/{id}", wrap(DeleteHandler(b.store, b.cfg, b.hooks)))

	if tls {
		b.ts = httptest.NewTLSServer(mux)
	} else {
		b.ts = httptest.NewServer(mux)
	}
	t.Cleanup(b.ts.Close)
	return b
}

// recordAuth notes the Authorization header before auth runs, keyed by the kind
// of request.
func (b *miniBroker) recordAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kind := "other"
		switch {
		case strings.HasSuffix(r.URL.Path, "/connect"):
			kind = "connect"
		case strings.HasSuffix(r.URL.Path, "/poll"):
			if r.Header.Get(HeaderSendOnly) == "true" {
				kind = "send"
			} else {
				kind = "poll"
			}
		case r.Method == http.MethodDelete:
			kind = "delete"
		}
		b.authMu.Lock()
		b.authSeen[kind] = append(b.authSeen[kind], r.Header.Get("Authorization"))
		b.authMu.Unlock()
		next.ServeHTTP(w, r)
	})
}

func (b *miniBroker) authFor(kind string) []string {
	b.authMu.Lock()
	defer b.authMu.Unlock()
	out := make([]string, len(b.authSeen[kind]))
	copy(out, b.authSeen[kind])
	return out
}

// handleProvider: the broker is the yamux client toward a provider, and when
// the provider leaves it closes every consumer session on that endpoint so
// they notice at once.
func (b *miniBroker) handleProvider(bs *brokerSession) {
	sess, err := ClientSession(bs.Session)
	if err != nil {
		return
	}
	defer func() {
		sess.Close()
		b.removeProvider(bs.Endpoint)
	}()

	b.mu.Lock()
	b.providers[bs.Endpoint] = sess
	b.mu.Unlock()

	<-sess.CloseChan()
}

// handleConsumer: the broker is the yamux server toward a consumer, and
// bridges each accepted stream to the provider.
func (b *miniBroker) handleConsumer(bs *brokerSession) {
	sess, err := ServerSession(bs.Session)
	if err != nil {
		return
	}

	b.mu.Lock()
	if b.consumers[bs.Endpoint] == nil {
		b.consumers[bs.Endpoint] = make(map[string]*yamux.Session)
	}
	b.consumers[bs.Endpoint][bs.ID] = sess
	b.mu.Unlock()

	defer func() {
		sess.Close()
		b.mu.Lock()
		delete(b.consumers[bs.Endpoint], bs.ID)
		b.mu.Unlock()
	}()

	done := sess.CloseChan()
	for {
		stream, err := sess.Accept()
		if err != nil {
			return
		}
		go b.bridge(stream, bs.Endpoint, done)
	}
}

// removeProvider drops the provider and closes every consumer yamux session on
// that endpoint, which is how a relay makes consumers reconnect promptly when
// a provider goes away.
func (b *miniBroker) removeProvider(endpoint string) {
	b.mu.Lock()
	delete(b.providers, endpoint)
	victims := make([]*yamux.Session, 0, len(b.consumers[endpoint]))
	for _, s := range b.consumers[endpoint] {
		victims = append(victims, s)
	}
	b.mu.Unlock()

	for _, s := range victims {
		s.Close()
	}
}

func (b *miniBroker) providerYamux(endpoint string) (*yamux.Session, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.providers[endpoint]
	return s, ok
}

// bridge splices a consumer stream to a provider stream, including a one-byte
// length-prefixed address header. That framing belongs to the application; it
// is reproduced here only so the bridge is realistic.
func (b *miniBroker) bridge(consumerStream net.Conn, endpoint string, done <-chan struct{}) {
	defer consumerStream.Close()

	addr, err := readAddrHeader(consumerStream)
	if err != nil {
		return
	}

	providerSess, ok := waitForProvider(b, endpoint, done, 5*time.Second)
	if !ok {
		return
	}

	providerStream, err := providerSess.Open()
	if err != nil {
		return
	}
	defer providerStream.Close()

	if err := writeAddrHeader(providerStream, addr); err != nil {
		return
	}

	var wg sync.WaitGroup
	wg.Go(func() { io.Copy(providerStream, consumerStream) })
	wg.Go(func() { io.Copy(consumerStream, providerStream) })
	wg.Wait()
}

func waitForProvider(b *miniBroker, endpoint string, done <-chan struct{}, timeout time.Duration) (*yamux.Session, bool) {
	deadline := time.After(timeout)
	for {
		if s, ok := b.providerYamux(endpoint); ok {
			return s, true
		}
		select {
		case <-done:
			return nil, false
		case <-deadline:
			return nil, false
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func readAddrHeader(r io.Reader) (string, error) {
	var lenBuf [1]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return "", err
	}
	addr := make([]byte, lenBuf[0])
	if _, err := io.ReadFull(r, addr); err != nil {
		return "", err
	}
	return string(addr), nil
}

func writeAddrHeader(w io.Writer, addr string) error {
	buf := make([]byte, 1+len(addr))
	buf[0] = byte(len(addr))
	copy(buf[1:], addr)
	_, err := w.Write(buf)
	return err
}

// ---------------------------------------------------------------------------
// the provider client
// ---------------------------------------------------------------------------

// miniProvider is the provider-side shape: ReconnectLoop around ServerSession
// plus AcceptLoop, with each accepted stream dialled out to a target.
type miniProvider struct {
	outcomes chan Outcome
	sessions chan string
}

func runMiniProvider(t *testing.T, ctx context.Context, brokerURL, token, endpoint string, insecure bool) *miniProvider {
	return runMiniProviderMode(t, ctx, brokerURL, token, endpoint, insecure, false)
}

// runMiniProviderMode is runMiniProvider's poll-mode-aware counterpart.
func runMiniProviderMode(t *testing.T, ctx context.Context, brokerURL, token, endpoint string, insecure, preferStream bool) *miniProvider {
	t.Helper()
	p := &miniProvider{
		outcomes: make(chan Outcome, 8),
		sessions: make(chan string, 8),
	}

	loop := &ReconnectLoop{
		Connect: func(ctx context.Context) (Conn, error) {
			c := &Connector{
				BaseURL:            brokerURL,
				AuthToken:          token,
				Meta:               map[string]string{"role": "provider", "endpoint": endpoint},
				PollGrace:          2 * time.Second,
				InsecureSkipVerify: insecure,
				PreferStream:       preferStream,
			}
			return c.Connect(ctx)
		},
		Serve: func(ctx context.Context, conn Conn) Outcome {
			p.sessions <- conn.SessionID()
			// A provider accepts streams, so it is the yamux server.
			sess, err := ServerSession(conn)
			if err != nil {
				return OutcomeTransportFailed
			}
			defer sess.Close()
			out := AcceptLoop(ctx, sess, conn, providerServeStream)
			p.outcomes <- out
			return out
		},
		InitialBackoff:  20 * time.Millisecond,
		MaxBackoff:      200 * time.Millisecond,
		PeerClosedPause: 10 * time.Millisecond,
	}
	go loop.Run(ctx)
	return p
}

// providerServeStream reads the address, dials it, and splices.
func providerServeStream(stream net.Conn) {
	defer stream.Close()

	addr, err := readAddrHeader(stream)
	if err != nil {
		return
	}
	target, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return
	}
	defer target.Close()

	var wg sync.WaitGroup
	wg.Go(func() { io.Copy(target, stream) })
	wg.Go(func() { io.Copy(stream, target) })
	wg.Wait()
}

// ---------------------------------------------------------------------------
// the consumer client
// ---------------------------------------------------------------------------

// miniConsumer is the consumer-side shape, including the part that must not be
// simplified away: after the yamux session closes, re-check TransportFailed to
// tell "the broker is gone" from "the broker closed my session because the
// provider left". Those want opposite reconnect behaviour.
type miniConsumer struct {
	mu   sync.Mutex
	sess *yamux.Session

	outcomes chan Outcome
	sessions chan string
	ready    chan struct{}
}

func runMiniConsumer(t *testing.T, ctx context.Context, brokerURL, token, endpoint string, insecure bool) *miniConsumer {
	return runMiniConsumerMode(t, ctx, brokerURL, token, endpoint, insecure, false)
}

// runMiniConsumerMode is runMiniConsumer's poll-mode-aware counterpart.
func runMiniConsumerMode(t *testing.T, ctx context.Context, brokerURL, token, endpoint string, insecure, preferStream bool) *miniConsumer {
	t.Helper()
	c := &miniConsumer{
		outcomes: make(chan Outcome, 8),
		sessions: make(chan string, 8),
		ready:    make(chan struct{}, 8),
	}

	loop := &ReconnectLoop{
		Connect: func(ctx context.Context) (Conn, error) {
			conn := &Connector{
				BaseURL:            brokerURL,
				AuthToken:          token,
				Meta:               map[string]string{"role": "consumer", "endpoint": endpoint},
				PollGrace:          2 * time.Second,
				InsecureSkipVerify: insecure,
				PreferStream:       preferStream,
			}
			return conn.Connect(ctx)
		},
		Serve: func(ctx context.Context, conn Conn) Outcome {
			// A consumer opens streams, so it is the yamux client.
			sess, err := ClientSession(conn)
			if err != nil {
				return OutcomeTransportFailed
			}
			defer sess.Close()

			c.setSession(sess)
			defer c.setSession(nil)
			c.sessions <- conn.SessionID()
			select {
			case c.ready <- struct{}{}:
			default:
			}

			out := c.waitForEnd(ctx, conn, sess)
			c.outcomes <- out
			return out
		},
		InitialBackoff:  20 * time.Millisecond,
		MaxBackoff:      200 * time.Millisecond,
		PeerClosedPause: 10 * time.Millisecond,
	}
	go loop.Run(ctx)
	return c
}

// waitForEnd is the four-way wait a consumer needs.
func (c *miniConsumer) waitForEnd(ctx context.Context, conn Conn, sess *yamux.Session) Outcome {
	select {
	case <-ctx.Done():
		return OutcomeShutdown

	case <-conn.TransportFailed():
		return OutcomeTransportFailed

	case <-sess.CloseChan():
		// ★ The re-check. A yamux close alone does not say which happened.
		select {
		case <-conn.TransportFailed():
			return OutcomeTransportFailed
		default:
			return OutcomePeerClosed
		}
	}
}

func (c *miniConsumer) setSession(s *yamux.Session) {
	c.mu.Lock()
	c.sess = s
	c.mu.Unlock()
}

func (c *miniConsumer) session() *yamux.Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sess
}

// dial opens a stream to target through the tunnel, the way the SOCKS5 dialer
// does.
func (c *miniConsumer) dial(t *testing.T, target string) net.Conn {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		sess := c.session()
		if sess == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		stream, err := sess.Open()
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err := writeAddrHeader(stream, target); err != nil {
			stream.Close()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return stream
	}
	t.Fatal("consumer never managed to open a tunnelled stream")
	return nil
}

func (c *miniConsumer) waitReady(t *testing.T) {
	t.Helper()
	select {
	case <-c.ready:
	case <-time.After(10 * time.Second):
		t.Fatal("consumer never established a session")
	}
}

// ---------------------------------------------------------------------------
// a target service to tunnel to
// ---------------------------------------------------------------------------

// echoTarget is the service behind the provider, which dials arbitrary
// addresses; here it dials this one.
func echoTarget(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				io.Copy(conn, conn)
			}()
		}
	}()
	return ln.Addr().String()
}

// ---------------------------------------------------------------------------
// the tests
// ---------------------------------------------------------------------------

// The whole topology: bytes travel consumer → broker → provider → target and
// back, over two independent pollmux tunnels with yamux on each.
func TestBrokerTopologyEndToEnd(t *testing.T)        { testBrokerTopologyEndToEnd(t, "") }
func TestBrokerTopologyEndToEnd_Stream(t *testing.T) { testBrokerTopologyEndToEnd(t, PollModeStream) }

func testBrokerTopologyEndToEnd(t *testing.T, mode string) {
	const token = "broker-token"
	target := echoTarget(t)
	b := newMiniBrokerWithMode(t, token, false, mode)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runMiniProviderMode(t, ctx, b.ts.URL, token, "home", false, mode == PollModeStream)
	c := runMiniConsumerMode(t, ctx, b.ts.URL, token, "home", false, mode == PollModeStream)
	c.waitReady(t)

	stream := c.dial(t, target)
	defer stream.Close()

	payload := []byte("hello through the whole chain")
	if _, err := stream.Write(payload); err != nil {
		t.Fatalf("write through tunnel: %v", err)
	}

	got := make([]byte, len(payload))
	stream.SetReadDeadline(time.Now().Add(15 * time.Second))
	if _, err := io.ReadFull(stream, got); err != nil {
		t.Fatalf("read through tunnel: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("got %q back, want %q", got, payload)
	}
}

// Megabytes through the full three-role chain, hashed. This is the case that
// exercises chunking, the poll buffer, and yamux flow control together.
func TestBrokerTopologyBulkTransfer(t *testing.T) {
	testBrokerTopologyBulkTransfer(t, "")
}
func TestBrokerTopologyBulkTransfer_Stream(t *testing.T) {
	testBrokerTopologyBulkTransfer(t, PollModeStream)
}

func testBrokerTopologyBulkTransfer(t *testing.T, mode string) {
	const token = "broker-token"
	target := echoTarget(t)
	b := newMiniBrokerWithMode(t, token, false, mode)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runMiniProviderMode(t, ctx, b.ts.URL, token, "home", false, mode == PollModeStream)
	c := runMiniConsumerMode(t, ctx, b.ts.URL, token, "home", false, mode == PollModeStream)
	c.waitReady(t)

	stream := c.dial(t, target)
	defer stream.Close()

	payload := make([]byte, 1<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	want := sha256.Sum256(payload)

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
		t.Fatalf("write through tunnel: %v", err)
	}

	select {
	case got := <-readDone:
		if got == nil {
			t.Fatal("bulk transfer came back short")
		}
		if sha256.Sum256(got) != want {
			t.Fatal("bulk transfer corrupted somewhere in the chain")
		}
	case <-time.After(90 * time.Second):
		t.Fatal("bulk transfer never completed")
	}
}

// ★ An application that puts every route behind auth would see a client that
// only authenticates connect get 401 on its first poll and never work. Nothing
// else in the suite proves polls and deletes carry the token.
func TestBrokerAuthenticatesEveryRequestKind(t *testing.T) {
	const token = "broker-token"
	b := newMiniBroker(t, token, false)

	c := &Connector{
		BaseURL:   b.ts.URL,
		AuthToken: token,
		Meta:      map[string]string{"role": "provider", "endpoint": "home"},
		PollGrace: 2 * time.Second,
	}
	conn, err := c.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Force at least one poll and one send.
	waitFor(t, 5*time.Second, func() bool { return len(b.authFor("poll")) >= 1 })
	if _, err := conn.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return len(b.authFor("send")) >= 1 })

	// And a delete.
	conn.Close()
	waitFor(t, 5*time.Second, func() bool { return len(b.authFor("delete")) >= 1 })

	want := "Bearer " + token
	for _, kind := range []string{"connect", "poll", "send", "delete"} {
		seen := b.authFor(kind)
		if len(seen) == 0 {
			t.Fatalf("no %s request was ever made", kind)
		}
		for i, got := range seen {
			if got != want {
				t.Fatalf("%s request %d carried Authorization %q, want %q — "+
					"every route can be behind auth", kind, i, got, want)
			}
		}
	}
}

// The negative half: a wrong token must fail the session rather than quietly
// spin, so the operator sees it.
func TestBrokerRejectsWrongTokenOnConnect(t *testing.T) {
	b := newMiniBroker(t, "right-token", false)

	c := &Connector{
		BaseURL:   b.ts.URL,
		AuthToken: "wrong-token",
		Meta:      map[string]string{"role": "provider", "endpoint": "home"},
		PollGrace: 500 * time.Millisecond,
	}
	if _, err := c.Connect(context.Background()); err == nil {
		t.Fatal("Connect succeeded with the wrong token")
	}
}

// ★ The peer-closed-versus-transport-failed distinction, measured rather than
// assumed — and in this topology the answer is not the intuitive one.
//
// The broker's only lever for evicting a consumer when its provider leaves is
// yamux.Session.Close(), and that closes the underlying conn too
// (yamux@v0.1.2/session.go:289 — GoAway is the one that does not). The
// underlying conn here *is* the pollmux Session, so closing the yamux layer
// tears the whole tunnel down. The consumer's parked poll then answers 410 and
// the client reports transport failure.
//
// So a provider departure is NOT observable as OutcomePeerClosed in this
// topology. The re-check is still correct code — it just cannot fire here,
// because there is no path that closes the yamux session while leaving the
// tunnel alive. An application that needs that distinction has to signal it
// itself, above the transport.
//
// What matters operationally is that the consumer finds out fast, and it does.
// Note how much rests on 410 here: were EOF answered as 204, the consumer's
// read pipe would never close, its local yamux would never see an end, and it
// would keep polling a dead session until the sweeper evicted it.
func TestConsumerRecoversPromptlyFromProviderDeparture(t *testing.T) {
	const token = "broker-token"
	b := newMiniBroker(t, token, false)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	providerCtx, stopProvider := context.WithCancel(ctx)
	runMiniProvider(t, providerCtx, b.ts.URL, token, "home", false)

	c := runMiniConsumer(t, ctx, b.ts.URL, token, "home", false)
	c.waitReady(t)
	firstSession := <-c.sessions

	// Wait until the broker actually has the provider registered, so the
	// departure below is a real transition.
	waitFor(t, 10*time.Second, func() bool {
		_, ok := b.providerYamux("home")
		return ok
	})

	start := time.Now()
	stopProvider()

	select {
	case out := <-c.outcomes:
		if out != OutcomeTransportFailed {
			t.Fatalf("consumer classified a provider departure as %v; in this topology the broker "+
				"cannot close the yamux layer without closing the tunnel, so transport failure is "+
				"the expected signal", out)
		}
		// The whole point: seconds, not a session_timeout.
		if elapsed := time.Since(start); elapsed > 3*time.Second {
			t.Fatalf("consumer took %v to notice the provider had left", elapsed)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("consumer never noticed the provider had left")
	}

	// And it comes back on its own, with a fresh session.
	second := waitForNewSession(t, c.sessions, firstSession)
	if second == firstSession {
		t.Fatal("consumer reused the dead session id")
	}
}

// The classification logic itself still has to be right, since it is what tells
// a broker outage from a peer departure wherever both are possible. Exercised
// directly against a Conn whose transport is healthy.
func TestConsumerReCheckClassifiesHealthyTunnelAsPeerClosed(t *testing.T) {
	a, bb := net.Pipe()
	connA, connB := newFakeConn(a), newFakeConn(bb)

	sess, err := ClientSession(connA)
	if err != nil {
		t.Fatalf("ClientSession: %v", err)
	}
	defer sess.Close()

	peer, err := ServerSession(connB)
	if err != nil {
		t.Fatalf("ServerSession: %v", err)
	}

	c := &miniConsumer{outcomes: make(chan Outcome, 1)}
	result := make(chan Outcome, 1)
	go func() {
		result <- c.waitForEnd(context.Background(), connA, sess)
	}()

	time.Sleep(30 * time.Millisecond)
	peer.Close() // the peer goes; connA never reports transport failure

	select {
	case out := <-result:
		if out != OutcomePeerClosed {
			t.Fatalf("re-check returned %v with a healthy transport, want OutcomePeerClosed", out)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the consumer's wait never returned")
	}
}

// The other half of the same distinction: the broker itself disappearing is a
// transport failure, and must be classified as one so the client backs off.
func TestConsumerSeesBrokerLossAsTransportFailed(t *testing.T) {
	const token = "broker-token"
	b := newMiniBroker(t, token, false)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := runMiniConsumer(t, ctx, b.ts.URL, token, "home", false)
	c.waitReady(t)

	// The broker goes away entirely.
	b.ts.Close()

	select {
	case out := <-c.outcomes:
		if out != OutcomeTransportFailed {
			t.Fatalf("consumer classified broker loss as %v, want OutcomeTransportFailed", out)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("consumer never noticed the broker was gone")
	}
}

// A graceful broker shutdown closes every session, so clients find out in
// seconds instead of waiting out session_timeout. This is Server.Stop's path.
func TestBrokerGracefulShutdownIsNoticedByBothRoles(t *testing.T) {
	const token = "broker-token"
	b := newMiniBroker(t, token, false)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runMiniProvider(t, ctx, b.ts.URL, token, "home", false)
	c := runMiniConsumer(t, ctx, b.ts.URL, token, "home", false)
	c.waitReady(t)

	waitFor(t, 10*time.Second, func() bool { return b.store.Len() >= 2 })

	// Server.Stop: close every live session.
	start := time.Now()
	for _, s := range b.store.All() {
		CloseSession(b.store, b.hooks, s, ReasonServerClose)
	}

	select {
	case <-c.outcomes:
		if elapsed := time.Since(start); elapsed > 3*time.Second {
			t.Fatalf("took %v for the consumer to notice a graceful shutdown", elapsed)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("consumer never noticed the graceful shutdown")
	}
}

// Both roles must come back on their own after the broker drops them, and with
// a fresh session id — reusing the old one would feed stale yamux frames into a
// new session.
func TestBothRolesReconnectWithFreshSessions(t *testing.T) {
	const token = "broker-token"
	target := echoTarget(t)
	b := newMiniBroker(t, token, false)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	p := runMiniProvider(t, ctx, b.ts.URL, token, "home", false)
	c := runMiniConsumer(t, ctx, b.ts.URL, token, "home", false)
	c.waitReady(t)

	firstProvider := <-p.sessions
	firstConsumer := <-c.sessions

	waitFor(t, 10*time.Second, func() bool { return b.store.Len() >= 2 })
	for _, s := range b.store.All() {
		CloseSession(b.store, b.hooks, s, ReasonServerClose)
	}

	secondProvider := waitForNewSession(t, p.sessions, firstProvider)
	secondConsumer := waitForNewSession(t, c.sessions, firstConsumer)

	if secondProvider == firstProvider {
		t.Fatal("provider reused its old session id")
	}
	if secondConsumer == firstConsumer {
		t.Fatal("consumer reused its old session id")
	}

	// And the tunnel actually works again afterwards.
	c.waitReady(t)
	stream := c.dial(t, target)
	defer stream.Close()

	if _, err := stream.Write([]byte("after reconnect")); err != nil {
		t.Fatalf("write after reconnect: %v", err)
	}
	got := make([]byte, len("after reconnect"))
	stream.SetReadDeadline(time.Now().Add(20 * time.Second))
	if _, err := io.ReadFull(stream, got); err != nil {
		t.Fatalf("read after reconnect: %v", err)
	}
	if string(got) != "after reconnect" {
		t.Fatalf("got %q after reconnect, want %q", got, "after reconnect")
	}
}

func waitForNewSession(t *testing.T, ch chan string, old string) string {
	t.Helper()
	deadline := time.After(20 * time.Second)
	for {
		select {
		case id := <-ch:
			if id != old {
				return id
			}
		case <-deadline:
			t.Fatal("no new session was established")
			return ""
		}
	}
}

// Deployments run with TLS, and some with self-signed certs, so the whole
// chain has to work over HTTPS too.
func TestBrokerTopologyOverTLS(t *testing.T) {
	const token = "broker-token"
	target := echoTarget(t)
	b := newMiniBroker(t, token, true)

	if !strings.HasPrefix(b.ts.URL, "https://") {
		t.Fatalf("test server URL %q is not HTTPS", b.ts.URL)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runMiniProvider(t, ctx, b.ts.URL, token, "home", true)
	c := runMiniConsumer(t, ctx, b.ts.URL, token, "home", true)
	c.waitReady(t)

	stream := c.dial(t, target)
	defer stream.Close()

	if _, err := stream.Write([]byte("over tls")); err != nil {
		t.Fatalf("write over TLS: %v", err)
	}
	got := make([]byte, len("over tls"))
	stream.SetReadDeadline(time.Now().Add(15 * time.Second))
	if _, err := io.ReadFull(stream, got); err != nil {
		t.Fatalf("read over TLS: %v", err)
	}
	if string(got) != "over tls" {
		t.Fatalf("got %q over TLS, want %q", got, "over tls")
	}
}

// Without InsecureSkipVerify a self-signed broker must be refused, or the flag
// would be doing nothing.
func TestTLSVerificationIsOnByDefault(t *testing.T) {
	b := newMiniBroker(t, "broker-token", true)

	c := &Connector{
		BaseURL:   b.ts.URL,
		AuthToken: "broker-token",
		Meta:      map[string]string{"role": "provider", "endpoint": "home"},
		PollGrace: 500 * time.Millisecond,
		// InsecureSkipVerify deliberately left false.
	}
	if _, err := c.Connect(context.Background()); err == nil {
		t.Fatal("Connect accepted a self-signed certificate without InsecureSkipVerify")
	}
}

// Several endpoints and several consumers on one broker: the multi-tenant
// shape.
func TestBrokerHandlesMultipleEndpointsAndConsumers(t *testing.T) {
	const token = "broker-token"
	b := newMiniBroker(t, token, false)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	targets := map[string]string{
		"alpha": echoTarget(t),
		"beta":  echoTarget(t),
	}
	consumers := make(map[string]*miniConsumer)

	for endpoint := range targets {
		runMiniProvider(t, ctx, b.ts.URL, token, endpoint, false)
	}
	for endpoint := range targets {
		c := runMiniConsumer(t, ctx, b.ts.URL, token, endpoint, false)
		c.waitReady(t)
		consumers[endpoint] = c
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(targets))
	for endpoint, target := range targets {
		wg.Go(func() {
			stream := consumers[endpoint].dial(t, target)
			defer stream.Close()

			msg := "traffic for " + endpoint
			if _, err := stream.Write([]byte(msg)); err != nil {
				errCh <- fmt.Errorf("%s: write: %w", endpoint, err)
				return
			}
			got := make([]byte, len(msg))
			stream.SetReadDeadline(time.Now().Add(20 * time.Second))
			if _, err := io.ReadFull(stream, got); err != nil {
				errCh <- fmt.Errorf("%s: read: %w", endpoint, err)
				return
			}
			if string(got) != msg {
				errCh <- fmt.Errorf("%s: got %q, want %q", endpoint, got, msg)
			}
		})
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

// Many streams at once through one tunnel, which is the case yamux
// multiplexing exists for and the one a browser produces.
func TestBrokerConcurrentStreamsThroughOneTunnel(t *testing.T) {
	const token = "broker-token"
	const streams = 12
	target := echoTarget(t)
	b := newMiniBroker(t, token, false)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runMiniProvider(t, ctx, b.ts.URL, token, "home", false)
	c := runMiniConsumer(t, ctx, b.ts.URL, token, "home", false)
	c.waitReady(t)

	var wg sync.WaitGroup
	errCh := make(chan error, streams)
	for i := range streams {
		wg.Go(func() {
			stream := c.dial(t, target)
			defer stream.Close()

			msg := fmt.Sprintf("stream-%02d", i)
			if _, err := stream.Write([]byte(msg)); err != nil {
				errCh <- fmt.Errorf("stream %d write: %w", i, err)
				return
			}
			got := make([]byte, len(msg))
			stream.SetReadDeadline(time.Now().Add(30 * time.Second))
			if _, err := io.ReadFull(stream, got); err != nil {
				errCh <- fmt.Errorf("stream %d read: %w", i, err)
				return
			}
			if string(got) != msg {
				errCh <- fmt.Errorf("stream %d: got %q, want %q — streams crossed", i, got, msg)
			}
		})
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

// A consumer that arrives before any provider must have its stream held rather
// than failed — a relay holds the request until the provider shows up. What
// pollmux has to supply is a tunnel that stays alive while that wait
// happens.
func TestConsumerBeforeProviderIsHeldNotFailed(t *testing.T) {
	const token = "broker-token"
	target := echoTarget(t)
	b := newMiniBroker(t, token, false)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := runMiniConsumer(t, ctx, b.ts.URL, token, "home", false)
	c.waitReady(t)

	stream := c.dial(t, target)
	defer stream.Close()

	// Write before any provider exists; the bridge parks waiting for one.
	if _, err := stream.Write([]byte("early bird")); err != nil {
		t.Fatalf("write before provider: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	runMiniProvider(t, ctx, b.ts.URL, token, "home", false)

	got := make([]byte, len("early bird"))
	stream.SetReadDeadline(time.Now().Add(20 * time.Second))
	if _, err := io.ReadFull(stream, got); err != nil {
		t.Fatalf("read after the provider arrived: %v", err)
	}
	if string(got) != "early bird" {
		t.Fatalf("got %q, want %q", got, "early bird")
	}
}

// D10: the default poll interval is zero, so the client re-polls immediately
// instead of adding latency to every exchange.
func TestPollIntervalDefaultsToZero(t *testing.T) {
	if DefaultPollInterval != 0 {
		t.Fatalf("DefaultPollInterval = %v, want 0", DefaultPollInterval)
	}

	b := newMiniBroker(t, "broker-token", false)
	c := &Connector{
		BaseURL:   b.ts.URL,
		AuthToken: "broker-token",
		Meta:      map[string]string{"role": "provider", "endpoint": "home"},
		PollGrace: 2 * time.Second,
		// PollInterval left at its zero value.
	}
	conn, err := c.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	s, ok := b.store.Get(conn.SessionID())
	if !ok {
		t.Fatal("session missing from the store")
	}

	// With no added interval a poll is parked essentially all the time.
	waitFor(t, 3*time.Second, func() bool { return s.PollInFlight() > 0 })
}

// A status endpoint reads the store directly. It has to see live sessions and
// stop seeing them once they end.
func TestStoreReflectsLiveSessionsForStatus(t *testing.T) {
	const token = "broker-token"
	b := newMiniBroker(t, token, false)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runMiniProvider(t, ctx, b.ts.URL, token, "home", false)
	c := runMiniConsumer(t, ctx, b.ts.URL, token, "home", false)
	c.waitReady(t)

	waitFor(t, 10*time.Second, func() bool { return b.store.Len() == 2 })

	roles := map[string]int{}
	for _, s := range b.store.All() {
		roles[s.Meta()["role"]]++
	}
	if roles["provider"] != 1 || roles["consumer"] != 1 {
		t.Fatalf("store shows roles %v, want one provider and one consumer", roles)
	}

	cancel()
	waitFor(t, 15*time.Second, func() bool { return b.store.Len() == 0 })
}
