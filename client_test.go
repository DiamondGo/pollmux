package pollmux

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"
)

// fakeServer speaks just enough of the protocol to exercise the client without
// depending on the real handlers, so a bug on one side cannot mask a bug on the
// other.
type fakeServer struct {
	ts     *httptest.Server
	prefix string

	limits    Limits
	sessionID string

	connectStatus int               // 0 means 200
	connectBody   string            // raw override; empty means marshal a ConnectResponse
	connectHeader map[string]string // extra response headers, e.g. Location
	connectMeta   map[string]string

	pollStatus int           // if non-zero, receive-only polls answer with it
	pollHang   chan struct{} // if non-nil, receive-only polls block on it

	down chan []byte // payloads to hand back on the next poll

	streamMode  bool          // if true, connect negotiates PollModeStream when asked
	heartbeatMS int64         // Limits.HeartbeatIntervalMS to advertise
	streamMaxMS int64         // Limits.StreamMaxDurationMS to advertise
	streamPush  chan frameMsg // frames to write on the next stream poll
	streamHang  bool          // if true, write headers then never push/close (watchdog test)

	mu       sync.Mutex
	sends    [][]byte
	sendHdrs []http.Header
	pollHdrs []http.Header
	deletes  []string
	connects []ConnectRequest
}

// frameMsg is one entry fed through fakeServer.streamPush: a frame the fake
// server's stream-mode poll handler should write next.
type frameMsg struct {
	typ     frameType
	payload []byte
}

func (f *fakeServer) pushData(b []byte) { f.streamPush <- frameMsg{typ: frameData, payload: b} }
func (f *fakeServer) pushHeartbeat()    { f.streamPush <- frameMsg{typ: frameHeartbeat} }
func (f *fakeServer) pushEnd()          { f.streamPush <- frameMsg{typ: frameEnd} }

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	f := &fakeServer{
		prefix:    "/tunnel",
		sessionID: "sess-1",
		limits: Limits{
			MaxSendBytes:     1 << 20,
			PollTimeoutMS:    200,
			SessionTimeoutMS: 5000,
			PollBufferBytes:  256 << 10,
		},
		down:       make(chan []byte, 16),
		streamPush: make(chan frameMsg, 16),
	}
	f.ts = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.ts.Close)
	return f
}

func (f *fakeServer) serve(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, f.prefix)

	switch {
	case path == "/connect" && r.Method == http.MethodPost:
		f.serveConnect(w, r)
	case strings.HasSuffix(path, "/poll") && r.Method == http.MethodPost:
		f.servePoll(w, r)
	case r.Method == http.MethodDelete:
		f.mu.Lock()
		f.deletes = append(f.deletes, strings.TrimPrefix(path, "/"))
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeServer) serveConnect(w http.ResponseWriter, r *http.Request) {
	var req ConnectRequest
	json.NewDecoder(r.Body).Decode(&req)
	f.mu.Lock()
	f.connects = append(f.connects, req)
	f.mu.Unlock()

	for k, v := range f.connectHeader {
		w.Header().Set(k, v)
	}
	if f.connectStatus != 0 && f.connectStatus != http.StatusOK {
		w.WriteHeader(f.connectStatus)
		io.WriteString(w, f.connectBody)
		return
	}
	if f.connectBody != "" {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, f.connectBody)
		return
	}
	resp := ConnectResponse{
		ProtocolVersion: ProtocolVersion,
		SessionID:       f.sessionID,
		Limits:          f.limits,
		Meta:            f.connectMeta,
	}
	if f.streamMode && req.PreferStreamMode {
		resp.PollMode = PollModeStream
		resp.Limits.HeartbeatIntervalMS = f.heartbeatMS
		resp.Limits.StreamMaxDurationMS = f.streamMaxMS
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func (f *fakeServer) servePoll(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get(HeaderSendOnly) == "true" {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.sends = append(f.sends, body)
		f.sendHdrs = append(f.sendHdrs, r.Header.Clone())
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		return
	}

	f.mu.Lock()
	f.pollHdrs = append(f.pollHdrs, r.Header.Clone())
	f.mu.Unlock()

	if f.pollStatus != 0 {
		w.WriteHeader(f.pollStatus)
		return
	}
	if f.pollHang != nil {
		// Hold the request open without ever writing a header. This is the
		// silent blackhole A1 is about.
		select {
		case <-f.pollHang:
		case <-r.Context().Done():
		}
		return
	}

	if f.streamMode {
		f.serveStreamPoll(w, r)
		return
	}

	select {
	case data := <-f.down:
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	case <-time.After(f.limits.PollTimeout()):
		w.WriteHeader(http.StatusNoContent)
	case <-r.Context().Done():
	}
}

// serveStreamPoll mirrors pollStream's own contract (immediate header flush,
// then frames pushed as they become available) closely enough that a client
// test exercising it validates the same timing contract the real server
// makes, not a looser one.
func (f *fakeServer) serveStreamPoll(w http.ResponseWriter, r *http.Request) {
	flusher := w.(http.Flusher)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	if f.streamHang {
		<-r.Context().Done()
		return
	}

	for {
		select {
		case msg := <-f.streamPush:
			writeFrame(w, msg.typ, msg.payload)
			flusher.Flush()
			if msg.typ == frameEnd {
				return
			}
		case <-r.Context().Done():
			return
		}
	}
}

func (f *fakeServer) sentBodies() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]byte, len(f.sends))
	copy(out, f.sends)
	return out
}

func (f *fakeServer) pollCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pollHdrs)
}

func (f *fakeServer) connector() *Connector {
	return &Connector{BaseURL: f.ts.URL, PollGrace: 200 * time.Millisecond}
}

func mustConnect(t *testing.T, c *Connector) Conn {
	t.Helper()
	conn, err := c.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// The whole point of pushing limits down: the server is authoritative, and the
// client silently clamps itself rather than needing a matching config.
func TestConnectClampsToServerLimits(t *testing.T) {
	f := newFakeServer(t)
	f.limits.MaxSendBytes = 100

	c := f.connector()
	c.MaxSendChunk = 100000 // far more aggressive than the server allows
	conn := mustConnect(t, c)

	if got := conn.Limits().MaxSendBytes; got != 100 {
		t.Fatalf("Limits().MaxSendBytes = %d, want the server's 100", got)
	}

	// Prove the clamp is real, not just reported: 250 bytes must go out as
	// 100 + 100 + 50, never as one oversized request.
	if _, err := conn.Write(make([]byte, 250)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	waitFor(t, time.Second, func() bool { return len(f.sentBodies()) == 3 })

	var sizes []int
	for _, b := range f.sentBodies() {
		sizes = append(sizes, len(b))
	}
	want := []int{100, 100, 50}
	for i := range want {
		if sizes[i] != want[i] {
			t.Fatalf("chunk sizes = %v, want %v", sizes, want)
		}
	}
}

// A client whose own preference is stricter than the server's must keep it.
func TestConnectKeepsClientChunkWhenSmaller(t *testing.T) {
	f := newFakeServer(t)
	f.limits.MaxSendBytes = 1 << 20

	c := f.connector()
	c.MaxSendChunk = 64
	conn := mustConnect(t, c)

	if _, err := conn.Write(make([]byte, 150)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	waitFor(t, time.Second, func() bool { return len(f.sentBodies()) == 3 })

	for i, b := range f.sentBodies() {
		if len(b) > 64 {
			t.Fatalf("chunk %d is %d bytes, want the client's stricter 64", i, len(b))
		}
	}
}

// A3's failure mode as a startup error: a poll cycle that reaches the server's
// session timeout means a healthy client gets swept as dead.
func TestConnectRejectsOversizedPollInterval(t *testing.T) {
	f := newFakeServer(t)
	f.limits.PollTimeoutMS = 30000
	f.limits.SessionTimeoutMS = 60000

	c := f.connector()
	c.PollInterval = 40 * time.Second

	_, err := c.Connect(context.Background())
	if err == nil {
		t.Fatal("Connect succeeded with a poll cycle past the server's session timeout")
	}
	// The message has to be actionable — it must name the offending value and
	// what to do, or an operator cannot act on it.
	for _, want := range []string{"PollInterval", "session_timeout", "40s"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error message lacks %q, got: %v", want, err)
		}
	}
}

func TestConnectAcceptsPollIntervalWithinBudget(t *testing.T) {
	f := newFakeServer(t)
	f.limits.PollTimeoutMS = 200
	f.limits.SessionTimeoutMS = 5000

	c := f.connector()
	c.PollInterval = 50 * time.Millisecond

	conn, err := c.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect rejected a poll cycle well inside the budget: %v", err)
	}
	conn.Close()
}

func TestConnectVersionMismatchIsTerminal(t *testing.T) {
	f := newFakeServer(t)
	f.connectStatus = http.StatusUpgradeRequired
	f.connectBody = `{"error":"unsupported protocol_version"}`

	_, err := f.connector().Connect(context.Background())
	if !errors.Is(err, ErrProtocolVersion) {
		t.Fatalf("Connect error = %v, want it to wrap ErrProtocolVersion", err)
	}
}

func TestConnectUnauthorized(t *testing.T) {
	f := newFakeServer(t)
	f.connectStatus = http.StatusUnauthorized
	f.connectBody = `{"error":"bad token"}`

	_, err := f.connector().Connect(context.Background())
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Connect error = %v, want it to wrap ErrUnauthorized", err)
	}
}

// A server with an anti-scanning redirect answers 3xx to anyone who fails auth.
// Following it would turn a clear auth failure into a confusing parse error, so
// the client must see the redirect itself.
func TestConnectDoesNotFollowRedirect(t *testing.T) {
	var followed bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		followed = true
		io.WriteString(w, "<html>decoy</html>")
	}))
	defer target.Close()

	f := newFakeServer(t)
	f.connectStatus = http.StatusFound
	f.connectHeader = map[string]string{"Location": target.URL}

	_, err := f.connector().Connect(context.Background())
	if err == nil {
		t.Fatal("Connect succeeded against a redirecting server")
	}
	if followed {
		t.Fatal("client followed the redirect; the decoy page was fetched")
	}
	if !strings.Contains(err.Error(), "redirect") {
		t.Fatalf("error does not mention the redirect: %v", err)
	}
}

func TestConnectSendsVersionAndMeta(t *testing.T) {
	f := newFakeServer(t)
	c := f.connector()
	c.Meta = map[string]string{"role": "provider", "endpoint": "home"}
	mustConnect(t, c)

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.connects) != 1 {
		t.Fatalf("server saw %d connect requests, want 1", len(f.connects))
	}
	got := f.connects[0]
	if got.ProtocolVersion != ProtocolVersion {
		t.Fatalf("protocol_version = %d, want %d", got.ProtocolVersion, ProtocolVersion)
	}
	if got.Meta["role"] != "provider" || got.Meta["endpoint"] != "home" {
		t.Fatalf("meta = %v, want the role/endpoint we declared", got.Meta)
	}
}

func TestConnectExposesServerMeta(t *testing.T) {
	f := newFakeServer(t)
	f.connectMeta = map[string]string{"subdomain": "myapp"}

	conn := mustConnect(t, f.connector())
	if got := conn.Meta()["subdomain"]; got != "myapp" {
		t.Fatalf("Meta()[subdomain] = %q, want %q", got, "myapp")
	}
	if got := conn.SessionID(); got != "sess-1" {
		t.Fatalf("SessionID = %q, want %q", got, "sess-1")
	}
}

func TestConnectRejectsEmptySessionID(t *testing.T) {
	f := newFakeServer(t)
	f.connectBody = `{"protocol_version":1,"session_id":"","limits":{"max_send_bytes":1024,"poll_timeout_ms":100,"session_timeout_ms":1000}}`

	if _, err := f.connector().Connect(context.Background()); err == nil {
		t.Fatal("Connect accepted an empty session_id")
	}
}

func TestConnectRejectsUnusableLimits(t *testing.T) {
	f := newFakeServer(t)
	f.connectBody = `{"protocol_version":1,"session_id":"s","limits":{"max_send_bytes":0,"poll_timeout_ms":100,"session_timeout_ms":1000}}`

	if _, err := f.connector().Connect(context.Background()); err == nil {
		t.Fatal("Connect accepted max_send_bytes=0")
	}
}

// ★ A1's direct acceptance test. A silent blackhole produces no RST and no FIN,
// so with no response header timeout the poll hangs forever and the transport
// never reports failure — the case a transport with no timeouts cannot detect.
func TestUnresponsiveServerTriggersTransportFailed(t *testing.T) {
	f := newFakeServer(t)
	f.limits.PollTimeoutMS = 300
	f.limits.SessionTimeoutMS = 5000
	f.pollHang = make(chan struct{})
	defer close(f.pollHang)

	c := f.connector()
	c.PollGrace = 200 * time.Millisecond // budget: 300ms + 200ms = 500ms
	conn := mustConnect(t, c)

	start := time.Now()
	select {
	case <-conn.TransportFailed():
		elapsed := time.Since(start)
		// It must not fire early either: a poll legitimately withholds its
		// response header for the whole poll timeout.
		if elapsed < 250*time.Millisecond {
			t.Fatalf("gave up after %v, before the server's poll timeout of 300ms had elapsed", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a poll that never got a response header never reported transport failure")
	}
}

// 410 exists so this case is fast. Sharing 204 with the idle heartbeat is what
// used to leave a client polling a dead session until its own timeout (A5).
func TestPoll410TriggersTransportFailed(t *testing.T) {
	f := newFakeServer(t)
	f.pollStatus = http.StatusGone

	conn := mustConnect(t, f.connector())

	select {
	case <-conn.TransportFailed():
	case <-time.After(2 * time.Second):
		t.Fatal("410 did not report transport failure")
	}
}

func TestPollFatalStatuses(t *testing.T) {
	for _, status := range []int{
		http.StatusUnauthorized,
		http.StatusNotFound,
		http.StatusGone,
		http.StatusRequestEntityTooLarge,
		http.StatusFound,
		http.StatusBadGateway,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			f := newFakeServer(t)
			f.pollStatus = status

			conn := mustConnect(t, f.connector())
			select {
			case <-conn.TransportFailed():
			case <-time.After(2 * time.Second):
				t.Fatalf("status %d did not report transport failure", status)
			}
		})
	}
}

// 204 is the normal heartbeat and must never be mistaken for a failure.
func TestPoll204IsHeartbeatNotFailure(t *testing.T) {
	f := newFakeServer(t)
	f.limits.PollTimeoutMS = 50 // time out quickly, over and over

	conn := mustConnect(t, f.connector())

	waitFor(t, 3*time.Second, func() bool { return f.pollCount() >= 3 })

	select {
	case <-conn.TransportFailed():
		t.Fatal("repeated 204 heartbeats were treated as transport failure")
	default:
	}
}

func TestReadDeliversPolledData(t *testing.T) {
	f := newFakeServer(t)
	conn := mustConnect(t, f.connector())

	f.down <- []byte("downstream payload")

	dst := make([]byte, 64)
	done := make(chan struct{})
	var n int
	var err error
	go func() {
		n, err = conn.Read(dst)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Read never returned the polled data")
	}
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got := string(dst[:n]); got != "downstream payload" {
		t.Fatalf("Read = %q, want %q", got, "downstream payload")
	}
}

// The coalesce window is paid once per burst. yamux emits a frame header and
// body as two writes, and merging them into one request is the difference
// between one and two round trips per frame over real RTT.
func TestWriteCoalescesBurstIntoOneRequest(t *testing.T) {
	f := newFakeServer(t)
	c := f.connector()
	c.CoalesceWindow = 150 * time.Millisecond
	conn := mustConnect(t, c)

	conn.Write([]byte("aaa"))
	conn.Write([]byte("bbb"))
	conn.Write([]byte("ccc"))

	waitFor(t, 2*time.Second, func() bool { return len(f.sentBodies()) >= 1 })
	time.Sleep(300 * time.Millisecond) // let any straggler request land

	bodies := f.sentBodies()
	if len(bodies) != 1 {
		t.Fatalf("burst went out as %d requests, want 1 coalesced request", len(bodies))
	}
	if got := string(bodies[0]); got != "aaabbbccc" {
		t.Fatalf("coalesced body = %q, want %q", got, "aaabbbccc")
	}
}

func TestSendCarriesSendOnlyHeader(t *testing.T) {
	f := newFakeServer(t)
	conn := mustConnect(t, f.connector())

	conn.Write([]byte("x"))
	waitFor(t, 2*time.Second, func() bool { return len(f.sentBodies()) >= 1 })

	f.mu.Lock()
	defer f.mu.Unlock()
	if got := f.sendHdrs[0].Get(HeaderSendOnly); got != "true" {
		t.Fatalf("%s = %q, want %q", HeaderSendOnly, got, "true")
	}
}

func TestPollCarriesReceiveOnlyHeader(t *testing.T) {
	f := newFakeServer(t)
	mustConnect(t, f.connector())

	waitFor(t, 2*time.Second, func() bool { return f.pollCount() >= 1 })

	f.mu.Lock()
	defer f.mu.Unlock()
	if got := f.pollHdrs[0].Get(HeaderReceiveOnly); got != "true" {
		t.Fatalf("%s = %q, want %q", HeaderReceiveOnly, got, "true")
	}
}

func TestLocalHealthIsPiggybackedOnPolls(t *testing.T) {
	f := newFakeServer(t)
	f.limits.PollTimeoutMS = 50

	var healthy atomic32Bool
	healthy.Store(true)

	c := f.connector()
	c.LocalHealth = func() bool { return healthy.Load() }
	mustConnect(t, c)

	waitFor(t, 2*time.Second, func() bool { return f.pollCount() >= 1 })
	f.mu.Lock()
	first := f.pollHdrs[0].Get(HeaderLocalHealth)
	f.mu.Unlock()
	if first != "ok" {
		t.Fatalf("%s = %q on a healthy client, want %q", HeaderLocalHealth, first, "ok")
	}

	healthy.Store(false)
	before := f.pollCount()
	waitFor(t, 2*time.Second, func() bool { return f.pollCount() > before+1 })

	f.mu.Lock()
	last := f.pollHdrs[len(f.pollHdrs)-1].Get(HeaderLocalHealth)
	f.mu.Unlock()
	if last != "down" {
		t.Fatalf("%s = %q after the local service went down, want %q", HeaderLocalHealth, last, "down")
	}
}

func TestCloseSendsDelete(t *testing.T) {
	f := newFakeServer(t)
	conn, err := f.connector().Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.deletes) != 1 || f.deletes[0] != "sess-1" {
		t.Fatalf("server saw deletes %v, want [sess-1]", f.deletes)
	}
}

// Close must abort the poll in flight rather than wait it out. Without a
// cancellable request, shutting down mid-poll would block for a full poll
// timeout — 30 seconds with the defaults.
func TestCloseIsPromptDuringLongPoll(t *testing.T) {
	f := newFakeServer(t)
	f.limits.PollTimeoutMS = 10000 // a poll that would park for 10s
	f.limits.SessionTimeoutMS = 30000
	f.pollHang = make(chan struct{})
	defer close(f.pollHang)

	c := f.connector()
	c.PollGrace = 30 * time.Second // ensure no timeout races us to the answer
	conn, err := c.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}

	waitFor(t, 2*time.Second, func() bool { return f.pollCount() >= 1 })

	start := time.Now()
	conn.Close()
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Close took %v while a poll was parked; it must cancel the request, not wait it out", elapsed)
	}
}

func TestWriteAfterCloseFails(t *testing.T) {
	f := newFakeServer(t)
	conn, err := f.connector().Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	conn.Close()

	if _, err := conn.Write([]byte("x")); err != io.ErrClosedPipe {
		t.Fatalf("Write after Close = %v, want io.ErrClosedPipe", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	f := newFakeServer(t)
	conn, err := f.connector().Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestConnectRequiresBaseURL(t *testing.T) {
	c := &Connector{}
	if _, err := c.Connect(context.Background()); err == nil {
		t.Fatal("Connect succeeded with no BaseURL")
	}
}

func TestConnectHonoursContextCancellation(t *testing.T) {
	f := newFakeServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := f.connector().Connect(ctx); err == nil {
		t.Fatal("Connect succeeded with an already-cancelled context")
	}
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

// atomic32Bool is a tiny race-free bool for tests.
type atomic32Bool struct {
	mu sync.Mutex
	v  bool
}

func (b *atomic32Bool) Store(v bool) {
	b.mu.Lock()
	b.v = v
	b.mu.Unlock()
}

func (b *atomic32Bool) Load() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.v
}

// --- frameReader unit tests -------------------------------------------------

func TestFrameReaderHandlesFramesSplitAcrossReads(t *testing.T) {
	var buf bytes.Buffer
	if err := writeFrame(&buf, frameData, []byte("hello")); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	if err := writeFrame(&buf, frameHeartbeat, nil); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	if err := writeFrame(&buf, frameData, []byte("world")); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}

	// One byte per Read call is the sharpest possible version of "a frame's
	// header and payload don't arrive together" — chunked transfer gives no
	// guarantee they will.
	fr := newFrameReader(iotest.OneByteReader(&buf), 1<<10)

	typ, payload, err := fr.next()
	if err != nil || typ != frameData || string(payload) != "hello" {
		t.Fatalf("frame 1 = (%v, %q, %v), want (frameData, %q, nil)", typ, payload, err, "hello")
	}
	typ, _, err = fr.next()
	if err != nil || typ != frameHeartbeat {
		t.Fatalf("frame 2 = (%v, %v), want (frameHeartbeat, nil)", typ, err)
	}
	typ, payload, err = fr.next()
	if err != nil || typ != frameData || string(payload) != "world" {
		t.Fatalf("frame 3 = (%v, %q, %v), want (frameData, %q, nil)", typ, payload, err, "world")
	}
	if _, _, err := fr.next(); !errors.Is(err, io.EOF) {
		t.Fatalf("next after last frame = %v, want io.EOF", err)
	}
}

func TestFrameReaderDistinguishesCleanEOFFromMidFrameEOF(t *testing.T) {
	t.Run("clean boundary", func(t *testing.T) {
		var buf bytes.Buffer
		writeFrame(&buf, frameHeartbeat, nil)
		fr := newFrameReader(&buf, 1<<10)
		if _, _, err := fr.next(); err != nil {
			t.Fatalf("first frame: %v", err)
		}
		if _, _, err := fr.next(); !errors.Is(err, io.EOF) {
			t.Fatalf("next at a clean boundary = %v, want io.EOF", err)
		}
	})
	t.Run("mid-header", func(t *testing.T) {
		fr := newFrameReader(bytes.NewReader([]byte{0x01, 0x00}), 1<<10)
		if _, _, err := fr.next(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("next mid-header = %v, want io.ErrUnexpectedEOF", err)
		}
	})
	t.Run("mid-payload", func(t *testing.T) {
		var buf bytes.Buffer
		writeFrame(&buf, frameData, []byte("hello"))
		truncated := buf.Bytes()[:frameHeaderLen+2] // header + 2 of 5 payload bytes
		fr := newFrameReader(bytes.NewReader(truncated), 1<<10)
		if _, _, err := fr.next(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("next mid-payload = %v, want io.ErrUnexpectedEOF", err)
		}
	})
}

func TestFrameReaderRejectsOversizedLength(t *testing.T) {
	var hdr [frameHeaderLen]byte
	hdr[0] = byte(frameData)
	binary.BigEndian.PutUint32(hdr[1:], 1000)
	fr := newFrameReader(bytes.NewReader(hdr[:]), 100)
	if _, _, err := fr.next(); err == nil {
		t.Fatal("next accepted a frame length above maxPayload without allocating")
	}
}

// --- client-side stream mode tests ------------------------------------------

func TestClientStreamModeDeliversDataWithoutWaitingForResponseEnd(t *testing.T) {
	f := newFakeServer(t)
	f.streamMode = true
	f.heartbeatMS = 5000
	f.streamMaxMS = 10000

	c := f.connector()
	c.PreferStream = true
	conn := mustConnect(t, c)

	f.pushData([]byte("a"))
	buf := make([]byte, 8)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(buf[:n]) != "a" {
		t.Fatalf("Read = %q, want %q", buf[:n], "a")
	}

	// No pushEnd: the same response stays open. Reading "b" here — instead
	// of only after the response ends — is the direct proof the client
	// decodes incrementally rather than io.ReadAll-ing the whole body.
	f.pushData([]byte("b"))
	n, err = conn.Read(buf)
	if err != nil {
		t.Fatalf("second Read: %v", err)
	}
	if string(buf[:n]) != "b" {
		t.Fatalf("second Read = %q, want %q", buf[:n], "b")
	}
}

func TestClientFallsBackToBatchWhenServerDoesNotOfferStream(t *testing.T) {
	f := newFakeServer(t) // f.streamMode left false: connect never returns PollMode
	c := f.connector()
	c.PreferStream = true
	conn := mustConnect(t, c)

	f.down <- []byte("batch payload")
	buf := make([]byte, 32)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(buf[:n]) != "batch payload" {
		t.Fatalf("Read = %q, want %q", buf[:n], "batch payload")
	}
}

func TestStreamIdleWatchdogTriggersTransportFailed(t *testing.T) {
	f := newFakeServer(t)
	f.streamMode = true
	f.heartbeatMS = 30
	f.streamMaxMS = 5000
	f.streamHang = true // headers arrive, then nothing — the watchdog must catch this

	c := f.connector()
	c.PreferStream = true
	c.PollGrace = 50 * time.Millisecond // idle timeout = heartbeatMS + PollGrace = 80ms
	conn := mustConnect(t, c)

	select {
	case <-conn.TransportFailed():
		t.Fatal("transport failed immediately, before the idle watchdog interval elapsed")
	case <-time.After(30 * time.Millisecond):
	}

	select {
	case <-conn.TransportFailed():
	case <-time.After(2 * time.Second):
		t.Fatal("TransportFailed did not fire after the idle watchdog should have expired")
	}
}

func TestStreamEndFrameReopensPollWithoutFailure(t *testing.T) {
	f := newFakeServer(t)
	f.streamMode = true
	f.heartbeatMS = 5000
	f.streamMaxMS = 10000

	c := f.connector()
	c.PreferStream = true
	conn := mustConnect(t, c)

	f.pushEnd() // ends the first stream-mode poll response cleanly

	waitFor(t, 2*time.Second, func() bool { return f.pollCount() >= 2 })
	f.pushData([]byte("after reopen"))

	buf := make([]byte, 32)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(buf[:n]) != "after reopen" {
		t.Fatalf("Read = %q, want %q", buf[:n], "after reopen")
	}

	select {
	case <-conn.TransportFailed():
		t.Fatal("a clean end frame must not be treated as a transport failure")
	default:
	}
}

func TestCloseIsPromptDuringLongStreamPoll(t *testing.T) {
	f := newFakeServer(t)
	f.streamMode = true
	f.heartbeatMS = 10000
	f.streamMaxMS = 30000
	f.streamHang = true

	c := f.connector()
	c.PreferStream = true
	c.PollGrace = 30 * time.Second // must not race the Close
	conn, err := c.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}

	waitFor(t, 2*time.Second, func() bool { return f.pollCount() >= 1 })

	start := time.Now()
	conn.Close()
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Close took %v while a stream poll was parked; it must cancel the request, not wait it out", elapsed)
	}
}

func TestConnectRejectsStreamModeWithNonPositiveHeartbeat(t *testing.T) {
	f := newFakeServer(t)
	f.streamMode = true
	f.heartbeatMS = 0 // server negotiates stream but forgets to send a heartbeat interval
	f.streamMaxMS = 5000

	c := f.connector()
	c.PreferStream = true
	if _, err := c.Connect(context.Background()); err == nil {
		t.Fatal("Connect succeeded despite a non-positive heartbeat_interval_ms in a stream-mode response")
	}
}

func TestConnectRejectsStreamModeWithNonPositiveStreamMaxDuration(t *testing.T) {
	f := newFakeServer(t)
	f.streamMode = true
	f.heartbeatMS = 5000
	f.streamMaxMS = 0

	c := f.connector()
	c.PreferStream = true
	if _, err := c.Connect(context.Background()); err == nil {
		t.Fatal("Connect succeeded despite a non-positive stream_max_duration_ms in a stream-mode response")
	}
}

func TestLocalHealthIsPiggybackedOnStreamPolls(t *testing.T) {
	f := newFakeServer(t)
	f.streamMode = true
	f.heartbeatMS = 5000
	f.streamMaxMS = 10000

	var healthy atomic32Bool
	healthy.Store(true)

	c := f.connector()
	c.PreferStream = true
	c.LocalHealth = func() bool { return healthy.Load() }
	mustConnect(t, c)

	waitFor(t, 2*time.Second, func() bool { return f.pollCount() >= 1 })
	f.mu.Lock()
	first := f.pollHdrs[0].Get(HeaderLocalHealth)
	f.mu.Unlock()
	if first != "ok" {
		t.Fatalf("%s = %q on a healthy client, want %q", HeaderLocalHealth, first, "ok")
	}
}

func TestClientStreamModeTreatsSessionGoneAsFailure(t *testing.T) {
	f := newFakeServer(t)
	f.streamMode = true
	f.heartbeatMS = 5000
	f.streamMaxMS = 10000

	c := f.connector()
	c.PreferStream = true
	conn := mustConnect(t, c)

	f.mu.Lock()
	f.pollStatus = http.StatusGone
	f.mu.Unlock()

	// End the current stream poll so the client opens a fresh one, which
	// hits the now-410 fakeServer path.
	f.pushEnd()

	select {
	case <-conn.TransportFailed():
	case <-time.After(2 * time.Second):
		t.Fatal("TransportFailed did not fire after the server reported the session gone (410)")
	}
}

func TestClientStreamModeHandlesHeartbeatFramesWithoutData(t *testing.T) {
	f := newFakeServer(t)
	f.streamMode = true
	f.heartbeatMS = 5000
	f.streamMaxMS = 10000

	c := f.connector()
	c.PreferStream = true
	conn := mustConnect(t, c)

	f.pushHeartbeat()
	f.pushHeartbeat()
	f.pushData([]byte("after heartbeats"))

	buf := make([]byte, 32)
	done := make(chan struct{})
	var n int
	var err error
	go func() {
		n, err = conn.Read(buf)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Read never returned after heartbeat frames were skipped")
	}
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(buf[:n]) != "after heartbeats" {
		t.Fatalf("Read = %q, want %q", buf[:n], "after heartbeats")
	}

	select {
	case <-conn.TransportFailed():
		t.Fatal("heartbeat frames must not be treated as a transport failure")
	default:
	}
}

func TestClientStreamModeFailsOnUnknownFrameType(t *testing.T) {
	f := newFakeServer(t)
	f.streamMode = true
	f.heartbeatMS = 5000
	f.streamMaxMS = 10000

	c := f.connector()
	c.PreferStream = true
	conn := mustConnect(t, c)

	f.streamPush <- frameMsg{typ: frameType(0x7F)}

	select {
	case <-conn.TransportFailed():
	case <-time.After(2 * time.Second):
		t.Fatal("an unknown stream frame type must be treated as a transport failure")
	}
}

func TestClientStreamModeFallsBackToDefaultFrameSizeWhenServerOmitsPollBufferBytes(t *testing.T) {
	f := newFakeServer(t)
	f.streamMode = true
	f.heartbeatMS = 5000
	f.streamMaxMS = 10000
	f.limits.PollBufferBytes = 0 // server sent no poll_buffer_bytes at all

	c := f.connector()
	c.PreferStream = true
	conn := mustConnect(t, c)

	// If the client left its frame-size cap at 0 instead of falling back to
	// DefaultPollBufferSize, decoding this frame would fail outright.
	f.pushData([]byte("still works"))
	buf := make([]byte, 32)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(buf[:n]) != "still works" {
		t.Fatalf("Read = %q, want %q", buf[:n], "still works")
	}
}
