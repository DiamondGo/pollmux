package pollmux

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func testWebSocketServerConfig() ServerConfig {
	return ServerConfig{
		PollTimeout:       200 * time.Millisecond,
		SessionTimeout:    400 * time.Millisecond,
		SweepInterval:     20 * time.Millisecond,
		CoalesceWindow:    5 * time.Millisecond,
		PollBufferSize:    4 << 10,
		MaxSendBytes:      1 << 16,
		HeartbeatInterval: 50 * time.Millisecond,
	}
}

// newWebSocketTestServer mirrors newTestServer's shape, with EnableWebSocket
// on and the WebSocketHandler mounted alongside the usual three.
func newWebSocketTestServer(t *testing.T, cfg ServerConfig, h Hooks) (*httptest.Server, *SessionStore) {
	t.Helper()
	cfg.EnableWebSocket = true
	st := NewSessionStore()
	mux := http.NewServeMux()
	mux.Handle("POST /tunnel/connect", ConnectHandler(st, cfg, h))
	mux.Handle("POST /tunnel/{id}/poll", PollHandler(st, cfg, h))
	mux.Handle("DELETE /tunnel/{id}", DeleteHandler(st, cfg, h))
	mux.Handle("GET /tunnel/{id}/ws", WebSocketHandler(st, cfg, h))
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts, st
}

// connectOKWebSocket connects with PreferWebSocket set and requires the
// server to have actually negotiated it, so every handler-level test below
// starts from a session WebSocketHandler will accept an attach for.
func connectOKWebSocket(t *testing.T, ts *httptest.Server) ConnectResponse {
	t.Helper()
	resp, cr := postConnect(t, ts, ConnectRequest{ProtocolVersion: ProtocolVersion, PreferWebSocket: true})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("connect status = %d, want 200", resp.StatusCode)
	}
	if cr.Transport != TransportWebSocket {
		t.Fatalf("Transport = %q, want %q — the test server must have EnableWebSocket on", cr.Transport, TransportWebSocket)
	}
	return cr
}

// dialWS attaches a raw WebSocket to an already-negotiated session, bypassing
// Connector entirely — these tests exercise WebSocketHandler directly, the
// same spirit as handler_test.go driving PollHandler with raw HTTP calls.
func dialWS(t *testing.T, ts *httptest.Server, id string) *websocket.Conn {
	t.Helper()
	ws, _, err := websocket.Dial(context.Background(), ts.URL+"/tunnel/"+id+"/ws", nil)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	t.Cleanup(func() { ws.CloseNow() })
	return ws
}

// --- negotiation -------------------------------------------------------------
//
// Mirrors the PollMode/UploadStreamMode negotiation tests: one per
// combination that matters, proving WebSocket transport is negotiated
// independently of, and does not disturb, the existing PollMode gate.

func TestConnectNegotiatesWebSocketWhenBothSidesAgree(t *testing.T) {
	cfg := testWebSocketServerConfig()
	ts, _ := newWebSocketTestServer(t, cfg, Hooks{})

	cr := connectOKWebSocket(t, ts)
	if cr.Limits.HeartbeatInterval() != cfg.HeartbeatInterval {
		t.Fatalf("heartbeat_interval_ms = %v, want %v", cr.Limits.HeartbeatInterval(), cfg.HeartbeatInterval)
	}
	// WebSocket transport needs none of PollMode's own machinery — it must
	// not get silently turned on as a side effect.
	if cr.PollMode == PollModeStream {
		t.Fatalf("PollMode = %q, want batch: websocket transport must not imply poll/stream mode", cr.PollMode)
	}
}

func TestConnectStaysOffWebSocketWhenServerDoesNotEnableIt(t *testing.T) {
	cfg := testServerConfig() // EnableWebSocket left false
	ts, _ := newTestServer(t, cfg, Hooks{})

	_, cr := postConnect(t, ts, ConnectRequest{ProtocolVersion: ProtocolVersion, PreferWebSocket: true})
	if cr.Transport != "" {
		t.Fatalf("Transport = %q, want empty when the server does not enable websocket", cr.Transport)
	}
}

func TestConnectStaysOffWebSocketWhenClientDoesNotPreferIt(t *testing.T) {
	cfg := testWebSocketServerConfig()
	ts, _ := newWebSocketTestServer(t, cfg, Hooks{})

	_, cr := postConnect(t, ts, ConnectRequest{ProtocolVersion: ProtocolVersion}) // PreferWebSocket left false
	if cr.Transport != "" {
		t.Fatalf("Transport = %q, want empty when the client does not prefer it", cr.Transport)
	}
}

// --- WebSocketHandler, driven directly ----------------------------------------

func TestWebSocketUpstreamDataReachesSession(t *testing.T) {
	ts, st := newWebSocketTestServer(t, testWebSocketServerConfig(), Hooks{})
	cr := connectOKWebSocket(t, ts)
	s, _ := st.Get(cr.SessionID)

	ws := dialWS(t, ts, cr.SessionID)
	if err := ws.Write(context.Background(), websocket.MessageBinary, wsEncode(frameData, []byte("upstream"))); err != nil {
		t.Fatalf("Write: %v", err)
	}

	dst := make([]byte, 32)
	n, err := s.Read(dst)
	if err != nil {
		t.Fatalf("Session.Read: %v", err)
	}
	if got := string(dst[:n]); got != "upstream" {
		t.Fatalf("Session.Read = %q, want %q", got, "upstream")
	}
}

func TestWebSocketDownstreamDataReachesClient(t *testing.T) {
	ts, st := newWebSocketTestServer(t, testWebSocketServerConfig(), Hooks{})
	cr := connectOKWebSocket(t, ts)
	s, _ := st.Get(cr.SessionID)

	ws := dialWS(t, ts, cr.SessionID)
	s.Write([]byte("downstream"))

	typ, msg, err := ws.Read(context.Background())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if typ != websocket.MessageBinary {
		t.Fatalf("message type = %v, want binary", typ)
	}
	ft, payload, err := wsDecode(msg)
	if err != nil {
		t.Fatalf("wsDecode: %v", err)
	}
	if ft != frameData || string(payload) != "downstream" {
		t.Fatalf("got frame %v %q, want data %q", ft, payload, "downstream")
	}
}

func TestWebSocketSendsHeartbeatWhenIdle(t *testing.T) {
	cfg := testWebSocketServerConfig()
	cfg.HeartbeatInterval = 30 * time.Millisecond
	ts, _ := newWebSocketTestServer(t, cfg, Hooks{})
	cr := connectOKWebSocket(t, ts)

	ws := dialWS(t, ts, cr.SessionID)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	typ, msg, err := ws.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if typ != websocket.MessageBinary {
		t.Fatalf("message type = %v, want binary", typ)
	}
	ft, payload, err := wsDecode(msg)
	if err != nil {
		t.Fatalf("wsDecode: %v", err)
	}
	if ft != frameHeartbeat || len(payload) != 0 {
		t.Fatalf("got frame %v %q, want an empty heartbeat", ft, payload)
	}
}

// The whole point of the heartbeat: a session with no application traffic at
// all must survive several heartbeat intervals without either side declaring
// it dead — this is the exact property the poll/send-stream transport lost
// through Cloudflare's request-body buffering (see README's "五、WebSocket
// 传输模式").
func TestWebSocketSurvivesIdlePastSeveralHeartbeats(t *testing.T) {
	cfg := testWebSocketServerConfig()
	cfg.HeartbeatInterval = 20 * time.Millisecond
	ts, _ := newWebSocketTestServer(t, cfg, Hooks{})

	c := &Connector{BaseURL: ts.URL, PreferWebSocket: true, PollGrace: 200 * time.Millisecond}
	conn := mustConnect(t, c)

	select {
	case <-conn.TransportFailed():
		t.Fatal("transport failed while the link was merely idle, not dead")
	case <-time.After(300 * time.Millisecond): // 15x the heartbeat interval
	}
}

func TestWebSocketSessionCloseIsNoticedPromptly(t *testing.T) {
	ts, st := newWebSocketTestServer(t, testWebSocketServerConfig(), Hooks{})
	c := &Connector{BaseURL: ts.URL, PreferWebSocket: true, PollGrace: 200 * time.Millisecond}
	conn := mustConnect(t, c)

	s, _ := st.Get(conn.SessionID())
	start := time.Now()
	CloseSession(st, Hooks{}, s, ReasonServerClose)

	select {
	case <-conn.TransportFailed():
		if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
			t.Fatalf("took %v to notice the server closed the session", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the client never noticed the server had closed its session")
	}
}

func TestWebSocketDoubleAttachIsRejected(t *testing.T) {
	ts, _ := newWebSocketTestServer(t, testWebSocketServerConfig(), Hooks{})
	cr := connectOKWebSocket(t, ts)

	first := dialWS(t, ts, cr.SessionID)

	_, resp, err := websocket.Dial(context.Background(), ts.URL+"/tunnel/"+cr.SessionID+"/ws", nil)
	if err == nil {
		t.Fatal("second attach succeeded, want it rejected")
	}
	if resp == nil || resp.StatusCode != http.StatusConflict {
		status := -1
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("second attach status = %d, want %d", status, http.StatusConflict)
	}

	// The first attach must be unaffected by the rejected second one.
	if err := first.Write(context.Background(), websocket.MessageBinary, wsEncode(frameHeartbeat, nil)); err != nil {
		t.Fatalf("first attach broke after a rejected second attach: %v", err)
	}
}

func TestWebSocketRejectsAttachToNonWebSocketSession(t *testing.T) {
	ts, _ := newWebSocketTestServer(t, testWebSocketServerConfig(), Hooks{})
	// Connect without PreferWebSocket: negotiates plain batch transport.
	cr := connectOK(t, ts)

	_, resp, err := websocket.Dial(context.Background(), ts.URL+"/tunnel/"+cr.SessionID+"/ws", nil)
	if err == nil {
		t.Fatal("attach to a batch-negotiated session succeeded, want it rejected")
	}
	if resp == nil || resp.StatusCode != http.StatusBadRequest {
		status := -1
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("attach status = %d, want %d", status, http.StatusBadRequest)
	}
}

func TestWebSocketUnknownSessionAnswers404(t *testing.T) {
	ts, _ := newWebSocketTestServer(t, testWebSocketServerConfig(), Hooks{})

	_, resp, err := websocket.Dial(context.Background(), ts.URL+"/tunnel/does-not-exist/ws", nil)
	if err == nil {
		t.Fatal("attach to an unknown session succeeded, want it rejected")
	}
	if resp == nil || resp.StatusCode != http.StatusNotFound {
		status := -1
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("attach status = %d, want %d", status, http.StatusNotFound)
	}
}

// --- Connector, via the real handler ------------------------------------------

func TestConnectorFallsBackWhenServerDoesNotOfferWebSocket(t *testing.T) {
	ts, _ := newTestServer(t, testServerConfig(), Hooks{}) // no EnableWebSocket
	c := &Connector{BaseURL: ts.URL, PreferWebSocket: true}
	conn := mustConnect(t, c)

	if _, ok := conn.(*wsConn); ok {
		t.Fatal("got a *wsConn from a server that never offered websocket transport")
	}

	// And the connection must still work, over the batch fallback.
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

// The client's own read-idle watchdog, proven against a deliberately broken
// peer (accepts, then never reads or writes anything) — the WebSocket
// counterpart to TestStreamIdleWatchdogTriggersTransportFailed.
func TestClientWebSocketIdleWatchdogTriggersTransportFailed(t *testing.T) {
	cfg := testWebSocketServerConfig()
	cfg.HeartbeatInterval = 30 * time.Millisecond
	cfg.EnableWebSocket = true
	st := NewSessionStore()
	mux := http.NewServeMux()
	mux.Handle("POST /tunnel/connect", ConnectHandler(st, cfg, Hooks{}))
	mux.HandleFunc("GET /tunnel/{id}/ws", func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer c.CloseNow()
		<-r.Context().Done() // accept, then go silent — never read or write
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	c := &Connector{BaseURL: ts.URL, PreferWebSocket: true, PollGrace: 50 * time.Millisecond}
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

// Mirrors TestConnectRejectsStreamModeWithNonPositiveHeartbeat: a server that
// negotiates websocket transport but forgets the heartbeat interval leaves
// the client with no way to size its idle watchdog, so Connect must refuse
// rather than silently picking an arbitrary value.
func TestConnectRejectsWebSocketWithNonPositiveHeartbeat(t *testing.T) {
	f := newFakeServer(t)
	f.websocketMode = true
	f.heartbeatMS = 0 // server negotiates websocket but forgets a heartbeat interval

	c := f.connector()
	c.PreferWebSocket = true
	if _, err := c.Connect(context.Background()); err == nil {
		t.Fatal("Connect succeeded despite a non-positive heartbeat_interval_ms in a websocket-transport response")
	}
}
