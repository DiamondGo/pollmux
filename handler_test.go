package pollmux

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func testServerConfig() ServerConfig {
	return ServerConfig{
		PollTimeout:    200 * time.Millisecond,
		SessionTimeout: 400 * time.Millisecond,
		SweepInterval:  20 * time.Millisecond,
		CoalesceWindow: 5 * time.Millisecond,
		PollBufferSize: 4 << 10,
		MaxSendBytes:   1 << 16,
	}
}

// newTestServer mounts the three handlers on net/http's own router, which is
// also the case SessionIDFunc's default is written for.
func newTestServer(t *testing.T, cfg ServerConfig, h Hooks) (*httptest.Server, *SessionStore) {
	t.Helper()
	st := NewSessionStore()
	mux := http.NewServeMux()
	mux.Handle("POST /tunnel/connect", ConnectHandler(st, cfg, h))
	mux.Handle("POST /tunnel/{id}/poll", PollHandler(st, cfg, h))
	mux.Handle("DELETE /tunnel/{id}", DeleteHandler(st, cfg, h))
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts, st
}

func postConnect(t *testing.T, ts *httptest.Server, req ConnectRequest) (*http.Response, ConnectResponse) {
	t.Helper()
	body, _ := json.Marshal(req)
	resp, err := ts.Client().Post(ts.URL+"/tunnel/connect", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("connect request: %v", err)
	}
	var cr ConnectResponse
	json.NewDecoder(resp.Body).Decode(&cr)
	resp.Body.Close()
	return resp, cr
}

func connectOK(t *testing.T, ts *httptest.Server) ConnectResponse {
	t.Helper()
	resp, cr := postConnect(t, ts, ConnectRequest{ProtocolVersion: ProtocolVersion})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("connect status = %d, want 200", resp.StatusCode)
	}
	return cr
}

func poll(t *testing.T, ts *httptest.Server, id string, body []byte, headers map[string]string) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/tunnel/"+id+"/poll", r)
	if err != nil {
		t.Fatalf("build poll request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("poll request: %v", err)
	}
	return resp
}

// ★ The ordering fix. A client's first poll can reach the server before
// OnConnect returns, so the session has to be in the store already — otherwise
// that poll gets a 404 and the client reconnect-loops. Proving it from inside
// OnConnect is the only way to pin the ordering down.
func TestSessionIsRegisteredBeforeOnConnect(t *testing.T) {
	var pollStatus int
	var tsRef *httptest.Server

	hooks := Hooks{
		OnConnect: func(s *Session, meta map[string]string) error {
			// A real HTTP poll, over the wire, while OnConnect is still running.
			resp := poll(t, tsRef, s.ID, []byte("early"), map[string]string{HeaderSendOnly: "true"})
			pollStatus = resp.StatusCode
			resp.Body.Close()
			return nil
		},
	}

	ts, _ := newTestServer(t, testServerConfig(), hooks)
	tsRef = ts

	connectOK(t, ts)

	if pollStatus != http.StatusOK {
		t.Fatalf("a poll arriving during OnConnect got status %d, want 200 — "+
			"the session must be in the store before the callback runs", pollStatus)
	}
}

func TestConnectRejectsUnsupportedVersion(t *testing.T) {
	ts, _ := newTestServer(t, testServerConfig(), Hooks{})

	resp, _ := postConnect(t, ts, ConnectRequest{ProtocolVersion: 999})
	if resp.StatusCode != http.StatusUpgradeRequired {
		t.Fatalf("connect status = %d, want 426 for an unsupported version", resp.StatusCode)
	}
}

func TestConnectHandsDownLimits(t *testing.T) {
	cfg := testServerConfig()
	cfg.MaxSendBytes = 4096
	cfg.PollBufferSize = 8192
	ts, _ := newTestServer(t, cfg, Hooks{})

	cr := connectOK(t, ts)

	if cr.Limits.MaxSendBytes != 4096 {
		t.Fatalf("max_send_bytes = %d, want 4096", cr.Limits.MaxSendBytes)
	}
	if cr.Limits.PollBufferBytes != 8192 {
		t.Fatalf("poll_buffer_bytes = %d, want 8192", cr.Limits.PollBufferBytes)
	}
	if cr.Limits.PollTimeout() != cfg.PollTimeout {
		t.Fatalf("poll_timeout = %v, want %v", cr.Limits.PollTimeout(), cfg.PollTimeout)
	}
	if cr.Limits.SessionTimeout() != cfg.SessionTimeout {
		t.Fatalf("session_timeout = %v, want %v", cr.Limits.SessionTimeout(), cfg.SessionTimeout)
	}
}

func TestConnectAuthenticateRejection(t *testing.T) {
	hooks := Hooks{
		Authenticate: func(*http.Request, ConnectRequest) (map[string]string, error) {
			return nil, StatusErrorf(http.StatusUnauthorized, "bad token")
		},
	}
	ts, st := newTestServer(t, testServerConfig(), hooks)

	resp, _ := postConnect(t, ts, ConnectRequest{ProtocolVersion: ProtocolVersion})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("connect status = %d, want 401", resp.StatusCode)
	}
	if st.Len() != 0 {
		t.Fatal("a rejected connect left a session in the store")
	}
}

// A hook that needs a different status — 400 for a malformed role, say
// — must be able to say so rather than having everything flattened to 401.
func TestConnectAuthenticateCanChooseStatus(t *testing.T) {
	hooks := Hooks{
		Authenticate: func(_ *http.Request, req ConnectRequest) (map[string]string, error) {
			if req.Meta["role"] != "provider" && req.Meta["role"] != "consumer" {
				return nil, StatusErrorf(http.StatusBadRequest, "role must be 'consumer' or 'provider'")
			}
			return nil, nil
		},
	}
	ts, _ := newTestServer(t, testServerConfig(), hooks)

	resp, _ := postConnect(t, ts, ConnectRequest{
		ProtocolVersion: ProtocolVersion,
		Meta:            map[string]string{"role": "nonsense"},
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("connect status = %d, want the 400 the hook asked for", resp.StatusCode)
	}
}

func TestConnectPlainErrorDefaultsTo401(t *testing.T) {
	hooks := Hooks{
		Authenticate: func(*http.Request, ConnectRequest) (map[string]string, error) {
			return nil, io.ErrUnexpectedEOF
		},
	}
	ts, _ := newTestServer(t, testServerConfig(), hooks)

	resp, _ := postConnect(t, ts, ConnectRequest{ProtocolVersion: ProtocolVersion})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("connect status = %d, want 401 for a plain error", resp.StatusCode)
	}
}

// Application meta is derived from credentials the client cannot forge, so it
// must win over anything the client declared about itself.
func TestConnectMergesMetaWithApplicationWinning(t *testing.T) {
	hooks := Hooks{
		Authenticate: func(*http.Request, ConnectRequest) (map[string]string, error) {
			return map[string]string{"subdomain": "myapp", "role": "trusted"}, nil
		},
	}
	ts, st := newTestServer(t, testServerConfig(), hooks)

	_, cr := postConnect(t, ts, ConnectRequest{
		ProtocolVersion: ProtocolVersion,
		Meta:            map[string]string{"role": "self-declared", "endpoint": "home"},
	})

	if cr.Meta["role"] != "trusted" {
		t.Fatalf("meta[role] = %q, want the authenticated value to win", cr.Meta["role"])
	}
	if cr.Meta["endpoint"] != "home" {
		t.Fatalf("meta[endpoint] = %q, want the client's value preserved", cr.Meta["endpoint"])
	}
	if cr.Meta["subdomain"] != "myapp" {
		t.Fatalf("meta[subdomain] = %q, want %q", cr.Meta["subdomain"], "myapp")
	}

	s, _ := st.Get(cr.SessionID)
	if got := s.Meta()["subdomain"]; got != "myapp" {
		t.Fatalf("session meta[subdomain] = %q, want the merged value", got)
	}
}

func TestConnectOnConnectFailureCleansUp(t *testing.T) {
	hooks := Hooks{
		OnConnect: func(*Session, map[string]string) error { return io.ErrUnexpectedEOF },
	}
	ts, st := newTestServer(t, testServerConfig(), hooks)

	resp, _ := postConnect(t, ts, ConnectRequest{ProtocolVersion: ProtocolVersion})
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("connect status = %d, want 500", resp.StatusCode)
	}
	if st.Len() != 0 {
		t.Fatal("a failed OnConnect left the session in the store")
	}
}

// Send-only must never wait. If it did, the upload direction would be gated on
// the poll in flight — the head-of-line blocking the split exists to avoid.
func TestPollSendOnlyReturnsImmediately(t *testing.T) {
	ts, st := newTestServer(t, testServerConfig(), Hooks{})
	cr := connectOK(t, ts)

	start := time.Now()
	resp := poll(t, ts, cr.SessionID, []byte("payload"), map[string]string{HeaderSendOnly: "true"})
	resp.Body.Close()
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("send-only status = %d, want 200", resp.StatusCode)
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("send-only took %v; it must not wait on the long poll (timeout is 200ms)", elapsed)
	}

	s, _ := st.Get(cr.SessionID)
	dst := make([]byte, 32)
	n, err := s.Read(dst)
	if err != nil {
		t.Fatalf("Session.Read: %v", err)
	}
	if got := string(dst[:n]); got != "payload" {
		t.Fatalf("Session.Read = %q, want %q", got, "payload")
	}
}

func TestPollDeliversDownstreamData(t *testing.T) {
	ts, st := newTestServer(t, testServerConfig(), Hooks{})
	cr := connectOK(t, ts)
	s, _ := st.Get(cr.SessionID)

	go func() {
		time.Sleep(20 * time.Millisecond)
		s.Write([]byte("downstream"))
	}()

	resp := poll(t, ts, cr.SessionID, nil, map[string]string{HeaderReceiveOnly: "true"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("poll status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "downstream" {
		t.Fatalf("poll body = %q, want %q", body, "downstream")
	}
}

func TestPollTimesOutWith204(t *testing.T) {
	ts, _ := newTestServer(t, testServerConfig(), Hooks{})
	cr := connectOK(t, ts)

	start := time.Now()
	resp := poll(t, ts, cr.SessionID, nil, map[string]string{HeaderReceiveOnly: "true"})
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("idle poll status = %d, want 204", resp.StatusCode)
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("idle poll returned after %v, want it to hold for the ~200ms poll timeout", elapsed)
	}
}

// ★ A5. A closed session must answer 410, not the 204 it shares with the idle
// heartbeat — otherwise the client keeps polling a dead session until its own
// timeout instead of reconnecting at once.
func TestPollOnClosedSessionAnswers410(t *testing.T) {
	ts, st := newTestServer(t, testServerConfig(), Hooks{})
	cr := connectOK(t, ts)
	s, _ := st.Get(cr.SessionID)

	s.Close()

	resp := poll(t, ts, cr.SessionID, nil, map[string]string{HeaderReceiveOnly: "true"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusGone {
		t.Fatalf("poll on a closed session = %d, want 410", resp.StatusCode)
	}
}

// The same distinction from the other side: a poll parked when the session is
// closed underneath it must convert to 410 rather than falling through to 204.
func TestPollParkedWhenSessionClosesAnswers410(t *testing.T) {
	ts, st := newTestServer(t, testServerConfig(), Hooks{})
	cr := connectOK(t, ts)
	s, _ := st.Get(cr.SessionID)

	go func() {
		time.Sleep(30 * time.Millisecond)
		s.Close()
	}()

	start := time.Now()
	resp := poll(t, ts, cr.SessionID, nil, map[string]string{HeaderReceiveOnly: "true"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusGone {
		t.Fatalf("parked poll on a closing session = %d, want 410", resp.StatusCode)
	}
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("took %v to report the close; it should not wait out the poll timeout", elapsed)
	}
}

func TestPollUnknownSessionAnswers404(t *testing.T) {
	ts, _ := newTestServer(t, testServerConfig(), Hooks{})

	resp := poll(t, ts, "no-such-session", nil, map[string]string{HeaderReceiveOnly: "true"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("poll on an unknown session = %d, want 404", resp.StatusCode)
	}
}

// ★ 413 is a protocol violation, not something to recover from: clients are
// told max_send_bytes and clamp to it. The session goes away so the bug is
// loud rather than silently absorbed.
func TestOversizedBodyIs413AndDropsTheSession(t *testing.T) {
	cfg := testServerConfig()
	cfg.MaxSendBytes = 64

	var mu sync.Mutex
	var reasons []DisconnectReason
	hooks := Hooks{
		OnDisconnect: func(_ *Session, reason DisconnectReason) {
			mu.Lock()
			reasons = append(reasons, reason)
			mu.Unlock()
		},
	}
	ts, st := newTestServer(t, cfg, hooks)
	cr := connectOK(t, ts)
	s, _ := st.Get(cr.SessionID)

	resp := poll(t, ts, cr.SessionID, make([]byte, 1000), map[string]string{HeaderSendOnly: "true"})
	resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized send status = %d, want 413", resp.StatusCode)
	}
	if !s.IsClosed() {
		t.Fatal("session survived a protocol violation")
	}
	if _, ok := st.Get(cr.SessionID); ok {
		t.Fatal("session is still in the store after a protocol violation")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(reasons) != 1 || reasons[0] != ReasonProtocolViolation {
		t.Fatalf("OnDisconnect reasons = %v, want exactly [protocol_violation]", reasons)
	}
}

func TestBodyAtExactlyMaxSendBytesIsAccepted(t *testing.T) {
	cfg := testServerConfig()
	cfg.MaxSendBytes = 64
	ts, _ := newTestServer(t, cfg, Hooks{})
	cr := connectOK(t, ts)

	resp := poll(t, ts, cr.SessionID, make([]byte, 64), map[string]string{HeaderSendOnly: "true"})
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("a body of exactly max_send_bytes got %d, want 200", resp.StatusCode)
	}
}

// A3. A parked poll is live proof the client is there; the count has to be
// visible while the poll is parked and back to zero once it returns.
func TestPollInFlightIsVisibleWhileParked(t *testing.T) {
	ts, st := newTestServer(t, testServerConfig(), Hooks{})
	cr := connectOK(t, ts)
	s, _ := st.Get(cr.SessionID)

	done := make(chan struct{})
	go func() {
		resp := poll(t, ts, cr.SessionID, nil, map[string]string{HeaderReceiveOnly: "true"})
		resp.Body.Close()
		close(done)
	}()

	waitFor(t, time.Second, func() bool { return s.PollInFlight() == 1 })

	<-done
	waitFor(t, time.Second, func() bool { return s.PollInFlight() == 0 })
}

// OnPoll runs before the wait, so a health report is acted on now rather than
// a poll timeout later.
func TestOnPollRunsBeforeTheWait(t *testing.T) {
	var mu sync.Mutex
	var health string
	var calledAt time.Time

	hooks := Hooks{
		OnPoll: func(_ *Session, r *http.Request) {
			mu.Lock()
			health = r.Header.Get(HeaderLocalHealth)
			calledAt = time.Now()
			mu.Unlock()
		},
	}
	ts, _ := newTestServer(t, testServerConfig(), hooks)
	cr := connectOK(t, ts)

	start := time.Now()
	resp := poll(t, ts, cr.SessionID, nil, map[string]string{
		HeaderReceiveOnly: "true",
		HeaderLocalHealth: "down",
	})
	resp.Body.Close()

	mu.Lock()
	defer mu.Unlock()
	if health != "down" {
		t.Fatalf("OnPoll saw %s = %q, want %q", HeaderLocalHealth, health, "down")
	}
	if delay := calledAt.Sub(start); delay > 100*time.Millisecond {
		t.Fatalf("OnPoll ran %v in, after the ~200ms wait had started; it must run before", delay)
	}
}

func TestDeleteClosesSessionAndReportsReason(t *testing.T) {
	var mu sync.Mutex
	var reasons []DisconnectReason
	hooks := Hooks{
		OnDisconnect: func(_ *Session, reason DisconnectReason) {
			mu.Lock()
			reasons = append(reasons, reason)
			mu.Unlock()
		},
	}
	ts, st := newTestServer(t, testServerConfig(), hooks)
	cr := connectOK(t, ts)
	s, _ := st.Get(cr.SessionID)

	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/tunnel/"+cr.SessionID, nil)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("delete request: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete status = %d, want 200", resp.StatusCode)
	}
	if !s.IsClosed() {
		t.Fatal("delete did not close the session")
	}
	if _, ok := st.Get(cr.SessionID); ok {
		t.Fatal("delete did not remove the session from the store")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(reasons) != 1 || reasons[0] != ReasonClientDelete {
		t.Fatalf("OnDisconnect reasons = %v, want exactly [client_delete]", reasons)
	}
}

func TestDeleteUnknownSessionAnswers404(t *testing.T) {
	ts, _ := newTestServer(t, testServerConfig(), Hooks{})

	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/tunnel/no-such-session", nil)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("delete request: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("delete of an unknown session = %d, want 404", resp.StatusCode)
	}
}

// Graceful shutdown path: close everything and tell the application once per
// session, with a reason that distinguishes it from an eviction.
func TestCloseSessionFiresOnceWithServerCloseReason(t *testing.T) {
	var mu sync.Mutex
	var reasons []DisconnectReason
	hooks := Hooks{
		OnDisconnect: func(_ *Session, reason DisconnectReason) {
			mu.Lock()
			reasons = append(reasons, reason)
			mu.Unlock()
		},
	}
	ts, st := newTestServer(t, testServerConfig(), hooks)
	cr := connectOK(t, ts)
	s, _ := st.Get(cr.SessionID)

	CloseSession(st, hooks, s, ReasonServerClose)
	CloseSession(st, hooks, s, ReasonServerClose) // repeat must be a no-op

	mu.Lock()
	defer mu.Unlock()
	if len(reasons) != 1 || reasons[0] != ReasonServerClose {
		t.Fatalf("OnDisconnect reasons = %v, want exactly [server_close]", reasons)
	}
}

func TestStartSweeperReportsEviction(t *testing.T) {
	evicted := make(chan DisconnectReason, 4)
	hooks := Hooks{
		OnDisconnect: func(_ *Session, reason DisconnectReason) { evicted <- reason },
	}
	cfg := testServerConfig()
	ts, st := newTestServer(t, cfg, hooks)
	cr := connectOK(t, ts)

	stop := StartSweeper(st, cfg, hooks)
	defer stop()

	select {
	case reason := <-evicted:
		if reason != ReasonEvicted {
			t.Fatalf("OnDisconnect reason = %v, want evicted", reason)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("an idle session was never swept")
	}

	if _, ok := st.Get(cr.SessionID); ok {
		t.Fatal("swept session is still in the store")
	}
}

// gorilla/mux and friends do not use PathValue, so the extractor has to be
// replaceable, which is how an application on another router wires it.
func TestCustomSessionIDFunc(t *testing.T) {
	cfg := testServerConfig()
	cfg.SessionIDFunc = func(r *http.Request) string {
		return r.Header.Get("X-Session-Id")
	}

	st := NewSessionStore()
	mux := http.NewServeMux()
	mux.Handle("POST /tunnel/connect", ConnectHandler(st, cfg, Hooks{}))
	mux.Handle("POST /anything/poll", PollHandler(st, cfg, Hooks{}))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cr := connectOK(t, ts)

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/anything/poll", strings.NewReader("hi"))
	req.Header.Set(HeaderSendOnly, "true")
	req.Header.Set("X-Session-Id", cr.SessionID)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("poll request: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("poll with a custom id extractor = %d, want 200", resp.StatusCode)
	}
}

func TestServerConfigRejectsUnknownPollMode(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("an unimplemented PollMode was accepted silently")
		}
	}()
	cfg := testServerConfig()
	cfg.PollMode = "carrier-pigeon"
	ConnectHandler(NewSessionStore(), cfg, Hooks{})
}

func TestServerConfigAcceptsStreamPollMode(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("PollModeStream was rejected: %v", r)
		}
	}()
	cfg := testServerConfig()
	cfg.PollMode = PollModeStream
	ConnectHandler(NewSessionStore(), cfg, Hooks{})
}

// A session timeout below twice the poll timeout sweeps healthy clients that
// are merely between polls. Failing at wiring time beats debugging it in
// production.
func TestServerConfigRejectsTooShortSessionTimeout(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("a session timeout below 2x the poll timeout was accepted")
		}
	}()
	cfg := ServerConfig{
		PollTimeout:    30 * time.Second,
		SessionTimeout: 45 * time.Second,
	}
	ConnectHandler(NewSessionStore(), cfg, Hooks{})
}

func TestServerConfigRejectsTooShortStreamMaxDuration(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("a StreamMaxDuration below 2x HeartbeatInterval was accepted")
		}
	}()
	cfg := testServerConfig()
	cfg.PollMode = PollModeStream
	cfg.HeartbeatInterval = 10 * time.Second
	cfg.StreamMaxDuration = 15 * time.Second // < 2x HeartbeatInterval
	ConnectHandler(NewSessionStore(), cfg, Hooks{})
}

func TestServerConfigDefaultsAreCoherent(t *testing.T) {
	cfg := ServerConfig{}.withDefaults()

	if cfg.SessionTimeout < 2*cfg.PollTimeout {
		t.Fatalf("default SessionTimeout=%v is below 2x PollTimeout=%v", cfg.SessionTimeout, cfg.PollTimeout)
	}
	if cfg.PollBufferSize != DefaultPollBufferSize {
		t.Fatalf("default PollBufferSize = %d, want %d", cfg.PollBufferSize, DefaultPollBufferSize)
	}
	l := cfg.limits("")
	if l.PollTimeoutMS != DefaultPollTimeout.Milliseconds() {
		t.Fatalf("limits poll_timeout_ms = %d, want %d", l.PollTimeoutMS, DefaultPollTimeout.Milliseconds())
	}
}

// The poll buffer caps one response; the rest waits for the next poll. This is
// the downstream throughput knob, so its behaviour needs pinning down.
func TestPollResponseIsCappedByPollBufferSize(t *testing.T) {
	cfg := testServerConfig()
	cfg.PollBufferSize = 100
	ts, st := newTestServer(t, cfg, Hooks{})
	cr := connectOK(t, ts)
	s, _ := st.Get(cr.SessionID)

	s.Write(make([]byte, 250))

	resp := poll(t, ts, cr.SessionID, nil, map[string]string{HeaderReceiveOnly: "true"})
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if len(body) != 100 {
		t.Fatalf("poll returned %d bytes, want it capped at the 100-byte buffer", len(body))
	}

	resp = poll(t, ts, cr.SessionID, nil, map[string]string{HeaderReceiveOnly: "true"})
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if len(body) != 100 {
		t.Fatalf("second poll returned %d bytes, want the next 100", len(body))
	}
}

// --- stream mode negotiation -------------------------------------------------

func TestConnectNegotiatesStreamModeWhenBothSidesAgree(t *testing.T) {
	cfg := testServerConfig()
	cfg.PollMode = PollModeStream
	cfg.HeartbeatInterval = 111 * time.Millisecond
	cfg.StreamMaxDuration = 999 * time.Millisecond
	ts, _ := newTestServer(t, cfg, Hooks{})

	_, cr := postConnect(t, ts, ConnectRequest{ProtocolVersion: ProtocolVersion, PreferStreamMode: true})
	if cr.PollMode != PollModeStream {
		t.Fatalf("PollMode = %q, want %q", cr.PollMode, PollModeStream)
	}
	if cr.Limits.HeartbeatInterval() != cfg.HeartbeatInterval {
		t.Fatalf("heartbeat_interval_ms = %v, want %v", cr.Limits.HeartbeatInterval(), cfg.HeartbeatInterval)
	}
	if cr.Limits.StreamMaxDuration() != cfg.StreamMaxDuration {
		t.Fatalf("stream_max_duration_ms = %v, want %v", cr.Limits.StreamMaxDuration(), cfg.StreamMaxDuration)
	}
}

func TestConnectStaysBatchWhenServerDoesNotOfferStream(t *testing.T) {
	cfg := testServerConfig() // PollMode left at "" (batch)
	ts, _ := newTestServer(t, cfg, Hooks{})

	_, cr := postConnect(t, ts, ConnectRequest{ProtocolVersion: ProtocolVersion, PreferStreamMode: true})
	if cr.PollMode == PollModeStream {
		t.Fatalf("PollMode = %q, want batch when the server does not offer stream", cr.PollMode)
	}
	if cr.Limits.HeartbeatIntervalMS != 0 {
		t.Fatalf("HeartbeatIntervalMS = %d, want 0 for a batch-negotiated session", cr.Limits.HeartbeatIntervalMS)
	}
}

func TestConnectStaysBatchWhenClientDoesNotPreferStream(t *testing.T) {
	cfg := testServerConfig()
	cfg.PollMode = PollModeStream
	ts, _ := newTestServer(t, cfg, Hooks{})

	_, cr := postConnect(t, ts, ConnectRequest{ProtocolVersion: ProtocolVersion}) // PreferStreamMode left false
	if cr.PollMode == PollModeStream {
		t.Fatalf("PollMode = %q, want batch when the client does not prefer stream", cr.PollMode)
	}
}

// --- pollStream ---------------------------------------------------------------

func testStreamServerConfig() ServerConfig {
	cfg := testServerConfig()
	cfg.PollMode = PollModeStream
	cfg.HeartbeatInterval = 5 * time.Second
	cfg.StreamMaxDuration = 10 * time.Second
	return cfg
}

func connectStream(t *testing.T, ts *httptest.Server) ConnectResponse {
	t.Helper()
	resp, cr := postConnect(t, ts, ConnectRequest{ProtocolVersion: ProtocolVersion, PreferStreamMode: true})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("connect status = %d, want 200", resp.StatusCode)
	}
	if cr.PollMode != PollModeStream {
		t.Fatalf("PollMode = %q, want %q — stream mode did not negotiate", cr.PollMode, PollModeStream)
	}
	return cr
}

func TestPollStreamDeliversFramesWithoutHandlerReturning(t *testing.T) {
	// HeartbeatInterval stays at testStreamServerConfig's generous default
	// (well above the write cadence below, with margin for scheduling
	// jitter under a loaded machine) — the test closes the session
	// explicitly at the end instead, so the handler ends promptly without
	// depending on a short heartbeat for teardown speed.
	cfg := testStreamServerConfig()
	ts, st := newTestServer(t, cfg, Hooks{})
	cr := connectStream(t, ts)
	s, _ := st.Get(cr.SessionID)

	resp := poll(t, ts, cr.SessionID, nil, map[string]string{HeaderReceiveOnly: "true"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream poll status = %d, want 200", resp.StatusCode)
	}
	fr := newFrameReader(resp.Body, 1<<20)

	chunks := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
	go func() {
		for _, c := range chunks {
			// Comfortably above cfg.CoalesceWindow (5ms) and any scheduling
			// jitter under a loaded machine (e.g. -race), so consecutive
			// writes reliably land as separate frames instead of merging.
			time.Sleep(150 * time.Millisecond)
			s.Write(c)
		}
	}()

	// Each chunk must arrive as its own frame while the HTTP response is
	// still open — proof pollStream doesn't wait for the handler to return
	// before the client can see data, which is the entire point of stream
	// mode.
	for i, want := range chunks {
		typ, payload, err := fr.next()
		if err != nil {
			t.Fatalf("frame %d: next: %v", i, err)
		}
		if typ != frameData {
			t.Fatalf("frame %d: type = %v, want frameData", i, typ)
		}
		if string(payload) != string(want) {
			t.Fatalf("frame %d: payload = %q, want %q", i, payload, want)
		}
	}

	// End the stream promptly instead of leaving it parked until
	// HeartbeatInterval or StreamMaxDuration, so test teardown is fast.
	s.Close()
}

func TestPollStreamSendsHeartbeatWhenIdle(t *testing.T) {
	cfg := testStreamServerConfig()
	cfg.HeartbeatInterval = 30 * time.Millisecond
	cfg.StreamMaxDuration = 500 * time.Millisecond
	ts, _ := newTestServer(t, cfg, Hooks{})
	cr := connectStream(t, ts)

	resp := poll(t, ts, cr.SessionID, nil, map[string]string{HeaderReceiveOnly: "true"})
	defer resp.Body.Close()
	fr := newFrameReader(resp.Body, 1<<20)

	typ, _, err := fr.next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if typ != frameHeartbeat {
		t.Fatalf("frame type = %v, want frameHeartbeat", typ)
	}
}

func TestPollStreamEndsAtMaxDuration(t *testing.T) {
	cfg := testStreamServerConfig()
	cfg.HeartbeatInterval = 20 * time.Millisecond
	cfg.StreamMaxDuration = 80 * time.Millisecond
	ts, _ := newTestServer(t, cfg, Hooks{})
	cr := connectStream(t, ts)

	start := time.Now()
	resp := poll(t, ts, cr.SessionID, nil, map[string]string{HeaderReceiveOnly: "true"})
	defer resp.Body.Close()
	fr := newFrameReader(resp.Body, 1<<20)

	sawEnd := false
	for {
		typ, _, err := fr.next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("next: %v", err)
		}
		if typ == frameEnd {
			sawEnd = true
		}
	}
	if !sawEnd {
		t.Fatal("body ended without an explicit frameEnd")
	}
	if elapsed := time.Since(start); elapsed < cfg.StreamMaxDuration {
		t.Fatalf("stream ended after %v, want at least StreamMaxDuration=%v", elapsed, cfg.StreamMaxDuration)
	} else if elapsed > cfg.StreamMaxDuration+2*time.Second {
		t.Fatalf("stream ended after %v, too long past StreamMaxDuration=%v", elapsed, cfg.StreamMaxDuration)
	}
}

// TestPollStreamEndsWhenSessionCloses checks the fatal path: the session
// closing mid-poll must produce frameGone, not frameEnd — reusing frameEnd
// here would make the client reopen a poll against a session that no longer
// exists instead of surfacing a failure (this is exactly what broke
// HttpBroker's provider-reconnect flow before frameGone existed).
func TestPollStreamEndsWhenSessionCloses(t *testing.T) {
	cfg := testStreamServerConfig()
	ts, st := newTestServer(t, cfg, Hooks{})
	cr := connectStream(t, ts)
	s, _ := st.Get(cr.SessionID)

	resp := poll(t, ts, cr.SessionID, nil, map[string]string{HeaderReceiveOnly: "true"})
	defer resp.Body.Close()

	done := make(chan error, 1)
	go func() {
		fr := newFrameReader(resp.Body, 1<<20)
		typ, _, err := fr.next()
		if err != nil {
			done <- err
			return
		}
		if typ != frameGone {
			done <- fmt.Errorf("frame type = %v, want frameGone", typ)
			return
		}
		_, _, err = fr.next()
		done <- err // want io.EOF: the response body closing right after frameGone
	}()

	time.Sleep(20 * time.Millisecond) // let the poll actually park before closing
	s.Close()

	select {
	case err := <-done:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("after frameGone, next() = %v, want io.EOF", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the stream to end after the session closed")
	}
}

// TestPollStreamDistinguishesGoneFromMaxDurationEnd is the regression test
// for the frameEnd/frameGone conflation bug: a session that closes mid-poll
// must send frameGone (fatal), and StreamMaxDuration rolling over on a
// still-alive session must keep sending frameEnd (benign) — the two must
// never collapse onto the same frame type again.
func TestPollStreamDistinguishesGoneFromMaxDurationEnd(t *testing.T) {
	cfg := testStreamServerConfig()
	cfg.HeartbeatInterval = 20 * time.Millisecond
	cfg.StreamMaxDuration = 80 * time.Millisecond
	ts, _ := newTestServer(t, cfg, Hooks{})
	cr := connectStream(t, ts)

	resp := poll(t, ts, cr.SessionID, nil, map[string]string{HeaderReceiveOnly: "true"})
	defer resp.Body.Close()
	fr := newFrameReader(resp.Body, 1<<20)

	for {
		typ, _, err := fr.next()
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if typ == frameGone {
			t.Fatal("StreamMaxDuration rollover must send frameEnd, not frameGone — the session is still alive")
		}
		if typ == frameEnd {
			return
		}
	}
}

func TestPollStreamCapsDataFrameAtPollBufferSize(t *testing.T) {
	cfg := testStreamServerConfig()
	cfg.PollBufferSize = 100
	ts, st := newTestServer(t, cfg, Hooks{})
	cr := connectStream(t, ts)
	s, _ := st.Get(cr.SessionID)

	resp := poll(t, ts, cr.SessionID, nil, map[string]string{HeaderReceiveOnly: "true"})
	defer resp.Body.Close()
	fr := newFrameReader(resp.Body, 1<<20)

	s.Write(make([]byte, 250))

	typ, payload, err := fr.next()
	if err != nil {
		t.Fatalf("first frame: next: %v", err)
	}
	if typ != frameData || len(payload) != 100 {
		t.Fatalf("first frame: type=%v len=%d, want frameData len 100", typ, len(payload))
	}

	typ, payload, err = fr.next()
	if err != nil {
		t.Fatalf("second frame: next: %v", err)
	}
	if typ != frameData || len(payload) != 100 {
		t.Fatalf("second frame: type=%v len=%d, want frameData len 100", typ, len(payload))
	}
}

// headerOnlyResponseWriter implements http.ResponseWriter but deliberately
// not http.Flusher, exercising pollStream's guard against a wrapping
// middleware that strips Flusher.
type headerOnlyResponseWriter struct {
	header http.Header
	status int
}

func (w *headerOnlyResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}
func (w *headerOnlyResponseWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *headerOnlyResponseWriter) WriteHeader(status int)      { w.status = status }

func TestPollStreamRequiresFlusher(t *testing.T) {
	cfg := testStreamServerConfig().withDefaults()
	s := newSession("sess-flusher", nil)
	s.pollMode = PollModeStream

	w := &headerOnlyResponseWriter{}
	r := httptest.NewRequest(http.MethodPost, "/tunnel/sess-flusher/poll", nil)
	pollStream(w, r, s, cfg)

	if w.status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d when the ResponseWriter does not support http.Flusher",
			w.status, http.StatusInternalServerError)
	}
}

// failingFlusher implements http.ResponseWriter and http.Flusher, but every
// Write after the response header returns an error — simulating a client
// that disconnected mid-stream, without needing a real broken TCP
// connection.
type failingFlusher struct {
	header     http.Header
	status     int
	headerSent bool
	writeErr   error
}

func (w *failingFlusher) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}
func (w *failingFlusher) WriteHeader(status int) { w.status = status; w.headerSent = true }
func (w *failingFlusher) Write(p []byte) (int, error) {
	if w.headerSent {
		return 0, w.writeErr
	}
	return len(p), nil
}
func (w *failingFlusher) Flush() {}

func TestPollStreamEndsPromptlyWhenDataFrameWriteFails(t *testing.T) {
	cfg := testStreamServerConfig().withDefaults()
	s := newSession("sess-write-fail-data", nil)
	s.pollMode = PollModeStream
	s.Write([]byte("data ready before the poll starts"))

	w := &failingFlusher{writeErr: io.ErrClosedPipe}
	r := httptest.NewRequest(http.MethodPost, "/tunnel/sess-write-fail-data/poll", nil)

	done := make(chan struct{})
	go func() {
		pollStream(w, r, s, cfg)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pollStream did not return after a data frame write failure")
	}
}

func TestPollStreamEndsPromptlyWhenHeartbeatFrameWriteFails(t *testing.T) {
	cfg := testStreamServerConfig()
	cfg.HeartbeatInterval = 10 * time.Millisecond
	cfg = cfg.withDefaults()
	s := newSession("sess-write-fail-heartbeat", nil)
	s.pollMode = PollModeStream
	// No data written: ReadAvailable times out and pollStream tries to send
	// a heartbeat frame, which the writer rejects.

	w := &failingFlusher{writeErr: io.ErrClosedPipe}
	r := httptest.NewRequest(http.MethodPost, "/tunnel/sess-write-fail-heartbeat/poll", nil)

	done := make(chan struct{})
	go func() {
		pollStream(w, r, s, cfg)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pollStream did not return after a heartbeat frame write failure")
	}
}
