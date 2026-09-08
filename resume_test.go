package pollmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/hashicorp/yamux"
)

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

// testResumeServerConfig is testServerConfig with two-way stream mode,
// WebSocket, and resume all switched on, so one server can negotiate every
// shape ResumeHandler has to deal with.
func testResumeServerConfig() ServerConfig {
	return ServerConfig{
		PollTimeout:       200 * time.Millisecond,
		SessionTimeout:    400 * time.Millisecond,
		SweepInterval:     20 * time.Millisecond,
		CoalesceWindow:    5 * time.Millisecond,
		PollBufferSize:    4 << 10,
		MaxSendBytes:      1 << 16,
		PollMode:          PollModeStream,
		HeartbeatInterval: 50 * time.Millisecond,
		StreamMaxDuration: 2 * time.Second,
		EnableWebSocket:   true,
		EnableResume:      true,
		ResumeGrace:       time.Second,
	}
}

// newResumeTestServer mounts every handler, including ResumeHandler.
func newResumeTestServer(t *testing.T, cfg ServerConfig, h Hooks) (*httptest.Server, *SessionStore) {
	t.Helper()
	st := NewSessionStore()
	mux := http.NewServeMux()
	mux.Handle("POST /tunnel/connect", ConnectHandler(st, cfg, h))
	mux.Handle("POST /tunnel/{id}/poll", PollHandler(st, cfg, h))
	mux.Handle("POST /tunnel/{id}/resume", ResumeHandler(st, cfg, h))
	mux.Handle("DELETE /tunnel/{id}", DeleteHandler(st, cfg, h))
	mux.Handle("GET /tunnel/{id}/ws", WebSocketHandler(st, cfg, h))
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts, st
}

// connectResumableStream negotiates a two-way stream, resumable session.
func connectResumableStream(t *testing.T, ts *httptest.Server) ConnectResponse {
	t.Helper()
	resp, cr := postConnect(t, ts, ConnectRequest{
		ProtocolVersion: ProtocolVersion, PreferStreamMode: true, PreferStreamUpload: true, PreferResume: true,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("connect status = %d, want 200", resp.StatusCode)
	}
	if !cr.Resumable {
		t.Fatal("server did not negotiate a resumable session")
	}
	return cr
}

// postResume performs the raw handshake.
func postResume(t *testing.T, ts *httptest.Server, id string, req ResumeRequest) (*http.Response, ResumeResponse) {
	t.Helper()
	body, _ := json.Marshal(req)
	resp, err := ts.Client().Post(ts.URL+"/tunnel/"+id+"/resume", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("resume request: %v", err)
	}
	var rr ResumeResponse
	json.NewDecoder(resp.Body).Decode(&rr)
	resp.Body.Close()
	return resp, rr
}

// openStreamPoll opens a raw receive-only stream poll and returns the frame
// reader over its body plus a cancel that drops the connection mid-stream —
// the client-side half of a seam failure.
func openStreamPoll(t *testing.T, ts *httptest.Server, id string) (*frameReader, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/tunnel/"+id+"/poll", nil)
	req.Header.Set(HeaderReceiveOnly, "true")
	resp, err := ts.Client().Do(req)
	if err != nil {
		cancel()
		t.Fatalf("stream poll: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		cancel()
		t.Fatalf("stream poll status = %d, want 200", resp.StatusCode)
	}
	t.Cleanup(func() { cancel(); resp.Body.Close() })
	return newFrameReader(resp.Body, 1<<20), cancel
}

// nextSeqData reads frames until a seq-data frame arrives, skipping
// heartbeats and acks (which it returns through acks).
func nextSeqData(t *testing.T, fr *frameReader) (off uint64, data []byte, acks []uint64) {
	t.Helper()
	for {
		typ, payload, err := fr.next()
		if err != nil {
			t.Fatalf("reading frame: %v", err)
		}
		switch typ {
		case frameSeqData:
			off, data, err = splitSeq(payload)
			if err != nil {
				t.Fatal(err)
			}
			return off, data, acks
		case frameAck:
			n, err := decodeAck(payload)
			if err != nil {
				t.Fatal(err)
			}
			acks = append(acks, n)
		case frameHeartbeat:
		case frameData:
			t.Fatal("server sent an unnumbered data frame on a resumable session")
		default:
			t.Fatalf("unexpected frame %#x", typ)
		}
	}
}

// rawSendStream posts one send-stream request carrying the given
// pre-encoded frames followed by frameEnd, returning the response.
func rawSendStream(t *testing.T, ts *httptest.Server, id string, frames ...[]byte) *http.Response {
	t.Helper()
	var body bytes.Buffer
	for _, f := range frames {
		body.Write(f)
	}
	writeFrame(&body, frameEnd, nil)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/tunnel/"+id+"/poll", &body)
	req.Header.Set(HeaderSendStream, "true")
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("send-stream: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}

func seqFrame(off uint64, data string) []byte {
	var b bytes.Buffer
	writeSeqFrame(&b, off, []byte(data))
	return b.Bytes()
}

func ackFrame(n uint64) []byte {
	var b bytes.Buffer
	writeFrame(&b, frameAck, encodeOffset(n))
	return b.Bytes()
}

// ---------------------------------------------------------------------------
// negotiation
// ---------------------------------------------------------------------------

func TestConnectNegotiatesResumeOnlyForResumableTransports(t *testing.T) {
	cases := []struct {
		name string
		cfg  func(*ServerConfig)
		req  ConnectRequest
		want bool
	}{
		{"two-way stream", nil, ConnectRequest{PreferStreamMode: true, PreferStreamUpload: true, PreferResume: true}, true},
		{"websocket", nil, ConnectRequest{PreferWebSocket: true, PreferResume: true}, true},
		{"websocket without stream prefs", nil, ConnectRequest{PreferWebSocket: true, PreferResume: true}, true},
		{"download-only stream", nil, ConnectRequest{PreferStreamMode: true, PreferResume: true}, false},
		{"upload-only stream", nil, ConnectRequest{PreferStreamUpload: true, PreferResume: true}, false},
		{"batch", nil, ConnectRequest{PreferResume: true}, false},
		{"client does not ask", nil, ConnectRequest{PreferStreamMode: true, PreferStreamUpload: true}, false},
		{"server has resume off", func(c *ServerConfig) { c.EnableResume = false },
			ConnectRequest{PreferStreamMode: true, PreferStreamUpload: true, PreferResume: true}, false},
		{"websocket off falls back to stream and stays resumable", func(c *ServerConfig) { c.EnableWebSocket = false },
			ConnectRequest{PreferWebSocket: true, PreferStreamMode: true, PreferStreamUpload: true, PreferResume: true}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testResumeServerConfig()
			if tc.cfg != nil {
				tc.cfg(&cfg)
			}
			ts, st := newResumeTestServer(t, cfg, Hooks{})
			tc.req.ProtocolVersion = ProtocolVersion
			resp, cr := postConnect(t, ts, tc.req)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("connect status = %d", resp.StatusCode)
			}
			if cr.Resumable != tc.want {
				t.Fatalf("Resumable = %v, want %v (transport=%q poll_mode=%q upload=%q)",
					cr.Resumable, tc.want, cr.Transport, cr.PollMode, cr.UploadStreamMode)
			}
			if tc.want && cr.Limits.ResumeGrace() != cfg.ResumeGrace {
				t.Fatalf("resume_grace_ms = %v, want %v", cr.Limits.ResumeGrace(), cfg.ResumeGrace)
			}
			if !tc.want && cr.Limits.ResumeGraceMS != 0 {
				t.Fatalf("resume_grace_ms = %d on a non-resumable connect, want 0", cr.Limits.ResumeGraceMS)
			}
			s, _ := st.Get(cr.SessionID)
			if s.Resumable() != tc.want {
				t.Fatalf("Session.Resumable() = %v, want %v", s.Resumable(), tc.want)
			}
		})
	}
}

func TestServerConfigRejectsExcessiveResumeGrace(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("check() accepted ResumeGrace above MaxResumeGrace")
		}
	}()
	cfg := testResumeServerConfig()
	cfg.ResumeGrace = MaxResumeGrace + time.Second
	cfg.check()
}

// ---------------------------------------------------------------------------
// ResumeHandler, driven directly
// ---------------------------------------------------------------------------

func TestResumeHandlerStatuses(t *testing.T) {
	ts, st := newResumeTestServer(t, testResumeServerConfig(), Hooks{})

	if resp, _ := postResume(t, ts, "nope", ResumeRequest{ProtocolVersion: ProtocolVersion}); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown session = %d, want 404", resp.StatusCode)
	}

	plain := connectOK(t, ts) // batch: never resumable
	if resp, _ := postResume(t, ts, plain.SessionID, ResumeRequest{ProtocolVersion: ProtocolVersion}); resp.StatusCode != http.StatusConflict {
		t.Fatalf("non-resumable session = %d, want 409", resp.StatusCode)
	}

	cr := connectResumableStream(t, ts)
	if resp, _ := postResume(t, ts, cr.SessionID, ResumeRequest{ProtocolVersion: 99}); resp.StatusCode != http.StatusUpgradeRequired {
		t.Fatalf("bad version = %d, want 426", resp.StatusCode)
	}

	// Nothing sent yet: only offset 0 is honourable.
	resp, rr := postResume(t, ts, cr.SessionID, ResumeRequest{ProtocolVersion: ProtocolVersion, RecvOffset: 0})
	if resp.StatusCode != http.StatusOK || !rr.Resumed || rr.RecvOffset != 0 {
		t.Fatalf("fresh resume = (%d, %+v), want 200 resumed at 0", resp.StatusCode, rr)
	}
	s, _ := st.Get(cr.SessionID)
	resp, _ = postResume(t, ts, cr.SessionID, ResumeRequest{ProtocolVersion: ProtocolVersion, RecvOffset: 5})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("out-of-range offset = %d, want 409", resp.StatusCode)
	}
	if s.Resumable() {
		t.Fatal("an impossible offset must end the session's resumability, never be guessed at")
	}
	if resp, _ := postResume(t, ts, cr.SessionID, ResumeRequest{ProtocolVersion: ProtocolVersion}); resp.StatusCode != http.StatusConflict {
		t.Fatalf("resume on a broken session = %d, want 409", resp.StatusCode)
	}

	closed := connectResumableStream(t, ts)
	cs, _ := st.Get(closed.SessionID)
	cs.Close() // closed but still in the store: the 410 path
	if resp, _ := postResume(t, ts, closed.SessionID, ResumeRequest{ProtocolVersion: ProtocolVersion}); resp.StatusCode != http.StatusGone {
		t.Fatalf("closed session = %d, want 410", resp.StatusCode)
	}
}

// ★ The seam. Downstream bytes that were drained from the pipe and written
// to a poll response the client never finished reading are the whole reason
// the reliable layer exists: after the resume they come back, from exactly
// the offset the client declared, and nothing else is lost or repeated.
func TestResumeReplaysDownstreamFromClientOffset(t *testing.T) {
	ts, st := newResumeTestServer(t, testResumeServerConfig(), Hooks{})
	cr := connectResumableStream(t, ts)
	s, _ := st.Get(cr.SessionID)

	fr, dropPoll := openStreamPoll(t, ts, cr.SessionID)
	s.Write([]byte("hello world"))
	off, data, _ := nextSeqData(t, fr)
	if off != 0 || string(data) != "hello world" {
		t.Fatalf("first frame = (%d, %q)", off, data)
	}
	// The client "received" only the first 5 bytes before the link died and
	// never acknowledged anything. Meanwhile the server writes more, which
	// goes into the replay buffer with no transport to send it on.
	dropPoll()
	waitFor(t, 2*time.Second, func() bool { return s.PollInFlight() == 0 })
	s.Write([]byte("!"))

	resp, rr := postResume(t, ts, cr.SessionID, ResumeRequest{ProtocolVersion: ProtocolVersion, RecvOffset: 5})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("resume = %d", resp.StatusCode)
	}
	if rr.RecvOffset != 0 {
		t.Fatalf("server recv_offset = %d, want 0 (nothing was uploaded)", rr.RecvOffset)
	}

	fr2, _ := openStreamPoll(t, ts, cr.SessionID)
	off, data, _ = nextSeqData(t, fr2)
	if off != 5 || string(data) != " world" {
		t.Fatalf("replayed frame = (%d, %q), want (5, %q)", off, data, " world")
	}
	off, data, _ = nextSeqData(t, fr2)
	if off != 11 || string(data) != "!" {
		t.Fatalf("frame after replay = (%d, %q), want (11, %q)", off, data, "!")
	}
	if s.rs.unacked() != 7 {
		t.Fatalf("unacked = %d, want 7 (offsets 5..12 retained until acked)", s.rs.unacked())
	}

	// An ack from the client releases the retained bytes.
	if resp := rawSendStream(t, ts, cr.SessionID, ackFrame(12)); resp.StatusCode != http.StatusOK {
		t.Fatalf("send-stream carrying ack = %d", resp.StatusCode)
	}
	waitFor(t, time.Second, func() bool { return s.rs.unacked() == 0 })
}

func TestResumeReportsUpstreamOffsetAndDedupsReplayedUploads(t *testing.T) {
	ts, st := newResumeTestServer(t, testResumeServerConfig(), Hooks{})
	cr := connectResumableStream(t, ts)
	s, _ := st.Get(cr.SessionID)

	if resp := rawSendStream(t, ts, cr.SessionID, seqFrame(0, "abc")); resp.StatusCode != http.StatusOK {
		t.Fatalf("send-stream = %d", resp.StatusCode)
	}
	// The client's replay after a failed leg overlaps what already arrived.
	if resp := rawSendStream(t, ts, cr.SessionID, seqFrame(1, "bcde")); resp.StatusCode != http.StatusOK {
		t.Fatalf("overlapping send-stream = %d", resp.StatusCode)
	}
	got := make([]byte, 16)
	total := 0
	for total < 5 {
		n, err := s.Read(got[total:])
		if err != nil {
			t.Fatal(err)
		}
		total += n
	}
	if string(got[:total]) != "abcde" {
		t.Fatalf("session read %q, want %q", got[:total], "abcde")
	}

	resp, rr := postResume(t, ts, cr.SessionID, ResumeRequest{ProtocolVersion: ProtocolVersion})
	if resp.StatusCode != http.StatusOK || rr.RecvOffset != 5 {
		t.Fatalf("resume = (%d, recv_offset %d), want 200 with 5", resp.StatusCode, rr.RecvOffset)
	}

	// A hole is fatal: refused, and the session stops being resumable.
	if resp := rawSendStream(t, ts, cr.SessionID, seqFrame(9, "x")); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("gap = %d, want 400", resp.StatusCode)
	}
	if s.Resumable() {
		t.Fatal("a gap must end resumability")
	}
	// Unnumbered data is refused too on a resumable session.
	fresh := connectResumableStream(t, ts)
	var plain bytes.Buffer
	writeFrame(&plain, frameData, []byte("raw"))
	if resp := rawSendStream(t, ts, fresh.SessionID, plain.Bytes()); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("plain data on resumable session = %d, want 400", resp.StatusCode)
	}
	// And a discrete send-only or batch poll is refused outright.
	fresh2 := connectResumableStream(t, ts)
	if resp := poll(t, ts, fresh2.SessionID, []byte("x"), map[string]string{HeaderSendOnly: "true"}); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("send-only on resumable session = %d, want 400", resp.StatusCode)
	}
}

// A resume must not trust its offsets while a stale handler may still be
// draining the pipe. The handshake kicks the parked poll and waits for it to
// leave; the client that opened it sees its body end.
func TestResumeKicksParkedStreamPoll(t *testing.T) {
	ts, st := newResumeTestServer(t, testResumeServerConfig(), Hooks{})
	cr := connectResumableStream(t, ts)
	s, _ := st.Get(cr.SessionID)

	fr, _ := openStreamPoll(t, ts, cr.SessionID)
	waitFor(t, time.Second, func() bool { return s.PollInFlight() == 1 })

	start := time.Now()
	resp, _ := postResume(t, ts, cr.SessionID, ResumeRequest{ProtocolVersion: ProtocolVersion})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("resume = %d", resp.StatusCode)
	}
	if time.Since(start) > detachWait {
		t.Fatal("resume waited out detachWait instead of kicking the parked poll")
	}
	// The kicked poll's body ends (without a frameGone: the session lives).
	for {
		typ, _, err := fr.next()
		if err != nil {
			break
		}
		if typ == frameGone {
			t.Fatal("kicked poll got frameGone; the session must survive a resume")
		}
	}
	if s.IsClosed() {
		t.Fatal("session closed by resume")
	}

	// The next attach works normally and starts with an ack.
	fr2, _ := openStreamPoll(t, ts, cr.SessionID)
	s.Write([]byte("after"))
	off, data, _ := nextSeqData(t, fr2)
	if off != 0 || string(data) != "after" {
		t.Fatalf("post-resume frame = (%d, %q)", off, data)
	}
}

// Two stream polls on the same direction: the newer one displaces the
// older, exactly what happens when a client reopens after a rollover the
// server has not finished noticing.
func TestNewerStreamPollDisplacesOlderOnResumableSession(t *testing.T) {
	ts, st := newResumeTestServer(t, testResumeServerConfig(), Hooks{})
	cr := connectResumableStream(t, ts)
	s, _ := st.Get(cr.SessionID)

	old, _ := openStreamPoll(t, ts, cr.SessionID)
	waitFor(t, time.Second, func() bool { return s.PollInFlight() == 1 })
	fresh, _ := openStreamPoll(t, ts, cr.SessionID)

	// The old one ends; the new one is served.
	for {
		if _, _, err := old.next(); err != nil {
			break
		}
	}
	s.Write([]byte("to the newcomer"))
	off, data, _ := nextSeqData(t, fresh)
	if off != 0 || string(data) != "to the newcomer" {
		t.Fatalf("newcomer frame = (%d, %q)", off, data)
	}
}

// ---------------------------------------------------------------------------
// lifecycle: grace, sweeper, conditional close, DELETE race
// ---------------------------------------------------------------------------

func TestDetachedResumableSessionOutlivesSessionTimeoutUntilGrace(t *testing.T) {
	cfg := testResumeServerConfig()
	cfg.SessionTimeout = 400 * time.Millisecond
	cfg.ResumeGrace = 900 * time.Millisecond
	var mu sync.Mutex
	var reasons []DisconnectReason
	h := Hooks{OnDisconnect: func(_ *Session, r DisconnectReason) { mu.Lock(); reasons = append(reasons, r); mu.Unlock() }}
	ts, st := newResumeTestServer(t, cfg, h)
	stop := StartSweeper(st, cfg, h)
	defer stop()

	cr := connectResumableStream(t, ts) // never attaches: detached from birth
	s, _ := st.Get(cr.SessionID)

	time.Sleep(600 * time.Millisecond) // past SessionTimeout, inside grace
	if _, ok := st.Get(cr.SessionID); !ok {
		t.Fatal("detached resumable session was swept before its grace ran out")
	}
	if dl, ok := s.ResumeDeadline(); !ok || time.Until(dl) <= 0 || time.Until(dl) > cfg.ResumeGrace {
		t.Fatalf("ResumeDeadline = (%v, %v)", dl, ok)
	}
	if CloseSessionIfNoPollInFlight(st, h, s, ReasonServerClose) {
		t.Fatal("conditional close killed a resumable session inside its grace")
	}

	waitFor(t, 2*time.Second, func() bool { _, ok := st.Get(cr.SessionID); return !ok })
	mu.Lock()
	defer mu.Unlock()
	if len(reasons) != 1 || reasons[0] != ReasonEvicted {
		t.Fatalf("disconnects = %v, want exactly one eviction", reasons)
	}
	if _, ok := s.ResumeDeadline(); ok {
		t.Fatal("ResumeDeadline reported on a closed session")
	}
}

func TestConditionalCloseSucceedsOnceGraceHasPassed(t *testing.T) {
	st := NewSessionStore()
	s := newSession("r", nil)
	s.enableResume(1<<20, 50*time.Millisecond)
	st.add(s)

	if CloseSessionIfNoPollInFlight(st, Hooks{}, s, ReasonServerClose) {
		t.Fatal("closed inside grace")
	}
	s.mu.Lock()
	s.detachedAt = time.Now().Add(-time.Second)
	s.mu.Unlock()
	if !CloseSessionIfNoPollInFlight(st, Hooks{}, s, ReasonServerClose) {
		t.Fatal("did not close after grace")
	}

	// A broken (no longer resumable) session is judged like any other.
	b := newSession("b", nil)
	b.enableResume(1<<20, time.Hour)
	st.add(b)
	b.rs.markBroken()
	if !CloseSessionIfNoPollInFlight(st, Hooks{}, b, ReasonServerClose) {
		t.Fatal("conditional close spared a session that is no longer resumable")
	}
}

func TestSweeperEvictsLongestDetachedBeyondCap(t *testing.T) {
	st := NewSessionStore()
	st.maxDetachedResumable = 2
	var ids []string
	for i, age := range []time.Duration{3 * time.Second, time.Second, 2 * time.Second} {
		s := newSession(string(rune('a'+i)), nil)
		s.enableResume(1<<20, time.Hour)
		s.mu.Lock()
		s.detachedAt = time.Now().Add(-age)
		s.mu.Unlock()
		st.add(s)
		ids = append(ids, s.ID)
	}
	var evicted []string
	st.sweep(time.Hour, func(s *Session) { evicted = append(evicted, s.ID) })
	if len(evicted) != 1 || evicted[0] != "a" {
		t.Fatalf("evicted %v, want just the longest-detached %q", evicted, "a")
	}
	if st.Len() != 2 {
		t.Fatalf("store has %d sessions, want 2", st.Len())
	}
	_ = ids
}

func TestResumeAfterDeleteFailsAndHookFiresOnce(t *testing.T) {
	var calls atomic.Int32
	h := Hooks{OnDisconnect: func(*Session, DisconnectReason) { calls.Add(1) }}
	ts, _ := newResumeTestServer(t, testResumeServerConfig(), h)
	cr := connectResumableStream(t, ts)

	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/tunnel/"+cr.SessionID, nil)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	resp, _ = postResume(t, ts, cr.SessionID, ResumeRequest{ProtocolVersion: ProtocolVersion})
	if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusGone {
		t.Fatalf("resume after delete = %d, want 404 or 410", resp.StatusCode)
	}
	if calls.Load() != 1 {
		t.Fatalf("OnDisconnect fired %d times, want 1", calls.Load())
	}
}

// ---------------------------------------------------------------------------
// client supervisor safety guard
// ---------------------------------------------------------------------------

func TestResumeStabilityGuardAbandonsConsecutiveFastLegs(t *testing.T) {
	var g resumeStabilityGuard
	for i := 1; i < maxConsecutiveFastResumes; i++ {
		if g.record(resumableLegsMinStable-time.Nanosecond, false) {
			t.Fatalf("guard abandoned after %d fast resumes, want %d", i, maxConsecutiveFastResumes)
		}
	}
	if !g.record(resumableLegsMinStable-time.Nanosecond, false) {
		t.Fatalf("guard did not abandon after %d consecutive fast resumes", maxConsecutiveFastResumes)
	}
}

func TestResumeStabilityGuardStableOrProductiveLegResetsCounter(t *testing.T) {
	var g resumeStabilityGuard
	for i := 0; i < maxConsecutiveFastResumes-1; i++ {
		if g.record(time.Millisecond, false) {
			t.Fatal("guard abandoned before reaching the limit")
		}
	}
	if g.record(resumableLegsMinStable, false) {
		t.Fatal("a stable leg must reset, not trip, the guard")
	}
	if g.consecutiveFast != 0 {
		t.Fatalf("consecutive fast count after stable leg = %d, want 0", g.consecutiveFast)
	}

	for i := 0; i < maxConsecutiveFastResumes-1; i++ {
		if g.record(time.Millisecond, false) {
			t.Fatalf("old fast failures leaked across reset at new failure %d", i+1)
		}
	}
	if g.record(time.Millisecond, true) {
		t.Fatal("a productive fast leg must reset, not trip, the guard")
	}
	if g.consecutiveFast != 0 {
		t.Fatalf("consecutive fast count after productive leg = %d, want 0", g.consecutiveFast)
	}
}

// A current resumable WebSocket attachment may be interrupted merely to
// re-check its state. With no received bytes there is no ack to overwrite the
// sentinel error; this is the exact path that used to leak errPipeInterrupted
// out of wsWritePumpResumable and close an otherwise healthy connection.
func TestWebSocketServerInterruptWithoutAckKeepsCurrentAttachment(t *testing.T) {
	cfg := testResumeServerConfig()
	ts, st := newResumeTestServer(t, cfg, Hooks{})
	resp, cr := postConnect(t, ts, ConnectRequest{
		ProtocolVersion: ProtocolVersion,
		PreferWebSocket: true,
		PreferResume:    true,
	})
	if resp.StatusCode != http.StatusOK || !cr.Resumable {
		t.Fatalf("connect = %d resumable=%v, want a resumable WebSocket", resp.StatusCode, cr.Resumable)
	}
	s, _ := st.Get(cr.SessionID)
	ws := dialWS(t, ts, cr.SessionID)

	// Read one heartbeat so the handler is attached and its write pump has
	// entered the normal pipe wait with recvOffset still zero.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, msg, err := ws.Read(ctx)
	cancel()
	if err != nil {
		t.Fatalf("initial heartbeat: %v", err)
	}
	if typ, _, err := wsDecode(msg); err != nil || typ != frameHeartbeat {
		t.Fatalf("initial frame = (%v, %v), want heartbeat", typ, err)
	}

	s.rs.interruptOut()

	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	_, msg, err = ws.Read(ctx)
	cancel()
	if err != nil {
		t.Fatalf("WebSocket closed after a harmless interrupt: %v", err)
	}
	if typ, _, err := wsDecode(msg); err != nil || typ != frameHeartbeat {
		t.Fatalf("frame after interrupt = (%v, %v), want heartbeat", typ, err)
	}
}

// ---------------------------------------------------------------------------
// end to end, through a proxy that breaks
// ---------------------------------------------------------------------------

// faultProxy is a TCP relay in front of the server that can kill every
// connection through it at once (the client sees exactly what a CDN's
// max-connection-age cutoff looks like), refuse new ones, or swallow
// upstream bytes while keeping the connection up.
type faultProxy struct {
	ln      net.Listener
	backend string

	mu    sync.Mutex
	conns map[net.Conn]struct{}

	refuse atomic.Bool // accept and close at once
	dropUp atomic.Bool // discard client→backend bytes
}

func newFaultProxy(t *testing.T, backendURL string) *faultProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &faultProxy{ln: ln, backend: strings.TrimPrefix(backendURL, "http://"), conns: make(map[net.Conn]struct{})}
	go p.serve()
	t.Cleanup(func() { ln.Close(); p.killAll() })
	return p
}

func (p *faultProxy) URL() string { return "http://" + p.ln.Addr().String() }

func (p *faultProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		if p.refuse.Load() {
			c.Close()
			continue
		}
		go p.handle(c)
	}
}

func (p *faultProxy) handle(client net.Conn) {
	backend, err := net.Dial("tcp", p.backend)
	if err != nil {
		client.Close()
		return
	}
	p.mu.Lock()
	p.conns[client] = struct{}{}
	p.conns[backend] = struct{}{}
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.conns, client)
		delete(p.conns, backend)
		p.mu.Unlock()
		client.Close()
		backend.Close()
	}()

	done := make(chan struct{}, 2)
	go func() {
		buf := make([]byte, 32<<10)
		for {
			n, err := client.Read(buf)
			if n > 0 && !p.dropUp.Load() {
				if _, werr := backend.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		done <- struct{}{}
	}()
	go func() { io.Copy(client, backend); done <- struct{}{} }()
	<-done
}

// connCount is how many relayed connections (both halves) are live.
func (p *faultProxy) connCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns)
}

// killAll drops every connection currently relayed, both halves.
func (p *faultProxy) killAll() {
	p.mu.Lock()
	conns := make([]net.Conn, 0, len(p.conns))
	for c := range p.conns {
		conns = append(conns, c)
	}
	p.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

// resumeE2E is the whole stack with resume on and a faultProxy between the
// Connector and the server.
type resumeE2E struct {
	ts    *httptest.Server
	proxy *faultProxy
	store *SessionStore
	cfg   ServerConfig
	hooks Hooks
	mode  string

	mu          sync.Mutex
	disconnects []DisconnectReason

	resumes atomic.Int32 // POST .../resume requests seen by the server
}

func newResumeE2E(t *testing.T, mode string, tweak func(*ServerConfig), middleware func(http.Handler) http.Handler) *resumeE2E {
	t.Helper()
	e := &resumeE2E{
		store: NewSessionStore(),
		mode:  mode,
		cfg: ServerConfig{
			PollTimeout:       500 * time.Millisecond,
			SessionTimeout:    time.Second,
			SweepInterval:     50 * time.Millisecond,
			CoalesceWindow:    2 * time.Millisecond,
			PollBufferSize:    64 << 10,
			MaxSendBytes:      256 << 10,
			PollMode:          PollModeStream,
			HeartbeatInterval: 200 * time.Millisecond,
			StreamMaxDuration: 2 * time.Second,
			EnableWebSocket:   mode == TransportWebSocket,
			EnableResume:      true,
			ResumeGrace:       3 * time.Second,
		},
	}
	if tweak != nil {
		tweak(&e.cfg)
	}
	e.hooks = Hooks{
		OnConnect: func(s *Session, _ map[string]string) error {
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
					go func() {
						defer stream.Close()
						io.Copy(stream, stream)
					}()
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
	mux.Handle("POST /tunnel/{id}/resume", ResumeHandler(e.store, e.cfg, e.hooks))
	mux.Handle("DELETE /tunnel/{id}", DeleteHandler(e.store, e.cfg, e.hooks))
	mux.Handle("GET /tunnel/{id}/ws", WebSocketHandler(e.store, e.cfg, e.hooks))
	var h http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/resume") {
			e.resumes.Add(1)
		}
		mux.ServeHTTP(w, r)
	})
	if middleware != nil {
		h = middleware(h)
	}
	e.ts = httptest.NewServer(h)
	t.Cleanup(e.ts.Close)
	stop := StartSweeper(e.store, e.cfg, e.hooks)
	t.Cleanup(stop)
	e.proxy = newFaultProxy(t, e.ts.URL)
	return e
}

func (e *resumeE2E) connector(tweak func(*Connector)) *Connector {
	c := &Connector{
		BaseURL:                e.proxy.URL(),
		PollGrace:              300 * time.Millisecond,
		SendTimeout:            time.Second,
		DialTimeout:            time.Second,
		PreferStream:           true,
		UploadStreamPreference: PollModeStream,
		PreferWebSocket:        e.mode == TransportWebSocket,
		PreferResume:           true,
	}
	if tweak != nil {
		tweak(c)
	}
	return c
}

func (e *resumeE2E) dial(t *testing.T, tweak func(*Connector)) (Conn, *yamux.Session) {
	t.Helper()
	conn, err := e.connector(tweak).Connect(context.Background())
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

func (e *resumeE2E) reasons() []DisconnectReason {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]DisconnectReason(nil), e.disconnects...)
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// A level-triggered interrupt can remain pending while no ack is due. It is a
// wake-up, not a failed leg: the client write pump must keep running. Drive the
// pump directly so a heartbeat observed by the peer is an explicit readiness
// signal; because interrupts are level-triggered, injecting one after that
// point guarantees the current or next nextOut wait consumes it.
func TestWebSocketClientInterruptWithoutAckDoesNotStopWriteLoop(t *testing.T) {
	serverWS := make(chan *websocket.Conn, 1)
	handlerDone := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		serverWS <- ws
		<-handlerDone
		ws.CloseNow()
	}))
	t.Cleanup(func() {
		close(handlerDone)
		ts.Close()
	})

	clientWS, _, err := websocket.Dial(context.Background(), ts.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { clientWS.CloseNow() })
	peerWS := <-serverWS

	ctx, cancel := context.WithCancel(context.Background())
	rc := &resumableConn{
		rl:             newReliable(NewBufferedPipe(), NewBufferedPipe(), DefaultMaxReplayBytes),
		heartbeat:      20 * time.Millisecond,
		idleTimeout:    time.Second,
		coalesceWindow: time.Millisecond,
		maxFrame:       4 << 10,
	}
	loopDone := make(chan error, 1)
	go func() { loopDone <- rc.wsWriteLoop(ctx, clientWS) }()
	t.Cleanup(func() {
		cancel()
		rc.rl.interruptOut()
		<-loopDone
	})

	readHeartbeat := func(stage string) {
		t.Helper()
		rctx, cancelRead := context.WithTimeout(context.Background(), 2*time.Second)
		typ, msg, err := peerWS.Read(rctx)
		cancelRead()
		if err != nil {
			t.Fatalf("%s heartbeat: %v", stage, err)
		}
		if typ != websocket.MessageBinary {
			t.Fatalf("%s message type = %v, want binary", stage, typ)
		}
		ft, payload, err := wsDecode(msg)
		if err != nil || ft != frameHeartbeat || len(payload) != 0 {
			t.Fatalf("%s frame = (%v, %q, %v), want empty heartbeat", stage, ft, payload, err)
		}
	}

	readHeartbeat("ready")
	rc.rl.interruptOut()
	readHeartbeat("after interrupt")
}

// Even if an unknown transport defect survives the targeted interrupt fix,
// successful handshakes followed by immediately dead replacement legs must be
// bounded. This proxy middleware accepts and instantly closes every WebSocket
// dial after the first resume, while allowing /resume itself to keep returning
// 200, reproducing the shape of the production spin.
func TestSuperviseAbandonsConsecutiveFastSuccessfulResumes(t *testing.T) {
	var poisonWebSockets atomic.Bool
	middleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/resume") {
				poisonWebSockets.Store(true)
				next.ServeHTTP(w, r)
				return
			}
			if poisonWebSockets.Load() && r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/ws") {
				ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
				if err == nil {
					ws.CloseNow()
				}
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	e := newResumeE2E(t, TransportWebSocket, nil, middleware)
	conn, err := e.connector(nil).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	waitFor(t, 2*time.Second, func() bool { return e.proxy.connCount() >= 2 })
	e.proxy.killAll()

	select {
	case <-conn.TransportFailed():
	case <-time.After(10 * time.Second):
		t.Fatal("supervisor kept resuming immediately dead legs instead of abandoning the session")
	}

	got := int(e.resumes.Load())
	// The initial leg is normally fast and contributes to the limit. If a
	// slow test machine kept it alive past the stable threshold, one extra
	// successful resume is needed before five poisoned legs accumulate.
	if got < maxConsecutiveFastResumes || got > maxConsecutiveFastResumes+1 {
		t.Fatalf("resume handshakes before abandonment = %d, want %d or %d",
			got, maxConsecutiveFastResumes, maxConsecutiveFastResumes+1)
	}
}

// ★ Acceptance: the transport is killed repeatedly in the middle of a bulk
// transfer, and the yamux session, the stream, and every byte survive.
func TestE2EResumeSurvivesTransportKills_Stream(t *testing.T) {
	testE2EResumeSurvivesTransportKills(t, PollModeStream)
}
func TestE2EResumeSurvivesTransportKills_WebSocket(t *testing.T) {
	testE2EResumeSurvivesTransportKills(t, TransportWebSocket)
}

func testE2EResumeSurvivesTransportKills(t *testing.T, mode string) {
	e := newResumeE2E(t, mode, nil, nil)
	conn, sess := e.dial(t, nil)
	if _, ok := conn.(*resumableConn); !ok {
		t.Fatalf("Connect returned %T, want *resumableConn", conn)
	}
	sessionID := conn.SessionID()

	stream, err := sess.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer stream.Close()

	const chunk = 32 << 10
	payload := make([]byte, 96*chunk) // 3MB
	rand.Read(payload)
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

	// Kill the link every dozen chunks — while data is in flight in both
	// directions — for a handful of seams per transfer.
	kills := 0
	for i := 0; i < len(payload); i += chunk {
		if _, err := stream.Write(payload[i : i+chunk]); err != nil {
			t.Fatalf("Write at %d: %v", i, err)
		}
		if i > 0 && (i/chunk)%12 == 0 {
			e.proxy.killAll()
			kills++
		}
	}

	select {
	case got := <-readDone:
		if got == nil {
			t.Fatal("the echo came back short")
		}
		if sha256.Sum256(got) != want {
			t.Fatal("echoed bytes differ from what was sent")
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("echo did not complete across %d transport kills", kills)
	}

	if kills < 3 {
		t.Fatalf("only %d kills were injected", kills)
	}
	if e.resumes.Load() == 0 {
		t.Fatal("no resume handshake happened: the kills did not exercise the seam")
	}
	if isClosed(conn.TransportFailed()) {
		t.Fatal("TransportFailed fired: the session was not kept")
	}
	if sess.IsClosed() {
		t.Fatal("yamux session closed across the kills")
	}
	if conn.SessionID() != sessionID || e.store.Len() != 1 {
		t.Fatalf("session identity changed: id %q→%q, store has %d", sessionID, conn.SessionID(), e.store.Len())
	}
	if r := e.reasons(); len(r) != 0 {
		t.Fatalf("server saw disconnects %v; a resumed session must never disconnect", r)
	}

	// And the session is fully healthy afterwards: a fresh stream works.
	second, err := sess.Open()
	if err != nil {
		t.Fatalf("Open after kills: %v", err)
	}
	defer second.Close()
	second.Write([]byte("still here"))
	buf := make([]byte, 10)
	if _, err := io.ReadFull(second, buf); err != nil || string(buf) != "still here" {
		t.Fatalf("post-kill stream echo = (%q, %v)", buf, err)
	}
}

// The transport is killed while the session is idle — no data in flight —
// and the next write still goes through, on the same session.
func TestE2EResumeIdleKillIsInvisible_Stream(t *testing.T) {
	testE2EResumeIdleKillIsInvisible(t, PollModeStream)
}
func TestE2EResumeIdleKillIsInvisible_WebSocket(t *testing.T) {
	testE2EResumeIdleKillIsInvisible(t, TransportWebSocket)
}

func testE2EResumeIdleKillIsInvisible(t *testing.T, mode string) {
	e := newResumeE2E(t, mode, nil, nil)
	conn, sess := e.dial(t, nil)
	stream, err := sess.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	stream.Write([]byte("before"))
	buf := make([]byte, 6)
	if _, err := io.ReadFull(stream, buf); err != nil {
		t.Fatal(err)
	}

	e.proxy.killAll()
	time.Sleep(300 * time.Millisecond)
	e.proxy.killAll() // and once more while it may be mid-resume

	stream.Write([]byte("after!"))
	stream.SetReadDeadline(time.Now().Add(15 * time.Second))
	if _, err := io.ReadFull(stream, buf); err != nil || string(buf) != "after!" {
		t.Fatalf("post-kill echo = (%q, %v)", buf, err)
	}
	if isClosed(conn.TransportFailed()) || len(e.reasons()) != 0 {
		t.Fatal("session did not survive an idle kill")
	}
	if e.resumes.Load() == 0 {
		t.Fatal("no resume handshake happened")
	}
}

// Grace runs out: the server evicts, the client gives up, and the caller
// gets exactly today's signal — TransportFailed — to build a new session.
func TestE2EResumeGraceExpiryFallsBackToTransportFailed(t *testing.T) {
	e := newResumeE2E(t, PollModeStream, func(c *ServerConfig) { c.ResumeGrace = 600 * time.Millisecond }, nil)
	conn, sess := e.dial(t, nil)
	stream, err := sess.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	e.proxy.refuse.Store(true)
	e.proxy.killAll()

	select {
	case <-conn.TransportFailed():
	case <-time.After(10 * time.Second):
		t.Fatal("TransportFailed never fired after the grace ran out")
	}
	waitFor(t, 5*time.Second, func() bool { return e.store.Len() == 0 })
	if r := e.reasons(); len(r) != 1 || r[0] != ReasonEvicted {
		t.Fatalf("disconnects = %v, want one eviction", r)
	}
	// yamux notices the read pipe closing and ends the session.
	select {
	case <-sess.CloseChan():
	case <-time.After(5 * time.Second):
		t.Fatal("yamux session did not end after the connection gave up")
	}
}

// The server closed the session (DELETE from elsewhere, an eviction, a
// shutdown): not a transport failure, and not something to resume.
func TestE2EResumeDoesNotResurrectAServerClosedSession(t *testing.T) {
	e := newResumeE2E(t, PollModeStream, nil, nil)
	conn, _ := e.dial(t, nil)
	s, _ := e.store.Get(conn.SessionID())
	CloseSession(e.store, e.hooks, s, ReasonServerClose)

	select {
	case <-conn.TransportFailed():
	case <-time.After(10 * time.Second):
		t.Fatal("TransportFailed never fired for a server-closed session")
	}
	if e.store.Len() != 0 {
		t.Fatal("a resume recreated the session")
	}
}

// Server has resume off: the client asked, got a plain session, and behaves
// exactly as today (a kill ends the session).
func TestE2EResumeDegradesWhenServerHasItOff(t *testing.T) {
	e := newResumeE2E(t, PollModeStream, func(c *ServerConfig) { c.EnableResume = false }, nil)
	conn, sess := e.dial(t, nil)
	if _, ok := conn.(*resumableConn); ok {
		t.Fatal("got a resumable conn from a server with resume off")
	}
	if conn.Limits().ResumeGraceMS != 0 {
		t.Fatal("resume_grace_ms handed down without resume")
	}
	// A round trip first, so both legs' connections exist to be killed.
	stream, err := sess.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	stream.Write([]byte("ping"))
	buf := make([]byte, 4)
	if _, err := io.ReadFull(stream, buf); err != nil {
		t.Fatal(err)
	}
	e.proxy.killAll()
	select {
	case <-conn.TransportFailed():
	case <-time.After(10 * time.Second):
		t.Fatal("non-resumable conn did not report the kill")
	}
	if e.resumes.Load() != 0 {
		t.Fatal("a non-resumable conn attempted a resume")
	}
}

// Client did not ask: nothing changes for it either.
func TestE2EResumeStaysOffWhenClientDoesNotAsk(t *testing.T) {
	e := newResumeE2E(t, PollModeStream, nil, nil)
	conn, _ := e.dial(t, func(c *Connector) { c.PreferResume = false })
	if _, ok := conn.(*resumableConn); ok {
		t.Fatal("got a resumable conn without asking")
	}
	s, _ := e.store.Get(conn.SessionID())
	if s.Resumable() {
		t.Fatal("server made the session resumable without being asked")
	}
}

// Auto upload probe fails on a resumable connect: the client cannot run the
// reliable layer over discrete uploads, so it drops that session and
// connects again without resume — one session left, one clean delete.
func TestE2EResumeFallsBackWhenUploadProbeFails(t *testing.T) {
	stall := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get(HeaderSendStreamProbe) == "true" {
				// A proxy that never forwards the body. Bounded rather than
				// waiting on r.Context(): net/http does not notice a dropped
				// connection while a request body sits unread, and
				// httptest.Server.Close would wait on this handler forever.
				select {
				case <-r.Context().Done():
				case <-time.After(2 * time.Second):
				}
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	e := newResumeE2E(t, PollModeStream, nil, stall)
	conn, _ := e.dial(t, func(c *Connector) {
		c.UploadStreamPreference = ""
		c.UploadProbeTimeout = 300 * time.Millisecond
	})
	if _, ok := conn.(*resumableConn); ok {
		t.Fatal("kept a resumable conn after the probe failed")
	}
	hc, ok := conn.(*httpConn)
	if !ok || hc.uploadStreamMode {
		t.Fatalf("fallback conn = %T (uploadStreamMode=%v), want *httpConn on discrete uploads", conn, ok && hc.uploadStreamMode)
	}
	waitFor(t, 2*time.Second, func() bool { return e.store.Len() == 1 })
	if r := e.reasons(); len(r) != 1 || r[0] != ReasonClientDelete {
		t.Fatalf("disconnects = %v, want the abandoned session deleted once", r)
	}
	s, _ := e.store.Get(conn.SessionID())
	if s.Resumable() {
		t.Fatal("the replacement session must not be resumable")
	}
}

// A replay buffer that outgrows MaxReplayBytes ends resumability cleanly:
// the connection keeps working until its transport actually fails, then
// falls back to TransportFailed instead of resuming with bytes it dropped.
func TestE2EResumeReplayOverflowGivesUpCleanly(t *testing.T) {
	e := newResumeE2E(t, PollModeStream, nil, nil)
	conn, sess := e.dial(t, func(c *Connector) { c.MaxReplayBytes = 32 << 10 })
	rc := conn.(*resumableConn)
	stream, err := sess.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	// A round trip first, so both legs' connections exist before anything
	// is done to them.
	stream.Write([]byte("ping"))
	buf := make([]byte, 4)
	if _, err := io.ReadFull(stream, buf); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return e.proxy.connCount() >= 4 })

	// Upstream bytes vanish in the proxy: sent, never acknowledged. The
	// first 64KB chunk alone overflows a 32KB replay buffer.
	e.proxy.dropUp.Store(true)
	if _, err := stream.Write(make([]byte, 128<<10)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return rc.rl.isBroken() })
	if isClosed(conn.TransportFailed()) {
		t.Fatal("overflow alone must not fail the transport while it is still up")
	}

	e.proxy.dropUp.Store(false)
	e.proxy.killAll()
	select {
	case <-conn.TransportFailed():
	case <-time.After(10 * time.Second):
		t.Fatal("a broken reliable layer must fall back to TransportFailed on the next kill")
	}
}

// Raw WebSocket on a resumable session: the same framing rules as stream.
func TestWebSocketResumableSessionUsesSeqFramesAndAcks(t *testing.T) {
	ts, st := newResumeTestServer(t, testResumeServerConfig(), Hooks{})
	resp, cr := postConnect(t, ts, ConnectRequest{ProtocolVersion: ProtocolVersion, PreferWebSocket: true, PreferResume: true})
	if resp.StatusCode != http.StatusOK || !cr.Resumable || cr.Transport != TransportWebSocket {
		t.Fatalf("connect = %d resumable=%v transport=%q", resp.StatusCode, cr.Resumable, cr.Transport)
	}
	s, _ := st.Get(cr.SessionID)

	ws := dialWS(t, ts, cr.SessionID)
	ctx := context.Background()
	if err := ws.Write(ctx, websocket.MessageBinary, wsEncodeSeq(0, []byte("up"))); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 8)
	n, _ := s.Read(got)
	if string(got[:n]) != "up" {
		t.Fatalf("session read %q", got[:n])
	}

	s.Write([]byte("down"))
	for {
		_, msg, err := ws.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ft, payload, _ := wsDecode(msg)
		if ft == frameSeqData {
			off, data, _ := splitSeq(payload)
			if off != 0 || string(data) != "down" {
				t.Fatalf("ws seq frame = (%d, %q)", off, data)
			}
			break
		}
		if ft == frameData {
			t.Fatal("server sent an unnumbered data frame on a resumable websocket")
		}
	}
	if s.rs.unacked() != 4 {
		t.Fatalf("unacked = %d, want 4", s.rs.unacked())
	}
	if err := ws.Write(ctx, websocket.MessageBinary, wsEncode(frameAck, encodeOffset(4))); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool { return s.rs.unacked() == 0 })

	// A second WebSocket displaces the first rather than getting 409.
	ws2 := dialWS(t, ts, cr.SessionID)
	if _, _, err := ws.Read(ctx); err == nil {
		t.Fatal("displaced websocket still readable")
	}
	s.Write([]byte("to ws2"))
	for {
		_, msg, err := ws2.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if ft, payload, _ := wsDecode(msg); ft == frameSeqData {
			off, data, _ := splitSeq(payload)
			if off != 4 || string(data) != "to ws2" {
				t.Fatalf("ws2 frame = (%d, %q)", off, data)
			}
			break
		}
	}
}

// ---------------------------------------------------------------------------
// the two-hop relay, the deployment this feature exists for
// ---------------------------------------------------------------------------

// ★ consumer → broker → provider, both hops behind a proxy that keeps
// killing connections, a tunnelled stream carrying a bulk transfer the whole
// time. With both hops resumable the stream never notices: no reconnect, no
// new session id on either side, every byte intact. This is the SSH-through-
// two-CDN-legs scenario from the design document.
func TestRelayStreamSurvivesKillsOnBothHopsWithResume(t *testing.T) {
	const token = "broker-token"
	b := newMiniBrokerOpts(t, token, false, PollModeStream, true)
	providerProxy := newFaultProxy(t, b.ts.URL)
	consumerProxy := newFaultProxy(t, b.ts.URL)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	p := runMiniProviderOpts(t, ctx, providerProxy.URL(), token, "home", false, true, true)
	c := runMiniConsumerOpts(t, ctx, consumerProxy.URL(), token, "home", false, true, true)
	c.waitReady(t)
	consumerSession := <-c.sessions
	providerSession := <-p.sessions
	waitFor(t, 10*time.Second, func() bool { _, ok := b.providerYamux("home"); return ok })

	target := echoTarget(t)
	stream := c.dial(t, target)
	defer stream.Close()

	const chunk = 16 << 10
	payload := make([]byte, 128*chunk) // 2MB
	rand.Read(payload)
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

	for i := 0; i < len(payload); i += chunk {
		if _, err := stream.Write(payload[i : i+chunk]); err != nil {
			t.Fatalf("Write at %d: %v", i, err)
		}
		switch (i / chunk) % 20 {
		case 7:
			consumerProxy.killAll()
		case 14:
			providerProxy.killAll()
		}
	}

	select {
	case got := <-readDone:
		if got == nil {
			t.Fatal("the tunnelled echo came back short")
		}
		if sha256.Sum256(got) != want {
			t.Fatal("tunnelled bytes differ from what was sent")
		}
	case <-time.After(60 * time.Second):
		t.Fatal("tunnelled echo did not complete across the kills")
	}

	// Neither role reconnected: no outcome fired, no new session id, and the
	// broker still holds exactly the two original sessions.
	select {
	case out := <-c.outcomes:
		t.Fatalf("consumer's Serve ended with %v; a resumed session must not end", out)
	case out := <-p.outcomes:
		t.Fatalf("provider's Serve ended with %v; a resumed session must not end", out)
	default:
	}
	select {
	case id := <-c.sessions:
		t.Fatalf("consumer got a new session %q (was %q)", id, consumerSession)
	case id := <-p.sessions:
		t.Fatalf("provider got a new session %q (was %q)", id, providerSession)
	default:
	}
	if b.store.Len() != 2 {
		t.Fatalf("broker holds %d sessions, want 2", b.store.Len())
	}
	for _, s := range b.store.All() {
		if !s.Resumable() {
			t.Fatalf("broker session %s lost resumability", s.ID)
		}
	}
}

// Same relay, resume off: the same kills end the stream and both roles
// reconnect with fresh sessions — today's behaviour, unchanged.
func TestRelayStreamDiesOnKillWithoutResume(t *testing.T) {
	const token = "broker-token"
	b := newMiniBrokerOpts(t, token, false, PollModeStream, false)
	consumerProxy := newFaultProxy(t, b.ts.URL)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runMiniProviderOpts(t, ctx, b.ts.URL, token, "home", false, true, false)
	c := runMiniConsumerOpts(t, ctx, consumerProxy.URL(), token, "home", false, true, false)
	c.waitReady(t)
	first := <-c.sessions
	waitFor(t, 10*time.Second, func() bool { _, ok := b.providerYamux("home"); return ok })

	stream := c.dial(t, echoTarget(t))
	defer stream.Close()
	stream.Write([]byte("ping"))
	buf := make([]byte, 4)
	if _, err := io.ReadFull(stream, buf); err != nil {
		t.Fatal(err)
	}

	consumerProxy.killAll()
	select {
	case out := <-c.outcomes:
		if out != OutcomeTransportFailed {
			t.Fatalf("outcome = %v, want transport failure", out)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("consumer never noticed the kill")
	}
	if second := waitForNewSession(t, c.sessions, first); second == first {
		t.Fatal("consumer reused the dead session id")
	}
}
