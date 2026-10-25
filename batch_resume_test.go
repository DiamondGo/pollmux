package pollmux

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"
)

func connectResumableBatch(t *testing.T, ts *httptest.Server) ConnectResponse {
	t.Helper()
	resp, cr := postConnect(t, ts, ConnectRequest{ProtocolVersion: ProtocolVersion, PreferResume: true, BatchResume: true})
	if resp.StatusCode != http.StatusOK || !cr.Resumable || !cr.BatchResume || cr.PollMode == PollModeStream {
		t.Fatalf("batch connect = (%d, %+v)", resp.StatusCode, cr)
	}
	return cr
}

// rawBatch makes a fully bounded request, including the acknowledgement even
// when zero. It returns a closed response and the bytes read from its body.
func rawBatch(t *testing.T, ts *httptest.Server, id string, send bool, off, ack uint64, body []byte) (*http.Response, []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/tunnel/"+id+"/poll", bytes.NewReader(body))
	setBatchOffset(req.Header, HeaderRecvOffset, ack)
	if send {
		req.Header.Set(HeaderSendOnly, "true")
		setBatchOffset(req.Header, HeaderSendOffset, off)
	} else {
		req.Header.Set(HeaderReceiveOnly, "true")
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return resp, data
}

func requireBatchOffset(t *testing.T, resp *http.Response, name string, want uint64) {
	t.Helper()
	got, err := batchOffset(resp.Header, name)
	if err != nil || got != want {
		t.Fatalf("%s = (%d, %v), want %d", name, got, err, want)
	}
}

func TestBatchResumeUploadDeduplicatesAndReportsOffset(t *testing.T) {
	ts, st := newResumeTestServer(t, testResumeServerConfig(), Hooks{})
	cr := connectResumableBatch(t, ts)
	s, _ := st.Get(cr.SessionID)
	for _, tc := range []struct {
		off  uint64
		data string
		want uint64
	}{
		{0, "abc", 3}, {0, "abc", 3}, {1, "bcde", 5},
	} {
		resp, _ := rawBatch(t, ts, cr.SessionID, true, tc.off, 0, []byte(tc.data))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("upload = %d", resp.StatusCode)
		}
		requireBatchOffset(t, resp, HeaderRecvOffset, tc.want)
	}
	got := make([]byte, 5)
	if _, err := io.ReadFull(s, got); err != nil || string(got) != "abcde" {
		t.Fatalf("upstream = %q, %v", got, err)
	}
	if s.toServer.Buffered() != 0 {
		t.Fatal("duplicate bytes reached the application")
	}
	resp, rr := postResume(t, ts, cr.SessionID, ResumeRequest{ProtocolVersion: ProtocolVersion})
	if resp.StatusCode != http.StatusOK || rr.RecvOffset != 5 {
		t.Fatalf("resume = (%d, %+v)", resp.StatusCode, rr)
	}
}

func TestBatchResumeDownstreamReplaysOnlyMissingBytes(t *testing.T) {
	cfg := testResumeServerConfig()
	cfg.PollBufferSize = 5
	ts, st := newResumeTestServer(t, cfg, Hooks{})
	cr := connectResumableBatch(t, ts)
	s, _ := st.Get(cr.SessionID)
	s.Write([]byte("abcdefghij"))
	resp, data := rawBatch(t, ts, cr.SessionID, false, 0, 0, nil)
	if resp.StatusCode != http.StatusOK || string(data) != "abcde" {
		t.Fatalf("first batch = %d %q", resp.StatusCode, data)
	}
	requireBatchOffset(t, resp, HeaderSendOffset, 0)
	if s.rs.unacked() != 5 {
		t.Fatal("HTTP write released unacknowledged data")
	}

	// Treat only the first two bytes as received before the old link failed.
	resp, _ = postResume(t, ts, cr.SessionID, ResumeRequest{ProtocolVersion: ProtocolVersion, RecvOffset: 2})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("resume = %d", resp.StatusCode)
	}
	resp, data = rawBatch(t, ts, cr.SessionID, false, 0, 2, nil)
	if string(data) != "cde" {
		t.Fatalf("replay = %q", data)
	}
	requireBatchOffset(t, resp, HeaderSendOffset, 2)
	resp, data = rawBatch(t, ts, cr.SessionID, false, 0, 5, nil)
	if string(data) != "fghij" {
		t.Fatalf("new data = %q", data)
	}
	requireBatchOffset(t, resp, HeaderSendOffset, 5)
	resp, _ = rawBatch(t, ts, cr.SessionID, false, 0, 10, nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("idle = %d", resp.StatusCode)
	}
	requireBatchOffset(t, resp, HeaderRecvOffset, 0)
	if s.rs.unacked() != 0 {
		t.Fatal("next poll did not release acknowledged replay")
	}
}

// A poll whose response never reached the client (a stale request delivered
// late, say) is not followed by a handshake; the next poll's ack alone must be
// enough to resend what was lost.
func TestBatchResumePollRestartsAtItsAcknowledgement(t *testing.T) {
	cfg := testResumeServerConfig()
	cfg.PollBufferSize = 5
	ts, st := newResumeTestServer(t, cfg, Hooks{})
	cr := connectResumableBatch(t, ts)
	s, _ := st.Get(cr.SessionID)
	s.Write([]byte("abcdefghij"))
	if _, data := rawBatch(t, ts, cr.SessionID, false, 0, 0, nil); string(data) != "abcde" {
		t.Fatalf("lost batch = %q", data)
	}
	resp, data := rawBatch(t, ts, cr.SessionID, false, 0, 0, nil)
	if string(data) != "abcde" {
		t.Fatalf("poll after lost response = %q, want the lost bytes again", data)
	}
	requireBatchOffset(t, resp, HeaderSendOffset, 0)
	resp, data = rawBatch(t, ts, cr.SessionID, false, 0, 5, nil)
	if string(data) != "fghij" {
		t.Fatalf("next batch = %q", data)
	}
	requireBatchOffset(t, resp, HeaderSendOffset, 5)
	// A stale ack cannot rewind below what the client already confirmed.
	resp, data = rawBatch(t, ts, cr.SessionID, false, 0, 0, nil)
	if string(data) != "fghij" {
		t.Fatalf("poll with stale ack = %q", data)
	}
	requireBatchOffset(t, resp, HeaderSendOffset, 5)
	if !s.Resumable() {
		t.Fatal("stale ack broke resumability")
	}
}

// Proxies may re-frame an empty POST as chunked; only actual bytes make a
// receive-only poll malformed.
func TestBatchResumeChunkedReceiveOnlyPoll(t *testing.T) {
	ts, st := newResumeTestServer(t, testResumeServerConfig(), Hooks{})
	cr := connectResumableBatch(t, ts)
	s, _ := st.Get(cr.SessionID)
	s.Write([]byte("down"))
	for _, tc := range []struct {
		chunks string
		want   int
	}{{"0\r\n\r\n", http.StatusOK}, {"3\r\nabc\r\n0\r\n\r\n", http.StatusBadRequest}} {
		conn, err := net.DialTimeout("tcp", strings.TrimPrefix(ts.URL, "http://"), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Fprintf(conn, "POST /tunnel/%s/poll HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n%s: true\r\n%s: 0\r\n\r\n%s",
			cr.SessionID, HeaderReceiveOnly, HeaderRecvOffset, tc.chunks); err != nil {
			t.Fatal(err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(resp.Body)
		conn.Close()
		if resp.StatusCode != tc.want {
			t.Fatalf("chunked poll %q = %d, want %d", tc.chunks, resp.StatusCode, tc.want)
		}
		if tc.want == http.StatusOK && string(data) != "down" {
			t.Fatalf("chunked poll data = %q", data)
		}
	}
}

func TestBatchResumeRejectsInvalidRequests(t *testing.T) {
	cases := []struct {
		name    string
		headers map[string]string
		body    string
		broken  bool
	}{
		{"missing ack", map[string]string{HeaderSendOnly: "true", HeaderSendOffset: "0"}, "abc", false},
		{"missing sequence", map[string]string{HeaderSendOnly: "true", HeaderRecvOffset: "0"}, "abc", false},
		{"negative sequence", map[string]string{HeaderSendOnly: "true", HeaderSendOffset: "-1", HeaderRecvOffset: "0"}, "abc", false},
		{"overflow sequence", map[string]string{HeaderSendOnly: "true", HeaderSendOffset: "18446744073709551616", HeaderRecvOffset: "0"}, "abc", false},
		{"malformed ack", map[string]string{HeaderReceiveOnly: "true", HeaderRecvOffset: "abc"}, "", false},
		{"both directions", map[string]string{HeaderSendOnly: "true", HeaderReceiveOnly: "true", HeaderSendOffset: "0", HeaderRecvOffset: "0"}, "abc", false},
		{"no direction", map[string]string{HeaderRecvOffset: "0"}, "", false},
		{"poll with body", map[string]string{HeaderReceiveOnly: "true", HeaderRecvOffset: "0"}, "abc", false},
		{"unnumbered stream", map[string]string{HeaderSendStream: "true"}, "abc", false},
		{"gap", map[string]string{HeaderSendOnly: "true", HeaderSendOffset: "1", HeaderRecvOffset: "0"}, "abc", true},
		{"impossible ack", map[string]string{HeaderReceiveOnly: "true", HeaderRecvOffset: "1"}, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts, st := newResumeTestServer(t, testResumeServerConfig(), Hooks{})
			cr := connectResumableBatch(t, ts)
			resp := poll(t, ts, cr.SessionID, []byte(tc.body), tc.headers)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d", resp.StatusCode)
			}
			s, _ := st.Get(cr.SessionID)
			if s.rs.recvOffsetNow() != 0 || s.toServer.Buffered() != 0 {
				t.Fatal("invalid data entered session")
			}
			if s.Resumable() == tc.broken {
				t.Fatalf("Resumable = %v, broken = %v", s.Resumable(), tc.broken)
			}
		})
	}
}

func TestBatchResumeIncompleteUploadIsNotDelivered(t *testing.T) {
	cfg := testResumeServerConfig()
	ts, st := newResumeTestServer(t, cfg, Hooks{})
	cr := connectResumableBatch(t, ts)
	r := httptest.NewRequest(http.MethodPost, "/tunnel/"+cr.SessionID+"/poll", io.MultiReader(strings.NewReader("abc"), iotest.ErrReader(io.ErrUnexpectedEOF)))
	r.SetPathValue("id", cr.SessionID)
	r.Header.Set(HeaderSendOnly, "true")
	setBatchOffset(r.Header, HeaderRecvOffset, 0)
	setBatchOffset(r.Header, HeaderSendOffset, 0)
	w := httptest.NewRecorder()
	PollHandler(st, cfg, Hooks{}).ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", w.Code)
	}
	s, _ := st.Get(cr.SessionID)
	if s.rs.recvOffsetNow() != 0 || s.toServer.Buffered() != 0 {
		t.Fatal("partial upload was delivered")
	}
	resp, _ := rawBatch(t, ts, cr.SessionID, true, 0, 0, []byte("abcdef"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("retry = %d", resp.StatusCode)
	}
	requireBatchOffset(t, resp, HeaderRecvOffset, 6)
}

func TestBatchResumeOversizedUploadClosesSession(t *testing.T) {
	cfg := testResumeServerConfig()
	cfg.MaxSendBytes = 4
	var closed atomic.Int32
	ts, st := newResumeTestServer(t, cfg, Hooks{OnDisconnect: func(_ *Session, reason DisconnectReason) {
		if reason != ReasonProtocolViolation {
			t.Errorf("reason = %v", reason)
		}
		closed.Add(1)
	}})
	cr := connectResumableBatch(t, ts)
	resp, _ := rawBatch(t, ts, cr.SessionID, true, 0, 0, []byte("12345"))
	if resp.StatusCode != http.StatusRequestEntityTooLarge || st.Len() != 0 || closed.Load() != 1 {
		t.Fatalf("oversized upload = status %d, sessions %d, callbacks %d", resp.StatusCode, st.Len(), closed.Load())
	}
}

func TestBatchResumeKicksParkedPoll(t *testing.T) {
	cfg := testResumeServerConfig()
	cfg.PollTimeout = 5 * time.Second
	cfg.SessionTimeout = 10 * time.Second
	ts, st := newResumeTestServer(t, cfg, Hooks{})
	cr := connectResumableBatch(t, ts)
	s, _ := st.Get(cr.SessionID)
	done := make(chan struct{})
	go func() {
		defer close(done)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/tunnel/"+cr.SessionID+"/poll", nil)
		req.Header.Set(HeaderReceiveOnly, "true")
		setBatchOffset(req.Header, HeaderRecvOffset, 0)
		if resp, err := ts.Client().Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	waitFor(t, time.Second, func() bool { return s.PollInFlight() == 1 })
	resp, _ := postResume(t, ts, cr.SessionID, ResumeRequest{ProtocolVersion: ProtocolVersion})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("resume = %d", resp.StatusCode)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("old poll did not detach promptly")
	}
}

func TestBatchResumeKicksIncompleteUploadBeforeExchangingOffsets(t *testing.T) {
	ts, st := newResumeTestServer(t, testResumeServerConfig(), Hooks{})
	cr := connectResumableBatch(t, ts)
	s, _ := st.Get(cr.SessionID)
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(ts.URL, "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Leave half of a declared body outstanding without closing the socket.
	if _, err := fmt.Fprintf(conn, "POST /tunnel/%s/poll HTTP/1.1\r\nHost: test\r\nContent-Length: 6\r\n%s: true\r\n%s: 0\r\n%s: 0\r\n\r\nabc",
		cr.SessionID, HeaderSendOnly, HeaderSendOffset, HeaderRecvOffset); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		s.att.mu.Lock()
		defer s.att.mu.Unlock()
		return s.att.up != nil
	})
	resp, rr := postResume(t, ts, cr.SessionID, ResumeRequest{ProtocolVersion: ProtocolVersion})
	if resp.StatusCode != http.StatusOK || rr.RecvOffset != 0 || s.toServer.Buffered() != 0 {
		t.Fatalf("resume with partial upload = %d, %+v", resp.StatusCode, rr)
	}
	conn.Close()
	resp, _ = rawBatch(t, ts, cr.SessionID, true, 0, 0, []byte("abcdef"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("replacement upload = %d", resp.StatusCode)
	}
	requireBatchOffset(t, resp, HeaderRecvOffset, 6)
}

func TestBatchResumeClientValidatesResponses(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		headers map[string]string
		body    string
	}{
		{"missing ack", 200, map[string]string{HeaderSendOffset: "0"}, "abc"},
		{"missing offset", 200, map[string]string{HeaderRecvOffset: "0"}, "abc"},
		{"bad offset", 200, map[string]string{HeaderRecvOffset: "0", HeaderSendOffset: "x"}, "abc"},
		{"gap", 200, map[string]string{HeaderRecvOffset: "0", HeaderSendOffset: "1"}, "abc"},
		{"bad ack", 204, map[string]string{HeaderRecvOffset: "1"}, ""},
		{"oversized", 200, map[string]string{HeaderRecvOffset: "0", HeaderSendOffset: "0"}, "123456789"},
		{"empty data", 200, map[string]string{HeaderRecvOffset: "0", HeaderSendOffset: "0"}, ""},
		{"truncated", 200, map[string]string{HeaderRecvOffset: "0", HeaderSendOffset: "0", "Content-Length": "8"}, "abc"},
		{"closed", 410, nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for k, v := range tc.headers {
					w.Header().Set(k, v)
				}
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			}))
			defer ts.Close()
			c := &resumableConn{rl: newReliable(NewBufferedPipe(), NewBufferedPipe(), 1024), pollClient: ts.Client(), pollURL: ts.URL, maxFrame: 8}
			if _, err := c.doBatchPoll(context.Background()); err == nil {
				t.Fatal("accepted invalid response")
			}
		})
	}
}

func TestBatchResumeClientDeduplicatesResponses(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setBatchOffset(w.Header(), HeaderRecvOffset, 0)
		setBatchOffset(w.Header(), HeaderSendOffset, 0)
		io.WriteString(w, "abc")
	}))
	defer ts.Close()
	c := &resumableConn{rl: newReliable(NewBufferedPipe(), NewBufferedPipe(), 1024), pollClient: ts.Client(), pollURL: ts.URL, maxFrame: 8}
	for i := 0; i < 2; i++ {
		if _, err := c.doBatchPoll(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if c.rl.in.Buffered() != 3 || c.rl.recvOffsetNow() != 3 {
		t.Fatal("replayed response reached application twice")
	}
}

func TestE2EResumeSurvivesTransportKills_HTTPBatchDirections(t *testing.T) {
	for _, mode := range []string{PollModeBatch, "stream-batch", "batch-stream"} {
		t.Run(mode, func(t *testing.T) { testE2EResumeSurvivesTransportKills(t, mode) })
	}
}

func TestE2EResumeIdleKillIsInvisible_HTTPBatchDirections(t *testing.T) {
	for _, mode := range []string{PollModeBatch, "stream-batch", "batch-stream"} {
		t.Run(mode, func(t *testing.T) { testE2EResumeIdleKillIsInvisible(t, mode) })
	}
}

// Deterministic faults at the two ambiguity points: after an upload has been
// delivered, and halfway through a download whose bytes left the server pipe.
func TestE2EBatchResumeLostResponses(t *testing.T) {
	for _, upload := range []bool{true, false} {
		t.Run(map[bool]string{true: "upload accepted, response lost", false: "download truncated"}[upload], func(t *testing.T) {
			var armed, injected atomic.Bool
			middleware := func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					match := r.Header.Get(HeaderReceiveOnly) == "true"
					if upload {
						match = r.Header.Get(HeaderSendOnly) == "true"
					}
					if !armed.Load() || !match {
						next.ServeHTTP(w, r)
						return
					}
					rec := httptest.NewRecorder()
					next.ServeHTTP(rec, r)
					if rec.Code == 200 && (upload || rec.Body.Len() > 1) && armed.CompareAndSwap(true, false) {
						injected.Store(true)
						if !upload {
							for k, vs := range rec.Header() {
								w.Header()[k] = vs
							}
							w.WriteHeader(rec.Code)
							w.Write(rec.Body.Bytes()[:rec.Body.Len()/2])
							w.(http.Flusher).Flush()
						}
						panic(http.ErrAbortHandler)
					}
					for k, vs := range rec.Header() {
						w.Header()[k] = vs
					}
					w.WriteHeader(rec.Code)
					w.Write(rec.Body.Bytes())
				})
			}
			e := newResumeE2E(t, PollModeBatch, nil, middleware)
			conn, sess := e.dial(t, nil)
			id := conn.SessionID()
			stream, err := sess.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			stream.SetDeadline(time.Now().Add(10 * time.Second))
			stream.Write([]byte("ready"))
			buf := make([]byte, 5)
			if _, err := io.ReadFull(stream, buf); err != nil {
				t.Fatal(err)
			}
			armed.Store(true)
			payload := bytes.Repeat([]byte("batch-resume"), 4096)
			if _, err := stream.Write(payload); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(stream, got); err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("echo corrupted: %v", err)
			}
			waitFor(t, 3*time.Second, func() bool { return e.resumes.Load() > 0 })
			if !injected.Load() || conn.SessionID() != id || sess.IsClosed() || isClosed(conn.TransportFailed()) || len(e.reasons()) != 0 {
				t.Fatal("fault did not preserve the original session and stream")
			}
		})
	}
}

func TestBatchResumeCapabilityIsBackwardCompatible(t *testing.T) {
	// Emulate an older server by ignoring the additive request capability.
	middleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/connect") {
				var req ConnectRequest
				json.NewDecoder(r.Body).Decode(&req)
				req.BatchResume = false
				body, _ := json.Marshal(req)
				r.Body = io.NopCloser(bytes.NewReader(body))
				r.ContentLength = int64(len(body))
			}
			next.ServeHTTP(w, r)
		})
	}
	e := newResumeE2E(t, PollModeBatch, nil, middleware)
	conn, sess := e.dial(t, nil)
	if _, ok := conn.(*resumableConn); ok {
		t.Fatal("older batch server unexpectedly enabled resume")
	}
	stream, err := sess.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	stream.SetDeadline(time.Now().Add(5 * time.Second))
	stream.Write([]byte("legacy"))
	got := make([]byte, 6)
	if _, err := io.ReadFull(stream, got); err != nil || string(got) != "legacy" {
		t.Fatalf("legacy echo = %q, %v", got, err)
	}
}

func TestBatchOffsetRejectsAmbiguousHeaders(t *testing.T) {
	for _, values := range [][]string{nil, {""}, {"-1"}, {"+1"}, {"1, 2"}, {"1", "2"}, {"18446744073709551616"}} {
		h := http.Header{}
		for _, v := range values {
			h.Add(HeaderRecvOffset, v)
		}
		if _, err := batchOffset(h, HeaderRecvOffset); err == nil {
			t.Fatalf("accepted headers %q", values)
		}
	}
	h := http.Header{}
	setBatchOffset(h, HeaderRecvOffset, ^uint64(0))
	if off, err := batchOffset(h, HeaderRecvOffset); err != nil || off != ^uint64(0) {
		t.Fatalf("max offset = %d, %v", off, err)
	}
}

func TestBatchResumeClientBoundsStalledResponseBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setBatchOffset(w.Header(), HeaderRecvOffset, 0)
		setBatchOffset(w.Header(), HeaderSendOffset, 0)
		w.Header().Set("Content-Length", strconv.Itoa(10))
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer ts.Close()
	client := ts.Client()
	client.Timeout = 50 * time.Millisecond
	c := &resumableConn{rl: newReliable(NewBufferedPipe(), NewBufferedPipe(), 1024), pollClient: client, pollURL: ts.URL, maxFrame: 16}
	if _, err := c.doBatchPoll(context.Background()); err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled body error = %v", err)
	}
}

func TestBatchResumeGraceExpiryEndsOriginalStreams(t *testing.T) {
	e := newResumeE2E(t, PollModeBatch, func(cfg *ServerConfig) { cfg.ResumeGrace = 300 * time.Millisecond }, nil)
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
	case <-time.After(5 * time.Second):
		t.Fatal("resume did not give up after grace expiry")
	}
	select {
	case <-sess.CloseChan():
	case <-time.After(time.Second):
		t.Fatal("original yamux session did not close")
	}
	waitFor(t, time.Second, func() bool { return e.store.Len() == 0 })
}

func TestBatchResumeDoesNotResurrectClosedSession(t *testing.T) {
	e := newResumeE2E(t, PollModeBatch, nil, nil)
	conn, _ := e.dial(t, nil)
	s, _ := e.store.Get(conn.SessionID())
	CloseSession(e.store, e.hooks, s, ReasonServerClose)
	select {
	case <-conn.TransportFailed():
	case <-time.After(3 * time.Second):
		t.Fatal("closed session did not fail")
	}
	if e.store.Len() != 0 || e.resumes.Load() != 0 {
		t.Fatal("client tried to resurrect a closed session")
	}
}

func TestBatchResumeReplayOverflowFailsOnlyAfterTransportLoss(t *testing.T) {
	e := newResumeE2E(t, PollModeBatch, nil, nil)
	conn, sess := e.dial(t, func(c *Connector) { c.MaxReplayBytes = 1024; c.MaxSendChunk = 8192 })
	stream, err := sess.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	stream.SetDeadline(time.Now().Add(5 * time.Second))
	payload := bytes.Repeat([]byte("x"), 8192)
	if _, err := stream.Write(payload); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(stream, got); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("echo after overflow = %v", err)
	}
	rc := conn.(*resumableConn)
	if !rc.rl.isBroken() || isClosed(conn.TransportFailed()) {
		t.Fatal("overflow must disable resume, not the live transport")
	}
	e.proxy.killAll()
	select {
	case <-conn.TransportFailed():
	case <-time.After(3 * time.Second):
		t.Fatal("overflowed replay was used to resume")
	}
}

func TestBatchResumeCloseWhileOfflineUnblocksReadersAndSupervisor(t *testing.T) {
	e := newResumeE2E(t, PollModeBatch, nil, nil)
	conn, sess := e.dial(t, nil)
	stream, err := sess.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	e.proxy.refuse.Store(true)
	e.proxy.killAll()
	done := make(chan error, 1)
	go func() { done <- conn.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close hung while offline")
	}
	select {
	case <-sess.CloseChan():
	case <-time.After(time.Second):
		t.Fatal("yamux read remained blocked after Close")
	}
}

func TestBatchResumeRejectsPollIntervalBeyondGrace(t *testing.T) {
	e := newResumeE2E(t, PollModeBatch, func(c *ServerConfig) { c.ResumeGrace = 100 * time.Millisecond }, nil)
	c := e.connector(func(c *Connector) { c.PollInterval = 100 * time.Millisecond })
	conn, err := c.Connect(context.Background())
	if conn != nil {
		conn.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "resume_grace") {
		t.Fatalf("bad interval error = %v", err)
	}
}

func TestBatchResumeRequestAttachmentsCannotRaceHandshake(t *testing.T) {
	a := newAttachments()
	if !a.beginResume() {
		t.Fatal("could not begin resume")
	}
	for _, kind := range []attachKind{attachUp, attachDown, attachBoth} {
		if _, err := a.attach(kind, func() {}); !errors.Is(err, errStillAttached) {
			t.Fatalf("attach during resume = %v", err)
		}
	}
	a.endResume()
	att, err := a.attach(attachBoth, func() {})
	if err != nil {
		t.Fatal(err)
	}
	a.detach(att)
}

func TestBatchResumePreservesMetadataAuthAndChunkLimits(t *testing.T) {
	var largest atomic.Int64
	var polls, sends, resumes atomic.Int32
	middleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer test-token" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.Header.Get(HeaderSendOnly) == "true" {
				sends.Add(1)
				for old := largest.Load(); r.ContentLength > old; old = largest.Load() {
					if largest.CompareAndSwap(old, r.ContentLength) {
						break
					}
				}
			}
			if r.Header.Get(HeaderReceiveOnly) == "true" {
				polls.Add(1)
				if r.Header.Get(HeaderLocalHealth) != "ok" {
					t.Error("local health was not forwarded")
				}
			}
			if strings.HasSuffix(r.URL.Path, "/resume") {
				resumes.Add(1)
			}
			next.ServeHTTP(w, r)
		})
	}
	e := newResumeE2E(t, PollModeBatch, func(c *ServerConfig) { c.MaxSendBytes = 2048 }, middleware)
	conn, sess := e.dial(t, func(c *Connector) {
		c.AuthToken = "test-token"
		c.Meta = map[string]string{"endpoint": "example"}
		c.LocalHealth = func() bool { return true }
		c.MaxSendChunk = 4096
	})
	stream, err := sess.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	stream.SetDeadline(time.Now().Add(10 * time.Second))
	for i := 0; i < 2; i++ {
		if i == 1 {
			e.proxy.killAll()
		}
		payload := bytes.Repeat([]byte("z"), 8192)
		if _, err := stream.Write(payload); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(stream, got); err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("echo = %v", err)
		}
	}
	if polls.Load() == 0 || sends.Load() < 8 || resumes.Load() == 0 || largest.Load() > 2048 || conn.Meta()["endpoint"] != "example" {
		t.Fatalf("limits/metadata not preserved: polls %d sends %d resumes %d largest %d meta %v", polls.Load(), sends.Load(), resumes.Load(), largest.Load(), conn.Meta())
	}
}

func TestBatchResumeConcurrentStreams(t *testing.T) {
	e := newResumeE2E(t, PollModeBatch, nil, nil)
	conn, sess := e.dial(t, nil)
	id := conn.SessionID()
	const count = 8
	ready := make(chan struct{}, count)
	start := make(chan struct{})
	done := make(chan error, count)
	for i := 0; i < count; i++ {
		go func(i int) {
			stream, err := sess.Open()
			if err != nil {
				ready <- struct{}{}
				done <- err
				return
			}
			defer stream.Close()
			stream.SetDeadline(time.Now().Add(10 * time.Second))
			ready <- struct{}{}
			<-start
			payload := bytes.Repeat([]byte{byte(i)}, 64<<10)
			for j := 0; j < 4; j++ {
				if _, err := stream.Write(payload); err != nil {
					done <- err
					return
				}
				got := make([]byte, len(payload))
				if _, err := io.ReadFull(stream, got); err != nil {
					done <- err
					return
				}
				if !bytes.Equal(got, payload) {
					done <- errors.New("stream contents interleaved or duplicated")
					return
				}
			}
			done <- nil
		}(i)
	}
	for i := 0; i < count; i++ {
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			t.Fatal("streams did not open")
		}
	}
	close(start)
	// A kill can land between finite batch requests and break nothing, so keep
	// going until one demonstrably forced a resume.
	for i := 0; i < 3 || e.resumes.Load() == 0; i++ {
		if i == 300 {
			t.Fatal("no transport kill forced a resume")
		}
		e.proxy.killAll()
		time.Sleep(15 * time.Millisecond)
	}
	for i := 0; i < count; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("stream did not recover")
		}
	}
	if conn.SessionID() != id || sess.IsClosed() || isClosed(conn.TransportFailed()) || e.resumes.Load() == 0 {
		t.Fatalf("concurrent streams did not retain their session: id changed=%v yamux closed=%v transport failed=%v resumes=%d",
			conn.SessionID() != id, sess.IsClosed(), isClosed(conn.TransportFailed()), e.resumes.Load())
	}
}

func TestBatchResumeOlderServerUploadProbeFallback(t *testing.T) {
	middleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get(HeaderSendStreamProbe) == "true" {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			if strings.HasSuffix(r.URL.Path, "/connect") {
				var req ConnectRequest
				json.NewDecoder(r.Body).Decode(&req)
				req.BatchResume = false
				body, _ := json.Marshal(req)
				r.Body = io.NopCloser(bytes.NewReader(body))
				r.ContentLength = int64(len(body))
			}
			next.ServeHTTP(w, r)
		})
	}
	e := newResumeE2E(t, PollModeStream, nil, middleware)
	conn, sess := e.dial(t, func(c *Connector) { c.UploadStreamPreference = "" })
	hc, ok := conn.(*httpConn)
	if !ok || hc.uploadStreamMode {
		t.Fatalf("legacy fallback = %T, want non-resumable discrete uploads", conn)
	}
	stream, err := sess.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	stream.SetDeadline(time.Now().Add(5 * time.Second))
	stream.Write([]byte("legacy"))
	buf := make([]byte, 6)
	if _, err := io.ReadFull(stream, buf); err != nil || string(buf) != "legacy" {
		t.Fatalf("legacy fallback echo = %q, %v", buf, err)
	}
}
