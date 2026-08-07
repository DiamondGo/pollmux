package pollmux

import (
	"context"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// withArtificialDelay simulates network RTT for a benchmark: httptest.Server
// runs in-process, so there is no real round trip to measure against. Delaying
// the poll endpoint specifically — not connect, not send — is what makes the
// "one poll_buffer_bytes per RTT" ceiling (README) reproducible in-process:
// every batch-mode poll round trip, and every stream-mode poll's initial
// header, pays this delay once.
func withArtificialDelay(h http.Handler, rtt time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(rtt)
		h.ServeHTTP(w, r)
	})
}

// benchmarkThroughput times how long it takes payloadMB of server-generated
// data to reach the client over a link with rtt of artificial latency on the
// poll endpoint. Run with -benchtime=1x: each call already does a full
// multi-second transfer, so there is nothing to gain from repeating it, and
// letting the default benchmark loop pick its own N just repeats the same
// measurement pointlessly.
func benchmarkThroughput(b *testing.B, mode string, rtt time.Duration, payloadMB int) {
	st := NewSessionStore()
	cfg := ServerConfig{
		PollTimeout:    2 * time.Second,
		SessionTimeout: 10 * time.Second,
		SweepInterval:  time.Second,
		PollMode:       mode,
	}
	if mode == PollModeStream {
		cfg.HeartbeatInterval = 500 * time.Millisecond
		cfg.StreamMaxDuration = 30 * time.Second
	}

	mux := http.NewServeMux()
	mux.Handle("POST /tunnel/connect", ConnectHandler(st, cfg, Hooks{}))
	mux.Handle("POST /tunnel/{id}/poll", withArtificialDelay(PollHandler(st, cfg, Hooks{}), rtt))
	mux.Handle("DELETE /tunnel/{id}", DeleteHandler(st, cfg, Hooks{}))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	c := &Connector{BaseURL: ts.URL, PollGrace: 5 * time.Second, PreferStream: mode == PollModeStream}
	conn, err := c.Connect(context.Background())
	if err != nil {
		b.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	s, ok := st.Get(conn.SessionID())
	if !ok {
		b.Fatal("session not found on server after connect")
	}

	payload := make([]byte, payloadMB<<20)
	rand.Read(payload)

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 64<<10)
		total := 0
		for total < len(payload) {
			n, err := conn.Read(buf)
			if err != nil {
				done <- err
				return
			}
			total += n
		}
		done <- nil
	}()

	b.ResetTimer()
	start := time.Now()

	if _, err := s.Write(payload); err != nil {
		b.Fatalf("Write: %v", err)
	}
	if err := <-done; err != nil {
		b.Fatalf("Read: %v", err)
	}

	elapsed := time.Since(start)
	b.StopTimer()
	b.ReportMetric(float64(len(payload))/(1<<20)/elapsed.Seconds(), "MB/s")
}

func BenchmarkThroughput_Batch_150msRTT(b *testing.B) {
	benchmarkThroughput(b, PollModeBatch, 150*time.Millisecond, 8)
}

func BenchmarkThroughput_Stream_150msRTT(b *testing.B) {
	benchmarkThroughput(b, PollModeStream, 150*time.Millisecond, 8)
}
