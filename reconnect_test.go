package pollmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

// nopRWC is a connection that does nothing, for loop tests that never move data.
type nopRWC struct{}

func (nopRWC) Read([]byte) (int, error)    { return 0, io.EOF }
func (nopRWC) Write(p []byte) (int, error) { return len(p), nil }
func (nopRWC) Close() error                { return nil }

// attemptRecorder timestamps each connect attempt so backoff behaviour can be
// measured rather than assumed.
type attemptRecorder struct {
	mu    sync.Mutex
	times []time.Time
}

func (a *attemptRecorder) record() {
	a.mu.Lock()
	a.times = append(a.times, time.Now())
	a.mu.Unlock()
}

func (a *attemptRecorder) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.times)
}

func (a *attemptRecorder) gaps() []time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []time.Duration
	for i := 1; i < len(a.times); i++ {
		out = append(out, a.times[i].Sub(a.times[i-1]))
	}
	return out
}

func TestReconnectLoopRequiresCallbacks(t *testing.T) {
	l := &ReconnectLoop{}
	if err := l.Run(context.Background()); err == nil {
		t.Fatal("Run succeeded with neither Connect nor Serve set")
	}
}

func TestReconnectLoopStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	l := &ReconnectLoop{
		Connect: func(context.Context) (Conn, error) { return newFakeConn(nopRWC{}), nil },
		Serve: func(ctx context.Context, _ Conn) Outcome {
			<-ctx.Done()
			return OutcomeShutdown
		},
	}

	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}
}

// An unreachable server is likely to stay unreachable for a moment, so
// successive attempts must spread out.
func TestReconnectLoopBacksOffOnRepeatedConnectFailure(t *testing.T) {
	var rec attemptRecorder
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := &ReconnectLoop{
		Connect: func(context.Context) (Conn, error) {
			rec.record()
			return nil, errors.New("server unreachable")
		},
		Serve:          func(context.Context, Conn) Outcome { return OutcomeTransportFailed },
		InitialBackoff: 30 * time.Millisecond,
		MaxBackoff:     120 * time.Millisecond,
	}

	go l.Run(ctx)
	waitForCount(t, 3*time.Second, &rec, 4)
	cancel()

	gaps := rec.gaps()
	if len(gaps) < 3 {
		t.Fatalf("only %d gaps recorded", len(gaps))
	}
	// 30ms, then 60ms, then 120ms. Generous lower bounds keep this stable on a
	// loaded machine while still proving growth.
	if gaps[0] > 55*time.Millisecond {
		t.Fatalf("first retry waited %v, want about the 30ms initial backoff", gaps[0])
	}
	if gaps[1] < 45*time.Millisecond {
		t.Fatalf("second retry waited %v, want it roughly doubled to 60ms", gaps[1])
	}
	if gaps[2] < 90*time.Millisecond {
		t.Fatalf("third retry waited %v, want it doubled again to 120ms", gaps[2])
	}
}

// A transport failure must pause before retrying, unlike a peer-closed outcome
// which only takes the short pause.
func TestReconnectLoopPausesAfterTransportFailure(t *testing.T) {
	var rec attemptRecorder
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := &ReconnectLoop{
		Connect: func(context.Context) (Conn, error) {
			rec.record()
			return newFakeConn(nopRWC{}), nil
		},
		Serve:           func(context.Context, Conn) Outcome { return OutcomeTransportFailed },
		InitialBackoff:  60 * time.Millisecond,
		MaxBackoff:      time.Second,
		PeerClosedPause: time.Millisecond, // must not be the one applied here
	}

	go l.Run(ctx)
	waitForCount(t, 3*time.Second, &rec, 3)
	cancel()

	for i, gap := range rec.gaps() {
		if gap < 40*time.Millisecond {
			t.Fatalf("gap %d was %v; a transport failure must wait the backoff, not the peer-closed pause", i, gap)
		}
	}
}

// A session that connects and dies immediately must escalate backoff rather
// than retry at InitialBackoff forever — that steady rate is what turns a
// reconnect replacement loop into a storm.
func TestReconnectLoopEscalatesBackoffOnFlapping(t *testing.T) {
	var rec attemptRecorder
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := &ReconnectLoop{
		Connect: func(context.Context) (Conn, error) {
			rec.record()
			return newFakeConn(nopRWC{}), nil
		},
		Serve:             func(context.Context, Conn) Outcome { return OutcomeTransportFailed },
		InitialBackoff:    30 * time.Millisecond,
		MaxBackoff:        2 * time.Second,
		MinStableDuration: 10 * time.Millisecond,
	}

	go l.Run(ctx)
	waitForCount(t, 3*time.Second, &rec, 5)
	cancel()

	gaps := rec.gaps()
	if len(gaps) < 3 {
		t.Fatalf("only %d gaps recorded", len(gaps))
	}
	if gaps[2] <= gaps[0] {
		t.Fatalf("backoff did not escalate on flapping sessions: gaps[0]=%v gaps[2]=%v", gaps[0], gaps[2])
	}
}

func TestReconnectLoopHonoursMaxBackoff(t *testing.T) {
	var rec attemptRecorder
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := &ReconnectLoop{
		Connect: func(context.Context) (Conn, error) {
			rec.record()
			return nil, errors.New("server unreachable")
		},
		Serve:          func(context.Context, Conn) Outcome { return OutcomeTransportFailed },
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     40 * time.Millisecond,
	}

	go l.Run(ctx)
	waitForCount(t, 3*time.Second, &rec, 5)
	cancel()

	for i, gap := range rec.gaps() {
		if gap > 200*time.Millisecond {
			t.Fatalf("gap %d was %v, want it capped near the 40ms MaxBackoff", i, gap)
		}
	}
}

// ★ The distinction the Outcome enum exists for. A peer leaving is not a link
// problem, and backing off would leave a healthy tunnel idle for minutes
// waiting on a peer that may already be back.
func TestReconnectLoopDoesNotBackOffOnPeerClosed(t *testing.T) {
	var rec attemptRecorder
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := &ReconnectLoop{
		Connect: func(context.Context) (Conn, error) {
			rec.record()
			return newFakeConn(nopRWC{}), nil
		},
		Serve:           func(context.Context, Conn) Outcome { return OutcomePeerClosed },
		InitialBackoff:  2 * time.Second, // would dominate if it were used
		MaxBackoff:      10 * time.Second,
		PeerClosedPause: 10 * time.Millisecond,
	}

	start := time.Now()
	go l.Run(ctx)
	waitForCount(t, 3*time.Second, &rec, 5)
	cancel()

	// Five attempts at the 2s backoff would take 8 seconds. At the peer-closed
	// pause it is well under one.
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("five peer-closed reconnects took %v; the backoff is being applied where it should not be", elapsed)
	}
}

// Backoff must come back down once the link recovers, or one bad patch leaves
// reconnects slow forever.
func TestReconnectLoopResetsBackoffAfterRecovery(t *testing.T) {
	var rec attemptRecorder
	var mu sync.Mutex
	attempt := 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := &ReconnectLoop{
		Connect: func(context.Context) (Conn, error) {
			rec.record()
			mu.Lock()
			defer mu.Unlock()
			attempt++
			// Fail the first three times so the backoff climbs to 240ms, then
			// let it connect.
			if attempt <= 3 {
				return nil, fmt.Errorf("attempt %d failed", attempt)
			}
			return newFakeConn(nopRWC{}), nil
		},
		Serve: func(context.Context, Conn) Outcome {
			time.Sleep(20 * time.Millisecond)
			return OutcomeTransportFailed
		},
		InitialBackoff:    30 * time.Millisecond,
		MaxBackoff:        2 * time.Second,
		MinStableDuration: 10 * time.Millisecond,
	}

	go l.Run(ctx)
	waitForCount(t, 5*time.Second, &rec, 6)
	cancel()

	gaps := rec.gaps()
	// gaps[3] follows the first successful connect. If the backoff had not been
	// reset it would still be around 240ms.
	if len(gaps) < 5 {
		t.Fatalf("only %d gaps recorded", len(gaps))
	}
	if gaps[3] > 120*time.Millisecond {
		t.Fatalf("the retry after a successful connect waited %v; the backoff was not reset", gaps[3])
	}
}

// Retrying cannot fix a version mismatch, and looping forever would hide a
// deployment mistake.
func TestReconnectLoopAbortsOnProtocolVersion(t *testing.T) {
	var rec attemptRecorder

	l := &ReconnectLoop{
		Connect: func(context.Context) (Conn, error) {
			rec.record()
			return nil, fmt.Errorf("connect: %w", ErrProtocolVersion)
		},
		Serve:          func(context.Context, Conn) Outcome { return OutcomeTransportFailed },
		InitialBackoff: 10 * time.Millisecond,
	}

	done := make(chan error, 1)
	go func() { done <- l.Run(context.Background()) }()

	select {
	case err := <-done:
		if !errors.Is(err, ErrProtocolVersion) {
			t.Fatalf("Run returned %v, want it to surface ErrProtocolVersion", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run kept retrying a protocol version mismatch")
	}

	if n := rec.count(); n != 1 {
		t.Fatalf("made %d connect attempts, want exactly 1", n)
	}
}

func TestReconnectLoopClosesEachConn(t *testing.T) {
	var mu sync.Mutex
	var closed int

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := &ReconnectLoop{
		Connect: func(context.Context) (Conn, error) {
			return newFakeConn(&countingRWC{onClose: func() {
				mu.Lock()
				closed++
				mu.Unlock()
			}}), nil
		},
		Serve:           func(context.Context, Conn) Outcome { return OutcomePeerClosed },
		PeerClosedPause: 5 * time.Millisecond,
	}

	go l.Run(ctx)
	time.Sleep(150 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if closed == 0 {
		t.Fatal("the loop never closed a connection it was finished with")
	}
}

type countingRWC struct {
	onClose func()
}

func (c *countingRWC) Read([]byte) (int, error)    { return 0, io.EOF }
func (c *countingRWC) Write(p []byte) (int, error) { return len(p), nil }
func (c *countingRWC) Close() error {
	c.onClose()
	return nil
}

func waitForCount(t *testing.T, timeout time.Duration, rec *attemptRecorder, want int) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if rec.count() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("only %d attempts within %v, want %d", rec.count(), timeout, want)
}
