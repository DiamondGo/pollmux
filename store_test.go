package pollmux

import (
	"sync"
	"testing"
	"time"
)

func TestSessionStoreAddGetRemove(t *testing.T) {
	st := NewSessionStore()

	if _, ok := st.Get("missing"); ok {
		t.Fatal("Get on an empty store reported a hit")
	}

	s := newSession("s1", nil)
	st.add(s)

	got, ok := st.Get("s1")
	if !ok {
		t.Fatal("Get after add reported a miss")
	}
	if got != s {
		t.Fatal("Get returned a different session")
	}
	if n := st.Len(); n != 1 {
		t.Fatalf("Len = %d, want 1", n)
	}

	st.Remove("s1")
	if _, ok := st.Get("s1"); ok {
		t.Fatal("Get after Remove reported a hit")
	}

	// Remove must not close the session — DeleteHandler closes it explicitly,
	// and a caller removing a session it still holds should keep it usable.
	if s.IsClosed() {
		t.Fatal("Remove closed the session; it must only unindex it")
	}
}

func TestSessionStoreAllSnapshot(t *testing.T) {
	st := NewSessionStore()
	st.add(newSession("s1", nil))
	st.add(newSession("s2", nil))

	all := st.All()
	if len(all) != 2 {
		t.Fatalf("All returned %d sessions, want 2", len(all))
	}

	// The snapshot must not be affected by later store mutation.
	st.Remove("s1")
	if len(all) != 2 {
		t.Fatalf("All result changed after Remove: %d sessions", len(all))
	}
}

func TestSweeperEvictsIdleSession(t *testing.T) {
	st := NewSessionStore()
	s := newSession("idle", nil)
	st.add(s)

	evicted := make(chan *Session, 1)
	stop := st.StartSweeper(5*time.Millisecond, 20*time.Millisecond, func(s *Session) {
		evicted <- s
	})
	defer stop()

	select {
	case got := <-evicted:
		if got != s {
			t.Fatal("onEvict got a different session")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("idle session was never evicted")
	}

	if _, ok := st.Get("idle"); ok {
		t.Fatal("evicted session is still in the store")
	}
	// The session must be closed before onEvict runs, so a poll parked on it
	// gets EOF and the client is told to reconnect.
	if !s.IsClosed() {
		t.Fatal("evicted session was not closed")
	}
}

// A3: a poll parked on the session means the client is holding a TCP connection
// open right now, so the session is demonstrably alive no matter how stale
// LastActive looks. Evicting it would kill a healthy client.
func TestSweeperSkipsSessionWithPollInFlight(t *testing.T) {
	st := NewSessionStore()
	s := newSession("polling", nil)
	s.pollInFlight.Add(1)
	st.add(s)

	evicted := make(chan *Session, 1)
	stop := st.StartSweeper(5*time.Millisecond, 20*time.Millisecond, func(s *Session) {
		evicted <- s
	})
	defer stop()

	// Well past the 20ms timeout — it must survive purely on the parked poll.
	select {
	case <-evicted:
		t.Fatal("evicted a session with a poll in flight")
	case <-time.After(150 * time.Millisecond):
	}
	if _, ok := st.Get("polling"); !ok {
		t.Fatal("session with a poll in flight disappeared from the store")
	}

	// Once the poll returns, the normal idle rule applies again.
	s.pollInFlight.Add(-1)
	select {
	case got := <-evicted:
		if got != s {
			t.Fatal("onEvict got a different session")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session was not evicted after its poll returned")
	}
}

func TestSweeperKeepsActiveSession(t *testing.T) {
	st := NewSessionStore()
	s := newSession("busy", nil)
	st.add(s)

	evicted := make(chan *Session, 1)
	stop := st.StartSweeper(5*time.Millisecond, 60*time.Millisecond, func(s *Session) {
		evicted <- s
	})
	defer stop()

	// Keep touching it the way a steady stream of requests would.
	deadline := time.After(200 * time.Millisecond)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.touch()
		case <-evicted:
			t.Fatal("evicted a session that was being touched")
		case <-deadline:
			if _, ok := st.Get("busy"); !ok {
				t.Fatal("active session disappeared from the store")
			}
			return
		}
	}
}

func TestSweeperStopIsIdempotent(t *testing.T) {
	st := NewSessionStore()
	stop := st.StartSweeper(5*time.Millisecond, time.Second, nil)

	stop()
	stop() // must not panic on a second close
}

// stop() waits for the sweeper goroutine, so a caller can rely on no further
// onEvict calls once it returns — that is what makes graceful shutdown orderly.
func TestSweeperStopEndsCallbacks(t *testing.T) {
	st := NewSessionStore()

	var mu sync.Mutex
	var calls int
	stop := st.StartSweeper(5*time.Millisecond, time.Millisecond, func(*Session) {
		mu.Lock()
		calls++
		mu.Unlock()
	})

	st.add(newSession("s1", nil))
	time.Sleep(50 * time.Millisecond)
	stop()

	mu.Lock()
	after := calls
	mu.Unlock()

	st.add(newSession("s2", nil))
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if calls != after {
		t.Fatalf("onEvict fired %d more times after stop() returned", calls-after)
	}
}

func TestSweeperNilCallbackIsAllowed(t *testing.T) {
	st := NewSessionStore()
	st.add(newSession("s1", nil))

	stop := st.StartSweeper(5*time.Millisecond, time.Millisecond, nil)
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if st.Len() == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("session was never evicted with a nil callback")
}

func TestSweeperDefaultsOnNonPositiveArgs(t *testing.T) {
	st := NewSessionStore()
	s := newSession("s1", nil)
	st.add(s)

	// Zero interval/timeout must fall back to the defaults rather than
	// spinning or evicting everything immediately.
	stop := st.StartSweeper(0, 0, nil)
	defer stop()

	time.Sleep(50 * time.Millisecond)
	if _, ok := st.Get("s1"); !ok {
		t.Fatal("session evicted under the default 60s timeout after 50ms")
	}
}
