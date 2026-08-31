package pollmux

import (
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestCloseSessionIfNoPollInFlight(t *testing.T) {
	st := NewSessionStore()
	s := newSession("idle", nil)
	st.add(s)

	var mu sync.Mutex
	calls := 0
	h := Hooks{OnDisconnect: func(got *Session, reason DisconnectReason) {
		mu.Lock()
		defer mu.Unlock()
		if got != s || reason != ReasonServerClose {
			t.Errorf("OnDisconnect(%p, %v), want (%p, %v)", got, reason, s, ReasonServerClose)
		}
		calls++
	}}

	if !CloseSessionIfNoPollInFlight(st, h, s, ReasonServerClose) {
		t.Fatal("idle close returned false")
	}
	if !s.IsClosed() || st.Len() != 0 {
		t.Fatal("idle close did not close and remove the session")
	}
	if CloseSessionIfNoPollInFlight(st, h, s, ReasonServerClose) {
		t.Fatal("second idle close returned true")
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("OnDisconnect called %d times, want 1", calls)
	}
}

func TestCloseSessionIfNoPollInFlightRejectsAttachedTransport(t *testing.T) {
	st := NewSessionStore()
	s := newSession("busy", nil)
	st.add(s)
	if !s.beginPoll() {
		t.Fatal("beginPoll failed")
	}

	if CloseSessionIfNoPollInFlight(st, Hooks{}, s, ReasonServerClose) {
		t.Fatal("closed a session with a poll attached")
	}
	if s.IsClosed() || st.Len() != 1 {
		t.Fatal("failed conditional close changed the session")
	}

	s.endPoll()
	if !CloseSessionIfNoPollInFlight(st, Hooks{}, s, ReasonServerClose) {
		t.Fatal("close failed after poll detached")
	}
}

func TestCloseSessionIfNoPollInFlightChecksStoreIdentity(t *testing.T) {
	st := NewSessionStore()
	old := newSession("same", nil)
	current := newSession("same", nil)
	st.add(current)

	if CloseSessionIfNoPollInFlight(st, Hooks{}, old, ReasonServerClose) {
		t.Fatal("closed a non-current session object")
	}
	if got, ok := st.Get("same"); !ok || got != current {
		t.Fatal("store's current session was changed")
	}
}

func TestCloseSessionIfNoPollInFlightRejectsWebSocketThenClosesAfterDetach(t *testing.T) {
	st := NewSessionStore()
	s := newSession("ws", nil)
	st.add(s)
	if !s.beginWebSocket() {
		t.Fatal("beginWebSocket failed")
	}
	if CloseSessionIfNoPollInFlight(st, Hooks{}, s, ReasonServerClose) {
		t.Fatal("closed a session with a websocket attached")
	}
	s.endWebSocket()
	if !CloseSessionIfNoPollInFlight(st, Hooks{}, s, ReasonServerClose) {
		t.Fatal("close failed after websocket detached")
	}
}

func TestBeginPollAndConditionalCloseAreLinearizable(t *testing.T) {
	for i := 0; i < 1000; i++ {
		st := NewSessionStore()
		s := newSession("race", nil)
		st.add(s)

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var began, closed bool
		go func() { defer wg.Done(); <-start; began = s.beginPoll() }()
		go func() {
			defer wg.Done()
			<-start
			closed = CloseSessionIfNoPollInFlight(st, Hooks{}, s, ReasonServerClose)
		}()
		close(start)
		wg.Wait()

		if began == closed {
			t.Fatalf("iteration %d: beginPoll=%v close=%v; exactly one must win", i, began, closed)
		}
		if began {
			s.endPoll()
		}
	}
}

func TestConditionalCloseRacesCloseSessionExactlyOnce(t *testing.T) {
	for i := 0; i < 200; i++ {
		st := NewSessionStore()
		s := newSession("race-close", nil)
		st.add(s)
		var mu sync.Mutex
		calls := 0
		h := Hooks{OnDisconnect: func(*Session, DisconnectReason) { mu.Lock(); calls++; mu.Unlock() }}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; CloseSession(st, h, s, ReasonServerClose) }()
		go func() { defer wg.Done(); <-start; CloseSessionIfNoPollInFlight(st, h, s, ReasonEvicted) }()
		close(start)
		wg.Wait()
		mu.Lock()
		if calls != 1 {
			t.Fatalf("iteration %d: OnDisconnect calls = %d, want 1", i, calls)
		}
		mu.Unlock()
	}
}

func TestConditionalCloseRacesDeleteExactlyOnce(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	h := Hooks{OnDisconnect: func(*Session, DisconnectReason) { mu.Lock(); calls++; mu.Unlock() }}
	ts, st := newTestServer(t, testServerConfig(), h)
	cr := connectOK(t, ts)
	s, _ := st.Get(cr.SessionID)

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/tunnel/"+cr.SessionID, nil)
		resp, err := ts.Client().Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}()
	go func() { defer wg.Done(); <-start; CloseSessionIfNoPollInFlight(st, h, s, ReasonEvicted) }()
	close(start)
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("OnDisconnect calls = %d, want 1", calls)
	}
}

func TestConditionalCloseRacesSweeperExactlyOnce(t *testing.T) {
	st := NewSessionStore()
	s := newSession("sweep-race", nil)
	s.mu.Lock()
	s.lastActive = time.Now().Add(-time.Hour)
	s.mu.Unlock()
	st.add(s)
	var mu sync.Mutex
	calls := 0
	callback := func(*Session) { mu.Lock(); calls++; mu.Unlock() }

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); <-start; st.sweep(time.Millisecond, callback) }()
	go func() {
		defer wg.Done()
		<-start
		CloseSessionIfNoPollInFlight(st, Hooks{OnDisconnect: func(*Session, DisconnectReason) { callback(s) }}, s, ReasonEvicted)
	}()
	close(start)
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("disconnect/eviction calls = %d, want 1", calls)
	}
}
