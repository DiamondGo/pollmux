package pollmux

import (
	"sync"
	"time"
)

// SessionStore holds live sessions by id. Applications normally do not add to
// it directly — ConnectHandler does that — but they read from it to answer
// status queries and to close everything during shutdown.
type SessionStore struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

// NewSessionStore creates an empty store.
func NewSessionStore() *SessionStore {
	return &SessionStore{sessions: make(map[string]*Session)}
}

// add registers a session. Called by ConnectHandler before Hooks.OnConnect, so
// that a poll racing ahead of the application's own bookkeeping still finds the
// session.
func (st *SessionStore) add(s *Session) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.sessions[s.ID] = s
}

// Get returns the session with the given id.
func (st *SessionStore) Get(id string) (*Session, bool) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	s, ok := st.sessions[id]
	return s, ok
}

// Remove drops a session from the store. It does not close the session.
func (st *SessionStore) Remove(id string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.sessions, id)
}

// All returns a snapshot of the live sessions.
func (st *SessionStore) All() []*Session {
	st.mu.RLock()
	defer st.mu.RUnlock()
	out := make([]*Session, 0, len(st.sessions))
	for _, s := range st.sessions {
		out = append(out, s)
	}
	return out
}

// Len returns the number of live sessions.
func (st *SessionStore) Len() int {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return len(st.sessions)
}

// StartSweeper scans every interval and evicts sessions idle longer than
// timeout. Each evicted session is removed from the store, closed, and then
// passed to onEvict (which may be nil).
//
// A session with a poll in flight is never evicted regardless of how old its
// last activity is: a parked poll means the client is holding a TCP connection
// open right now. Combined with a session timeout of twice the poll timeout,
// this is what brings worst-case detection down from minutes to roughly one
// poll cycle (A3).
//
// The returned stop function is idempotent and waits for the sweeper goroutine
// to exit, so a caller can rely on no further onEvict calls after it returns.
func (st *SessionStore) StartSweeper(interval, timeout time.Duration, onEvict func(*Session)) (stop func()) {
	if interval <= 0 {
		interval = DefaultSweepInterval
	}
	if timeout <= 0 {
		timeout = DefaultSessionTimeout
	}

	done := make(chan struct{})
	var wg sync.WaitGroup

	wg.Go(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				st.sweep(timeout, onEvict)
			}
		}
	})

	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
		wg.Wait()
	}
}

// removeAndMarkClosed removes s only if it is still the store's current object.
// Lock order throughout lifecycle operations is SessionStore.mu -> Session.mu.
func (st *SessionStore) removeAndMarkClosed(s *Session, requireIdle bool) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if current, ok := st.sessions[s.ID]; !ok || current != s {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || (requireIdle && s.pollInFlight != 0) {
		return false
	}
	s.closed = true
	delete(st.sessions, s.ID)
	return true
}

// sweep evicts every session that has gone quiet past timeout with no poll
// parked on it. The final expiry and transport checks happen while holding both
// lifecycle locks, so a poll cannot attach between checking and removal.
func (st *SessionStore) sweep(timeout time.Duration, onEvict func(*Session)) {
	now := time.Now()
	var expired []*Session

	st.mu.Lock()
	for id, s := range st.sessions {
		s.mu.Lock()
		if !s.closed && s.pollInFlight == 0 && now.Sub(s.lastActive) > timeout {
			s.closed = true
			delete(st.sessions, id)
			expired = append(expired, s)
		}
		s.mu.Unlock()
	}
	st.mu.Unlock()

	// Pipe closing and application callbacks always happen outside locks.
	for _, s := range expired {
		s.closePipes()
		if onEvict != nil {
			onEvict(s)
		}
	}
}
