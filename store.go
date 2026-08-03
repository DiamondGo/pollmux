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

// sweep evicts every session that has gone quiet past timeout with no poll
// parked on it.
func (st *SessionStore) sweep(timeout time.Duration, onEvict func(*Session)) {
	now := time.Now()
	var expired []*Session

	st.mu.Lock()
	for id, s := range st.sessions {
		if s.PollInFlight() > 0 {
			continue
		}
		if now.Sub(s.LastActive()) <= timeout {
			continue
		}
		delete(st.sessions, id)
		expired = append(expired, s)
	}
	st.mu.Unlock()

	// Closing and notifying happen outside the store lock: onEvict runs
	// application code, which must not be able to deadlock the store.
	//
	// closeOnce gates the callback so that a session already closed elsewhere
	// — a DELETE landing at the same moment, say — is not announced twice.
	for _, s := range expired {
		if s.closeOnce() && onEvict != nil {
			onEvict(s)
		}
	}
}
