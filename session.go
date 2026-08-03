package pollmux

import (
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"time"
)

// Session is one client's virtual connection, seen from the server side. It
// implements io.ReadWriteCloser so it can be handed straight to yamux.Client or
// yamux.Server.
//
// It holds transport state only — no application semantics. Applications keep
// their own data in their own registry and relate it by Session.ID, which is
// why this library needs neither generics nor any, and callers keep their type
// safety. Whatever the client declared at connect time is available, read-only,
// through Meta.
//
// Data flow:
//
//	client POST body  → toServer pipe → Session.Read()  → application/yamux
//	application/yamux → Session.Write() → toClient pipe → client poll response
type Session struct {
	// ID is the server-generated session identifier. Applications use it as the
	// key relating this session to their own per-session state.
	ID string

	toServer *BufferedPipe // client uploads, read by the application
	toClient *BufferedPipe // application writes, drained by client polls

	meta map[string]string

	// pollInFlight counts long polls currently parked in the handler. A value
	// above zero means a TCP connection is being held open by this client right
	// now, so the session is demonstrably alive and must not be evicted — and
	// when that TCP connection does break, the handler returns and the count
	// drops immediately, which is what turns a silent client death into a fast
	// detection instead of a session_timeout wait (A3).
	pollInFlight atomic.Int32

	mu         sync.Mutex
	lastActive time.Time
	closed     bool
}

// newSession creates a session with initialized pipes. meta is copied, so later
// mutation by the caller cannot affect the session.
func newSession(id string, meta map[string]string) *Session {
	return &Session{
		ID:         id,
		toServer:   NewBufferedPipe(),
		toClient:   NewBufferedPipe(),
		meta:       maps.Clone(meta),
		lastActive: time.Now(),
	}
}

// watchHighWater arms the high-water warning on both pipes.
func (s *Session) watchHighWater(threshold int, logger *slog.Logger) {
	if threshold <= 0 || logger == nil {
		return
	}
	s.toServer.WatchHighWater(threshold, logger.With("session_id", s.ID, "direction", "to_server"))
	s.toClient.WatchHighWater(threshold, logger.With("session_id", s.ID, "direction", "to_client"))
}

// Read reads data the client sent.
func (s *Session) Read(p []byte) (int, error) {
	return s.toServer.Read(p)
}

// Write queues data for delivery to the client on its next poll.
func (s *Session) Write(p []byte) (int, error) {
	return s.toClient.Write(p)
}

// Close closes both pipes, unblocking anything waiting on them. Idempotent.
//
// A long poll parked in ReadAvailable gets io.EOF and answers 410, so the client
// reconnects within seconds rather than polling a dead session until its own
// timeout expires.
func (s *Session) Close() error {
	s.closeOnce()
	return nil
}

// closeOnce closes the session and reports whether this call was the one that
// did it. Callers that fire a disconnect notification use that to guarantee it
// happens exactly once even when a delete and an eviction race.
func (s *Session) closeOnce() bool {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false
	}
	s.closed = true
	s.mu.Unlock()

	s.toServer.Close()
	s.toClient.Close()
	return true
}

// writeUpstream queues data the client sent for the application to read.
func (s *Session) writeUpstream(p []byte) (int, error) {
	return s.toServer.Write(p)
}

// IsClosed reports whether Close has been called.
func (s *Session) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// LastActive returns the time of the most recent request on this session.
func (s *Session) LastActive() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastActive
}

// touch records that a request just arrived.
func (s *Session) touch() {
	s.mu.Lock()
	s.lastActive = time.Now()
	s.mu.Unlock()
}

// PollInFlight returns the number of long polls currently parked on this
// session. Above zero means the client is demonstrably still connected.
func (s *Session) PollInFlight() int32 {
	return s.pollInFlight.Load()
}

// Meta returns a copy of the metadata the client declared at connect time,
// merged with whatever Hooks.Authenticate added. Read-only by construction:
// mutating the returned map does not affect the session.
func (s *Session) Meta() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.meta)
}

// HiWater returns the high-water mark of each direction's buffer, in bytes.
// See BufferedPipe for what bounds these.
func (s *Session) HiWater() (toServer, toClient int) {
	return s.toServer.HiWater(), s.toClient.HiWater()
}
