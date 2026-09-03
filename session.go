package pollmux

import (
	"log/slog"
	"maps"
	"sync"
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

	// pollMode is the mode negotiated for this session at connect time:
	// PollModeBatch or PollModeStream. Set once by ConnectHandler before the
	// session is published via SessionStore.add, and never mutated after —
	// so PollHandler can read it with no lock, the same way it already
	// treats meta as effectively immutable post-construction.
	pollMode string

	// transport is "" or TransportWebSocket, set once by ConnectHandler
	// alongside pollMode and likewise never mutated after. WebSocketHandler
	// checks it against TransportWebSocket before accepting an attach, so a
	// client that fell back to poll-based transport (server had
	// EnableWebSocket off, or never asked) cannot attach a WebSocket to a
	// session that was never negotiated for one.
	transport string

	// wsAttached guards against a session having more than one WebSocket
	// attached at once: two goroutines both draining toClient via Read would
	// split traffic unpredictably between two connections. In practice a
	// session is only ever attached once — ReconnectLoop always calls
	// Connect for a fresh session id rather than reattaching to an old one —
	// but WebSocketHandler still checks this defensively rather than
	// assuming the caller never will.
	wsAttached bool

	// pollInFlight counts active poll requests and persistent transports attached
	// to the session, including send-only requests, stream requests before they
	// park, and WebSocket attachments. A nonzero value means a client transport
	// is currently attached and prevents eviction; it does not necessarily mean
	// a long poll is parked.
	// now, so the session is demonstrably alive and must not be evicted — and
	// when that TCP connection does break, the handler returns and the count
	// drops immediately, which is what turns a silent client death into a fast
	// detection instead of a session_timeout wait (A3).
	pollInFlight int32

	// rs and att are only set for a session that negotiated
	// ConnectResponse.Resumable, by ConnectHandler before the session is
	// published, and never mutated after (same discipline as pollMode). rs
	// is the byte layer that numbers, retains, and replays each direction
	// (see reliable.go); att tracks which poll/WebSocket handler currently
	// owns each direction so a replacement can kick a stale one off before
	// taking over (see resume_server.go). resumeGrace is how long the
	// session may sit with no transport attached before the sweeper gives
	// up on it.
	rs          *reliable
	att         *attachments
	resumeGrace time.Duration

	mu         sync.Mutex
	lastActive time.Time
	closed     bool
	// detachedAt is when pollInFlight last dropped to zero — the moment a
	// resumable session's grace period starts. Only meaningful when rs is
	// set; starts at creation so a session whose client never attaches at
	// all is still bounded by the grace.
	detachedAt time.Time
}

// newSession creates a session with initialized pipes. meta is copied, so later
// mutation by the caller cannot affect the session.
func newSession(id string, meta map[string]string) *Session {
	now := time.Now()
	return &Session{
		ID:         id,
		toServer:   NewBufferedPipe(),
		toClient:   NewBufferedPipe(),
		meta:       maps.Clone(meta),
		lastActive: now,
		detachedAt: now,
	}
}

// enableResume arms the reliable layer on a freshly created session. Called
// by ConnectHandler before the session is published, never after.
func (s *Session) enableResume(maxReplay int, grace time.Duration) {
	s.rs = newReliable(s.toClient, s.toServer, maxReplay)
	s.att = newAttachments()
	s.resumeGrace = grace
}

// Resumable reports whether this session negotiated resumable transport and
// can still be resumed. It turns false for good once a direction's replay
// buffer overflowed or the peer broke the protocol — from then on the
// session behaves exactly like a non-resumable one.
func (s *Session) Resumable() bool {
	return s.rs != nil && !s.rs.isBroken()
}

// ResumeDeadline returns when a detached resumable session's grace period
// runs out, and true, if the session is resumable and currently has no
// transport attached. Otherwise it returns the zero time and false. A
// reaper that closes sessions whose transport dropped (see
// CloseSessionIfNoPollInFlight) can use this to leave a session alone
// while its client may still come back.
func (s *Session) ResumeDeadline() (time.Time, bool) {
	if !s.Resumable() {
		return time.Time{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pollInFlight != 0 || s.closed {
		return time.Time{}, false
	}
	return s.detachedAt.Add(s.resumeGrace), true
}

// inResumeGraceLocked reports whether s is a detached resumable session
// still inside its grace period. s.mu must be held.
func (s *Session) inResumeGraceLocked(now time.Time) bool {
	return s.rs != nil && s.pollInFlight == 0 && !s.rs.isBroken() &&
		now.Sub(s.detachedAt) <= s.resumeGrace
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
	won := s.markClosedLocked()
	s.mu.Unlock()
	if won {
		s.closePipes()
	}
	return won
}

// markClosedLocked performs the session lifecycle transition. s.mu must be held.
func (s *Session) markClosedLocked() bool {
	if s.closed {
		return false
	}
	s.closed = true
	return true
}

func (s *Session) closePipes() {
	s.toServer.Close()
	s.toClient.Close()
}

// beginPoll atomically attaches a poll or persistent transport unless the
// session has already been closed.
func (s *Session) beginPoll() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.pollInFlight++
	return true
}

func (s *Session) endPoll() {
	s.mu.Lock()
	s.pollInFlight--
	s.noteDetachedLocked()
	s.mu.Unlock()
}

// noteDetachedLocked stamps detachedAt when the last transport leaves a
// resumable session. s.mu must be held.
func (s *Session) noteDetachedLocked() {
	if s.rs != nil && s.pollInFlight == 0 {
		s.detachedAt = time.Now()
	}
}

func (s *Session) beginWebSocket() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.wsAttached {
		return false
	}
	s.wsAttached = true
	s.pollInFlight++
	return true
}

func (s *Session) endWebSocket() {
	s.mu.Lock()
	s.wsAttached = false
	s.pollInFlight--
	s.noteDetachedLocked()
	s.mu.Unlock()
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

// PollInFlight returns an observational snapshot of attached polls and
// persistent transports. It must not be used to decide whether to close the
// session; use CloseSessionIfNoPollInFlight for that atomic operation.
func (s *Session) PollInFlight() int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pollInFlight
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
