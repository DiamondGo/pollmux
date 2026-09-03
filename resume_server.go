package pollmux

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// attachKind names the direction a poll handler owns on a resumable
// session. Stream mode has two independent legs — the long poll carrying
// data down and the send-stream carrying data up — each attached and
// detached on its own; a WebSocket owns both at once.
type attachKind int

const (
	attachDown attachKind = iota
	attachUp
	attachBoth
)

// attachment is one handler's claim on a direction. kick must make that
// handler return promptly from wherever it is blocked — a parked pipe wait,
// a network write to a peer that is no longer reading — and done is closed
// when it has actually let go.
type attachment struct {
	kick     func()
	done     chan struct{}
	doneOnce sync.Once
}

// attachments enforces one transport per direction on a resumable session.
// The reliable layer numbers bytes as it takes them from the pipe, so two
// handlers draining the same direction would interleave the stream (see
// reliable.nextOut); and a resume must know that the previous transport has
// stopped for good before it trusts the offsets it is about to exchange.
// Both needs are met the same way: whoever wants a direction kicks the
// current owner and waits for it to leave.
type attachments struct {
	mu       sync.Mutex
	down, up *attachment
	resuming bool
}

func newAttachments() *attachments { return &attachments{} }

// errStillAttached is attach/kickAll's report that the previous owner did
// not let go within detachWait. It is transient: the handler is presumably
// stuck in a network write that will fail on its own shortly, so the client
// is told to retry (503) rather than to give up.
var errStillAttached = errors.New("pollmux: previous transport has not detached yet")

// detachWait bounds how long a replacement waits for a kicked attachment to
// leave. The kick cancels every wait the old handler can be in, so this is
// a safety net rather than an expected delay.
const detachWait = 3 * time.Second

func (a *attachments) slot(kind attachKind) **attachment {
	if kind == attachUp {
		return &a.up
	}
	return &a.down
}

// attach claims kind for a new handler, kicking and waiting out any current
// owner first. attachBoth claims both directions (WebSocket).
func (a *attachments) attach(kind attachKind, kick func()) (*attachment, error) {
	att := &attachment{kick: kick, done: make(chan struct{})}
	kinds := []attachKind{kind}
	if kind == attachBoth {
		kinds = []attachKind{attachDown, attachUp}
	}
	for _, k := range kinds {
		for {
			a.mu.Lock()
			slot := a.slot(k)
			old := *slot
			if old == nil {
				*slot = att
				a.mu.Unlock()
				break
			}
			a.mu.Unlock()
			if err := old.kickAndWait(); err != nil {
				a.detach(att) // release whatever this call already claimed
				return nil, err
			}
		}
	}
	return att, nil
}

// detach releases every direction att owns and marks it done. Idempotent.
func (a *attachments) detach(att *attachment) {
	a.mu.Lock()
	if a.down == att {
		a.down = nil
	}
	if a.up == att {
		a.up = nil
	}
	a.mu.Unlock()
	att.doneOnce.Do(func() { close(att.done) })
}

// isCurrent reports whether att still owns at least one direction — a
// handler woken by an interrupt uses it to tell "you have been replaced"
// from "an ack is due".
func (a *attachments) isCurrent(att *attachment) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.down == att || a.up == att
}

// kickAll detaches every current owner and waits for them to leave, so the
// caller (ResumeHandler) can read final offsets.
func (a *attachments) kickAll() error {
	a.mu.Lock()
	olds := []*attachment{a.down, a.up}
	a.mu.Unlock()
	for _, old := range olds {
		if old == nil {
			continue
		}
		if err := old.kickAndWait(); err != nil {
			return err
		}
	}
	return nil
}

func (att *attachment) kickAndWait() error {
	att.kick()
	select {
	case <-att.done:
		return nil
	case <-time.After(detachWait):
		return errStillAttached
	}
}

// beginResume makes resume handshakes mutually exclusive. A client's own
// retry can overlap a slow first attempt; the second one is told to come
// back (503), never to give up.
func (a *attachments) beginResume() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.resuming {
		return false
	}
	a.resuming = true
	return true
}

func (a *attachments) endResume() {
	a.mu.Lock()
	a.resuming = false
	a.mu.Unlock()
}

// ResumeHandler serves POST {prefix}/{id}/resume, the handshake a resumable
// client performs after its transport failed and before it attaches a new
// one. Register it alongside ConnectHandler/PollHandler/DeleteHandler (and
// WebSocketHandler); it only ever succeeds for a session ConnectHandler
// negotiated Resumable for (see ServerConfig.EnableResume).
//
// The exchange is symmetric: the client says how many downstream bytes it
// has received contiguously, the server rewinds its send side to exactly
// there and answers with the same number for the upstream direction. Both
// ends then replay from those offsets on the next transport, and neither
// yamux sees anything but a pause.
//
// Before trusting its own offsets the handler kicks whatever handler still
// owns either direction and waits for it to leave — a transport the client
// has already given up on may still look attached here for as long as it
// takes the server to notice. Any inconsistency (an offset outside what the
// replay buffer can honour, a session whose reliable layer already broke)
// is refused with 409, and the client falls back to a fresh session rather
// than resuming with the wrong bytes.
//
// Status codes: 200 resumed; 404 unknown session; 409 cannot be resumed
// (not negotiated, broken, or an offset out of range — give up); 410 closed;
// 426 protocol version; 503 try again shortly (a previous transport is
// still letting go, or another resume is in flight).
func ResumeHandler(st *SessionStore, cfg ServerConfig, h Hooks) http.Handler {
	cfg.check()
	cfg = cfg.withDefaults()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := cfg.SessionIDFunc(r)
		s, ok := st.Get(id)
		if !ok {
			writeError(w, http.StatusNotFound, "session not found")
			return
		}

		var req ResumeRequest
		body := http.MaxBytesReader(w, r.Body, 64<<10)
		if err := json.NewDecoder(body).Decode(&req); err != nil && err != io.EOF {
			writeError(w, http.StatusBadRequest, "malformed resume request: "+err.Error())
			return
		}
		if req.ProtocolVersion != ProtocolVersion {
			writeError(w, http.StatusUpgradeRequired,
				fmt.Sprintf("unsupported protocol_version %d, this server speaks %d", req.ProtocolVersion, ProtocolVersion))
			return
		}

		if s.rs == nil {
			writeError(w, http.StatusConflict, "session was not negotiated as resumable")
			return
		}
		// Counting as an attached transport for the duration of the
		// handshake is what keeps the sweeper and any conditional close
		// from evicting the session between the offset exchange and the
		// client's next attach; and endPoll restarts the grace clock.
		if !s.beginPoll() {
			writeError(w, http.StatusGone, "session closed")
			return
		}
		defer s.endPoll()
		s.touch()

		if s.rs.isBroken() {
			writeError(w, http.StatusConflict, "session is no longer resumable")
			return
		}
		if !s.att.beginResume() {
			writeError(w, http.StatusServiceUnavailable, "another resume is in progress")
			return
		}
		defer s.att.endResume()

		if err := s.att.kickAll(); err != nil {
			cfg.Logger.Debug("pollmux: resume waited out its detach budget", "session_id", id, "error", err)
			writeError(w, http.StatusServiceUnavailable, "previous transport is still detaching, retry")
			return
		}

		if !s.rs.resumeOut(req.RecvOffset) {
			cfg.Logger.Warn("pollmux: refusing resume with an offset the replay buffer cannot honour",
				"session_id", id, "client_recv_offset", req.RecvOffset)
			s.rs.markBroken()
			writeError(w, http.StatusConflict, "recv_offset is outside the replayable range")
			return
		}

		upRecv := s.rs.recvOffsetNow()
		cfg.Logger.Info("pollmux: session resumed",
			"session_id", id, "client_recv_offset", req.RecvOffset, "server_recv_offset", upRecv)
		writeJSON(w, http.StatusOK, ResumeResponse{Resumed: true, RecvOffset: upRecv})
	})
}

// negotiateResume decides whether a session is made resumable: the server
// must enable it, the client must ask, and the negotiated transport must be
// one the reliable layer covers — WebSocket, or stream mode in both
// directions. Batch mode is left out on purpose: one response per message
// gives the seam nothing to replay into.
func negotiateResume(cfg ServerConfig, req ConnectRequest, transport, pollMode, uploadMode string) bool {
	if !cfg.EnableResume || !req.PreferResume {
		return false
	}
	if transport == TransportWebSocket {
		return true
	}
	return pollMode == PollModeStream && uploadMode == PollModeStream
}
