package pollmux

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"time"
)

// PollModeBatch is the discrete-request poll mode: one send request in, one
// batched poll response out. Simple, but the downstream ceiling is one
// poll_buffer_bytes per round trip.
const PollModeBatch = "batch"

// PollModeStream is the low-latency downstream mode: PollHandler holds the
// long-poll response open and pushes framed chunks as they arrive instead of
// waiting for one ReadAvailable call and returning. See frame.go for the wire
// format inside the response body.
const PollModeStream = "stream"

// ServerConfig holds the server's half of the transport parameters. Every
// zero-valued field falls back to its documented default.
//
// The values here are authoritative: they are handed to each client at connect
// time, and the client clamps itself to them. That is what makes a mismatched
// pair of config files — the old failure mode — impossible.
type ServerConfig struct {
	// PollTimeout is how long a long poll waits before answering 204.
	PollTimeout time.Duration
	// SessionTimeout is how long a session may go idle before the sweeper
	// evicts it. Keep it at least twice PollTimeout, or a healthy client that
	// is simply between polls looks dead.
	SessionTimeout time.Duration
	// SweepInterval is how often the sweeper runs.
	SweepInterval time.Duration
	// CoalesceWindow is how long a poll response waits for more data once any
	// is available.
	CoalesceWindow time.Duration
	// PollBufferSize caps how many bytes one poll response may carry. This is
	// the downstream throughput knob: at best one buffer per round trip.
	PollBufferSize int
	// MaxSendBytes caps a single request body. Clients are told this value and
	// clamp to it, so exceeding it is a protocol violation, not a condition to
	// recover from.
	MaxSendBytes int

	// SessionIDFunc extracts the session id from a poll or delete request. The
	// zero value reads r.PathValue("id"), which suits net/http's own router;
	// gorilla/mux users pass func(r) string { return mux.Vars(r)["id"] }.
	SessionIDFunc func(*http.Request) string

	// HighWaterWarn logs a one-shot warning when either direction of a session
	// buffers this many bytes. Zero disables it. Useful for seeing how close a
	// deployment runs to the window × max_streams worst case.
	HighWaterWarn int

	// PollMode must be "", PollModeBatch, or PollModeStream. "" behaves
	// exactly like PollModeBatch.
	PollMode string

	// HeartbeatInterval is how long PollModeStream lets a poll go idle before
	// sending a heartbeat frame — the same role PollTimeout plays in batch
	// mode, reused as ReadAvailable's timeout argument. Ignored outside
	// stream mode.
	HeartbeatInterval time.Duration

	// StreamMaxDuration caps how long one stream-mode poll response may stay
	// open before the server ends it cleanly and the client reopens a new
	// one. This exists to stay under intermediate proxies' idle/read
	// timeouts, not for flow control — keep it comfortably below the
	// tightest timeout in your deployment's path (see README). Ignored
	// outside stream mode.
	StreamMaxDuration time.Duration

	// Logger receives diagnostics. Nil disables logging.
	Logger *slog.Logger
}

func (cfg ServerConfig) withDefaults() ServerConfig {
	if cfg.PollTimeout <= 0 {
		cfg.PollTimeout = DefaultPollTimeout
	}
	if cfg.SessionTimeout <= 0 {
		cfg.SessionTimeout = DefaultSessionTimeout
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = DefaultSweepInterval
	}
	if cfg.CoalesceWindow <= 0 {
		cfg.CoalesceWindow = DefaultCoalesceWindow
	}
	if cfg.PollBufferSize <= 0 {
		cfg.PollBufferSize = DefaultPollBufferSize
	}
	if cfg.MaxSendBytes <= 0 {
		cfg.MaxSendBytes = DefaultMaxSendBytes
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if cfg.StreamMaxDuration <= 0 {
		cfg.StreamMaxDuration = DefaultStreamMaxDuration
	}
	if cfg.SessionIDFunc == nil {
		cfg.SessionIDFunc = func(r *http.Request) string { return r.PathValue("id") }
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(discardHandler{})
	}
	return cfg
}

// check reports wiring mistakes that no runtime behaviour can paper over.
func (cfg ServerConfig) check() {
	switch cfg.PollMode {
	case "", PollModeBatch, PollModeStream:
	default:
		panic(fmt.Sprintf("pollmux: ServerConfig.PollMode = %q, only %q and %q are implemented",
			cfg.PollMode, PollModeBatch, PollModeStream))
	}
	if cfg.SessionTimeout > 0 && cfg.PollTimeout > 0 && cfg.SessionTimeout < 2*cfg.PollTimeout {
		panic(fmt.Sprintf("pollmux: ServerConfig.SessionTimeout=%v is below 2×PollTimeout=%v; "+
			"a healthy client that is merely between polls would be swept as dead",
			cfg.SessionTimeout, 2*cfg.PollTimeout))
	}
	// Same "fail at wiring time" spirit as the check above, for the
	// stream-mode equivalent knobs: a StreamMaxDuration too close to
	// HeartbeatInterval would force the stream to end almost as soon as it
	// opens.
	if cfg.PollMode == PollModeStream && cfg.StreamMaxDuration > 0 && cfg.HeartbeatInterval > 0 &&
		cfg.StreamMaxDuration < 2*cfg.HeartbeatInterval {
		panic(fmt.Sprintf("pollmux: ServerConfig.StreamMaxDuration=%v is below 2×HeartbeatInterval=%v; "+
			"the stream would barely open before being forced to end",
			cfg.StreamMaxDuration, 2*cfg.HeartbeatInterval))
	}
}

// limits is what gets handed down to clients at connect time for the given
// negotiated mode.
func (cfg ServerConfig) limits(mode string) Limits {
	l := Limits{
		MaxSendBytes:     cfg.MaxSendBytes,
		PollTimeoutMS:    cfg.PollTimeout.Milliseconds(),
		SessionTimeoutMS: cfg.SessionTimeout.Milliseconds(),
		PollBufferBytes:  cfg.PollBufferSize,
	}
	if mode == PollModeStream {
		l.HeartbeatIntervalMS = cfg.HeartbeatInterval.Milliseconds()
		l.StreamMaxDurationMS = cfg.StreamMaxDuration.Milliseconds()
	}
	return l
}

// negotiatePollMode decides the session's mode: stream only if the server is
// configured for it AND the client asked for it. The server is always the
// authority — a client that prefers stream against a batch-only server gets
// batch, silently, which is what lets old and new binaries interoperate.
func negotiatePollMode(serverMode string, clientPrefersStream bool) string {
	if serverMode == PollModeStream && clientPrefersStream {
		return PollModeStream
	}
	return PollModeBatch
}

// Hooks is where the application plugs its own logic in. Every field is
// optional.
type Hooks struct {
	// Authenticate vets a connect request. Returning an error rejects it: the
	// library answers 401 unless the error carries a status (see StatusError).
	// Any meta returned is merged over what the client declared and handed back
	// to the client in the connect response.
	Authenticate func(r *http.Request, req ConnectRequest) (meta map[string]string, err error)

	// OnConnect runs after the session is registered in the store and before
	// the response is written. This is where an application starts its yamux
	// goroutine and files the session in its own registry.
	//
	// The ordering is not incidental. Registering first is what stops the
	// client's first poll — which can arrive before this callback finishes —
	// from finding no session and getting a 404. Returning an error aborts the
	// connect and the session is discarded.
	OnConnect func(s *Session, meta map[string]string) error

	// OnPoll runs on every poll request, before the long-poll wait begins, so
	// it must return quickly. Use it to read piggybacked headers such as
	// X-Local-Health.
	OnPoll func(s *Session, r *http.Request)

	// OnDisconnect runs when a session ends, whatever ended it.
	OnDisconnect func(s *Session, reason DisconnectReason)
}

// StatusError lets a hook choose the HTTP status the library answers with.
// A plain error from Authenticate produces 401; wrap it to say otherwise.
type StatusError struct {
	Status int
	Err    error
}

func (e *StatusError) Error() string { return e.Err.Error() }
func (e *StatusError) Unwrap() error { return e.Err }

// StatusErrorf builds a StatusError, e.g.
// StatusErrorf(http.StatusBadRequest, "role must be consumer or provider").
func StatusErrorf(status int, format string, args ...any) error {
	return &StatusError{Status: status, Err: fmt.Errorf(format, args...)}
}

// ConnectHandler serves POST {prefix}/connect: it authenticates, creates a
// session, and hands the client the transport parameters it must obey.
func ConnectHandler(st *SessionStore, cfg ServerConfig, h Hooks) http.Handler {
	cfg.check()
	cfg = cfg.withDefaults()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ConnectRequest
		body := http.MaxBytesReader(w, r.Body, 64<<10)
		if err := json.NewDecoder(body).Decode(&req); err != nil && err != io.EOF {
			writeError(w, http.StatusBadRequest, "malformed connect request: "+err.Error())
			return
		}

		// Version first: nothing below is meaningful if we do not agree on the
		// wire format. 426 tells the client to stop retrying and upgrade.
		if req.ProtocolVersion != ProtocolVersion {
			cfg.Logger.Warn("pollmux: rejecting connect with an unsupported protocol version",
				"client_version", req.ProtocolVersion, "server_version", ProtocolVersion)
			writeError(w, http.StatusUpgradeRequired,
				fmt.Sprintf("unsupported protocol_version %d, this server speaks %d", req.ProtocolVersion, ProtocolVersion))
			return
		}

		var authMeta map[string]string
		if h.Authenticate != nil {
			var err error
			authMeta, err = h.Authenticate(r, req)
			if err != nil {
				status := http.StatusUnauthorized
				var se *StatusError
				if errors.As(err, &se) {
					status = se.Status
				}
				writeError(w, status, err.Error())
				return
			}
		}

		// Application-supplied meta wins: it is derived from credentials the
		// client cannot forge, unlike what the client declared about itself.
		meta := maps.Clone(req.Meta)
		if meta == nil {
			meta = make(map[string]string, len(authMeta))
		}
		maps.Copy(meta, authMeta)

		id, err := generateSessionID()
		if err != nil {
			cfg.Logger.Error("pollmux: failed to generate a session id", "error", err)
			writeError(w, http.StatusInternalServerError, "failed to generate session id")
			return
		}

		negotiatedMode := negotiatePollMode(cfg.PollMode, req.PreferStreamMode)

		s := newSession(id, meta)
		s.pollMode = negotiatedMode
		s.watchHighWater(cfg.HighWaterWarn, cfg.Logger)

		// Register before the callback. The client's first poll can beat
		// OnConnect to the server, and it must find the session waiting.
		st.add(s)

		if h.OnConnect != nil {
			if err := h.OnConnect(s, meta); err != nil {
				st.Remove(id)
				s.Close()
				cfg.Logger.Error("pollmux: OnConnect rejected the session", "session_id", id, "error", err)
				writeError(w, http.StatusInternalServerError, "failed to set up session")
				return
			}
		}

		cfg.Logger.Info("pollmux: session created", "session_id", id)

		writeJSON(w, http.StatusOK, ConnectResponse{
			ProtocolVersion: ProtocolVersion,
			SessionID:       id,
			Limits:          cfg.limits(negotiatedMode),
			Meta:            meta,
			PollMode:        negotiatedMode,
		})
	})
}

// PollHandler serves POST {prefix}/{id}/poll, which is two endpoints in one.
//
// X-Send-Only writes the body into the session and returns at once, never
// waiting. X-Receive-Only long-polls for downstream data. Splitting reads from
// writes this way is what keeps one direction from head-of-line blocking the
// other: a send never has to wait for the poll in flight.
func PollHandler(st *SessionStore, cfg ServerConfig, h Hooks) http.Handler {
	cfg.check()
	cfg = cfg.withDefaults()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := cfg.SessionIDFunc(r)
		s, ok := st.Get(id)
		if !ok {
			writeError(w, http.StatusNotFound, "session not found")
			return
		}

		s.touch()

		// Before the wait, not after: a callback that runs after a 30-second
		// park is useless for health reporting.
		if h.OnPoll != nil {
			h.OnPoll(s, r)
		}

		body := http.MaxBytesReader(w, r.Body, int64(cfg.MaxSendBytes))
		data, err := io.ReadAll(body)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				// Clients are told max_send_bytes at connect time and clamp to
				// it, so this cannot come from a correct client. Treat it as
				// the protocol violation it is: say so loudly and drop the
				// session, rather than negotiating downward and hiding the bug.
				cfg.Logger.Error("pollmux: closing session after a protocol violation — "+
					"request body exceeded the max_send_bytes handed down at connect time",
					"session_id", id, "max_send_bytes", cfg.MaxSendBytes)
				closeSession(st, h, s, ReasonProtocolViolation)
				writeError(w, http.StatusRequestEntityTooLarge,
					fmt.Sprintf("request body exceeds max_send_bytes=%d", cfg.MaxSendBytes))
				return
			}
			writeError(w, http.StatusBadRequest, "failed to read request body")
			return
		}

		if len(data) > 0 {
			if _, err := s.writeUpstream(data); err != nil {
				cfg.Logger.Debug("pollmux: dropping upstream data for a closed session",
					"session_id", id, "bytes", len(data), "error", err)
			}
		}

		if r.Header.Get(HeaderSendOnly) == "true" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if s.pollMode == PollModeStream {
			pollStream(w, r, s, cfg)
			return
		}

		// A parked poll is proof the client is still there, and the count
		// drops the moment this handler returns — including when the TCP
		// connection breaks underneath it.
		s.pollInFlight.Add(1)
		defer s.pollInFlight.Add(-1)

		buf := make([]byte, cfg.PollBufferSize)
		n, err := s.toClient.ReadAvailable(buf, cfg.PollTimeout, cfg.CoalesceWindow)

		if n > 0 {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			w.Write(buf[:n])
			return
		}

		if errors.Is(err, io.EOF) {
			// Not 204. Sharing the idle-heartbeat code here is what used to
			// leave a client polling a dead session until its own timeout
			// instead of reconnecting within seconds.
			writeError(w, http.StatusGone, "session closed")
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})
}

// pollStream serves the stream-mode half of a long poll: instead of one
// ReadAvailable call and one response, it holds the response open and keeps
// calling ReadAvailable in a loop, writing each result out as a frame and
// flushing immediately. cfg.HeartbeatInterval takes over ReadAvailable's
// timeout role — "how long to wait before answering" becomes "how long to
// wait before sending a heartbeat frame" — which is why ReadAvailable itself
// needs no changes.
func pollStream(w http.ResponseWriter, r *http.Request, s *Session, cfg ServerConfig) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		// Not reachable behind net/http's own server; guards against a
		// wrapping middleware that strips Flusher and would otherwise hang
		// this handler for cfg.StreamMaxDuration for no reason.
		writeError(w, http.StatusInternalServerError, "response writer does not support streaming")
		return
	}

	s.pollInFlight.Add(1)
	defer s.pollInFlight.Add(-1)

	// Headers must go out now, not on the first frame. The client's
	// ResponseHeaderTimeout for a stream-mode poll is short (it only needs
	// to cover connection setup, not a long-poll wait) — if this handler
	// waited for the first heartbeat/data frame before flushing headers, an
	// idle session would blow that timeout on every single stream poll.
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Accel-Buffering", "no") // best-effort: ask nginx-like proxies not to buffer
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	buf := make([]byte, cfg.PollBufferSize)
	deadline := time.Now().Add(cfg.StreamMaxDuration)

	for {
		select {
		case <-r.Context().Done():
			return // client or an intermediate proxy dropped the connection
		default:
		}

		n, err := s.toClient.ReadAvailable(buf, cfg.HeartbeatInterval, cfg.CoalesceWindow)
		switch {
		case errors.Is(err, io.EOF):
			// Session closed: toClient.Close() unblocks ReadAvailable with
			// io.EOF. This is fatal, unlike the StreamMaxDuration rollover
			// below — the session is gone, so a frameGone (not frameEnd)
			// tells the client to stop reopening polls against this session
			// id and reconnect instead, the same role batch mode's 410 plays.
			writeFrame(w, frameGone, nil)
			flusher.Flush()
			return
		case n > 0:
			if err := writeFrame(w, frameData, buf[:n]); err != nil {
				cfg.Logger.Debug("pollmux: stream write failed, ending stream poll",
					"session_id", s.ID, "error", err)
				return
			}
			flusher.Flush()
		default: // n == 0, err == nil: ReadAvailable's timeout fired — our heartbeat cadence
			if err := writeFrame(w, frameHeartbeat, nil); err != nil {
				cfg.Logger.Debug("pollmux: stream heartbeat write failed, ending stream poll",
					"session_id", s.ID, "error", err)
				return
			}
			flusher.Flush()
		}

		if time.Now().After(deadline) {
			writeFrame(w, frameEnd, nil)
			flusher.Flush()
			return
		}
	}
}

// DeleteHandler serves DELETE {prefix}/{id}, the clean way for a client to go
// away.
func DeleteHandler(st *SessionStore, cfg ServerConfig, h Hooks) http.Handler {
	cfg.check()
	cfg = cfg.withDefaults()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := cfg.SessionIDFunc(r)
		s, ok := st.Get(id)
		if !ok {
			writeError(w, http.StatusNotFound, "session not found")
			return
		}

		closeSession(st, h, s, ReasonClientDelete)
		cfg.Logger.Info("pollmux: session deleted", "session_id", id)
		w.WriteHeader(http.StatusOK)
	})
}

// CloseSession removes a session from the store, closes it, and reports the
// reason to Hooks.OnDisconnect. Use it for application-initiated teardown,
// notably graceful shutdown:
//
//	for _, s := range store.All() {
//	    pollmux.CloseSession(store, hooks, s, pollmux.ReasonServerClose)
//	}
//
// Closing the session makes any parked poll answer 410, so clients reconnect
// promptly instead of waiting out a timeout.
func CloseSession(st *SessionStore, h Hooks, s *Session, reason DisconnectReason) {
	closeSession(st, h, s, reason)
}

func closeSession(st *SessionStore, h Hooks, s *Session, reason DisconnectReason) {
	st.Remove(s.ID)
	// closeOnce is the guard: exactly one caller gets true, so a delete racing
	// an eviction cannot fire OnDisconnect twice.
	if s.closeOnce() && h.OnDisconnect != nil {
		h.OnDisconnect(s, reason)
	}
}

// StartSweeper runs the store's expiry scan with this config's timings, wired
// to Hooks.OnDisconnect. The returned stop function is idempotent and waits for
// the sweeper to finish.
func StartSweeper(st *SessionStore, cfg ServerConfig, h Hooks) (stop func()) {
	cfg.check()
	cfg = cfg.withDefaults()

	return st.StartSweeper(cfg.SweepInterval, cfg.SessionTimeout, func(s *Session) {
		cfg.Logger.Info("pollmux: evicting an idle session",
			"session_id", s.ID, "idle_for", time.Since(s.LastActive()))
		if h.OnDisconnect != nil {
			h.OnDisconnect(s, ReasonEvicted)
		}
	})
}

// generateSessionID returns a random 32-character hex string.
func generateSessionID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
