package pollmux

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Outcome says why a Serve callback returned, which decides how the reconnect
// loop should behave next.
type Outcome int

const (
	// OutcomeTransportFailed: the link itself broke. Back off before retrying —
	// whatever is wrong is probably still wrong a moment later.
	OutcomeTransportFailed Outcome = iota
	// OutcomePeerClosed: the transport is fine, the peer went away. Reconnect
	// almost immediately; backing off here would leave a working link idle for
	// minutes waiting on a peer that may already be back.
	OutcomePeerClosed
	// OutcomeSuperseded: a newer connect from the same Connector already
	// replaced this session. Stop reconnecting — another loop or session is
	// already active.
	OutcomeSuperseded
	// OutcomeShutdown: the context was cancelled. Stop.
	OutcomeShutdown
)

func (o Outcome) String() string {
	switch o {
	case OutcomeTransportFailed:
		return "transport_failed"
	case OutcomePeerClosed:
		return "peer_closed"
	case OutcomeSuperseded:
		return "superseded"
	case OutcomeShutdown:
		return "shutdown"
	default:
		return "unknown"
	}
}

// ReconnectLoop connects, serves, and reconnects until its context is
// cancelled.
//
// It covers both shapes the two projects need: the simple one that just
// reconnects on failure, and the one that has to tell "the server is gone" from
// "the server closed my session because my peer left". Serve expresses that
// difference through its Outcome, and it is the caller's job to make that
// judgement — typically by re-checking Conn.TransportFailed after the yamux
// session closes, since a yamux close alone does not say which happened.
type ReconnectLoop struct {
	// Connect obtains a new connection. Required.
	Connect func(ctx context.Context) (Conn, error)
	// Serve runs until the connection ends and reports why. Required.
	// The loop closes the Conn afterwards.
	Serve func(ctx context.Context, conn Conn) Outcome

	// InitialBackoff defaults to DefaultInitialBackoff.
	InitialBackoff time.Duration
	// MaxBackoff defaults to DefaultMaxBackoff.
	MaxBackoff time.Duration
	// PeerClosedPause is the short pause after a peer-closed outcome, just
	// enough to avoid a tight spin. Defaults to DefaultPeerClosedPause.
	PeerClosedPause time.Duration
	// MinStableDuration is how long Serve must run before a transport failure
	// resets backoff to InitialBackoff. Defaults to DefaultMinStableDuration.
	// Set to -1 to disable (backoff keeps escalating through short-lived
	// sessions).
	MinStableDuration time.Duration

	// Logger receives reconnect diagnostics. Nil disables logging.
	Logger *slog.Logger
}

// Run drives the loop until ctx is cancelled, returning ctx.Err().
//
// It returns early only for a failure that retrying cannot fix: a server that
// rejects our protocol version. Backing off forever against that would hide a
// deployment mistake behind an infinite retry.
func (l *ReconnectLoop) Run(ctx context.Context) error {
	if l.Connect == nil || l.Serve == nil {
		return errors.New("pollmux: ReconnectLoop requires both Connect and Serve")
	}

	logger := l.Logger
	if logger == nil {
		logger = slog.New(discardHandler{})
	}

	initial := orDuration(l.InitialBackoff, DefaultInitialBackoff)
	maxBackoff := orDuration(l.MaxBackoff, DefaultMaxBackoff)
	peerPause := orDuration(l.PeerClosedPause, DefaultPeerClosedPause)
	minStable := DefaultMinStableDuration
	if l.MinStableDuration != 0 {
		minStable = l.MinStableDuration
	}
	if minStable < 0 {
		minStable = 0
	}

	backoff := initial

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		conn, err := l.Connect(ctx)
		if err != nil {
			if errors.Is(err, ErrProtocolVersion) {
				logger.Error("pollmux: server rejected our protocol version, not retrying", "error", err)
				return err
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.Warn("pollmux: connect failed, will retry", "error", err, "retry_in", backoff)
			if !sleepOrDone(ctx, backoff) {
				return ctx.Err()
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		logger.Info("pollmux: connected", "session_id", conn.SessionID())

		serveStart := time.Now()
		outcome := l.Serve(ctx, conn)
		conn.Close()
		servedFor := time.Since(serveStart)

		switch outcome {
		case OutcomeShutdown:
			return ctx.Err()

		case OutcomeSuperseded:
			logger.Info("pollmux: session superseded by a newer connect, stopping reconnect loop")
			return nil

		case OutcomePeerClosed:
			logger.Info("pollmux: peer closed the session, reconnecting promptly")
			if !sleepOrDone(ctx, peerPause) {
				return ctx.Err()
			}
			// Deliberately no backoff change: the link is healthy.

		default: // OutcomeTransportFailed
			if minStable > 0 && servedFor >= minStable {
				backoff = initial
			}
			logger.Warn("pollmux: transport failed, reconnecting with backoff",
				"retry_in", backoff, "served_for", servedFor)
			if !sleepOrDone(ctx, backoff) {
				return ctx.Err()
			}
			backoff = min(backoff*2, maxBackoff)
		}
	}
}

// sleepOrDone waits for d, reporting false if ctx finished first.
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
