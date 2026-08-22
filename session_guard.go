package pollmux

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
)

// sessionGuard tracks whether a Conn's session is still the active one on its
// Connector. When the same client_id reconnects, the server closes the old
// session and its polls answer 410 — that is not a transport failure on the
// link, just an obsolete session being replaced.
type sessionGuard struct {
	owner             *Connector
	sessionID         string
	sessionSuperseded chan struct{}
	supersedeOnce     sync.Once
	cancel            context.CancelFunc
}

func newSessionGuard(owner *Connector, sessionID string, cancel context.CancelFunc) sessionGuard {
	return sessionGuard{
		owner:             owner,
		sessionID:         sessionID,
		sessionSuperseded: make(chan struct{}),
		cancel:            cancel,
	}
}

func (g *sessionGuard) SessionSuperseded() <-chan struct{} {
	return g.sessionSuperseded
}

func (g *sessionGuard) supersededByNewerConnect(logger *slog.Logger) bool {
	if g.owner != nil && !g.owner.isActiveSession(g.sessionID) {
		if logger != nil {
			logger.Debug("pollmux: session closed but already superseded by a newer connect",
				"session_id", g.sessionID)
		}
		g.supersede()
		return true
	}
	return false
}

func (g *sessionGuard) supersede() {
	g.supersedeOnce.Do(func() {
		close(g.sessionSuperseded)
		if g.cancel != nil {
			g.cancel()
		}
	})
}

func isSessionClosedErr(err error) bool {
	if errors.Is(err, errSessionGone) {
		return true
	}
	var f *fatalPollError
	return errors.As(err, &f) && f.status == http.StatusGone
}
