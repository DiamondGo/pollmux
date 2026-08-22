package pollmux

import (
	"context"
	"net"

	"github.com/hashicorp/yamux"
)

// AcceptLoop accepts yamux streams until the session ends, handing each one to
// handle in its own goroutine. It returns the Outcome to feed back to
// ReconnectLoop.
//
// It exists because four different things can end a session, and watching only
// some of them delays reconnection:
//
//   - ctx cancelled — we are shutting down;
//   - Conn.TransportFailed — the link broke;
//   - yamux CloseChan — the session ended, though not necessarily why;
//   - an Accept error — same, observed from the other direction.
//
// The last two need a second look at TransportFailed to be classified. A yamux
// close on its own is ambiguous: the transport may have died, or the server may
// have closed this session deliberately because our peer left. Those want
// opposite responses — back off versus retry at once — so the check is not
// optional.
func AcceptLoop(ctx context.Context, sess *yamux.Session, conn Conn, handle func(net.Conn)) Outcome {
	type acceptResult struct {
		stream net.Conn
		err    error
	}
	// Accept blocks, so it runs on its own goroutine and reports through a
	// channel that the select below can watch alongside everything else.
	acceptCh := make(chan acceptResult, 1)
	go func() {
		for {
			stream, err := sess.Accept()
			acceptCh <- acceptResult{stream, err}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return OutcomeShutdown

		case <-conn.TransportFailed():
			return OutcomeTransportFailed

		case <-conn.SessionSuperseded():
			return OutcomeSuperseded

		case <-sess.CloseChan():
			return classifyClose(conn)

		case res := <-acceptCh:
			if res.err != nil {
				return classifyClose(conn)
			}
			go handle(res.stream)
		}
	}
}

// classifyClose decides whether a finished yamux session means the link broke
// or the peer simply left.
func classifyClose(conn Conn) Outcome {
	select {
	case <-conn.TransportFailed():
		return OutcomeTransportFailed
	default:
		return OutcomePeerClosed
	}
}
