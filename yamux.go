package pollmux

import (
	"io"
	"time"

	"github.com/hashicorp/yamux"
)

// MaxStreamWindowSize is the per-stream receive window pollmux uses.
//
// It is yamux's floor, not a preference: yamux refuses any smaller value
// (mux.go rejects MaxStreamWindowSize < initialStreamWindow, which is 256KB).
// Lowering it to add backpressure — the obvious-looking idea — is therefore not
// available, and would be counterproductive anyway: the window bounds how much
// data a stream can have in flight, so a 64KB window would cap a single stream
// at 64KB per round trip, about 430KB/s at 150ms RTT, undoing the poll buffer's
// throughput work. At 256KB the window and the default poll buffer match.
//
// The cost is memory. The window is what bounds the buffering — BufferedPipe
// has no backpressure of its own — so the worst case is roughly
// MaxStreamWindowSize × concurrent streams per tunnel, on the order of 64MB at
// 256 streams, multiplied by the number of tunnels on a multi-tenant server.
// The lever for lowering that is the number of concurrent streams, not the
// window. See BufferedPipe.HiWater for measuring where a deployment actually
// sits.
const MaxStreamWindowSize = 256 << 10

// YamuxConfig returns the yamux configuration a pollmux Conn requires.
//
// The one setting that is not a preference is EnableKeepAlive=false. yamux's
// keepalive expects a PING to complete a round trip within
// ConnectionWriteTimeout (10s), but over long polling a PONG can wait for the
// client's next poll, which may be up to a full poll timeout (30s) away. The
// result is yamux declaring a perfectly healthy connection dead. Liveness is
// the poll loop's job instead: the client's response header timeout on one
// side, session timeout plus pollInFlight on the other.
//
// Prefer ClientSession and ServerSession over calling this directly.
func YamuxConfig() *yamux.Config {
	cfg := yamux.DefaultConfig()

	// The correctness-critical line. Everything else here is tuning.
	cfg.EnableKeepAlive = false
	// Still required: VerifyConfig rejects a zero interval even when keepalive
	// is disabled.
	cfg.KeepAliveInterval = 30 * time.Second

	cfg.MaxStreamWindowSize = MaxStreamWindowSize

	// Exactly one of LogOutput and Logger may be set. yamux's own chatter is
	// not useful to callers, who have their own logger.
	cfg.LogOutput = io.Discard
	cfg.Logger = nil

	return cfg
}

// ClientSession builds the yamux session for the end that opens streams.
func ClientSession(conn io.ReadWriteCloser) (*yamux.Session, error) {
	return yamux.Client(conn, YamuxConfig())
}

// ServerSession builds the yamux session for the end that accepts streams.
func ServerSession(conn io.ReadWriteCloser) (*yamux.Session, error) {
	return yamux.Server(conn, YamuxConfig())
}
