package pollmux

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
)

// fakeConn adapts any ReadWriteCloser to the Conn interface, so the glue can be
// tested over net.Pipe instead of a real HTTP server.
type fakeConn struct {
	io.ReadWriteCloser
	failed chan struct{}
	once   sync.Once
}

func newFakeConn(rwc io.ReadWriteCloser) *fakeConn {
	return &fakeConn{ReadWriteCloser: rwc, failed: make(chan struct{})}
}

func (c *fakeConn) TransportFailed() <-chan struct{} { return c.failed }
func (c *fakeConn) Limits() Limits                   { return Limits{} }
func (c *fakeConn) SessionID() string                { return "fake" }
func (c *fakeConn) Meta() map[string]string          { return nil }
func (c *fakeConn) fail()                            { c.once.Do(func() { close(c.failed) }) }

func TestYamuxConfigIsValid(t *testing.T) {
	if err := yamux.VerifyConfig(YamuxConfig()); err != nil {
		t.Fatalf("YamuxConfig is rejected by yamux: %v", err)
	}
}

// The one setting that is a correctness requirement rather than tuning: with
// keepalive on, a PONG can be stuck behind a long poll for longer than
// ConnectionWriteTimeout, and yamux declares a healthy link dead.
func TestYamuxConfigDisablesKeepAlive(t *testing.T) {
	cfg := YamuxConfig()
	if cfg.EnableKeepAlive {
		t.Fatal("EnableKeepAlive is true; long polling cannot complete a PING round trip in time")
	}
	// It still has to be non-zero, or VerifyConfig rejects the config outright.
	if cfg.KeepAliveInterval == 0 {
		t.Fatal("KeepAliveInterval is zero, which VerifyConfig rejects even with keepalive off")
	}
}

func TestYamuxConfigWindowSitsAtTheFloor(t *testing.T) {
	if got := YamuxConfig().MaxStreamWindowSize; got != MaxStreamWindowSize {
		t.Fatalf("MaxStreamWindowSize = %d, want %d", got, MaxStreamWindowSize)
	}

	// Pin the reason it cannot be lowered: yamux rejects anything smaller, so
	// "shrink the window for backpressure" is not an option available to us.
	cfg := YamuxConfig()
	cfg.MaxStreamWindowSize = 64 << 10
	if err := yamux.VerifyConfig(cfg); err == nil {
		t.Fatal("yamux accepted a 64KB window; the documented 256KB floor no longer holds")
	}
}

func TestClientAndServerSessionsInteroperate(t *testing.T) {
	a, b := net.Pipe()
	connA, connB := newFakeConn(a), newFakeConn(b)

	client, err := ClientSession(connA)
	if err != nil {
		t.Fatalf("ClientSession: %v", err)
	}
	defer client.Close()

	server, err := ServerSession(connB)
	if err != nil {
		t.Fatalf("ServerSession: %v", err)
	}
	defer server.Close()

	got := make(chan string, 1)
	go func() {
		stream, err := server.Accept()
		if err != nil {
			return
		}
		defer stream.Close()
		buf := make([]byte, 32)
		n, _ := stream.Read(buf)
		got <- string(buf[:n])
	}()

	stream, err := client.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer stream.Close()
	if _, err := stream.Write([]byte("ping")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	select {
	case s := <-got:
		if s != "ping" {
			t.Fatalf("server received %q, want %q", s, "ping")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the server side never received the stream payload")
	}
}
