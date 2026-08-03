package pollmux

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
)

// yamuxPair wires a client and a server session over net.Pipe, each behind a
// Conn so transport failure can be simulated independently of yamux.
type yamuxPair struct {
	clientConn, serverConn *fakeConn
	client, server         *yamux.Session
}

func newYamuxPair(t *testing.T) *yamuxPair {
	t.Helper()
	a, b := net.Pipe()
	p := &yamuxPair{clientConn: newFakeConn(a), serverConn: newFakeConn(b)}

	var err error
	if p.client, err = ClientSession(p.clientConn); err != nil {
		t.Fatalf("ClientSession: %v", err)
	}
	if p.server, err = ServerSession(p.serverConn); err != nil {
		t.Fatalf("ServerSession: %v", err)
	}
	t.Cleanup(func() {
		p.client.Close()
		p.server.Close()
	})
	return p
}

func TestAcceptLoopHandsOffStreams(t *testing.T) {
	p := newYamuxPair(t)

	var mu sync.Mutex
	var payloads []string
	handled := make(chan struct{}, 3)

	go AcceptLoop(context.Background(), p.server, p.serverConn, func(stream net.Conn) {
		defer stream.Close()
		buf := make([]byte, 32)
		n, _ := stream.Read(buf)
		mu.Lock()
		payloads = append(payloads, string(buf[:n]))
		mu.Unlock()
		handled <- struct{}{}
	})

	for _, msg := range []string{"one", "two", "three"} {
		stream, err := p.client.Open()
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if _, err := stream.Write([]byte(msg)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	for range 3 {
		select {
		case <-handled:
		case <-time.After(5 * time.Second):
			t.Fatal("not every stream reached the handler")
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(payloads) != 3 {
		t.Fatalf("handled %d streams, want 3", len(payloads))
	}
}

func TestAcceptLoopReturnsShutdownOnContextCancel(t *testing.T) {
	p := newYamuxPair(t)
	ctx, cancel := context.WithCancel(context.Background())

	result := make(chan Outcome, 1)
	go func() {
		result <- AcceptLoop(ctx, p.server, p.serverConn, func(net.Conn) {})
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case got := <-result:
		if got != OutcomeShutdown {
			t.Fatalf("AcceptLoop = %v, want OutcomeShutdown", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("AcceptLoop ignored its cancelled context")
	}
}

// The link breaking has to be noticed even though yamux itself is still happy —
// which is exactly the case where watching only the yamux session goes wrong.
func TestAcceptLoopReturnsTransportFailed(t *testing.T) {
	p := newYamuxPair(t)

	result := make(chan Outcome, 1)
	go func() {
		result <- AcceptLoop(context.Background(), p.server, p.serverConn, func(net.Conn) {})
	}()

	time.Sleep(30 * time.Millisecond)
	p.serverConn.fail()

	select {
	case got := <-result:
		if got != OutcomeTransportFailed {
			t.Fatalf("AcceptLoop = %v, want OutcomeTransportFailed", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("AcceptLoop did not notice the transport failing")
	}
}

// ★ A yamux close on its own is ambiguous. With the transport still healthy it
// means the peer left, which calls for an immediate retry rather than backoff.
func TestAcceptLoopClassifiesYamuxCloseAsPeerClosed(t *testing.T) {
	p := newYamuxPair(t)

	result := make(chan Outcome, 1)
	go func() {
		result <- AcceptLoop(context.Background(), p.server, p.serverConn, func(net.Conn) {})
	}()

	time.Sleep(30 * time.Millisecond)
	p.client.Close() // the peer goes away; our transport never failed

	select {
	case got := <-result:
		if got != OutcomePeerClosed {
			t.Fatalf("AcceptLoop = %v, want OutcomePeerClosed with a healthy transport", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("AcceptLoop did not return after the yamux session closed")
	}
}

// The same yamux close, but with the transport already dead, must be reported
// as a transport failure so the caller backs off instead of hammering.
func TestAcceptLoopClassifiesYamuxCloseAsTransportFailed(t *testing.T) {
	p := newYamuxPair(t)

	result := make(chan Outcome, 1)
	go func() {
		result <- AcceptLoop(context.Background(), p.server, p.serverConn, func(net.Conn) {})
	}()

	time.Sleep(30 * time.Millisecond)
	p.serverConn.fail()
	p.server.Close()

	select {
	case got := <-result:
		if got != OutcomeTransportFailed {
			t.Fatalf("AcceptLoop = %v, want OutcomeTransportFailed when the link died first", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("AcceptLoop did not return")
	}
}

func TestAcceptLoopReturnsOnAcceptError(t *testing.T) {
	p := newYamuxPair(t)

	result := make(chan Outcome, 1)
	go func() {
		result <- AcceptLoop(context.Background(), p.server, p.serverConn, func(net.Conn) {})
	}()

	time.Sleep(30 * time.Millisecond)
	p.server.Close() // Accept starts failing

	select {
	case got := <-result:
		if got != OutcomePeerClosed {
			t.Fatalf("AcceptLoop = %v, want OutcomePeerClosed", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("AcceptLoop did not return after Accept began failing")
	}
}
