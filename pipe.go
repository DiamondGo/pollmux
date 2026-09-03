package pollmux

import (
	"io"
	"log/slog"
	"sync"
	"time"
)

// BufferedPipe is a thread-safe in-memory pipe for passing data between an HTTP
// handler goroutine and a yamux session goroutine.
//
// Write appends data and wakes blocked readers. Read blocks until data is
// available or the pipe is closed. ReadAvailable is like Read but returns what
// is available after a timeout — that is the long-poll primitive.
//
// A note on backpressure: Write never blocks, so the pipe has none of its own.
// What bounds it is yamux flow control upstream — a stream stops writing once
// its send window is exhausted, so at most MaxStreamWindowSize bytes per stream
// can be sitting here unacknowledged. That makes the worst case
// window × max_streams per tunnel. WatchHighWater exists so a deployment can
// see how close it actually runs to that bound.
type BufferedPipe struct {
	mu     sync.Mutex
	cond   *sync.Cond
	buf    []byte
	closed bool

	hiWater       int
	warnThreshold int
	warned        bool
	logger        *slog.Logger

	// intr is a level-triggered interrupt flag for ReadAvailable, set by
	// interrupt and consumed by the next ReadAvailable call. The resumable
	// transport (see reliable.go) uses it for two things that both need to
	// pull a parked writer out of its long wait early: kicking a stale
	// attachment off a session so a replacement can attach, and nudging
	// the writer to send an acknowledgement when enough data has arrived.
	// Read is deliberately unaffected — it has no timeout to cut short.
	intr bool
}

// NewBufferedPipe creates a new BufferedPipe ready for use.
func NewBufferedPipe() *BufferedPipe {
	p := &BufferedPipe{}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// WatchHighWater arms a one-shot warning, logged the first time the buffered
// byte count reaches threshold. Pass a logger already decorated with whatever
// identifying attributes matter (session id, direction). A non-positive
// threshold or a nil logger disables the warning; HiWater keeps working either
// way. Call before the pipe is shared between goroutines.
func (p *BufferedPipe) WatchHighWater(threshold int, logger *slog.Logger) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.warnThreshold = threshold
	p.logger = logger
}

// HiWater returns the largest number of bytes ever buffered at once. This is
// the number to watch against the window × max_streams worst case.
func (p *BufferedPipe) HiWater() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hiWater
}

// Buffered returns the number of bytes currently buffered.
func (p *BufferedPipe) Buffered() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.buf)
}

// Write appends data to the buffer and signals waiting readers.
// Returns io.ErrClosedPipe if the pipe is closed.
func (p *BufferedPipe) Write(data []byte) (int, error) {
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return 0, io.ErrClosedPipe
	}

	p.buf = append(p.buf, data...)
	p.cond.Signal()

	warn := false
	if n := len(p.buf); n > p.hiWater {
		p.hiWater = n
		if p.warnThreshold > 0 && !p.warned && n >= p.warnThreshold {
			p.warned = true
			warn = true
		}
	}
	logger, hi, threshold := p.logger, p.hiWater, p.warnThreshold
	p.mu.Unlock()

	// Logged outside the lock: readers should never wait on a log write.
	if warn && logger != nil {
		logger.Warn("pollmux: buffered pipe passed its high-water threshold",
			"buffered_bytes", hi,
			"threshold_bytes", threshold,
		)
	}

	return len(data), nil
}

// Read blocks until data is available, then copies into dst.
// Returns io.EOF if the pipe is closed and empty.
func (p *BufferedPipe) Read(dst []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for len(p.buf) == 0 {
		if p.closed {
			return 0, io.EOF
		}
		p.cond.Wait()
	}

	n := copy(dst, p.buf)
	p.buf = p.buf[n:]
	return n, nil
}

// ReadAvailable reads whatever data is currently available, in two phases.
//
// Phase 1: if the pipe is empty, wait up to timeout for the first byte to
// arrive. This is the actual long-poll wait.
//
// Phase 2: once at least one byte is available, wait a further, much shorter
// coalesceWindow for more to accumulate (capped by len(dst)) instead of
// flushing immediately. Without this a poll response goes out the instant a
// single byte lands, and since the client immediately re-polls after receiving
// any data, every small trickle turns into its own full round trip and dst's
// capacity never gets used. Pass coalesceWindow <= 0 for DefaultCoalesceWindow.
//
// Returns (0, nil) if the timeout expires with no data at all — the caller
// answers 204. Returns io.EOF if the pipe is closed and empty, which the caller
// must distinguish from that timeout: it means the session is gone, and
// answering 204 there would leave the client polling an empty session until its
// own timeout instead of reconnecting (A5). Returns (0, errPipeInterrupted)
// if interrupt was called; only the resumable transport ever does that, so
// every other caller keeps seeing exactly the three outcomes above. An
// interrupted return says nothing about the pipe's contents — data may
// well be buffered (an interrupt that lands during the coalesce window is
// reported on the *following* call, after that call's data has been
// returned) — so a caller must treat it purely as "re-check your state and
// call again", never as "the pipe is empty".
func (p *BufferedPipe) ReadAvailable(dst []byte, timeout time.Duration, coalesceWindow time.Duration) (int, error) {
	if coalesceWindow <= 0 {
		coalesceWindow = DefaultCoalesceWindow
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// A pending interrupt wins over everything, including data already
	// buffered: the interrupter wants the caller back in its own loop now
	// (to notice it has been detached, or to send an ack), and the data is
	// still here for the next call.
	if p.intr {
		p.intr = false
		return 0, errPipeInterrupted
	}

	if len(p.buf) == 0 {
		if p.closed {
			return 0, io.EOF
		}

		// Phase 1: wait for the first byte (or timeout/close/interrupt).
		timedOut := false
		timer := time.AfterFunc(timeout, func() {
			p.mu.Lock()
			timedOut = true
			p.cond.Broadcast()
			p.mu.Unlock()
		})
		for len(p.buf) == 0 && !p.closed && !timedOut && !p.intr {
			p.cond.Wait()
		}
		timer.Stop()

		if len(p.buf) == 0 {
			if p.closed {
				return 0, io.EOF
			}
			if p.intr {
				p.intr = false
				return 0, errPipeInterrupted
			}
			return 0, nil // pure timeout, no data at all
		}
	}

	// Phase 2: at least one byte is available. Give it a brief window to
	// accumulate more before flushing, unless dst is already full or the pipe
	// closed in the meantime. An interrupt cuts the window short but does not
	// consume the flag — the data goes back first, and the very next call
	// returns errPipeInterrupted, so the interrupt is still handled promptly.
	if len(p.buf) < len(dst) && !p.closed && !p.intr {
		timedOut := false
		timer := time.AfterFunc(coalesceWindow, func() {
			p.mu.Lock()
			timedOut = true
			p.cond.Broadcast()
			p.mu.Unlock()
		})
		for len(p.buf) < len(dst) && !p.closed && !timedOut && !p.intr {
			p.cond.Wait()
		}
		timer.Stop()
	}

	n := copy(dst, p.buf)
	p.buf = p.buf[n:]
	return n, nil
}

// interrupt makes the current or next ReadAvailable call return
// errPipeInterrupted instead of waiting out its timeout. It is level-
// triggered: a call that lands while no reader is parked is remembered and
// consumed by the next ReadAvailable, so a writer that was busy between two
// calls still sees it promptly. A spurious interrupt is harmless to every
// caller in this package — each one re-checks its own state and simply calls
// ReadAvailable again — which is what makes the flag safe to leave pending.
func (p *BufferedPipe) interrupt() {
	p.mu.Lock()
	p.intr = true
	p.cond.Broadcast()
	p.mu.Unlock()
}

// Close closes the pipe, unblocking any waiting readers. Idempotent.
func (p *BufferedPipe) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	p.cond.Broadcast()
	return nil
}
