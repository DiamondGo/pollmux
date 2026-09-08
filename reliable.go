package pollmux

import (
	"errors"
	"sync"
	"time"
)

// errPipeInterrupted is BufferedPipe.ReadAvailable's answer to interrupt:
// nothing was read, and the caller should re-check its own state (has it
// been detached? is an ack due?) before waiting again.
var errPipeInterrupted = errors.New("pollmux: pipe wait interrupted")

// errReplayGap is reliable.recvIn's report that the peer sent data starting
// past the receiver's contiguous offset: a hole in the byte stream. There is
// no recovering from that — handing yamux a stream with bytes missing would
// desynchronise every frame after it — so the session must be abandoned.
var errReplayGap = errors.New("pollmux: resumable stream has a gap: peer skipped bytes")

// errBadAck is reliable.ack's report that the peer acknowledged more than
// was ever sent, which no correct peer can do.
var errBadAck = errors.New("pollmux: peer acknowledged bytes that were never sent")

// reliable is the resumable transport's byte layer, shared verbatim by both
// ends: on the server it sits between a Session's pipes and the poll/
// WebSocket handlers, on the client between resumableConn's pipes and its
// legs. Each direction is a cumulative byte count, exactly as TCP does it:
//
//   - Sending: bytes taken from out get consecutive offsets and are kept in
//     replay until the peer acknowledges them. sentCursor is where the
//     currently attached transport is up to; normally it tracks sendOffset,
//     and a resume rewinds it to whatever the peer says it has, so the next
//     transport replays the gap before taking anything new from out.
//   - Receiving: recvIn writes into in only what extends recvOffset. Bytes
//     below it are a replay the peer sent because it never saw our ack —
//     dropped. Bytes above it are a hole — fatal.
//
// Nothing here ever blocks a sender on the peer: Write into out always
// returns at once, and the only thing acknowledgements control is how much
// memory replay holds. A replay buffer that outgrows maxReplay marks the
// layer broken — the session keeps working over its current transport, but
// it can no longer be resumed and stops retaining anything.
type reliable struct {
	out *BufferedPipe // bytes the local application wants delivered to the peer
	in  *BufferedPipe // bytes received from the peer, for the local application

	mu          sync.Mutex
	sendOffset  uint64 // every byte ever taken from out sits below this
	ackedOffset uint64 // the peer has confirmed everything below this
	sentCursor  uint64 // the attached transport sends from here; ackedOffset <= sentCursor <= sendOffset
	replay      []byte // bytes [ackedOffset, sendOffset)
	maxReplay   int
	broken      bool

	recvOffset uint64 // contiguous bytes written to in
	lastAck    uint64 // recvOffset as of the last ack handed out by takeAck
	ackEager   int    // interrupt out's waiter once this much is unacknowledged
}

// ackEagerBytes is how much newly received data prompts an immediate ack
// (by interrupting the sending side's wait) rather than waiting for the next
// data frame or heartbeat to carry it. Half a stream window: yamux itself
// sends a window update at that point, so this matches the reverse-traffic
// cadence a bulk transfer already has.
const ackEagerBytes = MaxStreamWindowSize / 2

func newReliable(out, in *BufferedPipe, maxReplay int) *reliable {
	if maxReplay <= 0 {
		maxReplay = DefaultMaxReplayBytes
	}
	return &reliable{out: out, in: in, maxReplay: maxReplay, ackEager: ackEagerBytes}
}

// nextOut returns the next chunk for the attached transport to send and the
// offset of its first byte. Pending replay (bytes below sendOffset the
// current transport has not sent yet) always goes first; only once that is
// exhausted does it wait on out with ReadAvailable's usual semantics, so the
// error cases are ReadAvailable's: io.EOF when out is closed for good,
// errPipeInterrupted when interrupt was called, (0, nil) on a plain timeout.
//
// Only one transport may be attached per direction at a time — the caller
// (Session's attachment tracking on the server, resumableConn's supervisor
// on the client) guarantees it. Two concurrent callers would each pull bytes
// from out and then race to number them, which would reorder the stream.
func (r *reliable) nextOut(buf []byte, timeout, coalesce time.Duration) (off uint64, n int, err error) {
	r.mu.Lock()
	if !r.broken && r.sentCursor < r.sendOffset {
		start := int(r.sentCursor - r.ackedOffset)
		n = copy(buf, r.replay[start:])
		off = r.sentCursor
		r.sentCursor += uint64(n)
		r.mu.Unlock()
		return off, n, nil
	}
	r.mu.Unlock()

	n, err = r.out.ReadAvailable(buf, timeout, coalesce)
	if n == 0 {
		return 0, 0, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	off = r.sendOffset
	r.sendOffset += uint64(n)
	r.sentCursor = r.sendOffset
	if !r.broken {
		r.replay = append(r.replay, buf[:n]...)
		if len(r.replay) > r.maxReplay {
			r.markBrokenLocked()
		}
	}
	return off, n, nil
}

// recvIn accepts a data frame from the peer: whatever part of it extends
// recvOffset is written to in, a full duplicate is dropped, a gap is fatal.
func (r *reliable) recvIn(off uint64, p []byte) error {
	r.mu.Lock()
	switch {
	case off > r.recvOffset:
		r.markBrokenLocked()
		r.mu.Unlock()
		return errReplayGap
	case off+uint64(len(p)) <= r.recvOffset:
		r.mu.Unlock()
		return nil // already have all of it
	default:
		p = p[r.recvOffset-off:]
	}
	// Written under the lock so recvOffset never runs ahead of what is
	// actually in the pipe. BufferedPipe.Write never blocks, so this holds
	// r.mu only for a copy.
	if _, err := r.in.Write(p); err != nil {
		r.mu.Unlock()
		return err
	}
	r.recvOffset += uint64(len(p))
	wake := r.recvOffset-r.lastAck >= uint64(r.ackEager)
	r.mu.Unlock()

	if wake {
		// Nudge the sending side to carry an ack now instead of at its next
		// heartbeat, so the peer's replay buffer does not sit on half a
		// window per stream for seconds at a time.
		r.out.interrupt()
	}
	return nil
}

// ack records the peer's cumulative acknowledgement and releases replay.
func (r *reliable) ack(n uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n > r.sendOffset {
		r.markBrokenLocked()
		return errBadAck
	}
	if r.broken || n <= r.ackedOffset {
		return nil
	}
	r.replay = r.replay[n-r.ackedOffset:]
	if len(r.replay) == 0 {
		r.replay = nil // let the old backing array go once it is fully acked
	}
	r.ackedOffset = n
	if r.sentCursor < n {
		r.sentCursor = n
	}
	return nil
}

// takeAck returns the offset to acknowledge if recvOffset has moved since
// the last one was handed out. The caller commits to sending it; a lost ack
// is harmless because the next one is cumulative, and every fresh
// attachment starts by sending one (see resetAck).
func (r *reliable) takeAck() (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.recvOffset == r.lastAck {
		return 0, false
	}
	r.lastAck = r.recvOffset
	return r.recvOffset, true
}

// resetAck makes the next takeAck report the current recvOffset even if it
// was already acknowledged once, so a newly attached transport always opens
// with an ack the peer may have missed while the previous transport died.
func (r *reliable) resetAck() {
	r.mu.Lock()
	r.lastAck = 0
	r.mu.Unlock()
}

// resumeOut rewinds the sending side to peerRecv, what the peer says it
// has: everything below is dropped from replay, everything from there to
// sendOffset will be replayed by the next attached transport. It refuses —
// and the caller must then abandon the session rather than guess — if the
// layer is broken or peerRecv is outside [ackedOffset, sendOffset]: below
// means the peer claims not to have bytes it already acknowledged, above
// means it claims bytes that were never sent.
func (r *reliable) resumeOut(peerRecv uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.broken || peerRecv < r.ackedOffset || peerRecv > r.sendOffset {
		return false
	}
	r.replay = r.replay[peerRecv-r.ackedOffset:]
	if len(r.replay) == 0 {
		r.replay = nil
	}
	r.ackedOffset = peerRecv
	r.sentCursor = peerRecv
	return true
}

// recvOffsetNow is what this end reports in a resume handshake.
func (r *reliable) recvOffsetNow() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.recvOffset
}

type reliableProgress struct {
	sent uint64
	recv uint64
}

// progress snapshots the cumulative byte counts in both directions. The
// supervisor uses it to distinguish a rapidly failing but productive series
// of transports from a resume loop that never moves the session at all.
func (r *reliable) progress() reliableProgress {
	r.mu.Lock()
	defer r.mu.Unlock()
	return reliableProgress{sent: r.sendOffset, recv: r.recvOffset}
}

// unacked is how many sent bytes the peer has not acknowledged — the replay
// buffer's current size.
func (r *reliable) unacked() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.replay)
}

func (r *reliable) isBroken() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.broken
}

// markBroken gives up on resumability: the session keeps running over its
// current transport, but nothing is retained for replay any more and every
// resume attempt will be refused.
func (r *reliable) markBroken() {
	r.mu.Lock()
	r.markBrokenLocked()
	r.mu.Unlock()
}

func (r *reliable) markBrokenLocked() {
	r.broken = true
	r.replay = nil
	r.ackedOffset = r.sendOffset
	r.sentCursor = r.sendOffset
}

// interruptOut pulls the attached sender out of its wait on out (see
// BufferedPipe.interrupt): used to detach a stale transport promptly and to
// stop a leg on shutdown.
func (r *reliable) interruptOut() {
	r.out.interrupt()
}
