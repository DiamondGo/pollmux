package pollmux

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"
)

// --- reliable ---------------------------------------------------------------

func newTestReliable(maxReplay int) *reliable {
	return newReliable(NewBufferedPipe(), NewBufferedPipe(), maxReplay)
}

func mustNext(t *testing.T, r *reliable, n int) (uint64, []byte) {
	t.Helper()
	buf := make([]byte, n)
	off, got, err := r.nextOut(buf, 50*time.Millisecond, time.Millisecond)
	if err != nil {
		t.Fatalf("nextOut: %v", err)
	}
	return off, buf[:got]
}

func TestReliableNumbersRetainsAndReplays(t *testing.T) {
	r := newTestReliable(1 << 20)

	r.out.Write([]byte("hello "))
	off, data := mustNext(t, r, 64)
	if off != 0 || string(data) != "hello " {
		t.Fatalf("first chunk = (%d, %q), want (0, %q)", off, data, "hello ")
	}
	r.out.Write([]byte("world"))
	off, data = mustNext(t, r, 64)
	if off != 6 || string(data) != "world" {
		t.Fatalf("second chunk = (%d, %q), want (6, %q)", off, data, "world")
	}
	if r.unacked() != 11 {
		t.Fatalf("unacked = %d, want 11 before any ack", r.unacked())
	}

	// A partial ack releases exactly that prefix.
	if err := r.ack(4); err != nil {
		t.Fatal(err)
	}
	if r.unacked() != 7 {
		t.Fatalf("unacked = %d after ack(4), want 7", r.unacked())
	}

	// A resume to an offset inside the retained range rewinds the cursor:
	// the next chunk is a replay starting exactly there, and only then does
	// fresh data follow.
	if !r.resumeOut(8) {
		t.Fatal("resumeOut(8) refused an offset inside the retained range")
	}
	r.out.Write([]byte("!"))
	off, data = mustNext(t, r, 64)
	if off != 8 || string(data) != "rld" {
		t.Fatalf("replay chunk = (%d, %q), want (8, %q)", off, data, "rld")
	}
	off, data = mustNext(t, r, 64)
	if off != 11 || string(data) != "!" {
		t.Fatalf("post-replay chunk = (%d, %q), want (11, %q)", off, data, "!")
	}

	// Replay respects the caller's buffer size and picks up where it left.
	if !r.resumeOut(8) {
		t.Fatal("second resumeOut(8) refused")
	}
	off, data = mustNext(t, r, 2)
	if off != 8 || string(data) != "rl" {
		t.Fatalf("small replay chunk = (%d, %q), want (8, %q)", off, data, "rl")
	}
	off, data = mustNext(t, r, 2)
	if off != 10 || string(data) != "d!" {
		t.Fatalf("next small replay chunk = (%d, %q), want (10, %q)", off, data, "d!")
	}

	// Out of range in either direction is refused, never guessed.
	if r.resumeOut(7) {
		t.Fatal("resumeOut below ackedOffset must be refused")
	}
	if r.resumeOut(13) {
		t.Fatal("resumeOut above sendOffset must be refused")
	}
	if err := r.ack(99); !errors.Is(err, errBadAck) {
		t.Fatalf("ack beyond sendOffset = %v, want errBadAck", err)
	}
}

func TestReliableProgressOnlyCountsPeerVisibleBytes(t *testing.T) {
	r := newTestReliable(1 << 20)
	initial := r.progress()

	// Pulling local input assigns offsets before the caller attempts its
	// transport write. It must not count as progress: that write may fail
	// without a single byte reaching the peer.
	r.out.Write([]byte("abc"))
	mustNext(t, r, 64)
	if got := r.progress(); got != initial {
		t.Fatalf("progress changed after nextOut only: got %+v, want %+v", got, initial)
	}

	if err := r.ack(3); err != nil {
		t.Fatal(err)
	}
	afterAck := r.progress()
	if afterAck == initial || afterAck.acked != 3 {
		t.Fatalf("progress after peer ack = %+v, want acked=3", afterAck)
	}

	// A resume handshake's peer recv offset is equally authoritative even
	// when the in-band ack was lost with the old transport.
	r.out.Write([]byte("def"))
	mustNext(t, r, 64)
	beforeResume := r.progress()
	if !r.resumeOut(6) {
		t.Fatal("resumeOut refused the peer's received offset")
	}
	afterResume := r.progress()
	if afterResume == beforeResume || afterResume.acked != 6 {
		t.Fatalf("progress after resume offset = %+v, want acked=6", afterResume)
	}

	beforeRecv := r.progress()
	if err := r.recvIn(0, []byte("down")); err != nil {
		t.Fatal(err)
	}
	afterRecv := r.progress()
	if afterRecv == beforeRecv || afterRecv.recv != 4 {
		t.Fatalf("progress after received bytes = %+v, want recv=4", afterRecv)
	}
}

func TestReliableRecvDedupsReplayAndRefusesGaps(t *testing.T) {
	r := newTestReliable(1 << 20)

	if err := r.recvIn(0, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	// A replay overlapping what we have contributes only its tail.
	if err := r.recvIn(1, []byte("bcdef")); err != nil {
		t.Fatal(err)
	}
	// A replay entirely below recvOffset contributes nothing.
	if err := r.recvIn(0, []byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 16)
	n, _ := r.in.Read(got)
	if string(got[:n]) != "abcdef" {
		t.Fatalf("in pipe holds %q, want %q", got[:n], "abcdef")
	}
	if r.recvOffsetNow() != 6 {
		t.Fatalf("recvOffset = %d, want 6", r.recvOffsetNow())
	}

	// takeAck reports movement once, then stays quiet until more arrives or
	// resetAck forces a repeat.
	if ack, due := r.takeAck(); !due || ack != 6 {
		t.Fatalf("takeAck = (%d, %v), want (6, true)", ack, due)
	}
	if _, due := r.takeAck(); due {
		t.Fatal("takeAck reported an ack due with nothing new received")
	}
	r.resetAck()
	if ack, due := r.takeAck(); !due || ack != 6 {
		t.Fatalf("takeAck after resetAck = (%d, %v), want (6, true)", ack, due)
	}

	if err := r.recvIn(10, []byte("x")); !errors.Is(err, errReplayGap) {
		t.Fatalf("gap = %v, want errReplayGap", err)
	}
	if !r.isBroken() {
		t.Fatal("a gap must mark the layer broken")
	}
}

func TestReliableOverflowBreaksAndStopsRetaining(t *testing.T) {
	r := newTestReliable(8)
	r.out.Write(bytes.Repeat([]byte("a"), 6))
	mustNext(t, r, 64)
	if r.isBroken() {
		t.Fatal("broken before exceeding maxReplay")
	}
	r.out.Write(bytes.Repeat([]byte("b"), 6))
	off, data := mustNext(t, r, 64)
	if off != 6 || len(data) != 6 {
		t.Fatalf("chunk after overflow = (%d, %d bytes), want (6, 6): data must still flow", off, len(data))
	}
	if !r.isBroken() {
		t.Fatal("exceeding maxReplay must mark the layer broken")
	}
	if r.unacked() != 0 {
		t.Fatalf("unacked = %d after breaking, want 0: nothing should be retained any more", r.unacked())
	}
	if r.resumeOut(6) {
		t.Fatal("a broken layer must refuse to resume")
	}
	// Data keeps flowing over the current transport regardless.
	r.out.Write([]byte("c"))
	off, data = mustNext(t, r, 64)
	if off != 12 || string(data) != "c" {
		t.Fatalf("post-break chunk = (%d, %q), want (12, %q)", off, data, "c")
	}
}

func TestReliableEagerAckInterruptsSender(t *testing.T) {
	r := newTestReliable(1 << 20)
	r.ackEager = 4

	done := make(chan error, 1)
	go func() {
		_, _, err := r.nextOut(make([]byte, 64), 5*time.Second, time.Millisecond)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond) // let the sender park
	if err := r.recvIn(0, []byte("abcd")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, errPipeInterrupted) {
			t.Fatalf("sender woke with %v, want errPipeInterrupted", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("receiving ackEager bytes did not wake the parked sender")
	}
	if ack, due := r.takeAck(); !due || ack != 4 {
		t.Fatalf("takeAck after eager wake = (%d, %v), want (4, true)", ack, due)
	}
}

// --- BufferedPipe.interrupt ----------------------------------------------------

func TestBufferedPipeInterruptCutsWaitShort(t *testing.T) {
	p := NewBufferedPipe()
	done := make(chan error, 1)
	go func() {
		_, err := p.ReadAvailable(make([]byte, 8), 5*time.Second, time.Millisecond)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	p.interrupt()
	select {
	case err := <-done:
		if !errors.Is(err, errPipeInterrupted) {
			t.Fatalf("interrupted wait returned %v, want errPipeInterrupted", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("interrupt did not wake the parked reader")
	}

	// The flag was consumed: the next wait is a normal one.
	start := time.Now()
	if n, err := p.ReadAvailable(make([]byte, 8), 30*time.Millisecond, time.Millisecond); n != 0 || err != nil {
		t.Fatalf("plain wait = (%d, %v), want (0, nil) timeout", n, err)
	}
	if time.Since(start) < 25*time.Millisecond {
		t.Fatal("a consumed interrupt must not shorten the following wait")
	}
}

func TestBufferedPipeInterruptIsLevelTriggered(t *testing.T) {
	p := NewBufferedPipe()
	p.interrupt() // nobody waiting
	p.Write([]byte("data"))

	// A pending interrupt wins over buffered data, once.
	if _, err := p.ReadAvailable(make([]byte, 8), time.Second, time.Millisecond); !errors.Is(err, errPipeInterrupted) {
		t.Fatalf("pending interrupt not honoured: %v", err)
	}
	n, err := p.ReadAvailable(make([]byte, 8), time.Second, time.Millisecond)
	if err != nil || n != 4 {
		t.Fatalf("data after interrupt = (%d, %v), want (4, nil)", n, err)
	}

	// Close still wins once the interrupt has been consumed.
	p.Close()
	if _, err := p.ReadAvailable(make([]byte, 8), time.Second, time.Millisecond); !errors.Is(err, io.EOF) {
		t.Fatalf("closed pipe = %v, want io.EOF", err)
	}
}

// --- frame codec --------------------------------------------------------------

func TestSeqAndAckFramesRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := writeSeqFrame(&buf, 1<<40, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := writeFrame(&buf, frameAck, encodeOffset(12345)); err != nil {
		t.Fatal(err)
	}
	writeFrame(&buf, frameHeartbeat, nil)

	fr := newFrameReader(&buf, 7)
	typ, payload, err := fr.next()
	if err != nil || typ != frameSeqData {
		t.Fatalf("first frame = (%v, %v), want frameSeqData", typ, err)
	}
	off, data, err := splitSeq(payload)
	if err != nil || off != 1<<40 || string(data) != "payload" {
		t.Fatalf("splitSeq = (%d, %q, %v)", off, data, err)
	}

	typ, payload, err = fr.next()
	if err != nil || typ != frameAck {
		t.Fatalf("second frame = (%v, %v), want frameAck", typ, err)
	}
	if n, err := decodeAck(payload); err != nil || n != 12345 {
		t.Fatalf("decodeAck = (%d, %v), want 12345", n, err)
	}

	typ, _, err = fr.next()
	if err != nil || typ != frameHeartbeat {
		t.Fatalf("third frame = (%v, %v), want frameHeartbeat", typ, err)
	}
	if _, _, err := fr.next(); !errors.Is(err, io.EOF) {
		t.Fatalf("end = %v, want io.EOF", err)
	}
}

func TestFrameReaderAllowsSeqOffsetOverDataBound(t *testing.T) {
	// A seq-data frame carrying exactly maxPayload bytes of data is legal
	// even though its payload is 8 bytes longer than a plain data frame
	// could be; one byte more is not.
	var ok bytes.Buffer
	writeSeqFrame(&ok, 0, make([]byte, 16))
	if _, _, err := newFrameReader(&ok, 16).next(); err != nil {
		t.Fatalf("seq frame at the bound rejected: %v", err)
	}
	var tooBig bytes.Buffer
	writeSeqFrame(&tooBig, 0, make([]byte, 17))
	if _, _, err := newFrameReader(&tooBig, 16).next(); err == nil {
		t.Fatal("seq frame over the bound accepted")
	}
	var plainTooBig bytes.Buffer
	writeFrame(&plainTooBig, frameData, make([]byte, 17))
	if _, _, err := newFrameReader(&plainTooBig, 16).next(); err == nil {
		t.Fatal("plain data frame over the bound accepted")
	}
	if _, err := decodeAck([]byte{1, 2, 3}); err == nil {
		t.Fatal("short ack payload accepted")
	}
	if _, _, err := splitSeq([]byte{1, 2, 3}); err == nil {
		t.Fatal("short seq payload accepted")
	}
}

// --- attachments --------------------------------------------------------------

func TestAttachmentsReplaceStaleOwnerAndTrackCurrent(t *testing.T) {
	a := newAttachments()

	// The first owner's kick releases it, the way a real handler's deferred
	// detach does once its blocked call fails.
	var first *attachment
	var err error
	first, err = a.attach(attachDown, func() { a.detach(first) })
	if err != nil {
		t.Fatal(err)
	}
	if !a.isCurrent(first) {
		t.Fatal("fresh attachment is not current")
	}

	second, err := a.attach(attachDown, func() {})
	if err != nil {
		t.Fatalf("second attach: %v", err)
	}
	if a.isCurrent(first) || !a.isCurrent(second) {
		t.Fatal("second attach did not displace the first")
	}
	select {
	case <-first.done:
	default:
		t.Fatal("displaced attachment's done channel is still open")
	}

	// An owner whose kick achieves nothing is waited out and reported.
	start := time.Now()
	if _, err := a.attach(attachDown, func() {}); !errors.Is(err, errStillAttached) {
		t.Fatalf("attach over a stuck owner = %v, want errStillAttached", err)
	}
	if time.Since(start) < detachWait {
		t.Fatal("attach gave up before detachWait")
	}
	a.detach(second)
	if a.isCurrent(second) {
		t.Fatal("detached attachment still current")
	}

	// attachBoth claims both directions; kickAll clears them.
	var both *attachment
	both, err = a.attach(attachBoth, func() { a.detach(both) })
	if err != nil {
		t.Fatal(err)
	}
	if a.down != both || a.up != both {
		t.Fatal("attachBoth did not claim both directions")
	}
	if err := a.kickAll(); err != nil {
		t.Fatal(err)
	}
	if a.down != nil || a.up != nil {
		t.Fatal("kickAll left an owner in place")
	}

	if !a.beginResume() || a.beginResume() {
		t.Fatal("beginResume must be exclusive")
	}
	a.endResume()
	if !a.beginResume() {
		t.Fatal("beginResume after endResume must succeed")
	}
}
