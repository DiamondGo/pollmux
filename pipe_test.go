package pollmux

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBufferedPipeWriteThenRead(t *testing.T) {
	p := NewBufferedPipe()

	if n, err := p.Write([]byte("hello")); err != nil || n != 5 {
		t.Fatalf("Write = (%d, %v), want (5, nil)", n, err)
	}

	dst := make([]byte, 16)
	n, err := p.Read(dst)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got := string(dst[:n]); got != "hello" {
		t.Fatalf("Read = %q, want %q", got, "hello")
	}
}

func TestBufferedPipeReadBlocksUntilWrite(t *testing.T) {
	p := NewBufferedPipe()

	go func() {
		time.Sleep(20 * time.Millisecond)
		p.Write([]byte("late"))
	}()

	dst := make([]byte, 16)
	start := time.Now()
	n, err := p.Read(dst)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 15*time.Millisecond {
		t.Fatalf("Read returned after %v, expected it to block for the writer", elapsed)
	}
	if got := string(dst[:n]); got != "late" {
		t.Fatalf("Read = %q, want %q", got, "late")
	}
}

func TestBufferedPipeWriteAfterCloseFails(t *testing.T) {
	p := NewBufferedPipe()
	p.Close()

	if _, err := p.Write([]byte("x")); err != io.ErrClosedPipe {
		t.Fatalf("Write after Close = %v, want io.ErrClosedPipe", err)
	}
}

func TestBufferedPipeReadEOFAfterClose(t *testing.T) {
	p := NewBufferedPipe()
	p.Close()

	if _, err := p.Read(make([]byte, 4)); err != io.EOF {
		t.Fatalf("Read after Close = %v, want io.EOF", err)
	}
}

func TestBufferedPipeCloseUnblocksReader(t *testing.T) {
	p := NewBufferedPipe()

	errCh := make(chan error, 1)
	go func() {
		_, err := p.Read(make([]byte, 4))
		errCh <- err
	}()

	time.Sleep(10 * time.Millisecond)
	p.Close()

	select {
	case err := <-errCh:
		if err != io.EOF {
			t.Fatalf("blocked Read after Close = %v, want io.EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock the reader")
	}
}

func TestBufferedPipeCloseIsIdempotent(t *testing.T) {
	p := NewBufferedPipe()
	if err := p.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// ReadAvailable phase 1: an empty pipe waits for the first byte, and a timeout
// with nothing at all is reported as (0, nil) — the caller answers 204.
func TestReadAvailableTimesOutEmpty(t *testing.T) {
	p := NewBufferedPipe()

	start := time.Now()
	n, err := p.ReadAvailable(make([]byte, 128), 50*time.Millisecond, 2*time.Millisecond)
	elapsed := time.Since(start)

	if n != 0 || err != nil {
		t.Fatalf("ReadAvailable = (%d, %v), want (0, nil)", n, err)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("returned after %v, expected it to wait out the ~50ms timeout", elapsed)
	}
}

func TestReadAvailableWakesOnFirstByte(t *testing.T) {
	p := NewBufferedPipe()

	go func() {
		time.Sleep(20 * time.Millisecond)
		p.Write([]byte("data"))
	}()

	start := time.Now()
	n, err := p.ReadAvailable(make([]byte, 128), 5*time.Second, 2*time.Millisecond)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("ReadAvailable: %v", err)
	}
	if n != 4 {
		t.Fatalf("ReadAvailable = %d bytes, want 4", n)
	}
	if elapsed > time.Second {
		t.Fatalf("waited %v after data arrived, expected a prompt return", elapsed)
	}
}

// ReadAvailable phase 2: once one byte is available it waits a short window for
// more, so a trickle does not become one round trip per byte.
func TestReadAvailableCoalescesTrickle(t *testing.T) {
	p := NewBufferedPipe()

	go func() {
		p.Write([]byte("a"))
		time.Sleep(10 * time.Millisecond)
		p.Write([]byte("b"))
		time.Sleep(10 * time.Millisecond)
		p.Write([]byte("c"))
	}()

	// A 100ms coalesce window comfortably covers all three writes.
	n, err := p.ReadAvailable(make([]byte, 128), time.Second, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("ReadAvailable: %v", err)
	}
	if n != 3 {
		t.Fatalf("ReadAvailable = %d bytes, want all 3 coalesced into one response", n)
	}
}

// Phase 2 must not wait once dst is full — that would add latency for nothing.
func TestReadAvailableReturnsImmediatelyWhenDstFull(t *testing.T) {
	p := NewBufferedPipe()
	p.Write([]byte("abcd"))

	start := time.Now()
	n, err := p.ReadAvailable(make([]byte, 4), time.Second, 500*time.Millisecond)
	elapsed := time.Since(start)

	if err != nil || n != 4 {
		t.Fatalf("ReadAvailable = (%d, %v), want (4, nil)", n, err)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("waited %v with a full dst, expected an immediate return", elapsed)
	}
}

// A5 depends on this distinction: EOF (session gone) must be reported
// separately from a plain timeout, because they map to 410 and 204.
func TestReadAvailableEOFIsDistinctFromTimeout(t *testing.T) {
	p := NewBufferedPipe()
	p.Close()

	n, err := p.ReadAvailable(make([]byte, 128), time.Second, 2*time.Millisecond)
	if n != 0 || err != io.EOF {
		t.Fatalf("ReadAvailable on closed empty pipe = (%d, %v), want (0, io.EOF)", n, err)
	}
}

// Data buffered before Close must still come out; EOF only once it is drained.
func TestReadAvailableDrainsBeforeEOF(t *testing.T) {
	p := NewBufferedPipe()
	p.Write([]byte("tail"))
	p.Close()

	n, err := p.ReadAvailable(make([]byte, 128), time.Second, 2*time.Millisecond)
	if err != nil || n != 4 {
		t.Fatalf("first ReadAvailable = (%d, %v), want (4, nil)", n, err)
	}

	n, err = p.ReadAvailable(make([]byte, 128), time.Second, 2*time.Millisecond)
	if n != 0 || err != io.EOF {
		t.Fatalf("second ReadAvailable = (%d, %v), want (0, io.EOF)", n, err)
	}
}

func TestReadAvailableCloseDuringPhase1ReturnsEOF(t *testing.T) {
	p := NewBufferedPipe()

	go func() {
		time.Sleep(20 * time.Millisecond)
		p.Close()
	}()

	n, err := p.ReadAvailable(make([]byte, 128), 5*time.Second, 2*time.Millisecond)
	if n != 0 || err != io.EOF {
		t.Fatalf("ReadAvailable = (%d, %v), want (0, io.EOF) once the pipe closed", n, err)
	}
}

func TestHiWaterTracksPeak(t *testing.T) {
	p := NewBufferedPipe()

	p.Write(make([]byte, 100))
	p.Write(make([]byte, 50)) // peak is now 150
	if got := p.HiWater(); got != 150 {
		t.Fatalf("HiWater = %d, want 150", got)
	}

	// Draining lowers Buffered but must not lower the recorded peak.
	if _, err := p.Read(make([]byte, 150)); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got := p.Buffered(); got != 0 {
		t.Fatalf("Buffered = %d, want 0", got)
	}
	if got := p.HiWater(); got != 150 {
		t.Fatalf("HiWater after drain = %d, want it to stay at the peak 150", got)
	}
}

func TestHighWaterWarningFiresOnceAtThreshold(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	p := NewBufferedPipe()
	p.WatchHighWater(100, logger)

	p.Write(make([]byte, 99))
	if buf.Len() != 0 {
		t.Fatalf("warned below the threshold: %s", buf.String())
	}

	p.Write(make([]byte, 1)) // now at exactly 100
	first := buf.String()
	if !strings.Contains(first, "high-water") {
		t.Fatalf("no warning at the threshold, got: %q", first)
	}

	// One-shot: growing further must not produce a second warning.
	p.Write(make([]byte, 1000))
	if buf.String() != first {
		t.Fatalf("warned more than once:\n%s", buf.String())
	}
}

func TestHighWaterWarningDisabledByDefault(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	p := NewBufferedPipe()
	p.WatchHighWater(0, logger) // non-positive threshold disables it

	p.Write(make([]byte, 1<<20))
	if buf.Len() != 0 {
		t.Fatalf("warned with the threshold disabled: %s", buf.String())
	}
	if got := p.HiWater(); got != 1<<20 {
		t.Fatalf("HiWater = %d, want tracking to keep working, 1048576", got)
	}
}

// Run with -race: the pipe sits between an HTTP handler goroutine and a yamux
// goroutine, so concurrent access is its normal operating condition.
func TestBufferedPipeConcurrentWriteRead(t *testing.T) {
	p := NewBufferedPipe()

	const writers = 8
	const perWriter = 200
	const payload = 16

	var wg sync.WaitGroup
	for i := range writers {
		wg.Go(func() {
			chunk := bytes.Repeat([]byte{byte(i)}, payload)
			for range perWriter {
				if _, err := p.Write(chunk); err != nil {
					return
				}
			}
		})
	}

	readDone := make(chan int, 1)
	go func() {
		total := 0
		dst := make([]byte, 64)
		for {
			n, err := p.Read(dst)
			total += n
			if err != nil {
				readDone <- total
				return
			}
		}
	}()

	wg.Wait()
	p.Close()

	want := writers * perWriter * payload
	select {
	case got := <-readDone:
		if got != want {
			t.Fatalf("read %d bytes, want %d", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not finish")
	}
}
