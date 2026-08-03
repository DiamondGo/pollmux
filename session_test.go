package pollmux

import (
	"io"
	"testing"
	"time"
)

func TestSessionDirectionsAreSeparate(t *testing.T) {
	s := newSession("abc", nil)

	// The client's upload lands where the application reads.
	if _, err := s.toServer.Write([]byte("from client")); err != nil {
		t.Fatalf("toServer.Write: %v", err)
	}
	dst := make([]byte, 32)
	n, err := s.Read(dst)
	if err != nil {
		t.Fatalf("Session.Read: %v", err)
	}
	if got := string(dst[:n]); got != "from client" {
		t.Fatalf("Session.Read = %q, want %q", got, "from client")
	}

	// The application's write lands where the poll response drains.
	if _, err := s.Write([]byte("to client")); err != nil {
		t.Fatalf("Session.Write: %v", err)
	}
	n, err = s.toClient.Read(dst)
	if err != nil {
		t.Fatalf("toClient.Read: %v", err)
	}
	if got := string(dst[:n]); got != "to client" {
		t.Fatalf("toClient.Read = %q, want %q", got, "to client")
	}

	// Nothing from one direction may leak into the other.
	if got := s.toServer.Buffered(); got != 0 {
		t.Fatalf("toServer has %d leftover bytes, want 0", got)
	}
}

func TestSessionCloseIsIdempotentAndClosesBothPipes(t *testing.T) {
	s := newSession("abc", nil)

	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if !s.IsClosed() {
		t.Fatal("IsClosed = false after Close")
	}

	if _, err := s.Read(make([]byte, 4)); err != io.EOF {
		t.Fatalf("Read after Close = %v, want io.EOF", err)
	}
	if _, err := s.Write([]byte("x")); err != io.ErrClosedPipe {
		t.Fatalf("Write after Close = %v, want io.ErrClosedPipe", err)
	}
}

// Meta must be a snapshot in both directions: the caller cannot mutate the
// session through the map it passed in, nor through the map it gets back.
func TestSessionMetaIsSnapshot(t *testing.T) {
	in := map[string]string{"role": "provider", "endpoint": "home"}
	s := newSession("abc", in)

	in["role"] = "tampered"
	if got := s.Meta()["role"]; got != "provider" {
		t.Fatalf("mutating the caller's map changed the session: role = %q", got)
	}

	out := s.Meta()
	out["endpoint"] = "tampered"
	if got := s.Meta()["endpoint"]; got != "home" {
		t.Fatalf("mutating the returned map changed the session: endpoint = %q", got)
	}
}

func TestSessionMetaNilIsUsable(t *testing.T) {
	s := newSession("abc", nil)
	if got := s.Meta(); len(got) != 0 {
		t.Fatalf("Meta = %v, want empty", got)
	}
}

func TestSessionPollInFlightCounts(t *testing.T) {
	s := newSession("abc", nil)

	if got := s.PollInFlight(); got != 0 {
		t.Fatalf("PollInFlight = %d on a fresh session, want 0", got)
	}

	s.pollInFlight.Add(1)
	s.pollInFlight.Add(1)
	if got := s.PollInFlight(); got != 2 {
		t.Fatalf("PollInFlight = %d, want 2", got)
	}

	s.pollInFlight.Add(-1)
	s.pollInFlight.Add(-1)
	if got := s.PollInFlight(); got != 0 {
		t.Fatalf("PollInFlight = %d after both polls returned, want 0", got)
	}
}

func TestSessionTouchAdvancesLastActive(t *testing.T) {
	s := newSession("abc", nil)
	before := s.LastActive()

	time.Sleep(10 * time.Millisecond)
	s.touch()

	if !s.LastActive().After(before) {
		t.Fatalf("LastActive did not advance: %v then %v", before, s.LastActive())
	}
}

func TestSessionHiWaterReportsBothDirections(t *testing.T) {
	s := newSession("abc", nil)

	s.toServer.Write(make([]byte, 300))
	s.Write(make([]byte, 700))

	toServer, toClient := s.HiWater()
	if toServer != 300 {
		t.Fatalf("toServer high-water = %d, want 300", toServer)
	}
	if toClient != 700 {
		t.Fatalf("toClient high-water = %d, want 700", toClient)
	}
}
