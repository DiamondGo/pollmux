package pollmux

import (
	"encoding/binary"
	"fmt"
	"io"
)

// frameType tags each frame inside a stream-mode poll response body. Batch
// mode needs none of this — one HTTP response body is one message there, and
// the response ending is the only boundary that exists.
type frameType byte

const (
	frameData      frameType = 0x01
	frameHeartbeat frameType = 0x02
	// frameEnd is a benign end: StreamMaxDuration rolled over, or the client
	// itself is going away. The session is still alive — the client should
	// simply reopen a new stream poll.
	frameEnd frameType = 0x03
	// frameGone is a fatal end: the session was closed server-side (evicted,
	// deleted, or superseded) while this poll was parked. Reopening a poll
	// against the same session id would just get frameGone again forever, so
	// the client must treat this the same as batch mode's 410 — surface
	// TransportFailed and let the caller reconnect with a fresh session.
	frameGone frameType = 0x04
	// frameAck and frameSeqData only ever appear on a session that
	// negotiated ConnectResponse.Resumable (see reliable.go). frameAck's
	// payload is one 8-byte big-endian cumulative offset: "I have every
	// byte below this". frameSeqData replaces frameData on such a session:
	// its payload is an 8-byte big-endian offset of the first data byte,
	// followed by the data. Carrying the absolute offset on every data frame
	// is what lets a receiver drop a replayed byte it already has and refuse
	// a stream with a hole in it, instead of handing yamux a corrupted byte
	// stream — see reliable.recvIn.
	frameAck     frameType = 0x05
	frameSeqData frameType = 0x06
)

// seqHeaderLen is the size of the offset that prefixes a frameSeqData payload
// and that forms the whole of a frameAck payload.
const seqHeaderLen = 8

// frameHeaderLen is 1 byte type + 4 byte big-endian length. Only frameData
// gives the length field meaning; frameHeartbeat, frameEnd, and frameGone
// always encode zero length and carry no payload.
const frameHeaderLen = 5

// writeFrame writes one frame to w. It does not flush — the caller (a
// http.Flusher) decides when a frame is worth a round trip to the network.
func writeFrame(w io.Writer, typ frameType, payload []byte) error {
	var hdr [frameHeaderLen]byte
	hdr[0] = byte(typ)
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.Write(payload)
	return err
}

// writeSeqFrame writes one frameSeqData frame carrying payload at offset off,
// as three writes with no intermediate copy — a resumable session's data
// frames are the hot path, and payload can be a full PollBufferBytes.
func writeSeqFrame(w io.Writer, off uint64, payload []byte) error {
	var hdr [frameHeaderLen + seqHeaderLen]byte
	hdr[0] = byte(frameSeqData)
	binary.BigEndian.PutUint32(hdr[1:], uint32(seqHeaderLen+len(payload)))
	binary.BigEndian.PutUint64(hdr[frameHeaderLen:], off)
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.Write(payload)
	return err
}

// encodeOffset renders a frameAck payload (or a frameSeqData prefix).
func encodeOffset(n uint64) []byte {
	var b [seqHeaderLen]byte
	binary.BigEndian.PutUint64(b[:], n)
	return b[:]
}

// decodeAck parses a frameAck payload.
func decodeAck(payload []byte) (uint64, error) {
	if len(payload) != seqHeaderLen {
		return 0, fmt.Errorf("pollmux: ack frame payload is %d bytes, want %d", len(payload), seqHeaderLen)
	}
	return binary.BigEndian.Uint64(payload), nil
}

// splitSeq separates a frameSeqData payload into its offset and data.
func splitSeq(payload []byte) (uint64, []byte, error) {
	if len(payload) < seqHeaderLen {
		return 0, nil, fmt.Errorf("pollmux: seq-data frame payload is %d bytes, shorter than its %d-byte offset", len(payload), seqHeaderLen)
	}
	return binary.BigEndian.Uint64(payload), payload[seqHeaderLen:], nil
}

// frameReader decodes stream frames, returning exactly one frame per call to
// next and buffering nothing across the underlying io.Reader beyond what
// io.ReadFull needs — HTTP chunked transfer gives no guarantee that a
// frame's header and payload arrive in the same Read, so this must not
// assume that.
type frameReader struct {
	r          io.Reader
	maxPayload int
	hdr        [frameHeaderLen]byte
}

// newFrameReader wraps r. maxPayload bounds a single data frame's payload —
// pass the negotiated PollBufferBytes, the same number that already caps one
// batch-mode response, so a misbehaving or compromised peer can't force an
// unbounded allocation.
func newFrameReader(r io.Reader, maxPayload int) *frameReader {
	return &frameReader{r: r, maxPayload: maxPayload}
}

// next reads one full frame. It returns io.EOF exactly when the underlying
// reader ends cleanly at a frame boundary (io.ReadFull's own guarantee: zero
// bytes read before EOF) — that is a deliberate, clean end of the response
// body and callers should treat it the same as an explicit frameEnd frame.
// Anything that ends mid-frame comes back as io.ErrUnexpectedEOF (also
// io.ReadFull's doing) or a wrapped network error, and callers must treat
// that as a real transport failure, not a clean end.
func (f *frameReader) next() (frameType, []byte, error) {
	if _, err := io.ReadFull(f.r, f.hdr[:]); err != nil {
		return 0, nil, err // io.EOF (clean) or io.ErrUnexpectedEOF (mid-header)
	}
	typ := frameType(f.hdr[0])
	length := binary.BigEndian.Uint32(f.hdr[1:])
	if length == 0 {
		return typ, nil, nil
	}
	// A frameSeqData payload is the data plus its offset prefix, so it may
	// legitimately run seqHeaderLen past the negotiated data bound; a
	// frameAck payload is exactly one offset, whatever the data bound is.
	maxPayload := f.maxPayload
	switch typ {
	case frameSeqData:
		maxPayload += seqHeaderLen
	case frameAck:
		maxPayload = seqHeaderLen
	}
	if int64(length) > int64(maxPayload) {
		return 0, nil, fmt.Errorf("pollmux: stream frame length %d exceeds max %d", length, maxPayload)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(f.r, payload); err != nil {
		return 0, nil, err // io.ErrUnexpectedEOF (mid-payload) or a network error
	}
	return typ, payload, nil
}
