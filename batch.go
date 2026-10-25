package pollmux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// Resumable batches carry raw, bounded bodies, just like legacy batches. The
// offsets live in headers rather than frames, so MaxSendBytes/PollBufferSize
// still bound payload bytes and no streaming support is required of a proxy.
// A successful write to HTTP is not an acknowledgement: reliable retains the
// bytes until the peer reports receiving them in a later request/response.
func batchOffset(h http.Header, name string) (uint64, error) {
	values := h.Values(name)
	if len(values) != 1 {
		return 0, fmt.Errorf("pollmux: expected exactly one %s header", name)
	}
	n, err := strconv.ParseUint(values[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("pollmux: invalid %s: %w", name, err)
	}
	return n, nil
}

func setBatchOffset(h http.Header, name string, off uint64) {
	h.Set(name, strconv.FormatUint(off, 10))
}

// pollBatchResumable owns exactly one direction until the request finishes.
// ResumeHandler can cancel even a parked poll or a half-read upload before
// examining offsets. Incomplete uploads never enter the reliable byte stream.
func pollBatchResumable(w http.ResponseWriter, r *http.Request, s *Session, st *SessionStore, cfg ServerConfig, h Hooks, send bool) {
	ack, err := batchOffset(r.Header, HeaderRecvOffset)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var off uint64
	if send {
		off, err = batchOffset(r.Header, HeaderSendOffset)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	} else if r.ContentLength != 0 {
		writeError(w, http.StatusBadRequest, "receive-only batch must have an empty body")
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	rc := http.NewResponseController(w)
	defer rc.SetReadDeadline(time.Time{})
	defer rc.SetWriteDeadline(time.Time{})
	kind := attachDown
	if send {
		kind = attachUp
		// Set this before publishing the attachment: a concurrent resume must
		// never have its cancellation deadline overwritten by a later reset.
		rc.SetReadDeadline(time.Now().Add(cfg.PollTimeout + defaultStreamReadGrace))
	}
	att, err := s.att.attach(kind, func() {
		cancel()
		rc.SetReadDeadline(time.Now())
		rc.SetWriteDeadline(time.Now())
		s.rs.interruptOut()
	})
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "previous transport is still detaching, retry")
		return
	}
	defer s.att.detach(att)
	// A disconnected caller must not keep a poll parked for PollTimeout.
	stop := context.AfterFunc(ctx, s.rs.interruptOut)
	defer stop()

	var data []byte
	if send {
		data, err = io.ReadAll(http.MaxBytesReader(w, r.Body, int64(cfg.MaxSendBytes)))
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				closeSession(st, h, s, ReasonProtocolViolation)
				writeError(w, http.StatusRequestEntityTooLarge, "batch body exceeds max_send_bytes")
			} else {
				writeError(w, http.StatusBadRequest, "failed to read batch body")
			}
			return
		}
	}
	if ctx.Err() != nil {
		return
	}
	if err := s.rs.ack(ack); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if send {
		if err := s.rs.recvIn(off, data); err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, io.ErrClosedPipe) {
				status = http.StatusGone
			}
			writeError(w, status, err.Error())
			return
		}
		setBatchOffset(w.Header(), HeaderRecvOffset, s.rs.recvOffsetNow())
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
		return
	}

	buf := make([]byte, cfg.PollBufferSize)
	off, n, err := s.rs.nextOut(buf, cfg.PollTimeout, cfg.CoalesceWindow)
	if ctx.Err() != nil {
		return
	}
	if errors.Is(err, io.EOF) {
		writeError(w, http.StatusGone, "session closed")
		return
	}
	if err != nil && !errors.Is(err, errPipeInterrupted) {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	setBatchOffset(w.Header(), HeaderRecvOffset, s.rs.recvOffsetNow())
	if n == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	setBatchOffset(w.Header(), HeaderSendOffset, off)
	w.Header().Set("Content-Type", "application/octet-stream")
	// Explicit length lets a client detect a response truncated mid-batch.
	w.Header().Set("Content-Length", strconv.Itoa(n))
	w.WriteHeader(http.StatusOK)
	w.Write(buf[:n])
}

// batchDownLoop always has a receive-only request in flight, including while
// idle. It acknowledges downstream bytes on the next poll, not merely when a
// large transfer crosses reliable's eager-ack threshold.
func (c *resumableConn) batchDownLoop(ctx context.Context) error {
	for {
		got, err := c.doBatchPoll(ctx)
		if err != nil {
			return err
		}
		if !got && c.pollInterval > 0 && !sleepOrDone(ctx, c.pollInterval) {
			return ctx.Err()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (c *resumableConn) doBatchPoll(ctx context.Context) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.pollURL, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set(HeaderReceiveOnly, "true")
	setBatchOffset(req.Header, HeaderRecvOffset, c.rl.recvOffsetNow())
	c.setAuth(req)
	if c.localHealth != nil {
		if c.localHealth() {
			req.Header.Set(HeaderLocalHealth, "ok")
		} else {
			req.Header.Set(HeaderLocalHealth, "down")
		}
	}
	resp, err := c.pollClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return false, &fatalPollError{status: resp.StatusCode}
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, int64(c.maxFrame)+1))
	if err != nil {
		return false, fmt.Errorf("pollmux: reading batch response: %w", err)
	}
	if len(data) > c.maxFrame {
		return false, errors.New("pollmux: batch response exceeds poll_buffer_bytes")
	}
	ack, err := batchOffset(resp.Header, HeaderRecvOffset)
	if err != nil {
		return false, err
	}
	if resp.StatusCode == http.StatusOK {
		off, err := batchOffset(resp.Header, HeaderSendOffset)
		if err != nil {
			return false, err
		}
		if err := c.rl.recvIn(off, data); err != nil {
			return false, err
		}
	}
	if err := c.rl.ack(ack); err != nil {
		return false, err
	}
	return len(data) > 0, nil
}

// batchUpLoop retains one in-flight upload at a time. A failed request leaves
// its bytes in replay; supervise exchanges offsets before any retry, avoiding
// the ambiguous "server accepted it but its 200 was lost" duplicate delivery.
func (c *resumableConn) batchUpLoop(ctx context.Context) error {
	buf := make([]byte, c.maxSendChunk)
	c.rl.resetAck()
	for {
		off, n, err := c.rl.nextOut(buf, c.limits.PollTimeout(), c.coalesceWindow)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && !errors.Is(err, errPipeInterrupted) {
			return err
		}
		_, due := c.rl.takeAck()
		if n == 0 && !due {
			continue
		}
		if err := c.doBatchSend(ctx, off, buf[:n]); err != nil {
			return err
		}
	}
}

func (c *resumableConn) doBatchSend(ctx context.Context, off uint64, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.pollURL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set(HeaderSendOnly, "true")
	req.Header.Set("Content-Type", "application/octet-stream")
	setBatchOffset(req.Header, HeaderSendOffset, off)
	setBatchOffset(req.Header, HeaderRecvOffset, c.rl.recvOffsetNow())
	c.setAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return &fatalPollError{status: resp.StatusCode}
	}
	// Batch send responses have no body. Finish reading before accepting the
	// ack, so truncated/invalid responses are never mistaken for completion.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1))
	if err != nil {
		return err
	}
	if len(body) != 0 {
		return errors.New("pollmux: unexpected batch send response body")
	}
	ack, err := batchOffset(resp.Header, HeaderRecvOffset)
	if err != nil {
		return err
	}
	return c.rl.ack(ack)
}
