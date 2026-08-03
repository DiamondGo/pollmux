package pollmux

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrProtocolVersion is returned when the server answers 426. Retrying cannot
// help — both ends have to be upgraded together — so ReconnectLoop gives up
// instead of backing off forever.
var ErrProtocolVersion = errors.New("pollmux: server rejected our protocol version")

// ErrUnauthorized is returned when the server rejects our credentials. Unlike
// ErrProtocolVersion this is worth retrying: an operator may be fixing the
// token right now.
var ErrUnauthorized = errors.New("pollmux: authentication rejected")

// Conn is a long-poll virtual connection. It satisfies io.ReadWriteCloser, so
// it can be handed straight to ClientSession or ServerSession.
type Conn interface {
	io.ReadWriteCloser

	// TransportFailed is closed when the HTTP transport itself fails: a network
	// error, an invalid session, or a poll that outlived its response header
	// timeout.
	//
	// This is a different event from the yamux session ending because the peer
	// closed it. A caller that only watches the yamux session cannot tell "the
	// link broke" from "the peer left", and those want different responses —
	// backoff for the first, immediate retry for the second — so callers must
	// select on both.
	TransportFailed() <-chan struct{}

	// Limits returns the transport parameters the server handed down at connect
	// time. The client has already clamped itself to them.
	Limits() Limits

	// SessionID returns the server-assigned session id, useful for correlating
	// logs across both ends.
	SessionID() string

	// Meta returns the metadata the server returned at connect time, which is
	// whatever Hooks.Authenticate produced merged with what we declared.
	Meta() map[string]string
}

// Connector dials a pollmux server and returns a Conn.
//
// The zero value is not usable: BaseURL is required. Every timing field has a
// sane default, and the server's Limits override anything here that would be
// more aggressive than the server allows — the client may only be more
// conservative, never less.
type Connector struct {
	// BaseURL is the server root, e.g. "https://broker.example.com".
	BaseURL string
	// PathPrefix defaults to "/tunnel".
	PathPrefix string
	// AuthToken, if set, is sent as a bearer token on every request.
	AuthToken string
	// Meta is application-defined and opaque to pollmux — a relay might send
	// {role, endpoint}, a tunnel service {client_id}.
	Meta map[string]string

	// PollInterval is an extra pause between polls. It defaults to zero on
	// purpose: the waiting belongs in the server's long poll, and any value
	// here is added latency on every exchange. Connect rejects a value large
	// enough to push the polling gap past the server's session timeout.
	PollInterval time.Duration
	// PollGrace is added to the server's poll timeout to form the poll
	// request's ResponseHeaderTimeout. Defaults to DefaultPollGrace.
	PollGrace time.Duration
	// SendTimeout bounds a send-only request. Defaults to DefaultSendTimeout.
	SendTimeout time.Duration
	// DialTimeout bounds TCP connection setup. Defaults to DefaultDialTimeout.
	DialTimeout time.Duration
	// CoalesceWindow is how long the first send of a burst waits for
	// immediately-following writes to merge in. Defaults to
	// DefaultCoalesceWindow.
	CoalesceWindow time.Duration
	// MaxSendChunk caps a single request body. The effective value is the
	// smaller of this and the server's max_send_bytes.
	MaxSendChunk int

	// InsecureSkipVerify disables TLS verification. For self-signed test certs.
	InsecureSkipVerify bool
	// LocalHealth, if set, is called before each poll and reported to the
	// server in X-Local-Health. It lets the server distinguish "the tunnel is
	// up" from "the tunnel is up but the service behind it is down".
	LocalHealth func() bool
	// Logger receives transport diagnostics. Nil disables logging.
	Logger *slog.Logger
}

func (c *Connector) pathPrefix() string {
	if c.PathPrefix == "" {
		return "/tunnel"
	}
	return "/" + strings.Trim(c.PathPrefix, "/")
}

func (c *Connector) logger() *slog.Logger {
	if c.Logger == nil {
		return slog.New(discardHandler{})
	}
	return c.Logger
}

// Connect registers with the server and returns a ready virtual connection.
func (c *Connector) Connect(ctx context.Context) (Conn, error) {
	if c.BaseURL == "" {
		return nil, errors.New("pollmux: Connector.BaseURL is required")
	}

	base := strings.TrimRight(c.BaseURL, "/") + c.pathPrefix()
	dialTimeout := orDuration(c.DialTimeout, DefaultDialTimeout)
	sendTimeout := orDuration(c.SendTimeout, DefaultSendTimeout)
	pollGrace := orDuration(c.PollGrace, DefaultPollGrace)
	coalesce := orDuration(c.CoalesceWindow, DefaultCoalesceWindow)
	maxSendChunk := c.MaxSendChunk
	if maxSendChunk <= 0 {
		maxSendChunk = DefaultMaxSendChunk
	}

	// The connect request itself is a short exchange, so it uses the same
	// timeouts as a send rather than the poll client's long ones.
	connectClient := &http.Client{
		Timeout:       sendTimeout,
		Transport:     c.newTransport(dialTimeout, sendTimeout),
		CheckRedirect: noRedirect,
	}

	cr, err := c.doConnect(ctx, connectClient, base)
	if err != nil {
		return nil, err
	}

	if err := cr.Limits.Validate(); err != nil {
		return nil, err
	}

	// Startup self-check. A poll cycle longer than the server's session timeout
	// means a perfectly healthy client gets swept as dead, so fail loudly at
	// startup rather than reconnect-looping in production.
	if cycle := cr.Limits.PollTimeout() + c.PollInterval; cycle >= cr.Limits.SessionTimeout() {
		return nil, fmt.Errorf(
			"pollmux: PollInterval=%v is too large: a poll cycle of %v (server poll_timeout=%v + PollInterval) "+
				"reaches the server's session_timeout=%v, so this client would be swept as dead while healthy; "+
				"lower PollInterval or raise the server's session_timeout",
			c.PollInterval, cycle, cr.Limits.PollTimeout(), cr.Limits.SessionTimeout(),
		)
	}

	// The server is authoritative: we may clamp down, never up. This is what
	// makes an oversized request impossible from a well-behaved client, and in
	// turn what lets the server treat 413 as a protocol violation rather than
	// something to recover from.
	effectiveChunk := min(maxSendChunk, cr.Limits.MaxSendBytes)

	logger := c.logger().With("session_id", cr.SessionID)

	// Two clients, deliberately not shared. The poll client must tolerate a
	// response header withheld for the whole long-poll timeout; the send client
	// must not, or a stalled send would sit undetected for that same span.
	pollTransport := c.newTransport(dialTimeout, cr.Limits.PollTimeout()+pollGrace)
	sendTransport := c.newTransport(dialTimeout, sendTimeout)

	connCtx, cancel := context.WithCancel(context.Background())
	conn := &httpConn{
		sessionID:       cr.SessionID,
		pollURL:         fmt.Sprintf("%s/%s/poll", base, cr.SessionID),
		deleteURL:       fmt.Sprintf("%s/%s", base, cr.SessionID),
		authToken:       c.AuthToken,
		pollClient:      &http.Client{Transport: pollTransport, CheckRedirect: noRedirect},
		sendClient:      &http.Client{Timeout: sendTimeout, Transport: sendTransport, CheckRedirect: noRedirect},
		limits:          cr.Limits,
		meta:            cr.Meta,
		maxSendChunk:    effectiveChunk,
		pollInterval:    c.PollInterval,
		coalesceWindow:  coalesce,
		localHealth:     c.LocalHealth,
		logger:          logger,
		readPipe:        NewBufferedPipe(),
		transportFailed: make(chan struct{}),
		ctx:             connCtx,
		cancel:          cancel,
	}

	logger.Debug("pollmux: connected",
		"max_send_chunk", effectiveChunk,
		"poll_timeout", cr.Limits.PollTimeout(),
		"session_timeout", cr.Limits.SessionTimeout(),
		"poll_buffer_bytes", cr.Limits.PollBufferBytes,
	)

	conn.wg.Add(1)
	go conn.pollLoop()

	return conn, nil
}

// newTransport builds a transport with the timeouts that make a silent
// blackhole detectable. Leaving these at zero — as the pre-library code did —
// means a poll can hang forever with no RST or FIN ever arriving, and the
// transport-failed signal never fires (A1).
func (c *Connector) newTransport(dialTimeout, responseHeaderTimeout time.Duration) *http.Transport {
	t := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 15 * time.Second,
		}).DialContext,
		ResponseHeaderTimeout: responseHeaderTimeout,
		IdleConnTimeout:       90 * time.Second,
	}
	if c.InsecureSkipVerify {
		t.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return t
}

// doConnect performs POST {prefix}/connect and parses the response.
func (c *Connector) doConnect(ctx context.Context, client *http.Client, base string) (*ConnectResponse, error) {
	body, err := json.Marshal(ConnectRequest{
		ProtocolVersion: ProtocolVersion,
		Meta:            c.Meta,
	})
	if err != nil {
		return nil, fmt.Errorf("pollmux: failed to encode connect request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/connect", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("pollmux: failed to build connect request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.AuthToken)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("pollmux: connect failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		switch resp.StatusCode {
		case http.StatusUnauthorized:
			return nil, fmt.Errorf("%w (status 401) — check that AuthToken matches the server: %s",
				ErrUnauthorized, strings.TrimSpace(string(detail)))
		case http.StatusUpgradeRequired:
			return nil, fmt.Errorf("%w: we speak version %d, server said: %s",
				ErrProtocolVersion, ProtocolVersion, strings.TrimSpace(string(detail)))
		case http.StatusFound, http.StatusMovedPermanently, http.StatusSeeOther,
			http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
			// Servers with an anti-scanning redirect answer this to anyone who
			// fails auth. Following it would turn a clear auth failure into a
			// confusing parse error somewhere else.
			return nil, fmt.Errorf("pollmux: server redirected to %q (status %d) — usually a failed auth token "+
				"or an unauthorized-redirect rule; check AuthToken",
				resp.Header.Get("Location"), resp.StatusCode)
		default:
			return nil, fmt.Errorf("pollmux: connect returned status %d: %s",
				resp.StatusCode, strings.TrimSpace(string(detail)))
		}
	}

	var cr ConnectResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&cr); err != nil {
		return nil, fmt.Errorf("pollmux: failed to parse connect response: %w", err)
	}
	if cr.SessionID == "" {
		return nil, errors.New("pollmux: server returned an empty session_id")
	}
	if cr.ProtocolVersion != 0 && cr.ProtocolVersion != ProtocolVersion {
		return nil, fmt.Errorf("%w: we speak version %d, server answered version %d",
			ErrProtocolVersion, ProtocolVersion, cr.ProtocolVersion)
	}
	return &cr, nil
}

// httpConn is the Conn implementation: a poll loop pulling data down, and a
// flush loop pushing data up as separate requests.
//
// Read and write are deliberately independent. Because a send never waits for
// the poll in flight, one direction can never head-of-line block the other.
type httpConn struct {
	sessionID string
	pollURL   string
	deleteURL string
	authToken string

	pollClient *http.Client
	sendClient *http.Client

	limits Limits
	meta   map[string]string

	maxSendChunk   int
	pollInterval   time.Duration
	coalesceWindow time.Duration
	localHealth    func() bool
	logger         *slog.Logger

	readPipe *BufferedPipe

	transportFailed chan struct{}
	failOnce        sync.Once

	closed atomic.Bool
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Write-side coalescing. yamux issues a frame's header and body as two
	// back-to-back writes, and under load many frames queue up while a previous
	// send is in flight. One HTTP request per write is fine on a local link but
	// turns every tiny write into a full round trip over real RTT.
	writeMu       sync.Mutex
	writeBuf      []byte
	writeFlightOn bool
}

func (c *httpConn) SessionID() string { return c.sessionID }
func (c *httpConn) Limits() Limits    { return c.limits }

func (c *httpConn) Meta() map[string]string {
	return maps.Clone(c.meta)
}

func (c *httpConn) TransportFailed() <-chan struct{} { return c.transportFailed }

// fail signals transport failure once and unblocks anyone reading.
func (c *httpConn) fail() {
	c.failOnce.Do(func() {
		close(c.transportFailed)
		c.readPipe.Close()
	})
}

// Read returns data the server sent down the poll channel.
func (c *httpConn) Read(p []byte) (int, error) { return c.readPipe.Read(p) }

// Write queues data for delivery. It returns once the data is queued, not once
// the server has it — the same guarantee a real net.Conn's Write gives. A send
// failure surfaces asynchronously through TransportFailed.
func (c *httpConn) Write(p []byte) (int, error) {
	if c.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}

	c.writeMu.Lock()
	c.writeBuf = append(c.writeBuf, p...)
	if c.writeFlightOn {
		c.writeMu.Unlock()
		return len(p), nil
	}
	c.writeFlightOn = true
	c.writeMu.Unlock()

	c.wg.Add(1)
	go c.flushLoop()

	return len(p), nil
}

// Close stops both loops and tells the server to drop the session.
func (c *httpConn) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}

	// Cancelling aborts the poll request in flight. Without this, Close would
	// block until the current long poll returned — up to a full poll timeout.
	c.cancel()
	c.wg.Wait()
	c.readPipe.Close()

	// The DELETE needs a live context of its own, since c.ctx is now cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.deleteURL, nil)
	if err == nil {
		c.setAuth(req)
		if resp, err := c.sendClient.Do(req); err == nil {
			io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
			resp.Body.Close()
		}
	}

	c.sendClient.CloseIdleConnections()
	c.pollClient.CloseIdleConnections()
	return nil
}

func (c *httpConn) setAuth(req *http.Request) {
	if c.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.authToken)
	}
}

// pollLoop holds a long poll open continuously, feeding whatever comes back
// into the read pipe. It never sends data; sends go out on their own requests.
func (c *httpConn) pollLoop() {
	defer c.wg.Done()

	readCap := c.limits.PollBufferBytes
	if readCap <= 0 {
		readCap = DefaultPollBufferSize
	}

	for {
		if c.ctx.Err() != nil {
			return
		}

		gotData, err := c.doPoll(int64(readCap))
		if err != nil {
			if c.ctx.Err() != nil || c.closed.Load() {
				return // deliberate shutdown, not a failure
			}
			c.logger.Warn("pollmux: poll failed, signalling transport failure", "error", err)
			c.fail()
			return
		}

		// Only pause on an idle response. After data, poll again immediately:
		// the next batch is probably already waiting.
		if !gotData && c.pollInterval > 0 {
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(c.pollInterval):
			}
		}
	}
}

// fatalPollError marks a status that ends the connection.
type fatalPollError struct {
	status int
	detail string
}

func (e *fatalPollError) Error() string {
	if e.detail == "" {
		return fmt.Sprintf("server returned status %d", e.status)
	}
	return fmt.Sprintf("server returned status %d: %s", e.status, e.detail)
}

// doPoll runs one long poll. It reports whether data came back.
func (c *httpConn) doPoll(readCap int64) (bool, error) {
	req, err := http.NewRequestWithContext(c.ctx, http.MethodPost, c.pollURL, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set(HeaderReceiveOnly, "true")
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

	switch resp.StatusCode {
	case http.StatusOK:
		data, err := io.ReadAll(io.LimitReader(resp.Body, readCap))
		if err != nil {
			return false, fmt.Errorf("reading poll response: %w", err)
		}
		if len(data) == 0 {
			return false, nil
		}
		if _, err := c.readPipe.Write(data); err != nil {
			return false, fmt.Errorf("read pipe closed: %w", err)
		}
		return true, nil

	case http.StatusNoContent:
		// The normal heartbeat: the long poll timed out with nothing to send.
		return false, nil

	case http.StatusGone:
		// A5. Distinct from 204 on purpose: the session is gone, so reconnect
		// now instead of polling a dead session until our own timeout.
		return false, &fatalPollError{status: resp.StatusCode, detail: "session closed by server"}

	case http.StatusRequestEntityTooLarge:
		// We clamp every send to the server's max_send_bytes, so this cannot
		// happen against a correct server. Say so loudly rather than trying to
		// recover: a silent retry path here would hide a real bug.
		c.logger.Error("pollmux: server reported a protocol violation (413) — " +
			"a request body exceeded the max_send_bytes handed down at connect time")
		return false, &fatalPollError{status: resp.StatusCode, detail: "protocol violation"}

	default:
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<10))
		return false, &fatalPollError{status: resp.StatusCode, detail: strings.TrimSpace(string(detail))}
	}
}

// flushLoop drains the write buffer until it is empty, splitting anything over
// the negotiated chunk size across requests.
//
// The coalesce window is paid once per burst, not per Write: once a request is
// in flight, further writes queue up and go out on the next iteration with no
// added wait.
func (c *httpConn) flushLoop() {
	defer c.wg.Done()

	first := true
	for {
		if first {
			select {
			case <-c.ctx.Done():
				c.clearFlight()
				return
			case <-time.After(c.coalesceWindow):
			}
			first = false
		}

		c.writeMu.Lock()
		buf := c.writeBuf
		c.writeBuf = nil
		if len(buf) == 0 {
			c.writeFlightOn = false
			c.writeMu.Unlock()
			return
		}
		c.writeMu.Unlock()

		for len(buf) > 0 {
			n := min(len(buf), c.maxSendChunk)
			if err := c.doSend(buf[:n]); err != nil {
				if c.ctx.Err() == nil && !c.closed.Load() {
					c.logger.Warn("pollmux: send failed, signalling transport failure", "error", err)
					c.fail()
				}
				c.clearFlight()
				return
			}
			buf = buf[n:]
		}
	}
}

func (c *httpConn) clearFlight() {
	c.writeMu.Lock()
	c.writeFlightOn = false
	c.writeMu.Unlock()
}

// doSend posts one chunk as a send-only request.
func (c *httpConn) doSend(chunk []byte) error {
	req, err := http.NewRequestWithContext(c.ctx, http.MethodPost, c.pollURL, bytes.NewReader(chunk))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(HeaderSendOnly, "true")
	req.ContentLength = int64(len(chunk))
	c.setAuth(req)

	resp, err := c.sendClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))

	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent:
		return nil
	case http.StatusRequestEntityTooLarge:
		c.logger.Error("pollmux: server rejected a send as too large (413) — "+
			"this is a protocol violation, the chunk size was already clamped to the server's limit",
			"chunk_bytes", len(chunk),
			"max_send_chunk", c.maxSendChunk,
		)
		return &fatalPollError{status: resp.StatusCode, detail: "protocol violation"}
	default:
		return &fatalPollError{status: resp.StatusCode}
	}
}

// noRedirect stops the HTTP client from following redirects, so a 3xx is
// visible to our status handling instead of being chased into a confusing
// error somewhere downstream.
func noRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

func orDuration(v, fallback time.Duration) time.Duration {
	if v <= 0 {
		return fallback
	}
	return v
}

// discardHandler drops every record, so a nil Logger costs nothing.
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (h discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h discardHandler) WithGroup(string) slog.Handler           { return h }
