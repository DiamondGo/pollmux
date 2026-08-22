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

	// SessionSuperseded is closed when this session was replaced by a newer
	// connect from the same Connector. Callers should stop reconnecting — a
	// fresher session is already active.
	SessionSuperseded() <-chan struct{}

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
	// PreferStream asks the server to negotiate PollModeStream. Ignored by a
	// server not configured for it — Connect falls back to the ordinary
	// batch poll loop with no error and no visible difference to the caller
	// beyond throughput.
	PreferStream bool
	// UploadStreamPreference controls the upload direction independently of
	// PreferStream, mirroring the wire-level independence between
	// PreferStreamMode and PreferStreamUpload:
	//   - PollModeBatch: never use upload streaming, even if the server
	//     offers it. No probe is run.
	//   - PollModeStream: always use upload streaming once the server
	//     offers it. No probe is run — use this only once you have already
	//     confirmed the path (proxies, CDNs) between client and server
	//     forwards a long-lived chunked request body in real time.
	//   - "" (default): auto-detect. If the server offers upload streaming,
	//     Connect runs a one-time probe (bounded by UploadProbeTimeout)
	//     before deciding; a path that fails the probe falls back to the
	//     discrete per-chunk send path for the lifetime of this connection.
	UploadStreamPreference string
	// UploadProbeTimeout bounds the auto-detect probe run when
	// UploadStreamPreference is "". Defaults to DefaultUploadProbeTimeout.
	UploadProbeTimeout time.Duration
	// PreferWebSocket asks the server to attach this connection over a
	// single WebSocket instead of the poll/send-stream request pair
	// PreferStream/UploadStreamPreference negotiate. Ignored by a server not
	// configured for it (ServerConfig.EnableWebSocket false) — Connect falls
	// back to whatever PreferStream/UploadStreamPreference negotiated, with
	// no error and no visible difference to the caller beyond throughput and
	// how well the link survives a proxy that buffers long-lived HTTP
	// bodies (see README's "五、WebSocket 传输模式"). When this is honoured,
	// UploadStreamPreference and its probe are skipped entirely — a single
	// WebSocket connection carries both directions, so there is no separate
	// upload leg to probe.
	PreferWebSocket bool
	// Logger receives transport diagnostics. Nil disables logging.
	Logger *slog.Logger

	activeSessionMu sync.Mutex
	activeSessionID string
}

func (c *Connector) adoptSession(id string) {
	c.activeSessionMu.Lock()
	c.activeSessionID = id
	c.activeSessionMu.Unlock()
}

func (c *Connector) isActiveSession(id string) bool {
	c.activeSessionMu.Lock()
	defer c.activeSessionMu.Unlock()
	return c.activeSessionID == id
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
	switch c.UploadStreamPreference {
	case "", PollModeBatch, PollModeStream:
	default:
		return nil, fmt.Errorf("pollmux: Connector.UploadStreamPreference = %q, only \"\", %q, and %q are valid",
			c.UploadStreamPreference, PollModeBatch, PollModeStream)
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

	// WebSocket is negotiated independently of, and takes priority over, the
	// PollMode/UploadStreamMode branch below: a session either attaches over
	// one WebSocket or it doesn't, and if it does none of the poll/
	// send-stream machinery below is relevant. An old server, or one with
	// EnableWebSocket off, never sets Transport, so this branch is simply
	// never taken and every existing caller is unaffected.
	if cr.Transport == TransportWebSocket {
		if cr.Limits.HeartbeatIntervalMS <= 0 {
			return nil, fmt.Errorf("pollmux: server negotiated websocket transport but sent non-positive heartbeat_interval_ms=%d",
				cr.Limits.HeartbeatIntervalMS)
		}
		return c.connectWebSocket(ctx, base, cr, dialTimeout, pollGrace)
	}

	negotiatedMode := PollModeBatch
	if cr.PollMode == PollModeStream {
		negotiatedMode = PollModeStream
	}
	// Negotiated independently of negotiatedMode on the wire (see DESIGN.md),
	// but both need the same Heartbeat/StreamMaxDuration limits, so the
	// validation below fires whenever either one is stream.
	negotiatedUpload := cr.UploadStreamMode == PollModeStream
	if negotiatedMode == PollModeStream || negotiatedUpload {
		if cr.Limits.HeartbeatIntervalMS <= 0 {
			return nil, fmt.Errorf("pollmux: server negotiated stream mode but sent non-positive heartbeat_interval_ms=%d",
				cr.Limits.HeartbeatIntervalMS)
		}
		if cr.Limits.StreamMaxDurationMS <= 0 {
			return nil, fmt.Errorf("pollmux: server negotiated stream mode but sent non-positive stream_max_duration_ms=%d",
				cr.Limits.StreamMaxDurationMS)
		}
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
	//
	// Stream mode's response headers come back almost immediately (see
	// pollStream's immediate flush) — the long wait moves inside the body —
	// so its ResponseHeaderTimeout only needs to cover connection setup, not
	// a long-poll wait, unlike batch's PollTimeout()+pollGrace.
	var pollTransport *http.Transport
	if negotiatedMode == PollModeStream {
		pollTransport = c.newTransport(dialTimeout, pollGrace)
	} else {
		pollTransport = c.newTransport(dialTimeout, cr.Limits.PollTimeout()+pollGrace)
	}
	sendTransport := c.newTransport(dialTimeout, sendTimeout)

	connCtx, cancel := context.WithCancel(context.Background())
	conn := &httpConn{
		sessionGuard:    newSessionGuard(c, cr.SessionID, cancel),
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

		streamMode:        negotiatedMode == PollModeStream,
		streamIdleTimeout: cr.Limits.HeartbeatInterval() + pollGrace,
		streamMaxFrame:    orInt(cr.Limits.PollBufferBytes, DefaultPollBufferSize),
	}

	if negotiatedUpload {
		// The response to a real send-stream request only arrives once the
		// whole request ends (StreamMaxDuration rollover or the connection
		// closing), so — unlike sendClient's short SendTimeout for a
		// discrete send — this transport's ResponseHeaderTimeout has to
		// cover a whole stream leg, the same reasoning as pollTransport's
		// batch-mode branch above. The auto-detect probe below bounds itself
		// separately with its own request context, so it is not blocked by
		// this long timeout.
		sendStreamTransport := c.newTransport(dialTimeout, cr.Limits.StreamMaxDuration()+pollGrace)
		conn.sendStreamClient = &http.Client{Transport: sendStreamTransport, CheckRedirect: noRedirect}

		switch c.UploadStreamPreference {
		case PollModeStream:
			// Forced on: the caller has already confirmed the path forwards
			// a long-lived chunked body in real time, so skip the probe.
			conn.uploadStreamMode = true
		case PollModeBatch:
			// Unreachable in practice — doConnect only sets
			// PreferStreamUpload when UploadStreamPreference != PollModeBatch,
			// so the server would never have negotiated negotiatedUpload —
			// but kept explicit rather than falling through the default
			// (auto-probe) case if that invariant ever changes.
			conn.uploadStreamMode = false
		default: // "" — auto-detect
			probeTimeout := orDuration(c.UploadProbeTimeout, DefaultUploadProbeTimeout)
			conn.uploadStreamMode = conn.probeUploadStream(probeTimeout)
			if !conn.uploadStreamMode {
				logger.Warn("pollmux: upload-stream probe did not pass within timeout, "+
					"falling back to batch uploads for this connection — "+
					"a proxy on this path likely buffers a long-lived request body instead of forwarding it live",
					"probe_timeout", probeTimeout)
			}
		}

		if conn.uploadStreamMode {
			conn.writePipe = NewBufferedPipe()
		}
	}

	c.adoptSession(cr.SessionID)

	logger.Debug("pollmux: connected",
		"max_send_chunk", effectiveChunk,
		"poll_timeout", cr.Limits.PollTimeout(),
		"session_timeout", cr.Limits.SessionTimeout(),
		"poll_buffer_bytes", cr.Limits.PollBufferBytes,
		"poll_mode", negotiatedMode,
		"upload_stream_mode", conn.uploadStreamMode,
	)

	conn.wg.Add(1)
	if conn.streamMode {
		go conn.pollLoopStream()
	} else {
		go conn.pollLoop()
	}

	if conn.uploadStreamMode {
		conn.wg.Add(1)
		go conn.sendLoopStream()
	}

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
		ProtocolVersion:    ProtocolVersion,
		Meta:               c.Meta,
		PreferStreamMode:   c.PreferStream,
		PreferStreamUpload: c.UploadStreamPreference != PollModeBatch,
		PreferWebSocket:    c.PreferWebSocket,
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
	sessionGuard
	sessionID string
	pollURL   string
	deleteURL string
	authToken string

	pollClient       *http.Client
	sendClient       *http.Client
	sendStreamClient *http.Client // only set when uploadStreamMode is true

	limits Limits
	meta   map[string]string

	maxSendChunk   int
	pollInterval   time.Duration
	coalesceWindow time.Duration
	localHealth    func() bool
	logger         *slog.Logger

	readPipe *BufferedPipe

	// streamMode, streamIdleTimeout, and streamMaxFrame are only meaningful
	// when streamMode is true: streamIdleTimeout is HeartbeatInterval+
	// PollGrace, the read-idle watchdog's timeout, and streamMaxFrame caps a
	// single decoded data frame's payload (from Limits.PollBufferBytes).
	streamMode        bool
	streamIdleTimeout time.Duration
	streamMaxFrame    int

	transportFailed chan struct{}
	failOnce        sync.Once

	closed atomic.Bool
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Write-side coalescing for the batch write path (uploadStreamMode ==
	// false). yamux issues a frame's header and body as two back-to-back
	// writes, and under load many frames queue up while a previous send is
	// in flight. One HTTP request per write is fine on a local link but
	// turns every tiny write into a full round trip over real RTT.
	writeMu       sync.Mutex
	writeBuf      []byte
	writeFlightOn bool

	// uploadStreamMode and writePipe drive the upload-stream write path
	// instead: Write() pushes into writePipe (a BufferedPipe, same primitive
	// the server already uses for the download direction — see DESIGN.md's
	// "关键洞察") and sendLoopStream continuously drains it into one
	// long-lived POST body at a time, only set up when the server negotiated
	// it. writeMu/writeBuf/writeFlightOn above are unused in this mode.
	uploadStreamMode bool
	writePipe        *BufferedPipe
}

func (c *httpConn) SessionID() string { return c.sessionID }
func (c *httpConn) Limits() Limits    { return c.limits }

func (c *httpConn) Meta() map[string]string {
	return maps.Clone(c.meta)
}

func (c *httpConn) TransportFailed() <-chan struct{} { return c.transportFailed }

func (c *httpConn) onSessionClosed() {
	if c.supersededByNewerConnect(c.logger) {
		if c.readPipe != nil {
			c.readPipe.Close()
		}
		if c.writePipe != nil {
			c.writePipe.Close()
		}
		return
	}
	c.fail()
}

func (c *httpConn) onTransportProblem(err error) {
	if isSessionClosedErr(err) {
		c.onSessionClosed()
		return
	}
	if c.supersededByNewerConnect(c.logger) {
		if c.readPipe != nil {
			c.readPipe.Close()
		}
		if c.writePipe != nil {
			c.writePipe.Close()
		}
		return
	}
	c.logger.Warn("pollmux: transport problem, signalling failure", "error", err)
	c.fail()
}

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

	if c.uploadStreamMode {
		return c.writePipe.Write(p)
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
	if c.writePipe != nil {
		// Unblocks a feedSendStream parked in ReadAvailable so
		// sendLoopStream's current doSendStream call can wind down and the
		// goroutine c.wg.Wait() below is waiting on can exit.
		c.writePipe.Close()
	}
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
			if isSessionClosedErr(err) {
				c.onSessionClosed()
				return
			}
			c.onTransportProblem(err)
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

// errStreamEnd is doStreamPoll's sentinel for a clean end of one stream-mode
// poll response — an explicit frameEnd, or the body closing exactly at a
// frame boundary (see frameReader.next). pollLoopStream treats it as "open
// the next stream poll immediately", the same spirit as pollLoop re-polling
// right after data arrives.
var errStreamEnd = errors.New("pollmux: stream poll ended cleanly")

// errSessionGone is doStreamPoll's sentinel for a frameGone frame: the
// session was closed server-side. pollLoopStream treats any error other
// than errStreamEnd as fatal, so returning this (rather than errStreamEnd)
// is what makes pollLoopStream call c.fail() instead of reopening a poll
// against a session that no longer exists.
var errSessionGone = errors.New("pollmux: session closed by server")

// pollLoopStream is pollLoop's stream-mode counterpart: it holds one
// long-lived poll response open at a time and decodes frames from it as they
// arrive, instead of one discrete request per buffer. It is a separate loop
// rather than a shared one with pollLoop because the two have different
// failure semantics end to end (idle watchdog vs. ResponseHeaderTimeout,
// frameEnd vs. 204) — folding them together would hide that difference
// behind a flag instead of making it visible in the code.
func (c *httpConn) pollLoopStream() {
	defer c.wg.Done()
	for {
		if c.ctx.Err() != nil {
			return
		}

		err := c.doStreamPoll()
		if errors.Is(err, errStreamEnd) {
			continue // clean end: reopen a new stream poll right away
		}
		if c.ctx.Err() != nil || c.closed.Load() {
			return // deliberate shutdown, not a failure
		}
		if isSessionClosedErr(err) {
			c.onSessionClosed()
			return
		}
		c.onTransportProblem(err)
		return
	}
}

// doStreamPoll issues one stream-mode long poll and decodes frames from the
// response body as they arrive, writing each data frame's payload into the
// read pipe as soon as it is decoded — it never waits for the response to
// end. It always returns a non-nil error: errStreamEnd for a clean end,
// anything else for a real transport problem.
func (c *httpConn) doStreamPoll() error {
	reqCtx, cancelReq := context.WithCancel(c.ctx)
	defer cancelReq()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.pollURL, nil)
	if err != nil {
		return err
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
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		// fall through to frame decoding
	case http.StatusGone:
		return &fatalPollError{status: resp.StatusCode, detail: "session closed by server"}
	default:
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<10))
		return &fatalPollError{status: resp.StatusCode, detail: strings.TrimSpace(string(detail))}
	}

	// Idle watchdog: any frame, data or heartbeat, resets it. Nothing for
	// HeartbeatInterval+grace means the link is stuck even though the
	// response header already came back fine — cancelReq aborts just this
	// request's body read, and pollLoopStream reopens a fresh stream right
	// after (reported as a failure below via the reqCtx.Err() check).
	watchdog := time.AfterFunc(c.streamIdleTimeout, cancelReq)
	defer watchdog.Stop()

	fr := newFrameReader(resp.Body, c.streamMaxFrame)
	for {
		typ, payload, err := fr.next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return errStreamEnd // body closed cleanly at a frame boundary
			}
			if reqCtx.Err() != nil && c.ctx.Err() == nil {
				return fmt.Errorf("pollmux: stream idle watchdog fired after %v with no frame", c.streamIdleTimeout)
			}
			return fmt.Errorf("reading stream frame: %w", err)
		}
		watchdog.Reset(c.streamIdleTimeout)

		switch typ {
		case frameData:
			if len(payload) > 0 {
				if _, err := c.readPipe.Write(payload); err != nil {
					return fmt.Errorf("read pipe closed: %w", err)
				}
			}
		case frameHeartbeat:
			// nothing to do beyond the watchdog reset above
		case frameEnd:
			return errStreamEnd
		case frameGone:
			// The session was closed server-side. Unlike frameEnd, reopening
			// a poll against this same session id would just get frameGone
			// again forever — this is fatal, the same role batch mode's 410
			// plays, so pollLoopStream must call c.fail() instead of looping.
			return errSessionGone
		default:
			return fmt.Errorf("pollmux: unknown stream frame type %#x", typ)
		}
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
					c.onTransportProblem(err)
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

// probeUploadStream is Connect's connect-time auto-detect check, run once
// when Connector.UploadStreamPreference == "". It pushes uploadProbeFillerBytes
// of filler through a real send-stream request — comfortably more than
// MaxStreamWindowSize, the amount of unacknowledged upload data that
// reproduced the real Cloudflare hang (see README) — then ends the request
// (frameEnd) and waits for the response, bounded by timeout.
//
// This deliberately reuses the exact shape and timing of a real send-stream
// leg (see doSendStream/feedSendStream) rather than inventing a bespoke
// mid-stream-ack fast path: an early "reply while the request is still open"
// design turned out to be unreliable even on a local loopback connection —
// whether the client's HTTP transport surfaces the response before the body
// finishes sending depends on internal buffering behavior that isn't part of
// any documented contract. A bounded probe of realistic size, timed
// end-to-end, is slower to fail (up to timeout) but actually trustworthy.
//
// The probe never touches writePipe or the real session: the server
// recognizes HeaderSendStreamProbe and discards the frames instead of
// writing them upstream, so this is safe to run before any real data is
// queued to send.
func (c *httpConn) probeUploadStream(timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()

	pr, pw := io.Pipe()
	go func() {
		// Chunked well under any reasonable MaxSendBytes, rather than one
		// uploadProbeFillerBytes-sized frame, so a deployment with a small
		// configured MaxSendBytes still decodes the probe instead of
		// rejecting an oversized frame.
		const probeChunk = 32 << 10
		chunk := make([]byte, probeChunk)
		for sent := 0; sent < uploadProbeFillerBytes; {
			n := min(probeChunk, uploadProbeFillerBytes-sent)
			if err := writeFrame(pw, frameData, chunk[:n]); err != nil {
				pw.CloseWithError(err)
				return
			}
			sent += n
		}
		writeFrame(pw, frameEnd, nil)
		pw.Close()
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.pollURL, pr)
	if err != nil {
		pw.CloseWithError(err)
		return false
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(HeaderSendStream, "true")
	req.Header.Set(HeaderSendStreamProbe, "true")
	req.ContentLength = -1
	c.setAuth(req)

	resp, err := c.sendStreamClient.Do(req)
	if err != nil {
		// Includes context.DeadlineExceeded — the expected outcome on a path
		// that does not forward this request to the origin within timeout.
		return false
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	return resp.StatusCode == http.StatusOK
}

// sendLoopStream is flushLoop's upload-stream counterpart: instead of one
// discrete send-only request per chunk, it holds one long-lived POST body
// open at a time and feeds it frames as writePipe accumulates data, reopening
// a new request only when doSendStream ends cleanly (a client-decided
// rollover, or writePipe closing because Close() was called).
func (c *httpConn) sendLoopStream() {
	defer c.wg.Done()
	for {
		if c.ctx.Err() != nil {
			return
		}

		if err := c.doSendStream(); err != nil {
			if c.ctx.Err() != nil || c.closed.Load() {
				return // deliberate shutdown, not a failure
			}
			c.onTransportProblem(err)
			return
		}
		// nil error: a clean, client-decided rollover. Open the next request
		// immediately, the same spirit as pollLoopStream reopening after
		// errStreamEnd.
	}
}

// doSendStream opens one send-stream POST, whose body is fed by
// feedSendStream running concurrently, and waits for the server's response.
// The server only answers once the request body ends (see pollSendStream),
// so this call's lifetime is bounded by whatever feedSendStream decides —
// normally one StreamMaxDuration-ish leg.
func (c *httpConn) doSendStream() error {
	pr, pw := io.Pipe()
	feedErrCh := make(chan error, 1)
	go func() { feedErrCh <- c.feedSendStream(pw) }()

	req, err := http.NewRequestWithContext(c.ctx, http.MethodPost, c.pollURL, pr)
	if err != nil {
		pw.CloseWithError(err)
		<-feedErrCh
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(HeaderSendStream, "true")
	// Chunked transfer encoding, since the body's total length is not known
	// upfront — the same reason pollStream's response on the server side
	// carries no Content-Length either.
	req.ContentLength = -1
	c.setAuth(req)

	resp, err := c.sendStreamClient.Do(req)
	feedErr := <-feedErrCh // feedSendStream always returns once pw is closed one way or another
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))

	switch resp.StatusCode {
	case http.StatusOK:
		return feedErr // nil on a clean rollover; non-nil only if the local feeder itself failed
	case http.StatusGone:
		return &fatalPollError{status: resp.StatusCode, detail: "session closed by server"}
	default:
		return &fatalPollError{status: resp.StatusCode}
	}
}

// feedSendStream drains writePipe into pw as a sequence of frames until it
// decides to end this leg — either a clean StreamMaxDuration rollover
// (always checked right after a ReadAvailable call returns, never mid-frame:
// see DESIGN.md's "谁来决定轮转" for why the writer, not the reader, must own
// this decision) or writePipe closing because the connection is shutting
// down. It always returns nil unless writing to pw itself fails, in which
// case doSendStream's request is already doomed and the error is surfaced
// through sendLoopStream via doSendStream's own resp/err handling.
func (c *httpConn) feedSendStream(pw *io.PipeWriter) error {
	buf := make([]byte, c.streamMaxFrame)
	heartbeat := c.limits.HeartbeatInterval()
	deadline := time.Now().Add(c.limits.StreamMaxDuration())

	for {
		n, err := c.writePipe.ReadAvailable(buf, heartbeat, c.coalesceWindow)
		if errors.Is(err, io.EOF) {
			// writePipe closed: Close() was called. End this leg cleanly;
			// sendLoopStream's own c.ctx/c.closed check stops it from
			// reopening another one.
			writeFrame(pw, frameEnd, nil)
			pw.Close()
			return nil
		}

		if n > 0 {
			if err := writeFrame(pw, frameData, buf[:n]); err != nil {
				pw.CloseWithError(err)
				return err
			}
		} else if err := writeFrame(pw, frameHeartbeat, nil); err != nil {
			pw.CloseWithError(err)
			return err
		}

		if time.Now().After(deadline) {
			writeFrame(pw, frameEnd, nil)
			pw.Close()
			return nil
		}
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

func orInt(v, fallback int) int {
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
