// Package pollmux implements an HTTP long-polling virtual connection suitable
// for carrying a yamux session.
//
// The library only concerns itself with how bytes flow between two machines.
// It does not interpret those bytes, and it does not know what role either end
// plays. Application semantics (roles, endpoints, subdomains, client ids) travel
// as an opaque meta map.
//
// See README.md for the two constraints every caller must know: yamux keepalive
// must stay disabled, and the two directions have different throughput
// characteristics.
package pollmux

import (
	"fmt"
	"time"
)

// ProtocolVersion is the wire protocol version this build speaks. A server that
// receives a different version answers 426 and the client stops retrying —
// there is deliberately no legacy compatibility branch.
const ProtocolVersion = 1

// Request headers understood by PollHandler.
const (
	// HeaderSendOnly marks a request whose body is written to the session and
	// which returns immediately, without ever waiting for downstream data.
	HeaderSendOnly = "X-Send-Only"
	// HeaderReceiveOnly marks a long-poll request.
	HeaderReceiveOnly = "X-Receive-Only"
	// HeaderLocalHealth is an optional piggybacked report ("ok" or "down")
	// handed to Hooks.OnPoll. The library itself does not interpret it.
	HeaderLocalHealth = "X-Local-Health"
	// HeaderSendStream marks an upload-stream request: the body is a sequence
	// of frames (see frame.go), written continuously as data becomes
	// available and read continuously by the server, instead of one discrete
	// chunk per request. Mutually exclusive with HeaderSendOnly.
	HeaderSendStream = "X-Send-Stream"
	// HeaderSendStreamProbe marks a send-stream request as Connector's
	// connect-time transport probe (see Connector.UploadStreamPreference):
	// otherwise handled exactly like a real send-stream request, except the
	// server discards the frames instead of writing them upstream. Always
	// sent together with HeaderSendStream, never alone.
	HeaderSendStreamProbe = "X-Send-Stream-Probe"
)

// Server-side defaults. See ServerConfig.
const (
	DefaultPollTimeout    = 30 * time.Second
	DefaultSessionTimeout = 2 * DefaultPollTimeout // A3: 60s, not 5min
	DefaultSweepInterval  = 5 * time.Second
	DefaultPollBufferSize = 256 << 10 // B1 first layer: was a hard-coded 64KB
	DefaultMaxSendBytes   = 1 << 20

	// DefaultHeartbeatInterval and DefaultStreamMaxDuration only apply when
	// ServerConfig.PollMode == PollModeStream. Both are shared verbatim by
	// the upload direction (see ConnectRequest.PreferStreamUpload) — there is
	// no separate upload-specific pair of knobs.
	DefaultHeartbeatInterval = 10 * time.Second
	// DefaultStreamMaxDuration is kept comfortably under common intermediate
	// proxy read/idle timeouts (a 60s timeout is common; sitting right on top
	// of it leaves no margin), not just under SessionTimeout.
	DefaultStreamMaxDuration = 45 * time.Second

	// defaultStreamReadGrace is the server's local safety margin for the
	// upload-stream read-idle watchdog (PollHandler's pollSendStream): if no
	// frame — data or heartbeat — arrives within HeartbeatInterval plus this
	// much slack, the server gives up on the request. Unlike PollGrace this
	// never needs to match anything the client declares, so it stays an
	// unexported constant rather than a Limits field.
	defaultStreamReadGrace = 10 * time.Second
)

// Client-side defaults. See Connector.
const (
	// DefaultPollInterval is zero on purpose (D10): the waiting is supposed to
	// happen inside the server's long poll, not in a client-side sleep that
	// adds latency to every exchange.
	DefaultPollInterval = time.Duration(0)
	DefaultPollGrace    = 10 * time.Second
	DefaultSendTimeout  = 15 * time.Second
	DefaultDialTimeout  = 10 * time.Second
	DefaultMaxSendChunk = 512 << 10

	// DefaultUploadProbeTimeout bounds Connector's connect-time auto-detect
	// probe for upload streaming (see Connector.UploadStreamPreference).
	// Some intermediate proxies (observed with Cloudflare's standard tiers)
	// buffer a long-lived chunked request body instead of forwarding it to
	// the origin as it arrives, which — combined with yamux's flow-control
	// window — turns into a permanent hang under real traffic rather than a
	// clean error. The probe reproduces that exact condition once at connect
	// time so a broken path is caught here, bounded by this timeout, instead
	// of hanging a live session later.
	DefaultUploadProbeTimeout = 15 * time.Second
)

// uploadProbeFillerBytes is how many bytes of filler Connector's auto-detect
// probe (see probeUploadStream) pushes through a real send-stream request
// before ending it. It is deliberately more than MaxStreamWindowSize: that is
// the exact amount of unacknowledged upload data that reproduced the real
// Cloudflare hang (see README), so a probe that pushed less could pass on a
// path that still breaks under real traffic.
const uploadProbeFillerBytes = MaxStreamWindowSize + 4<<10

// DefaultCoalesceWindow is used by both directions when no explicit value is
// configured: the read side lets a poll response accumulate a little more data
// once any is available, and the write side lets back-to-back writes merge into
// one request. Negligible against real WAN round trips, but it bounds the added
// latency on a near-zero-latency link.
const DefaultCoalesceWindow = 2 * time.Millisecond

// Reconnect defaults. See ReconnectLoop.
const (
	DefaultInitialBackoff  = 1 * time.Second
	DefaultMaxBackoff      = 3 * time.Minute
	DefaultPeerClosedPause = 500 * time.Millisecond
)

// ConnectRequest is the JSON body of POST {prefix}/connect.
type ConnectRequest struct {
	ProtocolVersion int `json:"protocol_version"`
	// Meta is application-defined and opaque to pollmux — a relay might put
	// {role, endpoint} here, a tunnel service {client_id}.
	Meta map[string]string `json:"meta,omitempty"`
	// PreferStreamMode asks the server to negotiate PollModeStream for this
	// session. Ignored (silently falls back to batch) unless the server's own
	// ServerConfig.PollMode is PollModeStream — the server is always the
	// authority on what it supports.
	PreferStreamMode bool `json:"prefer_stream_mode,omitempty"`
	// PreferStreamUpload asks the server to also negotiate streamed uploads
	// (see ConnectResponse.UploadStreamMode). Negotiated independently of
	// PreferStreamMode on the wire — same server-side gate
	// (ServerConfig.PollMode == PollModeStream), but a separate field — so
	// that a new client talking to a server that only understands the older
	// download-only stream mode degrades cleanly instead of sending
	// HeaderSendStream requests the server has never heard of.
	PreferStreamUpload bool `json:"prefer_stream_upload,omitempty"`
	// PreferWebSocket asks the server to attach this session over a single
	// WebSocket connection (see ConnectResponse.Transport) instead of the
	// poll/send-stream request pair PreferStreamMode/PreferStreamUpload
	// negotiate. Independent of both of those on the wire — gated by its own
	// ServerConfig.EnableWebSocket, not ServerConfig.PollMode — so an old
	// server that has never heard of this field simply never sets
	// ConnectResponse.Transport and the client falls back to whatever
	// PreferStreamMode/PreferStreamUpload negotiated, unchanged.
	PreferWebSocket bool `json:"prefer_websocket,omitempty"`
}

// Limits are the transport parameters the server hands down at connect time.
//
// Every one of these is a number that must match on both ends, and the server
// is the only side that actually knows what it can accept. Pushing them down
// (rather than configuring them twice) is what makes a whole class of silent
// misconfiguration impossible — the same reasoning behind HTTP/2 SETTINGS and
// TCP MSS negotiation. A well-behaved client clamps itself to these, so a
// request that exceeds MaxSendBytes is a protocol violation rather than a
// situation to recover from gracefully.
type Limits struct {
	MaxSendBytes     int   `json:"max_send_bytes"`
	PollTimeoutMS    int64 `json:"poll_timeout_ms"`
	SessionTimeoutMS int64 `json:"session_timeout_ms"`
	PollBufferBytes  int   `json:"poll_buffer_bytes"`
	// HeartbeatIntervalMS and StreamMaxDurationMS are only set when this
	// connect negotiated PollModeStream (see ConnectResponse.PollMode). Zero
	// in a batch-mode response, same as today.
	HeartbeatIntervalMS int64 `json:"heartbeat_interval_ms,omitempty"`
	StreamMaxDurationMS int64 `json:"stream_max_duration_ms,omitempty"`
}

// PollTimeout returns PollTimeoutMS as a duration.
func (l Limits) PollTimeout() time.Duration {
	return time.Duration(l.PollTimeoutMS) * time.Millisecond
}

// SessionTimeout returns SessionTimeoutMS as a duration.
func (l Limits) SessionTimeout() time.Duration {
	return time.Duration(l.SessionTimeoutMS) * time.Millisecond
}

// HeartbeatInterval returns HeartbeatIntervalMS as a duration.
func (l Limits) HeartbeatInterval() time.Duration {
	return time.Duration(l.HeartbeatIntervalMS) * time.Millisecond
}

// StreamMaxDuration returns StreamMaxDurationMS as a duration.
func (l Limits) StreamMaxDuration() time.Duration {
	return time.Duration(l.StreamMaxDurationMS) * time.Millisecond
}

// Validate rejects limits that cannot be honoured, so a client fails at connect
// time with an actionable message instead of misbehaving later.
func (l Limits) Validate() error {
	if l.MaxSendBytes <= 0 {
		return fmt.Errorf("pollmux: server sent non-positive max_send_bytes=%d", l.MaxSendBytes)
	}
	if l.PollTimeoutMS <= 0 {
		return fmt.Errorf("pollmux: server sent non-positive poll_timeout_ms=%d", l.PollTimeoutMS)
	}
	if l.SessionTimeoutMS <= 0 {
		return fmt.Errorf("pollmux: server sent non-positive session_timeout_ms=%d", l.SessionTimeoutMS)
	}
	return nil
}

// ConnectResponse is the JSON body of a successful connect.
type ConnectResponse struct {
	ProtocolVersion int    `json:"protocol_version"`
	SessionID       string `json:"session_id"`
	Limits          Limits `json:"limits"`
	// Meta is produced by Hooks.Authenticate and merged with what the client
	// declared. Opaque to pollmux.
	Meta map[string]string `json:"meta,omitempty"`
	// PollMode is the mode the server actually negotiated for this session:
	// PollModeBatch or PollModeStream. Empty (old server, or a server that
	// doesn't know this field) means batch — new clients must treat an
	// empty/unrecognized value as batch, never as an error.
	PollMode string `json:"poll_mode,omitempty"`
	// UploadStreamMode is PollModeStream when the server also negotiated
	// streamed uploads for this session (see ConnectRequest.
	// PreferStreamUpload), empty otherwise — including when talking to an
	// older server that predates upload streaming and has no idea what this
	// field means. A client must treat empty the same as PollModeBatch: fall
	// back to the discrete writeBuf/flushLoop send path.
	UploadStreamMode string `json:"upload_stream_mode,omitempty"`
	// Transport is TransportWebSocket when the server negotiated a WebSocket
	// attachment for this session (see ConnectRequest.PreferWebSocket), empty
	// otherwise — including when talking to an older server that predates
	// WebSocket support. A client must treat empty as "use PollMode/
	// UploadStreamMode as negotiated above", never as an error: this field is
	// purely additive over the existing negotiation.
	Transport string `json:"transport,omitempty"`
}

// errorResponse is the JSON body of any non-2xx answer from the handlers.
type errorResponse struct {
	Error string `json:"error"`
}

// DisconnectReason says why a session ended. It is passed to Hooks.OnDisconnect
// so the application can tell an orderly client departure from an eviction.
type DisconnectReason int

const (
	// ReasonClientDelete: the client sent DELETE {prefix}/{id}.
	ReasonClientDelete DisconnectReason = iota
	// ReasonEvicted: the sweeper found the session idle past session_timeout
	// with no poll in flight.
	ReasonEvicted
	// ReasonServerClose: the application closed the session itself, typically
	// during graceful shutdown.
	ReasonServerClose
	// ReasonProtocolViolation: the client broke the protocol (e.g. sent a body
	// larger than the max_send_bytes it was handed at connect time).
	ReasonProtocolViolation
)

func (r DisconnectReason) String() string {
	switch r {
	case ReasonClientDelete:
		return "client_delete"
	case ReasonEvicted:
		return "evicted"
	case ReasonServerClose:
		return "server_close"
	case ReasonProtocolViolation:
		return "protocol_violation"
	default:
		return fmt.Sprintf("unknown(%d)", int(r))
	}
}
