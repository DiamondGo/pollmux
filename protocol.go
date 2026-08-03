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
)

// Server-side defaults. See ServerConfig.
const (
	DefaultPollTimeout    = 30 * time.Second
	DefaultSessionTimeout = 2 * DefaultPollTimeout // A3: 60s, not 5min
	DefaultSweepInterval  = 5 * time.Second
	DefaultPollBufferSize = 256 << 10 // B1 first layer: was a hard-coded 64KB
	DefaultMaxSendBytes   = 1 << 20
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
)

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
}

// PollTimeout returns PollTimeoutMS as a duration.
func (l Limits) PollTimeout() time.Duration {
	return time.Duration(l.PollTimeoutMS) * time.Millisecond
}

// SessionTimeout returns SessionTimeoutMS as a duration.
func (l Limits) SessionTimeout() time.Duration {
	return time.Duration(l.SessionTimeoutMS) * time.Millisecond
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
