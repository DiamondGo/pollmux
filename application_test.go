package pollmux

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// This file pins down the API shape an application needs from pollmux. Nothing
// here is a pollmux feature; it is a compile-and-run check that the seven
// things a real integration depends on are all possible. If one of these stops
// compiling, an application built on this library has been broken.

// brokerSession is what an application defines to get its type safety back,
// since pollmux.Session deliberately carries no application semantics.
type brokerSession struct {
	*Session
	Role     string
	Endpoint string
}

// endpointRegistry stands in for an application's own registry: the role model
// is application logic, not transport, so it stays outside the library.
type endpointRegistry struct {
	mu       sync.Mutex
	byID     map[string]*brokerSession
	byRole   map[string][]string
	removals []string
}

func newEndpointRegistry() *endpointRegistry {
	return &endpointRegistry{
		byID:   make(map[string]*brokerSession),
		byRole: make(map[string][]string),
	}
}

func (r *endpointRegistry) register(bs *brokerSession) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[bs.ID] = bs
	r.byRole[bs.Role] = append(r.byRole[bs.Role], bs.ID)
}

func (r *endpointRegistry) removeBySessionID(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.byID, id)
	r.removals = append(r.removals, id)
}

func (r *endpointRegistry) get(id string) (*brokerSession, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	bs, ok := r.byID[id]
	return bs, ok
}

func (r *endpointRegistry) removalCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.removals)
}

// authMiddleware proves the handlers are ordinary http.Handlers that an
// application can wrap with its own middleware.
func authMiddleware(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// TestApplicationWiringShape wires pollmux the way a real integration does, and
// exercises every contract item that integration depends on.
func TestApplicationWiringShape(t *testing.T) {
	const token = "secret-token"

	registry := newEndpointRegistry()
	store := NewSessionStore()

	var mu sync.Mutex
	var yamuxStarted []string
	var disconnects []DisconnectReason

	cfg := ServerConfig{
		PollTimeout:    300 * time.Millisecond,
		SessionTimeout: 600 * time.Millisecond,
		SweepInterval:  50 * time.Millisecond,
		PollBufferSize: 64 << 10,
		MaxSendBytes:   64 << 10,
		// (6) A router that does not use PathValue — a gorilla/mux application
		// passes func(r) string { return mux.Vars(r)["id"] } here.
		SessionIDFunc: func(r *http.Request) string {
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			if len(parts) < 2 {
				return ""
			}
			return parts[1]
		},
	}

	hooks := Hooks{
		// (3) The three-role check that used to live in handleConnect, now a
		// hook that can still answer 400 rather than a flattened 401.
		Authenticate: func(_ *http.Request, req ConnectRequest) (map[string]string, error) {
			role, endpoint := req.Meta["role"], req.Meta["endpoint"]
			if role != "consumer" && role != "provider" {
				return nil, StatusErrorf(http.StatusBadRequest, "role must be 'consumer' or 'provider'")
			}
			if endpoint == "" {
				return nil, StatusErrorf(http.StatusBadRequest, "missing endpoint")
			}
			return nil, nil
		},

		// (4) Starting a relay goroutine here is safe: the session is already
		// in the store, so a poll that beats this callback still finds it.
		OnConnect: func(s *Session, meta map[string]string) error {
			bs := &brokerSession{Session: s, Role: meta["role"], Endpoint: meta["endpoint"]}
			registry.register(bs)

			go func() {
				// (1) *Session goes straight to yamux as an io.ReadWriteCloser.
				// Providers get a client session, consumers a server one, just
				// as relay.go does today.
				var err error
				if bs.Role == "provider" {
					_, err = ClientSession(bs.Session)
				} else {
					_, err = ServerSession(bs.Session)
				}
				if err != nil {
					return
				}
				mu.Lock()
				yamuxStarted = append(yamuxStarted, bs.ID)
				mu.Unlock()
			}()
			return nil
		},

		// (5) One place to clean the application registry, whatever ended the
		// session.
		OnDisconnect: func(s *Session, reason DisconnectReason) {
			registry.removeBySessionID(s.ID)
			mu.Lock()
			disconnects = append(disconnects, reason)
			mu.Unlock()
		},
	}

	// (7) Bare http.Handlers, wrappable by the existing middleware.
	mux := http.NewServeMux()
	mux.Handle("/tunnel/connect", authMiddleware(token, ConnectHandler(store, cfg, hooks)))
	mux.Handle("/tunnel/{id}/poll", authMiddleware(token, PollHandler(store, cfg, hooks)))
	mux.Handle("/tunnel/{id}", authMiddleware(token, DeleteHandler(store, cfg, hooks)))

	ts := httptest.NewServer(mux)
	defer ts.Close()

	// --- (3) a bad role is rejected with the status the hook chose ----------
	resp := brokerConnect(t, ts, token, map[string]string{"role": "nonsense", "endpoint": "home"})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad role got status %d, want the hook's 400", resp.StatusCode)
	}
	resp.Body.Close()

	// --- middleware still guards everything --------------------------------
	resp = brokerConnect(t, ts, "wrong-token", map[string]string{"role": "provider", "endpoint": "home"})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad token got status %d, want 401 from the middleware", resp.StatusCode)
	}
	resp.Body.Close()

	// --- a real provider connects ------------------------------------------
	conn := brokerDial(t, ts, token, map[string]string{"role": "provider", "endpoint": "home"})
	defer conn.Close()

	// (2) The embedded session keeps its exported ID, and the application's own
	// fields survive alongside it.
	bs, ok := registry.get(conn.SessionID())
	if !ok {
		t.Fatal("OnConnect did not file the session in the application registry")
	}
	if bs.Role != "provider" || bs.Endpoint != "home" {
		t.Fatalf("brokerSession = {%q, %q}, want {provider, home}", bs.Role, bs.Endpoint)
	}
	if bs.ID != conn.SessionID() {
		t.Fatalf("embedded Session.ID = %q, want %q", bs.ID, conn.SessionID())
	}

	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(yamuxStarted) == 1
	})

	// (6) The custom id extractor is what routed that poll.
	waitFor(t, 3*time.Second, func() bool {
		s, ok := store.Get(conn.SessionID())
		return ok && s.PollInFlight() > 0
	})

	// --- (5) path one: the client leaves cleanly ---------------------------
	conn.Close()
	waitFor(t, 3*time.Second, func() bool { return registry.removalCount() == 1 })

	mu.Lock()
	if len(disconnects) != 1 || disconnects[0] != ReasonClientDelete {
		mu.Unlock()
		t.Fatalf("disconnect reasons = %v, want [client_delete]", disconnects)
	}
	mu.Unlock()

	// --- (5) path two: the sweeper evicts an abandoned session --------------
	stop := StartSweeper(store, cfg, hooks)
	defer stop()

	abandoned := brokerDial(t, ts, token, map[string]string{"role": "consumer", "endpoint": "home"})
	waitFor(t, 3*time.Second, func() bool { return store.Len() == 1 })
	abandoned.(*httpConn).cancel() // stop polling without telling the server

	waitFor(t, 10*time.Second, func() bool { return registry.removalCount() == 2 })

	mu.Lock()
	if len(disconnects) != 2 || disconnects[1] != ReasonEvicted {
		mu.Unlock()
		t.Fatalf("disconnect reasons = %v, want [client_delete evicted]", disconnects)
	}
	mu.Unlock()

	// --- (5) path three: the application closes it during shutdown ----------
	shutdown := brokerDial(t, ts, token, map[string]string{"role": "consumer", "endpoint": "home"})
	defer shutdown.Close()
	waitFor(t, 3*time.Second, func() bool { return store.Len() == 1 })

	for _, s := range store.All() {
		CloseSession(store, hooks, s, ReasonServerClose)
	}
	waitFor(t, 3*time.Second, func() bool { return registry.removalCount() == 3 })

	mu.Lock()
	defer mu.Unlock()
	if len(disconnects) != 3 || disconnects[2] != ReasonServerClose {
		t.Fatalf("disconnect reasons = %v, want [client_delete evicted server_close]", disconnects)
	}
}

// The compile-time half of the contract: these assertions fail to build rather
// than fail at runtime if the shapes drift.
var (
	_ io.ReadWriteCloser = (*Session)(nil)
	_ io.ReadWriteCloser = (*brokerSession)(nil) // embedding still satisfies it
	_ http.Handler       = ConnectHandler(nil, ServerConfig{}, Hooks{})
	_ http.Handler       = PollHandler(nil, ServerConfig{}, Hooks{})
	_ http.Handler       = DeleteHandler(nil, ServerConfig{}, Hooks{})
)

func brokerConnect(t *testing.T, ts *httptest.Server, token string, meta map[string]string) *http.Response {
	t.Helper()
	body, _ := json.Marshal(ConnectRequest{ProtocolVersion: ProtocolVersion, Meta: meta})
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/tunnel/connect", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("build connect request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("connect request: %v", err)
	}
	return resp
}

func brokerDial(t *testing.T, ts *httptest.Server, token string, meta map[string]string) Conn {
	t.Helper()
	c := &Connector{
		BaseURL:   ts.URL,
		AuthToken: token,
		Meta:      meta,
		PollGrace: 2 * time.Second,
	}
	conn, err := c.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect as %v: %v", meta, err)
	}
	return conn
}

// Worth knowing when integrating: pollmux keeps its own session index and the
// application keeps another, related by Session.ID. Whether an application
// keeps its own index at all is its decision — the library does not force
// either way, it only guarantees the two stay consistent through the hooks.
func TestStoreAndApplicationRegistryStayInSync(t *testing.T) {
	registry := newEndpointRegistry()
	cfg := testServerConfig()
	hooks := Hooks{
		OnConnect: func(s *Session, meta map[string]string) error {
			registry.register(&brokerSession{Session: s, Role: meta["role"], Endpoint: meta["endpoint"]})
			return nil
		},
		OnDisconnect: func(s *Session, _ DisconnectReason) {
			registry.removeBySessionID(s.ID)
		},
	}

	ts, store := newTestServer(t, cfg, hooks)

	var ids []string
	for i := range 3 {
		cr := connectOKWithMeta(t, ts, map[string]string{
			"role":     "provider",
			"endpoint": fmt.Sprintf("ep-%d", i),
		})
		ids = append(ids, cr.SessionID)
	}

	if store.Len() != 3 {
		t.Fatalf("store holds %d sessions, want 3", store.Len())
	}
	for _, id := range ids {
		if _, ok := registry.get(id); !ok {
			t.Fatalf("session %s is in the store but not in the application registry", id)
		}
	}

	for _, s := range store.All() {
		CloseSession(store, hooks, s, ReasonServerClose)
	}

	if store.Len() != 0 {
		t.Fatalf("store still holds %d sessions", store.Len())
	}
	if registry.removalCount() != 3 {
		t.Fatalf("application registry saw %d removals, want 3", registry.removalCount())
	}
}

func connectOKWithMeta(t *testing.T, ts *httptest.Server, meta map[string]string) ConnectResponse {
	t.Helper()
	resp, cr := postConnect(t, ts, ConnectRequest{ProtocolVersion: ProtocolVersion, Meta: meta})
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("connect status = %d, want 200", resp.StatusCode)
	}
	return cr
}
