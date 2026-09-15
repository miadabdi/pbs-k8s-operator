// Package pbstest provides a fake Proxmox Backup Server API backed by an
// httptest TLS server, for controller envtests. It is a normal (non-_test)
// package so controller tests can import it; it is not used by production code.
package pbstest

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
)

// Server is a fake PBS. Snapshots (ping) can be configured, namespace-ensure
// POSTs are recorded with their Authorization header and can be made to fail.
type Server struct {
	srv *httptest.Server

	mu        sync.Mutex
	snapshots any // JSON value returned as "data" by the snapshots list
	nsStatus  int // 0 => 200; otherwise the HTTP status namespace POSTs return
	nsAuths   []string
}

// Start launches the fake PBS. Register s.Close with the test framework's
// cleanup (t.Cleanup(srv.Close) in plain tests, DeferCleanup(srv.Close) in ginkgo).
func Start() *Server {
	s := &Server{snapshots: []any{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/api2/json/admin/datastore/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/snapshots"):
			s.mu.Lock()
			data := s.snapshots
			s.mu.Unlock()
			writeJSON(w, http.StatusOK, map[string]any{"data": data})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/namespace"):
			s.mu.Lock()
			s.nsAuths = append(s.nsAuths, r.Header.Get("Authorization"))
			status := s.nsStatus
			s.mu.Unlock()
			if status == 0 {
				writeJSON(w, http.StatusOK, map[string]any{"data": nil})
				return
			}
			body := "boom"
			if status == http.StatusBadRequest {
				body = "namespace already exists"
			}
			http.Error(w, body, status)
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	})
	s.srv = httptest.NewUnstartedServer(mux)
	s.srv.Config.ErrorLog = log.New(io.Discard, "", 0) // silence aborted handshakes (fingerprint-mismatch tests)
	s.srv.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	s.srv.StartTLS()
	return s
}

// Close shuts the fake PBS down.
func (s *Server) Close() { s.srv.Close() }

// Fingerprint returns the PBS-form (colon-hex uppercase sha256) fingerprint of
// the server's certificate — the value a CR/secret must pin to reach it.
func (s *Server) Fingerprint() string {
	sum := sha256.Sum256(s.srv.Certificate().Raw)
	hexed := strings.ToUpper(hex.EncodeToString(sum[:]))
	parts := make([]string, len(hexed)/2)
	for i := range parts {
		parts[i] = hexed[2*i : 2*i+2]
	}
	return strings.Join(parts, ":")
}

// Host returns the server hostname (for spec.host).
func (s *Server) Host() string {
	host, _, _ := net.SplitHostPort(s.addr())
	return host
}

// Port returns the server port (for spec.port).
func (s *Server) Port() int {
	_, port, _ := net.SplitHostPort(s.addr())
	p, _ := strconv.Atoi(port)
	return p
}

// SetSnapshots sets the snapshot list returned by GET .../snapshots (the ping).
func (s *Server) SetSnapshots(snaps any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshots = snaps
}

// SetNamespaceStatus switches POST .../namespace away from 200: 400 is the
// PBS "already exists" case; any other code is a hard failure.
func (s *Server) SetNamespaceStatus(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nsStatus = code
}

// NamespaceAuthHeaders returns the Authorization headers of every namespace
// POST received, in order — assert which token (e.g. bootstrap vs producer)
// actually created the namespace.
func (s *Server) NamespaceAuthHeaders() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.nsAuths...)
}

func (s *Server) addr() string { return s.srv.Listener.Addr().String() }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
