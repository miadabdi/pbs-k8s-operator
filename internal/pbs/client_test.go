package pbs

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

const (
	testTokenID     = "operator@pbs!producer"
	testTokenSecret = "01234567-abcd"
	testAuthHeader  = "PBSAPIToken=" + testTokenID + ":" + testTokenSecret
)

// newFake starts an httptest TLS server as a fake PBS and returns a Client
// configured with the server's address and the real fingerprint of its cert.
func newFake(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	srv := newTLSServer(h)
	t.Cleanup(srv.Close)
	return newClientForServer(t, srv, certFingerprint(t, srv))
}

// newTLSServer is httptest.NewTLSServer with its error log silenced — the
// fingerprint-mismatch test intentionally aborts handshakes, and the server
// would log each one, polluting test output.
func newTLSServer(h http.HandlerFunc) *httptest.Server {
	srv := httptest.NewUnstartedServer(h)
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.StartTLS()
	return srv
}

func newClientForServer(t *testing.T, srv *httptest.Server, fingerprint string) *Client {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse fake server URL: %v", err)
	}
	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatalf("split fake server host: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse fake server port: %v", err)
	}
	return NewClient(host, port, testTokenID, testTokenSecret, fingerprint)
}

func certFingerprint(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	cert := srv.Certificate()
	if cert == nil {
		t.Fatal("fake server has no certificate")
	}
	sum := sha256.Sum256(cert.Raw)
	return colonHex(sum[:])
}

// Coverage 1: the PBSAPIToken auth header is sent verbatim (tokenID contains
// '@' and '!' and must not be encoded).
func TestAuthHeaderSent(t *testing.T) {
	var gotAuth string
	c := newFake(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		fmt.Fprint(w, `{"data":[]}`)
	})

	if _, err := c.ListSnapshots(context.Background(), "k8s-test", "test-ns"); err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if gotAuth != testAuthHeader {
		t.Errorf("Authorization header = %q, want %q", gotAuth, testAuthHeader)
	}
}

// Coverage 2: certificate fingerprint verification.
func TestFingerprintVerification(t *testing.T) {
	respond := func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"data":[]}`)
	}

	t.Run("match succeeds", func(t *testing.T) {
		c := newFake(t, respond)
		if _, err := c.ListSnapshots(context.Background(), "k8s-test", "test-ns"); err != nil {
			t.Errorf("ListSnapshots with matching fingerprint: %v", err)
		}
	})

	t.Run("mismatch errors naming both fingerprints", func(t *testing.T) {
		srv := newTLSServer(respond)
		t.Cleanup(srv.Close)

		// 32 bytes of 0xAB in PBS colon-hex uppercase form.
		wrong := strings.Repeat("AB:", 31) + "AB"

		c := newClientForServer(t, srv, wrong)
		_, err := c.ListSnapshots(context.Background(), "k8s-test", "test-ns")
		if err == nil {
			t.Fatal("ListSnapshots with wrong fingerprint: want error, got nil")
		}
		real := certFingerprint(t, srv)
		if !strings.Contains(err.Error(), wrong) {
			t.Errorf("error %q does not name configured fingerprint %q", err, wrong)
		}
		if !strings.Contains(err.Error(), real) {
			t.Errorf("error %q does not name server cert fingerprint %q", err, real)
		}
	})
}
