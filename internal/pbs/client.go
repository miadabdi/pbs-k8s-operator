// Package pbs is a thin client for the Proxmox Backup Server REST API,
// covering only the config/admin plane the operator needs: namespace
// ensure, snapshot listing, and async task polling.
package pbs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// defaultTimeout is the per-request timeout applied when the caller's ctx
// carries no deadline of its own.
const defaultTimeout = 30 * time.Second

// Client talks to a single PBS server. Construct with NewClient.
type Client struct {
	baseURL   string
	authToken string // full Authorization header value
	hc        *http.Client
}

// NewClient returns a client for https://host:port authenticated with the
// given API token. fingerprint is the sha256 of the server certificate in
// PBS colon-hex uppercase form; the TLS handshake fails unless it matches.
func NewClient(host string, port int, tokenID, tokenSecret, fingerprint string) *Client {
	return &Client{
		baseURL:   fmt.Sprintf("https://%s:%d", host, port),
		authToken: "PBSAPIToken=" + tokenID + ":" + tokenSecret,
		hc: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					// Self-signed certs cannot chain to a root CA; the
					// pinning check below is the actual verification.
					InsecureSkipVerify: true, //nolint:gosec
					VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
						return verifyFingerprint(rawCerts, fingerprint)
					},
				},
			},
		},
	}
}

// verifyFingerprint pins the leaf certificate to the configured sha256.
// This is the trust boundary: a mismatch is a hard error.
func verifyFingerprint(rawCerts [][]byte, want string) error {
	if len(rawCerts) == 0 {
		return errors.New("pbs: server presented no certificate")
	}
	sum := sha256.Sum256(rawCerts[0])
	got := colonHex(sum[:])
	if fingerprintKey(got) != fingerprintKey(want) {
		return fmt.Errorf("pbs: certificate fingerprint mismatch: server cert is %s, configured fingerprint is %s", got, want)
	}
	return nil
}

// colonHex renders b as PBS prints fingerprints: colon-hex UPPERCASE.
func colonHex(b []byte) string {
	hexed := strings.ToUpper(hex.EncodeToString(b))
	parts := make([]string, len(hexed)/2)
	for i := range parts {
		parts[i] = hexed[2*i : 2*i+2]
	}
	return strings.Join(parts, ":")
}

// fingerprintKey normalizes a fingerprint for comparison: colons, spaces,
// and case differences between PBS and user input are irrelevant.
func fingerprintKey(fp string) string {
	return strings.NewReplacer(":", "", " ", "").Replace(strings.ToUpper(fp))
}

// apiError is a non-2xx REST response.
type apiError struct {
	Status int
	Body   string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("pbs: HTTP %d: %s", e.Status, e.Body)
}

// do performs one REST call: sets the token header, applies the default
// per-request timeout, unwraps the {"data":...} envelope into out.
func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultTimeout)
		defer cancel()
	}

	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("pbs: encode request body: %w", err)
		}
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, rdr)
	if err != nil {
		return fmt.Errorf("pbs: build request: %w", err)
	}
	req.Header.Set("Authorization", c.authToken)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("pbs: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("pbs: read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return &apiError{Status: resp.StatusCode, Body: string(buf)}
	}
	if out == nil {
		return nil
	}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(buf, &envelope); err != nil {
		return fmt.Errorf("pbs: decode response envelope: %w", err)
	}
	if err := json.Unmarshal(envelope.Data, out); err != nil {
		return fmt.Errorf("pbs: decode response data: %w", err)
	}
	return nil
}
