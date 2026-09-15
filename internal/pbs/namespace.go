package pbs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// EnsureNamespace creates namespace ns in the datastore if it does not
// exist yet. Namespace creation is idempotent server-side, so a 400
// "already exists" response is treated as success.
func (c *Client) EnsureNamespace(ctx context.Context, store, ns string) error {
	path := "/api2/json/admin/datastore/" + url.PathEscape(store) + "/namespace"
	err := c.do(ctx, http.MethodPost, path, map[string]string{"name": ns}, nil)
	var ae *apiError
	if errors.As(err, &ae) && ae.Status == http.StatusBadRequest && strings.Contains(strings.ToLower(ae.Body), "already exist") {
		return nil
	}
	if err != nil {
		return fmt.Errorf("pbs: ensure namespace %q in store %q: %w", ns, store, err)
	}
	return nil
}
