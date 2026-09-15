package pbs

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// Snapshot is one backup record as returned by the datastore snapshots list.
type Snapshot struct {
	BackupType string  `json:"backup-type"`
	BackupID   string  `json:"backup-id"`
	BackupTime float64 `json:"backup-time"`
	Size       int64   `json:"size"` // may be absent -> 0
}

// ListSnapshots lists the snapshots in namespace ns of the given datastore.
func (c *Client) ListSnapshots(ctx context.Context, store, ns string) ([]Snapshot, error) {
	path := "/api2/json/admin/datastore/" + url.PathEscape(store) + "/snapshots?" + url.Values{"ns": {ns}}.Encode()
	var snaps []Snapshot
	if err := c.do(ctx, http.MethodGet, path, nil, &snaps); err != nil {
		return nil, fmt.Errorf("pbs: list snapshots in %q ns %q: %w", store, ns, err)
	}
	return snaps, nil
}
