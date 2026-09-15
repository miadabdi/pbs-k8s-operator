package backup

import (
	"fmt"
	"time"

	"gitlab.sharifmind.ir/miad/pbs-operator/internal/pbs"
)

// ComposeSnapshotRef renders the PBS snapshot reference used by restore:
// "{backup-type}/{backup-id}/{backup-time as ISO8601 UTC, seconds precision, Z}".
// backup-time arrives as epoch float (e.g. 1747315200.0); truncate sub-seconds.
func ComposeSnapshotRef(backupType, backupID string, backupTime float64) string {
	return fmt.Sprintf("%s/%s/%s", backupType, backupID,
		time.Unix(int64(backupTime), 0).UTC().Format(time.RFC3339))
}

// LatestSnapshot filters a snapshot list to one backup group (type+id) and returns
// the record with max backup-time. Empty group → error.
func LatestSnapshot(snaps []pbs.Snapshot, backupType, backupID string) (pbs.Snapshot, error) {
	var best pbs.Snapshot
	found := false
	for _, s := range snaps {
		if s.BackupType != backupType || s.BackupID != backupID {
			continue
		}
		if !found || s.BackupTime > best.BackupTime {
			best, found = s, true
		}
	}
	if !found {
		return pbs.Snapshot{}, fmt.Errorf("backup: no snapshots in group %s/%s", backupType, backupID)
	}
	return best, nil
}
