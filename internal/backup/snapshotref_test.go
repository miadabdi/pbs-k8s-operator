package backup

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"gitlab.sharifmind.ir/miad/pbs-operator/internal/pbs"
)

func TestComposeSnapshotRef(t *testing.T) {
	tests := []struct {
		name string
		typ  string
		id   string
		bt   float64
	}{
		{"whole epoch", "ct", "100", 1747315200.0},
		{"sub-second truncated", "vm", "101", 1747315200.75},
		{"another epoch", "host", "pg-backup", 1750000100.25},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ComposeSnapshotRef(tc.typ, tc.id, tc.bt)
			// Expected reference computed in-test from the same rules; no magic dates.
			want := fmt.Sprintf("%s/%s/%s", tc.typ, tc.id,
				time.Unix(int64(tc.bt), 0).UTC().Format(time.RFC3339))
			if got != want {
				t.Errorf("ComposeSnapshotRef(%q,%q,%v) = %q, want %q", tc.typ, tc.id, tc.bt, got, want)
			}
			if !strings.HasSuffix(got, "Z") {
				t.Errorf("ref %q not UTC (Z-suffixed)", got)
			}
		})
	}
	if ComposeSnapshotRef("ct", "100", 1747315200.75) != ComposeSnapshotRef("ct", "100", 1747315200.0) {
		t.Error("sub-second epoch must truncate to the same reference as the whole second")
	}
}

func TestLatestSnapshot(t *testing.T) {
	snaps := []pbs.Snapshot{
		{BackupType: "ct", BackupID: "100", BackupTime: 1750000000.5},
		{BackupType: "ct", BackupID: "100", BackupTime: 1750000900.0}, // max of ct/100
		{BackupType: "vm", BackupID: "101", BackupTime: 1750009999.0}, // later but other group
		{BackupType: "ct", BackupID: "100", BackupTime: 1749000000.0},
		{BackupType: "ct", BackupID: "200", BackupTime: 1750009999.0}, // later but other id
	}
	got, err := LatestSnapshot(snaps, "ct", "100")
	if err != nil {
		t.Fatalf("LatestSnapshot(ct/100): %v", err)
	}
	if got.BackupTime != 1750000900.0 {
		t.Errorf("LatestSnapshot(ct/100) BackupTime = %v, want 1750000900 (group max)", got.BackupTime)
	}

	got, err = LatestSnapshot(snaps, "vm", "101")
	if err != nil {
		t.Fatalf("LatestSnapshot(vm/101): %v", err)
	}
	if got.BackupID != "101" {
		t.Errorf("LatestSnapshot(vm/101) BackupID = %q, want 101 (group filtering)", got.BackupID)
	}

	if _, err := LatestSnapshot(snaps, "ct", "999"); err == nil {
		t.Error("LatestSnapshot on empty group: want error, got nil")
	}
}
