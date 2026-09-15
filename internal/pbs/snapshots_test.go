package pbs

import (
	"context"
	"fmt"
	"net/http"
	"testing"
)

// Coverage 6: {"data":...} envelope unwraps into []Snapshot; absent size -> 0.
func TestListSnapshots(t *testing.T) {
	var gotPath, gotQuery string
	c := newFake(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery = r.URL.Path, r.URL.RawQuery
		fmt.Fprint(w, `{"data":[
			{"backup-type":"ct","backup-id":"100","backup-time":1750000000.5,"size":4096},
			{"backup-type":"vm","backup-id":"101","backup-time":1750000100.25}
		]}`)
	})

	snaps, err := c.ListSnapshots(context.Background(), "k8s-test", "test-ns")
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if want := "/api2/json/admin/datastore/k8s-test/snapshots"; gotPath != want {
		t.Errorf("path = %q, want %q", gotPath, want)
	}
	if want := "ns=test-ns"; gotQuery != want {
		t.Errorf("query = %q, want %q", gotQuery, want)
	}
	if len(snaps) != 2 {
		t.Fatalf("got %d snapshots, want 2: %+v", len(snaps), snaps)
	}
	s0, s1 := snaps[0], snaps[1]
	if s0.BackupType != "ct" || s0.BackupID != "100" || s0.BackupTime != 1750000000.5 || s0.Size != 4096 {
		t.Errorf("snaps[0] = %+v", s0)
	}
	if s1.BackupType != "vm" || s1.BackupID != "101" || s1.BackupTime != 1750000100.25 {
		t.Errorf("snaps[1] = %+v", s1)
	}
	if s1.Size != 0 {
		t.Errorf("snaps[1].Size = %d, want 0 when absent", s1.Size)
	}
}

func TestListSnapshotsHTTPError(t *testing.T) {
	c := newFake(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"message":"datastore 'k8s-test' does not exist"}`, http.StatusNotFound)
	})
	if _, err := c.ListSnapshots(context.Background(), "k8s-test", "test-ns"); err == nil {
		t.Fatal("ListSnapshots on 404: want error, got nil")
	}
}
