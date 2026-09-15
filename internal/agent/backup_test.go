package agent

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// fakeRunner scripts the proxmox-backup-client exec seam: one call per entry.
type fakeRunner struct {
	calls []struct {
		err    error
		stdout string
		stderr string
	}
	argv [][]string
	env  [][]string
}

func (f *fakeRunner) run(argv, env []string, stdout, stderr io.Writer) error {
	i := len(f.argv)
	if i >= len(f.calls) {
		return fmt.Errorf("fakeRunner: unexpected invocation %d: %v", i+1, argv)
	}
	f.argv = append(f.argv, argv)
	f.env = append(f.env, env)
	c := f.calls[i]
	if c.stdout != "" {
		fmt.Fprint(stdout, c.stdout)
	}
	if c.stderr != "" {
		fmt.Fprint(stderr, c.stderr)
	}
	return c.err
}

func testEnv(without ...string) func(string) string {
	m := map[string]string{
		"PBS_HOST":         "pbs.example.com",
		"PBS_PORT":         "8007",
		"PBS_DATASTORE":    "main",
		"PBS_NS":           "tenant1",
		"PBS_TOKEN_ID":     "operator@pbs!producer",
		"PBS_TOKEN_SECRET": "s3cret-token",
		"PBS_FINGERPRINT":  "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99",
		"PBS_KEYFILE":      `{"kdf":"none","data":"..."}`,
	}
	for _, k := range without {
		delete(m, k)
	}
	return func(k string) string { return m[k] }
}

func validConfig() config {
	c, missing := loadConfig(testEnv())
	if len(missing) != 0 {
		panic("testEnv incomplete: " + strings.Join(missing, ","))
	}
	return c
}

func deps(run func(argv, env []string, stdout, stderr io.Writer) error) BackupDeps {
	return BackupDeps{
		Getenv:   testEnv(),
		Hostname: func() (string, error) { return "bk-1", nil },
		Environ:  func() []string { return []string{"PATH=/usr/bin"} },
		Stdout:   &strings.Builder{},
		Stderr:   &strings.Builder{},
		Run:      run,
	}
}

// Snapshot-list fixture: two host/bk-1 snapshots (older first), one decoy id,
// one decoy type. Latest host/bk-1 is 1747401600 size 4096.
const snapshotListFixture = `{"data":[
  {"backup-type":"host","backup-id":"bk-1","backup-time":1747315200.0,"size":1234},
  {"backup-type":"host","backup-id":"bk-1","backup-time":1747401600.0,"size":4096},
  {"backup-type":"host","backup-id":"other","backup-time":1747488000.0,"size":99},
  {"backup-type":"ct","backup-id":"bk-1","backup-time":1750000000.0,"size":7}
]}`

// Bullet 1: env -> PBS_REPOSITORY composition (tokenID's @ and ! stay literal).
func TestClientEnv(t *testing.T) {
	got := clientEnv(validConfig())
	want := []string{
		"PBS_REPOSITORY=operator@pbs!producer@pbs.example.com:8007:main",
		"PBS_PASSWORD=s3cret-token",
		"PBS_FINGERPRINT=AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("clientEnv() =\n%q\nwant\n%q", got, want)
	}
}

// Bullet 2: client argv construction — --ns, --keyfile, ordered pxar pairs.
func TestBackupArgv(t *testing.T) {
	got := backupArgv("tenant1", "/tmp/kf.json", []string{"data-a", "zdata"})
	want := []string{
		"proxmox-backup-client", "backup",
		"--ns", "tenant1",
		"--keyfile", "/tmp/kf.json",
		"pvc-data-a.pxar:/backup/data-a",
		"pvc-zdata.pxar:/backup/zdata",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("backupArgv() = %q\nwant %q", got, want)
	}
}

func TestListArgv(t *testing.T) {
	got := listArgv("tenant1")
	want := []string{
		"proxmox-backup-client", "snapshot", "list",
		"--ns", "tenant1",
		"--output-format", "json",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("listArgv() = %q, want %q", got, want)
	}
}

// Bullet 3: snapshot-pick from fixture snapshot-list JSON -> correct ref + bytes.
func TestPickSnapshot(t *testing.T) {
	snaps, err := parseSnapshotList([]byte(snapshotListFixture))
	if err != nil {
		t.Fatalf("parseSnapshotList: %v", err)
	}
	if len(snaps) != 4 {
		t.Fatalf("parsed %d snapshots, want 4", len(snaps))
	}
	snap, err := latestHostSnapshot(snaps, "bk-1")
	if err != nil {
		t.Fatalf("latestHostSnapshot: %v", err)
	}
	if snap.BackupTime != 1747401600.0 || snap.Size != 4096 {
		t.Errorf("picked %+v, want backup-time 1747401600 size 4096", snap)
	}
	ref := snapshotRef(snap)
	if ref != "host/bk-1/2025-05-16T13:20:00Z" {
		t.Errorf("ref = %q, want host/bk-1/2025-05-16T13:20:00Z", ref)
	}
	if got := successJSON(snap); got != `{"snapshotRef":"host/bk-1/2025-05-16T13:20:00Z","bytes":4096}` {
		t.Errorf("successJSON = %s", got)
	}
}

// Bullets 4+5: full success flow through the exec seam.
func TestRunBackupSuccess(t *testing.T) {
	fr := &fakeRunner{calls: []struct {
		err    error
		stdout string
		stderr string
	}{
		{},                            // backup
		{stdout: snapshotListFixture}, // snapshot list
	}}
	d := deps(fr.run)
	termlog := filepath.Join(t.TempDir(), "termlog.json")

	code := RunBackup(d, []string{"data-a", "zdata"}, termlog)
	if code != 0 {
		t.Fatalf("RunBackup exit = %d, want 0", code)
	}
	// argv[0][5] is the --keyfile value (temp path, different each run).
	wantBackup := backupArgv("tenant1", fr.argv[0][5], []string{"data-a", "zdata"})
	if !reflect.DeepEqual(fr.argv[0], wantBackup) {
		t.Errorf("backup argv = %q\nwant %q", fr.argv[0], wantBackup)
	}
	if !reflect.DeepEqual(fr.argv[1], listArgv("tenant1")) {
		t.Errorf("list argv = %q\nwant %q", fr.argv[1], listArgv("tenant1"))
	}
	wantEnv := append([]string{"PATH=/usr/bin"}, clientEnv(validConfig())...)
	if !reflect.DeepEqual(fr.env[0], wantEnv) || !reflect.DeepEqual(fr.env[1], wantEnv) {
		t.Errorf("child env = %q\nwant %q", fr.env[0], wantEnv)
	}
	// TERMLOG override honored: the flow writes the contract JSON to the path
	// it was given, exact shape.
	b, err := os.ReadFile(termlog)
	if err != nil {
		t.Fatalf("read termlog: %v", err)
	}
	if got := string(b); got != `{"snapshotRef":"host/bk-1/2025-05-16T13:20:00Z","bytes":4096}`+"\n" {
		t.Errorf("termlog = %q", got)
	}
	if out := d.Stdout.(*strings.Builder).String(); !strings.Contains(out, `"snapshotRef":"host/bk-1/2025-05-16T13:20:00Z"`) {
		t.Errorf("stdout missing result JSON: %q", out)
	}
}

// Bullet 4: client failure -> error JSON (first stderr line), full stderr on
// our stderr, exit code fallback.
func TestRunBackupClientFailure(t *testing.T) {
	fr := &fakeRunner{calls: []struct {
		err    error
		stdout string
		stderr string
	}{
		{err: errors.New("exit status 3"), stderr: "Error: connection refused\n proxmox daemon unreachable\n"},
	}}
	d := deps(fr.run)
	termlog := filepath.Join(t.TempDir(), "termlog.json")

	code := RunBackup(d, []string{"data-a"}, termlog)
	if code != 1 {
		t.Fatalf("RunBackup exit = %d, want 1 (fallback)", code)
	}
	b, err := os.ReadFile(termlog)
	if err != nil {
		t.Fatalf("read termlog: %v", err)
	}
	if got := string(b); got != `{"error":"Error: connection refused"}`+"\n" {
		t.Errorf("termlog = %q", got)
	}
	if se := d.Stderr.(*strings.Builder).String(); !strings.Contains(se, "proxmox daemon unreachable") {
		t.Errorf("full stderr not streamed: %q", se)
	}
}

// Bullet 5: missing env -> exit 2 + error JSON, client never invoked.
func TestRunBackupMissingEnv(t *testing.T) {
	fr := &fakeRunner{}
	d := deps(fr.run)
	d.Getenv = testEnv("PBS_TOKEN_SECRET", "PBS_KEYFILE")
	termlog := filepath.Join(t.TempDir(), "termlog.json")

	code := RunBackup(d, []string{"data-a"}, termlog)
	if code != 2 {
		t.Fatalf("RunBackup exit = %d, want 2", code)
	}
	b, err := os.ReadFile(termlog)
	if err != nil {
		t.Fatalf("read termlog: %v", err)
	}
	if got := string(b); got != `{"error":"missing required env: PBS_TOKEN_SECRET, PBS_KEYFILE"}`+"\n" {
		t.Errorf("termlog = %q", got)
	}
	if len(fr.argv) != 0 {
		t.Errorf("client invoked %d times, want 0", len(fr.argv))
	}
}

func TestMaterializeKeyfile(t *testing.T) {
	path, cleanup, err := materializeKeyfile(`{"kdf":"none"}`)
	if err != nil {
		t.Fatalf("materializeKeyfile: %v", err)
	}
	defer cleanup()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read keyfile: %v", err)
	}
	if string(b) != `{"kdf":"none"}` {
		t.Errorf("keyfile content = %q", b)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("keyfile mode = %v, want 0600", fi.Mode().Perm())
	}
	cleanup()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("keyfile still present after cleanup: %v", err)
	}
}

func TestParseBackupArgs(t *testing.T) {
	t.Run("pvc order preserved, TERMLOG default", func(t *testing.T) {
		pvcs, termlog, err := ParseBackupArgs([]string{"--pvc", "b", "--pvc", "a"}, func(string) string { return "" })
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(pvcs, []string{"b", "a"}) {
			t.Errorf("pvcs = %q", pvcs)
		}
		if termlog != "/dev/termination-log" {
			t.Errorf("termlog = %q, want default", termlog)
		}
	})
	t.Run("TERMLOG env override honored", func(t *testing.T) {
		_, termlog, err := ParseBackupArgs([]string{"--pvc", "a"}, func(k string) string {
			if k == "TERMLOG" {
				return "/tmp/custom-termlog"
			}
			return ""
		})
		if err != nil {
			t.Fatal(err)
		}
		if termlog != "/tmp/custom-termlog" {
			t.Errorf("termlog = %q, want /tmp/custom-termlog", termlog)
		}
	})
	t.Run("--termlog flag beats env", func(t *testing.T) {
		_, termlog, err := ParseBackupArgs([]string{"--pvc", "a", "--termlog", "/x"}, func(k string) string {
			if k == "TERMLOG" {
				return "/from-env"
			}
			return ""
		})
		if err != nil {
			t.Fatal(err)
		}
		if termlog != "/x" {
			t.Errorf("termlog = %q, want /x", termlog)
		}
	})
	t.Run("no --pvc is an error", func(t *testing.T) {
		if _, _, err := ParseBackupArgs(nil, func(string) string { return "" }); err == nil {
			t.Error("expected error for missing --pvc")
		}
	})
}
