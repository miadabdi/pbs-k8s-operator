package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The restore argv contract, live-verified against the real client:
//   - --ns is REQUIRED on the subcommand (without it the client resolves the
//     ROOT namespace and fails "snapshot does not exist")
//   - --overwrite covers files only; directories still EEXIST — the agent
//     must empty the target first (RunRestoreVolume does, via emptyDir)
//   - --keyfile from the materialized PBS_KEYFILE temp file
func TestRestoreArgv(t *testing.T) {
	got := restoreArgv("tenant1", "/tmp/kf.json", "host/bk-1/2026-09-16T10:00:00Z", "pvc-data.pxar.didx", "/backup/data")
	want := []string{
		"proxmox-backup-client", "restore",
		"host/bk-1/2026-09-16T10:00:00Z", "pvc-data.pxar.didx", "/backup/data",
		"--ns", "tenant1",
		"--keyfile", "/tmp/kf.json",
		"--overwrite",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("restoreArgv() = %q\nwant %q", got, want)
	}
}

// emptyDir removes the CONTENTS of dir but never the dir itself (it is a
// volume mount point — removing it unmounts), including hidden entries.
func TestEmptyDir(t *testing.T) {
	dir := t.TempDir()
	for _, f := range []string{"probe.bin", ".hidden", "sub/deep/x"} {
		if err := os.MkdirAll(filepath.Join(dir, filepath.Dir(f)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, f), []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := emptyDir(dir); err != nil {
		t.Fatalf("emptyDir: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("dir still has %d entries: %v", len(entries), entries)
	}
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		t.Fatalf("dir itself must survive as a mount point: %v", err)
	}
	// Empty dir is fine (idempotent).
	if err := emptyDir(dir); err != nil {
		t.Fatalf("emptyDir on empty: %v", err)
	}
}

// ParseRestoreVolumeArgs: --ref required, --archive/--target repeatable pairs
// (counts must match), optional --configmap, termlog default.
func TestParseRestoreVolumeArgs(t *testing.T) {
	args := []string{
		"--ref", "host/bk/2026-09-16T10:00:00Z",
		"--archive", "pvc-a.pxar.didx", "--target", "/backup/a",
		"--archive", "pvc-b.pxar.didx", "--target", "/backup/b",
		"--configmap", "r-api",
	}
	ref, pairs, cm, termlog, err := ParseRestoreVolumeArgs(args, func(string) string { return "" })
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if ref != "host/bk/2026-09-16T10:00:00Z" || cm != "r-api" || termlog != "/dev/termination-log" {
		t.Fatalf("ref=%q cm=%q termlog=%q", ref, cm, termlog)
	}
	want := []RestorePair{{"pvc-a.pxar.didx", "/backup/a"}, {"pvc-b.pxar.didx", "/backup/b"}}
	if !reflect.DeepEqual(pairs, want) {
		t.Fatalf("pairs = %v, want %v", pairs, want)
	}
	for _, bad := range [][]string{
		{"--archive", "a.pxar.didx"},                            // no --ref
		{"--ref", "r"},                                          // no pairs
		{"--ref", "r", "--target", "/t"},                        // target without archive
		{"--ref", "r", "--archive", "a"},                        // archive without target
		{"--ref", "r", "--archive", "a", "--target", "/t", "x"}, // stray arg
	} {
		if _, _, _, _, err := ParseRestoreVolumeArgs(bad, func(string) string { return "" }); err == nil {
			t.Errorf("args %v parsed without error, want rejection", bad)
		}
	}
}

// RunRestoreVolume: for each pair — empty the target FIRST (EEXIST rule),
// then the client restore carrying --ns/--keyfile/--overwrite; a --configmap
// upload follows via kubectl (create --dry-run -o yaml, then apply -f file);
// success termlog is {"restored":..., "bytes":N}.
func TestRunRestoreVolume(t *testing.T) {
	target := t.TempDir()
	stale := filepath.Join(target, "stale-dir")
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatal(err)
	}
	termlog := filepath.Join(t.TempDir(), "termlog")

	run := func(argv, env []string, stdout, stderr io.Writer) error {
		_ = env
		switch {
		case argv[0] == "proxmox-backup-client":
			// EEXIST rule: the target must already be empty.
			if _, err := os.Stat(stale); !os.IsNotExist(err) {
				t.Error("target still had stale entries when the client restore ran")
			}
			if !pairIn(argv, "--ns", "tenant1") || !flagIn(argv, "--overwrite") || !pairIn(argv, "--keyfile", "") {
				t.Errorf("restore argv %q lacks --ns/--keyfile/--overwrite", argv)
			}
			// Leave a restored artifact so the byte count is nonzero.
			return os.WriteFile(filepath.Join(target, "api.yaml"), []byte("api: docs"), 0o644)
		case argv[1] == "create":
			want := "kubectl create configmap r1-api --from-file=api.yaml=" +
				filepath.Join(target, "api.yaml") + " --dry-run=client -o yaml"
			if strings.Join(argv, " ") != want {
				t.Errorf("kubectl create argv = %q\nwant %q", argv, want)
			}
			return nil
		case argv[1] == "apply":
			return nil
		}
		t.Errorf("unexpected exec %q", argv)
		return nil
	}
	code := RunRestoreVolume(RestoreDeps{
		Getenv: testEnv(), Environ: func() []string { return nil },
		Stdout: &strings.Builder{}, Stderr: &strings.Builder{}, Run: run,
	}, "host/bk/2026-09-16T10:00:00Z", []RestorePair{{"api.pxar.didx", target}}, "r1-api", termlog)
	if code != 0 {
		t.Fatalf("exit = %d, want 0", code)
	}
	out, err := os.ReadFile(termlog)
	if err != nil {
		t.Fatal(err)
	}
	var res struct {
		Restored string `json:"restored"`
		Bytes    int64  `json:"bytes"`
	}
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("termlog %q: %v", out, err)
	}
	if res.Restored != "api.pxar.didx" {
		t.Fatalf("termlog restored = %q", res.Restored)
	}
	if res.Bytes != int64(len("api: docs")) {
		t.Fatalf("termlog bytes = %d, want %d", res.Bytes, len("api: docs"))
	}
}

// A failing client restore exits with the client's code and writes the error
// termlog; the configmap upload never runs.
func TestRunRestoreVolumeClientFailure(t *testing.T) {
	target := t.TempDir()
	termlog := filepath.Join(t.TempDir(), "termlog")
	run := func(argv, env []string, stdout, stderr io.Writer) error {
		if argv[0] == "kubectl" {
			t.Error("kubectl ran although the restore failed")
		}
		fmt.Fprint(stderr, "Error: snapshot does not exist")
		return &fakeExit{7}
	}
	code := RunRestoreVolume(RestoreDeps{
		Getenv: testEnv(), Environ: func() []string { return nil },
		Stdout: &strings.Builder{}, Stderr: &strings.Builder{}, Run: run,
	}, "host/bk/2026-09-16T10:00:00Z", []RestorePair{{"api.pxar.didx", target}}, "", termlog)
	if code != 7 {
		t.Fatalf("exit = %d, want client's 7", code)
	}
	out, _ := os.ReadFile(termlog)
	if !strings.Contains(string(out), "snapshot does not exist") {
		t.Fatalf("termlog %q lacks the client error", out)
	}
}

// ParseApplyArgs: --phase pre|workload, --file required, comma lists.
func TestParseApplyArgs(t *testing.T) {
	phase, file, drop, keep, termlog, err := ParseApplyArgs(
		[]string{"--phase", "pre", "--file", "/staging/api/api.yaml", "--drop", "Secret,Probe", "--keep", "Probe"},
		func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if phase != "pre" || file != "/staging/api/api.yaml" || termlog != "/dev/termination-log" {
		t.Fatalf("phase=%q file=%q termlog=%q", phase, file, termlog)
	}
	if !reflect.DeepEqual(drop, []string{"Secret", "Probe"}) || !reflect.DeepEqual(keep, []string{"Probe"}) {
		t.Fatalf("drop=%v keep=%v", drop, keep)
	}
	if _, _, _, _, _, err := ParseApplyArgs([]string{"--phase", "bogus", "--file", "f"}, func(string) string { return "" }); err == nil {
		t.Error("bogus phase accepted")
	}
	if _, _, _, _, _, err := ParseApplyArgs([]string{"--phase", "pre"}, func(string) string { return "" }); err == nil {
		t.Error("missing --file accepted")
	}
}

// applyFixtureYAML: two pre docs, one CRD, one workload.
const applyFixtureYAML = `apiVersion: v1
kind: ConfigMap
metadata:
  name: cm1
  namespace: pg
---
apiVersion: v1
kind: Secret
metadata:
  name: sec1
  namespace: pg
---
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: probes.examples.sharifmind.ir
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: pg
  namespace: pg
`

// RunApplyManifests phase pre: apply the Namespace bucket, then CRDs + wait
// each Established (60s), then the PreVolume bucket — in that order, docs
// rewritten to the pod's namespace. DropKinds filter both api buckets.
func TestRunApplyManifestsPre(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "api.yaml")
	if err := os.WriteFile(file, []byte(applyFixtureYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	termlog := filepath.Join(dir, "termlog")

	var seq []string
	var bodies []string
	code := RunApplyManifests(ApplyDeps{
		PodNS:  func() (string, error) { return "restore-test", nil },
		Stdout: &strings.Builder{}, Stderr: &strings.Builder{},
		Run: func(argv, env []string, stdout, stderr io.Writer) error {
			_ = env
			switch {
			case argv[1] == "wait":
				seq = append(seq, "wait:"+argv[len(argv)-2])
			case argv[1] == "apply":
				body, _ := os.ReadFile(argv[len(argv)-1])
				bodies = append(bodies, string(body))
				seq = append(seq, "apply")
			}
			return nil
		},
	}, "pre", file, []string{"Secret"}, nil, termlog)
	if code != 0 {
		t.Fatalf("exit = %d, want 0", code)
	}
	if strings.Join(seq, ",") != "apply,wait:crd/probes.examples.sharifmind.ir,apply" {
		t.Fatalf("exec sequence = %v", seq)
	}
	// First apply: the CRD. Second: the pre docs (dropped Secret absent).
	if !strings.Contains(bodies[0], "kind: CustomResourceDefinition") {
		t.Errorf("first apply body:\n%s", bodies[0])
	}
	if !strings.Contains(bodies[1], "kind: ConfigMap") || strings.Contains(bodies[1], "kind: Secret") {
		t.Errorf("pre apply body wrong:\n%s", bodies[1])
	}
	if !strings.Contains(bodies[1], "namespace: restore-test") {
		t.Errorf("pre apply body lacks rewritten namespace:\n%s", bodies[1])
	}
	wantApplied(t, termlog, 2) // CRD + ConfigMap
}

// Phase workload: exactly one apply, workloads only, namespace rewritten.
func TestRunApplyManifestsWorkload(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "api.yaml")
	if err := os.WriteFile(file, []byte(applyFixtureYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	var bodies []string
	code := RunApplyManifests(ApplyDeps{
		PodNS:  func() (string, error) { return "restore-test", nil },
		Stdout: &strings.Builder{}, Stderr: &strings.Builder{},
		Run: func(argv, env []string, stdout, stderr io.Writer) error {
			if argv[1] == "apply" {
				body, _ := os.ReadFile(argv[len(argv)-1])
				bodies = append(bodies, string(body))
			}
			return nil
		},
	}, "workload", file, nil, nil, filepath.Join(dir, "termlog"))
	if code != 0 {
		t.Fatalf("exit = %d, want 0", code)
	}
	if len(bodies) != 1 || !strings.Contains(bodies[0], "kind: StatefulSet") {
		t.Fatalf("workload apply bodies = %q", bodies)
	}
	if !strings.Contains(bodies[0], "namespace: restore-test") {
		t.Fatalf("workload body lacks rewritten namespace:\n%s", bodies[0])
	}
}

// A kubectl failure exits passthrough with the error termlog.
func TestRunApplyManifestsFailure(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "api.yaml")
	if err := os.WriteFile(file, []byte(applyFixtureYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	termlog := filepath.Join(dir, "termlog")
	code := RunApplyManifests(ApplyDeps{
		PodNS:  func() (string, error) { return "restore-test", nil },
		Stdout: &strings.Builder{}, Stderr: &strings.Builder{},
		Run: func(argv, env []string, stdout, stderr io.Writer) error {
			fmt.Fprint(stderr, "Error from server (Forbidden): configmaps is forbidden")
			return &fakeExit{1}
		},
	}, "pre", file, nil, nil, termlog)
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	out, _ := os.ReadFile(termlog)
	if !strings.Contains(string(out), "forbidden") {
		t.Fatalf("termlog %q lacks kubectl error", out)
	}
}

// helpers -------------------------------------------------------------

// fakeExit satisfies the ExitCode() interface exitCode probes.
type fakeExit struct{ code int }

func (e *fakeExit) Error() string { return "exited" }

func (e *fakeExit) ExitCode() int { return e.code }

func wantApplied(t *testing.T, termlog string, n int) {
	t.Helper()
	out, err := os.ReadFile(termlog)
	if err != nil {
		t.Fatal(err)
	}
	var res struct {
		Applied int `json:"applied"`
	}
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("termlog %q: %v", out, err)
	}
	if res.Applied != n {
		t.Fatalf("termlog applied = %d, want %d", res.Applied, n)
	}
}

func pairIn(argv []string, flag, value string) bool {
	for i := 0; i+1 < len(argv); i++ {
		if argv[i] == flag && (value == "" || argv[i+1] == value) {
			return true
		}
	}
	return false
}

func flagIn(argv []string, flag string) bool {
	for _, a := range argv {
		if a == flag {
			return true
		}
	}
	return false
}
