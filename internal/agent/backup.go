// Package agent implements the pbs-agent backup subcommand: it wraps
// proxmox-backup-client for the Jobs built by internal/backup.BuildBackupJob
// and reports the result to the container termination log.
package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"gitlab.sharifmind.ir/miad/pbs-operator/internal/backup"
	"gitlab.sharifmind.ir/miad/pbs-operator/internal/pbs"
)

// clientBinary is the external command doing the actual backup work.
const clientBinary = "proxmox-backup-client"

// envKeys is the 8-key secret contract the Job injects (internal/backup.envContract).
var envKeys = [8]string{
	"PBS_HOST", "PBS_PORT", "PBS_DATASTORE", "PBS_NS",
	"PBS_TOKEN_ID", "PBS_TOKEN_SECRET", "PBS_FINGERPRINT", "PBS_KEYFILE",
}

// config holds the 8 contract values read once from the environment.
type config struct {
	Host, Port, Datastore, NS, TokenID, TokenSecret, Fingerprint, Keyfile string
}

// loadConfig reads the contract env vars, returning the missing ones.
// Values are whitespace-trimmed: secrets edited as YAML block scalars often
// carry a trailing newline, which PBS rejects.
func loadConfig(getenv func(string) string) (config, []string) {
	var c config
	var missing []string
	vals := []*string{&c.Host, &c.Port, &c.Datastore, &c.NS,
		&c.TokenID, &c.TokenSecret, &c.Fingerprint, &c.Keyfile}
	for i, k := range envKeys {
		v := strings.TrimSpace(getenv(k))
		if v == "" {
			missing = append(missing, k)
		}
		*vals[i] = v
	}
	return c, missing
}

// clientEnv builds the environment proxmox-backup-client authenticates with.
// PBS_REPOSITORY is <authid>@<host>:<port>:<datastore>; the authid itself
// contains literal @ and ! (e.g. operator@pbs!producer) — no escaping.
func clientEnv(c config) []string {
	return []string{
		"PBS_REPOSITORY=" + c.TokenID + "@" + c.Host + ":" + c.Port + ":" + c.Datastore,
		"PBS_PASSWORD=" + c.TokenSecret,
		"PBS_FINGERPRINT=" + c.Fingerprint,
	}
}

// backupArgv builds the backup command: one pxar pair per PVC, order preserved
// (the Job passes --pvc args in mount order), then the api.pxar pair for the
// serialized API objects when apiDir is set (the --api staging mount).
func backupArgv(ns, keyfilePath string, pvcs []string, apiDir string) []string {
	argv := []string{clientBinary, "backup", "--ns", ns, "--keyfile", keyfilePath}
	for _, pvc := range pvcs {
		argv = append(argv, fmt.Sprintf("pvc-%s.pxar:/backup/%s", pvc, pvc))
	}
	if apiDir != "" {
		argv = append(argv, "api.pxar:"+apiDir)
	}
	return argv
}

// notesArgv builds the post-upload command that attaches the retention-hint
// notes to the fresh snapshot. The backup subcommand has no --notes flag in
// the real client ("schema does not allow additional properties" — live-
// verified); `snapshot notes update` is the only channel.
func notesArgv(ref, notes string) []string {
	return []string{clientBinary, "snapshot", "notes", "update", ref, notes}
}

// listArgv builds the snapshot-list command used to find the fresh snapshot.
func listArgv(ns string) []string {
	return []string{clientBinary, "snapshot", "list", "--ns", ns, "--output-format", "json"}
}

// ClientRunner executes an external command (argv[0] plus argv[1:]) under the
// complete child environment env; stdout/stderr are the child's streams.
type ClientRunner func(argv, env []string, stdout, stderr io.Writer) error

// ExecClient is the real ClientRunner; unit tests never invoke it.
func ExecClient(argv, env []string, stdout, stderr io.Writer) error {
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = env
	cmd.Stdout, cmd.Stderr = stdout, stderr
	return cmd.Run()
}

// BackupDeps carries RunBackup's process seams; tests inject fakes.
type BackupDeps struct {
	Getenv   func(string) string
	Hostname func() (string, error) // backup-id = the Job's hostname
	Environ  func() []string
	Stdout   io.Writer
	Stderr   io.Writer
	Run      ClientRunner
}

// RunBackup performs the backup flow (steps 1-7 of the task contract) and
// returns the process exit code. The client binary's stdout/stderr stream
// through to ours; the termination log receives the controller contract JSON.
// apiDir (the --api staging mount) adds the api.pxar pair; empty skips it.
// notes (M3) is passed through to the client as --notes=<value>.
func RunBackup(d BackupDeps, pvcs []string, apiDir, notes, termlogPath string) int {
	// fail reports an error via the termlog error JSON and our stderr.
	fail := func(code int, format string, args ...any) int {
		msg := fmt.Sprintf(format, args...)
		writeTermlog(termlogPath, errorJSON(msg), d.Stderr)
		fmt.Fprintln(d.Stderr, "pbs-agent:", msg)
		return code
	}

	c, missing := loadConfig(d.Getenv)
	if len(missing) > 0 {
		return fail(2, "missing required env: %s", strings.Join(missing, ", "))
	}
	host, err := d.Hostname()
	if err != nil {
		return fail(1, "hostname: %v", err)
	}
	keyPath, cleanup, err := materializeKeyfile(c.Keyfile)
	if err != nil {
		return fail(1, "keyfile: %v", err)
	}
	defer cleanup()

	env := append(d.Environ(), clientEnv(c)...)

	// Step 4: run the backup, streaming client output to the Job logs while
	// teeing stderr for the error report.
	var errBuf bytes.Buffer
	tee := io.MultiWriter(d.Stderr, &errBuf)
	if err := d.Run(backupArgv(c.NS, keyPath, pvcs, apiDir), env, d.Stdout, tee); err != nil {
		return fail(exitCode(err), "%s", firstLine(errBuf.String()))
	}

	// Step 5: list snapshots and pick the newest in our backup group.
	var outBuf bytes.Buffer
	errBuf.Reset()
	if err := d.Run(listArgv(c.NS), env, &outBuf, tee); err != nil {
		return fail(exitCode(err), "%s", firstLine(errBuf.String()))
	}
	snaps, err := parseSnapshotList(outBuf.Bytes())
	if err != nil {
		return fail(1, "snapshot list: %v", err)
	}
	snap, err := latestHostSnapshot(snaps, host)
	if err != nil {
		return fail(1, "snapshot list: %v", err)
	}

	// Step 5.5 (M3): attach the retention-hint notes to the fresh snapshot
	// (the backup subcommand itself takes no notes flag). Notes are a HINT
	// channel: the snapshot is already safe, and setting notes needs
	// Datastore.Modify privileges the backup token may deliberately lack
	// (live-verified: testenv's producer token is Backup-only) — so a notes
	// failure is a loud stderr warning, never a failed backup.
	if notes != "" {
		errBuf.Reset()
		if err := d.Run(notesArgv(snapshotRef(snap), notes), env, io.Discard, tee); err != nil {
			fmt.Fprintf(d.Stderr, "pbs-agent: WARNING: snapshot %s uploaded, but setting notes failed: %s\n",
				snapshotRef(snap), firstLine(errBuf.String()))
		}
	}

	// Step 6: report the result to the termlog and stdout.
	out := successJSON(snap)
	writeTermlog(termlogPath, out, d.Stderr)
	fmt.Fprintln(d.Stdout, out)
	return 0
}

// materializeKeyfile writes the PBS_KEYFILE JSON content to a 0600 temp file;
// cleanup removes it.
func materializeKeyfile(content string) (path string, cleanup func(), err error) {
	f, err := os.CreateTemp("", "pbs-agent-keyfile-*.json") // already mode 0600
	if err != nil {
		return "", nil, err
	}
	if _, err := f.WriteString(content); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", nil, err
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return "", nil, err
	}
	return f.Name(), func() { os.Remove(f.Name()) }, nil
}

// parseSnapshotList decodes the client's `snapshot list --output-format json`
// output. The CLI prints a bare top-level array (live-verified); the
// {"data":[...]} envelope is the REST API shape — accept both.
func parseSnapshotList(data []byte) ([]pbs.Snapshot, error) {
	var out struct {
		Data []pbs.Snapshot `json:"data"`
	}
	if err := json.Unmarshal(data, &out); err == nil {
		return out.Data, nil
	}
	var snaps []pbs.Snapshot
	if err := json.Unmarshal(data, &snaps); err != nil {
		return nil, fmt.Errorf("decode: not an envelope or bare array: %w", err)
	}
	return snaps, nil
}

// latestHostSnapshot picks the newest snapshot of the "host" group with the
// given backup-id (the Job's hostname).
func latestHostSnapshot(snaps []pbs.Snapshot, backupID string) (pbs.Snapshot, error) {
	return backup.LatestSnapshot(snaps, "host", backupID)
}

// snapshotRef renders the snapshot reference for the termlog contract.
func snapshotRef(snap pbs.Snapshot) string {
	return backup.ComposeSnapshotRef(snap.BackupType, snap.BackupID, snap.BackupTime)
}

// successJSON renders the controller's success contract:
// {"snapshotRef":"host/<id>/<ISO8601Z>","bytes":N}.
func successJSON(snap pbs.Snapshot) string {
	b, err := json.Marshal(struct {
		SnapshotRef string `json:"snapshotRef"`
		Bytes       int64  `json:"bytes"`
	}{snapshotRef(snap), snap.Size})
	if err != nil { // unreachable: two scalars
		return errorJSON("render result: " + err.Error())
	}
	return string(b)
}

// errorJSON renders the termlog failure contract: {"error":"..."}.
func errorJSON(msg string) string {
	b, _ := json.Marshal(struct {
		Error string `json:"error"`
	}{msg})
	return string(b)
}

// writeTermlog writes one JSON line to the termination log; a write failure is
// reported to stderr but never masks the flow's exit code (the controller
// treats an unreadable termlog as a failure anyway).
func writeTermlog(path, jsonLine string, stderr io.Writer) {
	if err := os.WriteFile(path, []byte(jsonLine+"\n"), 0o644); err != nil {
		fmt.Fprintf(stderr, "pbs-agent: write termination log: %v\n", err)
	}
}

// exitCode maps a client error to its exit code, falling back to 1.
func exitCode(err error) int {
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if code := ee.ExitCode(); code >= 0 {
			return code
		}
	}
	return 1
}

// firstLine returns the first non-empty line, for the termlog error report.
func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return s
}

// ParseBackupArgs parses `backup --pvc <name> [--pvc <name>...] [--api <dir>]
// [--notes <value>] [--termlog <path>]`. The termlog path defaults to $TERMLOG,
// then /dev/termination-log. At least one --pvc or an --api dir is required
// (API-only backups have no PVCs).
func ParseBackupArgs(args []string, getenv func(string) string) (pvcs []string, apiDir, notes, termlog string, err error) {
	termlog = strings.TrimSpace(getenv("TERMLOG"))
	if termlog == "" {
		termlog = "/dev/termination-log"
	}
	fs := flag.NewFlagSet("backup", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Var(&pvcFlag{&pvcs}, "pvc", "PVC name (repeatable; mounted at /backup/<name>)")
	fs.StringVar(&apiDir, "api", "", "directory with the serialized API objects (staging ConfigMap mount)")
	fs.StringVar(&notes, "notes", "", "retention notes carried to the PBS snapshot")
	fs.StringVar(&termlog, "termlog", termlog, "termination log path")
	if err := fs.Parse(args); err != nil {
		return nil, "", "", "", err
	}
	if len(pvcs) == 0 && apiDir == "" {
		return nil, "", "", "", errors.New("backup: at least one --pvc or --api is required")
	}
	if fs.NArg() > 0 {
		return nil, "", "", "", fmt.Errorf("backup: unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	return pvcs, apiDir, notes, termlog, nil
}

// pvcFlag collects repeated --pvc values.
type pvcFlag struct{ dst *[]string }

func (p *pvcFlag) String() string { return strings.Join(*p.dst, ",") }

func (p *pvcFlag) Set(v string) error {
	*p.dst = append(*p.dst, v)
	return nil
}
