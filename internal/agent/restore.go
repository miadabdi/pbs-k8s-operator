// restore.go implements the pbs-agent restore subcommands: volume data
// restore (restore-volume) and api.yaml staged apply (apply-manifests). Both
// wrap external binaries (proxmox-backup-client / kubectl), pass exit codes
// through, and report to the container termination log like the backup path.
package agent

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"gitlab.sharifmind.ir/miad/pbs-operator/internal/backup"
)

// RestorePair is one archive→target restore unit.
type RestorePair struct {
	Archive string // e.g. "pvc-data.pxar.didx" or "api.pxar.didx"
	Target  string // e.g. "/backup/data" (a mounted volume root)
}

// restoreArgv builds the client restore command. --ns is REQUIRED on the
// subcommand (live-verified: without it the client looks in the ROOT
// namespace and fails "snapshot does not exist"); --overwrite only covers
// files — emptyDir handles directories before this runs.
func restoreArgv(ns, keyfilePath, ref, archive, target string) []string {
	return []string{
		clientBinary, "restore", ref, archive, target,
		"--ns", ns,
		"--keyfile", keyfilePath,
		"--overwrite",
	}
}

// kubectl is the external binary the apply/upload paths shell out to (the
// agent image installs a static build).
const kubectl = "kubectl"

// RestoreDeps carries RunRestoreVolume's process seams; tests inject fakes.
type RestoreDeps struct {
	Getenv  func(string) string
	Environ func() []string
	Stdout  io.Writer
	Stderr  io.Writer
	Run     ClientRunner
}

// RunRestoreVolume restores each archive from the snapshot ref into its
// target directory, then (optionally) uploads a restored api.yaml as a
// ConfigMap. Per pair: empty the target FIRST (pxar extraction refuses
// existing directory entries with EEXIST even with --overwrite), then run
// the client with the 8-key env contract. Exit codes pass through; the
// termlog success contract is {"restored":"<archives>","bytes":N}.
func RunRestoreVolume(d RestoreDeps, ref string, pairs []RestorePair, configMap, termlogPath string) int {
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
	keyPath, cleanup, err := materializeKeyfile(c.Keyfile)
	if err != nil {
		return fail(1, "keyfile: %v", err)
	}
	defer cleanup()

	env := append(d.Environ(), clientEnv(c)...)
	var restored []string
	var total int64
	for _, p := range pairs {
		if err := emptyDir(p.Target); err != nil {
			return fail(1, "empty %s: %v", p.Target, err)
		}
		var errBuf bytes.Buffer
		tee := io.MultiWriter(d.Stderr, &errBuf)
		if err := d.Run(restoreArgv(c.NS, keyPath, ref, p.Archive, p.Target), env, d.Stdout, tee); err != nil {
			return fail(exitCode(err), "%s", firstLine(errBuf.String()))
		}
		restored = append(restored, p.Archive)
		total += dirSize(p.Target)
	}

	// Optional upload: the restored api.yaml becomes the <name>-api
	// ConfigMap the apply Jobs mount. create --dry-run -o yaml → temp file →
	// apply: idempotent without a shell pipe.
	if configMap != "" {
		if n := len(pairs); n != 1 {
			return fail(2, "configmap upload needs exactly one archive/target pair, got %d", n)
		}
		if err := uploadConfigMap(d, configMap, filepath.Join(pairs[0].Target, "api.yaml")); err != nil {
			return fail(exitCode(err), "configmap upload: %v", err)
		}
	}

	out := renderRestoreJSON(restored, total)
	writeTermlog(termlogPath, out, d.Stderr)
	fmt.Fprintln(d.Stdout, out)
	return 0
}

// uploadConfigMap stores src as key api.yaml of the named ConfigMap in the
// pod's own namespace (kubectl resolves it from the SA token). kubectl's
// stderr streams through; its error carries the exit code.
func uploadConfigMap(d RestoreDeps, name, src string) error {
	createArgv := []string{
		kubectl, "create", "configmap", name,
		"--from-file=api.yaml=" + src,
		"--dry-run=client", "-o", "yaml",
	}
	var outBuf bytes.Buffer
	if err := d.Run(createArgv, d.Environ(), &outBuf, d.Stderr); err != nil {
		return err
	}
	f, err := os.CreateTemp("", "pbs-agent-cm-*.yaml")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(outBuf.Bytes()); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return d.Run([]string{kubectl, "apply", "-f", f.Name()}, d.Environ(), d.Stdout, d.Stderr)
}

// emptyDir removes every entry under dir — never the dir itself (it is a
// volume mount point). The pxar EEXIST rule makes this mandatory before any
// restore into a target that may hold prior state.
func emptyDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// dirSize sums the regular files under dir (the termlog byte count).
func dirSize(dir string) int64 {
	var total int64
	_ = filepath.WalkDir(dir, func(_ string, e fs.DirEntry, err error) error {
		if err == nil && !e.IsDir() {
			if info, ierr := e.Info(); ierr == nil {
				total += info.Size()
			}
		}
		return nil // unreadable entries just don't count
	})
	return total
}

func renderRestoreJSON(restored []string, bytes int64) string {
	return fmt.Sprintf(`{"restored":%q,"bytes":%d}`, strings.Join(restored, ","), bytes)
}

// ApplyDeps carries RunApplyManifests' process seams. PodNS resolves the
// restore target namespace — in-pod it is the serviceaccount namespace file.
type ApplyDeps struct {
	PodNS  func() (string, error)
	Stdout io.Writer
	Stderr io.Writer
	Run    ClientRunner
}

// saNamespaceFile is the downward-API path every pod carries.
const saNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// DefaultPodNS reads the pod's own namespace (the in-pod SA token context).
func DefaultPodNS() (string, error) {
	b, err := os.ReadFile(saNamespaceFile)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", saNamespaceFile, err)
	}
	return strings.TrimSpace(string(b)), nil
}

// RunApplyManifests applies the api.yaml payload in two phases:
//
//	pre      Namespace docs → CRDs (+ wait Established, 60s each) →
//	         PreVolume docs (PVCs, Secrets, ...). PVCs applied here stay
//	         unbound under WaitForFirstConsumer — the volume-restore Job is
//	         the consumer whose scheduling binds them.
//	workload Workload docs, after volume data is back.
//
// Every namespaced doc is rewritten to the pod's namespace (restores are
// cross-namespace). kubectl exit codes pass through; the termlog success
// contract is {"applied":N}.
func RunApplyManifests(d ApplyDeps, phase, file string, drop, keep []string, termlogPath string) int {
	fail := func(code int, format string, args ...any) int {
		msg := fmt.Sprintf(format, args...)
		writeTermlog(termlogPath, errorJSON(msg), d.Stderr)
		fmt.Fprintln(d.Stderr, "pbs-agent:", msg)
		return code
	}
	ns, err := d.PodNS()
	if err != nil {
		return fail(1, "target namespace: %v", err)
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return fail(1, "read %s: %v", file, err)
	}
	docs, err := backup.ParseAPIManifests(data, ns)
	if err != nil {
		return fail(1, "parse %s: %v", file, err)
	}
	nsDocs, crds, pre, work := backup.BucketDocs(docs, drop, keep)

	applied := 0
	apply := func(bucket []backup.PlannedDoc) bool {
		if len(bucket) == 0 {
			return true
		}
		if err := kubectlApply(d, bucket); err != nil {
			fail(exitCode(err), "kubectl apply: %v", err)
			return false
		}
		applied += len(bucket)
		return true
	}
	waitCRD := func(doc backup.PlannedDoc) bool {
		argv := []string{kubectl, "wait", "--for=condition=Established",
			"crd/" + doc.Name, "--timeout=60s"}
		if err := d.Run(argv, nil, d.Stdout, d.Stderr); err != nil {
			fail(exitCode(err), "crd %s not Established: %v", doc.Name, err)
			return false
		}
		return true
	}

	switch phase {
	case "pre":
		for _, step := range []func() bool{
			func() bool { return apply(nsDocs) },
			func() bool {
				if !apply(crds) {
					return false
				}
				for i := range crds {
					if !waitCRD(crds[i]) {
						return false
					}
				}
				return true
			},
			func() bool { return apply(pre) },
		} {
			if !step() {
				return 1
			}
		}
	case "workload":
		if !apply(work) {
			return 1
		}
	default:
		return fail(2, "unknown phase %q", phase)
	}

	out := fmt.Sprintf(`{"applied":%d}`, applied)
	writeTermlog(termlogPath, out, d.Stderr)
	fmt.Fprintln(d.Stdout, out)
	return 0
}

// kubectlApply writes the bucket to a temp file (kubectl apply -f - would
// need stdin this runner has no seam for) and applies it. On failure the
// error keeps the exit code (for exitCode) and carries kubectl's stderr.
func kubectlApply(d ApplyDeps, docs []backup.PlannedDoc) error {
	f, err := os.CreateTemp("", "pbs-agent-apply-*.yaml")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	for _, doc := range docs {
		if _, err := f.Write(append(doc.YAML, []byte("---\n")...)); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	var errBuf bytes.Buffer
	tee := io.MultiWriter(d.Stderr, &errBuf)
	if err := d.Run([]string{kubectl, "apply", "-f", f.Name()}, nil, d.Stdout, tee); err != nil {
		return fmt.Errorf("%w: %s", err, firstLine(errBuf.String()))
	}
	return nil
}

// ParseRestoreVolumeArgs parses `restore-volume --ref <ref> --archive <a>
// --target <t> [--archive <a> --target <t>...] [--configmap <name>]
// [--termlog <path>]`. At least one archive/target pair is required.
func ParseRestoreVolumeArgs(args []string, getenv func(string) string) (ref string, pairs []RestorePair, configMap, termlog string, err error) {
	termlog = strings.TrimSpace(getenv("TERMLOG"))
	if termlog == "" {
		termlog = "/dev/termination-log"
	}
	var archives, targets []string
	fs := flag.NewFlagSet("restore-volume", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&ref, "ref", "", "snapshot reference (type/id/ISO8601Z)")
	fs.Var(&pvcFlag{&archives}, "archive", "archive name (repeatable), e.g. pvc-data.pxar.didx")
	fs.Var(&pvcFlag{&targets}, "target", "target directory (repeatable, pairs with --archive)")
	fs.StringVar(&configMap, "configmap", "", "upload the restored api.yaml as this ConfigMap")
	fs.StringVar(&termlog, "termlog", termlog, "termination log path")
	if err = fs.Parse(args); err != nil {
		return "", nil, "", "", err
	}
	if ref == "" {
		return "", nil, "", "", errors.New("restore-volume: --ref is required")
	}
	if len(archives) == 0 {
		return "", nil, "", "", errors.New("restore-volume: at least one --archive/--target pair is required")
	}
	if len(archives) != len(targets) {
		return "", nil, "", "", fmt.Errorf("restore-volume: %d --archive flags but %d --targets", len(archives), len(targets))
	}
	if fs.NArg() > 0 {
		return "", nil, "", "", fmt.Errorf("restore-volume: unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	pairs = make([]RestorePair, len(archives))
	for i := range archives {
		pairs[i] = RestorePair{Archive: archives[i], Target: targets[i]}
	}
	return ref, pairs, configMap, termlog, nil
}

// ParseApplyArgs parses `apply-manifests --phase pre|workload --file <path>
// [--drop Kind,...] [--keep Kind,...] [--termlog <path>]`.
func ParseApplyArgs(args []string, getenv func(string) string) (phase, file string, drop, keep []string, termlog string, err error) {
	termlog = strings.TrimSpace(getenv("TERMLOG"))
	if termlog == "" {
		termlog = "/dev/termination-log"
	}
	var dropList, keepList string
	fs := flag.NewFlagSet("apply-manifests", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&phase, "phase", "", "apply phase: pre or workload")
	fs.StringVar(&file, "file", "", "path to the api.yaml manifest")
	fs.StringVar(&dropList, "drop", "", "comma-separated Kinds to drop")
	fs.StringVar(&keepList, "keep", "", "comma-separated Kinds to keep (wins over drop)")
	fs.StringVar(&termlog, "termlog", termlog, "termination log path")
	if err = fs.Parse(args); err != nil {
		return "", "", nil, nil, "", err
	}
	if phase != "pre" && phase != "workload" {
		return "", "", nil, nil, "", fmt.Errorf("apply-manifests: --phase must be pre or workload, got %q", phase)
	}
	if file == "" {
		return "", "", nil, nil, "", errors.New("apply-manifests: --file is required")
	}
	if fs.NArg() > 0 {
		return "", "", nil, nil, "", fmt.Errorf("apply-manifests: unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	return phase, file, csv(dropList), csv(keepList), termlog, nil
}

// csv splits a comma list, trimming and dropping empties.
func csv(list string) []string {
	if strings.TrimSpace(list) == "" {
		return nil
	}
	var out []string
	for _, f := range strings.Split(list, ",") {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}
