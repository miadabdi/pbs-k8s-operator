//go:build e2e

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package e2e: live scenario suite for M1 (see task-10 brief). Helpers here,
// scenarios in scenarios_test.go. Everything skips fast (one probe, sync.Once)
// when no live cluster + PBS is reachable, so CI without testenv is safe.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/go-logr/logr"

	pbsv1 "gitlab.sharifmind.ir/miad/pbs-operator/api/v1"
	"gitlab.sharifmind.ir/miad/pbs-operator/internal/pbs"
)

// Live-environment facts (controller-run preconditions, see task-10 brief).
const (
	e2eNS      = "e2e"             // suite-owned scratch namespace
	repoName   = "testenv"         // pre-provisioned cluster-scoped PBSRepo
	repoSecret = "pbsrepo-testenv" // its credentials secret, ns default
	pgNS       = "pg"              // fixture: one bound PVC on k8s-ctl1
	pgNode     = "k8s-ctl1"
	crdappNS   = "crdapp" // fixture: one bound PVC on k8s-node1
	crdappNode = "k8s-node1"
	agentImage = "pbs-agent:dev" // carries proxmox-backup-client too
	readyCond  = "Ready"
	suiteLabel = "e2e-test" // marks every CR the suite creates
	isoFormat  = "2006-01-02T15:04:05Z"
)

var (
	ctx       = context.Background()
	k8s       client.Client
	clientset *kubernetes.Clientset
	liveRepo  pbsv1.PBSRepo // spec of the pre-provisioned testenv repo
	creds     struct{ tokenID, tokenSecret, fingerprint string }
	envOnce   sync.Once
	envErr    error
	runID     = time.Now().Unix() // unique object names per run
)

func TestMain(m *testing.M) {
	logf.SetLogger(logr.Discard()) // silence the "SetLogger never called" nag
	code := m.Run()
	sweep() // best-effort: drop CRs left by aborted runs
	os.Exit(code)
}

// sweep deletes any suite-labeled PBSRepo/PBSBackup cluster-wide. Normal runs
// already cleaned up via t.Cleanup; this covers SIGKILLed ones.
func sweep() {
	if k8s == nil {
		return
	}
	sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repos := &pbsv1.PBSRepoList{}
	if err := k8s.List(sctx, repos, client.MatchingLabels{suiteLabel: "true"}); err == nil {
		for i := range repos.Items {
			_ = k8s.Delete(sctx, &repos.Items[i])
		}
	}
	backups := &pbsv1.PBSBackupList{}
	if err := k8s.List(sctx, backups, client.MatchingLabels{suiteLabel: "true"}); err == nil {
		for i := range backups.Items {
			_ = k8s.Delete(sctx, &backups.Items[i])
		}
	}
}

// requireEnv probes the live environment once; every test calls it first and
// skips with a clear message when the cluster, the PBSRepo, or its secret are
// absent (CI-safety).
func requireEnv(t *testing.T) {
	t.Helper()
	envOnce.Do(func() {
		cfg, err := config.GetConfig() // honors KUBECONFIG, like kubectl
		if err != nil {
			envErr = fmt.Errorf("kubeconfig: %w", err)
			return
		}
		scheme := runtime.NewScheme()
		if err := clientgoscheme.AddToScheme(scheme); err != nil {
			envErr = fmt.Errorf("scheme: %w", err)
			return
		}
		if err := pbsv1.AddToScheme(scheme); err != nil {
			envErr = fmt.Errorf("scheme: %w", err)
			return
		}
		if k8s, err = client.New(cfg, client.Options{Scheme: scheme}); err != nil {
			envErr = fmt.Errorf("client: %w", err)
			return
		}
		if clientset, err = kubernetes.NewForConfig(cfg); err != nil {
			envErr = fmt.Errorf("clientset: %w", err)
			return
		}
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		nodes := &corev1.NodeList{}
		if err := k8s.List(pctx, nodes); err != nil || len(nodes.Items) == 0 {
			envErr = fmt.Errorf("no reachable cluster: %v", err)
			return
		}
		if err := k8s.Get(pctx, types.NamespacedName{Name: repoName}, &liveRepo); err != nil {
			envErr = fmt.Errorf("PBSRepo %s not found (deploy the live env first): %w", repoName, err)
			return
		}
		sec := &corev1.Secret{}
		if err := k8s.Get(pctx, types.NamespacedName{Namespace: "default", Name: repoSecret}, sec); err != nil {
			envErr = fmt.Errorf("repo secret default/%s not found: %w", repoSecret, err)
			return
		}
		creds.tokenID, creds.tokenSecret, creds.fingerprint =
			string(sec.Data["tokenID"]), string(sec.Data["tokenSecret"]), string(sec.Data["fingerprint"])
		if creds.tokenID == "" || creds.tokenSecret == "" || creds.fingerprint == "" {
			envErr = fmt.Errorf("repo secret default/%s lacks tokenID/tokenSecret/fingerprint", repoSecret)
		}
	})
	if envErr != nil {
		t.Skipf("live environment unavailable, skipping: %v", envErr)
	}
}

// eventually polls check every 2s until it returns nil or the deadline passes.
func eventually(t *testing.T, timeout time.Duration, what string, check func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		if err := check(); err == nil {
			return
		} else {
			last = err
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("gave up after %v waiting for %s: %v", timeout, what, last)
}

// ensureE2ENS creates the scratch namespace if absent.
func ensureE2ENS(t *testing.T) {
	t.Helper()
	err := k8s.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: e2eNS}})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create namespace %s: %v", e2eNS, err)
	}
}

// newBackup creates a suite-owned PBSBackup (pvcs nil = all-PVCs default).
func newBackup(t *testing.T, ns, name, repoRef string, pvcs []string) {
	t.Helper()
	b := &pbsv1.PBSBackup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{suiteLabel: "true"}},
		Spec:       pbsv1.PBSBackupSpec{RepoRef: repoRef, PVCs: pvcs},
	}
	if err := k8s.Create(ctx, b); err != nil {
		t.Fatalf("create PBSBackup %s/%s: %v", ns, name, err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, b) })
}

// newRepo creates a suite-owned cluster-scoped PBSRepo from the given spec.
func newRepo(t *testing.T, name string, spec pbsv1.PBSRepoSpec) {
	t.Helper()
	r := &pbsv1.PBSRepo{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{suiteLabel: "true"}},
		Spec:       spec,
	}
	if err := k8s.Create(ctx, r); err != nil {
		t.Fatalf("create PBSRepo %s: %v", name, err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, r) })
}

// scratchRepoSpec copies the live repo's spec minus the bootstrap ref (the PBS
// namespace already exists; scratch repos only need to ping).
func scratchRepoSpec() pbsv1.PBSRepoSpec {
	s := liveRepo.Spec
	s.BootstrapTokenRef = pbsv1.NamespacedSecretRef{}
	return s
}

// flipFingerprint changes the first hex digit of fp — "flip one hex pair" from
// the brief — enough for the PBS client to reject the server cert.
func flipFingerprint(t *testing.T, fp string) string {
	t.Helper()
	if len(fp) < 2 {
		t.Fatalf("fingerprint %q too short to flip", fp)
	}
	b := []byte(fp)
	if b[0] == '3' {
		b[0] = '4'
	} else {
		b[0] = '3'
	}
	return string(b)
}

// getBackup fetches the current state of a PBSBackup.
func getBackup(t *testing.T, ns, name string) pbsv1.PBSBackup {
	t.Helper()
	var b pbsv1.PBSBackup
	if err := k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &b); err != nil {
		t.Fatalf("get PBSBackup %s/%s: %v", ns, name, err)
	}
	return b
}

// waitForBackupTerminal waits for phase Completed or Failed.
func waitForBackupTerminal(t *testing.T, ns, name string, timeout time.Duration) pbsv1.PBSBackup {
	t.Helper()
	var out pbsv1.PBSBackup
	eventually(t, timeout, "PBSBackup "+ns+"/"+name+" to reach a terminal phase", func() error {
		if err := k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &out); err != nil {
			return err
		}
		if out.Status.Phase != pbsv1.BackupPhaseCompleted && out.Status.Phase != pbsv1.BackupPhaseFailed {
			return fmt.Errorf("phase %q", out.Status.Phase)
		}
		return nil
	})
	return out
}

// waitForBackupHold waits for phase Scheduled + Ready=False/<reason> (the
// controller's "hold" state for blocked backups).
func waitForBackupHold(t *testing.T, ns, name, reason string, timeout time.Duration) {
	t.Helper()
	eventually(t, timeout, "PBSBackup "+ns+"/"+name+" to hold on "+reason, func() error {
		b := getBackup(t, ns, name)
		if b.Status.Phase != pbsv1.BackupPhaseScheduled {
			return fmt.Errorf("phase %q, want Scheduled", b.Status.Phase)
		}
		c := meta.FindStatusCondition(b.Status.Conditions, readyCond)
		if c == nil {
			return fmt.Errorf("no Ready condition")
		}
		if c.Status != metav1.ConditionFalse || c.Reason != reason {
			return fmt.Errorf("Ready=%s/%s, want False/%s", c.Status, c.Reason, reason)
		}
		return nil
	})
}

// waitForRepoCond waits for the PBSRepo Ready condition to reach
// wantStatus/wantReason.
func waitForRepoCond(t *testing.T, name string, wantStatus metav1.ConditionStatus, wantReason string, timeout time.Duration) {
	t.Helper()
	eventually(t, timeout, "PBSRepo "+name+" Ready="+string(wantStatus)+"/"+wantReason, func() error {
		var r pbsv1.PBSRepo
		if err := k8s.Get(ctx, types.NamespacedName{Name: name}, &r); err != nil {
			return err
		}
		c := meta.FindStatusCondition(r.Status.Conditions, readyCond)
		if c == nil {
			return fmt.Errorf("no Ready condition")
		}
		if c.Status != wantStatus || c.Reason != wantReason {
			return fmt.Errorf("Ready=%s/%s, want %s/%s (message %q)", c.Status, c.Reason, wantStatus, wantReason, c.Message)
		}
		return nil
	})
}

// requireJobPinned asserts the Job exists, requires the given node via
// hostname affinity, and its pod actually landed there. Returns the Job.
func requireJobPinned(t *testing.T, ns, jobName, wantNode string) *batchv1.Job {
	t.Helper()
	job := &batchv1.Job{}
	eventually(t, 90*time.Second, "Job "+ns+"/"+jobName+" to exist", func() error {
		return k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: jobName}, job)
	})
	pinned := false
	for _, term := range job.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		for _, req := range term.MatchExpressions {
			if req.Key == "kubernetes.io/hostname" && req.Operator == corev1.NodeSelectorOpIn &&
				len(req.Values) == 1 && req.Values[0] == wantNode {
				pinned = true
			}
		}
	}
	if !pinned {
		t.Fatalf("job %s not pinned to node %s via hostname affinity", jobName, wantNode)
	}
	var pod corev1.Pod
	eventually(t, 60*time.Second, "pod of job "+jobName, func() error {
		pods := &corev1.PodList{}
		if err := k8s.List(ctx, pods, client.InNamespace(ns), client.MatchingLabels{"pbsbackup": jobName}); err != nil {
			return err
		}
		if len(pods.Items) == 0 {
			return fmt.Errorf("no pod labeled pbsbackup=%s", jobName)
		}
		pod = pods.Items[0]
		return nil
	})
	if pod.Spec.NodeName != wantNode {
		t.Fatalf("job %s pod landed on node %q, want %q", jobName, pod.Spec.NodeName, wantNode)
	}
	return job
}

// requireJobExists waits for a Job to appear (no phase assertion).
func requireJobExists(t *testing.T, ns, jobName string) {
	t.Helper()
	eventually(t, 90*time.Second, "Job "+ns+"/"+jobName+" to exist", func() error {
		return k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: jobName}, &batchv1.Job{})
	})
}

// countBackupJobs counts Jobs in ns whose pbsbackup label starts with
// backupName (job names are <backup>-<node>).
func countBackupJobs(t *testing.T, ns, backupName string) int {
	t.Helper()
	jobs := &batchv1.JobList{}
	if err := k8s.List(ctx, jobs, client.InNamespace(ns)); err != nil {
		t.Fatalf("list jobs in %s: %v", ns, err)
	}
	n := 0
	for _, j := range jobs.Items {
		if strings.HasPrefix(j.Labels["pbsbackup"], backupName) {
			n++
		}
	}
	return n
}

// backupHasEvent reports an event on objName with the given reason whose
// message contains substr.
func backupHasEvent(ns, objName, reason, substr string) bool {
	evs := &corev1.EventList{}
	if err := k8s.List(ctx, evs, client.InNamespace(ns)); err != nil {
		return false
	}
	for _, e := range evs.Items {
		if e.InvolvedObject.Name == objName && e.Reason == reason && strings.Contains(e.Message, substr) {
			return true
		}
	}
	return false
}

// waitJobComplete waits for the Job to reach Complete (Failed is fatal, with
// pod logs for diagnosis).
func waitJobComplete(t *testing.T, ns, name string, timeout time.Duration) {
	t.Helper()
	job := &batchv1.Job{}
	eventually(t, timeout, "Job "+ns+"/"+name+" to complete", func() error {
		if err := k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, job); err != nil {
			return err
		}
		for _, c := range job.Status.Conditions {
			if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
				t.Fatalf("job %s failed: %s", name, podLogs(t, ns, name))
			}
			if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
				return nil
			}
		}
		return fmt.Errorf("not complete yet")
	})
}

// podLogs returns the merged log of the (single) pod of a Job, found via the
// e2e-verify label.
func podLogs(t *testing.T, ns, jobName string) string {
	t.Helper()
	pods := &corev1.PodList{}
	if err := k8s.List(ctx, pods, client.InNamespace(ns), client.MatchingLabels{"e2e-verify": jobName}); err != nil || len(pods.Items) == 0 {
		return fmt.Sprintf("(no pod found: %v)", err)
	}
	stream, err := clientset.CoreV1().Pods(ns).GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{}).Stream(ctx)
	if err != nil {
		return fmt.Sprintf("(logs: %v)", err)
	}
	defer stream.Close()
	body, _ := io.ReadAll(stream)
	return string(body)
}

// verifySnapshotOnPBS runs a one-off Job on the pbs-agent image executing
// `proxmox-backup-client snapshot list` with env from the repo secret,
// asserts the group/ref from snapshotRef is present in PBS, and returns the
// snapshot's file list (api.pxar.didx, pvc-*.pxar.didx, ...).
func verifySnapshotOnPBS(t *testing.T, jobName, snapshotRef string) []string {
	t.Helper()
	ensureE2ENS(t)
	parts := strings.SplitN(snapshotRef, "/", 3)
	if len(parts) != 3 || parts[0] != "host" || parts[1] != jobName {
		t.Fatalf("snapshotRef %q malformed, want host/%s/<ISO8601Z>", snapshotRef, jobName)
	}
	wantTime, err := time.Parse(isoFormat, parts[2])
	if err != nil {
		t.Fatalf("snapshotRef %q timestamp: %v", snapshotRef, err)
	}

	out := runVerifyJob(t, "list", "proxmox-backup-client snapshot list --ns "+
		liveRepo.Spec.Namespace+" --output-format json")
	var snaps []pbs.Snapshot
	if err := json.Unmarshal([]byte(out), &snaps); err != nil {
		t.Fatalf("parse snapshot list %q: %v", out, err)
	}
	for _, s := range snaps {
		if s.BackupType == "host" && s.BackupID == jobName {
			if d := s.BackupTime - float64(wantTime.Unix()); d < -120 || d > 120 {
				t.Fatalf("snapshot %s/%s backup-time %v is >2m from ref time %s", s.BackupType, s.BackupID, s.BackupTime, parts[2])
			}
			return snapshotFilesOnPBS(t, snapshotRef)
		}
	}
	t.Fatalf("no PBS snapshot host/%s found in %d snapshots for %s", jobName, len(snaps), liveRepo.Spec.Namespace)
	return nil
}

// snapshotFilesOnPBS returns the archive file names of one snapshot
// (api.pxar.didx, pvc-<name>.pxar.didx, ...) via `snapshot files <ref>`.
func snapshotFilesOnPBS(t *testing.T, snapshotRef string) []string {
	t.Helper()
	out := runVerifyJob(t, "files", "proxmox-backup-client snapshot files "+snapshotRef+
		" --ns "+liveRepo.Spec.Namespace+" --output-format json")
	var files []struct {
		Filename string `json:"filename"`
	}
	if err := json.Unmarshal([]byte(out), &files); err != nil {
		t.Fatalf("parse snapshot files %q: %v", out, err)
	}
	if len(files) == 0 {
		t.Fatalf("snapshot %s lists no files", snapshotRef)
	}
	names := make([]string, 0, len(files))
	for _, f := range files {
		names = append(names, f.Filename)
	}
	return names
}

// verifySeq disambiguates verify Jobs within one run: every invocation gets
// its own Job (async cleanup may leave the previous one terminating).
var verifySeq atomic.Uint64

// runVerifyJob runs one one-off verify Job executing clientCmd (a
// proxmox-backup-client invocation) with env from the repo secret and returns
// its stdout trimmed to the JSON array the CLI prints (stderr junk stripped).
func runVerifyJob(t *testing.T, kind, clientCmd string) string {
	t.Helper()
	name := fmt.Sprintf("e2e-verify-%s-%d-%d", kind, runID, verifySeq.Add(1))
	port := liveRepo.Spec.Port
	if port == 0 {
		port = 8007
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e2eNS, Labels: map[string]string{suiteLabel: "true"}},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptr.To[int32](0),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"e2e-verify": name}},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:            "verify",
						Image:           agentImage,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Command:         []string{"sh", "-c", clientCmd},
						Env: []corev1.EnvVar{
							{Name: "PBS_REPOSITORY", Value: fmt.Sprintf("%s@%s:%d:%s",
								creds.tokenID, liveRepo.Spec.Host, port, liveRepo.Spec.Datastore)},
							{Name: "PBS_PASSWORD", Value: creds.tokenSecret},
							{Name: "PBS_FINGERPRINT", Value: creds.fingerprint},
						},
					}},
				},
			},
		},
	}
	if err := k8s.Create(ctx, job); err != nil {
		t.Fatalf("create verify job: %v", err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, job) })
	waitJobComplete(t, e2eNS, name, 2*time.Minute)

	out := podLogs(t, e2eNS, name)
	// The CLI prints a bare JSON array on stdout; stderr junk may precede it.
	lo, hi := strings.Index(out, "["), strings.LastIndex(out, "]")
	if lo < 0 || hi < lo {
		t.Fatalf("verify job output has no JSON array: %s", out)
	}
	return out[lo : hi+1]
}

// newUnboundPVC creates a local-path PVC that never binds (the storage class
// is WaitForFirstConsumer and nothing mounts it).
func newUnboundPVC(t *testing.T, name string) {
	t.Helper()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e2eNS, Labels: map[string]string{suiteLabel: "true"}},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{"storage": resource.MustParse("64Mi")}},
			StorageClassName: ptr.To("local-path"),
		},
	}
	if err := k8s.Create(ctx, pvc); err != nil {
		t.Fatalf("create PVC %s/%s: %v", e2eNS, name, err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, pvc) })
}

// getSecret fetches a Secret or fatals.
func getSecret(t *testing.T, ns, name string) *corev1.Secret {
	t.Helper()
	s := &corev1.Secret{}
	if err := k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, s); err != nil {
		t.Fatalf("get secret %s/%s: %v", ns, name, err)
	}
	return s
}
