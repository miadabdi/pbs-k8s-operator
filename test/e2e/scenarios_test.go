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

package e2e

// The M1 acceptance matrix (task-10 brief): happy, failure, and stability
// scenarios, in brief order. Scenario 1 (TestBackupSingleNode) is the M1 gate:
// keep it FIRST and independent — every test creates its own objects, so any
// subset is runnable via -run.

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pbsv1 "gitlab.sharifmind.ir/miad/pbs-operator/api/v1"
	"gitlab.sharifmind.ir/miad/pbs-operator/internal/hooks"
)

// assertHappyBackup covers the shared happy path: terminal Completed phase,
// Job pinned to wantNode (affinity + real pod placement), snapshotRef shaped
// host/<job>/<ISO8601Z>, bytes > 0. PBS-side verification only for the gate.
func assertHappyBackup(t *testing.T, ns, name, wantNode string, verifyOnPBS bool) pbsv1.PBSBackup {
	t.Helper()
	b := waitForBackupTerminal(t, ns, name, 3*time.Minute)
	if b.Status.Phase != pbsv1.BackupPhaseCompleted {
		t.Fatalf("backup %s/%s phase %s, want Completed", ns, name, b.Status.Phase)
	}
	jobName := name + "-" + wantNode
	requireJobPinned(t, ns, jobName, wantNode)

	refRe := regexp.MustCompile(`^host/` + regexp.QuoteMeta(jobName) + `/(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)$`)
	if !refRe.MatchString(b.Status.SnapshotRef) {
		t.Fatalf("snapshotRef %q does not match host/%s/<ISO8601Z>", b.Status.SnapshotRef, jobName)
	}
	if b.Status.Bytes <= 0 {
		t.Fatalf("backup %s/%s reported bytes %d, want > 0", ns, name, b.Status.Bytes)
	}
	if verifyOnPBS {
		verifySnapshotOnPBS(t, jobName, b.Status.SnapshotRef)
	}
	return b
}

// Scenario 1 (M1 GATE): all-PVCs backup in the pg namespace (one bound PVC on
// k8s-ctl1) completes, is pinned, and the snapshot really exists on PBS.
func TestBackupSingleNode(t *testing.T) {
	requireEnv(t)
	name := fmt.Sprintf("pg-e2e-gate-%d", runID)
	newBackup(t, pgNS, name, repoName, nil)
	assertHappyBackup(t, pgNS, name, pgNode, true)
}

// Scenario 2: same shape in the crdapp namespace — the Job must land on the
// OTHER node (k8s-node1), proving per-node pinning.
func TestBackupSecondNamespace(t *testing.T) {
	requireEnv(t)
	name := fmt.Sprintf("crdapp-e2e-second-%d", runID)
	newBackup(t, crdappNS, name, repoName, nil)
	assertHappyBackup(t, crdappNS, name, crdappNode, false)
}

// Scenario 3: a second PBSBackup in the same namespace completes with a
// snapshot ref distinct from the first one's.
func TestBackupRerun(t *testing.T) {
	requireEnv(t)
	first := fmt.Sprintf("pg-e2e-rerun1-%d", runID)
	second := fmt.Sprintf("pg-e2e-rerun2-%d", runID)
	newBackup(t, pgNS, first, repoName, nil)
	newBackup(t, pgNS, second, repoName, nil)
	b1 := assertHappyBackup(t, pgNS, first, pgNode, false)
	b2 := assertHappyBackup(t, pgNS, second, pgNode, false)
	if b1.Status.SnapshotRef == b2.Status.SnapshotRef {
		t.Fatalf("rerun snapshot ref %q identical to first backup's", b2.Status.SnapshotRef)
	}
}

// Scenario 3b (M2 GATE): the same pg-namespace backup now also serializes the
// namespace's API objects — the snapshot's file list must contain api.pxar.didx
// alongside the pvc-*.pxar.didx data archives (one snapshot, both payloads).
func TestBackupWithAPISerialization(t *testing.T) {
	requireEnv(t)
	name := fmt.Sprintf("pg-e2e-api-%d", runID)
	newBackup(t, pgNS, name, repoName, nil)
	b := assertHappyBackup(t, pgNS, name, pgNode, true)

	files := snapshotFilesOnPBS(t, b.Status.SnapshotRef)
	hasAPI, hasPVC := false, false
	for _, f := range files {
		if f == "api.pxar.didx" {
			hasAPI = true
		}
		if strings.HasPrefix(f, "pvc-") && strings.HasSuffix(f, ".pxar.didx") {
			hasPVC = true
		}
	}
	if !hasAPI {
		t.Fatalf("snapshot %s files %v lack api.pxar.didx", b.Status.SnapshotRef, files)
	}
	if !hasPVC {
		t.Fatalf("snapshot %s files %v lack any pvc-*.pxar.didx (data and API must share the snapshot)", b.Status.SnapshotRef, files)
	}
}

// Scenario 4: PBSRepo with one flipped hex pair in the fingerprint goes
// Ready=False/Unreachable, and a PBSBackup referencing it holds in Scheduled
// with no Job created.
func TestRepoUnreachable(t *testing.T) {
	requireEnv(t)
	spec := scratchRepoSpec()
	spec.Fingerprint = flipFingerprint(t, spec.Fingerprint)
	repo := fmt.Sprintf("e2e-badfp-%d", runID)
	newRepo(t, repo, spec)
	waitForRepoCond(t, repo, metav1.ConditionFalse, "Unreachable", 60*time.Second)

	ensureE2ENS(t)
	backup := fmt.Sprintf("e2e-badfp-%d", runID)
	newBackup(t, e2eNS, backup, repo, nil)
	waitForBackupHold(t, e2eNS, backup, "RepoNotReady", 60*time.Second)
	if n := countBackupJobs(t, e2eNS, backup); n != 0 {
		t.Fatalf("backup %s has %d job(s), want none while repo is unreachable", backup, n)
	}
}

// Scenario 5: PBSRepo pointing at an absent secret goes Ready=False/
// SecretMissing with a message naming the secret.
func TestSecretMissing(t *testing.T) {
	requireEnv(t)
	spec := scratchRepoSpec()
	spec.SecretRef.Name = "e2e-no-such-secret"
	repo := fmt.Sprintf("e2e-nosecret-%d", runID)
	newRepo(t, repo, spec)
	waitForRepoCond(t, repo, metav1.ConditionFalse, "SecretMissing", 60*time.Second)

	var r pbsv1.PBSRepo
	if err := k8s.Get(ctx, client.ObjectKey{Name: repo}, &r); err != nil {
		t.Fatalf("get repo: %v", err)
	}
	c := meta.FindStatusCondition(r.Status.Conditions, readyCond)
	if c == nil {
		t.Fatalf("no Ready condition on repo %s", repo)
	}
	if want := spec.SecretRef.Name; !strings.Contains(c.Message, want) {
		t.Fatalf("SecretMissing message %q does not name the secret %q", c.Message, want)
	}
}

// Scenario 6: a PVC that never binds (local-path is WaitForFirstConsumer and
// nothing mounts it) keeps the PBSBackup non-terminal in Scheduled with a
// PlacementFailed event naming the PVC, and no Job is created.
func TestUnboundPVC(t *testing.T) {
	requireEnv(t)
	ensureE2ENS(t)
	backup := fmt.Sprintf("e2e-unbound-%d", runID)
	newUnboundPVC(t, backup)
	newBackup(t, e2eNS, backup, repoName, []string{backup})
	waitForBackupHold(t, e2eNS, backup, "PlacementFailed", 60*time.Second)
	eventually(t, 30*time.Second, "PlacementFailed event naming the PVC", func() error {
		if backupHasEvent(e2eNS, backup, "PlacementFailed", backup) {
			return nil
		}
		return fmt.Errorf("no PlacementFailed event naming PVC %s", backup)
	})
	if n := countBackupJobs(t, e2eNS, backup); n != 0 {
		t.Fatalf("backup %s has %d job(s), want none with an unbound PVC", backup, n)
	}
	if b := getBackup(t, e2eNS, backup); b.Status.Phase != pbsv1.BackupPhaseScheduled {
		t.Fatalf("backup %s phase %s drifted from Scheduled", backup, b.Status.Phase)
	}
}

// Scenario 7: a repo whose tokenSecret is corrupted AFTER it went Ready makes
// the backup Job run and fail with a client auth error; the PBSBackup ends
// Failed with an event. (The repo controller only re-pings on its 5m requeue,
// so the corrupted-secret window is minutes wide.)
func TestJobFailure(t *testing.T) {
	requireEnv(t)
	ensureE2ENS(t)
	name := fmt.Sprintf("e2e-badtoken-%d", runID)

	// Scratch secret cloned from the live repo credentials.
	src := getSecret(t, "default", repoSecret)
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e2eNS, Labels: map[string]string{suiteLabel: "true"}},
		Data:       src.Data,
	}
	if err := k8s.Create(ctx, sec); err != nil {
		t.Fatalf("create secret %s/%s: %v", e2eNS, name, err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, sec) })

	spec := scratchRepoSpec()
	spec.SecretRef.Name = name
	spec.SecretRef.Namespace = e2eNS
	newRepo(t, name, spec)
	waitForRepoCond(t, name, metav1.ConditionTrue, "Reachable", 90*time.Second)

	// Corrupt the token the Job will authenticate with. The repo controller
	// does not watch secrets, so Ready stays True until its 5m requeue —
	// the backup launches inside that window and its Job fails auth.
	before := sec.DeepCopy()
	sec.Data["tokenSecret"] = []byte("e2e-corrupted-token-secret")
	if err := k8s.Patch(ctx, sec, client.MergeFrom(before)); err != nil {
		t.Fatalf("corrupt secret: %v", err)
	}

	backup := fmt.Sprintf("pg-e2e-badtoken-%d", runID)
	newBackup(t, pgNS, backup, name, nil)
	b := waitForBackupTerminal(t, pgNS, backup, 3*time.Minute)
	if b.Status.Phase != pbsv1.BackupPhaseFailed {
		t.Fatalf("backup %s phase %s, want Failed", backup, b.Status.Phase)
	}
	jobName := backup + "-" + pgNode
	requireJobExists(t, pgNS, jobName) // the Job ran and failed
	eventually(t, 30*time.Second, "Failed event on backup", func() error {
		if backupHasEvent(pgNS, backup, "Failed", jobName) {
			return nil
		}
		return fmt.Errorf("no Failed event naming job %s", jobName)
	})
}

// Scenario 8: reconcile churn (annotation touched twice while the backup is
// in flight) still leaves exactly one Job per node.
func TestNoJobDuplication(t *testing.T) {
	requireEnv(t)
	name := fmt.Sprintf("pg-e2e-churn-%d", runID)
	newBackup(t, pgNS, name, repoName, nil)

	jobName := name + "-" + pgNode
	requireJobPinned(t, pgNS, jobName, pgNode) // job exists → churn now

	obj := getBackup(t, pgNS, name)
	for i, v := range []string{"1", "2"} {
		before := obj.DeepCopy()
		if obj.Annotations == nil {
			obj.Annotations = map[string]string{}
		}
		obj.Annotations["e2e.sharifmind.ir/churn"] = v
		if err := k8s.Patch(ctx, &obj, client.MergeFrom(before)); err != nil {
			t.Fatalf("churn patch %d: %v", i, err)
		}
	}

	assertHappyBackup(t, pgNS, name, pgNode, false)
	if n := countBackupJobs(t, pgNS, name); n != 1 {
		t.Fatalf("backup %s has %d job(s) after churn, want exactly 1", name, n)
	}
}

// Scenario 9: after a backup completes, deleting its Job must not resurrect
// it — no new Job within a bounded window and the phase stays Completed.
func TestTerminalNoResurrection(t *testing.T) {
	requireEnv(t)
	name := fmt.Sprintf("pg-e2e-term-%d", runID)
	newBackup(t, pgNS, name, repoName, nil)
	assertHappyBackup(t, pgNS, name, pgNode, false)

	jobName := name + "-" + pgNode
	if err := k8s.Delete(ctx, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: pgNS, Name: jobName}}); err != nil {
		t.Fatalf("delete job %s: %v", jobName, err)
	}

	// Deletion is async: first wait for the Job to be really gone...
	eventually(t, 60*time.Second, "job "+jobName+" to disappear", func() error {
		if n := countBackupJobs(t, pgNS, name); n != 0 {
			return fmt.Errorf("%d job(s) still present", n)
		}
		return nil
	})
	// ...then hold the line: no resurrection, phase stays terminal.
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if n := countBackupJobs(t, pgNS, name); n != 0 {
			t.Fatalf("job %s resurrected (count %d) after deletion", jobName, n)
		}
		if b := getBackup(t, pgNS, name); b.Status.Phase != pbsv1.BackupPhaseCompleted {
			t.Fatalf("backup %s phase drifted to %s", name, b.Status.Phase)
		}
		time.Sleep(5 * time.Second)
	}
}

// Scenario 10 (M3): a PBSSchedule `*/1 * * * *` in the pg namespace fires at
// least twice (each fire a Completed backup templated with Notes); one manual
// broken backup (S7 pattern) then flips errors_total; a metrics scrape pod
// asserts pbs_backup_errors_total{namespace="pg"} >= 1 and
// pbs_backup_last_success_timestamp_seconds{namespace="pg"} > 0.
func TestScheduledBackupsAndMetrics(t *testing.T) {
	requireEnv(t)
	name := fmt.Sprintf("pg-sched-%d", runID)
	notes := fmt.Sprintf(`{"e2e":"sched","run":%d}`, runID)
	newSchedule(t, pgNS, name, repoName, "*/1 * * * *", pbsv1.PBSBackupTemplate{Notes: notes})

	// ≤150s: at least two <schedule>-* backups reach Completed, Notes intact.
	var completed pbsv1.PBSBackup
	eventually(t, 150*time.Second, "two scheduled backups to reach Completed", func() error {
		list := &pbsv1.PBSBackupList{}
		if err := k8s.List(ctx, list, client.InNamespace(pgNS), client.MatchingLabels{"pbsschedule": name}); err != nil {
			return err
		}
		n := 0
		for i := range list.Items {
			b := list.Items[i]
			if b.Status.Phase == pbsv1.BackupPhaseCompleted {
				if b.Spec.Notes != notes {
					t.Fatalf("scheduled backup %s notes %q, want %q", b.Name, b.Spec.Notes, notes)
				}
				n++
				if b.CreationTimestamp.After(completed.CreationTimestamp.Time) {
					completed = b
				}
			}
		}
		if n < 2 {
			return fmt.Errorf("%d of 2 scheduled backups Completed", n)
		}
		return nil
	})

	// The backup's Job carried --notes into the agent argv (full chain proof
	// short of PBS itself: schedule → backup → job).
	job := &batchv1.Job{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: pgNS, Name: completed.Name + "-" + pgNode}, job); err != nil {
		t.Fatalf("get job of scheduled backup %s: %v", completed.Name, err)
	}
	cmd := job.Spec.Template.Spec.Containers[0].Command
	if !containsPair(cmd, "--notes", notes) {
		t.Fatalf("job %s command %q lacks --notes %s", job.Name, cmd, notes)
	}

	// 12b: the notes really landed on PBS. `snapshot list` JSON omits notes
	// on PBS 4.2.5 (live-verified), so read them back from the completed
	// backup's own snapshot ref directly.
	if completed.Status.SnapshotRef == "" {
		t.Fatalf("completed backup %s has no snapshotRef", completed.Name)
	}
	out := runClientJob(t, "notes", "proxmox-backup-client snapshot notes show "+
		completed.Status.SnapshotRef+" --ns "+liveRepo.Spec.Namespace)
	if strings.TrimSpace(out) != notes {
		t.Fatalf("snapshot %s notes %q, want %q", completed.Status.SnapshotRef, out, notes)
	}

	// One manual broken backup (S7 pattern): Ready repo whose token corrupts
	// after the health check → the Job runs, fails auth, backup goes Failed.
	broken := fmt.Sprintf("e2e-m3-badtoken-%d", runID)
	ensureE2ENS(t)
	src := getSecret(t, "default", repoSecret)
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: broken, Namespace: e2eNS, Labels: map[string]string{suiteLabel: "true"}},
		Data:       src.Data,
	}
	if err := k8s.Create(ctx, sec); err != nil {
		t.Fatalf("create secret %s/%s: %v", e2eNS, broken, err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, sec) })
	spec := scratchRepoSpec()
	spec.SecretRef.Name = broken
	spec.SecretRef.Namespace = e2eNS
	newRepo(t, broken, spec)
	waitForRepoCond(t, broken, metav1.ConditionTrue, "Reachable", 90*time.Second)
	before := sec.DeepCopy()
	sec.Data["tokenSecret"] = []byte("e2e-corrupted-token-secret")
	if err := k8s.Patch(ctx, sec, client.MergeFrom(before)); err != nil {
		t.Fatalf("corrupt secret: %v", err)
	}
	newBackup(t, pgNS, "pg-"+broken, broken, nil)
	failed := waitForBackupTerminal(t, pgNS, "pg-"+broken, 3*time.Minute)
	if failed.Status.Phase != pbsv1.BackupPhaseFailed {
		t.Fatalf("broken backup phase %s, want Failed", failed.Status.Phase)
	}

	// Scrape the manager metrics (bearer-authed pod) and assert both series.
	body := scrapeManagerMetrics(t)
	errs, ok := metricLine(body, "pbs_backup_errors_total", pgNS)
	if !ok || errs < 1 {
		t.Fatalf("pbs_backup_errors_total{namespace=%q} = %v (present: %v), want >= 1\nbody head:\n%s", pgNS, errs, ok, head(body, 400))
	}
	last, ok := metricLine(body, "pbs_backup_last_success_timestamp_seconds", pgNS)
	if !ok || last <= 0 {
		t.Fatalf("pbs_backup_last_success_timestamp_seconds{namespace=%q} = %v (present: %v), want > 0\nbody head:\n%s", pgNS, last, ok, head(body, 400))
	}
}

// Scenario 11 (M4): the pg fixture pod carries pre-hook annotations (fixture-
// applied via STS rolling update); the pg_dump hook runs inside the real pg
// pod before Jobs are created and the backup still Completes, with no
// HookFailed/HookInvalid event. (The dump's live spot-restore on the PBS VM is
// the controller's post-merge acceptance, outside this suite.)
func TestBackupWithPreHook(t *testing.T) {
	requireEnv(t)

	// The live pg pod must carry the hook contract, else this proves nothing.
	pods := &corev1.PodList{}
	if err := k8s.List(ctx, pods, client.InNamespace(pgNS), client.MatchingLabels{"app": "pg"}); err != nil || len(pods.Items) == 0 {
		t.Fatalf("no pg pod in %s (fixture not applied?): %v", pgNS, err)
	}
	ann := pods.Items[0].Annotations
	if ann[hooks.AnnotationContainer] != "postgres" || ann[hooks.AnnotationCommand] == "" {
		t.Fatalf("pg pod lacks pre-hook annotations (apply testenv/fixtures/pg.yaml first): %v", ann)
	}

	name := fmt.Sprintf("pg-e2e-hook-%d", runID)
	newBackup(t, pgNS, name, repoName, nil)
	assertHappyBackup(t, pgNS, name, pgNode, false)
	for _, reason := range []string{"HookFailed", "HookInvalid"} {
		if backupHasEvent(pgNS, name, reason, "") {
			t.Fatalf("backup %s completed but has a %s event", name, reason)
		}
	}
}

// containsPair reports whether flag,value appears adjacently in argv.
func containsPair(argv []string, flag, value string) bool {
	for i := 0; i+1 < len(argv); i++ {
		if argv[i] == flag && argv[i+1] == value {
			return true
		}
	}
	return false
}

// head returns the first n bytes of s for failure diagnostics.
func head(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// Scenario 12 (M5 GATE): byte-identical restore. A fresh pg backup (whose
// namespace carries a Probe CR) restores into a scratch target namespace:
// API objects reappear (Probe CR present), the PVC rebinds via the WFFC
// volume Job, pg comes Running, and a checksum Job proves the volume data
// byte-identical (probe.sha256 written at seed time, verified from the
// restored volume; dump.pgc present). A SECOND restore into the SAME
// namespace also Completes — the empty-first rule and re-apply over live
// state. Target ns is per-run: e2e reruns never fight stale restores.
func TestRestoreIntoFreshNamespace(t *testing.T) {
	requireEnv(t)

	// CRD coverage: a Probe CR rides the backup's api.yaml.
	probeName := fmt.Sprintf("e2e-probe-%d", runID)
	probeMsg := fmt.Sprintf("restore-me-%d", runID)
	newProbeCR(t, pgNS, probeName, probeMsg)

	// Snapshot source: one fresh, hooked pg backup (dump.pgc refreshed).
	backup := fmt.Sprintf("pg-e2e-restore-%d", runID)
	newBackup(t, pgNS, backup, repoName, nil)
	b := assertHappyBackup(t, pgNS, backup, pgNode, true) // gate-quality source

	// Target namespace; the PBSRestore lives in it (ownerRefs, CM, mounts).
	target := fmt.Sprintf("restore-test-%d", runID)
	nsObj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: target, Labels: map[string]string{suiteLabel: "true"}}}
	if err := k8s.Create(ctx, nsObj); err != nil {
		t.Fatalf("create namespace %s: %v", target, err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, nsObj) })

	restore1 := fmt.Sprintf("e2e-restore-%d", runID)
	newRestore(t, target, restore1, b.Status.SnapshotRef)
	if r := waitForRestoreTerminal(t, target, restore1, 3*time.Minute); r.Status.Phase != pbsv1.RestorePhaseCompleted {
		t.Fatalf("restore %s phase %s, want Completed (Ready: %s)", restore1, r.Status.Phase, restoreHoldMessage(r))
	}

	// API objects: the Probe CR is back with its payload.
	if got := probeCRMessage(t, target, probeName); got != probeMsg {
		t.Fatalf("Probe %s/%s message %q, want %q", target, probeName, got, probeMsg)
	}
	// PVC: re-created by the pre apply, bound by the volume Job's node pin.
	eventually(t, 60*time.Second, "PVC data-pg-0 to be Bound", func() error {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := k8s.Get(ctx, types.NamespacedName{Namespace: target, Name: "data-pg-0"}, pvc); err != nil {
			return err
		}
		if pvc.Status.Phase != corev1.ClaimBound {
			return fmt.Errorf("PVC phase %s", pvc.Status.Phase)
		}
		return nil
	})
	// Workload: the pg StatefulSet comes back and its pod is ready.
	eventually(t, 120*time.Second, "restored pg StatefulSet ready", func() error {
		sts := &appsv1.StatefulSet{}
		if err := k8s.Get(ctx, types.NamespacedName{Namespace: target, Name: "pg"}, sts); err != nil {
			return err
		}
		if sts.Status.ReadyReplicas != 1 {
			return fmt.Errorf("ready replicas %d/%d", sts.Status.ReadyReplicas, *sts.Spec.Replicas)
		}
		return nil
	})

	// Byte-identical gate: verify probe.bin against probe.sha256 FROM INSIDE
	// the restored volume, and dump.pgc present.
	checksum := fmt.Sprintf("e2e-checksum-%d", runID)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: checksum, Namespace: target,
			Labels: map[string]string{suiteLabel: "true", "e2e-verify": checksum},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptr.To[int32](0),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"e2e-verify": checksum}},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					// Same node as the PVC (RWO local-path).
					NodeSelector: map[string]string{"kubernetes.io/hostname": pgNode},
					Containers: []corev1.Container{{
						Name:    "checksum",
						Image:   "busybox:1.37",
						Command: []string{"sh", "-c", "cd /data && sha256sum -c probe.sha256 && test -s dump.pgc"},
						VolumeMounts: []corev1.VolumeMount{{
							Name: "data", MountPath: "/data",
						}},
					}},
					Volumes: []corev1.Volume{{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data-pg-0"},
						},
					}},
				},
			},
		},
	}
	if err := k8s.Create(ctx, job); err != nil {
		t.Fatalf("create checksum job: %v", err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, job) })
	waitJobComplete(t, target, checksum, 2*time.Minute)

	// Double restore into the SAME namespace → also Completed (empty-first
	// over live data, idempotent re-apply of API objects).
	restore2 := restore1 + "-again"
	newRestore(t, target, restore2, b.Status.SnapshotRef)
	if r := waitForRestoreTerminal(t, target, restore2, 3*time.Minute); r.Status.Phase != pbsv1.RestorePhaseCompleted {
		t.Fatalf("second restore %s phase %s, want Completed (Ready: %s)", restore2, r.Status.Phase, restoreHoldMessage(r))
	}
}
