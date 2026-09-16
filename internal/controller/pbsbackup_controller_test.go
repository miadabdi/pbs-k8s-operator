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

package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pbsv1 "gitlab.sharifmind.ir/miad/pbs-operator/api/v1"
	backuplib "gitlab.sharifmind.ir/miad/pbs-operator/internal/backup"
	"gitlab.sharifmind.ir/miad/pbs-operator/internal/hooks"
)

// ---- fixtures -----------------------------------------------------------

// makeReadyRepo creates a cluster-scoped PBSRepo already marked Ready, with
// its credentials secret (all 8 contract keys) in the repo's namespace.
func makeReadyRepo(ctx context.Context, name string) *pbsv1.PBSRepo {
	repo := &pbsv1.PBSRepo{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: pbsv1.PBSRepoSpec{
			Host:        "192.0.2.10",
			Datastore:   "store",
			Namespace:   "ns",
			Fingerprint: "AA:BB:CC",
			Port:        8007,
			SecretRef:   pbsv1.NamespacedSecretRef{Name: name + "-creds", Namespace: "default"},
		},
	}
	Expect(k8sClient.Create(ctx, repo)).To(Succeed())
	createSecret(ctx, repo.Spec.SecretRef.Name, "default", map[string][]byte{
		"host":        []byte("192.0.2.10"),
		"port":        []byte("8007"),
		"datastore":   []byte("store"),
		"namespace":   []byte("ns"),
		"tokenID":     []byte("op@pbs!producer"),
		"tokenSecret": []byte("prod-secret"),
		"fingerprint": []byte("AA:BB:CC"),
		"keyfile":     []byte("-----BEGIN PRIVATE KEY-----"),
	})
	repo.Status.Conditions = []metav1.Condition{{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "Reachable",
		LastTransitionTime: metav1.Now(),
	}}
	Expect(k8sClient.Status().Update(ctx, repo)).To(Succeed())
	return repo
}

// repoSecretCopy fetches the controller-copied repo secret from the backup's
// namespace (empty string when absent).
func repoSecretCopy(ctx context.Context, ns, name string) *corev1.Secret {
	s := &corev1.Secret{}
	err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, s)
	if apierrors.IsNotFound(err) {
		return nil
	}
	Expect(err).NotTo(HaveOccurred())
	return s
}

// makeLocalPV creates a PV with local-path style hostname node affinity.
func makeLocalPV(ctx context.Context, name, node string) *corev1.PersistentVolume {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:    corev1.ResourceList{"storage": resource.MustParse("1Gi")},
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				Local: &corev1.LocalVolumeSource{Path: "/mnt/" + name},
			},
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "kubernetes.io/hostname",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{node},
					}},
				}}},
			},
		},
	}
	Expect(k8sClient.Create(ctx, pv)).To(Succeed())
	return pv
}

// makePlainPV creates a PV with no node affinity (placement must come from a
// mounting pod). hostPath, not local: local volumes demand node affinity.
func makePlainPV(ctx context.Context, name string) *corev1.PersistentVolume {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:    corev1.ResourceList{"storage": resource.MustParse("1Gi")},
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/mnt/" + name},
			},
		},
	}
	Expect(k8sClient.Create(ctx, pv)).To(Succeed())
	return pv
}

// makePVC creates a PVC; pvName empty leaves it unbound.
func makePVC(ctx context.Context, name, ns, pvName string) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{"storage": resource.MustParse("1Gi")},
			},
			VolumeName: pvName,
		},
	}
	Expect(k8sClient.Create(ctx, pvc)).To(Succeed())
	return pvc
}

// makeMountingPod creates a pod bound to node that mounts pvc (placement fallback).
func makeMountingPod(ctx context.Context, name, ns, node, pvcName string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1.PodSpec{
			NodeName:      node,
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{{Name: "app", Image: "app:dev"}},
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
				},
			}},
		},
	}
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())
	return pod
}

// makeBackup creates a PBSBackup targeting repo with an explicit PVC list.
func makeBackup(ctx context.Context, name, ns, repo string, pvcs ...string) *pbsv1.PBSBackup {
	b := &pbsv1.PBSBackup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       pbsv1.PBSBackupSpec{RepoRef: repo, PVCs: pvcs},
	}
	Expect(k8sClient.Create(ctx, b)).To(Succeed())
	return b
}

func fetchBackup(ctx context.Context, ns, name string) *pbsv1.PBSBackup {
	b := &pbsv1.PBSBackup{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, b)).To(Succeed())
	return b
}

func reconcileBackup(ctx context.Context, ns, name string, rec record.EventRecorder) reconcile.Result {
	return reconcileBackupWith(ctx, ns, name, rec, nil)
}

// reconcileBackupWith runs one backup reconcile carrying optional metrics.
// The hook executor is the real SPDY one (envtest has no kubelet: an exec
// against it must fail — the M4 failure-path contract).
func reconcileBackupWith(ctx context.Context, ns, name string, rec record.EventRecorder, m *BackupMetrics) reconcile.Result {
	r := &PBSBackupReconciler{
		Client:       k8sClient,
		Scheme:       k8sClient.Scheme(),
		Recorder:     rec,
		AgentImage:   "pbs-agent:dev",
		Serializer:   serializer,
		Metrics:      m,
		HookExecutor: hooks.NewExecutor(cfg),
	}
	res, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: ns, Name: name},
	})
	Expect(err).NotTo(HaveOccurred())
	return res
}

// reconcileBackupHooks is reconcileBackupWith with an injectable hook
// executor (the seam for success/ordering specs).
func reconcileBackupHooks(ctx context.Context, ns, name string, rec record.EventRecorder, exec hooks.Executor) reconcile.Result {
	r := &PBSBackupReconciler{
		Client:       k8sClient,
		Scheme:       k8sClient.Scheme(),
		Recorder:     rec,
		AgentImage:   "pbs-agent:dev",
		Serializer:   serializer,
		HookExecutor: exec,
	}
	res, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: ns, Name: name},
	})
	Expect(err).NotTo(HaveOccurred())
	return res
}

// makeNamespace creates an empty namespace (serialization specs need one with
// known contents; "default" accumulates fixtures from every earlier spec).
func makeNamespace(ctx context.Context, name string) {
	err := k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}})
	Expect(err).NotTo(HaveOccurred(), "create namespace "+name)
}

// makeNode creates a Node object (envtest ships none) so node-pinned Jobs
// have something to be pinned to.
func makeNode(ctx context.Context, name string) {
	err := k8sClient.Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}})
	Expect(err).NotTo(HaveOccurred(), "create node "+name)
}

// makeSeedConfigMap seeds a ConfigMap so serialization has known content.
func makeSeedConfigMap(ctx context.Context, name, ns string) {
	Expect(k8sClient.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data:       map[string]string{"seed": "true"},
	})).To(Succeed())
}

// backupJobs lists the Jobs controller-owned by the backup (any namespace
// leftovers from other specs are filtered out by ownerRef UID).
func backupJobs(ctx context.Context, backup *pbsv1.PBSBackup) []batchv1.Job {
	all := &batchv1.JobList{}
	Expect(k8sClient.List(ctx, all, client.InNamespace(backup.Namespace))).To(Succeed())
	var out []batchv1.Job
	for _, j := range all.Items {
		for _, ref := range j.OwnerReferences {
			if ref.UID == backup.UID {
				out = append(out, j)
			}
		}
	}
	return out
}

// setJobCondition stamps a terminal condition onto a Job's status the way the
// job controller would (k8s validation wants startTime/completionTime and the
// SuccessCriteriaMet/FailureTarget precursor conditions).
func setJobCondition(ctx context.Context, ns, name string, cond batchv1.JobCondition) {
	job := &batchv1.Job{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, job)).To(Succeed())
	now := metav1.Now()
	job.Status.StartTime = &now
	job.Status.Conditions = append(job.Status.Conditions, cond)
	if cond.Type == batchv1.JobComplete {
		job.Status.CompletionTime = &now
		job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{
			Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue,
		})
	}
	if cond.Type == batchv1.JobFailed {
		job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{
			Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue,
		})
	}
	Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
}

// deleteJob removes a Job outright: envtest runs no garbage collector, so the
// default (orphan) propagation would leave it lingering with a deletionTimestamp.
func deleteJob(ctx context.Context, ns, name string) {
	bg := metav1.DeletePropagationBackground
	Expect(k8sClient.Delete(ctx, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}},
		&client.DeleteOptions{PropagationPolicy: &bg})).To(Succeed())
}

// makeResultPod creates the (already finished) pod a Job would leave behind,
// carrying the agent's /dev/termination-log JSON in the container status.
func makeResultPod(ctx context.Context, ns, jobName, message string) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName + "-pod",
			Namespace: ns,
			Labels:    map[string]string{"pbsbackup": jobName},
		},
		Spec: corev1.PodSpec{
			Hostname:      jobName,
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{{Name: "backup", Image: "pbs-agent:dev"}},
		},
	}
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())
	pod.Status.Phase = corev1.PodSucceeded
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "backup",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 0, Message: message,
		}},
	}}
	Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
}

// envOf returns the Job container env map for contract spot-checks.
func envOf(job *batchv1.Job) map[string]corev1.EnvVar {
	out := map[string]corev1.EnvVar{}
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		out[e.Name] = e
	}
	return out
}

// ---- unit checks --------------------------------------------------------

func TestJobName(t *testing.T) {
	if got := jobName("bk", "node-a"); got != "bk-node-a" {
		t.Fatalf("short name = %q", got)
	}
	// Overlong names: capped at 63 chars, deterministic, and distinct even
	// when two nodes share the truncated prefix.
	long := strings.Repeat("b", 80)
	a, b := jobName(long, "node-a"), jobName(long, "node-b")
	for _, n := range []string{a, b} {
		if len(n) != 63 {
			t.Fatalf("len(%q) = %d", n, len(n))
		}
		if strings.HasPrefix(n, "-") || strings.HasSuffix(n, "-") {
			t.Fatalf("%q not a DNS-1123 label", n)
		}
	}
	if a == b {
		t.Fatalf("hash suffix collision: %q", a)
	}
	if jobName(long, "node-a") != a {
		t.Fatal("not deterministic")
	}
}

// ---- scenarios ----------------------------------------------------------

var _ = Describe("PBSBackup Controller", func() {
	ctx := context.Background()

	It("1a. repo missing → Scheduled, warning event, requeue 1m, zero jobs", func() {
		makeBackup(ctx, "bk-1a", "default", "no-such-repo", "whatever-pvc")

		rec := record.NewFakeRecorder(16)
		res := reconcileBackup(ctx, "default", "bk-1a", rec)

		Expect(res.RequeueAfter).To(Equal(time.Minute))
		b := fetchBackup(ctx, "default", "bk-1a")
		Expect(b.Status.Phase).To(Equal(pbsv1.BackupPhaseScheduled))
		cond := meta.FindStatusCondition(b.Status.Conditions, "Ready")
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("RepoNotReady"))
		events := drainEvents(rec)
		Expect(events).To(HaveLen(1))
		Expect(events[0]).To(ContainSubstring("no-such-repo"))
		Expect(backupJobs(ctx, b)).To(BeEmpty())
	})

	It("1b. repo present but not Ready → same hold", func() {
		repo := makeReadyRepo(ctx, "bk-1b-repo")
		repo.Status.Conditions[0].Status = metav1.ConditionFalse
		Expect(k8sClient.Status().Update(ctx, repo)).To(Succeed())
		makeBackup(ctx, "bk-1b", "default", "bk-1b-repo", "whatever-pvc")

		rec := record.NewFakeRecorder(16)
		res := reconcileBackup(ctx, "default", "bk-1b", rec)

		Expect(res.RequeueAfter).To(Equal(time.Minute))
		Expect(fetchBackup(ctx, "default", "bk-1b").Status.Phase).To(Equal(pbsv1.BackupPhaseScheduled))
		Expect(drainEvents(rec)).To(HaveLen(1))
	})

	It("2. one node → exactly one correctly-pinned Job, phase Running", func() {
		repo := makeReadyRepo(ctx, "bk-2-repo")
		makeLocalPV(ctx, "bk-2-pv", "node-a")
		makePVC(ctx, "bk-2-data", "default", "bk-2-pv")
		makeBackup(ctx, "bk-2", "default", repo.Name, "bk-2-data")

		rec := record.NewFakeRecorder(32)
		res := reconcileBackup(ctx, "default", "bk-2", rec)

		Expect(res.RequeueAfter).To(Equal(time.Minute)) // running poll fallback; Owns() drives the rest
		b := fetchBackup(ctx, "default", "bk-2")
		Expect(b.Status.Phase).To(Equal(pbsv1.BackupPhaseRunning))
		Expect(b.Status.StartedAt).NotTo(BeNil())
		Expect(b.Status.Jobs).To(Equal([]pbsv1.BackupJobStatus{
			{Node: "node-a", Job: "bk-2-node-a", State: "active"},
		}))

		jobs := backupJobs(ctx, b)
		Expect(jobs).To(HaveLen(1))
		job := &jobs[0]
		Expect(job.Name).To(Equal("bk-2-node-a"))
		Expect(len(job.Name)).To(BeNumerically("<=", 63))

		// Controller-owned ownerRef.
		refs := job.GetOwnerReferences()
		Expect(refs).To(HaveLen(1))
		Expect(refs[0].UID).To(Equal(b.UID))
		Expect(refs[0].Controller).NotTo(BeNil())
		Expect(*refs[0].Controller).To(BeTrue())

		// Node pinning + backup-id hostname.
		sel := job.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		Expect(sel.NodeSelectorTerms[0].MatchExpressions[0].Key).To(Equal("kubernetes.io/hostname"))
		Expect(sel.NodeSelectorTerms[0].MatchExpressions[0].Values).To(Equal([]string{"node-a"}))
		Expect(job.Spec.Template.Spec.Hostname).To(Equal("bk-2-node-a"))

		// The repo secret is copied into the backup's namespace (secrets are
		// namespace-scoped; the Job's secretKeyRef must resolve locally).
		copyName := "pbsrepo-" + repo.Name
		copied := repoSecretCopy(ctx, "default", copyName)
		Expect(copied).NotTo(BeNil())
		cRefs := copied.GetOwnerReferences()
		Expect(cRefs).To(HaveLen(1))
		Expect(cRefs[0].UID).To(Equal(b.UID))
		Expect(cRefs[0].Controller).NotTo(BeNil())
		Expect(*cRefs[0].Controller).To(BeTrue())
		for k, v := range map[string]string{
			"host": "192.0.2.10", "port": "8007", "datastore": "store",
			"namespace": "ns", "tokenID": "op@pbs!producer",
			"tokenSecret": "prod-secret", "fingerprint": "AA:BB:CC",
			"keyfile": "-----BEGIN PRIVATE KEY-----",
		} {
			Expect(string(copied.Data[k])).To(Equal(v), "contract key "+k)
		}

		// BuildBackupJob contract spot-checks: env from the copied repo secret, PVC mount.
		env := envOf(job)
		Expect(env).To(HaveLen(8))
		Expect(env["PBS_HOST"].ValueFrom.SecretKeyRef.Name).To(Equal(copyName))
		Expect(env["PBS_HOST"].ValueFrom.SecretKeyRef.Key).To(Equal("host"))
		Expect(env["PBS_TOKEN_SECRET"].ValueFrom.SecretKeyRef.Key).To(Equal("tokenSecret"))
		c := job.Spec.Template.Spec.Containers[0]
		Expect(c.Image).To(Equal("pbs-agent:dev"))
		// PVC mount + the api-staging mount (M2: every backup serializes).
		Expect(c.VolumeMounts).To(HaveLen(2))
		Expect(c.VolumeMounts[0].MountPath).To(Equal("/backup/bk-2-data"))
		Expect(c.VolumeMounts[0].ReadOnly).To(BeTrue())
		Expect(c.VolumeMounts[1].MountPath).To(Equal("/staging/api"))
		Expect(c.VolumeMounts[1].ReadOnly).To(BeTrue())

		cond := meta.FindStatusCondition(b.Status.Conditions, "Ready")
		Expect(cond.Reason).To(Equal("Running"))
		Expect(cond.ObservedGeneration).To(Equal(b.Generation))
		Expect(drainEvents(rec)).To(HaveLen(1))
	})

	It("3. all Jobs Complete + termination-log JSON → Completed, no requeue", func() {
		repo := makeReadyRepo(ctx, "bk-3-repo")
		makeLocalPV(ctx, "bk-3-pv", "node-a")
		makePVC(ctx, "bk-3-data", "default", "bk-3-pv")
		makeBackup(ctx, "bk-3", "default", repo.Name, "bk-3-data")

		rec := record.NewFakeRecorder(32)
		reconcileBackup(ctx, "default", "bk-3", rec)

		setJobCondition(ctx, "default", "bk-3-node-a", batchv1.JobCondition{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
		})
		makeResultPod(ctx, "default", "bk-3-node-a",
			`{"snapshotRef":"host/bk-3-node-a/2026-09-16T10:00:00Z","bytes":123}`)

		res := reconcileBackup(ctx, "default", "bk-3", rec)

		Expect(res.RequeueAfter).To(BeZero())
		b := fetchBackup(ctx, "default", "bk-3")
		Expect(b.Status.Phase).To(Equal(pbsv1.BackupPhaseCompleted))
		Expect(b.Status.SnapshotRef).To(Equal("host/bk-3-node-a/2026-09-16T10:00:00Z"))
		Expect(b.Status.Bytes).To(Equal(int64(123)))
		Expect(b.Status.CompletedAt).NotTo(BeNil())
		Expect(b.Status.Jobs[0].State).To(Equal("ok:host/bk-3-node-a/2026-09-16T10:00:00Z"))
		events := drainEvents(rec)
		Expect(events).To(HaveLen(2)) // Running transition, then Completed
		Expect(events[1]).To(ContainSubstring("Completed"))
	})

	It("4. Job Failed condition → phase Failed + failure event, no requeue", func() {
		repo := makeReadyRepo(ctx, "bk-4-repo")
		makeLocalPV(ctx, "bk-4-pv", "node-a")
		makePVC(ctx, "bk-4-data", "default", "bk-4-pv")
		makeBackup(ctx, "bk-4", "default", repo.Name, "bk-4-data")

		rec := record.NewFakeRecorder(32)
		reconcileBackup(ctx, "default", "bk-4", rec)

		setJobCondition(ctx, "default", "bk-4-node-a", batchv1.JobCondition{
			Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded",
		})

		res := reconcileBackup(ctx, "default", "bk-4", rec)

		Expect(res.RequeueAfter).To(BeZero())
		b := fetchBackup(ctx, "default", "bk-4")
		Expect(b.Status.Phase).To(Equal(pbsv1.BackupPhaseFailed))
		Expect(b.Status.CompletedAt).NotTo(BeNil())
		Expect(b.Status.Jobs[0].State).To(Equal("failed"))
		events := drainEvents(rec)
		Expect(events).To(HaveLen(2))
		Expect(events[1]).To(ContainSubstring("bk-4-node-a"))
		Expect(events[1]).To(ContainSubstring("node-a"))
	})

	It("5. PVCs on two nodes → two Jobs, one per node, correctly grouped", func() {
		repo := makeReadyRepo(ctx, "bk-5-repo")
		makeLocalPV(ctx, "bk-5-pv-a", "node-a")
		makePVC(ctx, "bk-5-data-a", "default", "bk-5-pv-a")
		// Second PVC placed via the pod-list fallback (PV without affinity).
		makePlainPV(ctx, "bk-5-pv-b")
		makePVC(ctx, "bk-5-data-b", "default", "bk-5-pv-b")
		makeMountingPod(ctx, "bk-5-app", "default", "node-b", "bk-5-data-b")
		makeBackup(ctx, "bk-5", "default", repo.Name, "bk-5-data-a", "bk-5-data-b")

		res := reconcileBackup(ctx, "default", "bk-5", record.NewFakeRecorder(32))

		Expect(res.RequeueAfter).To(Equal(time.Minute))
		b := fetchBackup(ctx, "default", "bk-5")
		Expect(b.Status.Phase).To(Equal(pbsv1.BackupPhaseRunning))
		Expect(b.Status.Jobs).To(ConsistOf(
			pbsv1.BackupJobStatus{Node: "node-a", Job: "bk-5-node-a", State: "active"},
			pbsv1.BackupJobStatus{Node: "node-b", Job: "bk-5-node-b", State: "active"},
		))

		jobs := backupJobs(ctx, b)
		Expect(jobs).To(HaveLen(2))
		byName := map[string]*batchv1.Job{}
		for i := range jobs {
			byName[jobs[i].Name] = &jobs[i]
		}
		Expect(byName).To(HaveKey("bk-5-node-a"))
		Expect(byName).To(HaveKey("bk-5-node-b"))
		claims := func(j *batchv1.Job) []string {
			var out []string
			for _, v := range j.Spec.Template.Spec.Volumes {
				if v.PersistentVolumeClaim != nil { // skip the api-staging volume
					out = append(out, v.PersistentVolumeClaim.ClaimName)
				}
			}
			return out
		}
		Expect(claims(byName["bk-5-node-a"])).To(Equal([]string{"bk-5-data-a"}))
		Expect(claims(byName["bk-5-node-b"])).To(Equal([]string{"bk-5-data-b"}))

		// One shared copy of the repo secret; both Jobs reference it.
		Expect(repoSecretCopy(ctx, "default", "pbsrepo-"+repo.Name)).NotTo(BeNil())
		for name, j := range byName {
			Expect(envOf(j)["PBS_TOKEN_ID"].ValueFrom.SecretKeyRef.Name).To(Equal("pbsrepo-"+repo.Name), name)
		}
	})

	It("6. idempotent re-reconcile; Job deleted while running → event + requeue, no recreate", func() {
		repo := makeReadyRepo(ctx, "bk-6-repo")
		makeLocalPV(ctx, "bk-6-pv", "node-a")
		makePVC(ctx, "bk-6-data", "default", "bk-6-pv")
		makeBackup(ctx, "bk-6", "default", repo.Name, "bk-6-data")

		rec := record.NewFakeRecorder(32)
		reconcileBackup(ctx, "default", "bk-6", rec)
		drainEvents(rec)

		// Second reconcile: nothing new, no status churn, no events.
		jobUID := backupJobs(ctx, fetchBackup(ctx, "default", "bk-6"))[0].UID
		rv := fetchBackup(ctx, "default", "bk-6").ResourceVersion
		reconcileBackup(ctx, "default", "bk-6", rec)
		jobs := backupJobs(ctx, fetchBackup(ctx, "default", "bk-6"))
		Expect(jobs).To(HaveLen(1))
		Expect(jobs[0].UID).To(Equal(jobUID))
		Expect(fetchBackup(ctx, "default", "bk-6").ResourceVersion).To(Equal(rv))
		Expect(drainEvents(rec)).To(BeEmpty())

		// Job vanishes while non-terminal: warn + requeue, never recreate.
		deleteJob(ctx, "default", "bk-6-node-a")
		res := reconcileBackup(ctx, "default", "bk-6", rec)

		Expect(res.RequeueAfter).To(Equal(time.Minute))
		Expect(backupJobs(ctx, fetchBackup(ctx, "default", "bk-6"))).To(BeEmpty())
		events := drainEvents(rec)
		Expect(events).To(HaveLen(1))
		Expect(events[0]).To(ContainSubstring("bk-6-node-a"))
		b := fetchBackup(ctx, "default", "bk-6")
		Expect(b.Status.Phase).To(Equal(pbsv1.BackupPhaseRunning))
		cond := meta.FindStatusCondition(b.Status.Conditions, "Ready")
		Expect(cond.Reason).To(Equal("JobDeleted"))
	})

	It("7. terminal phase + Job deleted → full no-op", func() {
		repo := makeReadyRepo(ctx, "bk-7-repo")
		makeLocalPV(ctx, "bk-7-pv", "node-a")
		makePVC(ctx, "bk-7-data", "default", "bk-7-pv")
		makeBackup(ctx, "bk-7", "default", repo.Name, "bk-7-data")

		rec := record.NewFakeRecorder(32)
		reconcileBackup(ctx, "default", "bk-7", rec)
		setJobCondition(ctx, "default", "bk-7-node-a", batchv1.JobCondition{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
		})
		makeResultPod(ctx, "default", "bk-7-node-a",
			`{"snapshotRef":"host/bk-7-node-a/2026-09-16T11:00:00Z","bytes":7}`)
		reconcileBackup(ctx, "default", "bk-7", rec)
		drainEvents(rec)

		deleteJob(ctx, "default", "bk-7-node-a")

		rv := fetchBackup(ctx, "default", "bk-7").ResourceVersion
		res := reconcileBackup(ctx, "default", "bk-7", rec)

		Expect(res).To(Equal(reconcile.Result{}))
		Expect(fetchBackup(ctx, "default", "bk-7").ResourceVersion).To(Equal(rv))
		Expect(fetchBackup(ctx, "default", "bk-7").Status.Phase).To(Equal(pbsv1.BackupPhaseCompleted))
		Expect(backupJobs(ctx, fetchBackup(ctx, "default", "bk-7"))).To(BeEmpty())
		Expect(drainEvents(rec)).To(BeEmpty())
	})

	// M2 rule: the gate holds only when BOTH no PVCs are selected AND the
	// namespace serializes to nothing (an empty namespace has no API objects).
	It("extra. zero PVCs and nothing to serialize → stays Scheduled, event, requeue 5m", func() {
		makeNamespace(ctx, "bk-8")
		makeReadyRepo(ctx, "bk-8-repo")
		b := &pbsv1.PBSBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "bk-8", Namespace: "bk-8"},
			Spec: pbsv1.PBSBackupSpec{
				RepoRef:  "bk-8-repo",
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "no-such-app"}},
			},
		}
		Expect(k8sClient.Create(ctx, b)).To(Succeed())

		rec := record.NewFakeRecorder(16)
		res := reconcileBackup(ctx, "bk-8", "bk-8", rec)

		Expect(res.RequeueAfter).To(Equal(5 * time.Minute))
		fetched := fetchBackup(ctx, "bk-8", "bk-8")
		Expect(fetched.Status.Phase).To(Equal(pbsv1.BackupPhaseScheduled))
		Expect(drainEvents(rec)).To(HaveLen(1))
		Expect(backupJobs(ctx, fetched)).To(BeEmpty())
	})

	It("extra. unbound PVC → event naming the PVC, requeue 1m, no jobs", func() {
		makeReadyRepo(ctx, "bk-9-repo")
		makePVC(ctx, "bk-9-data", "default", "") // pending
		makeBackup(ctx, "bk-9", "default", "bk-9-repo", "bk-9-data")

		rec := record.NewFakeRecorder(16)
		res := reconcileBackup(ctx, "default", "bk-9", rec)

		Expect(res.RequeueAfter).To(Equal(time.Minute))
		b := fetchBackup(ctx, "default", "bk-9")
		Expect(b.Status.Phase).To(Equal(pbsv1.BackupPhaseScheduled))
		events := drainEvents(rec)
		Expect(events).To(HaveLen(1))
		Expect(events[0]).To(ContainSubstring("bk-9-data"))
		Expect(backupJobs(ctx, b)).To(BeEmpty())
	})

	It("extra. repo secret vanished → copy fails: event, requeue 1m, stays Scheduled, no jobs", func() {
		repo := makeReadyRepo(ctx, "bk-10-repo")
		makeBackup(ctx, "bk-10", "default", repo.Name, "whatever-pvc")
		// The repo stays Ready, but its source secret disappears.
		src := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Namespace: "default", Name: repo.Spec.SecretRef.Name,
		}, src)).To(Succeed())
		Expect(k8sClient.Delete(ctx, src)).To(Succeed())

		rec := record.NewFakeRecorder(16)
		res := reconcileBackup(ctx, "default", "bk-10", rec)

		Expect(res.RequeueAfter).To(Equal(time.Minute))
		b := fetchBackup(ctx, "default", "bk-10")
		Expect(b.Status.Phase).To(Equal(pbsv1.BackupPhaseScheduled))
		cond := meta.FindStatusCondition(b.Status.Conditions, "Ready")
		Expect(cond.Reason).To(Equal("SecretCopyFailed"))
		events := drainEvents(rec)
		Expect(events).To(HaveLen(1))
		Expect(events[0]).To(ContainSubstring(repo.Spec.SecretRef.Name))
		Expect(backupJobs(ctx, b)).To(BeEmpty())
		Expect(repoSecretCopy(ctx, "default", "pbsrepo-"+repo.Name)).To(BeNil())
	})

	// M2: API serialization lands in a controller-owned staging ConfigMap the
	// Job mounts at /staging/api (→ api.pxar).
	It("11. staging ConfigMap: api.yaml payload, ownerRef, managed label, Job mounts /staging/api", func() {
		makeNamespace(ctx, "bk-11")
		createSecret(ctx, "bk-11-seed", "bk-11", map[string][]byte{"k": []byte("seed")})
		makeSeedConfigMap(ctx, "bk-11-seed", "bk-11")
		repo := makeReadyRepo(ctx, "bk-11-repo")
		makeLocalPV(ctx, "bk-11-pv", "node-a")
		makePVC(ctx, "bk-11-data", "bk-11", "bk-11-pv")
		makeBackup(ctx, "bk-11", "bk-11", repo.Name, "bk-11-data")

		rec := record.NewFakeRecorder(32)
		reconcileBackup(ctx, "bk-11", "bk-11", rec)

		b := fetchBackup(ctx, "bk-11", "bk-11")
		staging := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "bk-11", Name: "bk-11-api"}, staging)).To(Succeed())
		api := staging.Data["api.yaml"]
		Expect(api).NotTo(BeEmpty())
		// Seeds and the PVC serialize; volatile fields and own artifacts don't.
		for _, want := range []string{"kind: Secret", "kind: ConfigMap", "kind: PersistentVolumeClaim"} {
			Expect(api).To(ContainSubstring(want))
		}
		for _, absent := range []string{
			"kind: PBSBackup", "kind: Job", "uid:", "resourceVersion:",
			"creationTimestamp:", "managedFields:", "bk-11-repo-creds", // source secret lives in default
		} {
			Expect(api).NotTo(ContainSubstring(absent))
		}
		// OwnerRef → GC with the CR; managed label → excluded from future runs.
		refs := staging.GetOwnerReferences()
		Expect(refs).To(HaveLen(1))
		Expect(refs[0].UID).To(Equal(b.UID))
		Expect(refs[0].Controller).NotTo(BeNil())
		Expect(*refs[0].Controller).To(BeTrue())
		Expect(staging.Labels[backuplib.ManagedLabel]).To(Equal("true"))
		// The copied repo secret is managed-labeled too: present, but not serialized.
		copied := repoSecretCopy(ctx, "bk-11", "pbsrepo-"+repo.Name)
		Expect(copied).NotTo(BeNil())
		Expect(copied.Labels[backuplib.ManagedLabel]).To(Equal("true"))
		Expect(api).NotTo(ContainSubstring("pbsrepo-" + repo.Name))

		// The Job mounts the ConfigMap at /staging/api and passes --api.
		jobs := backupJobs(ctx, b)
		Expect(jobs).To(HaveLen(1))
		job := &jobs[0]
		var vol *corev1.Volume
		for i := range job.Spec.Template.Spec.Volumes {
			if job.Spec.Template.Spec.Volumes[i].Name == "api-staging" {
				vol = &job.Spec.Template.Spec.Volumes[i]
			}
		}
		Expect(vol).NotTo(BeNil())
		Expect(vol.ConfigMap.Name).To(Equal("bk-11-api"))
		c := job.Spec.Template.Spec.Containers[0]
		Expect(c.Command).To(ContainElement("--api"))
		Expect(c.Command).To(ContainElement("/staging/api"))
		var mount *corev1.VolumeMount
		for i := range c.VolumeMounts {
			if c.VolumeMounts[i].MountPath == "/staging/api" {
				mount = &c.VolumeMounts[i]
			}
		}
		Expect(mount).NotTo(BeNil())
		Expect(mount.ReadOnly).To(BeTrue())

		// Skip-if-identical: a second reconcile must not touch the ConfigMap.
		rv := staging.ResourceVersion
		reconcileBackup(ctx, "bk-11", "bk-11", rec)
		again := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "bk-11", Name: "bk-11-api"}, again)).To(Succeed())
		Expect(again.ResourceVersion).To(Equal(rv))
	})

	// M2: a backup with no PVCs but a non-empty serialization is valid — one
	// Job on the FIRST node alphabetically (documented placement rule).
	It("12. zero PVCs + API-only backup → one Job on the first node alphabetically", func() {
		makeNamespace(ctx, "bk-12")
		createSecret(ctx, "bk-12-seed", "bk-12", map[string][]byte{"k": []byte("seed")})
		makeNode(ctx, "node-b")
		makeNode(ctx, "node-a")
		repo := makeReadyRepo(ctx, "bk-12-repo")
		b := &pbsv1.PBSBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "bk-12", Namespace: "bk-12"},
			Spec: pbsv1.PBSBackupSpec{
				RepoRef:  repo.Name,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "no-such-app"}},
			},
		}
		Expect(k8sClient.Create(ctx, b)).To(Succeed())

		rec := record.NewFakeRecorder(32)
		res := reconcileBackup(ctx, "bk-12", "bk-12", rec)

		Expect(res.RequeueAfter).To(Equal(time.Minute)) // Running poll fallback
		fetched := fetchBackup(ctx, "bk-12", "bk-12")
		Expect(fetched.Status.Phase).To(Equal(pbsv1.BackupPhaseRunning))
		Expect(fetched.Status.Jobs).To(Equal([]pbsv1.BackupJobStatus{
			{Node: "node-a", Job: "bk-12-node-a", State: "active"},
		}))

		jobs := backupJobs(ctx, fetched)
		Expect(jobs).To(HaveLen(1))
		job := &jobs[0]
		Expect(job.Name).To(Equal("bk-12-node-a"))
		sel := job.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		Expect(sel.NodeSelectorTerms[0].MatchExpressions[0].Values).To(Equal([]string{"node-a"}))
		// No PVCs to mount: only the staging volume.
		Expect(job.Spec.Template.Spec.Volumes).To(HaveLen(1))
		Expect(job.Spec.Template.Spec.Volumes[0].Name).To(Equal("api-staging"))
		c := job.Spec.Template.Spec.Containers[0]
		Expect(fmt.Sprint(c.Command)).To(Equal(fmt.Sprint([]string{"pbs-agent", "backup", "--api", "/staging/api"})))
		// The staging ConfigMap backs the backup.
		staging := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "bk-12", Name: "bk-12-api"}, staging)).To(Succeed())
		Expect(staging.Data["api.yaml"]).To(ContainSubstring("kind: Secret"))
	})
})

// ---- M3: Prometheus metrics on phase transitions --------------------------

var _ = Describe("PBSBackup metrics", func() {
	ctx := context.Background()

	// driveToCompleted walks one backup New → Running → Completed via direct
	// reconciles, back-dating StartedAt so the duration gauge is provable.
	driveToCompleted := func(ns, name string, m *BackupMetrics) {
		repo := makeReadyRepo(ctx, ns+"-repo")
		makeLocalPV(ctx, ns+"-pv", "node-a")
		makePVC(ctx, ns+"-data", ns, ns+"-pv")
		makeBackup(ctx, name, ns, repo.Name, ns+"-data")

		reconcileBackupWith(ctx, ns, name, record.NewFakeRecorder(16), m)

		// Back-date StartedAt: CompletedAt-StartedAt must land in the gauge.
		b := fetchBackup(ctx, ns, name)
		before := b.DeepCopy()
		early := metav1.NewTime(time.Now().Add(-90 * time.Second))
		b.Status.StartedAt = &early
		Expect(k8sClient.Status().Patch(ctx, b, client.MergeFrom(before))).To(Succeed())

		setJobCondition(ctx, ns, name+"-node-a", batchv1.JobCondition{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
		})
		makeResultPod(ctx, ns, name+"-node-a",
			`{"snapshotRef":"host/x/2026-09-16T10:00:00Z","bytes":1}`)
		reconcileBackupWith(ctx, ns, name, record.NewFakeRecorder(16), m)
	}

	It("success transition sets last_success and duration", func() {
		makeNamespace(ctx, "bk-m1")
		m := NewBackupMetrics(prometheus.NewRegistry())

		driveToCompleted("bk-m1", "bk-m1", m)

		b := fetchBackup(ctx, "bk-m1", "bk-m1")
		Expect(b.Status.Phase).To(Equal(pbsv1.BackupPhaseCompleted))
		last := testutil.ToFloat64(m.LastSuccess.WithLabelValues("bk-m1"))
		Expect(last).To(BeNumerically(">", 0))
		Expect(last).To(BeNumerically("~", float64(b.Status.CompletedAt.Unix()), 1))
		Expect(testutil.ToFloat64(m.Duration.WithLabelValues("bk-m1"))).To(BeNumerically("~", 90, 2))
		Expect(testutil.ToFloat64(m.Errors.WithLabelValues("bk-m1"))).To(Equal(0.0))
	})

	It("Failed transition increments errors_total and leaves last_success untouched", func() {
		makeNamespace(ctx, "bk-m2")
		m := NewBackupMetrics(prometheus.NewRegistry())

		repo := makeReadyRepo(ctx, "bk-m2-repo")
		makeLocalPV(ctx, "bk-m2-pv", "node-a")
		makePVC(ctx, "bk-m2-data", "bk-m2", "bk-m2-pv")
		makeBackup(ctx, "bk-m2", "bk-m2", repo.Name, "bk-m2-data")
		reconcileBackupWith(ctx, "bk-m2", "bk-m2", record.NewFakeRecorder(16), m)
		setJobCondition(ctx, "bk-m2", "bk-m2-node-a", batchv1.JobCondition{
			Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
		})
		reconcileBackupWith(ctx, "bk-m2", "bk-m2", record.NewFakeRecorder(16), m)

		Expect(fetchBackup(ctx, "bk-m2", "bk-m2").Status.Phase).To(Equal(pbsv1.BackupPhaseFailed))
		Expect(testutil.ToFloat64(m.Errors.WithLabelValues("bk-m2"))).To(Equal(1.0))
		// The failure gauges stay untouched: never set, never regressed.
		Expect(testutil.ToFloat64(m.LastSuccess.WithLabelValues("bk-m2"))).To(Equal(0.0))
		Expect(testutil.ToFloat64(m.Duration.WithLabelValues("bk-m2"))).To(Equal(0.0))
	})
})

// ---- M4: pre-exec hooks ----------------------------------------------------

// fakeExec records exec calls in order; err non-nil fails every exec.
type fakeExec struct {
	err   error
	calls []string // "ns/pod:container:argv..."
}

func (f *fakeExec) Exec(_ context.Context, ns, pod, container string, command []string, _ time.Duration) error {
	f.calls = append(f.calls, ns+"/"+pod+":"+container+":"+strings.Join(command, " "))
	return f.err
}

// makeHookedPod creates a pod with NO nodeName (envtest has no kubelet; a real
// exec must fail fast with "no host assigned") mounting pvcName, carrying the
// given annotations.
func makeHookedPod(ctx context.Context, name, ns, pvcName string, annotations map[string]string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Annotations: annotations},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{Name: "app", Image: "app:dev"},
				{Name: "sidecar", Image: "side:dev"},
			},
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
				},
			}},
		},
	}
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())
	return pod
}

var _ = Describe("PBSBackup pre-hooks", func() {
	ctx := context.Background()

	const dumpArgv = `["pg_dump","-U","postgres","-Fc","-f","/dump.pgc"]`

	// hookFixture seeds a namespace with repo, PV (node-a), PVC, and returns nothing.
	hookFixture := func(ns, pvc string) {
		makeNamespace(ctx, ns)
		repo := makeReadyRepo(ctx, ns+"-repo")
		makeLocalPV(ctx, ns+"-pv", "node-a")
		makePVC(ctx, pvc, ns, ns+"-pv")
		makeBackup(ctx, ns, ns, repo.Name, pvc)
	}

	It("h1. hooked pods exec sequentially in pod-name order, then Jobs launch", func() {
		hookFixture("bk-h1", "bk-h1-data")
		// z-pod names its container; a-pod relies on the first-container default.
		makeHookedPod(ctx, "z-pod", "bk-h1", "bk-h1-data",
			map[string]string{hooks.AnnotationCommand: dumpArgv, hooks.AnnotationContainer: "sidecar"})
		makeHookedPod(ctx, "a-pod", "bk-h1", "bk-h1-data",
			map[string]string{hooks.AnnotationCommand: `["sync"]`})

		fake := &fakeExec{}
		res := reconcileBackupHooks(ctx, "bk-h1", "bk-h1", record.NewFakeRecorder(32), fake)

		Expect(fake.calls).To(Equal([]string{
			"bk-h1/a-pod:app:sync", // alphabetical first, default container
			"bk-h1/z-pod:sidecar:" + `pg_dump -U postgres -Fc -f /dump.pgc`,
		}))
		b := fetchBackup(ctx, "bk-h1", "bk-h1")
		Expect(b.Status.Phase).To(Equal(pbsv1.BackupPhaseRunning))
		Expect(backupJobs(ctx, b)).To(HaveLen(1))

		// Re-reconcile (Jobs known): hooks never re-run.
		fake.calls = nil
		reconcileBackupHooks(ctx, "bk-h1", "bk-h1", record.NewFakeRecorder(32), fake)
		Expect(fake.calls).To(BeEmpty())
		Expect(res.RequeueAfter).To(Equal(time.Minute))
	})

	It("h2. real exec against envtest (no kubelet) → Failed/HookFailed, no Jobs, event", func() {
		hookFixture("bk-h2", "bk-h2-data")
		makeHookedPod(ctx, "bk-h2-pod", "bk-h2", "bk-h2-data",
			map[string]string{hooks.AnnotationCommand: dumpArgv, hooks.AnnotationContainer: "app"})

		rec := record.NewFakeRecorder(32)
		res := reconcileBackupWith(ctx, "bk-h2", "bk-h2", rec, nil)

		Expect(res.RequeueAfter).To(BeZero()) // terminal, no requeue
		b := fetchBackup(ctx, "bk-h2", "bk-h2")
		Expect(b.Status.Phase).To(Equal(pbsv1.BackupPhaseFailed))
		cond := meta.FindStatusCondition(b.Status.Conditions, "Ready")
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal("HookFailed"))
		Expect(cond.Message).To(ContainSubstring("bk-h2-pod"))
		events := drainEvents(rec)
		Expect(events).To(HaveLen(1))
		Expect(events[0]).To(ContainSubstring("HookFailed"))
		Expect(events[0]).To(ContainSubstring("bk-h2-pod"))
		Expect(backupJobs(ctx, b)).To(BeEmpty())
	})

	It("h3. malformed hook annotation → Failed/HookInvalid, no Jobs, no exec", func() {
		hookFixture("bk-h3", "bk-h3-data")
		makeHookedPod(ctx, "bk-h3-pod", "bk-h3", "bk-h3-data",
			map[string]string{hooks.AnnotationCommand: `["pg_dump", oops`, hooks.AnnotationContainer: "app"})

		fake := &fakeExec{}
		rec := record.NewFakeRecorder(32)
		res := reconcileBackupHooks(ctx, "bk-h3", "bk-h3", rec, fake)

		Expect(fake.calls).To(BeEmpty())
		Expect(res.RequeueAfter).To(BeZero())
		b := fetchBackup(ctx, "bk-h3", "bk-h3")
		Expect(b.Status.Phase).To(Equal(pbsv1.BackupPhaseFailed))
		cond := meta.FindStatusCondition(b.Status.Conditions, "Ready")
		Expect(cond.Reason).To(Equal("HookInvalid"))
		Expect(cond.Message).To(ContainSubstring("bk-h3-pod"))
		Expect(cond.Message).To(ContainSubstring(hooks.AnnotationCommand))
		events := drainEvents(rec)
		Expect(events).To(HaveLen(1))
		Expect(events[0]).To(ContainSubstring("HookInvalid"))
		Expect(backupJobs(ctx, b)).To(BeEmpty())
	})

	It("h4. unhooked mounting pod and hooked NON-mounting pod → untouched, Jobs as today", func() {
		hookFixture("bk-h4", "bk-h4-data")
		// Mounts the selected PVC but carries no annotations.
		makeHookedPod(ctx, "bk-h4-plain", "bk-h4", "bk-h4-data", nil)
		// Hooked, but mounts a PVC that is NOT selected.
		makePVC(ctx, "bk-h4-other", "bk-h4", "")
		makeHookedPod(ctx, "bk-h4-elsewhere", "bk-h4", "bk-h4-other",
			map[string]string{hooks.AnnotationCommand: `["boom"]`})

		fake := &fakeExec{}
		reconcileBackupHooks(ctx, "bk-h4", "bk-h4", record.NewFakeRecorder(32), fake)

		Expect(fake.calls).To(BeEmpty())
		b := fetchBackup(ctx, "bk-h4", "bk-h4")
		Expect(b.Status.Phase).To(Equal(pbsv1.BackupPhaseRunning))
		Expect(backupJobs(ctx, b)).To(HaveLen(1))
	})

	It("h5. exec error (non-zero exit) → Failed/HookFailed naming pod and error", func() {
		hookFixture("bk-h5", "bk-h5-data")
		makeHookedPod(ctx, "bk-h5-pod", "bk-h5", "bk-h5-data",
			map[string]string{hooks.AnnotationCommand: dumpArgv})

		fake := &fakeExec{err: fmt.Errorf("command terminated with exit code 1")}
		rec := record.NewFakeRecorder(32)
		reconcileBackupHooks(ctx, "bk-h5", "bk-h5", rec, fake)

		b := fetchBackup(ctx, "bk-h5", "bk-h5")
		Expect(b.Status.Phase).To(Equal(pbsv1.BackupPhaseFailed))
		cond := meta.FindStatusCondition(b.Status.Conditions, "Ready")
		Expect(cond.Reason).To(Equal("HookFailed"))
		Expect(cond.Message).To(ContainSubstring("bk-h5-pod"))
		Expect(cond.Message).To(ContainSubstring("exit code 1"))
		Expect(drainEvents(rec)).To(HaveLen(1))
		Expect(backupJobs(ctx, b)).To(BeEmpty())
	})
})
