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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pbsv1 "gitlab.sharifmind.ir/miad/pbs-operator/api/v1"
)

// envtest runs no agent: Job outcomes are faked by stamping batch conditions
// (setJobCondition) and creating the fetch Job's ConfigMap by hand — the
// controller orchestration is what these specs pin.

// restoreAPIYAML is the payload the fetch Job would upload: a PVC, its
// consuming STS (pinned to node-a), and a droppable Secret.
const restoreAPIYAML = `apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data-pg-0
  namespace: pg
spec:
  accessModes: [ReadWriteOnce]
---
apiVersion: v1
kind: Secret
metadata:
  name: pg-secret
  namespace: pg
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: pg
  namespace: pg
spec:
  template:
    spec:
      nodeSelector:
        kubernetes.io/hostname: node-a
  volumeClaimTemplates:
    - metadata:
        name: data
`

// makeRestore creates a PBSRestore whose target namespace is target (in the
// happy-path specs CR ns == target so ownerRefs apply).
func makeRestore(ctx context.Context, name, ns, target, repo string, drop, keep []string) *pbsv1.PBSRestore {
	rs := &pbsv1.PBSRestore{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: pbsv1.PBSRestoreSpec{
			RepoRef:         repo,
			SnapshotRef:     "host/pg-1-k8s-ctl1/2026-09-16T10:00:00Z",
			TargetNamespace: target,
			DropKinds:       drop,
			KeepKinds:       keep,
		},
	}
	Expect(k8sClient.Create(ctx, rs)).To(Succeed())
	return rs
}

func fetchRestore(ctx context.Context, ns, name string) *pbsv1.PBSRestore {
	rs := &pbsv1.PBSRestore{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, rs)).To(Succeed())
	return rs
}

func reconcileRestore(ctx context.Context, ns, name string, rec record.EventRecorder) reconcile.Result {
	r := &PBSRestoreReconciler{
		Client: k8sClient, Scheme: k8sClient.Scheme(),
		Recorder: rec, AgentImage: "pbs-agent:dev",
	}
	res, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: ns, Name: name},
	})
	Expect(err).NotTo(HaveOccurred())
	return res
}

// restoreJob fetches one pipeline Job by name (in the target ns).
func restoreJob(ctx context.Context, ns, name string) *batchv1.Job {
	j := &batchv1.Job{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, j)).To(Succeed())
	return j
}

// uploadAPIManifest creates the ConfigMap the fetch Job's agent would have
// uploaded (key api.yaml), controller-owned in the same namespace.
func uploadAPIManifest(ctx context.Context, rs *pbsv1.PBSRestore) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: rs.Name + "-api", Namespace: rs.Spec.TargetNamespace},
		Data:       map[string]string{"api.yaml": restoreAPIYAML},
	}
	Expect(k8sClient.Create(ctx, cm)).To(Succeed())
}

var _ = Describe("PBSRestore Controller", func() {
	ctx := context.Background()

	// 1. Happy path: the four-Job pipeline in order, ownerRefs, the fetch CM
	// mounted by the apply Jobs, and the WFFC note — the volume Job's own
	// nodeAffinity (from the restored workload's pin) decides where the
	// unbound PVC binds.
	It("1. four phases in order → Completed", func() {
		makeNode(ctx, "node-a")
		makeNode(ctx, "node-b")
		makeNamespace(ctx, "rs-happy")
		ns := "rs-happy"
		repo := makeReadyRepo(ctx, "rs1-repo")
		rs := makeRestore(ctx, "r1", ns, ns, repo.Name, []string{"Secret"}, []string{"Probe"})
		rec := record.NewFakeRecorder(64)

		// StagingAPI: fetch Job created (ownerRef, argv), ns + SA + binding
		// + repo secret copy provisioned.
		res := reconcileRestore(ctx, ns, "r1", rec)
		Expect(res.RequeueAfter).To(Equal(requeueWait))
		Expect(fetchRestore(ctx, ns, "r1").Status.Phase).To(Equal(pbsv1.RestorePhaseStagingAPI))
		fetch := restoreJob(ctx, ns, "r1-api-fetch")
		refs := fetch.GetOwnerReferences()
		Expect(refs).To(HaveLen(1))
		Expect(refs[0].UID).To(Equal(rs.UID))
		Expect(fetch.Spec.Template.Spec.Containers[0].Command).To(Equal([]string{
			"pbs-agent", "restore-volume",
			"--ref", "host/pg-1-k8s-ctl1/2026-09-16T10:00:00Z",
			"--archive", "api.pxar.didx",
			"--target", "/staging/api",
			"--configmap", "r1-api",
		}))
		Expect(fetch.Spec.Template.Spec.ServiceAccountName).To(Equal("pbs-restore"))
		sa := &corev1.ServiceAccount{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "pbs-restore"}, sa)).To(Succeed())
		crb := &rbacv1.ClusterRoleBinding{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "pbs-restore-" + ns}, crb)).To(Succeed())
		Expect(crb.RoleRef.Name).To(Equal(restoreRoleName))
		Expect(repoSecretCopy(ctx, ns, "pbsrepo-"+repo.Name)).NotTo(BeNil())

		// Fetch completes but the ConfigMap is not there yet: hold.
		setJobCondition(ctx, ns, "r1-api-fetch", batchv1.JobCondition{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
		reconcileRestore(ctx, ns, "r1", rec)
		got := fetchRestore(ctx, ns, "r1")
		cond := meta.FindStatusCondition(got.Status.Conditions, condReady)
		Expect(cond.Reason).To(Equal("ApiManifestMissing"))

		// The upload lands: api-pre mounts the CM and carries drop/keep.
		uploadAPIManifest(ctx, rs)
		reconcileRestore(ctx, ns, "r1", rec)
		pre := restoreJob(ctx, ns, "r1-api-pre")
		Expect(pre.GetOwnerReferences()[0].UID).To(Equal(rs.UID))
		Expect(pre.Spec.Template.Spec.Volumes[0].ConfigMap.Name).To(Equal("r1-api"))
		Expect(pre.Spec.Template.Spec.Containers[0].Command).To(Equal([]string{
			"pbs-agent", "apply-manifests",
			"--phase", "pre",
			"--file", "/staging/api/api.yaml",
			"--drop", "Secret",
			"--keep", "Probe",
		}))

		// api-pre completes → RestoringVolumes: ONE volume Job, pinned to
		// node-a (the STS's own pin; node-b untouched), PVC mounted rw.
		setJobCondition(ctx, ns, "r1-api-pre", batchv1.JobCondition{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
		reconcileRestore(ctx, ns, "r1", rec)
		Expect(fetchRestore(ctx, ns, "r1").Status.Phase).To(Equal(pbsv1.RestorePhaseRestoringVolumes))
		vol := restoreJob(ctx, ns, "r1-vol-node-a")
		Expect(vol.GetOwnerReferences()[0].UID).To(Equal(rs.UID))
		sel := vol.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		Expect(sel.NodeSelectorTerms[0].MatchExpressions[0].Values).To(Equal([]string{"node-a"}))
		Expect(vol.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName).To(Equal("data-pg-0"))
		Expect(vol.Spec.Template.Spec.Containers[0].VolumeMounts[0].ReadOnly).To(BeFalse())
		// No job for node-b: the only PVC belongs to node-a's STS.
		jobs := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, jobs, client.InNamespace(ns))).To(Succeed())
		Expect(jobs.Items).To(HaveLen(3))

		// Volumes done → ApplyingWorkloads → Completed.
		setJobCondition(ctx, ns, "r1-vol-node-a", batchv1.JobCondition{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
		reconcileRestore(ctx, ns, "r1", rec)
		workload := restoreJob(ctx, ns, "r1-api-workload")
		Expect(workload.Spec.Template.Spec.Containers[0].Command).To(Equal([]string{
			"pbs-agent", "apply-manifests",
			"--phase", "workload",
			"--file", "/staging/api/api.yaml",
			"--drop", "Secret",
			"--keep", "Probe",
		}))
		setJobCondition(ctx, ns, "r1-api-workload", batchv1.JobCondition{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
		res = reconcileRestore(ctx, ns, "r1", rec)

		Expect(res.RequeueAfter).To(BeZero())
		done := fetchRestore(ctx, ns, "r1")
		Expect(done.Status.Phase).To(Equal(pbsv1.RestorePhaseCompleted))
		Expect(done.Status.CompletedAt).NotTo(BeNil())
		Expect(done.Status.Jobs).To(ConsistOf(
			pbsv1.RestoreJobStatus{Phase: "StagingAPI", Job: "r1-api-fetch", State: "complete"},
			pbsv1.RestoreJobStatus{Phase: "StagingAPI", Job: "r1-api-pre", State: "complete"},
			pbsv1.RestoreJobStatus{Phase: "RestoringVolumes", Job: "r1-vol-node-a", State: "complete"},
			pbsv1.RestoreJobStatus{Phase: "ApplyingWorkloads", Job: "r1-api-workload", State: "complete"},
		))
		// The fetch CM got adopted (same ns): GCs with the CR.
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "r1-api"}, cm)).To(Succeed())
		Expect(cm.GetOwnerReferences()).To(HaveLen(1))
		Expect(cm.GetOwnerReferences()[0].UID).To(Equal(rs.UID))
	})

	// 2. A Job failing mid-sequence is terminal: later Jobs never appear.
	It("2. api-pre fails → Failed, no volume or workload Jobs", func() {
		makeNode(ctx, "node-a")
		makeNamespace(ctx, "rs-fail")
		ns := "rs-fail"
		repo := makeReadyRepo(ctx, "rs2-repo")
		rs := makeRestore(ctx, "r2", ns, ns, repo.Name, nil, nil)
		rec := record.NewFakeRecorder(32)

		reconcileRestore(ctx, ns, "r2", rec)
		setJobCondition(ctx, ns, "r2-api-fetch", batchv1.JobCondition{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
		uploadAPIManifest(ctx, rs)
		reconcileRestore(ctx, ns, "r2", rec)
		setJobCondition(ctx, ns, "r2-api-pre", batchv1.JobCondition{
			Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"})

		res := reconcileRestore(ctx, ns, "r2", rec)
		Expect(res.RequeueAfter).To(BeZero())
		failed := fetchRestore(ctx, ns, "r2")
		Expect(failed.Status.Phase).To(Equal(pbsv1.RestorePhaseFailed))
		Expect(failed.Status.CompletedAt).NotTo(BeNil())
		cond := meta.FindStatusCondition(failed.Status.Conditions, condReady)
		Expect(cond.Reason).To(Equal("Failed"))
		Expect(cond.Message).To(ContainSubstring("r2-api-pre"))
		jobs := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, jobs, client.InNamespace(ns))).To(Succeed())
		Expect(jobs.Items).To(HaveLen(2)) // fetch + api-pre only
		events := drainEvents(rec)
		Expect(events).NotTo(BeEmpty())
		Expect(events[len(events)-1]).To(ContainSubstring("r2-api-pre"))
	})

	// 3. Terminal phase is final: re-reconcile changes nothing.
	It("3. terminal no-op", func() {
		makeNode(ctx, "node-a")
		makeNamespace(ctx, "rs-term")
		ns := "rs-term"
		repo := makeReadyRepo(ctx, "rs3-repo")
		makeRestore(ctx, "r3", ns, ns, repo.Name, nil, nil)
		rec := record.NewFakeRecorder(32)
		reconcileRestore(ctx, ns, "r3", rec)
		setJobCondition(ctx, ns, "r3-api-fetch", batchv1.JobCondition{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
		uploadAPIManifest(ctx, fetchRestore(ctx, ns, "r3"))
		reconcileRestore(ctx, ns, "r3", rec)
		setJobCondition(ctx, ns, "r3-api-pre", batchv1.JobCondition{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
		reconcileRestore(ctx, ns, "r3", rec)
		setJobCondition(ctx, ns, "r3-vol-node-a", batchv1.JobCondition{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
		reconcileRestore(ctx, ns, "r3", rec)
		setJobCondition(ctx, ns, "r3-api-workload", batchv1.JobCondition{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
		reconcileRestore(ctx, ns, "r3", rec)
		drainEvents(rec)

		rv := fetchRestore(ctx, ns, "r3").ResourceVersion
		res := reconcileRestore(ctx, ns, "r3", rec)
		Expect(res).To(Equal(reconcile.Result{}))
		Expect(fetchRestore(ctx, ns, "r3").ResourceVersion).To(Equal(rv))
		Expect(drainEvents(rec)).To(BeEmpty())
	})

	// 4. Spec the pipeline cannot run with → terminal Failed, no Jobs.
	It("4. malformed snapshotRef → Failed/InvalidSpec, zero jobs", func() {
		makeNamespace(ctx, "rs4")
		ns := "rs4"
		repo := makeReadyRepo(ctx, "rs4-repo")
		rs := &pbsv1.PBSRestore{
			ObjectMeta: metav1.ObjectMeta{Name: "r4", Namespace: ns},
			Spec: pbsv1.PBSRestoreSpec{
				RepoRef: repo.Name, SnapshotRef: "not-a-ref", TargetNamespace: ns,
			},
		}
		Expect(k8sClient.Create(ctx, rs)).To(Succeed())

		rec := record.NewFakeRecorder(16)
		res := reconcileRestore(ctx, ns, "r4", rec)

		Expect(res.RequeueAfter).To(BeZero())
		got := fetchRestore(ctx, ns, "r4")
		Expect(got.Status.Phase).To(Equal(pbsv1.RestorePhaseFailed))
		cond := meta.FindStatusCondition(got.Status.Conditions, condReady)
		Expect(cond.Reason).To(Equal("InvalidSpec"))
		jobs := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, jobs, client.InNamespace(ns))).To(Succeed())
		Expect(jobs.Items).To(BeEmpty())
	})

	// 5. Repo gate: not Ready → hold, requeue 1m, no Jobs (backup semantics).
	It("5. repo not Ready → Scheduled hold", func() {
		repo := makeReadyRepo(ctx, "rs5-repo")
		repo.Status.Conditions[0].Status = metav1.ConditionFalse
		Expect(k8sClient.Status().Update(ctx, repo)).To(Succeed())
		makeNamespace(ctx, "rs5")
		ns := "rs5"
		makeRestore(ctx, "r5", ns, ns, repo.Name, nil, nil)

		rec := record.NewFakeRecorder(16)
		res := reconcileRestore(ctx, ns, "r5", rec)

		Expect(res.RequeueAfter).To(Equal(requeueWait))
		got := fetchRestore(ctx, ns, "r5")
		cond := meta.FindStatusCondition(got.Status.Conditions, condReady)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("RepoNotReady"))
		jobs := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, jobs, client.InNamespace(ns))).To(Succeed())
		Expect(jobs.Items).To(BeEmpty())
	})

	// 6. Cross-namespace target: Jobs run in the target, ownerRefs skipped
	// (cross-namespace owner references are invalid — GC would kill them).
	It("6. cross-namespace target → fetch Job there, no ownerRef", func() {
		makeNode(ctx, "node-a")
		repo := makeReadyRepo(ctx, "rs6-repo")
		makeRestore(ctx, "r6", "default", "rs6-target", repo.Name, nil, nil)

		res := reconcileRestore(ctx, "default", "r6", record.NewFakeRecorder(16))

		Expect(res.RequeueAfter).To(Equal(requeueWait))
		fetch := restoreJob(ctx, "rs6-target", "r6-api-fetch")
		Expect(fetch.GetOwnerReferences()).To(BeEmpty())
		Expect(fetchRestore(ctx, "default", "r6").Status.Phase).To(Equal(pbsv1.RestorePhaseStagingAPI))
		Expect(repoSecretCopy(ctx, "rs6-target", "pbsrepo-"+repo.Name)).NotTo(BeNil())
	})
})
