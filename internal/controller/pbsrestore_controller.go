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
	"errors"
	"reflect"
	"sort"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pbsv1 "gitlab.sharifmind.ir/miad/pbs-operator/api/v1"
	backuplib "gitlab.sharifmind.ir/miad/pbs-operator/internal/backup"
)

const (
	// restoreSA is the per-target-namespace ServiceAccount the restore Jobs
	// run as (created-if-missing; never owner-ref'd — the namespace outlives
	// any single restore).
	restoreSA = "pbs-restore"

	// restoreRoleName is the broad ClusterRole (config/rbac/restore-role.yaml,
	// kustomize namePrefix included) — bound through a per-target-ns
	// RoleBinding so its namespaced grants only ever apply inside the target.
	restoreRoleName = "pbs-operator-pbs-restore"

	// restoreClusterRoleName is the narrow cluster-scoped ClusterRole
	// (config/rbac/restore-cluster-role.yaml): namespaces + CRDs only, the
	// sole thing a ClusterRoleBinding may ever grant the Job SA.
	restoreClusterRoleName = "pbs-operator-pbs-restore-cluster"

	reasonRestoreInvalid = "InvalidSpec"
	reasonStagingAbsent  = "ApiManifestMissing"
)

// PBSRestoreReconciler reconciles a PBSRestore object through a four-Job
// pipeline into the target namespace:
//
//	<name>-api-fetch     restore api.pxar.didx from the snapshot → ConfigMap
//	                     <name>-api (the agent uploads it via kubectl)
//	<name>-api-pre       apply Namespace/CRD/PreVolume docs (PVCs land here,
//	                     unbound under WFFC)
//	<name>-vol-<node>    restore volume data per node — THIS Job is the WFFC
//	                     consumer; its nodeAffinity decides where each PVC binds
//	<name>-api-workload  apply workload docs (pods consume the restored data)
//
// Jobs run in the TARGET namespace (PVC mounts and the api ConfigMap are
// namespace-local). In the common case the PBSRestore lives in the target
// namespace too and everything is controller-owned; for a cross-namespace
// target, ownerRefs are skipped (cross-namespace owner references are invalid
// — the GC deletes such dependents) and Jobs are tracked by name.
type PBSRestoreReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// AgentImage is the restore agent container image (--agent-image flag).
	AgentImage string
}

// +kubebuilder:rbac:groups=pbs.sharifmind.ir,resources=pbsrestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pbs.sharifmind.ir,resources=pbsrestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pbs.sharifmind.ir,resources=pbsrestores/finalizers,verbs=update

// Restore Jobs are created in the TARGET namespace, which may differ from the
// PBSRestore's own.
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create

// The target namespace is created if missing; the repo credential secret is
// copied into it (secrets are namespace-scoped).
// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update

// The api.yaml ConfigMap (uploaded by the fetch Job) is read to plan the
// volume restore Jobs.
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// Fallback node for PVCs no workload pins (alphabetically first).
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch

// The pbs-restore ServiceAccount is provisioned per target namespace.
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create

// Provisioning the Job SA's rights per target namespace: a RoleBinding to
// the broad pbs-restore ClusterRole (scoping ALL its namespaced grants to
// the target — secrets and workloads of other namespaces stay unreachable)
// and a ClusterRoleBinding to the narrow pbs-restore-cluster role (namespaces
// + CRDs only — the whole cluster-wide surface a restore opens). `bind`
// scoped by resourceNames to exactly those two ClusterRoles is the
// least-privilege grant — creating a binding normally requires holding every
// permission the binding grants.
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterrolebindings,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles,resourceNames=pbs-operator-pbs-restore,verbs=bind
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles,resourceNames=pbs-operator-pbs-restore-cluster,verbs=bind

// Reconcile drives the phase machine New → StagingAPI → RestoringVolumes →
// ApplyingWorkloads → Completed|Failed. Terminal phases no-op. A failed Job
// fails the whole restore (terminal, no requeue).
func (r *PBSRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	var restore pbsv1.PBSRestore
	if err := r.Get(ctx, req.NamespacedName, &restore); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if restore.Status.Phase == pbsv1.RestorePhaseCompleted || restore.Status.Phase == pbsv1.RestorePhaseFailed {
		return ctrl.Result{}, nil
	}

	// Spec validation: everything the pipeline dereferences must be there.
	if problem := validateSpec(&restore); problem != "" {
		return r.failRestore(ctx, &restore, reasonRestoreInvalid, problem)
	}

	// Repo gate: must exist and be Ready (same semantics as backup).
	var repo pbsv1.PBSRepo
	if err := r.Get(ctx, types.NamespacedName{Name: restore.Spec.RepoRef}, &repo); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		return r.holdRestore(ctx, &restore, reasonRepoNotReady,
			"PBSRepo "+restore.Spec.RepoRef+" not found", requeueWait)
	}
	if !meta.IsStatusConditionTrue(repo.Status.Conditions, condReady) {
		return r.holdRestore(ctx, &restore, reasonRepoNotReady,
			"PBSRepo "+repo.Name+" is not Ready", requeueWait)
	}

	// Target namespace: created if missing, waited out while terminating.
	if problem, err := r.ensureTargetNamespace(ctx, &restore); err != nil || problem != "" {
		if err != nil {
			return ctrl.Result{}, err
		}
		return r.holdRestore(ctx, &restore, "TargetNamespaceTerminating", problem, requeueWait)
	}

	// Repo secret copy into the TARGET namespace (Jobs dereference it there).
	repoSecret, problem, err := r.ensureRepoSecret(ctx, &restore, &repo)
	if err != nil {
		return ctrl.Result{}, err
	}
	if problem != "" {
		return r.holdRestore(ctx, &restore, reasonSecretCopy, problem, requeueWait)
	}

	// Job SA + binding into the pre-defined broad role.
	if err := r.ensureJobRBAC(ctx, &restore); err != nil {
		return ctrl.Result{}, err
	}

	spec := backuplib.RestoreJobSpec{
		Restore:        restore.Name,
		Namespace:      restore.Spec.TargetNamespace,
		RepoSecret:     repoSecret,
		Ref:            restore.Spec.SnapshotRef,
		Image:          r.agentImage(),
		ServiceAccount: restoreSA,
	}

	// Step 1: fetch api.pxar.didx → ConfigMap.
	fetchName := jobName(restore.Name, "api-fetch")
	fetch, done, err := r.ensureJob(ctx, &restore, fetchName, func() *batchv1.Job {
		return backuplib.BuildAPIFetchJob(spec, restore.Name+"-api")
	})
	if err != nil || !done {
		return r.requeueOrHold(ctx, &restore, pbsv1.RestorePhaseStagingAPI, fetch, err)
	}

	// Step 2: the fetch Job must have uploaded the api ConfigMap.
	cm := &corev1.ConfigMap{}
	err = r.Get(ctx, types.NamespacedName{Namespace: spec.Namespace, Name: restore.Name + "-api"}, cm)
	if apierrors.IsNotFound(err) {
		return r.holdRestore(ctx, &restore, reasonStagingAbsent,
			"fetch Job completed but ConfigMap "+restore.Name+"-api is absent", requeueWait)
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	r.adoptConfigMap(ctx, &restore, cm) // same-namespace ownerRef, best-effort

	// Step 3: pre apply (namespaces → CRDs → pre-volume docs incl PVCs).
	preName := jobName(restore.Name, "api-pre")
	pre, done, err := r.ensureJob(ctx, &restore, preName, func() *batchv1.Job {
		return backuplib.BuildApplyJob(spec, "pre", restore.Name+"-api",
			restore.Spec.DropKinds, restore.Spec.KeepKinds)
	})
	if err != nil || !done {
		return r.requeueOrHold(ctx, &restore, pbsv1.RestorePhaseStagingAPI, pre, err)
	}

	// Step 4: volume data, one node-pinned Job per node (WFFC consumer).
	docs, err := backuplib.ParseAPIManifests([]byte(cm.Data["api.yaml"]), spec.Namespace)
	if err != nil {
		return r.failRestore(ctx, &restore, reasonRestoreInvalid,
			"api ConfigMap: "+err.Error())
	}
	_, _, preDocs, _ := backuplib.BucketDocs(docs, restore.Spec.DropKinds, restore.Spec.KeepKinds)
	pvcs := backuplib.PVCsFromDocs(preDocs)
	// ponytail: an api.yaml with no PVCs skips straight to the workload
	// apply — an empty/mis-serialized plan can then Complete without any
	// volume being restored; cross-check the snapshot's pvc-* archives if
	// that ever bites.
	if len(pvcs) > 0 {
		done, err := r.ensureVolumeJobs(ctx, &restore, spec, docs, pvcs)
		if err != nil || !done {
			return r.requeueOrHold(ctx, &restore, pbsv1.RestorePhaseRestoringVolumes, nil, err)
		}
	}

	// Step 5: workload apply; Complete ends the restore.
	workload, done, err := r.ensureJob(ctx, &restore, jobName(restore.Name, "api-workload"), func() *batchv1.Job {
		return backuplib.BuildApplyJob(spec, "workload", restore.Name+"-api",
			restore.Spec.DropKinds, restore.Spec.KeepKinds)
	})
	if err != nil || !done {
		return r.requeueOrHold(ctx, &restore, pbsv1.RestorePhaseApplyingWorkloads, workload, err)
	}
	return r.completeRestore(ctx, &restore)
}

// validateSpec rejects a spec the pipeline cannot run with, naming the field.
func validateSpec(rs *pbsv1.PBSRestore) string {
	switch {
	case rs.Spec.RepoRef == "":
		return "repoRef is required"
	case rs.Spec.SnapshotRef == "":
		return "snapshotRef is required"
	case rs.Spec.TargetNamespace == "":
		return "targetNamespace is required"
	}
	if parts := strings.Split(rs.Spec.SnapshotRef, "/"); len(parts) != 3 {
		return "snapshotRef " + rs.Spec.SnapshotRef + " is malformed, want <type>/<id>/<ISO8601Z>"
	}
	return ""
}

// ensureTargetNamespace creates the target namespace when missing. A
// terminating namespace (recreate in flight) is a hold, not an error.
func (r *PBSRestoreReconciler) ensureTargetNamespace(ctx context.Context, rs *pbsv1.PBSRestore) (string, error) {
	ns := &corev1.Namespace{}
	err := r.Get(ctx, types.NamespacedName{Name: rs.Spec.TargetNamespace}, ns)
	switch {
	case err == nil:
		if ns.DeletionTimestamp != nil {
			return "target namespace " + ns.Name + " is terminating", nil
		}
		return "", nil
	case apierrors.IsNotFound(err):
		if err := r.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: rs.Spec.TargetNamespace},
		}); err != nil && !apierrors.IsAlreadyExists(err) {
			return "", err
		}
		return "", nil
	default:
		return "", err
	}
}

// ensureRepoSecret copies the repo's credentials into the target namespace
// as "pbsrepo-<repo>" (same contract-key copy as the backup controller, but
// NO ownerRef: the target namespace is shared — a second restore into it
// must not GC the first's secret — and cross-namespace ownerRefs are invalid
// anyway). Skip-if-equal keeps re-reconciles write-free.
func (r *PBSRestoreReconciler) ensureRepoSecret(ctx context.Context, rs *pbsv1.PBSRestore, repo *pbsv1.PBSRepo) (name, problem string, err error) {
	srcNS := repo.Spec.SecretRef.Namespace
	if srcNS == "" {
		srcNS = operatorNamespace()
	}
	src := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: srcNS, Name: repo.Spec.SecretRef.Name}, src); err != nil {
		if apierrors.IsNotFound(err) {
			return "", "repo secret " + srcNS + "/" + repo.Spec.SecretRef.Name + " not found", nil
		}
		return "", "", err
	}
	data := make(map[string][]byte, len(repoSecretKeys))
	for _, k := range repoSecretKeys {
		if v, ok := src.Data[k]; ok {
			data[k] = v
		}
	}
	name = "pbsrepo-" + repo.Name
	existing := &corev1.Secret{}
	switch err := r.Get(ctx, types.NamespacedName{Namespace: rs.Spec.TargetNamespace, Name: name}, existing); {
	case err == nil:
		if contractEqual(existing.Data, data) && existing.Labels[backuplib.ManagedLabel] != "" {
			return name, "", nil
		}
		existing.Data = data
		if existing.Labels == nil {
			existing.Labels = map[string]string{}
		}
		existing.Labels[backuplib.ManagedLabel] = "true"
		if err := r.Update(ctx, existing); err != nil {
			return "", "", err
		}
		return name, "", nil
	case apierrors.IsNotFound(err):
		copySec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: rs.Spec.TargetNamespace,
				Labels:    map[string]string{backuplib.ManagedLabel: "true"},
			},
			Data: data,
		}
		if err := r.Create(ctx, copySec); err != nil && !apierrors.IsAlreadyExists(err) {
			return "", "", err
		}
		return name, "", nil
	default:
		return "", "", err
	}
}

// rbacLabels mark every RBAC object the controller provisions: the managed
// label plus the restore's own name. These bindings have no possible
// ownerRef (cluster-scoped dependents of a namespaced owner are invalid), so
// they outlive their PBSRestore — the labels are the GC contract: a sweep
// listing pbs.sharifmind.ir/managed + pbsrestore=<name> finds and deletes
// exactly this restore's RBAC (a finalizer-based sweep is deferred; without
// labels, anyone recreating a swept namespace + SA name would reactivate a
// stale binding).
func rbacLabels(restore string) map[string]string {
	return map[string]string{
		backuplib.ManagedLabel: "true",
		"pbsrestore":           restore,
	}
}

// ensureJobRBAC provisions, for the target namespace: the pbs-restore
// ServiceAccount, a RoleBinding to the broad pbs-restore ClusterRole (this
// is what scopes the broad grants — secrets/workloads of OTHER namespaces
// stay unreachable), and a ClusterRoleBinding to the narrow
// pbs-restore-cluster role (namespaces + CRDs only). Create-if-missing,
// never deleted here — see rbacLabels for the sweep path.
func (r *PBSRestoreReconciler) ensureJobRBAC(ctx context.Context, rs *pbsv1.PBSRestore) error {
	sa := &corev1.ServiceAccount{}
	err := r.Get(ctx, types.NamespacedName{Namespace: rs.Spec.TargetNamespace, Name: restoreSA}, sa)
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:      restoreSA,
				Namespace: rs.Spec.TargetNamespace,
				Labels:    rbacLabels(rs.Name),
			},
		}); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	} else if err != nil {
		return err
	}

	rb := &rbacv1.RoleBinding{}
	err = r.Get(ctx, types.NamespacedName{Namespace: rs.Spec.TargetNamespace, Name: restoreSA}, rb)
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      restoreSA,
				Namespace: rs.Spec.TargetNamespace,
				Labels:    rbacLabels(rs.Name),
			},
			Subjects: []rbacv1.Subject{{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      restoreSA,
				Namespace: rs.Spec.TargetNamespace,
			}},
			RoleRef: rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName,
				Kind:     "ClusterRole",
				Name:     restoreRoleName,
			},
		}); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	} else if err != nil {
		return err
	}

	crb := &rbacv1.ClusterRoleBinding{}
	err = r.Get(ctx, types.NamespacedName{Name: "pbs-restore-" + rs.Spec.TargetNamespace}, crb)
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "pbs-restore-" + rs.Spec.TargetNamespace,
				Labels: rbacLabels(rs.Name),
			},
			Subjects: []rbacv1.Subject{{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      restoreSA,
				Namespace: rs.Spec.TargetNamespace,
			}},
			RoleRef: rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName,
				Kind:     "ClusterRole",
				Name:     restoreClusterRoleName,
			},
		}); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	} else if err != nil {
		return err
	}
	return nil
}

// ensureVolumeJobs creates one restore-volume Job per node for the restored
// PVCs. Node choice: the consuming workload's own hostname pin (the restored
// workloads land there, and local-path RWO data must be on the same node),
// else the alphabetically first node — deterministic across reconciles.
// Reports done=true once every volume Job is Complete.
func (r *PBSRestoreReconciler) ensureVolumeJobs(ctx context.Context, rs *pbsv1.PBSRestore,
	spec backuplib.RestoreJobSpec, docs []backuplib.ParsedDoc, pvcs []string) (bool, error) {
	fallback, err := firstNode(ctx, r.Client)
	if err != nil {
		return false, err
	}
	groups := map[string][]string{}
	for _, pvc := range pvcs {
		node := backuplib.NodeForPVC(docs, pvc)
		if node == "" {
			if fallback == "" {
				return false, &restoreHold{reasonUnplaceable,
					"PVC " + pvc + " has no pinned node and the cluster has no nodes"}
			}
			node = fallback
		}
		groups[node] = append(groups[node], pvc)
	}
	nodes := make([]string, 0, len(groups))
	for node := range groups {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	for _, node := range nodes {
		sort.Strings(groups[node])
		_, done, err := r.ensureJob(ctx, rs, jobName(rs.Name, "vol-"+node), func() *batchv1.Job {
			return backuplib.BuildRestoreVolumeJob(spec, node, groups[node])
		})
		if err != nil || !done {
			return false, err
		}
	}
	return true, nil
}

// restoreHold carries a user-facing problem (a failed or deleted pipeline
// Job, an unplaceable PVC) through ensure* call sites as an error, resolved
// by requeueOrHold into a terminal Failed or a hold.
type restoreHold struct{ reason, msg string }

func (h *restoreHold) Error() string { return h.msg }

// ensureJob get-or-creates one pipeline Job and reports whether it is
// Complete. A failed Job maps to a terminal restoreHold; a Job absent
// although status knows it (deleted mid-run) maps to a JobDeleted hold —
// never recreate, TTL/GC owns cleanup. The builder runs only on first
// create; ownerRef only when the Job's namespace matches the PBSRestore's
// (cross-namespace ownerRefs are invalid).
func (r *PBSRestoreReconciler) ensureJob(ctx context.Context, rs *pbsv1.PBSRestore,
	name string, build func() *batchv1.Job) (*batchv1.Job, bool, error) {
	job := &batchv1.Job{}
	switch err := r.Get(ctx, types.NamespacedName{Namespace: rs.Spec.TargetNamespace, Name: name}, job); {
	case err == nil:
		if jobCondition(job, batchv1.JobFailed) {
			return job, false, &restoreHold{reasonFailed, "job " + name + " failed"}
		}
		return job, jobCondition(job, batchv1.JobComplete), nil
	case apierrors.IsNotFound(err):
		if knownJob(rs, name) {
			// A Job that was recorded COMPLETE and is now absent finished its
			// work and was TTL-reaped (1h) — it counts as done, or a long
			// restore wedges at this step forever. Any other recorded state
			// stays the JobDeleted hold.
			if recordedJobState(rs, name) == "complete" {
				return nil, true, nil
			}
			return nil, false, &restoreHold{reasonJobDeleted,
				"job " + name + " was deleted before the restore finished; not recreating it"}
		}
		job = build()
		job.Name = name // builders may carry a blank Name; the controller owns naming
		if job.Namespace == rs.Namespace {
			if err := ctrl.SetControllerReference(rs, job, r.Scheme); err != nil {
				return nil, false, err
			}
		}
		if err := r.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, false, err
		}
		return job, false, nil
	default:
		return nil, false, err
	}
}

// knownJob reports whether status.Jobs already tracked the name (deleted-
// mid-run detection).
func knownJob(rs *pbsv1.PBSRestore, name string) bool {
	for _, js := range rs.Status.Jobs {
		if js.Job == name {
			return true
		}
	}
	return false
}

// recordedJobState is the state status.Jobs last recorded for name ("" when
// unknown).
func recordedJobState(rs *pbsv1.PBSRestore, name string) string {
	for _, js := range rs.Status.Jobs {
		if js.Job == name {
			return js.State
		}
	}
	return ""
}

// requeueOrHold converts an ensure* outcome into the reconcile result. A
// restoreHold with reason Failed is terminal (Failed phase, no requeue);
// any other hold requeues 1m. Otherwise the phase is recorded with
// refreshed Jobs and StartedAt (set once), requeue 1m as the poll fallback
// (Owns(Job) drives the fast path).
func (r *PBSRestoreReconciler) requeueOrHold(ctx context.Context, rs *pbsv1.PBSRestore,
	phase pbsv1.RestorePhase, job *batchv1.Job, err error) (ctrl.Result, error) {
	var hold *restoreHold
	if errors.As(err, &hold) {
		if hold.reason == reasonFailed {
			return r.failRestore(ctx, rs, hold.reason, hold.msg)
		}
		return r.holdRestore(ctx, rs, hold.reason, hold.msg, requeueWait)
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	_, rerr := r.recordRestore(ctx, rs, phase, metav1.Condition{
		Type: condReady, Status: metav1.ConditionFalse, Reason: reasonRunning,
		Message: "restore in phase " + string(phase),
	}, func(s *pbsv1.PBSRestoreStatus) {
		if s.StartedAt == nil {
			now := metav1.Now()
			s.StartedAt = &now
		}
		s.Jobs = r.liveJobs(ctx, rs)
	})
	if rerr != nil {
		return ctrl.Result{}, rerr
	}
	return ctrl.Result{RequeueAfter: requeueWait}, nil
}

// liveJobs snapshots the restore's Jobs (label pbsrestore=<name> in the
// target ns) as status.Jobs, pipeline order, with the phase each Job was
// launched in derived from its name suffix. Entries for Jobs no longer live
// (TTL-reaped after completing) are carried over from the previous snapshot
// — the recorded state is the pipeline's memory of finished work.
func (r *PBSRestoreReconciler) liveJobs(ctx context.Context, rs *pbsv1.PBSRestore) []pbsv1.RestoreJobStatus {
	list := &batchv1.JobList{}
	if err := r.List(ctx, list,
		client.InNamespace(rs.Spec.TargetNamespace),
		client.MatchingLabels{"pbsrestore": rs.Name}); err != nil {
		return rs.Status.Jobs // keep the last snapshot on a transient error
	}
	out := make([]pbsv1.RestoreJobStatus, 0, len(list.Items)+len(rs.Status.Jobs))
	live := make(map[string]bool, len(list.Items))
	for i := range list.Items {
		j := &list.Items[i]
		live[j.Name] = true
		out = append(out, pbsv1.RestoreJobStatus{
			Phase: jobPhaseByName(rs, j.Name),
			Job:   j.Name,
			State: jobState(j),
		})
	}
	for _, js := range rs.Status.Jobs {
		if !live[js.Job] {
			out = append(out, js) // TTL-reaped: keep its last recorded state
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Job < out[j].Job })
	return out
}

// jobPhaseByName maps a pipeline Job name to the restore phase it runs in
// (the names are this controller's own deterministic construction).
func jobPhaseByName(rs *pbsv1.PBSRestore, name string) string {
	base := strings.TrimPrefix(name, rs.Name+"-")
	switch {
	case strings.HasPrefix(base, "vol-"):
		return string(pbsv1.RestorePhaseRestoringVolumes)
	case base == "api-workload":
		return string(pbsv1.RestorePhaseApplyingWorkloads)
	default: // api-fetch, api-pre (incl. hash-truncated variants)
		return string(pbsv1.RestorePhaseStagingAPI)
	}
}

// adoptConfigMap adds the controller ownerRef to the fetch-uploaded
// ConfigMap when it lives in the PBSRestore's own namespace (GC then reaps
// it with the CR). Cross-namespace it is skipped silently; best-effort.
func (r *PBSRestoreReconciler) adoptConfigMap(ctx context.Context, rs *pbsv1.PBSRestore, cm *corev1.ConfigMap) {
	if cm.Namespace != rs.Namespace {
		return
	}
	for _, ref := range cm.OwnerReferences {
		if ref.UID == rs.UID {
			return
		}
	}
	before := cm.DeepCopy()
	if err := ctrl.SetControllerReference(rs, cm, r.Scheme); err == nil {
		_ = r.Patch(ctx, cm, client.MergeFrom(before))
	}
}

// holdRestore blocks progress with Ready=False/<reason> and a
// transition-only warning event; the phase stays monotonic (never regresses
// from an in-flight phase).
func (r *PBSRestoreReconciler) holdRestore(ctx context.Context, rs *pbsv1.PBSRestore, reason, message string, wait time.Duration) (ctrl.Result, error) {
	transitioned, err := r.recordRestore(ctx, rs, holdPhase(rs.Status.Phase), metav1.Condition{
		Type: condReady, Status: metav1.ConditionFalse, Reason: reason, Message: message,
	}, nil)
	if transitioned {
		r.Recorder.Event(rs, corev1.EventTypeWarning, reason, message)
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: wait}, nil
}

// holdPhase keeps an in-flight phase label on holds (New reads better as
// StagingAPI once anything happened).
func holdPhase(current pbsv1.RestorePhase) pbsv1.RestorePhase {
	if current == pbsv1.RestorePhaseNew {
		return pbsv1.RestorePhaseStagingAPI
	}
	return current
}

// completeRestore marks the restore Completed (CompletedAt set once) with a
// transition-only Normal event.
func (r *PBSRestoreReconciler) completeRestore(ctx context.Context, rs *pbsv1.PBSRestore) (ctrl.Result, error) {
	message := "restore of " + rs.Spec.SnapshotRef + " into " + rs.Spec.TargetNamespace + " completed"
	transitioned, err := r.recordRestore(ctx, rs, pbsv1.RestorePhaseCompleted, metav1.Condition{
		Type: condReady, Status: metav1.ConditionTrue, Reason: reasonCompleted, Message: message,
	}, func(s *pbsv1.PBSRestoreStatus) {
		s.Jobs = r.liveJobs(ctx, rs)
		if s.CompletedAt == nil {
			now := metav1.Now()
			s.CompletedAt = &now
		}
	})
	if transitioned {
		r.Recorder.Event(rs, corev1.EventTypeNormal, reasonCompleted, message)
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil // terminal: no requeue
}

// failRestore marks the restore Failed (terminal) with a transition-only
// warning event.
func (r *PBSRestoreReconciler) failRestore(ctx context.Context, rs *pbsv1.PBSRestore, reason, message string) (ctrl.Result, error) {
	transitioned, err := r.recordRestore(ctx, rs, pbsv1.RestorePhaseFailed, metav1.Condition{
		Type: condReady, Status: metav1.ConditionFalse, Reason: reason, Message: message,
	}, func(s *pbsv1.PBSRestoreStatus) {
		s.Jobs = r.liveJobs(ctx, rs)
		if s.CompletedAt == nil {
			now := metav1.Now()
			s.CompletedAt = &now
		}
	})
	if transitioned {
		r.Recorder.Event(rs, corev1.EventTypeWarning, reason, message)
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil // terminal: no requeue
}

// recordRestore stamps the phase and Ready condition (plus extra status
// mutations) and patches status only when something changed; it reports
// whether the condition CONTENT transitioned (callers gate events on it).
func (r *PBSRestoreReconciler) recordRestore(ctx context.Context, rs *pbsv1.PBSRestore,
	phase pbsv1.RestorePhase, cond metav1.Condition, extra func(*pbsv1.PBSRestoreStatus)) (bool, error) {
	before := rs.DeepCopy()
	cond.ObservedGeneration = rs.Generation
	transitioned := meta.SetStatusCondition(&rs.Status.Conditions, cond)
	rs.Status.Phase = phase
	if extra != nil {
		extra(&rs.Status)
	}
	if reflect.DeepEqual(before.Status, rs.Status) {
		return transitioned, nil
	}
	if err := r.Status().Patch(ctx, rs, client.MergeFrom(before)); err != nil {
		return false, err
	}
	return transitioned, nil
}

func (r *PBSRestoreReconciler) agentImage() string {
	if r.AgentImage == "" {
		return defaultAgentImage
	}
	return r.AgentImage
}

// SetupWithManager sets up the controller with the Manager.
func (r *PBSRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pbsv1.PBSRestore{}).
		Owns(&batchv1.Job{}).
		Named("pbsrestore").
		Complete(r)
}
