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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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
	// Aliased: the local variable "backup" in Reconcile shadows the package name.
	backuplib "gitlab.sharifmind.ir/miad/pbs-operator/internal/backup"
)

const (
	// Requeue cadences: requeueWait covers fixable-from-outside problems
	// (repo warming up, PVC not yet bound/placeable, a Job deleted mid-run);
	// requeueWaitSlow is the "workload may appear later" wait when no PVC is
	// selected at all.
	requeueWait     = time.Minute
	requeueWaitSlow = 5 * time.Minute

	reasonRepoNotReady = "RepoNotReady"
	reasonNoPVCs       = "NoPVCsSelected"
	reasonUnplaceable  = "PlacementFailed"
	reasonSecretCopy   = "SecretCopyFailed"
	reasonJobDeleted   = "JobDeleted"
	reasonBadResult    = "ResultUnreadable"
	reasonRunning      = "Running"
	reasonCompleted    = "Completed"
	reasonFailed       = "Failed"

	defaultAgentImage = "pbs-agent:dev"
)

// PBSBackupReconciler reconciles a PBSBackup object: it waits for the target
// PBSRepo to be Ready, resolves each selected PVC to the node holding its
// data, launches one node-pinned backup Job per node, and tracks the Jobs to
// a terminal phase.
type PBSBackupReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// AgentImage is the backup agent container image (--agent-image flag).
	AgentImage string
}

// +kubebuilder:rbac:groups=pbs.sharifmind.ir,resources=pbsbackups,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=pbs.sharifmind.ir,resources=pbsbackups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;watch
// The repo credentials secret is copied into each PBSBackup's namespace
// before Jobs are created (secrets are namespace-scoped).
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=create;update
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=pbs.sharifmind.ir,resources=pbsrepos,verbs=get;list;watch

// Reconcile walks the phase machine New → Scheduled → Running →
// Completed|Failed. Terminal phases no-op. See the helper docs for events,
// conditions, and requeue cadences.
func (r *PBSBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	var backup pbsv1.PBSBackup
	if err := r.Get(ctx, req.NamespacedName, &backup); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 1. Terminal phases are final: no requeue, no Job resurrection.
	if backup.Status.Phase == pbsv1.BackupPhaseCompleted || backup.Status.Phase == pbsv1.BackupPhaseFailed {
		return ctrl.Result{}, nil
	}

	// 2. Repo gate: the PBSRepo must exist and be Ready. PBS reachability is
	// the repo controller's job — this controller never talks to PBS in M1.
	var repo pbsv1.PBSRepo
	if err := r.Get(ctx, types.NamespacedName{Name: backup.Spec.RepoRef}, &repo); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		return r.hold(ctx, &backup, reasonRepoNotReady, "PBSRepo "+backup.Spec.RepoRef+" not found", requeueWait)
	}
	if !meta.IsStatusConditionTrue(repo.Status.Conditions, condReady) {
		return r.hold(ctx, &backup, reasonRepoNotReady, "PBSRepo "+repo.Name+" is not Ready", requeueWait)
	}

	// Secrets are namespace-scoped: the repo's credentials secret lives in
	// secretRef.namespace, but the Jobs (and their secretKeyRef env) run in
	// the backup's namespace — copy the contract keys across first.
	repoSecret, problem, err := r.ensureRepoSecret(ctx, &backup, &repo)
	if err != nil {
		return ctrl.Result{}, err
	}
	if problem != "" {
		return r.hold(ctx, &backup, reasonSecretCopy, problem, requeueWait)
	}

	// 3. PVC selection: spec.pvcs > spec.selector > all PVCs in the namespace.
	pvcs, problem, err := r.selectPVCs(ctx, &backup)
	if err != nil {
		return ctrl.Result{}, err
	}
	if problem != "" {
		return r.hold(ctx, &backup, reasonUnplaceable, problem, requeueWait)
	}
	if len(pvcs) == 0 {
		return r.hold(ctx, &backup, reasonNoPVCs, "no PVCs selected (the workload may appear later)", requeueWaitSlow)
	}

	// 4. Placement: map every PVC to a node (PV affinity, else a mounting pod).
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(backup.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	groups := map[string][]string{}
	for i := range pvcs {
		node, problem, err := r.placePVC(ctx, &backup, &pvcs[i], pods.Items)
		if err != nil {
			return ctrl.Result{}, err
		}
		if problem != "" {
			return r.hold(ctx, &backup, reasonUnplaceable, problem, requeueWait)
		}
		groups[node] = append(groups[node], pvcs[i].Name)
	}

	// 5-6. One deterministic, controller-owned Job per node; never duplicate,
	// never recreate a Job that vanished mid-run (TTL/GC owns cleanup).
	nodes := make([]string, 0, len(groups))
	for node := range groups {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	known := map[string]bool{}
	for _, js := range backup.Status.Jobs {
		known[js.Job] = true
	}
	placed := make([]placedJob, 0, len(nodes))
	for _, node := range nodes {
		name := jobName(backup.Name, node)
		sort.Strings(groups[node])
		job := &batchv1.Job{}
		switch err := r.Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: name}, job); {
		case err == nil:
			// Already launched.
		case apierrors.IsNotFound(err):
			if known[name] {
				return r.hold(ctx, &backup, reasonJobDeleted,
					"job "+name+" (node "+node+") was deleted before the backup finished; not recreating it",
					requeueWait)
			}
			job = backuplib.BuildBackupJob(backuplib.BackupJobSpec{
				Name:       name,
				Namespace:  backup.Namespace,
				Node:       node,
				RepoSecret: repoSecret,
				PVCs:       groups[node],
				Image:      r.agentImage(),
			})
			if err := ctrl.SetControllerReference(&backup, job, r.Scheme); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.Create(ctx, job); err != nil {
				if !apierrors.IsAlreadyExists(err) { // lost a create race: re-get
					return ctrl.Result{}, err
				}
				if err := r.Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: name}, job); err != nil {
					return ctrl.Result{}, err
				}
			}
		default:
			return ctrl.Result{}, err
		}
		placed = append(placed, placedJob{node: node, job: job})
	}

	// 7. Job states → backup phase: any Failed → Failed; any active →
	// Running; all Complete → collect the termination-log results.
	var failed *placedJob
	allComplete := true
	for i := range placed {
		switch {
		case jobCondition(placed[i].job, batchv1.JobFailed):
			if failed == nil {
				failed = &placed[i]
			}
		case !jobCondition(placed[i].job, batchv1.JobComplete):
			allComplete = false
		}
	}
	if failed != nil {
		return r.failBackup(ctx, &backup, reasonFailed,
			"job "+failed.job.Name+" (node "+failed.node+") failed", placed)
	}
	if !allComplete {
		return r.runBackup(ctx, &backup, placed)
	}
	return r.completeBackup(ctx, &backup, placed)
}

// placedJob pairs a launched Job with the node its PVCs live on.
type placedJob struct {
	node string
	job  *batchv1.Job
}

// repoSecretKeys is the exact agent env contract copied from the repo's
// credentials secret (mirrors internal/backup envContract).
var repoSecretKeys = []string{
	"host", "port", "datastore", "namespace",
	"tokenID", "tokenSecret", "fingerprint", "keyfile",
}

// ensureRepoSecret copies the repo's credentials secret into the backup's
// namespace as "pbsrepo-<repo>" (deterministic, recognizable), carrying only
// the contract keys verbatim. The copy is controller-owned by the backup (GCs
// with the CR) and is created-or-updated idempotently: keys are refreshed when
// they drift. Returns the copy's name; problem is a user-facing copy failure
// (e.g. the source secret vanished although the repo is Ready).
// ponytail: with several backups to the same repo in one namespace, the first
// backup owns the copy and it GCs with THAT CR; per-backup copies if that
// ever bites.
func (r *PBSBackupReconciler) ensureRepoSecret(ctx context.Context, b *pbsv1.PBSBackup, repo *pbsv1.PBSRepo) (name, problem string, err error) {
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
	switch err := r.Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: name}, existing); {
	case err == nil:
		if contractEqual(existing.Data, data) {
			return name, "", nil
		}
		existing.Data = data
		if err := r.Update(ctx, existing); err != nil {
			return "", "", err
		}
		return name, "", nil
	case apierrors.IsNotFound(err):
		copySec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: b.Namespace},
			Data:       data,
		}
		if err := ctrl.SetControllerReference(b, copySec, r.Scheme); err != nil {
			return "", "", err
		}
		if err := r.Create(ctx, copySec); err != nil && !apierrors.IsAlreadyExists(err) {
			return "", "", err
		}
		return name, "", nil
	default:
		return "", "", err
	}
}

// contractEqual compares two secret payloads on the repo contract keys only.
func contractEqual(a, b map[string][]byte) bool {
	for _, k := range repoSecretKeys {
		if !bytes.Equal(a[k], b[k]) {
			return false
		}
	}
	return true
}

// hold blocks progress without regressing the phase machine (a Running backup
// stays Running). Phase becomes Scheduled if still New; the Ready condition is
// False/<reason> with a transition-only warning event; requeue after wait.
func (r *PBSBackupReconciler) hold(ctx context.Context, b *pbsv1.PBSBackup, reason, message string, wait time.Duration) (ctrl.Result, error) {
	phase := pbsv1.BackupPhaseScheduled
	if b.Status.Phase == pbsv1.BackupPhaseRunning {
		phase = pbsv1.BackupPhaseRunning
	}
	transitioned, err := r.recordOutcome(ctx, b, phase, metav1.Condition{
		Type: condReady, Status: metav1.ConditionFalse, Reason: reason, Message: message,
	}, nil)
	if transitioned {
		r.Recorder.Event(b, corev1.EventTypeWarning, reason, message)
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: wait}, nil
}

// runBackup marks the backup Running (StartedAt set once) and refreshes
// status.Jobs. Owns(&batchv1.Job{}) drives completion updates; the requeue is
// only a lost-watch fallback.
func (r *PBSBackupReconciler) runBackup(ctx context.Context, b *pbsv1.PBSBackup, placed []placedJob) (ctrl.Result, error) {
	transitioned, err := r.recordOutcome(ctx, b, pbsv1.BackupPhaseRunning, metav1.Condition{
		Type: condReady, Status: metav1.ConditionFalse, Reason: reasonRunning,
		Message: "backup jobs running",
	}, func(s *pbsv1.PBSBackupStatus) {
		if s.StartedAt == nil {
			now := metav1.Now()
			s.StartedAt = &now
		}
		s.Jobs = jobStatuses(placed)
	})
	if transitioned {
		r.Recorder.Event(b, corev1.EventTypeNormal, reasonRunning, "backup jobs are running")
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueWait}, nil
}

// completeBackup collects every Job's termination-log JSON and finishes the
// backup. Single node: the parsed ref becomes status.SnapshotRef.
// ponytail: multi-node keeps refs per-job in Jobs[].State ("ok:<ref>");
// aggregating them into one SnapshotRef needs live multi-node runs first.
func (r *PBSBackupReconciler) completeBackup(ctx context.Context, b *pbsv1.PBSBackup, placed []placedJob) (ctrl.Result, error) {
	statuses := make([]pbsv1.BackupJobStatus, 0, len(placed))
	var total int64
	singleRef := ""
	for _, pj := range placed {
		ref, bytes, problem, err := r.jobResult(ctx, b.Namespace, pj.job)
		if err != nil {
			return ctrl.Result{}, err
		}
		if problem != "" {
			return r.failBackup(ctx, b, reasonBadResult, problem, placed)
		}
		total += bytes
		singleRef = ref
		statuses = append(statuses, pbsv1.BackupJobStatus{Node: pj.node, Job: pj.job.Name, State: "ok:" + ref})
	}
	message := fmt.Sprintf("%d node job(s) completed", len(placed))
	if len(placed) == 1 {
		message = "snapshot " + singleRef
	}
	transitioned, err := r.recordOutcome(ctx, b, pbsv1.BackupPhaseCompleted, metav1.Condition{
		Type: condReady, Status: metav1.ConditionTrue, Reason: reasonCompleted, Message: message,
	}, func(s *pbsv1.PBSBackupStatus) {
		s.Jobs = statuses
		s.Bytes = total
		if len(placed) == 1 {
			s.SnapshotRef = singleRef
		}
		if s.CompletedAt == nil {
			now := metav1.Now()
			s.CompletedAt = &now
		}
	})
	if transitioned {
		r.Recorder.Event(b, corev1.EventTypeNormal, reasonCompleted, message)
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil // terminal: no requeue
}

// failBackup marks the backup Failed (CompletedAt set once) with a
// transition-only warning event carrying the given message.
func (r *PBSBackupReconciler) failBackup(ctx context.Context, b *pbsv1.PBSBackup, reason, message string, placed []placedJob) (ctrl.Result, error) {
	transitioned, err := r.recordOutcome(ctx, b, pbsv1.BackupPhaseFailed, metav1.Condition{
		Type: condReady, Status: metav1.ConditionFalse, Reason: reason, Message: message,
	}, func(s *pbsv1.PBSBackupStatus) {
		s.Jobs = jobStatuses(placed)
		if s.CompletedAt == nil {
			now := metav1.Now()
			s.CompletedAt = &now
		}
	})
	if transitioned {
		r.Recorder.Event(b, corev1.EventTypeWarning, reason, message)
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil // terminal: no requeue
}

// recordOutcome stamps the phase and the Ready condition (plus any extra
// status mutations) and patches status only when something changed. It reports
// whether the condition content transitioned — callers gate events on it.
func (r *PBSBackupReconciler) recordOutcome(ctx context.Context, b *pbsv1.PBSBackup, phase pbsv1.BackupPhase, cond metav1.Condition, extra func(*pbsv1.PBSBackupStatus)) (bool, error) {
	before := b.DeepCopy()
	cond.ObservedGeneration = b.Generation
	transitioned := meta.SetStatusCondition(&b.Status.Conditions, cond)
	b.Status.Phase = phase
	if extra != nil {
		extra(&b.Status)
	}
	if reflect.DeepEqual(before.Status, b.Status) {
		return transitioned, nil
	}
	if err := r.Status().Patch(ctx, b, client.MergeFrom(before)); err != nil {
		return false, err
	}
	return transitioned, nil
}

// selectPVCs resolves the PVC selection precedence documented in the types:
// spec.pvcs list, else spec.selector match, else every PVC in the namespace.
// The problem string names a spec problem for the caller to hold on.
func (r *PBSBackupReconciler) selectPVCs(ctx context.Context, b *pbsv1.PBSBackup) ([]corev1.PersistentVolumeClaim, string, error) {
	switch {
	case len(b.Spec.PVCs) > 0:
		out := make([]corev1.PersistentVolumeClaim, 0, len(b.Spec.PVCs))
		for _, name := range b.Spec.PVCs {
			pvc := &corev1.PersistentVolumeClaim{}
			if err := r.Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: name}, pvc); err != nil {
				if apierrors.IsNotFound(err) {
					return nil, "PVC " + name + " not found in namespace " + b.Namespace, nil
				}
				return nil, "", err
			}
			out = append(out, *pvc)
		}
		return out, "", nil

	case b.Spec.Selector != nil:
		selector, err := metav1.LabelSelectorAsSelector(b.Spec.Selector)
		if err != nil {
			return nil, "invalid selector: " + err.Error(), nil
		}
		list := &corev1.PersistentVolumeClaimList{}
		if err := r.List(ctx, list, client.InNamespace(b.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
			return nil, "", err
		}
		return list.Items, "", nil

	default:
		list := &corev1.PersistentVolumeClaimList{}
		if err := r.List(ctx, list, client.InNamespace(b.Namespace)); err != nil {
			return nil, "", err
		}
		return list.Items, "", nil
	}
}

// placePVC maps one PVC to its node: a bound PV's local-path affinity first,
// then any scheduled pod mounting it (backup.ResolveNode). The problem string
// names the PVC so operators know which volume is stuck.
func (r *PBSBackupReconciler) placePVC(ctx context.Context, b *pbsv1.PBSBackup, pvc *corev1.PersistentVolumeClaim, pods []corev1.Pod) (node, problem string, err error) {
	if pvc.Spec.VolumeName == "" {
		return "", "PVC " + pvc.Name + " is not bound to a PersistentVolume yet", nil
	}
	pv := &corev1.PersistentVolume{}
	if err := r.Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, pv); err != nil {
		if apierrors.IsNotFound(err) {
			return "", "PV " + pvc.Spec.VolumeName + " for PVC " + pvc.Name + " not found", nil
		}
		return "", "", err
	}
	node, err = backuplib.ResolveNode(*pvc, pv, pods)
	if err != nil {
		return "", err.Error(), nil // ResolveNode's message names the PVC
	}
	return node, "", nil
}

// jobResult reads the agent's /dev/termination-log JSON from the Job's pod
// (found via the pbsbackup=<job> label):
//
//	{"snapshotRef":"host/<id>/<ISO>","bytes":123}
//
// The problem string reports a missing or unreadable result (agent contract
// violation); err is reserved for API failures.
func (r *PBSBackupReconciler) jobResult(ctx context.Context, ns string, job *batchv1.Job) (ref string, bytes int64, problem string, err error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(ns), client.MatchingLabels{"pbsbackup": job.Name}); err != nil {
		return "", 0, "", err
	}
	for _, pod := range pods.Items {
		for _, cs := range pod.Status.ContainerStatuses {
			msg := ""
			if cs.State.Terminated != nil {
				msg = cs.State.Terminated.Message
			}
			if msg == "" && cs.LastTerminationState.Terminated != nil {
				msg = cs.LastTerminationState.Terminated.Message
			}
			if msg == "" {
				continue
			}
			var res struct {
				SnapshotRef string `json:"snapshotRef"`
				Bytes       int64  `json:"bytes"`
			}
			if json.Unmarshal([]byte(msg), &res) != nil || res.SnapshotRef == "" {
				return "", 0, "job " + job.Name + ": unreadable termination log " + strconv.Quote(msg), nil
			}
			return res.SnapshotRef, res.Bytes, "", nil
		}
	}
	return "", 0, "job " + job.Name + ": no pod with a termination log found", nil
}

// jobStatuses refreshes status.Jobs from the live Job objects.
func jobStatuses(placed []placedJob) []pbsv1.BackupJobStatus {
	out := make([]pbsv1.BackupJobStatus, 0, len(placed))
	for _, pj := range placed {
		out = append(out, pbsv1.BackupJobStatus{Node: pj.node, Job: pj.job.Name, State: jobState(pj.job)})
	}
	return out
}

// jobState summarizes a Job's batch condition for status.Jobs.
func jobState(job *batchv1.Job) string {
	switch {
	case jobCondition(job, batchv1.JobFailed):
		return "failed"
	case jobCondition(job, batchv1.JobComplete):
		return "complete"
	default:
		return "active"
	}
}

// jobCondition reports whether the Job carries cond set to True.
func jobCondition(job *batchv1.Job, cond batchv1.JobConditionType) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == cond && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// jobName is the deterministic Job name for one node: "<backup>-<node>". It
// doubles as the pod hostname (the PBS backup-id), so it must fit the 63-char
// DNS-1123 label limit: overlong names are truncated and suffixed with an
// 8-hex FNV-1a hash of the full string, keeping two nodes that truncate to
// the same prefix distinct (and stable across reconciles).
func jobName(backup, node string) string {
	full := backup + "-" + node
	if len(full) <= 63 {
		return full
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(full))
	suffix := fmt.Sprintf("-%08x", h.Sum32())
	return strings.TrimRight(full[:63-len(suffix)], "-") + suffix
}

func (r *PBSBackupReconciler) agentImage() string {
	if r.AgentImage == "" {
		return defaultAgentImage
	}
	return r.AgentImage
}

// SetupWithManager sets up the controller with the Manager.
func (r *PBSBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pbsv1.PBSBackup{}).
		Owns(&batchv1.Job{}).
		Named("pbsbackup").
		Complete(r)
}
