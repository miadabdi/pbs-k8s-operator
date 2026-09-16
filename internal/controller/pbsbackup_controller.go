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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/recorder"

	pbsv1 "gitlab.sharifmind.ir/miad/pbs-operator/api/v1"
	// Aliased: the local variable "backup" in Reconcile shadows the package name.
	backuplib "gitlab.sharifmind.ir/miad/pbs-operator/internal/backup"
	"gitlab.sharifmind.ir/miad/pbs-operator/internal/hooks"

	"github.com/prometheus/client_golang/prometheus"
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
	reasonHookInvalid  = "HookInvalid"
	reasonHookFailed   = "HookFailed"
	reasonJobDeleted   = "JobDeleted"
	reasonBadResult    = "ResultUnreadable"
	reasonRunning      = "Running"
	reasonCompleted    = "Completed"
	reasonFailed       = "Failed"

	defaultAgentImage = "pbs-agent:dev"
)

// PBSBackupReconciler reconciles a PBSBackup object: it waits for the target
// PBSRepo to be Ready, serializes the namespace's API objects into a staging
// ConfigMap (M2), resolves each selected PVC to the node holding its data,
// launches one node-pinned backup Job per node, and tracks the Jobs to a
// terminal phase.
type PBSBackupReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder recorder.EventRecorder

	// AgentImage is the backup agent container image (--agent-image flag).
	AgentImage string

	// Serializer snapshots the backup namespace's API objects for api.pxar.
	// Nil disables API serialization (M1 mode: no staging ConfigMap, and the
	// zero-PVC gate reverts to "no PVCs → hold").
	Serializer *backuplib.Serializer

	// Metrics holds the operator's Prometheus vecs (M3); nil disables emission.
	Metrics *BackupMetrics

	// HookExecutor runs pre-backup hook commands inside target containers
	// (M4); main wires hooks.NewExecutor(mgr.GetConfig()).
	HookExecutor hooks.Executor
}

// BackupMetrics is the operator's Prometheus contract. Labels: namespace only.
// last_success and duration are SET (never added) on Completed transitions
// only; errors_total counts Failed transitions. A failure never touches
// last_success (prod BackupStale/BackupJobFailed compatibility).
type BackupMetrics struct {
	LastSuccess *prometheus.GaugeVec
	Duration    *prometheus.GaugeVec
	Errors      *prometheus.CounterVec
}

// NewBackupMetrics builds the metric vecs and registers them on reg (pass the
// manager's metrics registry in main).
func NewBackupMetrics(reg prometheus.Registerer) *BackupMetrics {
	m := &BackupMetrics{
		LastSuccess: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pbs_backup_last_success_timestamp_seconds",
			Help: "Unix timestamp of the last PBSBackup Completed transition in the namespace.",
		}, []string{"namespace"}),
		Duration: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pbs_backup_duration_seconds",
			Help: "CompletedAt-StartedAt seconds of the last successful PBSBackup in the namespace.",
		}, []string{"namespace"}),
		Errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pbs_backup_errors_total",
			Help: "PBSBackups that reached the Failed phase in the namespace.",
		}, []string{"namespace"}),
	}
	reg.MustRegister(m.LastSuccess, m.Duration, m.Errors)
	return m
}

// observeCompleted stamps the success gauges (terminal transition only).
func (r *PBSBackupReconciler) observeCompleted(b *pbsv1.PBSBackup) {
	if r.Metrics == nil || b.Status.CompletedAt == nil {
		return
	}
	r.Metrics.LastSuccess.WithLabelValues(b.Namespace).Set(float64(b.Status.CompletedAt.Unix()))
	if b.Status.StartedAt != nil {
		r.Metrics.Duration.WithLabelValues(b.Namespace).
			Set(b.Status.CompletedAt.Sub(b.Status.StartedAt.Time).Seconds())
	}
}

// observeFailed bumps the error counter (terminal transition only).
func (r *PBSBackupReconciler) observeFailed(b *pbsv1.PBSBackup) {
	if r.Metrics == nil {
		return
	}
	r.Metrics.Errors.WithLabelValues(b.Namespace).Inc()
}

// +kubebuilder:rbac:groups=pbs.sharifmind.ir,resources=pbsbackups,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=pbs.sharifmind.ir,resources=pbsbackups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// M4: pre-backup hooks exec into target pods through the pods/exec subresource.
// +kubebuilder:rbac:groups=core,resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;watch
// The repo credentials secret is copied into each PBSBackup's namespace
// before Jobs are created (secrets are namespace-scoped).
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=create;update
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// Events are emitted through the events.k8s.io/v1 recorder (mgr.GetEventRecorder).
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=pbs.sharifmind.ir,resources=pbsrepos,verbs=get;list;watch
// M2: namespace API serialization reads every namespaced object type.
// Accepted risk, stated explicitly: read-only get;list over ALL groups and
// resources — secrets included, in every namespace — auto-expanding with
// every newly installed CRD. Inherent to dynamic full-namespace
// serialization: a namespaced Role cannot express it, and the API-only node
// pick needs cluster-scoped nodes. Same posture as Velero-class backup
// operators; accepted deliberately for this operator.
// +kubebuilder:rbac:groups=*,resources=*,verbs=get;list
// firstNode lists Nodes through the manager's cached client: the Node
// informer needs watch on top of the wildcard's get;list (without it the
// watch 403s forever and the informer re-lists, and a cold informer can
// answer before its initial sync → spurious "no nodes found").
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch
// The staging ConfigMap (<backup>-api) carries the serialized api.yaml. The
// controller reads it through the manager's cached client, so the ConfigMap
// informer needs watch (same class as the Node rule above; without it the
// watch 403s and the reflector retries forever).
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;create;update;watch

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

	// 3.5 API serialization (M2): snapshot every API object in the backup's
	// namespace into a staging ConfigMap; the Job archives it as api.pxar. An
	// empty namespace yields no ConfigMap — the agent then skips api.pxar.
	staging := ""
	if r.Serializer != nil {
		apiYAML, err := r.Serializer.SerializeNamespace(ctx, backup.Namespace)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("serialize namespace %s: %w", backup.Namespace, err)
		}
		if apiYAML != "" {
			if staging, err = r.ensureStagingConfigMap(ctx, &backup, apiYAML); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	// Zero-PVC gate (M2 rule): hold only when there is nothing at all to back
	// up — no PVCs AND an empty serialization. API-only backups are valid.
	if len(pvcs) == 0 && staging == "" {
		return r.hold(ctx, &backup, reasonNoPVCs,
			"no PVCs selected and the namespace has no API objects to serialize (the workload may appear later)",
			requeueWaitSlow)
	}

	// 4. Placement: map every PVC to a node (PV affinity, else a mounting pod).
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(backup.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	groups := map[string][]string{}
	if len(pvcs) == 0 {
		// API-only backup: no volume pins the Job. Run the single Job on the
		// FIRST node alphabetically — deterministic across reconciles and
		// restarts.
		// ponytail: no readiness/cordon filtering; a NotReady first node
		// stalls the backup — filter on Ready if that ever bites.
		node, err := r.firstNode(ctx)
		if err != nil {
			return ctrl.Result{}, err
		}
		if node == "" {
			return r.hold(ctx, &backup, reasonUnplaceable,
				"no nodes found to run the API-only backup job", requeueWait)
		}
		groups[node] = nil
	}
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

	// 4.5 Pre-exec hooks (M4): before the FIRST Job creation, run every hooked
	// pod's command sequentially. status.Jobs non-empty means Jobs (and hooks)
	// already ran — re-reconciles must never re-exec hooks. Any hook problem
	// is terminal (Failed, no Jobs, no requeue).
	if len(backup.Status.Jobs) == 0 {
		problem, reason, err := r.runPreHooks(ctx, pvcs, pods.Items)
		if err != nil {
			return ctrl.Result{}, err
		}
		if problem != "" {
			return r.failBackup(ctx, &backup, reason, problem, nil)
		}
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
				Name:             name,
				Namespace:        backup.Namespace,
				Node:             node,
				RepoSecret:       repoSecret,
				PVCs:             groups[node],
				StagingConfigMap: staging,
				Notes:            backup.Spec.Notes,
				Image:            r.agentImage(),
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

// runPreHooks (M4) executes the pre-backup hook of every pod in the backup's
// namespace that mounts one of the selected PVCs, sequentially in pod-name
// order (deterministic across reconciles). Pods without hook annotations are
// untouched, as are hooked pods mounting unselected PVCs. An invalid
// annotation (HookInvalid) or a failed exec (HookFailed) is terminal: the
// returned problem/reason feed a Failed phase and no Jobs are created.
// ponytail: crash between hook exec and the status write re-execs hooks once
// on the next reconcile; per-backup hook-run bookkeeping if that ever bites.
func (r *PBSBackupReconciler) runPreHooks(ctx context.Context, pvcs []corev1.PersistentVolumeClaim, pods []corev1.Pod) (problem, reason string, err error) {
	selected := make(map[string]bool, len(pvcs))
	for i := range pvcs {
		selected[pvcs[i].Name] = true
	}
	names := make([]string, 0, len(pods))
	byName := make(map[string]*corev1.Pod, len(pods))
	for i := range pods {
		for _, vol := range pods[i].Spec.Volumes {
			if vol.PersistentVolumeClaim != nil && selected[vol.PersistentVolumeClaim.ClaimName] {
				names = append(names, pods[i].Name)
				byName[pods[i].Name] = &pods[i]
				break
			}
		}
	}
	sort.Strings(names)
	for _, name := range names {
		pod := byName[name]
		container, command, hasHook, perr := hooks.ParseHook(pod)
		if perr != nil {
			return "pod " + pod.Name + ": " + perr.Error(), reasonHookInvalid, nil
		}
		if !hasHook {
			continue
		}
		if r.HookExecutor == nil {
			return "pre-hook in pod " + pod.Name + " cannot run: no hook executor configured",
				reasonHookFailed, nil
		}
		if eerr := r.HookExecutor.Exec(ctx, pod.Namespace, pod.Name, container, command, hooks.HookTimeout); eerr != nil {
			return "pre-hook in pod " + pod.Name + " failed: " + eerr.Error(), reasonHookFailed, nil
		}
	}
	return "", "", nil
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
		// The managed label keeps the copy out of api.yaml (M2 serializer).
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
				Namespace: b.Namespace,
				Labels:    map[string]string{backuplib.ManagedLabel: "true"},
			},
			Data: data,
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

// ensureStagingConfigMap creates (or refreshes) the "<backup>-api" ConfigMap
// in the backup's namespace, key api.yaml = the serialized namespace. It is
// controller-owned (GCs with the CR) and managed-labeled (excluded from later
// serializations). Skip-if-identical: an unchanged payload never writes, so
// reconciles cannot hot-loop on it.
// ponytail: a ConfigMap tops out at 1 MiB — upgrade path: agent-side
// serialization with a read-only Job SA when namespaces outgrow it.
func (r *PBSBackupReconciler) ensureStagingConfigMap(ctx context.Context, b *pbsv1.PBSBackup, apiYAML string) (string, error) {
	name := b.Name + "-api"
	existing := &corev1.ConfigMap{}
	switch err := r.Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: name}, existing); {
	case err == nil:
		if existing.Data["api.yaml"] == apiYAML {
			return name, nil
		}
		existing.Data = map[string]string{"api.yaml": apiYAML}
		if err := r.Update(ctx, existing); err != nil {
			return "", err
		}
		return name, nil
	case apierrors.IsNotFound(err):
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: b.Namespace,
				Labels:    map[string]string{backuplib.ManagedLabel: "true"},
			},
			Data: map[string]string{"api.yaml": apiYAML},
		}
		if err := ctrl.SetControllerReference(b, cm, r.Scheme); err != nil {
			return "", err
		}
		if err := r.Create(ctx, cm); err != nil && !apierrors.IsAlreadyExists(err) {
			return "", err
		}
		return name, nil
	default:
		return "", err
	}
}

// firstNode returns the alphabetically first node name in the cluster, or ""
// when no Node objects exist (shared by the backup and restore reconcilers).
func (r *PBSBackupReconciler) firstNode(ctx context.Context) (string, error) {
	return firstNode(ctx, r.Client)
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
		r.Recorder.Eventf(b, nil, corev1.EventTypeWarning, reason, "", message)
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
		r.Recorder.Eventf(b, nil, corev1.EventTypeNormal, reasonRunning, "", "backup jobs are running")
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
		r.Recorder.Eventf(b, nil, corev1.EventTypeNormal, reasonCompleted, "", message)
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	r.observeCompleted(b)
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
		r.Recorder.Eventf(b, nil, corev1.EventTypeWarning, reason, "", message)
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	r.observeFailed(b)
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

// firstNode lists the cluster's nodes and returns the alphabetically first
// name, "" when none exist. Package-level: both reconcilers use it as the
// deterministic default placement.
func firstNode(ctx context.Context, c client.Client) (string, error) {
	nodes := &corev1.NodeList{}
	if err := c.List(ctx, nodes); err != nil {
		return "", err
	}
	if len(nodes.Items) == 0 {
		return "", nil
	}
	names := make([]string, 0, len(nodes.Items))
	for i := range nodes.Items {
		names = append(names, nodes.Items[i].Name)
	}
	sort.Strings(names)
	return names[0], nil
}
