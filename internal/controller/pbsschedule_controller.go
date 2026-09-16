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
	"reflect"
	"strconv"
	"time"

	"github.com/robfig/cron/v3"
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
)

const (
	reasonValid       = "Valid"
	reasonInvalidCron = "InvalidCron"
	reasonRepoMissing = "RepoMissing"
	reasonBackupMade  = "BackupCreated"

	// scheduleRequeueCap bounds the requeue cadence so clock/config drift is
	// noticed within a minute even for sparse crons.
	scheduleRequeueCap = time.Minute
)

// PBSScheduleReconciler reconciles a PBSSchedule: it validates the cron
// expression and the target repo, then creates a PBSBackup (from Template)
// whenever a fire time came due — firing ONCE per reconcile, catching up to
// the LATEST missed slot without backfilling. Schedules only create backups
// from the leader (reconcile-only creation under leader election).
type PBSScheduleReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// Now is the clock seam (envtest); nil → time.Now.
	Now func() time.Time
}

// +kubebuilder:rbac:groups=pbs.sharifmind.ir,resources=pbsschedules,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pbs.sharifmind.ir,resources=pbsschedules/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pbs.sharifmind.ir,resources=pbsschedules/finalizers,verbs=update
// The schedule controller creates the backups it templated and lists them by
// label to maintain status.active (creation carries the ownerRef).
// +kubebuilder:rbac:groups=pbs.sharifmind.ir,resources=pbsbackups,verbs=get;list;watch;create

// Reconcile: invalid cron → Ready=False/InvalidCron + 5m requeue; repo
// missing/not Ready → Ready=False/RepoMissing + 1m requeue; otherwise compute
// the next fire, create a backup when due (unless suspended), refresh
// LastScheduleTime/NextScheduleTime/Active, and requeue at min(next fire, 1m).
func (r *PBSScheduleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	var schedule pbsv1.PBSSchedule
	if err := r.Get(ctx, req.NamespacedName, &schedule); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	orig := schedule.DeepCopy() // status snapshot: patches diff against this

	// 1. Cron gate: an unparsable schedule can never fire.
	sched, err := cron.ParseStandard(schedule.Spec.Schedule)
	if err != nil {
		return r.hold(ctx, &schedule, reasonInvalidCron,
			"invalid cron expression "+strconv.Quote(schedule.Spec.Schedule)+": "+err.Error(),
			5*time.Minute)
	}

	// 2. Repo gate: the target PBSRepo must exist and be Ready.
	var repo pbsv1.PBSRepo
	switch err := r.Get(ctx, types.NamespacedName{Name: schedule.Spec.RepoRef}, &repo); {
	case apierrors.IsNotFound(err):
		return r.hold(ctx, &schedule, reasonRepoMissing,
			"PBSRepo "+schedule.Spec.RepoRef+" not found", time.Minute)
	case err != nil:
		return ctrl.Result{}, err
	case !meta.IsStatusConditionTrue(repo.Status.Conditions, condReady):
		return r.hold(ctx, &schedule, reasonRepoMissing,
			"PBSRepo "+repo.Name+" is not Ready", time.Minute)
	}

	now := r.now()

	// 3. Fire logic: the slot after max(creation, LastScheduleTime) is due
	// when it is <= now; multiple missed slots collapse into the LATEST one
	// (one backup per reconcile catch-up, never a backfill).
	base := schedule.CreationTimestamp.Time
	if schedule.Status.LastScheduleTime != nil && schedule.Status.LastScheduleTime.After(base) {
		base = schedule.Status.LastScheduleTime.Time
	}
	if fire := sched.Next(base); !fire.After(now) {
		// ponytail: O(missed slots) catch-up loop — a */1 cron stale for a
		// year iterates ~525k cheap Next() calls once; switch to chunked
		// advancement if that ever matters.
		for next := sched.Next(fire); !next.After(now); next = sched.Next(fire) {
			fire = next
		}
		if !schedule.Spec.Suspend {
			if _, err := r.fireBackup(ctx, &schedule, fire); err != nil {
				return ctrl.Result{}, err
			}
			schedule.Status.LastScheduleTime = &metav1.Time{Time: fire}
		}
	}

	// 4. Status refresh: next fire (observability) + active backups.
	next := sched.Next(now)
	schedule.Status.NextScheduleTime = &metav1.Time{Time: next}
	active, err := r.activeBackups(ctx, &schedule)
	if err != nil {
		return ctrl.Result{}, err
	}
	schedule.Status.Active = active
	if err := r.patchStatus(ctx, orig, &schedule); err != nil {
		return ctrl.Result{}, err
	}

	// 5. Requeue: min(until next fire, 1m).
	wait := next.Sub(r.now())
	if wait <= 0 || wait > scheduleRequeueCap {
		wait = scheduleRequeueCap
	}
	return ctrl.Result{RequeueAfter: wait}, nil
}

// fireBackup creates the schedule's PBSBackup for the given slot. The name
// is deterministic per slot ("<schedule>-<unix>") — the CronJob idempotency
// trick: the manager's CACHED client can serve a stale LastScheduleTime right
// after a fire (watch propagation lag), and a second reconcile then re-enters
// the same slot; a deterministic name turns that duplicate create into an
// ignored AlreadyExists instead of a second backup. Labeled pbsschedule=<name>,
// controller-owned (GC with the schedule), spec from Template + RepoRef +
// Notes. Reports whether THIS call created the backup (false = already there).
func (r *PBSScheduleReconciler) fireBackup(ctx context.Context, schedule *pbsv1.PBSSchedule, fire time.Time) (bool, error) {
	backup := &pbsv1.PBSBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", schedule.Name, fire.Unix()),
			Namespace: schedule.Namespace,
			Labels:    map[string]string{"pbsschedule": schedule.Name},
		},
		Spec: pbsv1.PBSBackupSpec{
			RepoRef:  schedule.Spec.RepoRef,
			PVCs:     schedule.Spec.Template.PVCs,
			Selector: schedule.Spec.Template.Selector,
			Notes:    schedule.Spec.Template.Notes,
		},
	}
	if err := ctrl.SetControllerReference(schedule, backup, r.Scheme); err != nil {
		return false, err
	}
	if err := r.Create(ctx, backup); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return false, nil // stale-cache re-entry: the slot is already served
		}
		return false, err
	}
	r.Recorder.Event(schedule, corev1.EventTypeNormal, reasonBackupMade,
		"created PBSBackup "+backup.Name+" for fire time "+fire.Format(time.RFC3339))
	return true, nil
}

// activeBackups lists the schedule's labeled PBSBackups and returns the names
// of those still non-terminal (Completed/Failed drop out).
func (r *PBSScheduleReconciler) activeBackups(ctx context.Context, schedule *pbsv1.PBSSchedule) ([]string, error) {
	list := &pbsv1.PBSBackupList{}
	if err := r.List(ctx, list,
		client.InNamespace(schedule.Namespace),
		client.MatchingLabels{"pbsschedule": schedule.Name}); err != nil {
		return nil, err
	}
	var out []string
	for i := range list.Items {
		b := &list.Items[i]
		if b.Status.Phase != pbsv1.BackupPhaseCompleted && b.Status.Phase != pbsv1.BackupPhaseFailed {
			out = append(out, b.Name)
		}
	}
	return out, nil
}

// hold marks Ready=False/<reason> with a transition-only warning event and
// requeues after wait; the fire computation is skipped entirely.
func (r *PBSScheduleReconciler) hold(ctx context.Context, schedule *pbsv1.PBSSchedule, reason, message string, wait time.Duration) (ctrl.Result, error) {
	before := schedule.DeepCopy()
	cond := metav1.Condition{
		Type:    condReady,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: message,
	}
	cond.ObservedGeneration = schedule.Generation
	transitioned := meta.SetStatusCondition(&schedule.Status.Conditions, cond)
	if err := r.patchStatusIfChanged(ctx, before, schedule); err != nil {
		return ctrl.Result{}, err
	}
	if transitioned {
		r.Recorder.Event(schedule, corev1.EventTypeWarning, reason, message)
	}
	return ctrl.Result{RequeueAfter: wait}, nil
}

// patchStatus stamps Ready=True/Valid (with ObservedGeneration) and patches
// status only when something changed (dirty-gating keeps watches quiet).
// before is the pre-Reconcile snapshot: the merge patch must carry ALL status
// mutations (fire time, active list), not just the condition.
func (r *PBSScheduleReconciler) patchStatus(ctx context.Context, before, schedule *pbsv1.PBSSchedule) error {
	cond := metav1.Condition{
		Type:    condReady,
		Status:  metav1.ConditionTrue,
		Reason:  reasonValid,
		Message: "schedule is valid; next fire " + schedule.Status.NextScheduleTime.Format(time.RFC3339),
	}
	if schedule.Spec.Suspend {
		cond.Message = "schedule is suspended; next fire " + schedule.Status.NextScheduleTime.Format(time.RFC3339)
	}
	cond.ObservedGeneration = schedule.Generation
	meta.SetStatusCondition(&schedule.Status.Conditions, cond)
	return r.patchStatusIfChanged(ctx, before, schedule)
}

// patchStatusIfChanged patches the status subresource when it differs.
func (r *PBSScheduleReconciler) patchStatusIfChanged(ctx context.Context, before, after *pbsv1.PBSSchedule) error {
	if reflect.DeepEqual(before.Status, after.Status) {
		return nil
	}
	return r.Status().Patch(ctx, after, client.MergeFrom(before))
}

// now reads the clock seam.
func (r *PBSScheduleReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// SetupWithManager sets up the controller with the Manager. Owns(PBSBackup)
// re-reconciles the schedule when a fired backup changes phase, keeping
// status.active fresh.
func (r *PBSScheduleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pbsv1.PBSSchedule{}).
		Owns(&pbsv1.PBSBackup{}).
		Named("pbsschedule").
		Complete(r)
}
