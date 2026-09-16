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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/robfig/cron/v3"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pbsv1 "gitlab.sharifmind.ir/miad/pbs-operator/api/v1"
)

// makeSchedule creates a PBSSchedule in ns from spec.
func makeSchedule(ctx context.Context, name, ns string, spec pbsv1.PBSScheduleSpec) *pbsv1.PBSSchedule {
	s := &pbsv1.PBSSchedule{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       spec,
	}
	Expect(k8sClient.Create(ctx, s)).To(Succeed())
	return s
}

// fetchSchedule fetches the current schedule state.
func fetchSchedule(ctx context.Context, ns, name string) *pbsv1.PBSSchedule {
	s := &pbsv1.PBSSchedule{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, s)).To(Succeed())
	return s
}

// reconcileSchedule runs one reconcile with the given clock seam.
func reconcileSchedule(ctx context.Context, ns, name string, rec *fakeEventRecorder, now func() time.Time) reconcile.Result {
	r := &PBSScheduleReconciler{
		Client:   k8sClient,
		Scheme:   k8sClient.Scheme(),
		Recorder: rec,
		Now:      now,
	}
	res, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: ns, Name: name},
	})
	Expect(err).NotTo(HaveOccurred())
	return res
}

// scheduleBackups lists the PBSBackups a schedule created (ownerRef UID).
func scheduleBackups(ctx context.Context, s *pbsv1.PBSSchedule) []pbsv1.PBSBackup {
	all := &pbsv1.PBSBackupList{}
	Expect(k8sClient.List(ctx, all, client.InNamespace(s.Namespace), client.MatchingLabels{"pbsschedule": s.Name})).To(Succeed())
	var out []pbsv1.PBSBackup
	for i := range all.Items {
		for _, ref := range all.Items[i].OwnerReferences {
			if ref.UID == s.UID {
				out = append(out, all.Items[i])
			}
		}
	}
	return out
}

// fixedClock returns a clock seam pinned at t.
func fixedClock(t time.Time) func() time.Time { return func() time.Time { return t } }

// readyCondStatus extracts (status, reason) of the Ready condition.
func readyCondStatus(conds []metav1.Condition) (string, string) {
	for _, c := range conds {
		if c.Type == condReady {
			return string(c.Status), c.Reason
		}
	}
	return "", ""
}

var _ = Describe("PBSSchedule Controller", func() {
	ctx := context.Background()

	// A schedule spec pointing at a ready repo with an hourly slot.
	hourly := func(repo string) pbsv1.PBSScheduleSpec {
		return pbsv1.PBSScheduleSpec{
			RepoRef:  repo,
			Schedule: "0 * * * *",
			Template: pbsv1.PBSBackupTemplate{
				PVCs:  []string{"pg-data"},
				Notes: `{"keep-daily":7}`,
			},
		}
	}

	It("1. invalid cron → Ready=False/InvalidCron, warning event, requeue 5m, no backups", func() {
		makeNamespace(ctx, "sched-1")
		makeSchedule(ctx, "sched-1", "sched-1", hourly("whatever"))

		// "99 * * * *" passes the CRD's 5-token pattern but fails cron parsing.
		s := fetchSchedule(ctx, "sched-1", "sched-1")
		s.Spec.Schedule = "99 * * * *"
		Expect(k8sClient.Update(ctx, s)).To(Succeed())

		rec := newFakeEventRecorder(16)
		res := reconcileSchedule(ctx, "sched-1", "sched-1", rec, fixedClock(time.Now()))

		Expect(res.RequeueAfter).To(Equal(5 * time.Minute))
		got := fetchSchedule(ctx, "sched-1", "sched-1")
		status, reason := readyCondStatus(got.Status.Conditions)
		Expect(status).To(Equal("False"))
		Expect(reason).To(Equal("InvalidCron"))
		Expect(scheduleBackups(ctx, got)).To(BeEmpty())
		events := drainEvents(rec)
		Expect(events).To(HaveLen(1))
		Expect(events[0]).To(ContainSubstring("99 * * * *"))

		// Re-reconcile: same condition → no duplicate event.
		reconcileSchedule(ctx, "sched-1", "sched-1", rec, fixedClock(time.Now()))
		Expect(drainEvents(rec)).To(BeEmpty())
	})

	It("2. repo missing → Ready=False/RepoMissing, requeue 1m", func() {
		makeNamespace(ctx, "sched-2")
		makeSchedule(ctx, "sched-2", "sched-2", hourly("no-such-repo"))

		rec := newFakeEventRecorder(16)
		res := reconcileSchedule(ctx, "sched-2", "sched-2", rec, fixedClock(time.Now()))

		Expect(res.RequeueAfter).To(Equal(time.Minute))
		got := fetchSchedule(ctx, "sched-2", "sched-2")
		status, reason := readyCondStatus(got.Status.Conditions)
		Expect(status).To(Equal("False"))
		Expect(reason).To(Equal("RepoMissing"))
		Expect(scheduleBackups(ctx, got)).To(BeEmpty())
		Expect(drainEvents(rec)).To(HaveLen(1))
	})

	It("2b. repo present but not Ready → RepoMissing too", func() {
		makeNamespace(ctx, "sched-2b")
		repo := makeReadyRepo(ctx, "sched-2b-repo")
		repo.Status.Conditions[0].Status = metav1.ConditionFalse
		Expect(k8sClient.Status().Update(ctx, repo)).To(Succeed())
		makeSchedule(ctx, "sched-2b", "sched-2b", hourly(repo.Name))

		res := reconcileSchedule(ctx, "sched-2b", "sched-2b", newFakeEventRecorder(16), fixedClock(time.Now()))

		Expect(res.RequeueAfter).To(Equal(time.Minute))
		_, reason := readyCondStatus(fetchSchedule(ctx, "sched-2b", "sched-2b").Status.Conditions)
		Expect(reason).To(Equal("RepoMissing"))
	})

	It("3. valid, not due yet → Ready=True/Valid, no backup, NextScheduleTime set, requeue capped at 1m", func() {
		makeNamespace(ctx, "sched-3")
		repo := makeReadyRepo(ctx, "sched-3-repo")
		makeSchedule(ctx, "sched-3", "sched-3", hourly(repo.Name))

		// Created "now": the next hourly slot is at most 60m out, so the
		// 1m requeue cap applies.
		now := time.Now().Truncate(time.Second)
		rec := newFakeEventRecorder(16)
		res := reconcileSchedule(ctx, "sched-3", "sched-3", rec, fixedClock(now))

		Expect(res.RequeueAfter).To(Equal(time.Minute))
		got := fetchSchedule(ctx, "sched-3", "sched-3")
		status, reason := readyCondStatus(got.Status.Conditions)
		Expect(status).To(Equal("True"))
		Expect(reason).To(Equal("Valid"))
		Expect(got.Status.LastScheduleTime).To(BeNil())
		Expect(scheduleBackups(ctx, got)).To(BeEmpty())
		// Next fire is the next full hour strictly after now.
		Expect(got.Status.NextScheduleTime).NotTo(BeNil())
		Expect(got.Status.NextScheduleTime.After(now)).To(BeTrue())
	})

	It("4. due → PBSBackup created from template (ownerRef, label, notes, generateName), LastScheduleTime = fire time", func() {
		makeNamespace(ctx, "sched-4")
		repo := makeReadyRepo(ctx, "sched-4-repo")
		s := makeSchedule(ctx, "sched-4", "sched-4", hourly(repo.Name))

		// Creation at T0; clock 90m later → the T0+60m slot is due.
		t0 := s.CreationTimestamp.Time.Truncate(time.Second)
		now := t0.Add(90 * time.Minute)
		rec := newFakeEventRecorder(16)
		reconcileSchedule(ctx, "sched-4", "sched-4", rec, fixedClock(now))

		created := scheduleBackups(ctx, fetchSchedule(ctx, "sched-4", "sched-4"))
		Expect(created).To(HaveLen(1))
		b := created[0]
		Expect(b.Name).To(HavePrefix("sched-4-"))
		Expect(b.Spec.RepoRef).To(Equal(repo.Name))
		Expect(b.Spec.PVCs).To(Equal([]string{"pg-data"}))
		Expect(b.Spec.Notes).To(Equal(`{"keep-daily":7}`))
		refs := b.GetOwnerReferences()
		Expect(refs).To(HaveLen(1))
		Expect(refs[0].UID).To(Equal(s.UID))
		Expect(*refs[0].Controller).To(BeTrue())

		got := fetchSchedule(ctx, "sched-4", "sched-4")
		Expect(got.Status.LastScheduleTime).NotTo(BeNil())
		// The recorded slot is the latest fire time <= now (hour-aligned by
		// the cron library, so compare temporally, not structurally).
		sched, err := cron.ParseStandard("0 * * * *")
		Expect(err).NotTo(HaveOccurred())
		fire := got.Status.LastScheduleTime.Time
		Expect(fire).To(BeTemporally("<=", now))
		Expect(fire).To(BeTemporally(">", t0))
		Expect(sched.Next(fire)).To(BeTemporally(">", now))
		// The backup name encodes the slot (per-fire idempotency).
		Expect(b.Name).To(Equal(fmt.Sprintf("sched-4-%d", fire.Unix())))
		Expect(got.Status.Active).To(ConsistOf(b.Name))
		events := drainEvents(rec)
		Expect(events).To(HaveLen(1))
		Expect(events[0]).To(ContainSubstring(b.Name))
	})

	It("5. missed two fires → exactly ONE backup (catch-up = latest, no backfill)", func() {
		makeNamespace(ctx, "sched-5")
		repo := makeReadyRepo(ctx, "sched-5-repo")
		s := makeSchedule(ctx, "sched-5", "sched-5", hourly(repo.Name))

		t0 := s.CreationTimestamp.Time.Truncate(time.Second)
		now := t0.Add(150 * time.Minute) // two hourly slots missed by then
		reconcileSchedule(ctx, "sched-5", "sched-5", newFakeEventRecorder(16), fixedClock(now))

		got := fetchSchedule(ctx, "sched-5", "sched-5")
		Expect(scheduleBackups(ctx, got)).To(HaveLen(1))
		// The LATEST missed slot is the one recorded: a next fire after it
		// lies in the future — had the FIRST been recorded, it would not.
		sched, err := cron.ParseStandard("0 * * * *")
		Expect(err).NotTo(HaveOccurred())
		fire := got.Status.LastScheduleTime.Time
		Expect(fire).To(BeTemporally("<=", now))
		Expect(sched.Next(fire)).To(BeTemporally(">", now))
	})

	It("6. second reconcile after a fire → no duplicate", func() {
		makeNamespace(ctx, "sched-6")
		repo := makeReadyRepo(ctx, "sched-6-repo")
		s := makeSchedule(ctx, "sched-6", "sched-6", hourly(repo.Name))

		t0 := s.CreationTimestamp.Time.Truncate(time.Second)
		now := t0.Add(90 * time.Minute)
		reconcileSchedule(ctx, "sched-6", "sched-6", newFakeEventRecorder(16), fixedClock(now))
		reconcileSchedule(ctx, "sched-6", "sched-6", newFakeEventRecorder(16), fixedClock(now.Add(time.Minute)))

		Expect(scheduleBackups(ctx, fetchSchedule(ctx, "sched-6", "sched-6"))).To(HaveLen(1))
	})

	It("7. Suspend=true and due → no creation; NextScheduleTime still computed", func() {
		makeNamespace(ctx, "sched-7")
		repo := makeReadyRepo(ctx, "sched-7-repo")
		s := makeSchedule(ctx, "sched-7", "sched-7", hourly(repo.Name))
		s.Spec.Suspend = true
		Expect(k8sClient.Update(ctx, s)).To(Succeed())

		t0 := s.CreationTimestamp.Time.Truncate(time.Second)
		now := t0.Add(90 * time.Minute)
		reconcileSchedule(ctx, "sched-7", "sched-7", newFakeEventRecorder(16), fixedClock(now))

		got := fetchSchedule(ctx, "sched-7", "sched-7")
		Expect(scheduleBackups(ctx, got)).To(BeEmpty())
		Expect(got.Status.LastScheduleTime).To(BeNil())
		Expect(got.Status.NextScheduleTime).NotTo(BeNil())
		_, reason := readyCondStatus(got.Status.Conditions)
		Expect(reason).To(Equal("Valid"))
	})

	It("8. Active drops the backup once terminal", func() {
		makeNamespace(ctx, "sched-8")
		repo := makeReadyRepo(ctx, "sched-8-repo")
		s := makeSchedule(ctx, "sched-8", "sched-8", hourly(repo.Name))

		t0 := s.CreationTimestamp.Time.Truncate(time.Second)
		now := t0.Add(90 * time.Minute)
		reconcileSchedule(ctx, "sched-8", "sched-8", newFakeEventRecorder(16), fixedClock(now))
		got := fetchSchedule(ctx, "sched-8", "sched-8")
		Expect(got.Status.Active).To(HaveLen(1))

		// Mark the created backup Completed the way the backup controller
		// would, then re-reconcile the schedule.
		created := scheduleBackups(ctx, got)
		created[0].Status.Phase = pbsv1.BackupPhaseCompleted
		Expect(k8sClient.Status().Update(ctx, &created[0])).To(Succeed())
		reconcileSchedule(ctx, "sched-8", "sched-8", newFakeEventRecorder(16), fixedClock(now))

		Expect(fetchSchedule(ctx, "sched-8", "sched-8").Status.Active).To(BeEmpty())
	})
})
