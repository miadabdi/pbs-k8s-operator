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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pbsv1 "gitlab.sharifmind.ir/miad/pbs-operator/api/v1"
	"gitlab.sharifmind.ir/miad/pbs-operator/internal/pbs/pbstest"
)

const (
	producerTokenID      = "operator@pbs!producer"
	producerTokenSecret  = "prod-secret-1"
	bootstrapTokenID     = "bootstrap@pbs!boot"
	bootstrapTokenSecret = "boot-secret-1"
)

var wrongFingerprint = strings.TrimRight(strings.Repeat("00:", 32), ":")

// newTestRepo builds a cluster-scoped PBSRepo pointing at the fake PBS.
func newTestRepo(name string, srv *pbstest.Server) *pbsv1.PBSRepo {
	fp := wrongFingerprint
	if srv != nil {
		fp = srv.Fingerprint()
	}
	return &pbsv1.PBSRepo{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: pbsv1.PBSRepoSpec{
			Host:        "127.0.0.1",
			Datastore:   "test-store",
			Namespace:   "ns-one",
			Fingerprint: fp,
			Port:        8007,
			SecretRef:   pbsv1.NamespacedSecretRef{Name: name + "-creds", Namespace: "default"},
		},
	}
}

func createSecret(ctx context.Context, name, ns string, keys map[string][]byte) *corev1.Secret {
	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data:       keys,
	}
	Expect(k8sClient.Create(ctx, s)).To(Succeed())
	return s
}

func credsKeys(tokenID, tokenSecret, fingerprint string) map[string][]byte {
	return map[string][]byte{
		"tokenID":     []byte(tokenID),
		"tokenSecret": []byte(tokenSecret),
		"fingerprint": []byte(fingerprint),
	}
}

func drainEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func fetchRepo(ctx context.Context, name string) *pbsv1.PBSRepo {
	repo := &pbsv1.PBSRepo{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, repo)).To(Succeed())
	return repo
}

func readyCondition(ctx context.Context, name string) *metav1.Condition {
	return meta.FindStatusCondition(fetchRepo(ctx, name).Status.Conditions, "Ready")
}

func doReconcile(ctx context.Context, name string, rec record.EventRecorder) reconcile.Result {
	r := &PBSRepoReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: rec}
	result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
	Expect(err).NotTo(HaveOccurred())
	return result
}

var _ = Describe("PBSRepo Controller", func() {
	ctx := context.Background()

	BeforeEach(func() {
		// The operator-namespace fallback scenario needs this namespace to exist.
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "pbs-operator-system"}}
		err := k8sClient.Create(ctx, ns)
		Expect(err == nil || apierrors.IsAlreadyExists(err)).To(BeTrue())
	})

	It("missing secret → Ready=False SecretMissing, event, requeue 2m", func() {
		repo := newTestRepo("repo-secret-missing", nil)
		repo.Spec.Host = "127.0.0.1" // unreachable anyway; must not be probed
		Expect(k8sClient.Create(ctx, repo)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, fetchRepo(ctx, repo.Name))).To(Succeed()) })

		rec := record.NewFakeRecorder(16)
		res := doReconcile(ctx, repo.Name, rec)

		Expect(res.RequeueAfter).To(Equal(2 * time.Minute))
		cond := readyCondition(ctx, repo.Name)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("SecretMissing"))
		Expect(cond.Message).To(ContainSubstring(repo.Spec.SecretRef.Name))
		events := drainEvents(rec)
		Expect(events).To(HaveLen(1))
		Expect(events[0]).To(ContainSubstring("SecretMissing"))
	})

	It("secret missing a contract key → SecretMissing names the key", func() {
		repo := newTestRepo("repo-key-missing", nil)
		Expect(k8sClient.Create(ctx, repo)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, fetchRepo(ctx, repo.Name))).To(Succeed()) })

		secret := credsKeys(producerTokenID, producerTokenSecret, wrongFingerprint)
		delete(secret, "tokenSecret")
		createSecret(ctx, repo.Spec.SecretRef.Name, "default", secret)

		rec := record.NewFakeRecorder(16)
		res := doReconcile(ctx, repo.Name, rec)

		Expect(res.RequeueAfter).To(Equal(2 * time.Minute))
		cond := readyCondition(ctx, repo.Name)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal("SecretMissing"))
		Expect(cond.Message).To(ContainSubstring("tokenSecret"))
	})

	It("wrong fingerprint → Ready=False Unreachable, event, requeue 2m", func() {
		srv := pbstest.Start()
		DeferCleanup(srv.Close)
		repo := newTestRepo("repo-bad-fp", srv)
		repo.Spec.Host, repo.Spec.Port, repo.Spec.Fingerprint = srv.Host(), int32(srv.Port()), wrongFingerprint
		Expect(k8sClient.Create(ctx, repo)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, fetchRepo(ctx, repo.Name))).To(Succeed()) })
		createSecret(ctx, repo.Spec.SecretRef.Name, "default", credsKeys(producerTokenID, producerTokenSecret, wrongFingerprint))

		rec := record.NewFakeRecorder(16)
		res := doReconcile(ctx, repo.Name, rec)

		Expect(res.RequeueAfter).To(Equal(2 * time.Minute))
		cond := readyCondition(ctx, repo.Name)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("Unreachable"))
		Expect(cond.Message).To(ContainSubstring("fingerprint"))
		Expect(drainEvents(rec)).To(HaveLen(1))
	})

	It("healthy + bootstrap → Ready=True Reachable, namespace POST carries the bootstrap token", func() {
		srv := pbstest.Start()
		DeferCleanup(srv.Close)
		repo := newTestRepo("repo-healthy-boot", srv)
		repo.Spec.Host, repo.Spec.Port = srv.Host(), int32(srv.Port())
		repo.Spec.BootstrapTokenRef = pbsv1.NamespacedSecretRef{Name: repo.Name + "-boot", Namespace: "default"}
		Expect(k8sClient.Create(ctx, repo)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, fetchRepo(ctx, repo.Name))).To(Succeed()) })
		createSecret(ctx, repo.Spec.SecretRef.Name, "default", credsKeys(producerTokenID, producerTokenSecret, srv.Fingerprint()))
		createSecret(ctx, repo.Spec.BootstrapTokenRef.Name, "default",
			map[string][]byte{"tokenID": []byte(bootstrapTokenID), "tokenSecret": []byte(bootstrapTokenSecret)})

		before := time.Now()
		rec := record.NewFakeRecorder(16)
		res := doReconcile(ctx, repo.Name, rec)

		Expect(res.RequeueAfter).To(Equal(5 * time.Minute))
		cond := readyCondition(ctx, repo.Name)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal("Reachable"))
		fetched := fetchRepo(ctx, repo.Name)
		Expect(fetched.Status.LastProbeTime).NotTo(BeNil())
		Expect(fetched.Status.LastProbeTime.Time).To(BeTemporally(">=", before.Truncate(time.Second)))

		auths := srv.NamespaceAuthHeaders()
		Expect(auths).To(HaveLen(1))
		Expect(auths[0]).To(Equal("PBSAPIToken=" + bootstrapTokenID + ":" + bootstrapTokenSecret))
		Expect(auths[0]).NotTo(ContainSubstring(producerTokenID))
		Expect(drainEvents(rec)).To(HaveLen(1))
	})

	It("bootstrap unset → no namespace POST, still Reachable", func() {
		srv := pbstest.Start()
		DeferCleanup(srv.Close)
		repo := newTestRepo("repo-healthy-noboot", srv)
		repo.Spec.Host, repo.Spec.Port = srv.Host(), int32(srv.Port())
		Expect(k8sClient.Create(ctx, repo)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, fetchRepo(ctx, repo.Name))).To(Succeed()) })
		createSecret(ctx, repo.Spec.SecretRef.Name, "default", credsKeys(producerTokenID, producerTokenSecret, srv.Fingerprint()))

		res := doReconcile(ctx, repo.Name, record.NewFakeRecorder(16))

		Expect(res.RequeueAfter).To(Equal(5 * time.Minute))
		Expect(readyCondition(ctx, repo.Name).Reason).To(Equal("Reachable"))
		Expect(srv.NamespaceAuthHeaders()).To(BeEmpty())
	})

	It("transitions emit events; unchanged repeat reconcile is a no-op (no hot-loop)", func() {
		srv := pbstest.Start()
		DeferCleanup(srv.Close)
		repo := newTestRepo("repo-transitions", srv)
		repo.Spec.Host, repo.Spec.Port = srv.Host(), int32(srv.Port())
		Expect(k8sClient.Create(ctx, repo)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, fetchRepo(ctx, repo.Name))).To(Succeed()) })

		rec := record.NewFakeRecorder(64)
		// 1st: secret absent → SecretMissing transition (event 1).
		res := doReconcile(ctx, repo.Name, rec)
		Expect(res.RequeueAfter).To(Equal(2 * time.Minute))
		Expect(readyCondition(ctx, repo.Name).Reason).To(Equal("SecretMissing"))

		// 2nd: secret appears → Reachable transition (event 2).
		createSecret(ctx, repo.Spec.SecretRef.Name, "default", credsKeys(producerTokenID, producerTokenSecret, srv.Fingerprint()))
		res = doReconcile(ctx, repo.Name, rec)
		Expect(res.RequeueAfter).To(Equal(5 * time.Minute))
		Expect(readyCondition(ctx, repo.Name).Reason).To(Equal("Reachable"))
		Expect(drainEvents(rec)).To(HaveLen(2))

		// 3rd: nothing changed → no event, no status patch (resourceVersion stable).
		rv := fetchRepo(ctx, repo.Name).ResourceVersion
		res = doReconcile(ctx, repo.Name, rec)
		Expect(res.RequeueAfter).To(Equal(5 * time.Minute))
		Expect(fetchRepo(ctx, repo.Name).ResourceVersion).To(Equal(rv))
		Expect(drainEvents(rec)).To(BeEmpty())
	})

	It("bootstrap namespace creation fails → BootstrapFailed, requeue 5m", func() {
		srv := pbstest.Start()
		DeferCleanup(srv.Close)
		srv.SetNamespaceStatus(500)
		repo := newTestRepo("repo-boot-fails", srv)
		repo.Spec.Host, repo.Spec.Port = srv.Host(), int32(srv.Port())
		repo.Spec.BootstrapTokenRef = pbsv1.NamespacedSecretRef{Name: repo.Name + "-boot", Namespace: "default"}
		Expect(k8sClient.Create(ctx, repo)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, fetchRepo(ctx, repo.Name))).To(Succeed()) })
		createSecret(ctx, repo.Spec.SecretRef.Name, "default", credsKeys(producerTokenID, producerTokenSecret, srv.Fingerprint()))
		createSecret(ctx, repo.Spec.BootstrapTokenRef.Name, "default",
			map[string][]byte{"tokenID": []byte(bootstrapTokenID), "tokenSecret": []byte(bootstrapTokenSecret)})

		rec := record.NewFakeRecorder(16)
		res := doReconcile(ctx, repo.Name, rec)

		Expect(res.RequeueAfter).To(Equal(5 * time.Minute))
		cond := readyCondition(ctx, repo.Name)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("BootstrapFailed"))
		Expect(cond.Message).To(ContainSubstring("ensure namespace"))
		Expect(drainEvents(rec)).To(HaveLen(1))
	})

	It("empty secretRef.namespace → operator namespace fallback", func() {
		srv := pbstest.Start()
		DeferCleanup(srv.Close)
		repo := newTestRepo("repo-opns", srv)
		repo.Spec.Host, repo.Spec.Port = srv.Host(), int32(srv.Port())
		repo.Spec.SecretRef = pbsv1.NamespacedSecretRef{Name: repo.Name + "-creds"} // no namespace
		Expect(k8sClient.Create(ctx, repo)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, fetchRepo(ctx, repo.Name))).To(Succeed()) })
		createSecret(ctx, repo.Spec.SecretRef.Name, "pbs-operator-system", credsKeys(producerTokenID, producerTokenSecret, srv.Fingerprint()))

		res := doReconcile(ctx, repo.Name, record.NewFakeRecorder(16))

		Expect(res.RequeueAfter).To(Equal(5 * time.Minute))
		Expect(readyCondition(ctx, repo.Name).Reason).To(Equal("Reachable"))
	})
})
