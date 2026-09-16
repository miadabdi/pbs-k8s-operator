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
	"os"
	"strconv"
	"strings"
	"time"

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
	"gitlab.sharifmind.ir/miad/pbs-operator/internal/pbs"
)

const (
	condReady = "Ready"

	reasonReachable       = "Reachable"
	reasonUnreachable     = "Unreachable"
	reasonSecretMissing   = "SecretMissing"
	reasonBootstrapFailed = "BootstrapFailed"

	// requeueFailure is the retry cadence for fixable-from-outside problems
	// (missing secret, unreachable server); requeueSteady is the periodic
	// health check after success and the slow retry after a bootstrap failure.
	requeueFailure = 2 * time.Minute
	requeueSteady  = 5 * time.Minute

	pingTimeout = 15 * time.Second

	// lastProbeFreshFor suppresses LastProbeTime rewrites on back-to-back
	// reconciles (a status patch would watch-trigger another reconcile and
	// hot-loop); the 5m health requeue still advances it.
	lastProbeFreshFor = time.Minute

	defaultPort = 8007

	// operatorNamespaceFallback is used when spec.secretRef.namespace is empty
	// and POD_NAMESPACE is unset (e.g. running outside a Pod).
	operatorNamespaceFallback = "pbs-operator-system"
)

// PBSRepoReconciler reconciles a PBSRepo object: it validates the credentials
// secret, pings the PBS server, and (optionally) bootstraps the namespace.
type PBSRepoReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder recorder.EventRecorder
}

// +kubebuilder:rbac:groups=pbs.sharifmind.ir,resources=pbsrepos,verbs=get;list;watch
// +kubebuilder:rbac:groups=pbs.sharifmind.ir,resources=pbsrepos/status,verbs=get;update;patch
// Secrets are read cluster-wide: PBSRepo is a cluster-scoped CR whose
// secretRef may point at any namespace, so namespace-scoped RBAC cannot cover it.
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// Events are emitted through the events.k8s.io/v1 recorder (mgr.GetEventRecorder).
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile moves a PBSRepo toward Ready=Reachable. See the constant block and
// helpers for the exact conditions, events, and requeue cadences.
func (r *PBSRepoReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	var repo pbsv1.PBSRepo
	if err := r.Get(ctx, req.NamespacedName, &repo); err != nil {
		// Deleted (or not yet cached): nothing to do — no finalizers in M1.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	creds, problem, err := r.producerCreds(ctx, &repo)
	if err != nil {
		return ctrl.Result{}, err
	}
	if problem != "" {
		return r.fail(ctx, &repo, reasonSecretMissing, problem, requeueFailure)
	}

	// The client is rebuilt every reconcile: cheap, and never holds a stale secret.
	producer := pbs.NewClient(creds.host, creds.port, creds.tokenID, creds.tokenSecret, creds.fingerprint)

	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if _, err := producer.ListSnapshots(pingCtx, creds.datastore, creds.namespace); err != nil {
		return r.fail(ctx, &repo, reasonUnreachable, err.Error(), requeueFailure)
	}

	if ref := repo.Spec.BootstrapTokenRef; ref.Name != "" {
		token, problem, err := r.bootstrapToken(ctx, ref)
		if err != nil {
			return ctrl.Result{}, err
		}
		if problem != "" {
			return r.fail(ctx, &repo, reasonSecretMissing, "bootstrap secret "+ref.Name+": "+problem, requeueFailure)
		}
		bootstrapper := pbs.NewClient(creds.host, creds.port, token.id, token.secret, creds.fingerprint)
		nsCtx, cancel := context.WithTimeout(ctx, pingTimeout)
		defer cancel()
		if err := bootstrapper.EnsureNamespace(nsCtx, creds.datastore, creds.namespace); err != nil {
			return r.fail(ctx, &repo, reasonBootstrapFailed, err.Error(), requeueSteady)
		}
	}

	return r.succeed(ctx, &repo)
}

// fail records Ready=False with the given reason, emits an event if the
// condition transitioned, and requeues after requeue.
func (r *PBSRepoReconciler) fail(ctx context.Context, repo *pbsv1.PBSRepo, reason, message string, requeue time.Duration) (ctrl.Result, error) {
	transitioned, err := r.setCondition(ctx, repo, metav1.Condition{
		Type:    condReady,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: message,
	}, false)
	if transitioned {
		r.Recorder.Eventf(repo, nil, corev1.EventTypeWarning, reason, "", message)
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// succeed records Ready=True Reachable (with LastProbeTime) and requeues for
// periodic health.
func (r *PBSRepoReconciler) succeed(ctx context.Context, repo *pbsv1.PBSRepo) (ctrl.Result, error) {
	transitioned, err := r.setCondition(ctx, repo, metav1.Condition{
		Type:    condReady,
		Status:  metav1.ConditionTrue,
		Reason:  reasonReachable,
		Message: "PBS server reachable",
		// Message is deliberately constant: a varying one would patch status
		// and emit an event on every health requeue.
	}, true)
	if transitioned {
		r.Recorder.Eventf(repo, nil, corev1.EventTypeNormal, reasonReachable, "", "PBS server is reachable")
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueSteady}, nil
}

// setCondition applies cond via meta.SetStatusCondition (never dropping other
// conditions) and, when stampProbe is set, refreshes LastProbeTime unless it is
// still fresh (hot-loop guard). Status is patched only when something changed.
// It reports whether the condition content itself transitioned (the caller
// emits the event) and any patch error.
func (r *PBSRepoReconciler) setCondition(ctx context.Context, repo *pbsv1.PBSRepo, cond metav1.Condition, stampProbe bool) (bool, error) {
	before := repo.DeepCopy()
	cond.ObservedGeneration = repo.Generation
	transitioned := meta.SetStatusCondition(&repo.Status.Conditions, cond)
	dirty := transitioned
	if stampProbe && (repo.Status.LastProbeTime == nil || time.Since(repo.Status.LastProbeTime.Time) > lastProbeFreshFor) {
		now := metav1.Now()
		repo.Status.LastProbeTime = &now
		dirty = true
	}
	if !dirty {
		return false, nil
	}
	if err := r.Status().Patch(ctx, repo, client.MergeFrom(before)); err != nil {
		return false, err
	}
	return transitioned, nil
}

// repoCreds is everything needed to talk to the PBS server on behalf of a
// PBSRepo, merged from the CR spec and its credentials secret.
type repoCreds struct {
	tokenID, tokenSecret, fingerprint string
	host                              string
	port                              int
	datastore, namespace              string
}

// producerCreds resolves spec.secretRef. The returned problem string is a
// user-facing SecretMissing message (empty when the credentials are usable).
// Required secret keys: tokenID, tokenSecret. Fingerprint comes from
// spec.fingerprint first, the secret's fingerprint key as fallback — at least
// one must be set (CRD contract: CR fields win over Secret keys). Optional
// keys host/port/datastore/namespace fill gaps — CR spec fields win.
func (r *PBSRepoReconciler) producerCreds(ctx context.Context, repo *pbsv1.PBSRepo) (repoCreds, string, error) {
	secret, problem, err := r.fetchSecret(ctx, repo.Spec.SecretRef)
	if err != nil || problem != "" {
		return repoCreds{}, problem, err
	}
	get := func(k string) string { return string(secret.Data[k]) }

	var missing []string
	for _, k := range []string{"tokenID", "tokenSecret"} {
		if get(k) == "" {
			missing = append(missing, k)
		}
	}
	fingerprint := repo.Spec.Fingerprint
	if fingerprint == "" {
		fingerprint = get("fingerprint")
		if fingerprint == "" {
			missing = append(missing, "fingerprint (or spec.fingerprint)")
		}
	}
	if len(missing) > 0 {
		return repoCreds{}, fmt.Sprintf("secret %s/%s missing keys: %s", secret.Namespace, secret.Name, strings.Join(missing, ", ")), nil
	}

	port := int(repo.Spec.Port)
	if port == 0 {
		if v := get("port"); v != "" {
			p, convErr := strconv.Atoi(v)
			if convErr != nil {
				return repoCreds{}, fmt.Sprintf("secret %s/%s: invalid port %q", secret.Namespace, secret.Name, v), nil
			}
			port = p
		} else {
			port = defaultPort
		}
	}

	return repoCreds{
		tokenID:     get("tokenID"),
		tokenSecret: get("tokenSecret"),
		fingerprint: fingerprint,
		host:        pick(repo.Spec.Host, get("host")),
		port:        port,
		datastore:   pick(repo.Spec.Datastore, get("datastore")),
		namespace:   pick(repo.Spec.Namespace, get("namespace")),
	}, "", nil
}

// bootstrapToken resolves spec.bootstrapTokenRef down to the API token pair.
func (r *PBSRepoReconciler) bootstrapToken(ctx context.Context, ref pbsv1.NamespacedSecretRef) (token struct{ id, secret string }, problem string, err error) {
	secret, problem, err := r.fetchSecret(ctx, ref)
	if err != nil || problem != "" {
		return token, problem, err
	}
	for _, k := range []string{"tokenID", "tokenSecret"} {
		if string(secret.Data[k]) == "" {
			return token, "missing key " + k, nil
		}
	}
	return struct{ id, secret string }{string(secret.Data["tokenID"]), string(secret.Data["tokenSecret"])}, "", nil
}

// fetchSecret gets the secret referenced by ref, defaulting an empty namespace
// to the operator's own. The problem string is a user-facing message when the
// secret does not exist.
func (r *PBSRepoReconciler) fetchSecret(ctx context.Context, ref pbsv1.NamespacedSecretRef) (*corev1.Secret, string, error) {
	if ref.Name == "" {
		return nil, "secretRef.name is empty", nil
	}
	ns := ref.Namespace
	if ns == "" {
		ns = operatorNamespace()
	}
	secret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: ns}, secret)
	if apierrors.IsNotFound(err) {
		return nil, fmt.Sprintf("secret %s/%s not found", ns, ref.Name), nil
	}
	if err != nil {
		return nil, "", err
	}
	return secret, "", nil
}

// operatorNamespace is the namespace secrets are looked up in when a ref
// leaves it empty: the operator's own Pod namespace, with a fallback for
// non-Pod (dev/test) runs.
func operatorNamespace() string {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	return operatorNamespaceFallback
}

// pick returns the CR value when set, else the secret-provided fallback.
func pick(crValue, secretValue string) string {
	if crValue != "" {
		return crValue
	}
	return secretValue
}

// SetupWithManager sets up the controller with the Manager.
func (r *PBSRepoReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pbsv1.PBSRepo{}).
		Named("pbsrepo").
		Complete(r)
}
