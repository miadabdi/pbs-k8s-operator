package backup

import (
	"context"
	"errors"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/yaml"
)

// ManagedLabel marks objects the operator itself created (copied repo
// secrets, staging ConfigMaps, Jobs). SerializeNamespace skips them so a
// backup never archives the operator's own scaffolding.
const ManagedLabel = "pbs.sharifmind.ir/managed"

// lastAppliedAnnotation is kubectl's full last-applied object: it duplicates
// the rendered spec and leaks managed-field history into backups.
const lastAppliedAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

// excludedKinds are never serialized: events churn constantly, Jobs are
// re-derivable from their owners, and the operator's own CRs recreate
// themselves from the samples/manifests that installed them.
var excludedKinds = map[schema.GroupKind]bool{
	{Group: "", Kind: "Event"}:                        true,
	{Group: "events.k8s.io", Kind: "Event"}:           true,
	{Group: "batch", Kind: "Job"}:                     true,
	{Group: "pbs.sharifmind.ir", Kind: "PBSBackup"}:   true,
	{Group: "pbs.sharifmind.ir", Kind: "PBSSchedule"}: true,
	{Group: "pbs.sharifmind.ir", Kind: "PBSRestore"}:  true,
}

// volatileMetadata keys change on every server-side write; stripping them
// keeps the rendered YAML deterministic (diffable, stable across reruns).
var volatileMetadata = []string{"uid", "resourceVersion", "creationTimestamp", "generation", "managedFields"}

// ResourceDiscovery is the discovery seam SerializeNamespace needs; every
// discovery.DiscoveryInterface (and its cached variants) satisfies it. Narrow
// on purpose: unit tests fake it in one line.
type ResourceDiscovery interface {
	ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error)
}

// Serializer renders a namespace's API objects as one multi-document YAML
// string (the staging ConfigMap payload behind api.pxar).
type Serializer struct {
	dyn   dynamic.Interface
	disco ResourceDiscovery
}

// NewSerializer wires a Serializer from a dynamic client and a discovery
// client (both built from the manager's rest config in cmd/main.go).
func NewSerializer(dyn dynamic.Interface, disco ResourceDiscovery) *Serializer {
	return &Serializer{dyn: dyn, disco: disco}
}

// SerializeNamespace lists every namespaced resource present in ns and
// returns a single multi-document YAML string (documents separated by "---").
//   - Deterministic order: GroupKind alphabetical, then object name.
//   - Excluded: events (both APIs), jobs.batch, the operator's own CRs, and
//     any object carrying the pbs.sharifmind.ir/managed label.
//   - Per object, volatile fields are stripped before render (see
//     volatileMetadata); Secrets and ConfigMaps are INCLUDED — the PBS
//     keyfile encrypts them server-side.
//   - An empty namespace still returns "" without error; the agent then
//     skips the api.pxar pair.
//
// ponytail: one discovery round-trip per call (no cache) — revisit with a
// cached discovery client if reconcile frequency makes it hot.
func (s *Serializer) SerializeNamespace(ctx context.Context, ns string) (string, error) {
	lists, err := s.disco.ServerPreferredNamespacedResources()
	if err != nil {
		// Partial results are usable: some aggregated APIs failing discovery
		// (or vanishing mid-call) must not fail the whole backup.
		var failed *discovery.ErrGroupDiscoveryFailed
		if !errors.As(err, &failed) {
			return "", err
		}
	}

	type doc struct {
		gk, name string
		obj      map[string]any
	}
	var found []doc
	for _, list := range lists {
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			return "", err
		}
		for _, ar := range list.APIResources {
			gk := gv.WithKind(ar.Kind).GroupKind()
			if !ar.Namespaced || strings.Contains(ar.Name, "/") || excludedKinds[gk] || !hasVerb(ar.Verbs, "list") {
				continue
			}
			items, err := s.dyn.Resource(gv.WithResource(ar.Name)).Namespace(ns).List(ctx, metav1.ListOptions{})
			if err != nil {
				if skippableListError(err) {
					continue
				}
				return "", err
			}
			for i := range items.Items {
				u := &items.Items[i]
				if u.GetLabels()[ManagedLabel] != "" {
					continue
				}
				found = append(found, doc{gk.String(), u.GetName(), u.Object})
			}
		}
	}
	sort.Slice(found, func(i, j int) bool {
		if found[i].gk != found[j].gk {
			return found[i].gk < found[j].gk
		}
		return found[i].name < found[j].name
	})

	rendered := make([]string, 0, len(found))
	for _, d := range found {
		stripVolatile(d.obj)
		b, err := yaml.Marshal(d.obj)
		if err != nil {
			return "", err
		}
		rendered = append(rendered, string(b))
	}
	if len(rendered) == 0 {
		return "", nil
	}
	return strings.Join(rendered, "\n---\n") + "\n", nil
}

// stripVolatile removes the status and the volatile metadata keys in place.
func stripVolatile(obj map[string]any) {
	delete(obj, "status")
	meta, ok := obj["metadata"].(map[string]any)
	if !ok {
		return
	}
	for _, f := range volatileMetadata {
		delete(meta, f)
	}
	if anns, ok := meta["annotations"].(map[string]any); ok {
		delete(anns, lastAppliedAnnotation)
		if len(anns) == 0 {
			delete(meta, "annotations")
		}
	}
}

// hasVerb reports whether the discovery verbs allow the operation. Some
// aggregated APIs advertise verbs: ["*"] — that grants every verb.
func hasVerb(verbs []string, verb string) bool {
	for _, v := range verbs {
		if v == verb || v == "*" {
			return true
		}
	}
	return false
}

// skippableListError reports whether a per-resource list failure can be
// ignored: aggregated APIs flap (metrics.k8s.io 503s), versions vanish
// mid-discovery, and some review-style resources reject list outright.
func skippableListError(err error) bool {
	return apierrors.IsNotFound(err) || apierrors.IsForbidden(err) ||
		apierrors.IsMethodNotSupported(err) || apierrors.IsServiceUnavailable(err)
}
