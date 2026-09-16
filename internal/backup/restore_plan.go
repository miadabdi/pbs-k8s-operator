package backup

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"

	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

// PlannedDoc is one manifest staged for apply: its Kind, Name, and the
// renderable YAML (namespace already rewritten to the restore target).
type PlannedDoc struct {
	Kind, Name string
	YAML       []byte
}

// ParsedDoc is one decoded document of an api.yaml payload.
type ParsedDoc struct {
	Kind, Namespace, Name string
	Obj                   map[string]any
}

// clusterScopedKinds never get the namespace rewrite (they have none).
var clusterScopedKinds = map[string]bool{
	"Namespace":                true,
	"CustomResourceDefinition": true,
}

// workloadKinds are applied last, after volumes are restored: their pods would
// consume (and bind) the PVCs the pre-phase applied.
var workloadKinds = map[string]bool{
	"Deployment":  true,
	"StatefulSet": true,
	"DaemonSet":   true,
	"Job":         true,
	"CronJob":     true,
	"ReplicaSet":  true,
}

// ParseAPIManifests decodes the multi-doc api.yaml (the format serialize.go
// writes), rewriting every namespaced doc's metadata.namespace to targetNS
// (restores are cross-namespace: "pg" must become the target). Namespace and
// CRD docs are cluster-scoped and stay untouched. An unparseable doc, or one
// lacking kind/metadata.name, is an error NAMING the doc — never a silent
// skip. targetNS "" leaves namespaces as-is.
func ParseAPIManifests(data []byte, targetNS string) ([]ParsedDoc, error) {
	reader := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
	var docs []ParsedDoc
	for i := 0; ; i++ {
		raw, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("manifest doc %d: %w", i, err)
		}
		if isBlankDoc(raw) {
			continue // bare separator / blank / comment-only doc
		}
		var obj map[string]any
		if err := yaml.Unmarshal(raw, &obj); err != nil {
			return nil, fmt.Errorf("manifest doc %d (%s): %w", i, firstSnippet(raw), err)
		}
		kind, _ := obj["kind"].(string)
		meta, _ := obj["metadata"].(map[string]any)
		name, _ := meta["name"].(string)
		if kind == "" || name == "" {
			return nil, fmt.Errorf("manifest doc %d (%s): missing kind or metadata.name", i, firstSnippet(raw))
		}
		if targetNS != "" && !clusterScopedKinds[kind] {
			if meta == nil {
				meta = map[string]any{}
				obj["metadata"] = meta
			}
			meta["namespace"] = targetNS
		}
		switch kind {
		case "Service":
			stripServiceAllocations(obj)
		case "PersistentVolumeClaim":
			stripPVCBinding(obj)
		}
		ns, _ := meta["namespace"].(string)
		docs = append(docs, ParsedDoc{Kind: kind, Namespace: ns, Name: name, Obj: obj})
	}
	return docs, nil
}

// pvcBindAnnotations record the SOURCE claim's binding state; replaying
// them on a fresh claim (bind-completed with no volumeName) desyncs the
// controllers into ClaimLost instead of a clean WFFC bind (live-verified).
var pvcBindAnnotations = []string{
	"pv.kubernetes.io/bind-completed",
	"pv.kubernetes.io/bound-by-controller",
	"volume.kubernetes.io/selected-node",
}

// stripPVCBinding makes a serialized PVC restorable: the source's volumeName
// points at a PV bound to the SOURCE claim (naming it leaves the target claim
// Lost), and its bind annotations desync WaitForFirstConsumer. Stripped, the
// claim is fresh: WFFC binds it when the volume-restore Job (the consumer)
// schedules.
func stripPVCBinding(obj map[string]any) {
	spec, _ := obj["spec"].(map[string]any)
	delete(spec, "volumeName")
	meta, _ := obj["metadata"].(map[string]any)
	anns, _ := meta["annotations"].(map[string]any)
	for _, k := range pvcBindAnnotations {
		delete(anns, k)
	}
	if len(anns) == 0 {
		delete(meta, "annotations")
	}
}

// stripServiceAllocations removes every SOURCE-owned allocation from a
// Service doc so the target assigns fresh: the cluster IP(s) (applying the
// source's fails "provided IP is already allocated" — live-verified), the
// node ports (NodePort/LB services pin host ports cluster-wide; a second
// restore or a live source service keeps them allocated), and the
// load-balancer IP / health-check node port.
func stripServiceAllocations(obj map[string]any) {
	spec, _ := obj["spec"].(map[string]any)
	delete(spec, "clusterIP")
	delete(spec, "clusterIPs")
	delete(spec, "loadBalancerIP")
	delete(spec, "healthCheckNodePort")
	if ports, ok := spec["ports"].([]any); ok {
		for _, p := range ports {
			if pm, ok := p.(map[string]any); ok {
				delete(pm, "nodePort")
			}
		}
	}
}

// droppedKinds are never restored regardless of drop/keep: bare Pods are
// derived state (workloads recreate them; restored ones carry stale owner
// UIDs and GC kills them — and stale backup-job pods without the managed
// label ride along as garbage). Endpoints/EndpointSlice and
// ControllerRevisions are rebuilt by the restored Service's/StatefulSet's own
// controllers — worse, a restored ControllerRevision collides by hash-name
// with the live STS's and its stale ownerRef fails re-apply with "Only one
// reference can have Controller set to true" (live-verified). PodMetrics
// (metrics.k8s.io) is a read-only virtual API the serializer catches via its
// list verb — applying it fails with MethodNotAllowed (live-verified).
var droppedKinds = map[string]bool{
	"Pod":                true,
	"Endpoints":          true,
	"EndpointSlice":      true,
	"PodMetrics":         true,
	"ControllerRevision": true,
}

// BucketDocs classifies parsed docs into apply buckets, source order kept:
//
//	ns    Namespace docs (create the target namespace itself if archived)
//	crds  CustomResourceDefinitions (must be Established before CRs apply)
//	pre   everything else — PVCs, Secrets, ConfigMaps, Services,
//	      ServiceAccounts, custom resources... applied before volumes so the
//	      PVCs exist (WFFC: they stay unbound until a consumer schedules)
//	work  workload kinds (pods) — applied last, after volume data is back
//
// droppedKinds (bare Pods, Endpoints/EndpointSlice) never restore. drop/keep
// are Kind-name filters applied to BOTH api buckets: a kind in drop is
// removed unless it is also in keep (Keep wins over Drop). Keep alone does
// not restrict — it is an exception list, not a whitelist.
func BucketDocs(docs []ParsedDoc, drop, keep []string) (ns, crds, pre, work []PlannedDoc) {
	dropped, kept := kindSet(drop), kindSet(keep)
	for _, d := range docs {
		if droppedKinds[d.Kind] || (dropped[d.Kind] && !kept[d.Kind]) {
			continue
		}
		pd := PlannedDoc{Kind: d.Kind, Name: d.Name, YAML: renderDoc(d.Obj)}
		switch {
		case d.Kind == "Namespace":
			ns = append(ns, pd)
		case d.Kind == "CustomResourceDefinition":
			crds = append(crds, pd)
		case workloadKinds[d.Kind]:
			work = append(work, pd)
		default:
			pre = append(pre, pd)
		}
	}
	return ns, crds, pre, work
}

// PVCsFromDocs lists the PVC names from the PreVolume bucket (order kept) —
// the volumes the restore must fill before workloads apply.
func PVCsFromDocs(pre []PlannedDoc) []string {
	var out []string
	for _, d := range pre {
		if d.Kind == "PersistentVolumeClaim" {
			out = append(out, d.Name)
		}
	}
	return out
}

// NodeForPVC returns the kubernetes.io/hostname the PVC's consuming workload
// pins via its pod template's nodeSelector — direct volume references
// (Deployment/DaemonSet/Job/...) and StatefulSet claim templates (PVC names
// are <template>-<sts>-<ordinal>). "" when nothing pins it.
// ponytail: nodeSelector only — pod affinity/topology spreads fall back to
// the controller's default node; extend here if those workloads matter.
func NodeForPVC(docs []ParsedDoc, pvc string) string {
	for _, d := range docs {
		if !workloadKinds[d.Kind] {
			continue
		}
		spec, _ := d.Obj["spec"].(map[string]any)
		tmpl, _ := spec["template"].(map[string]any)
		podSpec, _ := tmpl["spec"].(map[string]any)

		if claimsPodVolume(podSpec, pvc) || stsClaimTemplate(spec, d.Name, pvc) {
			if node := hostnameSelector(podSpec); node != "" {
				return node
			}
		}
	}
	return ""
}

// claimsPodVolume reports whether the pod template mounts pvc directly.
func claimsPodVolume(podSpec map[string]any, pvc string) bool {
	vols, _ := podSpec["volumes"].([]any)
	for _, v := range vols {
		vm, _ := v.(map[string]any)
		ref, _ := vm["persistentVolumeClaim"].(map[string]any)
		if claim, _ := ref["claimName"].(string); claim == pvc {
			return true
		}
	}
	return false
}

// stsClaimTemplate reports whether pvc derives from one of the StatefulSet's
// volumeClaimTemplates (<tmpl-name>-<sts-name>-<ordinal>).
func stsClaimTemplate(spec map[string]any, stsName, pvc string) bool {
	tcts, _ := spec["volumeClaimTemplates"].([]any)
	for _, t := range tcts {
		tm, _ := t.(map[string]any)
		tmeta, _ := tm["metadata"].(map[string]any)
		tname, _ := tmeta["name"].(string)
		if tname != "" && strings.HasPrefix(pvc, tname+"-"+stsName+"-") {
			return true
		}
	}
	return false
}

// hostnameSelector reads kubernetes.io/hostname from a pod template's
// nodeSelector.
func hostnameSelector(podSpec map[string]any) string {
	sel, _ := podSpec["nodeSelector"].(map[string]any)
	node, _ := sel["kubernetes.io/hostname"].(string)
	return node
}

// kindSet lowercases nothing: Kind names are exact ("Secret", not "secret").
func kindSet(kinds []string) map[string]bool {
	out := make(map[string]bool, len(kinds))
	for _, k := range kinds {
		out[k] = true
	}
	return out
}

// renderDoc re-marshals a parsed doc to YAML. Unreachable failure: the object
// came from a JSON round-trip, everything there is YAML-marshalable — on error
// the doc renders empty and kubectl apply fails loudly downstream.
func renderDoc(obj map[string]any) []byte {
	b, err := yaml.Marshal(obj)
	if err != nil {
		return nil
	}
	return b
}

// isBlankDoc reports whether raw carries no manifest: empty, separator
// ("---") and comment lines only. The YAML reader surfaces consecutive
// separators as their own "documents".
func isBlankDoc(raw []byte) bool {
	for _, line := range bytes.Split(raw, []byte("\n")) {
		l := bytes.TrimSpace(line)
		if len(l) == 0 || bytes.HasPrefix(l, []byte("---")) || bytes.HasPrefix(l, []byte("#")) {
			continue
		}
		return false
	}
	return true
}

// firstSnippet is the doc's first line, to name offenders in errors.
func firstSnippet(raw []byte) string {
	s := strings.TrimSpace(string(raw))
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 60 {
		s = s[:60] + "..."
	}
	return s
}
