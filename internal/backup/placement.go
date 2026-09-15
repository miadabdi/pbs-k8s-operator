package backup

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

const hostnameKey = "kubernetes.io/hostname"

// nodeFromAffinity extracts the node from a local-path PV's nodeAffinity:
// a kubernetes.io/hostname In requirement (matchExpressions or matchFields).
// Any other shape yields no match.
func nodeFromAffinity(pv *corev1.PersistentVolume) (string, bool) {
	if pv == nil || pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return "", false
	}
	for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		for _, reqs := range [][]corev1.NodeSelectorRequirement{term.MatchExpressions, term.MatchFields} {
			for _, req := range reqs {
				if req.Key == hostnameKey && req.Operator == corev1.NodeSelectorOpIn && len(req.Values) > 0 {
					return req.Values[0], true
				}
			}
		}
	}
	return "", false
}

// ResolveNode picks the node whose local disk holds this PVC's data.
//  1. PV spec.nodeAffinity.required.nodeTerms — look for matchExpressions/matchFields
//     with key "kubernetes.io/hostname" (In operator) — that value is the node
//     (local-path PVs carry exactly this). Any other nodeAffinity shape: no match.
//  2. Fallback: the pods list — a pod mounting this PVC (claim name match, ReadOnly
//     or not) provides spec.nodeName.
//  3. Neither → error that NAMES the PVC (operators must know which volume is
//     unplaceable). Nil PV (PVC not bound) → same error path.
func ResolveNode(pvc corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, pods []corev1.Pod) (string, error) {
	if node, ok := nodeFromAffinity(pv); ok {
		return node, nil
	}
	for _, pod := range pods {
		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim != nil && vol.PersistentVolumeClaim.ClaimName == pvc.Name && pod.Spec.NodeName != "" {
				return pod.Spec.NodeName, nil
			}
		}
	}
	return "", fmt.Errorf("backup: cannot resolve node for PVC %s/%s: PV has no kubernetes.io/hostname node affinity and no scheduled pod mounts it",
		pvc.Namespace, pvc.Name)
}
