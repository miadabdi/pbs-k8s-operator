package backup

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// affinityPV builds a PV carrying a local-path-shaped nodeAffinity: one term
// with a kubernetes.io/hostname In requirement, via matchExpressions or
// matchFields.
func affinityPV(node string, fields bool) *corev1.PersistentVolume {
	req := corev1.NodeSelectorRequirement{
		Key:      "kubernetes.io/hostname",
		Operator: corev1.NodeSelectorOpIn,
		Values:   []string{node},
	}
	term := corev1.NodeSelectorTerm{}
	if fields {
		term.MatchFields = []corev1.NodeSelectorRequirement{req}
	} else {
		term.MatchExpressions = []corev1.NodeSelectorRequirement{req}
	}
	return &corev1.PersistentVolume{Spec: corev1.PersistentVolumeSpec{
		NodeAffinity: &corev1.VolumeNodeAffinity{
			Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{term}},
		},
	}}
}

// mountingPod builds a pod on node that mounts claim (ReadOnly or not).
func mountingPod(claim, node string) corev1.Pod {
	return corev1.Pod{Spec: corev1.PodSpec{
		NodeName: node,
		Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
		}}},
	}}
}

func TestResolveNode(t *testing.T) {
	pvc := corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-pvc", Namespace: "app"}}
	tests := []struct {
		name      string
		pv        *corev1.PersistentVolume
		pods      []corev1.Pod
		wantNode  string
		wantErrIn string // non-empty: expect error whose text contains this
	}{
		{
			name:     "local-path matchExpressions hostname",
			pv:       affinityPV("k8s-node1", false),
			wantNode: "k8s-node1",
		},
		{
			name:     "matchFields hostname variant",
			pv:       affinityPV("k8s-node2", true),
			wantNode: "k8s-node2",
		},
		{
			name:     "no nodeAffinity, pod fallback",
			pv:       &corev1.PersistentVolume{},
			pods:     []corev1.Pod{mountingPod("data-pvc", "k8s-node3")},
			wantNode: "k8s-node3",
		},
		{
			name:     "pod not mounting this PVC is skipped",
			pv:       &corev1.PersistentVolume{},
			pods:     []corev1.Pod{mountingPod("other-pvc", "k8s-node4"), mountingPod("data-pvc", "k8s-node5")},
			wantNode: "k8s-node5",
		},
		{
			name:      "nil PV and no mounting pod",
			wantErrIn: "data-pvc",
		},
		{
			name: "unresolvable affinity shape, no pods",
			pv: &corev1.PersistentVolume{Spec: corev1.PersistentVolumeSpec{
				NodeAffinity: &corev1.VolumeNodeAffinity{
					Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key: "topology.kubernetes.io/zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"z1"},
						}},
					}}},
				},
			}},
			wantErrIn: "data-pvc",
		},
		{
			name: "hostname with NotIn operator is not a match",
			pv: &corev1.PersistentVolume{Spec: corev1.PersistentVolumeSpec{
				NodeAffinity: &corev1.VolumeNodeAffinity{
					Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"k8s-node1"},
						}},
					}}},
				},
			}},
			wantErrIn: "data-pvc",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolveNode(pvc, tc.pv, tc.pods)
			if tc.wantErrIn != "" {
				if err == nil {
					t.Fatalf("ResolveNode = %q, want error naming PVC %q", got, tc.wantErrIn)
				}
				if !strings.Contains(err.Error(), tc.wantErrIn) {
					t.Errorf("error %q does not name PVC %q", err, tc.wantErrIn)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveNode: %v", err)
			}
			if got != tc.wantNode {
				t.Errorf("ResolveNode = %q, want %q", got, tc.wantNode)
			}
		})
	}
}
