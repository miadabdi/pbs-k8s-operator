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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// RestorePhase enumerates the lifecycle phases of a PBSRestore.
// +kubebuilder:validation:Enum=New;StagingAPI;RestoringVolumes;ApplyingWorkloads;Completed;Failed
type RestorePhase string

const (
	// RestorePhaseNew is the phase of a restore that has not started yet.
	RestorePhaseNew RestorePhase = "New"
	// RestorePhaseStagingAPI covers the fetch Job (api.pxar.didx → ConfigMap)
	// and the pre apply Job (namespaces/CRDs/PVCs/secrets/...).
	RestorePhaseStagingAPI RestorePhase = "StagingAPI"
	// RestorePhaseRestoringVolumes is the phase of the per-node volume data
	// restore Jobs (the WFFC consumers that bind the restored PVCs).
	RestorePhaseRestoringVolumes RestorePhase = "RestoringVolumes"
	// RestorePhaseApplyingWorkloads is the phase of the workload apply Job
	// (pods land, consuming the restored volumes).
	RestorePhaseApplyingWorkloads RestorePhase = "ApplyingWorkloads"
	// RestorePhaseCompleted is the phase of a successfully finished restore.
	RestorePhaseCompleted RestorePhase = "Completed"
	// RestorePhaseFailed is the phase of a failed restore.
	RestorePhaseFailed RestorePhase = "Failed"
)

// PBSRestoreSpec defines the desired state of PBSRestore.
type PBSRestoreSpec struct {
	// repoRef is the name of the PBSRepo (cluster-scoped) to restore from.
	RepoRef string `json:"repoRef"`

	// snapshotRef is the PBS snapshot to restore, "host/<id>/<ISO8601Z>"
	// (from a PBSBackup's status or `snapshot list`).
	SnapshotRef string `json:"snapshotRef"`

	// targetNamespace receives the restored API objects and volumes; it is
	// created if missing. Jobs run there (PVC mounts and the api ConfigMap
	// are namespace-local), so in the common case the PBSRestore itself
	// lives in the target namespace.
	TargetNamespace string `json:"targetNamespace"`

	// dropKinds lists Kinds excluded from BOTH api apply phases (exact Kind
	// names, e.g. ["Secret","Probe"]).
	// +optional
	DropKinds []string `json:"dropKinds,omitempty"`

	// keepKinds lists Kinds exempt from dropKinds (Keep wins over Drop when
	// both name the same Kind). Keep alone does not restrict — it is an
	// exception list, not a whitelist.
	// +optional
	KeepKinds []string `json:"keepKinds,omitempty"`
}

// RestoreJobStatus reports one restore Job.
type RestoreJobStatus struct {
	// phase is the restore phase the Job belongs to.
	Phase string `json:"phase"`

	// job is the name of the k8s Job.
	Job string `json:"job"`

	// state is a raw summary of the Job's latest condition.
	State string `json:"state"`
}

// PBSRestoreStatus defines the observed state of PBSRestore.
type PBSRestoreStatus struct {
	// phase is the current lifecycle phase of the restore.
	// +kubebuilder:default=New
	// +optional
	Phase RestorePhase `json:"phase,omitempty"`

	// conditions represent the current state of the PBSRestore resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// startedAt is when the first restore Job was created.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// completedAt is when the restore reached a terminal phase.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// jobs holds one RestoreJobStatus per launched Job.
	// +optional
	Jobs []RestoreJobStatus `json:"jobs,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Snapshot",type=string,JSONPath=`.spec.snapshotRef`
// +kubebuilder:printcolumn:name="Started",type=date,JSONPath=`.status.startedAt`

// PBSRestore is the Schema for the pbsrestores API.
type PBSRestore struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PBSRestore
	// +required
	Spec PBSRestoreSpec `json:"spec"`

	// status defines the observed state of PBSRestore
	// +optional
	Status PBSRestoreStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PBSRestoreList contains a list of PBSRestore.
type PBSRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PBSRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PBSRestore{}, &PBSRestoreList{})
		return nil
	})
}
