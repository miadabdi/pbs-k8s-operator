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

// BackupPhase enumerates the lifecycle phases of a PBSBackup.
// +kubebuilder:validation:Enum=New;Scheduled;Running;Completed;Failed
type BackupPhase string

const (
	// BackupPhaseNew is the phase of a backup that has not started yet.
	BackupPhaseNew BackupPhase = "New"
	// BackupPhaseScheduled is the phase of a backup waiting for its Jobs.
	BackupPhaseScheduled BackupPhase = "Scheduled"
	// BackupPhaseRunning is the phase of a backup whose Jobs are running.
	BackupPhaseRunning BackupPhase = "Running"
	// BackupPhaseCompleted is the phase of a successfully finished backup.
	BackupPhaseCompleted BackupPhase = "Completed"
	// BackupPhaseFailed is the phase of a failed backup.
	BackupPhaseFailed BackupPhase = "Failed"
)

// PBSBackupSpec defines the desired state of PBSBackup.
//
// PVC selection: if pvcs is set it wins; else selector is used; else ALL
// PVCs in the PBSBackup's namespace are selected. M1 backs up PVC data only
// (API objects land in M2; hooks in M4).
type PBSBackupSpec struct {
	// repoRef is the name of the PBSRepo (cluster-scoped) to back up into.
	RepoRef string `json:"repoRef"`

	// pvcs is an explicit list of PVC names (in the PBSBackup's namespace).
	// Takes precedence over selector when set.
	// +optional
	PVCs []string `json:"pvcs,omitempty"`

	// selector selects PVCs by label in the PBSBackup's namespace. Used only
	// when pvcs is unset; when both are unset, all PVCs in the namespace are
	// selected.
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty"`

	// notes is attached to the PBS snapshot post-upload via
	// proxmox-backup-client `snapshot notes update` (best-effort; requires
	// Datastore.Modify on the datastore namespace). PBS has no arbitrary
	// snapshot labels, so Notes is the retention-hint channel (operators can
	// encode keep-policy JSON here); pruning itself stays server-side.
	// +optional
	Notes string `json:"notes,omitempty"`
}

// BackupJobStatus reports the k8s backup Job for one involved node.
type BackupJobStatus struct {
	// node is the k8s node whose PVCs this Job backs up.
	Node string `json:"node"`

	// job is the name of the k8s Job.
	Job string `json:"job"`

	// state is a raw summary of the Job's latest condition/phase.
	State string `json:"state"`
}

// PBSBackupStatus defines the observed state of PBSBackup.
type PBSBackupStatus struct {
	// phase is the current lifecycle phase of the backup.
	// +kubebuilder:default=New
	// +optional
	Phase BackupPhase `json:"phase,omitempty"`

	// snapshotRef is the primary PBS snapshot reference, "type/id/ISO8601Z".
	// +optional
	SnapshotRef string `json:"snapshotRef,omitempty"`

	// jobs holds one BackupJobStatus per involved node.
	// +optional
	Jobs []BackupJobStatus `json:"jobs,omitempty"`

	// startedAt is when the backup started running.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// completedAt is when the backup reached a terminal phase.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// bytes is the total size backed up.
	// +optional
	Bytes int64 `json:"bytes,omitempty"`

	// conditions represent the current state of the PBSBackup resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Snapshot",type=string,JSONPath=`.status.snapshotRef`
// +kubebuilder:printcolumn:name="Started",type=date,JSONPath=`.status.startedAt`

// PBSBackup is the Schema for the pbsbackups API.
type PBSBackup struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PBSBackup
	// +required
	Spec PBSBackupSpec `json:"spec"`

	// status defines the observed state of PBSBackup
	// +optional
	Status PBSBackupStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PBSBackupList contains a list of PBSBackup.
type PBSBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PBSBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PBSBackup{}, &PBSBackupList{})
		return nil
	})
}
