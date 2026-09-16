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

// PBSBackupTemplate is the PBSBackup body a PBSSchedule instantiates on each
// fire: a subset of PBSBackupSpec (the fields that make sense per-fire —
// RepoRef comes from the schedule itself).
type PBSBackupTemplate struct {
	// pvcs is an explicit list of PVC names, as in PBSBackupSpec.
	// +optional
	PVCs []string `json:"pvcs,omitempty"`

	// selector selects PVCs by label, as in PBSBackupSpec.
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty"`

	// notes is carried verbatim to each created PBSBackup's spec.notes and
	// from there attached to the PBS snapshot post-upload via
	// proxmox-backup-client `snapshot notes update` (best-effort; requires
	// Datastore.Modify on the datastore namespace). PBS has no arbitrary
	// snapshot labels, so Notes is THE retention-hint channel: operators can
	// e.g. encode a keep-policy JSON here; pruning itself stays server-side.
	// +optional
	Notes string `json:"notes,omitempty"`
}

// PBSScheduleSpec defines the desired state of PBSSchedule.
type PBSScheduleSpec struct {
	// repoRef is the name of the cluster-scoped PBSRepo backups are sent to.
	// +kubebuilder:validation:Required
	RepoRef string `json:"repoRef"`

	// schedule is a standard 5-field cron expression (minute hour dom month dow).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^(\S+\s+){4}\S+$`
	Schedule string `json:"schedule"`

	// template is the PBSBackup body created on each fire.
	// +kubebuilder:validation:Required
	Template PBSBackupTemplate `json:"template"`

	// suspend stops new backup creation; NextScheduleTime is still computed.
	// +optional
	Suspend bool `json:"suspend,omitempty"`
}

// PBSScheduleStatus defines the observed state of PBSSchedule.
type PBSScheduleStatus struct {
	// lastScheduleTime is the fire time the schedule last created a backup at.
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`

	// nextScheduleTime is the computed next fire time; observability only.
	// +optional
	NextScheduleTime *metav1.Time `json:"nextScheduleTime,omitempty"`

	// active lists the names of PBSBackups from this schedule that have not
	// reached a terminal phase yet.
	// +optional
	Active []string `json:"active,omitempty"`

	// conditions represent the current state of the PBSSchedule resource:
	// Ready=True/Valid when the cron parses and the repo is Ready;
	// Ready=False/InvalidCron or /RepoMissing otherwise.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="Suspend",type=boolean,JSONPath=`.spec.suspend`
// +kubebuilder:printcolumn:name="LastSchedule",type=date,JSONPath=`.status.lastScheduleTime`

// PBSSchedule is the Schema for the pbsschedules API
type PBSSchedule struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PBSSchedule
	// +required
	Spec PBSScheduleSpec `json:"spec"`

	// status defines the observed state of PBSSchedule
	// +optional
	Status PBSScheduleStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PBSScheduleList contains a list of PBSSchedule
type PBSScheduleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PBSSchedule `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PBSSchedule{}, &PBSScheduleList{})
		return nil
	})
}
