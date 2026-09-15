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

// NamespacedSecretRef references a Secret by name, optionally in a specific
// namespace. An empty namespace means the operator's own namespace.
type NamespacedSecretRef struct {
	// name of the Secret.
	Name string `json:"name"`

	// namespace of the Secret. Empty means the operator's own namespace.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// PBSRepoSpec defines the desired state of PBSRepo.
type PBSRepoSpec struct {
	// host is the hostname or IP of the PBS server, e.g. "192.168.56.10".
	Host string `json:"host"`

	// port is the PBS API port.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=8007
	// +optional
	Port int32 `json:"port,omitempty"`

	// datastore is the PBS datastore name to back up into (min length 3, PBS rule).
	// +kubebuilder:validation:MinLength=3
	Datastore string `json:"datastore"`

	// namespace is the PBS namespace inside the datastore, e.g. "test-ns".
	Namespace string `json:"namespace"`

	// fingerprint is the PBS server certificate fingerprint
	// (sha256, colon-separated hex, uppercase).
	Fingerprint string `json:"fingerprint"`

	// secretRef references the Secret holding the client credentials.
	// Expected keys: tokenID, tokenSecret, keyfile (and optionally host, port,
	// datastore, namespace, fingerprint — CR fields win over Secret keys).
	SecretRef NamespacedSecretRef `json:"secretRef"`

	// bootstrapTokenRef references a Secret with a token capable of
	// pre-creating the PBS namespace. Absent means skip bootstrap.
	// +optional
	BootstrapTokenRef NamespacedSecretRef `json:"bootstrapTokenRef,omitempty"`
}

// PBSRepoStatus defines the observed state of PBSRepo.
type PBSRepoStatus struct {
	// conditions represent the current state of the PBSRepo resource.
	// Condition type "Ready"; reasons include "Reachable", "Unreachable",
	// "SecretMissing", etc. (set via meta.SetStatusCondition).
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// lastProbeTime is when the PBS server was last successfully probed.
	// +optional
	LastProbeTime *metav1.Time `json:"lastProbeTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=pbsrepos,scope=Cluster
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`

// PBSRepo is the Schema for the pbsrepos API.
type PBSRepo struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PBSRepo
	// +required
	Spec PBSRepoSpec `json:"spec"`

	// status defines the observed state of PBSRepo
	// +optional
	Status PBSRepoStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PBSRepoList contains a list of PBSRepo.
type PBSRepoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PBSRepo `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PBSRepo{}, &PBSRepoList{})
		return nil
	})
}
