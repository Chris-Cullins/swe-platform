package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// InstallationSpec is intentionally empty. The Installation object's immutable
// UID is the durable identity used by claimed Project namespaces.
type InstallationSpec struct{}

// InstallationStatus is reserved for installation-level observations.
type InstallationStatus struct {
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Installation is the stable, system-namespaced identity of one platform
// installation. Project namespace claims bind to its immutable UID.
type Installation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   InstallationSpec   `json:"spec,omitempty"`
	Status InstallationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// InstallationList contains a list of Installation objects.
type InstallationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Installation `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Installation{}, &InstallationList{})
}
