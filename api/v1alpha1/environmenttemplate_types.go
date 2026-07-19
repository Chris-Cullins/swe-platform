package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WarmPoolSpec configures pre-booted environments for near-zero cold start.
type WarmPoolSpec struct {
	// Min is the number of unclaimed environments to keep ready.
	// +kubebuilder:validation:Minimum=0
	Min int32 `json:"min"`
}

// EnvironmentTemplateSpec defines a class of environments.
type EnvironmentTemplateSpec struct {
	// Image is the container image for the environment.
	Image string `json:"image"`

	// Size names a resource preset (tiny | small | medium | large).
	// +kubebuilder:validation:Enum=tiny;small;medium;large
	// +kubebuilder:default=medium
	Size string `json:"size,omitempty"`

	// RuntimeClass is the RuntimeClass used for isolation (e.g. gvisor).
	// +optional
	RuntimeClass string `json:"runtimeClass,omitempty"`

	// Backend selects where environments execute. Only "pod" is supported in v1alpha1.
	// +kubebuilder:default=pod
	// +kubebuilder:validation:Enum=pod
	Backend EnvironmentBackend `json:"backend,omitempty"`

	// IdleTimeout is how long an environment may be inactive before it is paused.
	// +kubebuilder:default="15m"
	// +optional
	IdleTimeout *metav1.Duration `json:"idleTimeout,omitempty"`

	// DiskSize is the size of the workspace volume.
	// +kubebuilder:default="40Gi"
	// +optional
	DiskSize *resource.Quantity `json:"diskSize,omitempty"`

	// WarmPool configures pre-booted standby environments.
	// +optional
	WarmPool *WarmPoolSpec `json:"warmPool,omitempty"`
}

// EnvironmentTemplateStatus defines the observed state of EnvironmentTemplate.
type EnvironmentTemplateStatus struct {
	// WarmPoolReady is the number of unclaimed ready environments in the pool.
	// +optional
	WarmPoolReady int32 `json:"warmPoolReady,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// EnvironmentTemplate is a reusable class of environments.
type EnvironmentTemplate struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EnvironmentTemplateSpec   `json:"spec,omitempty"`
	Status EnvironmentTemplateStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EnvironmentTemplateList contains a list of EnvironmentTemplate.
type EnvironmentTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EnvironmentTemplate `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EnvironmentTemplate{}, &EnvironmentTemplateList{})
}
