package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EnvironmentPhase describes where an Environment is in its lifecycle.
// +kubebuilder:validation:Enum=Creating;Setup;Ready;Running;Idle;Paused;Resuming;Failed;Terminated
type EnvironmentPhase string

const (
	EnvironmentPhaseCreating   EnvironmentPhase = "Creating"
	EnvironmentPhaseSetup      EnvironmentPhase = "Setup"
	EnvironmentPhaseReady      EnvironmentPhase = "Ready"
	EnvironmentPhaseRunning    EnvironmentPhase = "Running"
	EnvironmentPhaseIdle       EnvironmentPhase = "Idle"
	EnvironmentPhasePaused     EnvironmentPhase = "Paused"
	EnvironmentPhaseResuming   EnvironmentPhase = "Resuming"
	EnvironmentPhaseFailed     EnvironmentPhase = "Failed"
	EnvironmentPhaseTerminated EnvironmentPhase = "Terminated"
)

// EnvironmentBackend selects where the environment executes.
// +kubebuilder:validation:Enum=pod;kubevirt;external-runner
type EnvironmentBackend string

const (
	EnvironmentBackendPod            EnvironmentBackend = "pod"
	EnvironmentBackendKubeVirt       EnvironmentBackend = "kubevirt"
	EnvironmentBackendExternalRunner EnvironmentBackend = "external-runner"
)

// EnvironmentSpec defines the desired state of Environment.
type EnvironmentSpec struct {
	// ProjectRef is the name of the Project this environment belongs to.
	// +optional
	ProjectRef string `json:"projectRef,omitempty"`

	// TemplateRef is the name of the EnvironmentTemplate to build from.
	TemplateRef string `json:"templateRef"`

	// Backend overrides the template's backend.
	// +optional
	Backend EnvironmentBackend `json:"backend,omitempty"`

	// Paused requests that the environment be paused (pod deleted, disk retained).
	// +optional
	Paused bool `json:"paused,omitempty"`
}

// EnvironmentEndpoints exposes how to reach services inside the environment.
type EnvironmentEndpoints struct {
	// Sandboxd is the address of the environment's sandboxd gRPC API.
	// +optional
	Sandboxd string `json:"sandboxd,omitempty"`

	// Terminal is the websocket address of the shared terminal.
	// +optional
	Terminal string `json:"terminal,omitempty"`
}

// EnvironmentStatus defines the observed state of Environment.
type EnvironmentStatus struct {
	// +optional
	Phase EnvironmentPhase `json:"phase,omitempty"`

	// ClaimedBy is the Run currently using a reusable Environment. Controller-
	// owned Environments do not use claims. Name and UID fence stale Runs.
	// +optional
	ClaimedBy *RunReference `json:"claimedBy,omitempty"`

	// +optional
	Endpoints EnvironmentEndpoints `json:"endpoints,omitempty"`

	// PodName is the name of the backing pod, when one exists.
	// +optional
	PodName string `json:"podName,omitempty"`

	// LastActiveAt records the last time the environment saw user or agent activity.
	// The idle reaper uses it to decide when to pause.
	// +optional
	LastActiveAt *metav1.Time `json:"lastActiveAt,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Template",type=string,JSONPath=`.spec.templateRef`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Environment is one ephemeral machine an agent works in.
type Environment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EnvironmentSpec   `json:"spec,omitempty"`
	Status EnvironmentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EnvironmentList contains a list of Environment.
type EnvironmentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Environment `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Environment{}, &EnvironmentList{})
}
