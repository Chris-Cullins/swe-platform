package v1alpha1

import (
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const EnvironmentConditionReady = "Ready"

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

// EnvironmentBackend selects where the environment executes. The v1alpha1 Go
// API retains planned backend names for compatibility, but CRD admission only
// accepts pod until another backend is implemented.
// +kubebuilder:validation:Enum=pod
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

	// Backend overrides the template's backend when set. Only pod is currently
	// supported.
	// +kubebuilder:validation:Enum=pod
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
	// ObservedGeneration is the Environment generation reflected by this status.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

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

	// ImageID is the immutable runtime image identity reported for the current
	// environment container. It is cleared when no backing pod exists.
	// +optional
	ImageID string `json:"imageID,omitempty"`

	// LastActiveAt records the last time the environment saw user or agent activity.
	// The idle reaper uses it to decide when to pause.
	// +optional
	LastActiveAt *metav1.Time `json:"lastActiveAt,omitempty"`

	// PodRecoveryAttempts is the number of terminal Pod replacements attempted
	// since the current Pod recovery sequence last became ready.
	// +optional
	PodRecoveryAttempts int32 `json:"podRecoveryAttempts,omitempty"`

	// PodRecoveryExhausted reports that automatic terminal Pod replacement has
	// consumed its retry budget. It remains set until sandboxd becomes ready.
	// +optional
	PodRecoveryExhausted bool `json:"podRecoveryExhausted,omitempty"`

	// PodRecoveryUID identifies the exact terminal Pod covered by the pending or
	// in-progress recovery attempt.
	// +optional
	PodRecoveryUID types.UID `json:"podRecoveryUID,omitempty"`

	// PodRecoveryNextAttemptAt is when the controller may next replace a
	// terminal Pod.
	// +optional
	PodRecoveryNextAttemptAt *metav1.Time `json:"podRecoveryNextAttemptAt,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// IsEnvironmentReady reports whether readiness is true for the current spec
// generation. Phase remains a human-readable summary, not the readiness contract.
func IsEnvironmentReady(environment *Environment) bool {
	if !environment.DeletionTimestamp.IsZero() {
		return false
	}
	condition := apimeta.FindStatusCondition(environment.Status.Conditions, EnvironmentConditionReady)
	return environment.Status.ObservedGeneration == environment.Generation && condition != nil &&
		condition.ObservedGeneration == environment.Generation && condition.Status == metav1.ConditionTrue
}

// EffectiveEnvironmentBackend resolves an Environment's explicit override
// before its template default. Empty values preserve the admission default of
// pod for objects created before CRD defaulting or in tests.
func EffectiveEnvironmentBackend(environment *Environment, template *EnvironmentTemplate) EnvironmentBackend {
	if environment.Spec.Backend != "" {
		return environment.Spec.Backend
	}
	if template.Spec.Backend != "" {
		return template.Spec.Backend
	}
	return EnvironmentBackendPod
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
