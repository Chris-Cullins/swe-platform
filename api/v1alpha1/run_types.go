package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RunState describes where a Run is in its lifecycle.
// +kubebuilder:validation:Enum=Queued;Running;NeedsInput;Done;Failed;Cancelled
type RunState string

const (
	RunStateQueued     RunState = "Queued"
	RunStateRunning    RunState = "Running"
	RunStateNeedsInput RunState = "NeedsInput"
	RunStateDone       RunState = "Done"
	RunStateFailed     RunState = "Failed"
	RunStateCancelled  RunState = "Cancelled"
)

// RunSpec defines one agent task executing in an environment.
type RunSpec struct {
	// EnvironmentRef is the name of the Environment to run in.
	EnvironmentRef string `json:"environmentRef"`

	// Agent names the agent adapter to use (e.g. claude-code, aider).
	Agent string `json:"agent"`

	// Prompt is the task handed to the agent.
	Prompt string `json:"prompt"`

	// Notify lists inboxes (run names, or "parent") that receive lifecycle
	// events when this run reaches a terminal state.
	// +optional
	// +listType=set
	Notify []string `json:"notify,omitempty"`

	// ParentRef links this run to the run that spawned it, if any.
	// +optional
	ParentRef string `json:"parentRef,omitempty"`
}

// RunUsage records consumption attributable to a run.
type RunUsage struct {
	// +optional
	CPUSeconds int64 `json:"cpuSeconds,omitempty"`
	// +optional
	TokensIn int64 `json:"tokensIn,omitempty"`
	// +optional
	TokensOut int64 `json:"tokensOut,omitempty"`
}

// RunStatus defines the observed state of Run.
type RunStatus struct {
	// +optional
	State RunState `json:"state,omitempty"`

	// Branch is the git branch holding the run's changes, once pushed.
	// +optional
	Branch string `json:"branch,omitempty"`

	// TranscriptRef points at the stored transcript (adapter-owned format).
	// +optional
	TranscriptRef string `json:"transcriptRef,omitempty"`

	// +optional
	Usage RunUsage `json:"usage,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Agent",type=string,JSONPath=`.spec.agent`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Run is one agent task executing in an environment.
type Run struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RunSpec   `json:"spec,omitempty"`
	Status RunStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RunList contains a list of Run.
type RunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Run `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Run{}, &RunList{})
}
