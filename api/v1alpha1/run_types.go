package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// RunState describes where a Run is in its lifecycle.
// +kubebuilder:validation:Enum=Allocating;EnvironmentReady;AdapterAccepted;Running;NeedsInput;Paused;Succeeded;Failed;Cancelled
type RunState string

const (
	RunStateAllocating       RunState = "Allocating"
	RunStateEnvironmentReady RunState = "EnvironmentReady"
	RunStateAdapterAccepted  RunState = "AdapterAccepted"
	RunStateRunning          RunState = "Running"
	RunStateNeedsInput       RunState = "NeedsInput"
	RunStatePaused           RunState = "Paused"
	RunStateSucceeded        RunState = "Succeeded"
	RunStateFailed           RunState = "Failed"
	RunStateCancelled        RunState = "Cancelled"
)

// RunSpec defines one agent task executing in an environment.
// A Run either claims environmentRef or asks the controller to allocate an
// Environment from templateRef/projectRef.
// +kubebuilder:validation:XValidation:rule="has(self.environmentRef) ? (!has(self.templateRef) && !has(self.projectRef)) : (has(self.templateRef) || has(self.projectRef))",message="set environmentRef or templateRef/projectRef, not both"
// +kubebuilder:validation:XValidation:rule="self.agent == oldSelf.agent && self.prompt == oldSelf.prompt && ((!has(self.environmentRef) && !has(oldSelf.environmentRef)) || (has(self.environmentRef) && has(oldSelf.environmentRef) && self.environmentRef == oldSelf.environmentRef)) && ((!has(self.projectRef) && !has(oldSelf.projectRef)) || (has(self.projectRef) && has(oldSelf.projectRef) && self.projectRef == oldSelf.projectRef)) && ((!has(self.templateRef) && !has(oldSelf.templateRef)) || (has(self.templateRef) && has(oldSelf.templateRef) && self.templateRef == oldSelf.templateRef)) && ((!has(self.credentialProfileRef) && !has(oldSelf.credentialProfileRef)) || (has(self.credentialProfileRef) && has(oldSelf.credentialProfileRef) && self.credentialProfileRef == oldSelf.credentialProfileRef))",message="agent, prompt, environment selection, and credential profile are immutable"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.cancel) || !oldSelf.cancel || (has(self.cancel) && self.cancel)",message="cancel cannot be unset"
type RunSpec struct {
	// EnvironmentRef claims an existing Environment. Claimed Environments are
	// released, not deleted, when the Run terminates.
	// +optional
	// +kubebuilder:validation:MinLength=1
	EnvironmentRef string `json:"environmentRef,omitempty"`

	// ProjectRef configures a controller-allocated Environment and supplies its
	// default template when templateRef is empty.
	// +optional
	// +kubebuilder:validation:MinLength=1
	ProjectRef string `json:"projectRef,omitempty"`

	// TemplateRef selects the template for a controller-allocated Environment.
	// +optional
	// +kubebuilder:validation:MinLength=1
	TemplateRef string `json:"templateRef,omitempty"`

	// CredentialProfileRef selects an AgentCredentialProfile for this run.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	CredentialProfileRef string `json:"credentialProfileRef,omitempty"`

	// Agent names the agent adapter to use (e.g. claude-code, aider).
	Agent string `json:"agent"`

	// Prompt is the task handed to the agent.
	Prompt string `json:"prompt"`

	// Cancel requests idempotent cancellation. It cannot be unset.
	// +optional
	Cancel bool `json:"cancel,omitempty"`

	// Notify lists inboxes (run names, or "parent") that receive lifecycle
	// events when this run reaches a terminal state.
	// +optional
	// +listType=set
	Notify []string `json:"notify,omitempty"`

	// ParentRef links this run to the run that spawned it, if any.
	// +optional
	ParentRef string `json:"parentRef,omitempty"`
}

// RunReference identifies a Run without allowing a same-name replacement to
// inherit or release its Environment claim.
type RunReference struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
}

// EnvironmentOwnership determines terminal and deletion cleanup.
// +kubebuilder:validation:Enum=Owned;Claimed
type EnvironmentOwnership string

const (
	EnvironmentOwnershipOwned   EnvironmentOwnership = "Owned"
	EnvironmentOwnershipClaimed EnvironmentOwnership = "Claimed"
)

// RunEnvironmentReference records the exact Environment allocated to a Run.
type RunEnvironmentReference struct {
	Name      string               `json:"name"`
	UID       types.UID            `json:"uid"`
	Ownership EnvironmentOwnership `json:"ownership"`
}

// RunCredentialProfileReference records the exact credential profile selected for a Run.
type RunCredentialProfileReference struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
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

	// EnvironmentRef is the exact owned or claimed Environment incarnation. It
	// remains as historical identity after terminal cleanup.
	// +optional
	EnvironmentRef *RunEnvironmentReference `json:"environmentRef,omitempty"`

	// CredentialProfileRef is the exact profile incarnation selected for this Run.
	// It remains as historical identity after the Run terminates.
	// +optional
	CredentialProfileRef *RunCredentialProfileReference `json:"credentialProfileRef,omitempty"`

	// ObservedGeneration is the Run generation reflected by this status.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// AcceptedEnvironmentEpoch is the Environment lifecycle epoch in which the
	// adapter most recently accepted this Run. A different current epoch fences
	// observation until credentials are rematerialized and acceptance succeeds.
	// +kubebuilder:validation:Minimum=0
	// +optional
	AcceptedEnvironmentEpoch *int64 `json:"acceptedEnvironmentEpoch,omitempty"`

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
