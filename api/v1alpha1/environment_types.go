package v1alpha1

import (
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const EnvironmentConditionReady = "Ready"

// EnvironmentSuspensionReason records why the lifecycle controller suspended
// an Environment. It is observed state, not user-owned policy.
// +kubebuilder:validation:Enum=Hold;Idle;Requested
type EnvironmentSuspensionReason string

const (
	EnvironmentSuspensionReasonHold      EnvironmentSuspensionReason = "Hold"
	EnvironmentSuspensionReasonIdle      EnvironmentSuspensionReason = "Idle"
	EnvironmentSuspensionReasonRequested EnvironmentSuspensionReason = "Requested"
)

// EnvironmentActivitySource identifies an independently updated activity
// slot. One latest request per source keeps the API bounded.
// +kubebuilder:validation:Enum=Terminal;Portal;Inbox;Agent;Run
type EnvironmentActivitySource string

const (
	EnvironmentActivitySourceTerminal EnvironmentActivitySource = "Terminal"
	EnvironmentActivitySourcePortal   EnvironmentActivitySource = "Portal"
	EnvironmentActivitySourceInbox    EnvironmentActivitySource = "Inbox"
	EnvironmentActivitySourceAgent    EnvironmentActivitySource = "Agent"
	EnvironmentActivitySourceRun      EnvironmentActivitySource = "Run"
)

// EnvironmentLifecycleRequest is a durable, idempotent lifecycle intent. UID
// prevents a stale request targeting a replacement object and
// HoldPolicyRevision prevents policy changes from reusing an old authorization
// decision.
// +kubebuilder:validation:XValidation:rule="size(self.environmentUID) > 0",message="environmentUID must not be empty"
type EnvironmentLifecycleRequest struct {
	// ID is chosen by the requester and is its idempotency key.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	ID string `json:"id"`

	// EnvironmentUID is the exact Environment incarnation targeted.
	EnvironmentUID types.UID `json:"environmentUID"`

	// HoldPolicyRevision is the policy revision observed by the requester.
	// +kubebuilder:validation:Minimum=0
	HoldPolicyRevision int64 `json:"holdPolicyRevision"`
}

// EnvironmentWakeRequest is an ordinary wake intent scoped to the observed
// suspension reason. This prevents an Idle wake racing a cleanup fence from
// resuming Requested suspension after teardown completes.
type EnvironmentWakeRequest struct {
	EnvironmentLifecycleRequest `json:",inline"`

	// ExpectedSuspensionReason limits the transition this request may release.
	// Empty requests written before scoped wake intents were introduced are
	// interpreted as Idle so migration remains fail-closed for cleanup fences.
	// +optional
	ExpectedSuspensionReason EnvironmentSuspensionReason `json:"expectedSuspensionReason,omitempty"`
}

// EnvironmentActivityRequest is one source's latest activity intent.
type EnvironmentActivityRequest struct {
	Source                      EnvironmentActivitySource `json:"source"`
	EnvironmentLifecycleRequest `json:",inline"`
}

// EnvironmentActivityReceipt records the latest request consumed for a source.
type EnvironmentActivityReceipt struct {
	Source    EnvironmentActivitySource `json:"source"`
	RequestID string                    `json:"requestID"`
}

// EnvironmentHoldPolicy is explicit user/admin-owned policy. Changing Enabled
// requires increasing Revision so in-flight ordinary wake and activity intents
// are fenced from the new decision.
// +kubebuilder:validation:XValidation:rule="self == oldSelf || self.revision > oldSelf.revision",message="revision must increase when hold policy changes"
type EnvironmentHoldPolicy struct {
	Enabled bool `json:"enabled"`

	// +kubebuilder:validation:Minimum=1
	Revision int64 `json:"revision"`
}

// EnvironmentLifecycleSpec contains policy and bounded caller-owned intent.
// Only the lifecycle controller turns these requests into observed transitions.
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.hold) || has(self.hold)",message="hold policy cannot be removed; disable it at a higher revision"
type EnvironmentLifecycleSpec struct {
	// Hold is explicit policy and is never cleared by ordinary wake traffic.
	// +optional
	Hold *EnvironmentHoldPolicy `json:"hold,omitempty"`

	// Wake is the requester's latest ordinary wake intent.
	// +optional
	Wake *EnvironmentWakeRequest `json:"wake,omitempty"`

	// Suspend requests a backend fence without creating an explicit hold. Run
	// cleanup uses this to preserve a workspace after accepted work stops.
	// +optional
	Suspend *EnvironmentLifecycleRequest `json:"suspend,omitempty"`

	// Activity contains legacy source-keyed activity requests. New activity
	// publishers use bounded metadata slots so heartbeats do not advance the
	// Environment generation; the lifecycle controller consumes both forms.
	// +optional
	// +listType=map
	// +listMapKey=source
	// +kubebuilder:validation:MaxItems=5
	Activity []EnvironmentActivityRequest `json:"activity,omitempty"`
}

// EnvironmentLifecycleStatus is controller-owned observed lifecycle state.
type EnvironmentLifecycleStatus struct {
	// Suspended is the controller's decision that backend execution must be absent.
	// +optional
	Suspended bool `json:"suspended,omitempty"`

	// SuspensionReason explains the current suspension.
	// +optional
	SuspensionReason EnvironmentSuspensionReason `json:"suspensionReason,omitempty"`

	// SuspensionRequestID identifies the request that caused Requested suspension.
	// +optional
	SuspensionRequestID string `json:"suspensionRequestID,omitempty"`

	// Epoch increases before each transition into suspension. Backends and
	// endpoints from an older epoch are fenced.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Epoch int64 `json:"epoch,omitempty"`

	// ObservedHoldPolicyRevision is the revision used for this decision.
	// +optional
	ObservedHoldPolicyRevision int64 `json:"observedHoldPolicyRevision,omitempty"`

	// LastWakeRequestID is the latest valid wake consumed, including a wake
	// refused because an explicit hold was enabled.
	// +optional
	LastWakeRequestID string `json:"lastWakeRequestID,omitempty"`

	// LastSuspendRequestID is the latest suspension request accepted. It keeps
	// replay of an acknowledged request idempotent after a later wake.
	// +optional
	LastSuspendRequestID string `json:"lastSuspendRequestID,omitempty"`

	// PendingSuspendRequestID identifies the accepted request whose backend
	// teardown has not yet been acknowledged. It remains authoritative across
	// later hold-policy revisions.
	// +optional
	PendingSuspendRequestID string `json:"pendingSuspendRequestID,omitempty"`

	// ActivityReceipts makes activity consumption idempotent and bounded.
	// +optional
	// +listType=map
	// +listMapKey=source
	// +kubebuilder:validation:MaxItems=5
	ActivityReceipts []EnvironmentActivityReceipt `json:"activityReceipts,omitempty"`
}

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
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.lifecycle) || !has(oldSelf.lifecycle.hold) || (has(self.lifecycle) && has(self.lifecycle.hold))",message="hold policy cannot be removed; disable it at a higher revision"
type EnvironmentSpec struct {
	// ProjectRef is the name of the Project this environment belongs to.
	// +optional
	ProjectRef string `json:"projectRef,omitempty"`

	// TemplateRef is the name of the EnvironmentTemplate to build from.
	// +kubebuilder:validation:MinLength=1
	TemplateRef string `json:"templateRef"`

	// Backend overrides the template's backend when set. Only pod is currently
	// supported.
	// +kubebuilder:validation:Enum=pod
	// +optional
	Backend EnvironmentBackend `json:"backend,omitempty"`

	// Lifecycle contains explicit hold policy and bounded lifecycle requests.
	// +optional
	Lifecycle EnvironmentLifecycleSpec `json:"lifecycle,omitempty"`

	// Paused is a deprecated compatibility input. The lifecycle controller
	// migrates true to an explicit hold and clears it. New clients must
	// use Lifecycle.Hold or lifecycle requests.
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

	// Lifecycle is observed state owned by the lifecycle controller.
	// +optional
	Lifecycle EnvironmentLifecycleStatus `json:"lifecycle,omitempty"`

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
