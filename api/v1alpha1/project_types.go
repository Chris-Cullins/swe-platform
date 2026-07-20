package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// ChangesWorkflow selects what happens to a run's changes by default.
// +kubebuilder:validation:Enum=branch-pr;ship-to-main
type ChangesWorkflow string

const (
	ChangesWorkflowBranchPR   ChangesWorkflow = "branch-pr"
	ChangesWorkflowShipToMain ChangesWorkflow = "ship-to-main"
)

// ProjectSpec defines a repository and its shared configuration.
type ProjectSpec struct {
	// Repositories contains the single git repository URL this project works on.
	// It intentionally remains a list pending a future structured multi-repository contract.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=1
	// +listType=set
	Repositories []string `json:"repositories"`

	// TemplateRef is the default EnvironmentTemplate for this project.
	// +optional
	TemplateRef string `json:"templateRef,omitempty"`

	// ChangesWorkflow selects the default action for finished runs.
	// +kubebuilder:default=branch-pr
	// +optional
	ChangesWorkflow ChangesWorkflow `json:"changesWorkflow,omitempty"`

	// EgressAllowlist lists hosts environments for this project may reach.
	// Everything else is denied by the egress proxy.
	// +optional
	// +listType=set
	EgressAllowlist []string `json:"egressAllowlist,omitempty"`
}

// ProjectStatus defines the observed state of Project.
type ProjectStatus struct {
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Project is a repository plus the configuration environments need to work on it.
type Project struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ProjectSpec   `json:"spec,omitempty"`
	Status ProjectStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ProjectList contains a list of Project.
type ProjectList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Project `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Project{}, &ProjectList{})
}
