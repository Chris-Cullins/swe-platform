package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// InstallationIsolationMode is the administrator-selected installation-wide
// isolation contract. It is configuration, not evidence that isolation is
// active.
// +kubebuilder:validation:Enum=UnrestrictedDevelopment;RestrictedProductionCalicoV3_32_1
type InstallationIsolationMode string

const (
	InstallationIsolationModeUnrestrictedDevelopment           InstallationIsolationMode = "UnrestrictedDevelopment"
	InstallationIsolationModeRestrictedProductionCalicoV3_32_1 InstallationIsolationMode = "RestrictedProductionCalicoV3_32_1"
)

// InstallationRuntimeClassExpectation names the exact RuntimeClass contract
// required by a restricted isolation selection.
type InstallationRuntimeClassExpectation struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Name string `json:"name"`

	// Handler is the exact RuntimeClass handler expected by the administrator.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Handler string `json:"handler"`
}

// InstallationStorageClassExpectation names the exact StorageClass contract
// required by a restricted isolation selection.
type InstallationStorageClassExpectation struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Name string `json:"name"`

	// CSIDriver is the exact CSI provisioner expected on the StorageClass.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	CSIDriver string `json:"csiDriver"`
}

// InstallationIsolationSpec is one explicit installation-wide administrator
// selection. Restricted inputs are references and expectations only; the
// administrator-owned egress policy ConfigMap remains network-policy authority.
// +kubebuilder:validation:XValidation:rule="self.mode == 'UnrestrictedDevelopment' ? (!has(self.policyConfigMapName) && !has(self.runtimeClass) && !has(self.storageClass)) : (has(self.policyConfigMapName) && has(self.runtimeClass) && has(self.storageClass))",message="development mode must omit restricted inputs; restricted mode requires policyConfigMapName, runtimeClass, and storageClass"
type InstallationIsolationSpec struct {
	Mode InstallationIsolationMode `json:"mode"`

	// PolicyConfigMapName references the existing immutable administrator-owned
	// egress policy ConfigMap contract.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	PolicyConfigMapName string `json:"policyConfigMapName,omitempty"`

	// +optional
	RuntimeClass *InstallationRuntimeClassExpectation `json:"runtimeClass,omitempty"`

	// +optional
	StorageClass *InstallationStorageClassExpectation `json:"storageClass,omitempty"`
}

// InstallationSpec preserves an omitted isolation selection only for staged
// migration of legacy installations. There is deliberately no default.
type InstallationSpec struct {
	// Isolation is the sole installation-wide isolation selection. Installation
	// UID remains the tenancy identity regardless of this configuration.
	// +optional
	Isolation *InstallationIsolationSpec `json:"isolation,omitempty"`
}

// InstallationIsolationState is bounded controller-owned vocabulary reserved
// for the staged activation flow. No controller writes it yet.
// +kubebuilder:validation:Enum=LegacyUnclassified;Fencing;Blocked;Active
type InstallationIsolationState string

const (
	InstallationIsolationStateLegacyUnclassified InstallationIsolationState = "LegacyUnclassified"
	InstallationIsolationStateFencing            InstallationIsolationState = "Fencing"
	InstallationIsolationStateBlocked            InstallationIsolationState = "Blocked"
	InstallationIsolationStateActive             InstallationIsolationState = "Active"
)

// InstallationRuntimeClassIdentity records an exact non-secret RuntimeClass
// incarnation and observed handler.
// +kubebuilder:validation:XValidation:rule="size(self.uid) > 0 && size(self.uid) <= 128",message="object UID must be 1-128 bytes"
type InstallationRuntimeClassIdentity struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Name string `json:"name"`

	UID types.UID `json:"uid"`

	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Handler string `json:"handler"`
}

// InstallationStorageClassIdentity records an exact non-secret StorageClass
// incarnation and observed CSI driver.
// +kubebuilder:validation:XValidation:rule="size(self.uid) > 0 && size(self.uid) <= 128",message="object UID must be 1-128 bytes"
type InstallationStorageClassIdentity struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Name string `json:"name"`

	UID types.UID `json:"uid"`

	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	CSIDriver string `json:"csiDriver"`
}

// InstallationPolicyConfigMapIdentity records the exact immutable policy
// ConfigMap incarnation and canonical content digest.
// +kubebuilder:validation:XValidation:rule="size(self.uid) > 0 && size(self.uid) <= 128",message="ConfigMap UID must be 1-128 bytes"
type InstallationPolicyConfigMapIdentity struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Name string `json:"name"`

	UID types.UID `json:"uid"`

	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	ContentSHA256 string `json:"contentSHA256"`
}

// InstallationStatus contains bounded non-secret observations reserved for the
// future installation isolation controller. This slice does not write status.
type InstallationStatus struct {
	// +optional
	IsolationState InstallationIsolationState `json:"isolationState,omitempty"`

	// ObservedIsolation is the exact selection used for these observations.
	// +optional
	ObservedIsolation *InstallationIsolationSpec `json:"observedIsolation,omitempty"`

	// IsolationRevision is SHA-256 over the versioned canonical selection and
	// exact resolved object identities.
	// +optional
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	IsolationRevision string `json:"isolationRevision,omitempty"`

	// +optional
	PolicyConfigMap *InstallationPolicyConfigMapIdentity `json:"policyConfigMap,omitempty"`

	// +optional
	RuntimeClass *InstallationRuntimeClassIdentity `json:"runtimeClass,omitempty"`

	// +optional
	StorageClass *InstallationStorageClassIdentity `json:"storageClass,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=16
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
