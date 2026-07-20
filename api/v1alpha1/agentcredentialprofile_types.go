package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// AgentCredentialAPIKeySecretType identifies controller-managed API key Secrets.
	AgentCredentialAPIKeySecretType corev1.SecretType = "swe.dev/agent-api-key"
	// AgentCredentialAPIKeySecretKey is the only data key allowed in an API key Secret.
	AgentCredentialAPIKeySecretKey = "apiKey"
	// AgentCredentialAPIKeyMaxBytes is the maximum permitted API key size.
	AgentCredentialAPIKeyMaxBytes = 16 * 1024
)

// AgentCredentialSecretName returns the deterministic backing Secret name for a profile UID.
func AgentCredentialSecretName(profileUID types.UID) string {
	digest := sha256.Sum256([]byte(profileUID))
	return "agent-credential-" + hex.EncodeToString(digest[:16])
}

// AgentCredentialType identifies the credential format stored for an agent.
// +kubebuilder:validation:Enum=APIKey
type AgentCredentialType string

const AgentCredentialTypeAPIKey AgentCredentialType = "APIKey"

// AgentCredentialProfileSpec describes a purpose-scoped agent credential.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable"
type AgentCredentialProfileSpec struct {
	// Adapter is the agent adapter allowed to consume this credential.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Adapter string `json:"adapter"`

	// CredentialType selects the credential format.
	CredentialType AgentCredentialType `json:"credentialType"`
}

// +kubebuilder:object:root=true

// AgentCredentialProfile declares metadata for an agent credential. Secret material is
// stored separately and is never represented in this API.
type AgentCredentialProfile struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +kubebuilder:validation:Required
	Spec AgentCredentialProfileSpec `json:"spec"`
}

// +kubebuilder:object:root=true

// AgentCredentialProfileList contains a list of AgentCredentialProfile.
type AgentCredentialProfileList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AgentCredentialProfile `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AgentCredentialProfile{}, &AgentCredentialProfileList{})
}
