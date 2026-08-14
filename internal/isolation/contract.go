// Package isolation validates the inert Installation isolation selection and
// derives its canonical identity. It is intentionally not wired to a controller.
package isolation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/egresspolicy"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	// RevisionDomain changes whenever canonical isolation identity semantics or
	// a security-relevant input changes.
	RevisionDomain = "swe.dev/isolation/installation-revision/v1"

	configMapAPIVersion    = "v1"
	runtimeClassAPIVersion = "node.k8s.io/v1"
	storageClassAPIVersion = "storage.k8s.io/v1"
)

// PolicyConfigMapIdentity is the exact immutable policy object and canonical
// content used to resolve a restricted selection.
type PolicyConfigMapIdentity struct {
	UID           types.UID
	ContentSHA256 [sha256.Size]byte
}

// RuntimeClassIdentity is the exact RuntimeClass object and observed handler.
type RuntimeClassIdentity struct {
	UID     types.UID
	Handler string
}

// StorageClassIdentity is the exact StorageClass object and observed CSI
// provisioner.
type StorageClassIdentity struct {
	UID       types.UID
	CSIDriver string
}

// RevisionInputs are the selected Installation contract and exact objects that
// resolved it. Restricted identities are absent for unrestricted development.
type RevisionInputs struct {
	InstallationUID types.UID
	Selection       platformv1alpha1.InstallationIsolationSpec

	PolicyConfigMap *PolicyConfigMapIdentity
	RuntimeClass    *RuntimeClassIdentity
	StorageClass    *StorageClassIdentity
}

// Revision is the SHA-256 digest of canonical installation isolation inputs.
type Revision [sha256.Size]byte

func (r Revision) String() string { return hex.EncodeToString(r[:]) }

// ValidateSelection validates the complete cross-field selection contract.
// Nil is accepted only for the staged legacy migration represented by an
// omitted Installation.spec.isolation.
func ValidateSelection(selection *platformv1alpha1.InstallationIsolationSpec) error {
	if selection == nil {
		return nil
	}
	switch selection.Mode {
	case platformv1alpha1.InstallationIsolationModeUnrestrictedDevelopment:
		if selection.PolicyConfigMapName != "" || selection.RuntimeClass != nil || selection.StorageClass != nil {
			return errors.New("unrestricted development isolation must omit restricted inputs")
		}
		return nil
	case platformv1alpha1.InstallationIsolationModeRestrictedProductionCalicoV3_32_1:
		if selection.PolicyConfigMapName == "" || selection.RuntimeClass == nil || selection.StorageClass == nil {
			return errors.New("restricted isolation requires policy ConfigMap, RuntimeClass, and StorageClass expectations")
		}
		if err := dnsSubdomain("policy ConfigMap name", selection.PolicyConfigMapName, 253); err != nil {
			return err
		}
		if err := dnsSubdomain("RuntimeClass name", selection.RuntimeClass.Name, 253); err != nil {
			return err
		}
		if problems := validation.IsDNS1123Label(selection.RuntimeClass.Handler); len(problems) != 0 {
			return fmt.Errorf("RuntimeClass handler is invalid: %s", problems[0])
		}
		if err := dnsSubdomain("StorageClass name", selection.StorageClass.Name, 253); err != nil {
			return err
		}
		if err := dnsSubdomain("CSI driver", selection.StorageClass.CSIDriver, 63); err != nil {
			return err
		}
		return nil
	default:
		return errors.New("isolation mode must be UnrestrictedDevelopment or RestrictedProductionCalicoV3_32_1")
	}
}

func dnsSubdomain(field, value string, maximum int) error {
	if len(value) > maximum {
		return fmt.Errorf("%s must not exceed %d bytes", field, maximum)
	}
	if problems := validation.IsDNS1123Subdomain(value); len(problems) != 0 {
		return fmt.Errorf("%s is invalid: %s", field, problems[0])
	}
	return nil
}

// CanonicalBytes validates and serializes revision inputs deterministically.
func (in RevisionInputs) CanonicalBytes() ([]byte, error) {
	if in.InstallationUID == "" {
		return nil, errors.New("Installation UID is required")
	}
	if err := ValidateSelection(&in.Selection); err != nil {
		return nil, err
	}

	type canonicalObject struct {
		APIVersion string    `json:"apiVersion"`
		Kind       string    `json:"kind"`
		Name       string    `json:"name"`
		UID        types.UID `json:"uid"`
		Value      string    `json:"value,omitempty"`
		SHA256     string    `json:"sha256,omitempty"`
	}
	type canonicalSelection struct {
		Mode                platformv1alpha1.InstallationIsolationMode `json:"mode"`
		PolicyConfigMapName string                                     `json:"policyConfigMapName,omitempty"`
		RuntimeClassName    string                                     `json:"runtimeClassName,omitempty"`
		RuntimeClassHandler string                                     `json:"runtimeClassHandler,omitempty"`
		StorageClassName    string                                     `json:"storageClassName,omitempty"`
		StorageClassDriver  string                                     `json:"storageClassDriver,omitempty"`
	}
	type canonicalInputs struct {
		Domain                      string             `json:"domain"`
		EgressPolicyAPIVersion      string             `json:"egressPolicyAPIVersion,omitempty"`
		EgressRuntimeRevisionDomain string             `json:"egressRuntimeRevisionDomain,omitempty"`
		InstallationUID             types.UID          `json:"installationUID"`
		Selection                   canonicalSelection `json:"selection"`
		PolicyConfigMap             *canonicalObject   `json:"policyConfigMap,omitempty"`
		RuntimeClass                *canonicalObject   `json:"runtimeClass,omitempty"`
		StorageClass                *canonicalObject   `json:"storageClass,omitempty"`
	}

	canonical := canonicalInputs{
		Domain:          RevisionDomain,
		InstallationUID: in.InstallationUID,
		Selection: canonicalSelection{
			Mode: in.Selection.Mode,
		},
	}
	switch in.Selection.Mode {
	case platformv1alpha1.InstallationIsolationModeUnrestrictedDevelopment:
		if in.PolicyConfigMap != nil || in.RuntimeClass != nil || in.StorageClass != nil {
			return nil, errors.New("unrestricted development revision must omit resolved restricted objects")
		}
	case platformv1alpha1.InstallationIsolationModeRestrictedProductionCalicoV3_32_1:
		if in.PolicyConfigMap == nil || in.RuntimeClass == nil || in.StorageClass == nil {
			return nil, errors.New("restricted isolation revision requires all resolved object identities")
		}
		if in.PolicyConfigMap.UID == "" || in.PolicyConfigMap.ContentSHA256 == ([sha256.Size]byte{}) || in.RuntimeClass.UID == "" || in.StorageClass.UID == "" {
			return nil, errors.New("restricted isolation revision requires exact non-empty object identities and policy content SHA-256")
		}
		if in.RuntimeClass.Handler != in.Selection.RuntimeClass.Handler {
			return nil, errors.New("observed RuntimeClass handler does not match the selected expectation")
		}
		if in.StorageClass.CSIDriver != in.Selection.StorageClass.CSIDriver {
			return nil, errors.New("observed StorageClass CSI driver does not match the selected expectation")
		}
		canonical.EgressPolicyAPIVersion = egresspolicy.ConfigAPIVersion
		canonical.EgressRuntimeRevisionDomain = egresspolicy.RuntimeRevisionDomain
		canonical.Selection.PolicyConfigMapName = in.Selection.PolicyConfigMapName
		canonical.Selection.RuntimeClassName = in.Selection.RuntimeClass.Name
		canonical.Selection.RuntimeClassHandler = in.Selection.RuntimeClass.Handler
		canonical.Selection.StorageClassName = in.Selection.StorageClass.Name
		canonical.Selection.StorageClassDriver = in.Selection.StorageClass.CSIDriver
		canonical.PolicyConfigMap = &canonicalObject{APIVersion: configMapAPIVersion, Kind: "ConfigMap", Name: in.Selection.PolicyConfigMapName, UID: in.PolicyConfigMap.UID, SHA256: hex.EncodeToString(in.PolicyConfigMap.ContentSHA256[:])}
		canonical.RuntimeClass = &canonicalObject{APIVersion: runtimeClassAPIVersion, Kind: "RuntimeClass", Name: in.Selection.RuntimeClass.Name, UID: in.RuntimeClass.UID, Value: in.RuntimeClass.Handler}
		canonical.StorageClass = &canonicalObject{APIVersion: storageClassAPIVersion, Kind: "StorageClass", Name: in.Selection.StorageClass.Name, UID: in.StorageClass.UID, Value: in.StorageClass.CSIDriver}
	}
	return json.Marshal(canonical)
}

// DeriveRevision returns SHA-256 over CanonicalBytes.
func (in RevisionInputs) DeriveRevision() (Revision, error) {
	canonical, err := in.CanonicalBytes()
	if err != nil {
		return Revision{}, err
	}
	return sha256.Sum256(canonical), nil
}
