// Package tenancy defines the exact installation and Project namespace claim
// boundary shared by the operator, control plane, and onboarding CLI.
package tenancy

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

type Mode string

const (
	ModeScoped       Mode = "scoped"
	ModeTrustedAdmin Mode = "trusted-admin"

	InstallationNamespaceAnnotation = "swe.dev/installation-namespace"
	InstallationNameAnnotation      = "swe.dev/installation-name"
	InstallationUIDAnnotation       = "swe.dev/installation-uid"
	ProjectNameAnnotation           = "swe.dev/project-name"
	ProjectUIDAnnotation            = "swe.dev/project-uid"
	LifecycleAnnotation             = "swe.dev/project-namespace-lifecycle"
	LifecycleOperationAnnotation    = "swe.dev/project-namespace-operation"

	CatalogSourceAnnotation    = "swe.dev/catalog-source"
	CatalogNameAnnotation      = "swe.dev/catalog-name"
	CatalogRevisionAnnotation  = "swe.dev/catalog-revision"
	CatalogSourceUIDAnnotation = "swe.dev/catalog-source-uid"

	BaselineVersionAnnotation = "swe.dev/project-baseline-version"
	BaselineVersion           = "v1"
	EnvironmentServiceAccount = "swe-environment"
	BaselineResourceQuota     = "swe-project"
	BaselineIngressPolicy     = "swe-default-deny-ingress"
	OperatorRoleBinding       = "swe-platform-operator"
	ControlPlaneRoleBinding   = "swe-platform-control-plane"

	OperatorServiceAccountAnnotation     = "swe.dev/operator-service-account"
	OperatorClusterRoleAnnotation        = "swe.dev/operator-cluster-role"
	ControlPlaneServiceAccountAnnotation = "swe.dev/control-plane-service-account"
	ControlPlaneClusterRoleAnnotation    = "swe.dev/control-plane-cluster-role"
)

type Lifecycle string

const (
	LifecycleActive  Lifecycle = "active"
	LifecycleFencing Lifecycle = "fencing"
	LifecycleFenced  Lifecycle = "fenced"

	OperationOnboarding  = "onboarding"
	OperationOffboarding = "offboarding"
)

var ErrOutOfScope = errors.New("namespace is outside this installation's active claim")

type InstallationIdentity struct {
	Key types.NamespacedName
	UID types.UID
}

type Claim struct {
	NamespaceUID types.UID
	ProjectName  string
	ProjectUID   types.UID
	Lifecycle    Lifecycle
	Operation    string
}

type Verifier struct {
	Reader       client.Reader
	Installation InstallationIdentity
	Mode         Mode
}

func ParseMode(value string) (Mode, error) {
	mode := Mode(strings.TrimSpace(value))
	switch mode {
	case ModeScoped, ModeTrustedAdmin:
		return mode, nil
	default:
		return "", fmt.Errorf("tenancy mode must be %q or %q", ModeScoped, ModeTrustedAdmin)
	}
}

func LoadInstallation(ctx context.Context, reader client.Reader, key types.NamespacedName) (InstallationIdentity, *platformv1alpha1.Installation, error) {
	if reader == nil {
		return InstallationIdentity{}, nil, errors.New("installation reader is required")
	}
	if key.Namespace == "" || key.Name == "" {
		return InstallationIdentity{}, nil, errors.New("installation namespace and name are required")
	}
	var installation platformv1alpha1.Installation
	if err := reader.Get(ctx, key, &installation); err != nil {
		return InstallationIdentity{}, nil, fmt.Errorf("get Installation %s/%s: %w", key.Namespace, key.Name, err)
	}
	if installation.UID == "" || !installation.DeletionTimestamp.IsZero() {
		return InstallationIdentity{}, nil, fmt.Errorf("Installation %s/%s has no stable live UID", key.Namespace, key.Name)
	}
	identity := InstallationIdentity{Key: key, UID: installation.UID}
	return identity, &installation, nil
}

func (v Verifier) VerifyInstallation(ctx context.Context) error {
	if v.Reader == nil {
		return errors.New("tenancy verifier reader is required")
	}
	var current platformv1alpha1.Installation
	if err := v.Reader.Get(ctx, v.Installation.Key, &current); err != nil {
		return fmt.Errorf("revalidate Installation %s/%s: %w", v.Installation.Key.Namespace, v.Installation.Key.Name, err)
	}
	if current.UID != v.Installation.UID || !current.DeletionTimestamp.IsZero() {
		return fmt.Errorf("%w: Installation identity changed", ErrOutOfScope)
	}
	return nil
}

func (v Verifier) VerifyNamespace(ctx context.Context, namespace string) (Claim, error) {
	if err := v.VerifyInstallation(ctx); err != nil {
		return Claim{}, err
	}
	if strings.TrimSpace(namespace) == "" {
		return Claim{}, fmt.Errorf("%w: namespace is required", ErrOutOfScope)
	}
	var ns corev1.Namespace
	if err := v.Reader.Get(ctx, types.NamespacedName{Name: namespace}, &ns); err != nil {
		return Claim{}, fmt.Errorf("revalidate Namespace %q: %w", namespace, err)
	}
	if ns.UID == "" || !ns.DeletionTimestamp.IsZero() {
		return Claim{}, fmt.Errorf("%w: Namespace %q is not a stable live identity", ErrOutOfScope, namespace)
	}
	annotations := ns.GetAnnotations()
	if annotations[InstallationNamespaceAnnotation] != v.Installation.Key.Namespace ||
		annotations[InstallationNameAnnotation] != v.Installation.Key.Name ||
		types.UID(annotations[InstallationUIDAnnotation]) != v.Installation.UID {
		return Claim{}, fmt.Errorf("%w: Namespace %q is not claimed by Installation %s/%s (%s)", ErrOutOfScope, namespace, v.Installation.Key.Namespace, v.Installation.Key.Name, v.Installation.UID)
	}
	claim := Claim{
		NamespaceUID: ns.UID,
		ProjectName:  strings.TrimSpace(annotations[ProjectNameAnnotation]),
		ProjectUID:   types.UID(strings.TrimSpace(annotations[ProjectUIDAnnotation])),
		Lifecycle:    Lifecycle(strings.TrimSpace(annotations[LifecycleAnnotation])),
		Operation:    strings.TrimSpace(annotations[LifecycleOperationAnnotation]),
	}
	if claim.ProjectName == "" || claim.ProjectUID == "" {
		return Claim{}, fmt.Errorf("%w: Namespace %q has an incomplete Project identity", ErrOutOfScope, namespace)
	}
	switch claim.Lifecycle {
	case LifecycleActive, LifecycleFenced:
		if claim.Operation != "" {
			return Claim{}, fmt.Errorf("%w: Namespace %q lifecycle %q must not have operation %q", ErrOutOfScope, namespace, claim.Lifecycle, claim.Operation)
		}
	case LifecycleFencing:
		if claim.Operation != OperationOnboarding && claim.Operation != OperationOffboarding {
			return Claim{}, fmt.Errorf("%w: Namespace %q fencing operation %q is invalid", ErrOutOfScope, namespace, claim.Operation)
		}
	default:
		return Claim{}, fmt.Errorf("%w: Namespace %q has invalid lifecycle %q", ErrOutOfScope, namespace, claim.Lifecycle)
	}
	var projects platformv1alpha1.ProjectList
	if err := v.Reader.List(ctx, &projects, client.InNamespace(namespace)); err != nil {
		return Claim{}, fmt.Errorf("list Projects in Namespace %q: %w", namespace, err)
	}
	if len(projects.Items) != 1 || projects.Items[0].Name != claim.ProjectName || projects.Items[0].UID != claim.ProjectUID || !projects.Items[0].DeletionTimestamp.IsZero() {
		return Claim{}, fmt.Errorf("%w: Namespace %q must contain exactly Project %s (%s)", ErrOutOfScope, namespace, claim.ProjectName, claim.ProjectUID)
	}
	return claim, nil
}

func (v Verifier) ValidateConfiguredNamespaces(ctx context.Context, namespaces []string) error {
	if v.Mode != ModeScoped {
		return nil
	}
	seen := make(map[string]struct{}, len(namespaces))
	for _, namespace := range namespaces {
		namespace = strings.TrimSpace(namespace)
		if namespace == "" {
			return errors.New("tenancy namespace must not be blank")
		}
		if namespace == v.Installation.Key.Namespace {
			return fmt.Errorf("system namespace %q must not be configured as a Project namespace", namespace)
		}
		if _, exists := seen[namespace]; exists {
			return fmt.Errorf("tenancy namespace %q is configured more than once", namespace)
		}
		seen[namespace] = struct{}{}
		_, err := v.VerifyNamespace(ctx, namespace)
		if err != nil {
			return err
		}
	}
	return nil
}

func IsCatalogSource(template *platformv1alpha1.EnvironmentTemplate) bool {
	return template != nil && template.Annotations[CatalogSourceAnnotation] == "true"
}

// ValidateManagedTemplate proves that a runnable same-namespace Template is
// the retained local copy selected for this exact Installation and Project.
func ValidateManagedTemplate(template *platformv1alpha1.EnvironmentTemplate, installation InstallationIdentity, claim Claim) error {
	if template == nil || IsCatalogSource(template) {
		return errors.New("catalog sources are not runnable Project templates")
	}
	annotations := template.GetAnnotations()
	if annotations[InstallationNamespaceAnnotation] != installation.Key.Namespace ||
		annotations[InstallationNameAnnotation] != installation.Key.Name ||
		types.UID(annotations[InstallationUIDAnnotation]) != installation.UID ||
		annotations[ProjectNameAnnotation] != claim.ProjectName ||
		types.UID(annotations[ProjectUIDAnnotation]) != claim.ProjectUID ||
		annotations[CatalogNameAnnotation] != template.Name ||
		strings.TrimSpace(annotations[CatalogRevisionAnnotation]) == "" ||
		strings.TrimSpace(annotations[CatalogSourceUIDAnnotation]) == "" {
		return fmt.Errorf("%w: Template %s/%s is not an exact managed copy for Project %s (%s)", ErrOutOfScope, template.Namespace, template.Name, claim.ProjectName, claim.ProjectUID)
	}
	return nil
}

func permitsLifecycle(actual Lifecycle, permitted []Lifecycle) bool {
	return slices.Contains(permitted, actual)
}
