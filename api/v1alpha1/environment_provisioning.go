package v1alpha1

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
)

var environmentSizeResources = map[string]corev1.ResourceList{
	"tiny":   {corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2Gi")},
	"small":  {corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")},
	"medium": {corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("16Gi")},
	"large":  {corev1.ResourceCPU: resource.MustParse("16"), corev1.ResourceMemory: resource.MustParse("32Gi")},
}

func ResolveEnvironmentProvisioning(env *Environment, tmpl *EnvironmentTemplate, project *Project) *EnvironmentProvisioningSnapshot {
	backend := EffectiveEnvironmentBackend(env, tmpl)
	size := tmpl.Spec.Size
	if _, ok := environmentSizeResources[size]; !ok {
		size = "medium"
	}
	disk := resource.MustParse("40Gi")
	if tmpl.Spec.DiskSize != nil {
		disk = tmpl.Spec.DiskSize.DeepCopy()
	}
	s := &EnvironmentProvisioningSnapshot{
		Template: EnvironmentProvisioningTemplate{Name: tmpl.Name, UID: tmpl.UID, Generation: tmpl.Generation},
		Backend:  backend, Image: tmpl.Spec.Image, Size: size,
		Resources: provisioningResources(environmentSizeResources[size]), RuntimeClassName: tmpl.Spec.RuntimeClass, DiskSize: disk,
	}
	if project != nil && len(project.Spec.Repositories) == 1 {
		s.Project = &EnvironmentProvisioningProject{Name: project.Name, UID: project.UID, Generation: project.Generation, Repository: project.Spec.Repositories[0]}
	}
	return s
}

// ValidateEnvironmentProvisioningSnapshot rejects legacy, partial, or torn
// snapshots before controllers consume any nested values.
func ValidateEnvironmentProvisioningSnapshot(env *Environment, s *EnvironmentProvisioningSnapshot) error {
	if err := ValidateEnvironmentProvisioningTemplateSnapshot(env, s); err != nil {
		return err
	}
	if !s.TemplateVerified {
		return fmt.Errorf("provisioning template source is not verified")
	}
	if s.Project == nil {
		if env.Spec.ProjectRef != "" {
			return fmt.Errorf("provisioning snapshot is missing project %q", env.Spec.ProjectRef)
		}
		return nil
	}
	if env.Spec.ProjectRef == "" || s.Project.Name != env.Spec.ProjectRef || s.Project.UID == "" || s.Project.Generation < 1 || strings.TrimSpace(s.Project.Repository) == "" {
		return fmt.Errorf("provisioning snapshot has an invalid project source")
	}
	if !s.ProjectVerified {
		return fmt.Errorf("provisioning project source is not verified")
	}
	return nil
}

// ValidateEnvironmentProvisioningTemplateSnapshot validates the sole allowed
// partial state used while a verified warm snapshot is acquiring a Project.
func ValidateEnvironmentProvisioningTemplateSnapshot(env *Environment, s *EnvironmentProvisioningSnapshot) error {
	if s == nil {
		return fmt.Errorf("provisioning snapshot is missing")
	}
	if strings.TrimSpace(s.Template.Name) == "" || s.Template.Name != env.Spec.TemplateRef || s.Template.UID == "" || s.Template.Generation < 1 {
		return fmt.Errorf("provisioning snapshot has an invalid template source")
	}
	if !validProvisioningBackend(s.Backend) || env.Spec.Backend != "" && s.Backend != env.Spec.Backend || strings.TrimSpace(s.Image) == "" ||
		!validProvisioningSize(s.Size) || s.DiskSize.Sign() <= 0 || !validProvisioningResources(s.Resources) {
		return fmt.Errorf("provisioning snapshot has incomplete resolved inputs")
	}
	if s.Project != nil && (s.Project.Name == "" || s.Project.UID == "" || s.Project.Generation < 1 || strings.TrimSpace(s.Project.Repository) == "") {
		return fmt.Errorf("provisioning snapshot has an invalid project source")
	}
	if s.Project == nil && s.ProjectVerified {
		return fmt.Errorf("provisioning project verification has no project source")
	}
	return nil
}

func validProvisioningBackend(backend EnvironmentBackend) bool {
	switch backend {
	case EnvironmentBackendPod, EnvironmentBackendKubeVirt, EnvironmentBackendExternalRunner:
		return true
	default:
		return false
	}
}

func validProvisioningSize(size string) bool {
	_, ok := environmentSizeResources[size]
	return ok
}

func validProvisioningResources(resources map[string]resource.Quantity) bool {
	if len(resources) != 2 {
		return false
	}
	cpu, hasCPU := resources[string(corev1.ResourceCPU)]
	memory, hasMemory := resources[string(corev1.ResourceMemory)]
	return hasCPU && hasMemory && cpu.Sign() > 0 && memory.Sign() > 0
}

func provisioningResources(resources corev1.ResourceList) map[string]resource.Quantity {
	result := make(map[string]resource.Quantity, len(resources))
	for name, quantity := range resources {
		result[string(name)] = quantity.DeepCopy()
	}
	return result
}

func ProvisioningSnapshotsEqual(a, b *EnvironmentProvisioningSnapshot) bool {
	return equality.Semantic.DeepEqual(a, b)
}

func ProvisioningSnapshotsEqualIgnoringVerification(a, b *EnvironmentProvisioningSnapshot) bool {
	if a == nil || b == nil {
		return a == b
	}
	left, right := a.DeepCopy(), b.DeepCopy()
	left.TemplateVerified, left.ProjectVerified = false, false
	right.TemplateVerified, right.ProjectVerified = false, false
	return ProvisioningSnapshotsEqual(left, right)
}

// ProvisioningSnapshotCurrentTemplate deliberately compares only provisioning
// inputs, not Template generation or policy such as idle timeout/warm pool.
func ProvisioningSnapshotCurrentTemplate(s *EnvironmentProvisioningSnapshot, tmpl *EnvironmentTemplate) bool {
	if s == nil || !s.TemplateVerified || s.Project != nil || s.Template.Name != tmpl.Name || s.Template.UID != tmpl.UID {
		return false
	}
	env := &Environment{Spec: EnvironmentSpec{TemplateRef: tmpl.Name, Backend: s.Backend}}
	if err := ValidateEnvironmentProvisioningTemplateSnapshot(env, s); err != nil {
		return false
	}
	projection := ResolveEnvironmentProvisioning(env, tmpl, nil)
	projection.Template.Generation = s.Template.Generation
	return ProvisioningSnapshotsEqualIgnoringVerification(s, projection)
}
