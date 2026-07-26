package tenancy

import (
	"context"
	"fmt"
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

// PrepareCatalogSources binds chart-rendered catalog Templates to the exact
// live Installation UID. Helm cannot render that server-assigned identity.
func PrepareCatalogSources(ctx context.Context, kube client.Client, installation *platformv1alpha1.Installation) error {
	var templates platformv1alpha1.EnvironmentTemplateList
	if err := kube.List(ctx, &templates, client.InNamespace(installation.Namespace)); err != nil {
		return fmt.Errorf("list installation catalog sources: %w", err)
	}
	for i := range templates.Items {
		template := &templates.Items[i]
		annotations := template.GetAnnotations()
		if annotations[CatalogSourceAnnotation] != "true" ||
			annotations[InstallationNamespaceAnnotation] != installation.Namespace ||
			annotations[InstallationNameAnnotation] != installation.Name {
			continue
		}
		if annotations[CatalogNameAnnotation] == "" || annotations[CatalogRevisionAnnotation] == "" {
			return fmt.Errorf("catalog source %s/%s has incomplete name or revision metadata", template.Namespace, template.Name)
		}
		if value := annotations[InstallationUIDAnnotation]; value != "" && value != string(installation.UID) {
			return fmt.Errorf("catalog source %s/%s belongs to a different Installation UID %s", template.Namespace, template.Name, value)
		}
		before := template.DeepCopy()
		if template.Annotations == nil {
			template.Annotations = make(map[string]string)
		}
		template.Annotations[InstallationUIDAnnotation] = string(installation.UID)
		ownerFound := false
		for _, owner := range template.OwnerReferences {
			if owner.APIVersion != platformv1alpha1.GroupVersion.String() || owner.Kind != "Installation" {
				continue
			}
			if ownerFound || owner.Name != installation.Name || owner.UID != installation.UID {
				return fmt.Errorf("catalog source %s/%s has a conflicting Installation owner %s (%s)", template.Namespace, template.Name, owner.Name, owner.UID)
			}
			ownerFound = true
		}
		if !ownerFound {
			template.OwnerReferences = append(template.OwnerReferences, metav1.OwnerReference{
				APIVersion: platformv1alpha1.GroupVersion.String(),
				Kind:       "Installation",
				Name:       installation.Name,
				UID:        installation.UID,
			})
		}
		if reflect.DeepEqual(before.Annotations, template.Annotations) && reflect.DeepEqual(before.OwnerReferences, template.OwnerReferences) {
			continue
		}
		if err := kube.Patch(ctx, template, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return fmt.Errorf("bind catalog source %s/%s to Installation: %w", template.Namespace, template.Name, err)
		}
	}
	return nil
}
