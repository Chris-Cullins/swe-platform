package tenancy

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

func TestPrepareCatalogSourcesOwnershipAndIdempotence(t *testing.T) {
	i := &platformv1alpha1.Installation{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "i-1"}}
	s := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "system", Annotations: map[string]string{CatalogSourceAnnotation: "true", InstallationNamespaceAnnotation: "system", InstallationNameAnnotation: "main", CatalogNameAnnotation: "small", CatalogRevisionAnnotation: "r1"}}}
	c := fake.NewClientBuilder().WithScheme(tenancyScheme(t)).WithObjects(i, s).Build()
	if err := PrepareCatalogSources(context.Background(), c, i); err != nil {
		t.Fatal(err)
	}
	var got platformv1alpha1.EnvironmentTemplate
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(s), &got); err != nil {
		t.Fatal(err)
	}
	if got.Annotations[InstallationUIDAnnotation] != "i-1" || len(got.OwnerReferences) != 1 || got.OwnerReferences[0].UID != "i-1" {
		t.Fatalf("not exactly stamped: %#v", got.ObjectMeta)
	}
	rv := got.ResourceVersion
	if err := PrepareCatalogSources(context.Background(), c, i); err != nil {
		t.Fatal(err)
	}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(s), &got)
	if got.ResourceVersion != rv {
		t.Fatalf("idempotent call wrote object: %s -> %s", rv, got.ResourceVersion)
	}
}

func TestPrepareCatalogSourcesRefusesStaleOwnership(t *testing.T) {
	for _, kind := range []string{"annotation", "owner UID", "owner name"} {
		t.Run(kind, func(t *testing.T) {
			i := &platformv1alpha1.Installation{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "i-1"}}
			a := map[string]string{CatalogSourceAnnotation: "true", InstallationNamespaceAnnotation: "system", InstallationNameAnnotation: "main", CatalogNameAnnotation: "small", CatalogRevisionAnnotation: "r1"}
			s := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "system", Annotations: a}}
			switch kind {
			case "owner UID":
				s.OwnerReferences = []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Installation", Name: "main", UID: "old"}}
			case "owner name":
				s.OwnerReferences = []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Installation", Name: "other", UID: "other-uid"}}
			default:
				a[InstallationUIDAnnotation] = "old"
			}
			c := fake.NewClientBuilder().WithScheme(tenancyScheme(t)).WithObjects(i, s).Build()
			if err := PrepareCatalogSources(context.Background(), c, i); err == nil {
				t.Fatal("stale source accepted")
			}
		})
	}
}
