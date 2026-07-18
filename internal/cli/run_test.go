package cli

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

func TestClaimWarmEnvironment(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "warm-small-test",
			Namespace:       "default",
			ResourceVersion: "1",
			Labels:          map[string]string{warmPoolLabel: "small"},
			OwnerReferences: []metav1.OwnerReference{{Name: "small"}},
		},
		Spec:   platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
		Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady},
	}
	clients := &kubeClients{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build()}

	claimed, ok, err := claimWarmEnvironment(context.Background(), clients, "default", "small", "example")
	if err != nil {
		t.Fatalf("claimWarmEnvironment() error = %v", err)
	}
	if !ok || claimed.Name != env.Name {
		t.Fatalf("claimWarmEnvironment() = (%#v, %v), want %q", claimed, ok, env.Name)
	}
	var updated platformv1alpha1.Environment
	if err := clients.Get(context.Background(), client.ObjectKeyFromObject(env), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Spec.ProjectRef != "example" || updated.Labels[warmPoolLabel] != "" || len(updated.OwnerReferences) != 0 || updated.Annotations[claimedAnnotation] == "" {
		t.Fatalf("claimed environment = %#v", updated)
	}
}
