package lifecycle

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

func TestExecutionFenceRevalidateRejectsEachStaleComponent(t *testing.T) {
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "environment", Namespace: "project", UID: "env-uid"},
		Spec: platformv1alpha1.EnvironmentSpec{Lifecycle: platformv1alpha1.EnvironmentLifecycleSpec{
			Hold: &platformv1alpha1.EnvironmentHoldPolicy{Revision: 11},
		}},
		Status: platformv1alpha1.EnvironmentStatus{
			ExecutionGeneration: 7,
			Lifecycle:           platformv1alpha1.EnvironmentLifecycleStatus{Epoch: 5},
		},
	}
	fence := CaptureExecutionFence(environment)

	for _, test := range []struct {
		name   string
		want   error
		mutate func(*platformv1alpha1.Environment)
	}{
		{name: "UID", want: ErrEnvironmentIncarnationChanged, mutate: func(current *platformv1alpha1.Environment) { current.UID = "replacement-uid" }},
		{name: "execution generation", want: ErrExecutionGenerationChanged, mutate: func(current *platformv1alpha1.Environment) { current.Status.ExecutionGeneration++ }},
		{name: "lifecycle epoch", want: ErrLifecycleEpochChanged, mutate: func(current *platformv1alpha1.Environment) { current.Status.Lifecycle.Epoch++ }},
		{name: "hold revision", want: ErrHoldPolicyChanged, mutate: func(current *platformv1alpha1.Environment) { current.Spec.Lifecycle.Hold.Revision++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			current := environment.DeepCopy()
			test.mutate(current)
			scheme := runtime.NewScheme()
			if err := platformv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(current).Build()
			if _, err := fence.Revalidate(context.Background(), reader); !errors.Is(err, test.want) {
				t.Fatalf("Revalidate() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestExecutionFenceRevalidateReturnsCurrentEnvironment(t *testing.T) {
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "environment", Namespace: "project", UID: "env-uid"},
		Status:     platformv1alpha1.EnvironmentStatus{ExecutionGeneration: 1},
	}
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
	current, err := CaptureExecutionFence(environment).Revalidate(context.Background(), reader)
	if err != nil {
		t.Fatal(err)
	}
	if current.UID != environment.UID {
		t.Fatalf("revalidated UID = %q, want %q", current.UID, environment.UID)
	}
}
