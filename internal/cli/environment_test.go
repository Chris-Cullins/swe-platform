package cli

import (
	"context"
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

func TestSetEnvironmentHoldUsesMonotonicIdempotentPolicy(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{}
	environment.Name = "shared"
	environment.Namespace = "ns"
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
	key := types.NamespacedName{Namespace: environment.Namespace, Name: environment.Name}

	for _, step := range []struct {
		enabled  bool
		revision int64
	}{
		{enabled: true, revision: 1},
		{enabled: true, revision: 1},
		{enabled: false, revision: 2},
		{enabled: false, revision: 2},
		{enabled: true, revision: 3},
	} {
		revision, err := setEnvironmentHold(context.Background(), kube, key, step.enabled)
		if err != nil || revision != step.revision {
			t.Fatalf("setEnvironmentHold(%t) = revision %d, error %v; want %d", step.enabled, revision, err, step.revision)
		}
		var current platformv1alpha1.Environment
		if err := kube.Get(context.Background(), key, &current); err != nil {
			t.Fatal(err)
		}
		if current.Spec.Paused || current.Spec.Lifecycle.Hold == nil || current.Spec.Lifecycle.Hold.Enabled != step.enabled || current.Spec.Lifecycle.Hold.Revision != step.revision {
			t.Fatalf("hold policy after enabled=%t: spec=%#v", step.enabled, current.Spec)
		}
	}
}

func TestSetEnvironmentHoldRetriesConflictAndPreservesConcurrentLifecycleIntents(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{}
	environment.Name = "shared"
	environment.Namespace = "ns"
	environment.UID = "env-uid"
	environment.Spec.Lifecycle.Wake = &platformv1alpha1.EnvironmentWakeRequest{EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "wake-1", EnvironmentUID: environment.UID}}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
	key := types.NamespacedName{Namespace: environment.Namespace, Name: environment.Name}
	patches := 0
	kube := interceptor.NewClient(base, interceptor.Funcs{Patch: func(ctx context.Context, underlying client.WithWatch, object client.Object, patch client.Patch, options ...client.PatchOption) error {
		patches++
		if patches == 1 {
			var concurrent platformv1alpha1.Environment
			if err := underlying.Get(ctx, key, &concurrent); err != nil {
				return err
			}
			concurrent.Spec.Lifecycle.Suspend = &platformv1alpha1.EnvironmentLifecycleRequest{ID: "suspend-concurrent", EnvironmentUID: concurrent.UID}
			if err := underlying.Update(ctx, &concurrent); err != nil {
				return err
			}
			return apierrors.NewConflict(schema.GroupResource{Group: platformv1alpha1.GroupVersion.Group, Resource: "environments"}, object.GetName(), errors.New("simulated hold conflict"))
		}
		return underlying.Patch(ctx, object, patch, options...)
	}})

	revision, err := setEnvironmentHold(context.Background(), kube, key, true)
	if err != nil || revision != 1 || patches != 2 {
		t.Fatalf("conflicting hold = revision %d, patches %d, error %v", revision, patches, err)
	}
	var current platformv1alpha1.Environment
	if err := base.Get(context.Background(), key, &current); err != nil {
		t.Fatal(err)
	}
	if current.Spec.Lifecycle.Hold == nil || !current.Spec.Lifecycle.Hold.Enabled || current.Spec.Lifecycle.Hold.Revision != 1 ||
		current.Spec.Lifecycle.Wake == nil || current.Spec.Lifecycle.Wake.ID != "wake-1" ||
		current.Spec.Lifecycle.Suspend == nil || current.Spec.Lifecycle.Suspend.ID != "suspend-concurrent" {
		t.Fatalf("concurrent lifecycle intent was lost: %#v", current.Spec.Lifecycle)
	}
}

func TestSetEnvironmentHoldFailsClosedDuringLegacyPauseMigration(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "release", true: "hold"}[enabled], func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := platformv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			environment := &platformv1alpha1.Environment{}
			environment.Name = "shared"
			environment.Namespace = "ns"
			environment.Spec.Paused = true
			kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
			key := types.NamespacedName{Namespace: environment.Namespace, Name: environment.Name}

			revision, err := setEnvironmentHold(context.Background(), kube, key, enabled)
			if err == nil || revision != 0 {
				t.Fatalf("legacy pause = revision %d, error %v", revision, err)
			}
			var current platformv1alpha1.Environment
			if err := kube.Get(context.Background(), key, &current); err != nil {
				t.Fatal(err)
			}
			if !current.Spec.Paused || current.Spec.Lifecycle.Hold != nil {
				t.Fatalf("legacy spec was changed by CLI: %#v", current.Spec)
			}
		})
	}
}

func TestRootIncludesEnvironmentHoldCommands(t *testing.T) {
	root := NewRootCommand()
	for _, args := range [][]string{{"environment", "hold"}, {"environment", "release"}} {
		command, _, err := root.Find(args)
		if err != nil || command == root {
			t.Fatalf("root.Find(%v) = command %q, error %v", args, command.Name(), err)
		}
	}
}
