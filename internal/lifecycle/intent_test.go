package lifecycle

import (
	"context"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

func TestRecordActivityIsSourceBoundedAndFenced(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{}
	environment.Name = "environment"
	environment.Namespace = "project"
	environment.UID = "env-uid"
	environment.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Revision: 3}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
	key := client.ObjectKeyFromObject(environment)

	for _, requestID := range []string{"terminal-1", "terminal-1", "terminal-2"} {
		if err := RecordActivity(context.Background(), kubeClient, key, environment.UID, 3, platformv1alpha1.EnvironmentActivitySourceTerminal, requestID); err != nil {
			t.Fatal(err)
		}
	}
	if err := RecordActivity(context.Background(), kubeClient, key, environment.UID, 3, platformv1alpha1.EnvironmentActivitySourcePortal, "portal-1"); err != nil {
		t.Fatal(err)
	}
	var updated platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), key, &updated); err != nil {
		t.Fatal(err)
	}
	requests := ActivityRequests(&updated)
	if len(requests) != 2 || requests[0].ID != "terminal-2" || requests[1].ID != "portal-1" {
		t.Fatalf("bounded activity slots = %#v", requests)
	}
	if updated.Generation != environment.Generation || len(updated.Spec.Lifecycle.Activity) != 0 {
		t.Fatalf("activity changed execution spec: generation=%d spec=%#v", updated.Generation, updated.Spec.Lifecycle)
	}
	if err := RecordActivity(context.Background(), kubeClient, key, types.UID("replacement"), 3, platformv1alpha1.EnvironmentActivitySourceInbox, "stale"); err == nil {
		t.Fatal("replacement UID activity was accepted")
	}
}

func TestRecordActivityRejectsMalformedRequests(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{}
	environment.Name = "environment"
	environment.Namespace = "project"
	environment.UID = "env-uid"
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
	key := client.ObjectKeyFromObject(environment)

	for _, test := range []struct {
		name     string
		uid      types.UID
		revision int64
		source   platformv1alpha1.EnvironmentActivitySource
		id       string
	}{
		{name: "unknown source", uid: environment.UID, source: "Other", id: "request"},
		{name: "empty ID", uid: environment.UID, source: platformv1alpha1.EnvironmentActivitySourceTerminal},
		{name: "oversized ID", uid: environment.UID, source: platformv1alpha1.EnvironmentActivitySourceTerminal, id: string(make([]byte, 129))},
		{name: "empty UID", source: platformv1alpha1.EnvironmentActivitySourceTerminal, id: "request"},
		{name: "negative revision", uid: environment.UID, revision: -1, source: platformv1alpha1.EnvironmentActivitySourceTerminal, id: "request"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := RecordActivity(context.Background(), kubeClient, key, test.uid, test.revision, test.source, test.id); err == nil {
				t.Fatal("RecordActivity() accepted malformed request")
			}
		})
	}
	var retained platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), key, &retained); err != nil {
		t.Fatal(err)
	}
	if len(retained.Annotations) != 0 {
		t.Fatalf("malformed activity wrote annotations: %#v", retained.Annotations)
	}
}

func TestMalformedAnnotationDoesNotShadowLegacyActivity(t *testing.T) {
	environment := &platformv1alpha1.Environment{}
	environment.UID = "env-uid"
	environment.Spec.Lifecycle.Activity = []platformv1alpha1.EnvironmentActivityRequest{{
		Source:                      platformv1alpha1.EnvironmentActivitySourceTerminal,
		EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "legacy", EnvironmentUID: environment.UID},
	}}
	environment.Annotations = map[string]string{activityAnnotation(platformv1alpha1.EnvironmentActivitySourceTerminal): `{"source":"Terminal","id":"","environmentUID":"env-uid","holdPolicyRevision":0}`}

	requests := ActivityRequests(environment)
	if len(requests) != 1 || requests[0].ID != "legacy" {
		t.Fatalf("activity requests = %#v, want valid legacy fallback", requests)
	}
}

func TestWakeAndSuspendRequestsCarryExactFences(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{}
	environment.Name = "environment"
	environment.Namespace = "project"
	environment.UID = "env-uid"
	environment.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Revision: 7}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
	key := client.ObjectKeyFromObject(environment)

	if err := RequestWake(context.Background(), kubeClient, key, environment.UID, 7, "wake-7"); err != nil {
		t.Fatal(err)
	}
	if err := RequestSuspend(context.Background(), kubeClient, key, environment.UID, 7, "suspend-7"); err != nil {
		t.Fatal(err)
	}
	var updated platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), key, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Spec.Lifecycle.Wake == nil || updated.Spec.Lifecycle.Wake.EnvironmentUID != environment.UID || updated.Spec.Lifecycle.Wake.HoldPolicyRevision != 7 || updated.Spec.Lifecycle.Wake.ID != "wake-7" || updated.Spec.Lifecycle.Wake.ExpectedSuspensionReason != platformv1alpha1.EnvironmentSuspensionReasonIdle {
		t.Fatalf("wake = %#v", updated.Spec.Lifecycle.Wake)
	}
	if updated.Spec.Lifecycle.Suspend == nil || updated.Spec.Lifecycle.Suspend.EnvironmentUID != environment.UID || updated.Spec.Lifecycle.Suspend.HoldPolicyRevision != 7 || updated.Spec.Lifecycle.Suspend.ID != "suspend-7" {
		t.Fatalf("suspend = %#v", updated.Spec.Lifecycle.Suspend)
	}
}

func TestRequestedWakeCarriesExactReasonAndRejectsHold(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{}
	environment.Name = "environment"
	environment.Namespace = "project"
	environment.UID = "env-uid"
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
	key := client.ObjectKeyFromObject(environment)

	if err := RequestWakeForReason(context.Background(), kubeClient, key, environment.UID, 0, "run-wake", platformv1alpha1.EnvironmentSuspensionReasonRequested); err != nil {
		t.Fatal(err)
	}
	var updated platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), key, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Spec.Lifecycle.Wake == nil || updated.Spec.Lifecycle.Wake.ExpectedSuspensionReason != platformv1alpha1.EnvironmentSuspensionReasonRequested {
		t.Fatalf("requested wake = %#v", updated.Spec.Lifecycle.Wake)
	}
	if err := RequestWakeForReason(context.Background(), kubeClient, key, environment.UID, 0, "hold-wake", platformv1alpha1.EnvironmentSuspensionReasonHold); err == nil {
		t.Fatal("ordinary wake accepted explicit Hold reason")
	}
}

func TestStalePolicyPublisherCannotOverwriteCurrentIntent(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{}
	environment.Name = "environment"
	environment.Namespace = "project"
	environment.UID = "env-uid"
	environment.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Revision: 2}
	environment.Spec.Lifecycle.Wake = &platformv1alpha1.EnvironmentWakeRequest{EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "current", EnvironmentUID: environment.UID, HoldPolicyRevision: 2}}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
	key := client.ObjectKeyFromObject(environment)

	err := RequestWake(context.Background(), kubeClient, key, environment.UID, 1, "stale")
	if !errors.Is(err, ErrHoldPolicyChanged) {
		t.Fatalf("stale publisher error = %v", err)
	}
	var retained platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), key, &retained); err != nil {
		t.Fatal(err)
	}
	if retained.Spec.Lifecycle.Wake == nil || retained.Spec.Lifecycle.Wake.ID != "current" {
		t.Fatalf("stale publisher replaced current wake: %#v", retained.Spec.Lifecycle.Wake)
	}
}
