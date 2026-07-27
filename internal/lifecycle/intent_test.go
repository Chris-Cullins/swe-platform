package lifecycle

import (
	"context"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

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
	environment.Status.ExecutionGeneration = 4
	environment.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Revision: 3}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
	key := client.ObjectKeyFromObject(environment)
	fence := CaptureExecutionFence(environment)

	for _, requestID := range []string{"terminal-1", "terminal-1", "terminal-2"} {
		if err := RecordActivity(context.Background(), kubeClient, fence, platformv1alpha1.EnvironmentActivitySourceTerminal, requestID); err != nil {
			t.Fatal(err)
		}
	}
	if err := RecordActivity(context.Background(), kubeClient, fence, platformv1alpha1.EnvironmentActivitySourcePortal, "portal-1"); err != nil {
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
	replacement := environment.DeepCopy()
	replacement.UID = types.UID("replacement")
	if err := RecordActivity(context.Background(), kubeClient, CaptureExecutionFence(replacement), platformv1alpha1.EnvironmentActivitySourceInbox, "stale"); err == nil {
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
	environment.Status.ExecutionGeneration = 4
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
	key := client.ObjectKeyFromObject(environment)

	for _, test := range []struct {
		name                string
		uid                 types.UID
		executionGeneration int64
		revision            int64
		source              platformv1alpha1.EnvironmentActivitySource
		id                  string
	}{
		{name: "unknown source", uid: environment.UID, executionGeneration: 4, source: "Other", id: "request"},
		{name: "empty ID", uid: environment.UID, executionGeneration: 4, source: platformv1alpha1.EnvironmentActivitySourceTerminal},
		{name: "oversized ID", uid: environment.UID, executionGeneration: 4, source: platformv1alpha1.EnvironmentActivitySourceTerminal, id: string(make([]byte, 129))},
		{name: "empty UID", executionGeneration: 4, source: platformv1alpha1.EnvironmentActivitySourceTerminal, id: "request"},
		{name: "zero execution generation", uid: environment.UID, source: platformv1alpha1.EnvironmentActivitySourceTerminal, id: "request"},
		{name: "negative revision", uid: environment.UID, executionGeneration: 4, revision: -1, source: platformv1alpha1.EnvironmentActivitySourceTerminal, id: "request"},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := environment.DeepCopy()
			candidate.UID = test.uid
			candidate.Status.ExecutionGeneration = test.executionGeneration
			if test.revision != 0 {
				candidate.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Revision: test.revision}
			}
			if err := RecordActivity(context.Background(), kubeClient, CaptureExecutionFence(candidate), test.source, test.id); err == nil {
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
	environment.Status.ExecutionGeneration = 1
	environment.Spec.Lifecycle.Activity = []platformv1alpha1.EnvironmentActivityRequest{{
		Source:                      platformv1alpha1.EnvironmentActivitySourceTerminal,
		EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "legacy", EnvironmentUID: environment.UID}, ExecutionGeneration: 1,
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
	if err := RequestSuspend(context.Background(), kubeClient, key, environment.UID, 7, "suspend-7", 1); err != nil {
		t.Fatal(err)
	}
	var updated platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), key, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Spec.Lifecycle.Wake == nil || updated.Spec.Lifecycle.Wake.EnvironmentUID != environment.UID || updated.Spec.Lifecycle.Wake.HoldPolicyRevision != 7 || updated.Spec.Lifecycle.Wake.ID != "wake-7" || updated.Spec.Lifecycle.Wake.ExpectedSuspensionReason != platformv1alpha1.EnvironmentSuspensionReasonIdle {
		t.Fatalf("wake = %#v", updated.Spec.Lifecycle.Wake)
	}
	if updated.Spec.Lifecycle.Suspend == nil || updated.Spec.Lifecycle.Suspend.EnvironmentUID != environment.UID || updated.Spec.Lifecycle.Suspend.HoldPolicyRevision != 7 || updated.Spec.Lifecycle.Suspend.ID != "suspend-7" || updated.Spec.Lifecycle.Suspend.Sequence != 1 {
		t.Fatalf("suspend = %#v", updated.Spec.Lifecycle.Suspend)
	}
}

func TestRequestSuspendPreservesSequenceHighWatermark(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{}
	environment.Name = "environment"
	environment.Namespace = "project"
	environment.UID = "env-uid"
	environment.Spec.Lifecycle.Suspend = &platformv1alpha1.EnvironmentSuspendRequest{
		EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "suspend-2", EnvironmentUID: environment.UID},
		Sequence:                    2,
	}
	environment.Status.Lifecycle.LastSuspendRequestSequence = 1
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(environment).Build()
	key := client.ObjectKeyFromObject(environment)

	for _, request := range []struct {
		id       string
		sequence int64
	}{
		{id: "suspend-1", sequence: 1},
		{id: "different-at-2", sequence: 2},
		{id: "suspend-2", sequence: 3},
	} {
		if err := RequestSuspend(context.Background(), kubeClient, key, environment.UID, 0, request.id, request.sequence); err != nil {
			t.Fatal(err)
		}
	}
	var unchanged platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), key, &unchanged); err != nil {
		t.Fatal(err)
	}
	if unchanged.Spec.Lifecycle.Suspend == nil || unchanged.Spec.Lifecycle.Suspend.ID != "suspend-2" || unchanged.Spec.Lifecycle.Suspend.Sequence != 2 {
		t.Fatalf("older, equal, or reused-ID request replaced high watermark: %#v", unchanged.Spec.Lifecycle.Suspend)
	}

	if err := RequestSuspend(context.Background(), kubeClient, key, environment.UID, 0, "suspend-3", 3); err != nil {
		t.Fatal(err)
	}
	var advanced platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), key, &advanced); err != nil {
		t.Fatal(err)
	}
	if advanced.Spec.Lifecycle.Suspend == nil || advanced.Spec.Lifecycle.Suspend.ID != "suspend-3" || advanced.Spec.Lifecycle.Suspend.Sequence != 3 {
		t.Fatalf("new request did not advance high watermark: %#v", advanced.Spec.Lifecycle.Suspend)
	}
	advanced.Spec.Lifecycle.Suspend = nil
	if err := kubeClient.Update(context.Background(), &advanced); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(context.Background(), key, &advanced); err != nil {
		t.Fatal(err)
	}
	advanced.Status.Lifecycle.LastSuspendRequestSequence = 3
	if err := kubeClient.Status().Update(context.Background(), &advanced); err != nil {
		t.Fatal(err)
	}
	if err := RequestSuspend(context.Background(), kubeClient, key, environment.UID, 0, "suspend-2-replay", 2); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(context.Background(), key, &advanced); err != nil {
		t.Fatal(err)
	}
	if advanced.Spec.Lifecycle.Suspend != nil {
		t.Fatalf("status high watermark allowed an older request: %#v", advanced.Spec.Lifecycle.Suspend)
	}
	if err := RequestSuspend(context.Background(), kubeClient, key, environment.UID, 0, "invalid", 0); err == nil {
		t.Fatal("RequestSuspend accepted a nonpositive sequence")
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

func TestRecordActivityAtomicallyRejectsExecutionTransition(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{}
	environment.Name = "environment"
	environment.Namespace = "project"
	environment.UID = "env-uid"
	environment.Status.ExecutionGeneration = 1
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(environment).Build()
	transitioned := false
	kubeClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Patch: func(ctx context.Context, underlying client.WithWatch, object client.Object, patch client.Patch, options ...client.PatchOption) error {
			if !transitioned {
				transitioned = true
				var current platformv1alpha1.Environment
				if err := underlying.Get(ctx, client.ObjectKeyFromObject(environment), &current); err != nil {
					return err
				}
				current.Status.ExecutionGeneration = 2
				if err := underlying.Status().Update(ctx, &current); err != nil {
					return err
				}
			}
			return underlying.Patch(ctx, object, patch, options...)
		},
	})

	err := RecordActivity(context.Background(), kubeClient, CaptureExecutionFence(environment), platformv1alpha1.EnvironmentActivitySourceTerminal, "stale-terminal")
	if !errors.Is(err, ErrExecutionGenerationChanged) {
		t.Fatalf("RecordActivity() error = %v, want execution-generation fence", err)
	}
	var retained platformv1alpha1.Environment
	if err := baseClient.Get(context.Background(), client.ObjectKeyFromObject(environment), &retained); err != nil {
		t.Fatal(err)
	}
	if retained.Status.ExecutionGeneration != 2 || len(retained.Annotations) != 0 {
		t.Fatalf("stale activity survived atomic transition: %#v", retained)
	}
}
