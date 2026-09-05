package controllers

import (
	"context"
	"errors"
	"testing"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type changesSinkFunc func(context.Context, string, string, string, controlplane.CaptureChangesRequest) error

func (f changesSinkFunc) CaptureChanges(ctx context.Context, ns, name, uid string, r controlplane.CaptureChangesRequest) error {
	return f(ctx, ns, name, uid, r)
}

func TestChangesBaselineCaptureIsDurableAndNeverReplacedOnReaccept(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}}
	r := reconciler(t, &scriptedAdapter{}, run)
	calls := 0
	r.Changes = changesSinkFunc(func(_ context.Context, ns, name, uid string, request controlplane.CaptureChangesRequest) error {
		calls++
		if ns != "ns" || name != "r" || uid != "run-uid" || !request.Baseline {
			t.Fatal("incorrect identity")
		}
		return nil
	})
	if err := r.captureChanges(context.Background(), run, nil, true, false); err != nil {
		t.Fatal(err)
	}
	var persisted platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &persisted); err != nil {
		t.Fatal(err)
	}
	if !apiMeta.IsStatusConditionTrue(persisted.Status.Conditions, changesBaselineCondition) {
		t.Fatal("baseline marker missing")
	}
	for range 3 {
		if err := r.captureChanges(context.Background(), &persisted, &platformv1alpha1.Environment{Status: platformv1alpha1.EnvironmentStatus{ExecutionGeneration: 7}}, true, false); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatal("baseline replaced on resume", calls)
	}
}

func TestChangesFinalCaptureFailureBlocksClaimRelease(t *testing.T) {
	run, env := adapterRaceObjects(platformv1alpha1.EnvironmentOwnershipClaimed, platformv1alpha1.RunStateSucceeded)
	r := reconciler(t, &scriptedAdapter{}, run, env)
	r.Changes = changesSinkFunc(func(context.Context, string, string, string, controlplane.CaptureChangesRequest) error {
		return errors.New("storage unavailable")
	})
	if _, err := r.cleanupTerminal(context.Background(), run); err == nil {
		t.Fatal("capture failure did not gate cleanup")
	}
	var current platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.ClaimedBy == nil {
		t.Fatal("released claim without retaining final outcome")
	}
}
