package controllers

import (
	"context"
	"fmt"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type RunChangesSink interface {
	CaptureChanges(context.Context, string, string, string, controlplane.CaptureChangesRequest) error
}

const changesBaselineCondition = "ChangesBaselineCaptured"
const changesFinalCondition = "ChangesFinalCaptured"

func (r *RunReconciler) captureChanges(ctx context.Context, run *platformv1alpha1.Run, env *platformv1alpha1.Environment, baseline, final bool) error {
	if r.Changes == nil {
		return nil
	}
	condition := ""
	if baseline {
		condition = changesBaselineCondition
	}
	if final {
		condition = changesFinalCondition
	}
	if condition != "" && apiMeta.IsStatusConditionTrue(run.Status.Conditions, condition) {
		return nil
	}
	claim, err := r.Scope.Revalidate(ctx, run.Namespace, tenancy.LifecycleActive, tenancy.LifecycleFencing, tenancy.LifecycleFenced)
	if err != nil {
		return err
	}
	if claim.Lifecycle != tenancy.LifecycleActive {
		if !final || baseline || !terminalRunState(run.Status.State) || (claim.Lifecycle != tenancy.LifecycleFenced && claim.Operation != tenancy.OperationOffboarding) {
			return fmt.Errorf("Changes capture requires active tenancy")
		}
		// Offboarding withdraws capture authority. Preserve prior non-final bytes,
		// but do not let an intentionally denied observation block safety cleanup.
		apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: changesFinalCondition, Status: metav1.ConditionTrue, Reason: "OffboardingUnavailable", Message: "final capture unavailable after tenancy fencing; last observation remains retained, not final", ObservedGeneration: run.Generation})
		return r.Status().Update(ctx, run)
	}
	request := controlplane.CaptureChangesRequest{Baseline: baseline, Final: final}
	if run.Status.EnvironmentRef != nil {
		request.EnvironmentUID = string(run.Status.EnvironmentRef.UID)
	}
	if env != nil {
		fence := lifecycle.CaptureExecutionFence(env)
		request.EnvironmentUID = string(fence.EnvironmentUID())
		request.ExecutionGeneration = fence.ExecutionGeneration()
		request.LifecycleEpoch = fence.LifecycleEpoch()
		request.HoldPolicyRevision = fence.HoldPolicyRevision()
	}
	if err := r.Changes.CaptureChanges(ctx, run.Namespace, run.Name, string(run.UID), request); err != nil {
		return err
	}
	if condition != "" {
		apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: condition, Status: metav1.ConditionTrue, Reason: "Retained", Message: "bounded changes capture outcome retained for review", ObservedGeneration: run.Generation})
		return r.Status().Update(ctx, run)
	}
	return nil
}
