package controllers

import (
	"context"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// reconcileLifecycleIntent is the sole owner of lifecycle transitions. Other
// components publish fenced intents in spec or bounded metadata slots; this
// method consumes each valid request once and decides whether execution is suspended.
func (r *EnvironmentReconciler) reconcileLifecycleIntent(ctx context.Context, env *platformv1alpha1.Environment) (bool, error) {
	activityRequests := lifecycle.ActivityRequests(env)
	if env.Spec.Lifecycle.Hold == nil && env.Spec.Lifecycle.Wake == nil && env.Spec.Lifecycle.Suspend == nil && len(activityRequests) == 0 &&
		!env.Status.Lifecycle.Suspended && env.Status.Lifecycle.Epoch == 0 && env.Status.Lifecycle.ObservedHoldPolicyRevision == 0 &&
		env.Status.Lifecycle.LastWakeRequestID == "" && env.Status.Lifecycle.LastSuspendRequestID == "" && env.Status.Lifecycle.LastSuspendRequestSequence == 0 &&
		env.Status.Lifecycle.PendingSuspendRequestID == "" && len(env.Status.Lifecycle.ActivityReceipts) == 0 {
		return false, nil
	}
	policyRevision := int64(0)
	held := false
	if env.Spec.Lifecycle.Hold != nil {
		policyRevision = env.Spec.Lifecycle.Hold.Revision
		held = env.Spec.Lifecycle.Hold.Enabled
	}
	valid := func(request *platformv1alpha1.EnvironmentLifecycleRequest) bool {
		return request != nil && request.EnvironmentUID == env.UID && request.HoldPolicyRevision == policyRevision
	}

	before := env.Status.DeepCopy()
	wasSuspended := env.Status.Lifecycle.Suspended
	previousReason := env.Status.Lifecycle.SuspensionReason
	lifecycleStatus := &env.Status.Lifecycle
	lifecycleStatus.ObservedHoldPolicyRevision = policyRevision
	now := metav1.NewTime(r.now())
	for i := range activityRequests {
		request := &activityRequests[i]
		if !valid(&request.EnvironmentLifecycleRequest) || request.ExecutionGeneration != env.Status.ExecutionGeneration ||
			activityReceipt(lifecycleStatus.ActivityReceipts, request.Source, request.ExecutionGeneration) == request.ID {
			continue
		}
		setActivityReceipt(&lifecycleStatus.ActivityReceipts, request.Source, request.ExecutionGeneration, request.ID)
		if env.Status.LastActiveAt == nil || now.After(env.Status.LastActiveAt.Time) {
			env.Status.LastActiveAt = &now
		}
	}

	suspend := env.Spec.Lifecycle.Suspend
	suspendValid := suspend != nil && valid(&suspend.EnvironmentLifecycleRequest)
	suspendInFlight := lifecycleStatus.PendingSuspendRequestID != ""
	suspendNew := suspendValid && !suspendInFlight && suspend.Sequence > lifecycleStatus.LastSuspendRequestSequence && suspend.ID != lifecycleStatus.LastSuspendRequestID
	if suspendNew {
		lifecycleStatus.LastSuspendRequestID = suspend.ID
		lifecycleStatus.LastSuspendRequestSequence = suspend.Sequence
		lifecycleStatus.PendingSuspendRequestID = suspend.ID
	}
	wake := env.Spec.Lifecycle.Wake
	wakeNew := wake != nil && valid(&wake.EnvironmentLifecycleRequest) && wake.ID != lifecycleStatus.LastWakeRequestID
	wakeSuspensionReason := lifecycleStatus.SuspensionReason
	switch {
	case held:
		wakeSuspensionReason = platformv1alpha1.EnvironmentSuspensionReasonHold
	case suspendNew || suspendInFlight:
		wakeSuspensionReason = platformv1alpha1.EnvironmentSuspensionReasonRequested
	}
	wakeReasonMatches := false
	if wake != nil {
		expectedReason := wake.ExpectedSuspensionReason
		if expectedReason == "" {
			expectedReason = platformv1alpha1.EnvironmentSuspensionReasonIdle
		}
		wakeReasonMatches = expectedReason == wakeSuspensionReason
	}
	backendFencePending := lifecycleStatus.Suspended && env.Status.Phase != platformv1alpha1.EnvironmentPhasePaused
	consumeWake := wakeNew && (held || !wakeReasonMatches || !suspendNew && !suspendInFlight && !backendFencePending)
	if consumeWake {
		lifecycleStatus.LastWakeRequestID = wake.ID
	}

	suspended := lifecycleStatus.Suspended
	reason := lifecycleStatus.SuspensionReason
	requestID := lifecycleStatus.SuspensionRequestID
	switch {
	case held:
		suspended = true
		reason = platformv1alpha1.EnvironmentSuspensionReasonHold
		requestID = ""
	case suspendNew || suspendInFlight:
		suspended = true
		reason = platformv1alpha1.EnvironmentSuspensionReasonRequested
		requestID = lifecycleStatus.PendingSuspendRequestID
	case lifecycleStatus.Suspended && lifecycleStatus.SuspensionReason == platformv1alpha1.EnvironmentSuspensionReasonHold && !backendFencePending:
		// Disabling a hold at a newer policy revision is itself the authorized
		// release; no ordinary stale wake is required. The backend fence must
		// complete first so release cannot reuse the previous execution domain.
		suspended = false
		reason = ""
		requestID = ""
	case consumeWake && wakeReasonMatches && lifecycleStatus.SuspensionReason != platformv1alpha1.EnvironmentSuspensionReasonHold:
		suspended = false
		reason = ""
		requestID = ""
	}
	if suspended && !lifecycleStatus.Suspended {
		lifecycleStatus.Epoch++
	}
	lifecycleStatus.Suspended = suspended
	lifecycleStatus.SuspensionReason = reason
	lifecycleStatus.SuspensionRequestID = requestID

	if apiequality.Semantic.DeepEqual(*before, env.Status) {
		return false, nil
	}
	if err := r.Status().Update(ctx, env); err != nil {
		return false, err
	}
	if !wasSuspended && lifecycleStatus.Suspended {
		r.Metrics.observeLifecycle("suspend", lifecycleStatus.SuspensionReason)
	} else if wasSuspended && !lifecycleStatus.Suspended {
		r.Metrics.observeLifecycle("resume", previousReason)
	}
	return true, nil
}

func validLifecycleRequest(env *platformv1alpha1.Environment, request *platformv1alpha1.EnvironmentLifecycleRequest) bool {
	policyRevision := int64(0)
	if env.Spec.Lifecycle.Hold != nil {
		policyRevision = env.Spec.Lifecycle.Hold.Revision
	}
	return request.EnvironmentUID == env.UID && request.HoldPolicyRevision == policyRevision
}

func activityReceipt(receipts []platformv1alpha1.EnvironmentActivityReceipt, source platformv1alpha1.EnvironmentActivitySource, executionGeneration int64) string {
	for i := range receipts {
		if receipts[i].Source == source && receipts[i].ExecutionGeneration == executionGeneration {
			return receipts[i].RequestID
		}
	}
	return ""
}

func setActivityReceipt(receipts *[]platformv1alpha1.EnvironmentActivityReceipt, source platformv1alpha1.EnvironmentActivitySource, executionGeneration int64, requestID string) {
	for i := range *receipts {
		if (*receipts)[i].Source == source {
			(*receipts)[i].RequestID = requestID
			(*receipts)[i].ExecutionGeneration = executionGeneration
			return
		}
	}
	*receipts = append(*receipts, platformv1alpha1.EnvironmentActivityReceipt{Source: source, RequestID: requestID, ExecutionGeneration: executionGeneration})
}
