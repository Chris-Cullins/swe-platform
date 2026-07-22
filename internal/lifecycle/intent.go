package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

var ErrEnvironmentIncarnationChanged = errors.New("environment incarnation changed")
var ErrHoldPolicyChanged = errors.New("environment hold policy changed")

const activityAnnotationPrefix = "lifecycle.swe.dev/activity-"

// HoldPolicyRevision returns the current explicit hold-policy fence.
func HoldPolicyRevision(environment *platformv1alpha1.Environment) int64 {
	if environment.Spec.Lifecycle.Hold == nil {
		return 0
	}
	return environment.Spec.Lifecycle.Hold.Revision
}

// RequestWake publishes one durable ordinary wake intent. Repeating requestID
// is idempotent. The lifecycle controller decides whether policy permits it.
func RequestWake(ctx context.Context, kube client.Client, key types.NamespacedName, expectedUID types.UID, policyRevision int64, requestID string) error {
	return RequestWakeForReason(ctx, kube, key, expectedUID, policyRevision, requestID, platformv1alpha1.EnvironmentSuspensionReasonIdle)
}

// RequestWakeForReason publishes a wake authorized for one observed automatic
// or requested suspension reason. Explicit holds are never ordinary-wakeable.
func RequestWakeForReason(ctx context.Context, kube client.Client, key types.NamespacedName, expectedUID types.UID, policyRevision int64, requestID string, reason platformv1alpha1.EnvironmentSuspensionReason) error {
	if reason != platformv1alpha1.EnvironmentSuspensionReasonIdle && reason != platformv1alpha1.EnvironmentSuspensionReasonRequested {
		return fmt.Errorf("suspension reason %q is not ordinary-wakeable", reason)
	}
	return patchIntent(ctx, kube, key, expectedUID, policyRevision, func(environment *platformv1alpha1.Environment) {
		environment.Spec.Lifecycle.Wake = &platformv1alpha1.EnvironmentWakeRequest{
			EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{
				ID: requestID, EnvironmentUID: expectedUID, HoldPolicyRevision: policyRevision,
			},
			ExpectedSuspensionReason: reason,
		}
	})
}

// RequestSuspend publishes a requested execution fence without changing hold
// policy. Sequence must increase for each logical suspension request targeting
// one Environment incarnation.
func RequestSuspend(ctx context.Context, kube client.Client, key types.NamespacedName, expectedUID types.UID, policyRevision int64, requestID string, sequence int64) error {
	if sequence < 1 {
		return fmt.Errorf("suspension request sequence must be positive")
	}
	return patchIntent(ctx, kube, key, expectedUID, policyRevision, func(environment *platformv1alpha1.Environment) {
		current := environment.Spec.Lifecycle.Suspend
		if sequence <= environment.Status.Lifecycle.LastSuspendRequestSequence ||
			current != nil && (current.Sequence >= sequence || current.ID == requestID) {
			return
		}
		environment.Spec.Lifecycle.Suspend = &platformv1alpha1.EnvironmentSuspendRequest{
			EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{
				ID: requestID, EnvironmentUID: expectedUID, HoldPolicyRevision: policyRevision,
			},
			Sequence: sequence,
		}
	})
}

// RecordActivity publishes one source's latest activity intent. Activity uses
// fixed metadata slots so heartbeats are durable without advancing generation;
// only execution-contract changes make Environment status stale.
func RecordActivity(ctx context.Context, kube client.Client, key types.NamespacedName, expectedUID types.UID, policyRevision int64, source platformv1alpha1.EnvironmentActivitySource, requestID string) error {
	request := platformv1alpha1.EnvironmentActivityRequest{
		Source: source,
		EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{
			ID: requestID, EnvironmentUID: expectedUID, HoldPolicyRevision: policyRevision,
		},
	}
	if err := validateActivityRequest(request); err != nil {
		return err
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode activity request: %w", err)
	}
	return patchIntent(ctx, kube, key, expectedUID, policyRevision, func(environment *platformv1alpha1.Environment) {
		if environment.Annotations == nil {
			environment.Annotations = make(map[string]string)
		}
		environment.Annotations[activityAnnotation(source)] = string(encoded)
	})
}

// ActivityRequests returns the bounded caller-owned activity intents. Metadata
// slots supersede legacy spec slots from the first lifecycle API revision.
func ActivityRequests(environment *platformv1alpha1.Environment) []platformv1alpha1.EnvironmentActivityRequest {
	requests := make(map[platformv1alpha1.EnvironmentActivitySource]platformv1alpha1.EnvironmentActivityRequest, len(environment.Spec.Lifecycle.Activity))
	for _, request := range environment.Spec.Lifecycle.Activity {
		if validateActivityRequest(request) == nil {
			requests[request.Source] = request
		}
	}
	for _, source := range activitySources() {
		value, ok := environment.Annotations[activityAnnotation(source)]
		if !ok {
			continue
		}
		var request platformv1alpha1.EnvironmentActivityRequest
		if json.Unmarshal([]byte(value), &request) != nil || request.Source != source || validateActivityRequest(request) != nil ||
			request.EnvironmentUID != environment.UID || request.HoldPolicyRevision != HoldPolicyRevision(environment) {
			continue
		}
		requests[request.Source] = request
	}
	result := make([]platformv1alpha1.EnvironmentActivityRequest, 0, len(requests))
	for _, source := range activitySources() {
		if request, ok := requests[source]; ok {
			result = append(result, request)
		}
	}
	return result
}

func validateActivityRequest(request platformv1alpha1.EnvironmentActivityRequest) error {
	validSource := false
	for _, source := range activitySources() {
		if request.Source == source {
			validSource = true
			break
		}
	}
	if !validSource {
		return fmt.Errorf("invalid activity source %q", request.Source)
	}
	length := utf8.RuneCountInString(request.ID)
	if length < 1 || length > 128 {
		return fmt.Errorf("activity request ID length must be between 1 and 128 characters")
	}
	if request.EnvironmentUID == "" {
		return fmt.Errorf("activity Environment UID must not be empty")
	}
	if request.HoldPolicyRevision < 0 {
		return fmt.Errorf("activity hold policy revision must not be negative")
	}
	return nil
}

func activitySources() []platformv1alpha1.EnvironmentActivitySource {
	return []platformv1alpha1.EnvironmentActivitySource{
		platformv1alpha1.EnvironmentActivitySourceTerminal,
		platformv1alpha1.EnvironmentActivitySourcePortal,
		platformv1alpha1.EnvironmentActivitySourceInbox,
		platformv1alpha1.EnvironmentActivitySourceAgent,
		platformv1alpha1.EnvironmentActivitySourceRun,
	}
}

func activityAnnotation(source platformv1alpha1.EnvironmentActivitySource) string {
	return activityAnnotationPrefix + strings.ToLower(string(source))
}

func patchIntent(ctx context.Context, kube client.Client, key types.NamespacedName, expectedUID types.UID, expectedPolicyRevision int64, mutate func(*platformv1alpha1.Environment)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var environment platformv1alpha1.Environment
		if err := kube.Get(ctx, key, &environment); err != nil {
			return err
		}
		if environment.UID != expectedUID {
			return ErrEnvironmentIncarnationChanged
		}
		if HoldPolicyRevision(&environment) != expectedPolicyRevision {
			return ErrHoldPolicyChanged
		}
		before := environment.DeepCopy()
		mutate(&environment)
		if apiequality.Semantic.DeepEqual(before.Spec, environment.Spec) && apiequality.Semantic.DeepEqual(before.Annotations, environment.Annotations) {
			return nil
		}
		return kube.Patch(ctx, &environment, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	})
}
