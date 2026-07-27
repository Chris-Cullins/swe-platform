package lifecycle

import (
	"context"
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

var ErrExecutionFenceChanged = errors.New("environment execution fence changed")
var ErrLifecycleEpochChanged = errors.New("environment lifecycle epoch changed")

// ExecutionFence is the complete backend-neutral identity of one Environment
// execution contract. Its fields are intentionally private so consumers can
// only obtain a complete fence by capturing an Environment.
type ExecutionFence struct {
	key                 types.NamespacedName
	environmentUID      types.UID
	executionGeneration int64
	lifecycleEpoch      int64
	holdPolicyRevision  int64
}

// CaptureExecutionFence captures the exact Environment identity that must
// remain current across an execution-scoped operation.
func CaptureExecutionFence(environment *platformv1alpha1.Environment) ExecutionFence {
	return ExecutionFence{
		key:                 client.ObjectKeyFromObject(environment),
		environmentUID:      environment.UID,
		executionGeneration: environment.Status.ExecutionGeneration,
		lifecycleEpoch:      environment.Status.Lifecycle.Epoch,
		holdPolicyRevision:  HoldPolicyRevision(environment),
	}
}

// Revalidate reads the Environment and rejects a change to any component of
// the captured execution fence. Callers choose an uncached reader when this is
// a post-call or current-identity proof.
func (f ExecutionFence) Revalidate(ctx context.Context, reader client.Reader) (*platformv1alpha1.Environment, error) {
	var environment platformv1alpha1.Environment
	if err := reader.Get(ctx, f.key, &environment); err != nil {
		return nil, err
	}
	if err := f.Validate(&environment); err != nil {
		return nil, err
	}
	return &environment, nil
}

// Validate rejects a change to any component of the captured execution fence
// on an Environment already read by the caller.
func (f ExecutionFence) Validate(environment *platformv1alpha1.Environment) error {
	if client.ObjectKeyFromObject(environment) != f.key || environment.UID != f.environmentUID {
		return executionFenceChanged(ErrEnvironmentIncarnationChanged)
	}
	if f.executionGeneration < 1 || environment.Status.ExecutionGeneration != f.executionGeneration {
		return executionFenceChanged(ErrExecutionGenerationChanged)
	}
	if environment.Status.Lifecycle.Epoch != f.lifecycleEpoch {
		return executionFenceChanged(ErrLifecycleEpochChanged)
	}
	if HoldPolicyRevision(environment) != f.holdPolicyRevision {
		return executionFenceChanged(ErrHoldPolicyChanged)
	}
	return nil
}

func executionFenceChanged(component error) error {
	return fmt.Errorf("%w: %w", ErrExecutionFenceChanged, component)
}

// EnvironmentUID returns the immutable Environment identity in the fence.
func (f ExecutionFence) EnvironmentUID() types.UID { return f.environmentUID }

// ExecutionGeneration returns the backend-neutral execution generation.
func (f ExecutionFence) ExecutionGeneration() int64 { return f.executionGeneration }

// LifecycleEpoch returns the lifecycle suspension epoch.
func (f ExecutionFence) LifecycleEpoch() int64 { return f.lifecycleEpoch }

// HoldPolicyRevision returns the captured explicit hold-policy revision.
func (f ExecutionFence) HoldPolicyRevision() int64 { return f.holdPolicyRevision }
