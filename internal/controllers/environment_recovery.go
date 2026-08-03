package controllers

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"
	"time"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// reconcilePodRecoveryMigration losslessly moves deprecated flat recovery state
// into its nested representation, or clears stale flat state when nested state
// is already authoritative.
func (r *EnvironmentReconciler) reconcilePodRecoveryMigration(ctx context.Context, env *platformv1alpha1.Environment) (ctrl.Result, bool, error) {
	if legacyRecoveryMeaningful(&env.Status) {
		executionGeneration := int64(0)
		if !nestedRecoveryMeaningful(env.Status.Recovery) {
			var pod corev1.Pod
			err := r.apiReader().Get(ctx, client.ObjectKey{Namespace: env.Namespace, Name: envPodName(env)}, &pod)
			if err == nil {
				podGeneration, valid := podExecutionGeneration(&pod)
				if pod.UID == env.Status.PodRecoveryUID &&
					exactControllerOwner(&pod, platformv1alpha1.GroupVersion.String(), "Environment", env.Name, env.UID) &&
					valid && podGeneration == env.Status.ExecutionGeneration {
					executionGeneration = podGeneration
				}
			} else if !errors.IsNotFound(err) {
				return ctrl.Result{}, true, err
			}
		}
		if err := r.updatePodRecoveryStatus(ctx, env, func(current *platformv1alpha1.Environment) {
			if !nestedRecoveryMeaningful(current.Status.Recovery) {
				current.Status.Recovery = platformv1alpha1.EnvironmentRecoveryStatus{
					Attempts: current.Status.PodRecoveryAttempts, Exhausted: current.Status.PodRecoveryExhausted,
					ExecutionGeneration: executionGeneration, NextAttemptAt: current.Status.PodRecoveryNextAttemptAt,
				}
			}
			current.Status.PodRecoveryAttempts = 0
			current.Status.PodRecoveryExhausted = false
			current.Status.PodRecoveryUID = ""
			current.Status.PodRecoveryNextAttemptAt = nil
		}); err != nil {
			if stderrors.Is(err, errPodRecoveryChanged) {
				return ctrl.Result{Requeue: true}, true, nil
			}
			return ctrl.Result{}, true, err
		}
		// Always observe the nested representation on a separate reconcile before
		// enforcing its deadline or exhaustion latch. Migration is not a recovery
		// transition and therefore emits no attempt/exhausted metric.
		return ctrl.Result{Requeue: true}, true, nil
	}
	return ctrl.Result{}, false, nil
}

// reconcilePendingPodRecovery advances persisted nested recovery before
// ensurePod can create a replacement. This keeps backoff and exhaustion
// effective even when a terminal Pod disappears without the controller
// deleting it. Flat recovery state is migrated by an earlier reconcile phase.
func (r *EnvironmentReconciler) reconcilePendingPodRecovery(ctx context.Context, env *platformv1alpha1.Environment) (ctrl.Result, bool, error) {
	recovery, _ := effectivePodRecovery(&env.Status)
	ready := apimeta.FindStatusCondition(env.Status.Conditions, platformv1alpha1.EnvironmentConditionReady)
	if recovery.Exhausted {
		message := fmt.Sprintf("automatic recovery is exhausted after %d terminal pod replacements", recovery.Attempts)
		if ready == nil || ready.ObservedGeneration != env.Generation || ready.Status != metav1.ConditionFalse || ready.Reason != "PodRecoveryExhausted" {
			if err := r.setEnvironmentStatus(ctx, env, platformv1alpha1.EnvironmentPhaseFailed, env.Status.PodName, "", "PodRecoveryExhausted", message); err != nil {
				return ctrl.Result{}, true, err
			}
		}
		return ctrl.Result{}, true, nil
	}
	nextAttemptAt := recovery.NextAttemptAt
	if nextAttemptAt == nil {
		return ctrl.Result{}, false, nil
	}
	now := r.now()
	if now.Before(nextAttemptAt.Time) {
		if ready == nil || ready.ObservedGeneration != env.Generation || ready.Status != metav1.ConditionFalse || ready.Reason != "PodRecoveryPending" {
			message := fmt.Sprintf("terminal pod recovery attempt %d of %d is scheduled for %s", recovery.Attempts+1, podRecoveryLimit, nextAttemptAt.Time.UTC().Format(time.RFC3339))
			if err := r.setEnvironmentStatus(ctx, env, podRecoveryPhase(env, nil), "", "", "PodRecoveryPending", message); err != nil {
				return ctrl.Result{}, true, err
			}
		}
		return ctrl.Result{RequeueAfter: nextAttemptAt.Sub(now)}, true, nil
	}

	attempts := recovery.Attempts + 1
	message := fmt.Sprintf("replacing terminal environment pod (recovery attempt %d of %d)", attempts, podRecoveryLimit)
	if err := r.updatePodRecoveryStatus(ctx, env, func(current *platformv1alpha1.Environment) {
		applyEnvironmentStatus(current, podRecoveryPhase(current, nil), "", "", "PodRecovering", message, env.Status.LastActiveAt)
		current.Status.Recovery.Attempts = attempts
		current.Status.Recovery.ExecutionGeneration = recovery.ExecutionGeneration
		current.Status.Recovery.NextAttemptAt = nil
		clearChildOwnershipCollision(current)
	}); err != nil {
		if stderrors.Is(err, errPodRecoveryChanged) {
			return ctrl.Result{Requeue: true}, true, nil
		}
		return ctrl.Result{}, true, err
	}
	r.Metrics.observePodRecovery("attempt")
	// Reconcile again before deleting or creating so the persisted attempt marker
	// is always observed, including across a concurrent generation change.
	return ctrl.Result{Requeue: true}, true, nil
}

// reconcileTerminalPod replaces an owned terminal Pod using a persisted,
// bounded retry budget. The backend-neutral execution generation deduplicates
// accounting for the exact failed execution, while deleteObservedChild fences
// deletion to the exact privately observed Pod incarnation.
func (r *EnvironmentReconciler) reconcileTerminalPod(ctx context.Context, env *platformv1alpha1.Environment, pod *corev1.Pod) (ctrl.Result, error) {
	executionGeneration, ok := podExecutionGeneration(pod)
	if !ok {
		return ctrl.Result{}, fmt.Errorf("terminal environment pod has invalid execution generation")
	}
	if executionGeneration != env.Status.ExecutionGeneration {
		return ctrl.Result{Requeue: true}, nil
	}
	now := r.now()
	recovery, _ := effectivePodRecovery(&env.Status)
	attempts := recovery.Attempts
	recoveryGeneration := recovery.ExecutionGeneration
	nextAttemptAt := recovery.NextAttemptAt

	if recoveryGeneration != executionGeneration {
		if attempts >= podRecoveryLimit {
			message := fmt.Sprintf("environment pod %s after %d recovery attempts; automatic recovery is exhausted", strings.ToLower(string(pod.Status.Phase)), attempts)
			if err := r.updatePodRecoveryStatus(ctx, env, func(current *platformv1alpha1.Environment) {
				applyEnvironmentStatus(current, platformv1alpha1.EnvironmentPhaseFailed, pod.Name, "", "PodRecoveryExhausted", message, env.Status.LastActiveAt)
				current.Status.Recovery.Exhausted = true
				current.Status.Recovery.ExecutionGeneration = executionGeneration
				clearChildOwnershipCollision(current)
			}); err != nil {
				if stderrors.Is(err, errPodRecoveryChanged) {
					return ctrl.Result{Requeue: true}, nil
				}
				return ctrl.Result{}, err
			}
			r.Metrics.observePodRecovery("exhausted")
			log.FromContext(ctx).Info("environment pod recovery exhausted", "environment", env.Name, "pod", pod.Name, "attempts", attempts)
			return ctrl.Result{}, nil
		}
		next := metav1.NewTime(now.Add(podRecoveryBackoff(attempts)))
		message := fmt.Sprintf("environment pod %s; recovery attempt %d of %d is scheduled for %s", strings.ToLower(string(pod.Status.Phase)), attempts+1, podRecoveryLimit, next.Time.UTC().Format(time.RFC3339))
		if err := r.updatePodRecoveryStatus(ctx, env, func(current *platformv1alpha1.Environment) {
			applyEnvironmentStatus(current, podRecoveryPhase(current, pod), "", "", "PodRecoveryPending", message, env.Status.LastActiveAt)
			current.Status.Recovery.Attempts = attempts
			current.Status.Recovery.Exhausted = false
			current.Status.Recovery.ExecutionGeneration = executionGeneration
			current.Status.Recovery.NextAttemptAt = &next
			clearChildOwnershipCollision(current)
		}); err != nil {
			if stderrors.Is(err, errPodRecoveryChanged) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, err
		}
		log.FromContext(ctx).Info("scheduled environment pod recovery", "environment", env.Name, "pod", pod.Name, "attempt", attempts+1, "maxAttempts", podRecoveryLimit, "nextAttemptAt", next.Time)
		return ctrl.Result{RequeueAfter: podRecoveryBackoff(attempts)}, nil
	}

	if nextAttemptAt != nil {
		if now.Before(nextAttemptAt.Time) {
			return ctrl.Result{RequeueAfter: nextAttemptAt.Sub(now)}, nil
		}
		return ctrl.Result{Requeue: true}, nil
	}
	if err := r.deleteObservedChild(ctx, pod); err != nil && !errors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("delete terminal pod for recovery: %w", err)
	}
	log.FromContext(ctx).Info("replacing terminal environment pod", "environment", env.Name, "pod", pod.Name, "attempt", attempts, "maxAttempts", podRecoveryLimit)
	return ctrl.Result{Requeue: true}, nil
}

func podRecoveryPhase(env *platformv1alpha1.Environment, pod *corev1.Pod) platformv1alpha1.EnvironmentPhase {
	if isResumeReplacement(env) || pod != nil && podIsResume(pod) {
		return platformv1alpha1.EnvironmentPhaseResuming
	}
	return platformv1alpha1.EnvironmentPhaseCreating
}

func podRecoveryBackoff(attempts int32) time.Duration {
	return podRecoveryDelay * time.Duration(1<<attempts)
}

func nestedRecoveryMeaningful(recovery platformv1alpha1.EnvironmentRecoveryStatus) bool {
	return recovery.Attempts != 0 || recovery.Exhausted || recovery.ExecutionGeneration != 0 || recovery.NextAttemptAt != nil
}

func legacyRecoveryMeaningful(status *platformv1alpha1.EnvironmentStatus) bool {
	return status.PodRecoveryAttempts != 0 || status.PodRecoveryExhausted || status.PodRecoveryUID != "" || status.PodRecoveryNextAttemptAt != nil
}

// effectivePodRecovery defines coexistence deterministically: any meaningful
// nested state is authoritative; legacy state is consulted only otherwise.
func effectivePodRecovery(status *platformv1alpha1.EnvironmentStatus) (platformv1alpha1.EnvironmentRecoveryStatus, bool) {
	if nestedRecoveryMeaningful(status.Recovery) || !legacyRecoveryMeaningful(status) {
		return status.Recovery, false
	}
	return platformv1alpha1.EnvironmentRecoveryStatus{
		Attempts: status.PodRecoveryAttempts, Exhausted: status.PodRecoveryExhausted,
		NextAttemptAt: status.PodRecoveryNextAttemptAt,
	}, true
}

func (r *EnvironmentReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}
