package controllers

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	stderrors "errors"
	"fmt"
	"math/big"
	"net"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	sandboxdauth "github.com/Chris-Cullins/swe-platform/sandboxd/auth"
)

// sizePresets maps EnvironmentTemplate size names to pod resource requests/limits.
var sizePresets = map[string]corev1.ResourceList{
	"tiny":   {corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2Gi")},
	"small":  {corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")},
	"medium": {corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("16Gi")},
	"large":  {corev1.ResourceCPU: resource.MustParse("16"), corev1.ResourceMemory: resource.MustParse("32Gi")},
}

const (
	defaultDiskSize    = "40Gi"
	defaultIdleTimeout = 15 * time.Minute
	projectHookTimeout = "30m"
	hookKillAfter      = "5s"
	podRecoveryLimit   = int32(3)
	podRecoveryDelay   = 5 * time.Second
	templateRefField   = "spec.templateRef"
	warmPoolLabel      = "swe.dev/warm-pool"
	projectAnnotation  = "swe.dev/project"
)

var (
	errPodReplacing                  = stderrors.New("environment pod is being replaced")
	errPodRecoveryChanged            = stderrors.New("environment pod recovery state changed")
	errEnvironmentIncarnationChanged = stderrors.New("environment incarnation changed")
)

type childOwnershipCollisionError struct {
	kind string
	name string
}

func (e *childOwnershipCollisionError) Error() string {
	return fmt.Sprintf("%s %q is not owned by this environment", e.kind, e.name)
}

const (
	sandboxdCredentialMount    = "/var/run/swe-platform/sandboxd"
	sandboxdSecurityRevision   = "2"
	sandboxdRevisionAnnotation = "swe.dev/sandboxd-security-revision"
	environmentFinalizer       = "swe.dev/environment-security"
)

const hookRunnerScript = `set -eu
run_hook() {
	hook="$1"
	timeout --kill-after="$SWE_HOOK_KILL_AFTER" "$SWE_HOOK_TIMEOUT" /bin/sh "$hook" || {
		status="$?"
		if [ "$status" -eq 124 ] || [ "$status" -eq 137 ]; then
			echo "$hook timed out after $SWE_HOOK_TIMEOUT" >&2
			exit 124
		fi
		echo "$hook failed with exit code $status" >&2
		exit "$status"
	}
}
`

const projectSetupScript = hookRunnerScript + `
if [ ! -d /workspace/.git ]; then
	git clone -- "$SWE_REPOSITORY" /workspace
fi
if ! git -c safe.directory=/workspace -C /workspace config --local --get swe.setup-complete >/dev/null 2>&1; then
	if [ -f /workspace/.agents/setup ]; then
		run_hook /workspace/.agents/setup
	fi
	git -c safe.directory=/workspace -C /workspace config --local swe.setup-complete true
fi
if [ "${SWE_RESUMING:-false}" = true ] && [ -f /workspace/.agents/resume ]; then
	run_hook /workspace/.agents/resume
fi
`

// EnvironmentReconciler reconciles Environment objects into pods + workspace volumes.
type EnvironmentReconciler struct {
	client.Client
	Scheme                *runtime.Scheme
	ControlPlaneNamespace string
	ControlPlaneName      string
	ControlPlaneInstance  string
	Now                   func() time.Time
}

// +kubebuilder:rbac:groups=swe.dev,resources=environments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=swe.dev,resources=environments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=swe.dev,resources=environmenttemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=swe.dev,resources=projects,verbs=get;list;watch
// +kubebuilder:rbac:groups=swe.dev,resources=runs,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;create;update;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives an Environment toward its desired state:
// pod + PVC present when active, pod deleted (PVC retained) when paused.
func (r *EnvironmentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	defer func() {
		if stderrors.Is(err, errEnvironmentIncarnationChanged) {
			result = ctrl.Result{}
			err = nil
		}
	}()

	var env platformv1alpha1.Environment
	if err := r.Get(ctx, req.NamespacedName, &env); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !env.DeletionTimestamp.IsZero() {
		return r.reconcileDeleting(ctx, &env)
	}
	if !controllerutil.ContainsFinalizer(&env, environmentFinalizer) {
		controllerutil.AddFinalizer(&env, environmentFinalizer)
		if err := r.Update(ctx, &env); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	// Fencing must not depend on a still-readable template or successful setup.
	// Cancellation/finalization can therefore stop an execution domain even
	// after its template was deleted or provisioning became permanently broken.
	if env.Spec.Paused {
		result, err := r.reconcilePaused(ctx, &env)
		if err != nil {
			return ctrl.Result{}, r.fail(ctx, &env, fmt.Errorf("pause environment: %w", err))
		}
		return result, nil
	}
	if result, handled, err := r.reconcilePendingPodRecovery(ctx, &env); handled || err != nil {
		return result, err
	}
	var tmpl platformv1alpha1.EnvironmentTemplate
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: env.Spec.TemplateRef}, &tmpl); err != nil {
		return ctrl.Result{}, r.fail(ctx, &env, fmt.Errorf("get template %q: %w", env.Spec.TemplateRef, err))
	}
	backend := platformv1alpha1.EffectiveEnvironmentBackend(&env, &tmpl)
	if backend != platformv1alpha1.EnvironmentBackendPod {
		return r.reconcileUnsupportedBackend(ctx, &env, backend)
	}

	pvcReady, err := r.ensureWorkspacePVC(ctx, &env, &tmpl)
	if err != nil {
		return ctrl.Result{}, r.fail(ctx, &env, fmt.Errorf("ensure workspace PVC: %w", err))
	}
	if !pvcReady {
		return ctrl.Result{Requeue: true}, nil
	}
	policyReady, err := r.ensureSandboxdNetworkPolicy(ctx, &env)
	if err != nil {
		return ctrl.Result{}, r.fail(ctx, &env, fmt.Errorf("ensure sandboxd network policy: %w", err))
	}
	if !policyReady {
		return ctrl.Result{Requeue: true}, nil
	}

	if env.Status.Phase == platformv1alpha1.EnvironmentPhasePaused {
		now := metav1.Now()
		env.Status.LastActiveAt = &now
		return ctrl.Result{Requeue: true}, r.setPhase(ctx, &env, platformv1alpha1.EnvironmentPhaseResuming, "", "")
	}

	pod, err := r.ensurePod(ctx, &env, &tmpl)
	if stderrors.Is(err, errPodReplacing) {
		return ctrl.Result{Requeue: true}, nil
	}
	if err != nil {
		return ctrl.Result{}, r.fail(ctx, &env, fmt.Errorf("ensure pod: %w", err))
	}
	if pod == nil {
		return ctrl.Result{Requeue: true}, nil
	}
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		return r.reconcileTerminalPod(ctx, &env, pod)
	}

	if err := r.syncStatus(ctx, &env, pod); err != nil {
		return ctrl.Result{}, err
	}
	if pod.Status.Phase != corev1.PodRunning {
		return ctrl.Result{}, nil
	}
	return r.reconcileIdle(ctx, &env, &tmpl)
}

// reconcilePendingPodRecovery advances persisted recovery before ensurePod can
// create a replacement. This keeps backoff and exhaustion effective even when
// a terminal Pod disappears without the controller deleting it.
func (r *EnvironmentReconciler) reconcilePendingPodRecovery(ctx context.Context, env *platformv1alpha1.Environment) (ctrl.Result, bool, error) {
	ready := apimeta.FindStatusCondition(env.Status.Conditions, platformv1alpha1.EnvironmentConditionReady)
	if env.Status.PodRecoveryExhausted {
		message := fmt.Sprintf("automatic recovery is exhausted after %d terminal pod replacements", env.Status.PodRecoveryAttempts)
		if ready == nil || ready.ObservedGeneration != env.Generation || ready.Status != metav1.ConditionFalse || ready.Reason != "PodRecoveryExhausted" {
			if err := r.setEnvironmentStatus(ctx, env, platformv1alpha1.EnvironmentPhaseFailed, env.Status.PodName, "", "PodRecoveryExhausted", message); err != nil {
				return ctrl.Result{}, true, err
			}
		}
		return ctrl.Result{}, true, nil
	}
	nextAttemptAt := env.Status.PodRecoveryNextAttemptAt
	if nextAttemptAt == nil {
		return ctrl.Result{}, false, nil
	}
	now := r.now()
	if now.Before(nextAttemptAt.Time) {
		if ready == nil || ready.ObservedGeneration != env.Generation || ready.Status != metav1.ConditionFalse || ready.Reason != "PodRecoveryPending" {
			message := fmt.Sprintf("terminal pod recovery attempt %d of %d is scheduled for %s", env.Status.PodRecoveryAttempts+1, podRecoveryLimit, nextAttemptAt.Time.UTC().Format(time.RFC3339))
			if err := r.setEnvironmentStatus(ctx, env, platformv1alpha1.EnvironmentPhaseCreating, "", "", "PodRecoveryPending", message); err != nil {
				return ctrl.Result{}, true, err
			}
		}
		return ctrl.Result{RequeueAfter: nextAttemptAt.Sub(now)}, true, nil
	}

	attempts := env.Status.PodRecoveryAttempts + 1
	message := fmt.Sprintf("replacing terminal environment pod (recovery attempt %d of %d)", attempts, podRecoveryLimit)
	if err := r.updatePodRecoveryStatus(ctx, env, func(current *platformv1alpha1.Environment) {
		applyEnvironmentStatus(current, platformv1alpha1.EnvironmentPhaseCreating, "", "", "PodRecovering", message, env.Status.LastActiveAt)
		current.Status.PodRecoveryAttempts = attempts
		current.Status.PodRecoveryUID = env.Status.PodRecoveryUID
		current.Status.PodRecoveryNextAttemptAt = nil
		clearChildOwnershipCollision(current)
	}); err != nil {
		if stderrors.Is(err, errPodRecoveryChanged) {
			return ctrl.Result{Requeue: true}, true, nil
		}
		return ctrl.Result{}, true, err
	}
	// Reconcile again before deleting or creating so the persisted attempt marker
	// is always observed, including across a concurrent generation change.
	return ctrl.Result{Requeue: true}, true, nil
}

// reconcileTerminalPod replaces an owned terminal Pod using a persisted,
// bounded retry budget. The Pod UID fences both the delay and delete
// so retries after controller or API failures cannot consume the budget twice
// or delete a same-name replacement.
func (r *EnvironmentReconciler) reconcileTerminalPod(ctx context.Context, env *platformv1alpha1.Environment, pod *corev1.Pod) (ctrl.Result, error) {
	now := r.now()
	attempts := env.Status.PodRecoveryAttempts
	recoveryUID := env.Status.PodRecoveryUID
	nextAttemptAt := env.Status.PodRecoveryNextAttemptAt

	if recoveryUID != pod.UID {
		if attempts >= podRecoveryLimit {
			message := fmt.Sprintf("environment pod %s after %d recovery attempts; automatic recovery is exhausted", strings.ToLower(string(pod.Status.Phase)), attempts)
			if err := r.updatePodRecoveryStatus(ctx, env, func(current *platformv1alpha1.Environment) {
				applyEnvironmentStatus(current, platformv1alpha1.EnvironmentPhaseFailed, pod.Name, "", "PodRecoveryExhausted", message, env.Status.LastActiveAt)
				current.Status.PodRecoveryExhausted = true
				clearChildOwnershipCollision(current)
			}); err != nil {
				if stderrors.Is(err, errPodRecoveryChanged) {
					return ctrl.Result{Requeue: true}, nil
				}
				return ctrl.Result{}, err
			}
			log.FromContext(ctx).Info("environment pod recovery exhausted", "environment", env.Name, "pod", pod.Name, "attempts", attempts)
			return ctrl.Result{}, nil
		}
		next := metav1.NewTime(now.Add(podRecoveryBackoff(attempts)))
		message := fmt.Sprintf("environment pod %s; recovery attempt %d of %d is scheduled for %s", strings.ToLower(string(pod.Status.Phase)), attempts+1, podRecoveryLimit, next.Time.UTC().Format(time.RFC3339))
		if err := r.updatePodRecoveryStatus(ctx, env, func(current *platformv1alpha1.Environment) {
			applyEnvironmentStatus(current, platformv1alpha1.EnvironmentPhaseCreating, "", "", "PodRecoveryPending", message, env.Status.LastActiveAt)
			current.Status.PodRecoveryAttempts = attempts
			current.Status.PodRecoveryExhausted = false
			current.Status.PodRecoveryUID = pod.UID
			current.Status.PodRecoveryNextAttemptAt = &next
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

func podRecoveryBackoff(attempts int32) time.Duration {
	return podRecoveryDelay * time.Duration(1<<attempts)
}

func (r *EnvironmentReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// reconcileUnsupportedBackend withdraws the published connection identity
// before stopping any legacy pod admitted under an older CRD. It retains the
// workspace PVC and removes credentials only after the pod is gone.
func (r *EnvironmentReconciler) reconcileUnsupportedBackend(ctx context.Context, env *platformv1alpha1.Environment, backend platformv1alpha1.EnvironmentBackend) (ctrl.Result, error) {
	hadPublishedConnection := env.Status.PodName != "" || env.Status.Endpoints.Sandboxd != "" || platformv1alpha1.IsEnvironmentReady(env)
	message := fmt.Sprintf("environment backend %q is not supported; only %q is available", backend, platformv1alpha1.EnvironmentBackendPod)
	if err := r.setEnvironmentStatus(ctx, env, platformv1alpha1.EnvironmentPhaseFailed, "", "", "UnsupportedBackend", message); err != nil {
		return ctrl.Result{}, err
	}
	if hadPublishedConnection {
		return ctrl.Result{Requeue: true}, nil
	}

	var pod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envPodName(env)}, &pod); err == nil {
		if !metav1.IsControlledBy(&pod, env) {
			return ctrl.Result{}, nil
		}
		if err := r.deleteObservedChild(ctx, &pod); err != nil && !errors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("delete pod for unsupported backend: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	} else if !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	var credentials corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envCredentialName(env)}, &credentials); err == nil {
		if !metav1.IsControlledBy(&credentials, env) {
			return ctrl.Result{}, nil
		}
		if err := r.deleteObservedChild(ctx, &credentials); err != nil && !errors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("revoke credentials for unsupported backend: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	} else if !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// reconcileIdle schedules the next activity check or requests a pause once the
// template's idle timeout has elapsed. An exact, non-terminal Run owner or
// claim is authoritative activity regardless of timestamps. The subsequent
// optimistic Environment patch closes the race with a concurrent claim.
func (r *EnvironmentReconciler) reconcileIdle(ctx context.Context, env *platformv1alpha1.Environment, tmpl *platformv1alpha1.EnvironmentTemplate) (ctrl.Result, error) {
	if env.Labels[warmPoolLabel] != "" {
		return ctrl.Result{}, nil
	}
	timeout := defaultIdleTimeout
	if tmpl.Spec.IdleTimeout != nil {
		timeout = tmpl.Spec.IdleTimeout.Duration
	}
	active, err := r.hasActiveRun(ctx, env)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("check active run: %w", err)
	}
	if active {
		return ctrl.Result{RequeueAfter: timeout}, nil
	}
	remaining := timeout
	if env.Status.LastActiveAt != nil {
		remaining = env.Status.LastActiveAt.Add(timeout).Sub(r.now())
	}
	if remaining > 0 {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}

	before := env.DeepCopy()
	env.Spec.Paused = true
	patch := client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})
	if err := r.Patch(ctx, env, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("request idle pause: %w", err)
	}
	if err := r.setPhase(ctx, env, platformv1alpha1.EnvironmentPhaseIdle, env.Status.PodName, env.Status.Endpoints.Sandboxd); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

func (r *EnvironmentReconciler) hasActiveRun(ctx context.Context, env *platformv1alpha1.Environment) (bool, error) {
	var reference *platformv1alpha1.RunReference
	if owner := metav1.GetControllerOf(env); owner != nil && owner.APIVersion == platformv1alpha1.GroupVersion.String() && owner.Kind == "Run" {
		reference = &platformv1alpha1.RunReference{Name: owner.Name, UID: owner.UID}
	} else if env.Status.ClaimedBy != nil {
		reference = env.Status.ClaimedBy
	}
	if reference == nil || reference.Name == "" || reference.UID == "" {
		return false, nil
	}
	var run platformv1alpha1.Run
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: reference.Name}, &run); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return run.UID == reference.UID && !terminalRunState(run.Status.State), nil
}

// reconcileDeleting orders revocation: stop sandboxd, remove its credentials,
// then remove network isolation and allow the Environment to disappear.
func (r *EnvironmentReconciler) reconcileDeleting(ctx context.Context, env *platformv1alpha1.Environment) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(env, environmentFinalizer) {
		return ctrl.Result{}, nil
	}
	if err := r.setEnvironmentStatus(ctx, env, platformv1alpha1.EnvironmentPhaseCreating, "", "", "Deleting", "environment deletion is in progress"); err != nil {
		return ctrl.Result{}, fmt.Errorf("withdraw readiness during environment deletion: %w", err)
	}
	var pod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envPodName(env)}, &pod); err == nil {
		if !metav1.IsControlledBy(&pod, env) {
			// A foreign fixed-name object must not be destroyed by this finalizer.
		} else if err := r.deleteObservedChild(ctx, &pod); err != nil && !errors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("delete pod during environment deletion: %w", err)
		} else {
			return ctrl.Result{Requeue: true}, nil
		}
	} else if !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	var credentials corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envCredentialName(env)}, &credentials); err == nil {
		if !metav1.IsControlledBy(&credentials, env) {
			// Leave foreign objects untouched.
		} else if err := r.deleteObservedChild(ctx, &credentials); err != nil && !errors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("revoke sandboxd credentials during environment deletion: %w", err)
		} else {
			return ctrl.Result{Requeue: true}, nil
		}
	} else if !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	var policy networkingv1.NetworkPolicy
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envNetworkPolicyName(env)}, &policy); err == nil {
		if !metav1.IsControlledBy(&policy, env) {
			// Leave foreign objects untouched.
		} else if err := r.deleteObservedChild(ctx, &policy); err != nil && !errors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("delete sandboxd network policy: %w", err)
		} else {
			return ctrl.Result{Requeue: true}, nil
		}
	} else if !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	var pvc corev1.PersistentVolumeClaim
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envPVCName(env)}, &pvc); err == nil {
		if !metav1.IsControlledBy(&pvc, env) {
			// Leave foreign objects untouched.
		} else if err := r.deleteObservedChild(ctx, &pvc); err != nil && !errors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("delete workspace during environment deletion: %w", err)
		} else {
			return ctrl.Result{Requeue: true}, nil
		}
	} else if !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	controllerutil.RemoveFinalizer(env, environmentFinalizer)
	return ctrl.Result{}, r.Update(ctx, env)
}

// reconcilePaused deletes the pod (if any) and keeps the workspace volume.
func (r *EnvironmentReconciler) reconcilePaused(ctx context.Context, env *platformv1alpha1.Environment) (ctrl.Result, error) {
	podName := envPodName(env)
	var pod corev1.Pod
	err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: podName}, &pod)
	if err == nil {
		if !metav1.IsControlledBy(&pod, env) {
			return ctrl.Result{}, &childOwnershipCollisionError{kind: "Pod", name: podName}
		}
		if delErr := r.deleteObservedChild(ctx, &pod); delErr != nil && !errors.IsNotFound(delErr) {
			return ctrl.Result{}, fmt.Errorf("delete pod for pause: %w", delErr)
		}
		return ctrl.Result{Requeue: true}, nil
	} else if !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	var credentials corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envCredentialName(env)}, &credentials); err == nil {
		if !metav1.IsControlledBy(&credentials, env) {
			return ctrl.Result{}, &childOwnershipCollisionError{kind: "Secret", name: credentials.Name}
		}
		if err := r.deleteObservedChild(ctx, &credentials); err != nil && !errors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("revoke sandboxd credentials: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	} else if !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, r.setPhase(ctx, env, platformv1alpha1.EnvironmentPhasePaused, "", "")
}

func (r *EnvironmentReconciler) ensureWorkspacePVC(ctx context.Context, env *platformv1alpha1.Environment, tmpl *platformv1alpha1.EnvironmentTemplate) (bool, error) {
	pvcName := envPVCName(env)
	var pvc corev1.PersistentVolumeClaim
	err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: pvcName}, &pvc)
	if err == nil {
		if !metav1.IsControlledBy(&pvc, env) {
			return false, &childOwnershipCollisionError{kind: "PersistentVolumeClaim", name: pvcName}
		}
		return true, nil
	}
	if !errors.IsNotFound(err) {
		return false, err
	}

	size := resource.MustParse(defaultDiskSize)
	if tmpl.Spec.DiskSize != nil {
		size = *tmpl.Spec.DiskSize
	}

	pvc = corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: env.Namespace,
			Name:      pvcName,
			Labels:    envLabels(env),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: size},
			},
		},
	}
	if err := controllerutil.SetControllerReference(env, &pvc, r.Scheme); err != nil {
		return false, err
	}
	if err := r.Create(ctx, &pvc); err != nil {
		return false, collisionOnAlreadyExists(err, "PersistentVolumeClaim", pvcName)
	}
	return true, nil
}

// ensurePod returns the backing pod, creating it if necessary.
func (r *EnvironmentReconciler) ensurePod(ctx context.Context, env *platformv1alpha1.Environment, tmpl *platformv1alpha1.EnvironmentTemplate) (*corev1.Pod, error) {
	podName := envPodName(env)
	var pod corev1.Pod
	err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: podName}, &pod)
	if err == nil {
		if metav1.IsControlledBy(&pod, env) && !pod.DeletionTimestamp.IsZero() {
			if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
				secure, err := r.currentSandboxdPod(ctx, env, &pod)
				if err != nil {
					return nil, err
				}
				if secure {
					return &pod, nil
				}
			}
			if err := r.setEnvironmentStatus(ctx, env, platformv1alpha1.EnvironmentPhaseCreating, "", "", "PodTerminating", "the previous environment pod is terminating"); err != nil {
				return nil, err
			}
			if env.Status.PodName != "" || env.Status.Endpoints.Sandboxd != "" {
				return nil, errPodReplacing
			}
			return nil, nil
		}
		if metav1.IsControlledBy(&pod, env) {
			secure, err := r.currentSandboxdPod(ctx, env, &pod)
			if err != nil {
				return nil, err
			}
			if pod.Annotations[projectAnnotation] != env.Spec.ProjectRef {
				if err := r.setPhase(ctx, env, platformv1alpha1.EnvironmentPhaseSetup, "", ""); err != nil {
					return nil, err
				}
				if env.Status.PodName != "" || env.Status.Endpoints.Sandboxd != "" {
					return nil, errPodReplacing
				}
				if err := r.deleteObservedChild(ctx, &pod); err != nil && !errors.IsNotFound(err) {
					return nil, fmt.Errorf("replace pod for project change: %w", err)
				}
				return nil, errPodReplacing
			}
			if secure {
				return &pod, nil
			}
		}
		if !metav1.IsControlledBy(&pod, env) {
			return nil, &childOwnershipCollisionError{kind: "Pod", name: podName}
		}
		if err := r.setEnvironmentStatus(ctx, env, platformv1alpha1.EnvironmentPhaseCreating, "", "", "PodReplacing", "the environment pod is being replaced before readiness can be restored"); err != nil {
			return nil, err
		}
		if env.Status.PodName != "" || env.Status.Endpoints.Sandboxd != "" {
			return nil, errPodReplacing
		}
		if err := r.deleteObservedChild(ctx, &pod); err != nil && !errors.IsNotFound(err) {
			return nil, err
		}
		return nil, nil
	}
	if !errors.IsNotFound(err) {
		return nil, err
	}
	var existingCredentials corev1.Secret
	err = r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envCredentialName(env)}, &existingCredentials)
	if err == nil {
		if !metav1.IsControlledBy(&existingCredentials, env) {
			return nil, &childOwnershipCollisionError{kind: "Secret", name: existingCredentials.Name}
		}
		// The prior pod disappeared, so rotate this incarnation's Secret in place.
	}
	if err != nil && !errors.IsNotFound(err) {
		return nil, err
	}
	identity, trust, terminalToken, err := r.rotateSandboxdCredentials(ctx, env)
	if err != nil {
		return nil, fmt.Errorf("rotate sandboxd credentials: %w", err)
	}
	var credentials corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envCredentialName(env)}, &credentials); err != nil {
		return nil, fmt.Errorf("get rotated sandboxd credentials: %w", err)
	}

	resources, ok := sizePresets[tmpl.Spec.Size]
	if !ok {
		resources = sizePresets["medium"]
	}

	pod = corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: env.Namespace,
			Name:      podName,
			Labels:    envLabels(env),
			Annotations: map[string]string{
				projectAnnotation:                 env.Spec.ProjectRef,
				sandboxdauth.IdentityAnnotation:   identity,
				sandboxdauth.TrustAnnotation:      string(trust),
				sandboxdauth.TokenAnnotation:      terminalToken,
				sandboxdauth.SecretUIDAnnotation:  string(credentials.UID),
				sandboxdauth.SecretNameAnnotation: credentials.Name,
				sandboxdRevisionAnnotation:        sandboxdSecurityRevision,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyAlways,
			AutomountServiceAccountToken: ptr(false),
			SecurityContext: &corev1.PodSecurityContext{
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name:            "environment",
				Image:           tmpl.Spec.Image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"sandboxd", "serve"},
				Args: []string{
					"-tls-cert=" + sandboxdCredentialMount + "/" + sandboxdauth.TLSCertKey,
					"-tls-key=" + sandboxdCredentialMount + "/" + sandboxdauth.TLSKeyKey,
					"-capabilities=" + sandboxdCredentialMount + "/" + sandboxdauth.CapabilitiesKey,
				},
				Ports: []corev1.ContainerPort{{
					Name:          "sandboxd",
					ContainerPort: 50051,
				}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler:   corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: sandboxdHealthcheckCommand()}},
					PeriodSeconds:  2,
					TimeoutSeconds: 2,
				},
				StartupProbe: &corev1.Probe{
					ProbeHandler:     corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: sandboxdHealthcheckCommand()}},
					PeriodSeconds:    2,
					FailureThreshold: 30,
					TimeoutSeconds:   2,
				},
				LivenessProbe: &corev1.Probe{
					ProbeHandler:     corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: sandboxdHealthcheckCommand()}},
					PeriodSeconds:    10,
					FailureThreshold: 3,
					TimeoutSeconds:   2,
				},
				Resources: corev1.ResourceRequirements{
					Requests: resources,
					Limits:   resources,
				},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: ptr(false),
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "workspace", MountPath: "/workspace"},
					{Name: "sandboxd-credentials", MountPath: sandboxdCredentialMount, ReadOnly: true},
				},
			}},
			Volumes: []corev1.Volume{
				{
					Name: "workspace",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: envPVCName(env)},
					},
				},
				{
					Name: "sandboxd-credentials",
					VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
						SecretName:  envCredentialName(env),
						DefaultMode: ptr(int32(0o444)),
						Items: []corev1.KeyToPath{
							{Key: sandboxdauth.TLSCertKey, Path: sandboxdauth.TLSCertKey},
							{Key: sandboxdauth.TLSKeyKey, Path: sandboxdauth.TLSKeyKey},
							{Key: sandboxdauth.CapabilitiesKey, Path: sandboxdauth.CapabilitiesKey},
							{Key: sandboxdauth.HealthTokenKey, Path: sandboxdauth.HealthTokenKey},
						},
					}},
				},
			},
		},
	}
	if tmpl.Spec.RuntimeClass != "" {
		pod.Spec.RuntimeClassName = &tmpl.Spec.RuntimeClass
	}
	if env.Spec.ProjectRef != "" {
		var project platformv1alpha1.Project
		if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: env.Spec.ProjectRef}, &project); err != nil {
			return nil, fmt.Errorf("get project %q: %w", env.Spec.ProjectRef, err)
		}
		if len(project.Spec.Repositories) != 1 {
			return nil, fmt.Errorf("project %q must have exactly one repository, got %d", env.Spec.ProjectRef, len(project.Spec.Repositories))
		}
		repository := project.Spec.Repositories[0]
		projectEnv := []corev1.EnvVar{
			{Name: "SWE_REPOSITORY", Value: repository},
			{Name: "SWE_HOOK_TIMEOUT", Value: projectHookTimeout},
			{Name: "SWE_HOOK_KILL_AFTER", Value: hookKillAfter},
		}
		if env.Status.Phase == platformv1alpha1.EnvironmentPhaseResuming {
			projectEnv = append(projectEnv, corev1.EnvVar{Name: "SWE_RESUMING", Value: "true"})
		}
		pod.Spec.InitContainers = []corev1.Container{{
			Name:                     "project-setup",
			Image:                    tmpl.Spec.Image,
			ImagePullPolicy:          corev1.PullIfNotPresent,
			Command:                  []string{"/bin/sh", "-c", projectSetupScript},
			Env:                      projectEnv,
			TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
			Resources: corev1.ResourceRequirements{
				Requests: resources,
				Limits:   resources,
			},
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: ptr(false),
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			},
			VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: "/workspace"}},
		}}
		if project.Spec.SecretRef != nil {
			projectSecret := []corev1.EnvFromSource{{
				SecretRef: &corev1.SecretEnvSource{LocalObjectReference: *project.Spec.SecretRef},
			}}
			pod.Spec.InitContainers[0].EnvFrom = projectSecret
			pod.Spec.Containers[0].EnvFrom = projectSecret
		}
	}
	if err := controllerutil.SetControllerReference(env, &pod, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, &pod); err != nil {
		return nil, collisionOnAlreadyExists(err, "Pod", podName)
	}
	// Bind the private adapter credential to the exact execution incarnation.
	// Real API servers always assign a Pod UID. Fake clients do not, so an empty
	// UID remains unusable rather than weakening the live check.
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envCredentialName(env)}, &credentials); err != nil {
		return nil, err
	}
	credentials.Annotations[sandboxdauth.PodUIDAnnotation] = string(pod.UID)
	if err := r.Update(ctx, &credentials); err != nil {
		return nil, fmt.Errorf("bind sandboxd credentials to pod: %w", err)
	}
	return &pod, nil
}

func (r *EnvironmentReconciler) currentSandboxdPod(ctx context.Context, env *platformv1alpha1.Environment, pod *corev1.Pod) (bool, error) {
	if !metav1.IsControlledBy(pod, env) || pod.Annotations[sandboxdRevisionAnnotation] != sandboxdSecurityRevision ||
		pod.Annotations[sandboxdauth.IdentityAnnotation] == "" || pod.Annotations[sandboxdauth.TrustAnnotation] == "" ||
		pod.Annotations[sandboxdauth.TokenAnnotation] == "" || pod.Annotations[sandboxdauth.SecretNameAnnotation] != envCredentialName(env) {
		return false, nil
	}
	var secret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envCredentialName(env)}, &secret); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get sandboxd credentials: %w", err)
	}
	if !exactControllerOwner(&secret, platformv1alpha1.GroupVersion.String(), "Environment", env.Name, env.UID) {
		return false, &childOwnershipCollisionError{kind: "Secret", name: secret.Name}
	}
	return secret.UID != "" && pod.Annotations[sandboxdauth.SecretUIDAnnotation] == string(secret.UID) &&
		secret.Annotations[sandboxdauth.IdentityAnnotation] == pod.Annotations[sandboxdauth.IdentityAnnotation] &&
		pod.UID != "" && secret.Annotations[sandboxdauth.PodUIDAnnotation] == string(pod.UID) &&
		len(secret.Data[sandboxdauth.TLSCertKey]) > 0 && len(secret.Data[sandboxdauth.TLSKeyKey]) > 0 &&
		len(secret.Data[sandboxdauth.CapabilitiesKey]) > 0 && len(secret.Data[sandboxdauth.HealthTokenKey]) > 0 &&
		len(secret.Data[sandboxdauth.ProcessTokenKey]) > 0, nil
}

func (r *EnvironmentReconciler) rotateSandboxdCredentials(ctx context.Context, env *platformv1alpha1.Environment) (string, []byte, string, error) {
	identity, err := randomCredential(18)
	if err != nil {
		return "", nil, "", err
	}
	serverName := identity + ".sandboxd.swe.dev"
	certificate, privateKey, err := issueSandboxdCertificate(serverName)
	if err != nil {
		return "", nil, "", err
	}
	terminalToken, err := randomCredential(32)
	if err != nil {
		return "", nil, "", err
	}
	healthToken, err := randomCredential(32)
	if err != nil {
		return "", nil, "", err
	}
	processToken, err := randomCredential(32)
	if err != nil {
		return "", nil, "", err
	}
	capabilities, err := json.Marshal(sandboxdauth.Config{Grants: []sandboxdauth.Grant{
		{TokenHash: sandboxdauth.TokenVerifier(terminalToken), Capabilities: []sandboxdauth.Capability{sandboxdauth.CapabilityHealth, sandboxdauth.CapabilityTerminal}},
		{TokenHash: sandboxdauth.TokenVerifier(healthToken), Capabilities: []sandboxdauth.Capability{sandboxdauth.CapabilityHealth}},
		{TokenHash: sandboxdauth.TokenVerifier(processToken), Capabilities: []sandboxdauth.Capability{sandboxdauth.CapabilityProcess}},
	}})
	if err != nil {
		return "", nil, "", err
	}

	key := types.NamespacedName{Namespace: env.Namespace, Name: envCredentialName(env)}
	var secret corev1.Secret
	err = r.Get(ctx, key, &secret)
	if errors.IsNotFound(err) {
		secret = corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}}
		if err := controllerutil.SetControllerReference(env, &secret, r.Scheme); err != nil {
			return "", nil, "", err
		}
		secret.Data = sandboxdCredentialData(certificate, privateKey, capabilities, healthToken, processToken)
		secret.Annotations = map[string]string{sandboxdauth.IdentityAnnotation: serverName}
		if err := r.Create(ctx, &secret); err != nil {
			return "", nil, "", collisionOnAlreadyExists(err, "Secret", key.Name)
		}
	} else if err != nil {
		return "", nil, "", err
	} else {
		if !exactControllerOwner(&secret, platformv1alpha1.GroupVersion.String(), "Environment", env.Name, env.UID) {
			return "", nil, "", &childOwnershipCollisionError{kind: "Secret", name: secret.Name}
		}
		secret.Data = sandboxdCredentialData(certificate, privateKey, capabilities, healthToken, processToken)
		if secret.Annotations == nil {
			secret.Annotations = map[string]string{}
		}
		secret.Annotations[sandboxdauth.IdentityAnnotation] = serverName
		if err := r.Update(ctx, &secret); err != nil {
			return "", nil, "", err
		}
	}
	return serverName, certificate, terminalToken, nil
}

func sandboxdCredentialData(certificate, privateKey, capabilities []byte, healthToken, processToken string) map[string][]byte {
	return map[string][]byte{
		sandboxdauth.TLSCertKey:      certificate,
		sandboxdauth.TLSKeyKey:       privateKey,
		sandboxdauth.CapabilitiesKey: capabilities,
		sandboxdauth.HealthTokenKey:  []byte(healthToken),
		sandboxdauth.ProcessTokenKey: []byte(processToken),
	}
}

func sandboxdHealthcheckCommand() []string {
	return []string{
		"sandboxd", "healthcheck",
		"-ca=" + sandboxdCredentialMount + "/" + sandboxdauth.TLSCertKey,
		"-token=" + sandboxdCredentialMount + "/" + sandboxdauth.HealthTokenKey,
	}
}

func issueSandboxdCertificate(serverName string) ([]byte, []byte, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: serverName},
		DNSNames:              []string{serverName},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, err
	}
	encodedKey, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encodedKey}), nil
}

func randomCredential(size int) (string, error) {
	contents := make([]byte, size)
	if _, err := rand.Read(contents); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(contents), nil
}

func (r *EnvironmentReconciler) ensureSandboxdNetworkPolicy(ctx context.Context, env *platformv1alpha1.Environment) (bool, error) {
	name := envNetworkPolicyName(env)
	var policy networkingv1.NetworkPolicy
	err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: name}, &policy)
	if err != nil && !errors.IsNotFound(err) {
		return false, err
	}
	creating := errors.IsNotFound(err)
	if creating {
		policy = networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: env.Namespace, Name: name}}
	}
	original := policy.DeepCopy()
	if !creating {
		if !metav1.IsControlledBy(&policy, env) {
			return false, &childOwnershipCollisionError{kind: "NetworkPolicy", name: name}
		}
	}
	policy.Labels = envLabels(env)

	controlPlaneNamespace := r.ControlPlaneNamespace
	if controlPlaneNamespace == "" {
		controlPlaneNamespace = env.Namespace
	}
	controlPlaneName := r.ControlPlaneName
	if controlPlaneName == "" {
		controlPlaneName = "swe-platform"
	}
	controlPlaneInstance := r.ControlPlaneInstance
	if controlPlaneInstance == "" {
		controlPlaneInstance = "swe-platform"
	}
	protocol := corev1.ProtocolTCP
	policy.Spec = networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"swe.dev/environment": envSelectorLabel(env)}},
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
		Ingress: []networkingv1.NetworkPolicyIngressRule{{
			From: []networkingv1.NetworkPolicyPeer{{
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
					"kubernetes.io/metadata.name": controlPlaneNamespace,
				}},
				PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
					"app.kubernetes.io/name":      controlPlaneName,
					"app.kubernetes.io/instance":  controlPlaneInstance,
					"app.kubernetes.io/component": "control-plane",
				}},
			}, {
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": controlPlaneNamespace}},
				PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
					"app.kubernetes.io/name": controlPlaneName, "app.kubernetes.io/instance": controlPlaneInstance,
					"app.kubernetes.io/component": "operator",
				}},
			}},
			Ports: []networkingv1.NetworkPolicyPort{{Protocol: &protocol, Port: ptr(intstr.FromInt32(50051))}},
		}},
	}
	if err := controllerutil.SetControllerReference(env, &policy, r.Scheme); err != nil {
		return false, err
	}
	if creating {
		if err := r.Create(ctx, &policy); err != nil {
			return false, collisionOnAlreadyExists(err, "NetworkPolicy", name)
		}
		return true, nil
	}
	if apiequality.Semantic.DeepEqual(original.Labels, policy.Labels) &&
		apiequality.Semantic.DeepEqual(original.OwnerReferences, policy.OwnerReferences) &&
		apiequality.Semantic.DeepEqual(original.Spec, policy.Spec) {
		return true, nil
	}
	if err := r.Update(ctx, &policy); err != nil {
		return false, err
	}
	return true, nil
}

// syncStatus maps Kubernetes-native pod readiness and container failure state
// onto the Environment's generation-aware readiness contract.
func (r *EnvironmentReconciler) syncStatus(ctx context.Context, env *platformv1alpha1.Environment, pod *corev1.Pod) error {
	phase, reason, message := environmentPodState(env, pod)
	sandboxdEndpoint := ""
	if phase == platformv1alpha1.EnvironmentPhaseReady {
		sandboxdEndpoint = net.JoinHostPort(pod.Status.PodIP, "50051")
		if env.Status.LastActiveAt == nil || (env.Status.Phase != platformv1alpha1.EnvironmentPhaseReady && env.Status.Phase != platformv1alpha1.EnvironmentPhaseRunning) {
			now := metav1.Now()
			env.Status.LastActiveAt = &now
		}
	}
	return r.updateEnvironmentStatus(ctx, env, func(current *platformv1alpha1.Environment) {
		applyEnvironmentStatus(current, phase, pod.Name, sandboxdEndpoint, reason, message, env.Status.LastActiveAt)
		current.Status.ImageID = environmentImageID(pod)
		if phase == platformv1alpha1.EnvironmentPhaseReady {
			current.Status.PodRecoveryAttempts = 0
			current.Status.PodRecoveryExhausted = false
			current.Status.PodRecoveryUID = ""
			current.Status.PodRecoveryNextAttemptAt = nil
		}
		clearChildOwnershipCollision(current)
	})
}

func environmentImageID(pod *corev1.Pod) string {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "environment" {
			return status.ImageID
		}
	}
	return ""
}

func environmentPodState(env *platformv1alpha1.Environment, pod *corev1.Pod) (platformv1alpha1.EnvironmentPhase, string, string) {
	resuming := podIsResume(pod) || env.Status.Phase == platformv1alpha1.EnvironmentPhaseResuming
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionFalse && condition.Reason == corev1.PodReasonUnschedulable {
			return platformv1alpha1.EnvironmentPhaseCreating, "Unschedulable", messageOr(condition.Message, "the scheduler cannot currently place the environment pod")
		}
	}
	for _, status := range pod.Status.InitContainerStatuses {
		if terminated := status.State.Terminated; terminated != nil && terminated.ExitCode != 0 {
			return initContainerFailure(status.Name, terminated, resuming)
		}
		if status.State.Waiting != nil {
			waiting := status.State.Waiting
			if terminated := status.LastTerminationState.Terminated; terminated != nil && terminated.ExitCode != 0 {
				return initContainerFailure(status.Name, terminated, resuming)
			}
			if imagePullFailure(waiting.Reason) {
				return platformv1alpha1.EnvironmentPhaseFailed, "ImagePullFailed", containerStatusMessage(status.Name, waiting.Reason, waiting.Message)
			}
			if waiting.Reason == "CrashLoopBackOff" {
				return platformv1alpha1.EnvironmentPhaseFailed, setupReason(resuming, "Failed"), containerStatusMessage(status.Name, waiting.Reason, waiting.Message)
			}
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Waiting != nil {
			waiting := status.State.Waiting
			if imagePullFailure(waiting.Reason) {
				return platformv1alpha1.EnvironmentPhaseFailed, "ImagePullFailed", containerStatusMessage(status.Name, waiting.Reason, waiting.Message)
			}
			if waiting.Reason == "CrashLoopBackOff" {
				return platformv1alpha1.EnvironmentPhaseFailed, "SandboxdCrashLoopBackOff", containerStatusMessage(status.Name, waiting.Reason, waiting.Message)
			}
		}
	}

	switch pod.Status.Phase {
	case corev1.PodPending:
		if resuming {
			return platformv1alpha1.EnvironmentPhaseResuming, "ResumeInProgress", "repository resume and sandboxd startup are in progress"
		}
		if len(pod.Spec.InitContainers) > 0 {
			return platformv1alpha1.EnvironmentPhaseSetup, "SetupInProgress", "repository setup and sandboxd startup are in progress"
		}
		return platformv1alpha1.EnvironmentPhaseCreating, "Provisioning", "environment pod is provisioning"
	case corev1.PodRunning:
		if podReady(pod) && pod.Status.PodIP != "" {
			return platformv1alpha1.EnvironmentPhaseReady, "SandboxdReady", "setup is complete and sandboxd is ready"
		}
		return platformv1alpha1.EnvironmentPhaseCreating, "SandboxdNotReady", "sandboxd has not passed its readiness probe"
	case corev1.PodFailed:
		return platformv1alpha1.EnvironmentPhaseFailed, "PodFailed", messageOr(pod.Status.Message, "environment pod failed")
	case corev1.PodSucceeded:
		return platformv1alpha1.EnvironmentPhaseTerminated, "PodTerminated", "environment pod terminated"
	default:
		return platformv1alpha1.EnvironmentPhaseCreating, "Provisioning", "environment pod is provisioning"
	}
}

func initContainerFailure(name string, terminated *corev1.ContainerStateTerminated, resuming bool) (platformv1alpha1.EnvironmentPhase, string, string) {
	reason := setupReason(resuming, "Failed")
	if terminated.ExitCode == 124 || terminated.ExitCode == 137 {
		reason = setupReason(resuming, "HookTimedOut")
	}
	message := fmt.Sprintf("init container %s exited with code %d", name, terminated.ExitCode)
	if terminated.Message != "" {
		message += ": " + terminated.Message
	}
	return platformv1alpha1.EnvironmentPhaseFailed, reason, message
}

func messageOr(message, fallback string) string {
	if message != "" {
		return message
	}
	return fallback
}

func podIsResume(pod *corev1.Pod) bool {
	for _, container := range pod.Spec.InitContainers {
		for _, variable := range container.Env {
			if variable.Name == "SWE_RESUMING" && variable.Value == "true" {
				return true
			}
		}
	}
	return false
}

func imagePullFailure(reason string) bool {
	return reason == "ErrImagePull" || reason == "ImagePullBackOff" || reason == "InvalidImageName"
}

func setupReason(resuming bool, suffix string) string {
	if resuming {
		return "Resume" + suffix
	}
	return "Setup" + suffix
}

func containerStatusMessage(name, reason, message string) string {
	if message == "" {
		return fmt.Sprintf("container %s: %s", name, reason)
	}
	return fmt.Sprintf("container %s: %s: %s", name, reason, message)
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (r *EnvironmentReconciler) setPhase(ctx context.Context, env *platformv1alpha1.Environment, phase platformv1alpha1.EnvironmentPhase, podName, sandboxdEndpoint string) error {
	reason, message := phaseReadiness(phase)
	return r.setEnvironmentStatus(ctx, env, phase, podName, sandboxdEndpoint, reason, message)
}

func (r *EnvironmentReconciler) fail(ctx context.Context, env *platformv1alpha1.Environment, err error) error {
	log.FromContext(ctx).Error(err, "reconcile failed", "environment", env.Name)
	var collision *childOwnershipCollisionError
	reason := "ReconcileFailed"
	if invalidEnvironmentConfiguration(err) {
		reason = "InvalidConfiguration"
	}
	statusErr := r.updateEnvironmentStatus(ctx, env, func(current *platformv1alpha1.Environment) {
		applyEnvironmentStatus(current, platformv1alpha1.EnvironmentPhaseFailed, "", "", reason, err.Error(), env.Status.LastActiveAt)
		if stderrors.As(err, &collision) {
			apimeta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
				Type:               "ChildOwnershipConflict",
				Status:             metav1.ConditionTrue,
				ObservedGeneration: current.Generation,
				Reason:             "ResourceCollision",
				Message:            collision.Error(),
			})
		}
	})
	if statusErr != nil {
		return statusErr
	}
	if collision != nil {
		return nil
	}
	return err
}

func (r *EnvironmentReconciler) setEnvironmentStatus(ctx context.Context, env *platformv1alpha1.Environment, phase platformv1alpha1.EnvironmentPhase, podName, sandboxdEndpoint, reason, message string) error {
	return r.updateEnvironmentStatus(ctx, env, func(current *platformv1alpha1.Environment) {
		applyEnvironmentStatus(current, phase, podName, sandboxdEndpoint, reason, message, env.Status.LastActiveAt)
		clearChildOwnershipCollision(current)
	})
}

func (r *EnvironmentReconciler) updateEnvironmentStatus(ctx context.Context, env *platformv1alpha1.Environment, mutate func(*platformv1alpha1.Environment)) error {
	key := client.ObjectKeyFromObject(env)
	expectedUID := env.UID
	expectedGeneration := env.Generation
	var updated platformv1alpha1.Environment
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := r.Get(ctx, key, &updated); err != nil {
			return err
		}
		if updated.UID != expectedUID {
			return errEnvironmentIncarnationChanged
		}
		if updated.Generation != expectedGeneration {
			return nil
		}
		before := updated.DeepCopy()
		mutate(&updated)
		if apiequality.Semantic.DeepEqual(before.Status, updated.Status) {
			return nil
		}
		return r.Status().Update(ctx, &updated)
	})
	if err == nil {
		env.Status = updated.Status
	}
	return err
}

// updatePodRecoveryStatus permits spec generation changes while refusing to
// overwrite a concurrent recovery transition. Recovery is incarnation state:
// only sandboxd readiness, not an ordinary spec edit, resets it.
func (r *EnvironmentReconciler) updatePodRecoveryStatus(ctx context.Context, env *platformv1alpha1.Environment, mutate func(*platformv1alpha1.Environment)) error {
	key := client.ObjectKeyFromObject(env)
	expectedUID := env.UID
	expected := env.Status
	var updated platformv1alpha1.Environment
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := r.Get(ctx, key, &updated); err != nil {
			return err
		}
		if updated.UID != expectedUID {
			return errEnvironmentIncarnationChanged
		}
		if !samePodRecoveryState(&expected, &updated.Status) {
			return errPodRecoveryChanged
		}
		before := updated.DeepCopy()
		mutate(&updated)
		if apiequality.Semantic.DeepEqual(before.Status, updated.Status) {
			return nil
		}
		return r.Status().Update(ctx, &updated)
	})
	if err == nil {
		env.Status = updated.Status
	}
	return err
}

func samePodRecoveryState(left, right *platformv1alpha1.EnvironmentStatus) bool {
	return left.PodRecoveryAttempts == right.PodRecoveryAttempts &&
		left.PodRecoveryExhausted == right.PodRecoveryExhausted &&
		left.PodRecoveryUID == right.PodRecoveryUID &&
		apiequality.Semantic.DeepEqual(left.PodRecoveryNextAttemptAt, right.PodRecoveryNextAttemptAt)
}

func applyEnvironmentStatus(env *platformv1alpha1.Environment, phase platformv1alpha1.EnvironmentPhase, podName, sandboxdEndpoint, reason, message string, lastActiveAt *metav1.Time) {
	env.Status.ObservedGeneration = env.Generation
	env.Status.Phase = phase
	env.Status.PodName = podName
	env.Status.ImageID = ""
	env.Status.Endpoints.Sandboxd = sandboxdEndpoint
	if lastActiveAt != nil && (env.Status.LastActiveAt == nil || lastActiveAt.After(env.Status.LastActiveAt.Time)) {
		env.Status.LastActiveAt = lastActiveAt.DeepCopy()
	}
	apimeta.SetStatusCondition(&env.Status.Conditions, metav1.Condition{
		Type:               platformv1alpha1.EnvironmentConditionReady,
		Status:             boolConditionStatus(phase == platformv1alpha1.EnvironmentPhaseReady || phase == platformv1alpha1.EnvironmentPhaseRunning),
		ObservedGeneration: env.Generation,
		Reason:             reason,
		Message:            message,
	})
}

func phaseReadiness(phase platformv1alpha1.EnvironmentPhase) (string, string) {
	switch phase {
	case platformv1alpha1.EnvironmentPhaseSetup:
		return "SetupInProgress", "repository setup is in progress"
	case platformv1alpha1.EnvironmentPhaseResuming:
		return "ResumeInProgress", "repository resume is in progress"
	case platformv1alpha1.EnvironmentPhaseReady, platformv1alpha1.EnvironmentPhaseRunning:
		return "SandboxdReady", "setup is complete and sandboxd is ready"
	case platformv1alpha1.EnvironmentPhaseIdle:
		return "PauseRequested", "environment is idle and pause was requested"
	case platformv1alpha1.EnvironmentPhasePaused:
		return "Paused", "environment is paused; workspace and transcript are retained"
	case platformv1alpha1.EnvironmentPhaseFailed:
		return "ReconcileFailed", "environment reconciliation failed"
	case platformv1alpha1.EnvironmentPhaseTerminated:
		return "PodTerminated", "environment pod terminated"
	default:
		return "Provisioning", "environment infrastructure is provisioning"
	}
}

func invalidEnvironmentConfiguration(err error) bool {
	message := err.Error()
	return strings.Contains(message, "get template ") || strings.Contains(message, "get project ") || strings.Contains(message, " has no repositories")
}

func clearChildOwnershipCollision(env *platformv1alpha1.Environment) bool {
	if apimeta.FindStatusCondition(env.Status.Conditions, "ChildOwnershipConflict") == nil {
		return false
	}
	return apimeta.SetStatusCondition(&env.Status.Conditions, metav1.Condition{
		Type:               "ChildOwnershipConflict",
		Status:             metav1.ConditionFalse,
		ObservedGeneration: env.Generation,
		Reason:             "CollisionResolved",
		Message:            "all child resources are owned by this Environment",
	})
}

func collisionOnAlreadyExists(err error, kind, name string) error {
	if errors.IsAlreadyExists(err) {
		return &childOwnershipCollisionError{kind: kind, name: name}
	}
	return err
}

func (r *EnvironmentReconciler) deleteObservedChild(ctx context.Context, object client.Object) error {
	uid := object.GetUID()
	if uid == "" {
		return fmt.Errorf("refuse to delete %T %s/%s without UID", object, object.GetNamespace(), object.GetName())
	}
	return r.Delete(ctx, object, client.Preconditions{UID: &uid})
}

func envPodName(env *platformv1alpha1.Environment) string { return envChildName(env, "") }
func envPVCName(env *platformv1alpha1.Environment) string { return envChildName(env, "") }
func envCredentialName(env *platformv1alpha1.Environment) string {
	return envChildName(env, "-sandboxd")
}
func envNetworkPolicyName(env *platformv1alpha1.Environment) string {
	return envChildName(env, "-sandboxd")
}

// envChildName preserves valid legacy names so existing Environments retain
// their workspaces across upgrades. Names that would exceed 63 characters are
// bounded and include the Environment UID, avoiding collisions between long
// same-name Environment incarnations.
func envChildName(env *platformv1alpha1.Environment, suffix string) string {
	legacyName := "env-" + env.Name + suffix
	if len(env.Name) <= 63 {
		return legacyName
	}
	digest := sha256.Sum256([]byte(env.UID))
	incarnation := hex.EncodeToString(digest[:])[:12]
	const prefix = "env-"
	maxEnvironmentLength := 63 - len(prefix) - 1 - len(incarnation) - len(suffix)
	environmentName := env.Name
	if len(environmentName) > maxEnvironmentLength {
		environmentName = strings.TrimRight(environmentName[:maxEnvironmentLength], "-.")
	}
	return prefix + environmentName + "-" + incarnation + suffix
}

func envSelectorLabel(env *platformv1alpha1.Environment) string {
	if len(env.Name) <= 63 {
		return env.Name
	}
	return envPodName(env)
}

func envLabels(env *platformv1alpha1.Environment) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by": "swe-platform",
		"swe.dev/environment":          envSelectorLabel(env),
	}
}

func ptr[T any](v T) *T { return &v }

// SetupWithManager registers the controller with the manager.
func (r *EnvironmentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &platformv1alpha1.Environment{}, templateRefField, func(object client.Object) []string {
		environment := object.(*platformv1alpha1.Environment)
		return []string{environment.Spec.TemplateRef}
	}); err != nil {
		return fmt.Errorf("index environments by template: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&platformv1alpha1.Environment{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Watches(&platformv1alpha1.Run{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, object client.Object) []reconcile.Request {
			run := object.(*platformv1alpha1.Run)
			if run.Status.EnvironmentRef == nil {
				return nil
			}
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Status.EnvironmentRef.Name}}}
		})).
		Watches(&platformv1alpha1.EnvironmentTemplate{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, object client.Object) []reconcile.Request {
			var environments platformv1alpha1.EnvironmentList
			if err := r.List(ctx, &environments, client.InNamespace(object.GetNamespace()), client.MatchingFields{templateRefField: object.GetName()}); err != nil {
				log.FromContext(ctx).Error(err, "list environments for template", "template", object.GetName())
				return nil
			}
			requests := make([]reconcile.Request, 0, len(environments.Items))
			for i := range environments.Items {
				requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&environments.Items[i])})
			}
			return requests
		})).
		Complete(r)
}
