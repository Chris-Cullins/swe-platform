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
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	nodev1 "k8s.io/api/node/v1"
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

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
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
	defaultDiskSize               = "40Gi"
	defaultIdleTimeout            = 15 * time.Minute
	projectHookTimeout            = "30m"
	hookKillAfter                 = "5s"
	podRecoveryLimit              = int32(3)
	podRecoveryDelay              = 5 * time.Second
	templateRefField              = "spec.templateRef"
	projectRefField               = "spec.projectRef"
	provisioningRuntimeClassField = "status.provisioning.runtimeClassName"
	warmPoolLabel                 = "swe.dev/warm-pool"
	projectAnnotation             = "swe.dev/project"
	runtimeClassUIDAnnotation     = "swe.dev/runtime-class-uid"
	executionGenerationAnnotation = "swe.dev/execution-generation"
)

var (
	errPodReplacing                  = stderrors.New("environment pod is being replaced")
	errPodRecoveryChanged            = stderrors.New("environment pod recovery state changed")
	errEnvironmentIncarnationChanged = stderrors.New("environment incarnation changed")
	errEnvironmentExecutionChanged   = stderrors.New("environment execution changed")
)

type childOwnershipCollisionError struct {
	kind string
	name string
}

func (e *childOwnershipCollisionError) Error() string {
	return fmt.Sprintf("%s %q is not owned by this environment", e.kind, e.name)
}

type terminalEnvironmentError struct {
	err error
}

func (e *terminalEnvironmentError) Error() string { return e.err.Error() }
func (e *terminalEnvironmentError) Unwrap() error { return e.err }

func terminalEnvironment(err error) error {
	return &terminalEnvironmentError{err: err}
}

const (
	sandboxdCredentialMount    = "/var/run/swe-platform/sandboxd"
	sandboxdSecurityRevision   = "4"
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
	APIReader                     client.Reader
	Scheme                        *runtime.Scheme
	Scope                         *tenancy.ReconcileScope
	Metrics                       *OperatorMetrics
	ControlPlaneNamespace         string
	ControlPlaneName              string
	ControlPlaneInstance          string
	EnvironmentServiceAccountName string
	Now                           func() time.Time
}

// +kubebuilder:rbac:groups=swe.dev,resources=environments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=swe.dev,resources=environments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=swe.dev,resources=environmenttemplates,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=swe.dev,resources=installations,verbs=get;list;watch
// +kubebuilder:rbac:groups=swe.dev,resources=projects,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get
// +kubebuilder:rbac:groups=swe.dev,resources=runs,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=node.k8s.io,resources=runtimeclasses,verbs=get;list;watch

// reconcileInvalidProvisioningConfiguration withdraws the published
// connection before fencing an execution domain whose referenced provisioning
// configuration cannot be safely established. The workspace and ingress policy
// are retained, and credentials are revoked only after the exact
// Environment-owned Pod is gone.
func (r *EnvironmentReconciler) reconcileInvalidProvisioningConfiguration(ctx context.Context, env *platformv1alpha1.Environment, message string) (ctrl.Result, error) {
	hadPublishedConnection := env.Status.PodName != "" || env.Status.Endpoints.Sandboxd != "" || platformv1alpha1.IsEnvironmentReady(env)
	persisted, err := r.setInvalidProvisioningStatus(ctx, env, message)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !persisted || hadPublishedConnection {
		return ctrl.Result{Requeue: true}, nil
	}
	result, _, err := r.reconcileInvalidProvisioningFence(ctx, env)
	return result, err
}

// setInvalidProvisioningStatus reports whether the invalid status was persisted
// for the exact Environment UID and generation observed by this reconcile. A
// concurrent spec correction wins without allowing stale teardown to begin.
func (r *EnvironmentReconciler) setInvalidProvisioningStatus(ctx context.Context, env *platformv1alpha1.Environment, message string) (bool, error) {
	var current platformv1alpha1.Environment
	if err := r.apiReader().Get(ctx, client.ObjectKeyFromObject(env), &current); err != nil {
		return false, err
	}
	if current.UID != env.UID {
		return false, errEnvironmentIncarnationChanged
	}
	if current.Generation != env.Generation {
		return false, nil
	}
	before := current.DeepCopy()
	applyEnvironmentStatus(&current, platformv1alpha1.EnvironmentPhaseFailed, "", "", "InvalidConfiguration", message, env.Status.LastActiveAt)
	clearChildOwnershipCollision(&current)
	if !apiequality.Semantic.DeepEqual(before.Status, current.Status) {
		if err := r.Status().Update(ctx, &current); err != nil {
			if errors.IsConflict(err) {
				return false, nil
			}
			return false, err
		}
	}
	env.Status = current.Status
	return true, nil
}

func invalidProvisioningFenceStarted(env *platformv1alpha1.Environment) bool {
	ready := apimeta.FindStatusCondition(env.Status.Conditions, platformv1alpha1.EnvironmentConditionReady)
	return env.Status.Phase == platformv1alpha1.EnvironmentPhaseFailed && ready != nil &&
		ready.Status == metav1.ConditionFalse && ready.Reason == "InvalidConfiguration"
}

func (r *EnvironmentReconciler) reconcileInvalidProvisioningFence(ctx context.Context, env *platformv1alpha1.Environment) (ctrl.Result, bool, error) {
	var pod corev1.Pod
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envPodName(env)}, &pod); err == nil {
		if exactControllerOwner(&pod, platformv1alpha1.GroupVersion.String(), "Environment", env.Name, env.UID) {
			if err := r.deleteObservedChild(ctx, &pod); err != nil && !errors.IsNotFound(err) {
				return ctrl.Result{}, true, fmt.Errorf("delete pod for invalid provisioning configuration: %w", err)
			}
			return ctrl.Result{Requeue: true}, true, nil
		}
	} else if !errors.IsNotFound(err) {
		return ctrl.Result{}, true, err
	}

	var credentials corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envCredentialName(env)}, &credentials); err == nil {
		if !exactControllerOwner(&credentials, platformv1alpha1.GroupVersion.String(), "Environment", env.Name, env.UID) {
			return ctrl.Result{}, false, nil
		}
		if err := r.deleteObservedChild(ctx, &credentials); err != nil && !errors.IsNotFound(err) {
			return ctrl.Result{}, true, fmt.Errorf("revoke credentials for invalid provisioning configuration: %w", err)
		}
		return ctrl.Result{Requeue: true}, true, nil
	} else if !errors.IsNotFound(err) {
		return ctrl.Result{}, true, err
	}
	return ctrl.Result{}, false, nil
}

// reconcileLegacyProvisioningMigration fences a pre-snapshot execution before
// allowing current authoritative Template and Project inputs to be captured.
// Its progress is durable in a positive execution generation and the presence
// of the exact-owned retained PVC. Foreign fixed-name children are never
// adopted or deleted.
func (r *EnvironmentReconciler) reconcileLegacyProvisioningMigration(ctx context.Context, env *platformv1alpha1.Environment) (bool, ctrl.Result, bool, error) {
	// Pre-snapshot controllers reserved a positive execution generation before
	// creating execution resources. Generation zero is an ordinary environment
	// that has not started, even if a malformed or stale fixed-name child exists.
	if env.Status.ExecutionGeneration <= 0 {
		return false, ctrl.Result{}, false, nil
	}
	// Suspension owns execution teardown independently of workspace migration
	// authority. Once a valid PVC identity has been durably captured—or before
	// reporting that no valid identity exists—fence the Pod and then credential.
	// A completed teardown may remain Failed without oscillating through Paused.
	fenceSuspended := func() (ctrl.Result, bool, error) {
		if !env.Status.Lifecycle.Suspended {
			return ctrl.Result{}, false, nil
		}
		needsFencing := env.Status.PodName != "" || env.Status.Endpoints.Sandboxd != "" || platformv1alpha1.IsEnvironmentReady(env)
		if !needsFencing {
			var pod corev1.Pod
			if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envPodName(env)}, &pod); err == nil {
				needsFencing = true
			} else if !errors.IsNotFound(err) {
				return ctrl.Result{}, true, err
			}
		}
		if !needsFencing {
			var credentials corev1.Secret
			if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envCredentialName(env)}, &credentials); err == nil {
				needsFencing = true
			} else if !errors.IsNotFound(err) {
				return ctrl.Result{}, true, err
			}
		}
		if needsFencing || (env.Status.Phase != platformv1alpha1.EnvironmentPhasePaused && env.Status.Phase != platformv1alpha1.EnvironmentPhaseFailed) {
			result, err := r.reconcilePaused(ctx, env)
			if err != nil {
				return ctrl.Result{}, true, r.fail(ctx, env, fmt.Errorf("pause environment before provisioning migration: %w", err))
			}
			return result, true, nil
		}
		return ctrl.Result{}, false, nil
	}

	prepare := func() (ctrl.Result, bool, error) {
		if env.Status.Phase == platformv1alpha1.EnvironmentPhaseResuming &&
			env.Status.PodName == "" && env.Status.Endpoints.Sandboxd == "" && !platformv1alpha1.IsEnvironmentReady(env) {
			return ctrl.Result{}, false, nil
		}
		if err := r.setEnvironmentStatus(ctx, env, platformv1alpha1.EnvironmentPhaseResuming, "", "", "ProvisioningMigration", "legacy execution is fenced before current provisioning sources are frozen"); err != nil {
			return ctrl.Result{}, true, err
		}
		return ctrl.Result{Requeue: true}, true, nil
	}

	// Prove migration authority before fencing anything. A durable workspace is
	// required, and every retained fixed-name child must belong to this exact
	// Environment incarnation.
	var workspace corev1.PersistentVolumeClaim
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envPVCName(env)}, &workspace); err == nil {
		if !exactControllerOwner(&workspace, platformv1alpha1.GroupVersion.String(), "Environment", env.Name, env.UID) {
			if result, handled, err := fenceSuspended(); handled || err != nil {
				return true, result, handled, err
			}
			return false, ctrl.Result{}, true, r.fail(ctx, env, &childOwnershipCollisionError{kind: "PersistentVolumeClaim", name: workspace.Name})
		}
		if !workspace.DeletionTimestamp.IsZero() {
			if result, handled, err := fenceSuspended(); handled || err != nil {
				return true, result, handled, err
			}
			return false, ctrl.Result{}, true, r.fail(ctx, env, terminalEnvironment(fmt.Errorf("legacy workspace PVC %q is deleting; refusing provisioning migration", workspace.Name)))
		}
	} else if errors.IsNotFound(err) {
		if result, handled, err := fenceSuspended(); handled || err != nil {
			return true, result, handled, err
		}
		return false, ctrl.Result{}, true, r.fail(ctx, env, terminalEnvironment(fmt.Errorf("legacy execution generation %d has no retained workspace PVC; refusing replacement", env.Status.ExecutionGeneration)))
	} else {
		return false, ctrl.Result{}, true, err
	}
	if env.Status.ProvisioningMigrationPVCUID == "" {
		before := env.DeepCopy()
		env.Status.ProvisioningMigrationPVCUID = workspace.UID
		if err := r.Status().Patch(ctx, env, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			if errors.IsConflict(err) {
				return false, ctrl.Result{Requeue: true}, true, nil
			}
			return false, ctrl.Result{}, true, err
		}
		return false, ctrl.Result{Requeue: true}, true, nil
	}
	if workspace.UID != env.Status.ProvisioningMigrationPVCUID {
		if result, handled, err := fenceSuspended(); handled || err != nil {
			return true, result, handled, err
		}
		return false, ctrl.Result{}, true, r.fail(ctx, env, terminalEnvironment(fmt.Errorf("legacy workspace PVC %q UID %q does not match migration UID %q", workspace.Name, workspace.UID, env.Status.ProvisioningMigrationPVCUID)))
	}
	if result, handled, err := fenceSuspended(); handled || err != nil {
		return true, result, handled, err
	}

	var policy networkingv1.NetworkPolicy
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envNetworkPolicyName(env)}, &policy); err == nil {
		if !exactControllerOwner(&policy, platformv1alpha1.GroupVersion.String(), "Environment", env.Name, env.UID) {
			return false, ctrl.Result{}, true, r.fail(ctx, env, &childOwnershipCollisionError{kind: "NetworkPolicy", name: policy.Name})
		}
	} else if !errors.IsNotFound(err) {
		return false, ctrl.Result{}, true, err
	}
	var pod corev1.Pod
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envPodName(env)}, &pod); err == nil {
		if !exactControllerOwner(&pod, platformv1alpha1.GroupVersion.String(), "Environment", env.Name, env.UID) {
			return false, ctrl.Result{}, true, r.fail(ctx, env, &childOwnershipCollisionError{kind: "Pod", name: pod.Name})
		}
		if result, handled, err := prepare(); handled || err != nil {
			return false, result, handled, err
		}
		if err := r.deleteObservedChild(ctx, &pod); err != nil && !errors.IsNotFound(err) {
			return false, ctrl.Result{}, true, fmt.Errorf("delete legacy Pod before provisioning migration: %w", err)
		}
		return false, ctrl.Result{Requeue: true}, true, nil
	} else if !errors.IsNotFound(err) {
		return false, ctrl.Result{}, true, err
	}

	var credentials corev1.Secret
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envCredentialName(env)}, &credentials); err == nil {
		if !exactControllerOwner(&credentials, platformv1alpha1.GroupVersion.String(), "Environment", env.Name, env.UID) {
			return false, ctrl.Result{}, true, r.fail(ctx, env, &childOwnershipCollisionError{kind: "Secret", name: credentials.Name})
		}
		if result, handled, err := prepare(); handled || err != nil {
			return false, result, handled, err
		}
		if err := r.deleteObservedChild(ctx, &credentials); err != nil && !errors.IsNotFound(err) {
			return false, ctrl.Result{}, true, fmt.Errorf("delete legacy credential before provisioning migration: %w", err)
		}
		return false, ctrl.Result{Requeue: true}, true, nil
	} else if !errors.IsNotFound(err) {
		return false, ctrl.Result{}, true, err
	}

	if env.Status.Provisioning != nil {
		return true, ctrl.Result{}, false, nil
	}
	return true, ctrl.Result{}, false, nil
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
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envPodName(env)}, &pod); err == nil {
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

// reconcileIdle schedules the next activity check or records an idle
// suspension once the
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

	before := env.Status.DeepCopy()
	env.Status.Lifecycle.Suspended = true
	env.Status.Lifecycle.SuspensionReason = platformv1alpha1.EnvironmentSuspensionReasonIdle
	env.Status.Lifecycle.SuspensionRequestID = ""
	env.Status.Lifecycle.Epoch++
	applyEnvironmentStatus(env, platformv1alpha1.EnvironmentPhaseIdle, env.Status.PodName, env.Status.Endpoints.Sandboxd, "PauseRequested", "environment is idle and suspension was recorded", env.Status.LastActiveAt)
	if apiequality.Semantic.DeepEqual(*before, env.Status) {
		return ctrl.Result{}, nil
	}
	if err := r.Status().Update(ctx, env); err != nil {
		return ctrl.Result{}, fmt.Errorf("record idle suspension: %w", err)
	}
	r.Metrics.observeLifecycle("suspend", platformv1alpha1.EnvironmentSuspensionReasonIdle)
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
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envPodName(env)}, &pod); err == nil {
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
	if env.Status.PodName != "" || env.Status.Endpoints.Sandboxd != "" || platformv1alpha1.IsEnvironmentReady(env) {
		if err := r.setEnvironmentStatus(ctx, env, platformv1alpha1.EnvironmentPhaseIdle, "", "", "PauseRequested", "environment suspension is fencing the current execution domain"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	podName := envPodName(env)
	var pod corev1.Pod
	err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: podName}, &pod)
	if err == nil {
		if !exactControllerOwner(&pod, platformv1alpha1.GroupVersion.String(), "Environment", env.Name, env.UID) {
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
		if !exactControllerOwner(&credentials, platformv1alpha1.GroupVersion.String(), "Environment", env.Name, env.UID) {
			return ctrl.Result{}, &childOwnershipCollisionError{kind: "Secret", name: credentials.Name}
		}
		if err := r.deleteObservedChild(ctx, &credentials); err != nil && !errors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("revoke sandboxd credentials: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	} else if !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	if env.Status.Phase == platformv1alpha1.EnvironmentPhasePaused && env.Status.Lifecycle.PendingSuspendRequestID != "" {
		env.Status.Lifecycle.PendingSuspendRequestID = ""
		if err := r.Status().Update(ctx, env); err != nil {
			return ctrl.Result{}, fmt.Errorf("acknowledge suspension request: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	return ctrl.Result{}, r.setPhase(ctx, env, platformv1alpha1.EnvironmentPhasePaused, "", "")
}

func (r *EnvironmentReconciler) ensureWorkspacePVC(ctx context.Context, env *platformv1alpha1.Environment, tmpl *platformv1alpha1.EnvironmentTemplate) (bool, error) {
	pvcName := envPVCName(env)
	legacyPVCUID := types.UID("")
	if env.Status.Provisioning != nil {
		legacyPVCUID = env.Status.Provisioning.LegacyWorkspacePVCUID
	}
	var pvc corev1.PersistentVolumeClaim
	err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: pvcName}, &pvc)
	if err == nil {
		if !exactControllerOwner(&pvc, platformv1alpha1.GroupVersion.String(), "Environment", env.Name, env.UID) {
			return false, &childOwnershipCollisionError{kind: "PersistentVolumeClaim", name: pvcName}
		}
		if !pvc.DeletionTimestamp.IsZero() {
			return false, terminalEnvironment(fmt.Errorf("workspace PVC %q is deleting", pvcName))
		}
		if legacyPVCUID != "" && pvc.UID != legacyPVCUID {
			return false, terminalEnvironment(fmt.Errorf("legacy workspace PVC %q UID %q does not match frozen UID %q", pvcName, pvc.UID, legacyPVCUID))
		}
		return true, nil
	}
	if !errors.IsNotFound(err) {
		return false, err
	}
	if legacyPVCUID != "" {
		return false, terminalEnvironment(fmt.Errorf("legacy workspace PVC %q with frozen UID %q is missing", pvcName, legacyPVCUID))
	}

	size := resource.MustParse(defaultDiskSize)
	if env.Status.Provisioning != nil {
		size = env.Status.Provisioning.DiskSize.DeepCopy()
	} else if tmpl.Spec.DiskSize != nil {
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

// envImagePullPolicy applies the Kubernetes default pull-policy convention to
// environment images: mutable :latest (or untagged, which implies :latest)
// references must re-pull so a long-lived cluster does not pin a stale image;
// immutable tags, including the locally kind-loaded :dev, and digest
// references use the node cache.
func envImagePullPolicy(image string) corev1.PullPolicy {
	if strings.Contains(image, "@") {
		return corev1.PullIfNotPresent
	}
	if i := strings.LastIndex(image, ":"); i >= 0 && !strings.Contains(image[i:], "/") {
		if image[i+1:] != "latest" {
			return corev1.PullIfNotPresent
		}
	}
	return corev1.PullAlways
}

// ensurePod resolves the optional Project and returns the backing pod, creating
// it if necessary. Reconcile resolves the Project earlier so validation happens
// before any child resource is created.
func (r *EnvironmentReconciler) ensurePod(ctx context.Context, env *platformv1alpha1.Environment, tmpl *platformv1alpha1.EnvironmentTemplate) (*corev1.Pod, error) {
	project, err := r.resolveEnvironmentProject(ctx, env)
	if err != nil {
		return nil, err
	}
	if project != nil && len(project.Spec.EgressAllowlist) != 0 {
		return nil, terminalEnvironment(stderrors.New(unsupportedEgressAllowlistMessage(project.Name)))
	}
	if err := validateEnvironmentProject(project); err != nil {
		return nil, err
	}
	runtimeClassUID := types.UID("")
	runtimeClassName := tmpl.Spec.RuntimeClass
	if env.Status.Provisioning != nil {
		runtimeClassName = env.Status.Provisioning.RuntimeClassName
	}
	if runtimeClassName != "" {
		var runtimeClass nodev1.RuntimeClass
		if err := r.apiReader().Get(ctx, types.NamespacedName{Name: runtimeClassName}, &runtimeClass); err != nil {
			return nil, err
		}
		runtimeClassUID = runtimeClass.UID
	}
	return r.ensurePodForProject(ctx, env, tmpl, project, runtimeClassUID, tenancy.Claim{})
}

func (r *EnvironmentReconciler) resolveEnvironmentProject(ctx context.Context, env *platformv1alpha1.Environment) (*platformv1alpha1.Project, error) {
	if env.Spec.ProjectRef == "" {
		return nil, nil
	}
	var project platformv1alpha1.Project
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: env.Spec.ProjectRef}, &project); err != nil {
		wrapped := fmt.Errorf("get project %q: %w", env.Spec.ProjectRef, err)
		if errors.IsNotFound(err) {
			wrapped = terminalEnvironment(wrapped)
		}
		return nil, wrapped
	}
	return &project, nil
}

func validateEnvironmentProject(project *platformv1alpha1.Project) error {
	if project != nil && !project.DeletionTimestamp.IsZero() {
		return terminalEnvironment(fmt.Errorf("project %q is deleting", project.Name))
	}
	if project != nil && len(project.Spec.Repositories) != 1 {
		return terminalEnvironment(fmt.Errorf("project %q must have exactly one repository, got %d", project.Name, len(project.Spec.Repositories)))
	}
	return nil
}

func unsupportedEgressAllowlistMessage(projectName string) string {
	return fmt.Sprintf("project %q has a non-empty egressAllowlist, which is unsupported until GitHub issue #68 is implemented", projectName)
}

// backendCreationSourcesCurrent is the final uncached authority fence around a
// Pod create. Source generations may advance after the immutable provisioning
// inputs were captured, but source and Environment incarnations may not change.
func (r *EnvironmentReconciler) backendCreationSourcesCurrent(ctx context.Context, env *platformv1alpha1.Environment, claim tenancy.Claim) (bool, error) {
	var currentEnv platformv1alpha1.Environment
	if err := r.apiReader().Get(ctx, client.ObjectKeyFromObject(env), &currentEnv); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	hold := currentEnv.Spec.Lifecycle.Hold
	if currentEnv.UID != env.UID || currentEnv.Generation != env.Generation || !currentEnv.DeletionTimestamp.IsZero() ||
		currentEnv.Spec.Paused || currentEnv.Status.Lifecycle.Suspended || hold != nil && hold.Enabled ||
		!platformv1alpha1.ProvisioningSnapshotsEqual(currentEnv.Status.Provisioning, env.Status.Provisioning) {
		return false, nil
	}
	// Direct unit-level callers may exercise the lower-level Pod constructor
	// without the reconcile pipeline's required provisioning snapshot.
	if env.Status.Provisioning == nil {
		return true, nil
	}

	snapshot := env.Status.Provisioning
	var tmpl platformv1alpha1.EnvironmentTemplate
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: snapshot.Template.Name}, &tmpl); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if tmpl.UID != snapshot.Template.UID || !tmpl.DeletionTimestamp.IsZero() || tenancy.IsCatalogSource(&tmpl) {
		return false, nil
	}
	if r.Scope != nil && r.Scope.Verifier != nil && tenancy.ValidateManagedTemplate(&tmpl, r.Scope.Verifier.Installation, claim) != nil {
		return false, nil
	}

	if snapshot.Project == nil {
		if currentEnv.Spec.ProjectRef != "" {
			return false, nil
		}
	} else {
		if currentEnv.Spec.ProjectRef != snapshot.Project.Name {
			return false, nil
		}
		var project platformv1alpha1.Project
		if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: snapshot.Project.Name}, &project); err != nil {
			if errors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		if project.UID != snapshot.Project.UID || !project.DeletionTimestamp.IsZero() {
			return false, nil
		}
	}
	if snapshot.LegacyWorkspacePVCUID != "" {
		var pvc corev1.PersistentVolumeClaim
		if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envPVCName(env)}, &pvc); err != nil {
			if errors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		if pvc.UID != snapshot.LegacyWorkspacePVCUID || !pvc.DeletionTimestamp.IsZero() ||
			!exactControllerOwner(&pvc, platformv1alpha1.GroupVersion.String(), "Environment", env.Name, env.UID) {
			return false, nil
		}
	}
	return true, nil
}

func (r *EnvironmentReconciler) ensurePodForProject(ctx context.Context, env *platformv1alpha1.Environment, tmpl *platformv1alpha1.EnvironmentTemplate, project *platformv1alpha1.Project, runtimeClassUID types.UID, claim tenancy.Claim) (*corev1.Pod, error) {
	podName := envPodName(env)
	resuming := env.Status.Phase == platformv1alpha1.EnvironmentPhaseResuming
	var pod corev1.Pod
	err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: podName}, &pod)
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
	executionGeneration, err := r.reserveExecutionGeneration(ctx, env)
	if err != nil {
		return nil, fmt.Errorf("reserve execution generation: %w", err)
	}

	resources, ok := sizePresets[tmpl.Spec.Size]
	image := tmpl.Spec.Image
	runtimeClassName := tmpl.Spec.RuntimeClass
	repository := ""
	if env.Status.Provisioning != nil {
		resources = make(corev1.ResourceList, len(env.Status.Provisioning.Resources))
		for name, quantity := range env.Status.Provisioning.Resources {
			resources[corev1.ResourceName(name)] = quantity.DeepCopy()
		}
		ok = true
		image = env.Status.Provisioning.Image
		runtimeClassName = env.Status.Provisioning.RuntimeClassName
		if env.Status.Provisioning.Project != nil {
			repository = env.Status.Provisioning.Project.Repository
		}
	}
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
				executionGenerationAnnotation:     strconv.FormatInt(executionGeneration, 10),
				sandboxdauth.IdentityAnnotation:   identity,
				sandboxdauth.TrustAnnotation:      string(trust),
				sandboxdauth.TokenAnnotation:      terminalToken,
				sandboxdauth.SecretUIDAnnotation:  string(credentials.UID),
				sandboxdauth.SecretNameAnnotation: credentials.Name,
				sandboxdRevisionAnnotation:        sandboxdSecurityRevision,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: ptr(false),
			ServiceAccountName:           r.EnvironmentServiceAccountName,
			SecurityContext: &corev1.PodSecurityContext{
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name:            "environment",
				Image:           image,
				ImagePullPolicy: envImagePullPolicy(image),
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
	if runtimeClassName != "" {
		pod.Spec.RuntimeClassName = &runtimeClassName
		pod.Annotations[runtimeClassUIDAnnotation] = string(runtimeClassUID)
	}
	if project != nil {
		if repository == "" {
			repository = project.Spec.Repositories[0]
		}
		projectEnv := []corev1.EnvVar{
			{Name: "SWE_REPOSITORY", Value: repository},
			{Name: "SWE_HOOK_TIMEOUT", Value: projectHookTimeout},
			{Name: "SWE_HOOK_KILL_AFTER", Value: hookKillAfter},
		}
		if resuming {
			projectEnv = append(projectEnv, corev1.EnvVar{Name: "SWE_RESUMING", Value: "true"})
		}
		pod.Spec.InitContainers = []corev1.Container{{
			Name:                     "project-setup",
			Image:                    image,
			ImagePullPolicy:          envImagePullPolicy(image),
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
	}
	if err := controllerutil.SetControllerReference(env, &pod, r.Scheme); err != nil {
		return nil, err
	}
	current, err := r.backendCreationSourcesCurrent(ctx, env, claim)
	if err != nil {
		return nil, err
	}
	if !current {
		return nil, fmt.Errorf("provisioning authority changed before Pod creation")
	}
	if err := r.Create(ctx, &pod); err != nil {
		return nil, collisionOnAlreadyExists(err, "Pod", podName)
	}
	current, err = r.backendCreationSourcesCurrent(ctx, env, claim)
	if err != nil || !current {
		deleteErr := r.deleteObservedChild(ctx, &pod)
		if deleteErr != nil && !errors.IsNotFound(deleteErr) {
			return nil, fmt.Errorf("delete Pod after provisioning authority changed: %w", deleteErr)
		}
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("provisioning authority changed during Pod creation")
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
	executionGeneration, generationValid := podExecutionGeneration(pod)
	if !metav1.IsControlledBy(pod, env) || pod.Spec.RestartPolicy != corev1.RestartPolicyNever ||
		!generationValid || executionGeneration != env.Status.ExecutionGeneration ||
		pod.Annotations[sandboxdRevisionAnnotation] != sandboxdSecurityRevision ||
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
		len(secret.Data[sandboxdauth.ProcessTokenKey]) > 0 && len(secret.Data[sandboxdauth.FilesystemTokenKey]) > 0 &&
		len(secret.Data[sandboxdauth.ServiceObservationTokenKey]) > 0 && len(secret.Data[sandboxdauth.PortalTokenKey]) > 0, nil
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
	filesystemToken, err := randomCredential(32)
	if err != nil {
		return "", nil, "", err
	}
	observationToken, err := randomCredential(32)
	if err != nil {
		return "", nil, "", err
	}
	portalToken, err := randomCredential(32)
	if err != nil {
		return "", nil, "", err
	}
	capabilities, err := json.Marshal(sandboxdauth.Config{Grants: []sandboxdauth.Grant{
		{TokenHash: sandboxdauth.TokenVerifier(terminalToken), Capabilities: []sandboxdauth.Capability{sandboxdauth.CapabilityHealth, sandboxdauth.CapabilityTerminal}},
		{TokenHash: sandboxdauth.TokenVerifier(healthToken), Capabilities: []sandboxdauth.Capability{sandboxdauth.CapabilityHealth}},
		{TokenHash: sandboxdauth.TokenVerifier(processToken), Capabilities: []sandboxdauth.Capability{sandboxdauth.CapabilityProcess}},
		{TokenHash: sandboxdauth.TokenVerifier(filesystemToken), Capabilities: []sandboxdauth.Capability{sandboxdauth.CapabilityFilesystem}},
		{TokenHash: sandboxdauth.TokenVerifier(observationToken), Capabilities: []sandboxdauth.Capability{sandboxdauth.CapabilityServiceObservation}},
		{TokenHash: sandboxdauth.TokenVerifier(portalToken), Capabilities: []sandboxdauth.Capability{sandboxdauth.CapabilityPortal}},
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
		secret.Data = sandboxdCredentialData(certificate, privateKey, capabilities, healthToken, processToken, filesystemToken, observationToken, portalToken)
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
		secret.Data = sandboxdCredentialData(certificate, privateKey, capabilities, healthToken, processToken, filesystemToken, observationToken, portalToken)
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

func sandboxdCredentialData(certificate, privateKey, capabilities []byte, healthToken, processToken, filesystemToken, observationToken, portalToken string) map[string][]byte {
	return map[string][]byte{
		sandboxdauth.TLSCertKey:                 certificate,
		sandboxdauth.TLSKeyKey:                  privateKey,
		sandboxdauth.CapabilitiesKey:            capabilities,
		sandboxdauth.HealthTokenKey:             []byte(healthToken),
		sandboxdauth.ProcessTokenKey:            []byte(processToken),
		sandboxdauth.FilesystemTokenKey:         []byte(filesystemToken),
		sandboxdauth.ServiceObservationTokenKey: []byte(observationToken),
		sandboxdauth.PortalTokenKey:             []byte(portalToken),
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
func (r *EnvironmentReconciler) apiReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// reserveExecutionGeneration durably allocates a fresh backend-neutral
// execution identity before a Pod creation attempt. Values may be skipped
// after failed or uncertain creates, but are never reused.
func (r *EnvironmentReconciler) reserveExecutionGeneration(ctx context.Context, env *platformv1alpha1.Environment) (int64, error) {
	var current platformv1alpha1.Environment
	if err := r.apiReader().Get(ctx, client.ObjectKeyFromObject(env), &current); err != nil {
		return 0, err
	}
	if current.UID != env.UID {
		return 0, errEnvironmentIncarnationChanged
	}
	if current.Generation != env.Generation || !current.DeletionTimestamp.IsZero() || current.Spec.Paused || current.Status.Lifecycle.Suspended {
		return 0, fmt.Errorf("environment changed before backend creation")
	}
	if current.Status.ExecutionGeneration == math.MaxInt64 {
		return 0, terminalEnvironment(fmt.Errorf("execution generation is exhausted"))
	}
	next := current.Status.ExecutionGeneration + 1
	before := current.DeepCopy()
	phase := platformv1alpha1.EnvironmentPhaseCreating
	if current.Status.Phase == platformv1alpha1.EnvironmentPhaseResuming {
		// Keep resume intent durable across a failed create. The next attempt
		// must still run the Project resume hook for the retained workspace.
		phase = platformv1alpha1.EnvironmentPhaseResuming
	}
	applyEnvironmentStatus(&current, phase, "", "", "ExecutionProvisioning", fmt.Sprintf("backend execution generation %d is being provisioned", next), current.Status.LastActiveAt)
	current.Status.ExecutionGeneration = next
	if err := r.Status().Patch(ctx, &current, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return 0, err
	}
	env.Status = current.Status
	return next, nil
}

func podExecutionGeneration(pod *corev1.Pod) (int64, bool) {
	value := pod.Annotations[executionGenerationAnnotation]
	generation, err := strconv.ParseInt(value, 10, 64)
	return generation, err == nil && generation > 0 && strconv.FormatInt(generation, 10) == value
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
		if err := r.apiReader().Get(ctx, key, &updated); err != nil {
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
	return left.ExecutionGeneration == right.ExecutionGeneration &&
		left.Recovery.Attempts == right.Recovery.Attempts &&
		left.Recovery.Exhausted == right.Recovery.Exhausted &&
		left.Recovery.ExecutionGeneration == right.Recovery.ExecutionGeneration &&
		apiequality.Semantic.DeepEqual(left.Recovery.NextAttemptAt, right.Recovery.NextAttemptAt) &&
		left.PodRecoveryAttempts == right.PodRecoveryAttempts &&
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
