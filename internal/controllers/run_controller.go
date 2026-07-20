package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/sandboxclient"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const runFinalizer = "swe.dev/run-cleanup"

const adapterPollInterval = 2 * time.Second
const credentialReadyTimeout = time.Minute

var errAllocatedEnvironmentGone = errors.New("allocated environment is gone or no longer claimed by this run")
var errExplicitEnvironmentClaimed = errors.New("explicit environment is already claimed")

// ErrAdapterCancellationPending means cancellation was accepted but the
// adapter-owned execution tree has not reached a terminal state yet.
var ErrAdapterCancellationPending = errors.New("adapter cancellation is pending")

// ErrAdapterEventRejected means the transcript transport permanently rejected
// an adapter event. Retrying the same event cannot make progress.
var ErrAdapterEventRejected = errors.New("adapter event permanently rejected")

const (
	runConditionEnvironmentReady           = "EnvironmentReady"
	runConditionCredentialProfileBound     = "CredentialProfileBound"
	runConditionAdapterAcceptanceAttempted = "AdapterAcceptanceAttempted"
	runConditionAdapterAccepted            = "AdapterAccepted"
)

// AdapterObservation is the adapter-neutral state observed for accepted work.
type AdapterObservation string

const (
	AdapterObservationAccepted   AdapterObservation = "Accepted"
	AdapterObservationRunning    AdapterObservation = "Running"
	AdapterObservationNeedsInput AdapterObservation = "NeedsInput"
	AdapterObservationSucceeded  AdapterObservation = "Succeeded"
	AdapterObservationFailed     AdapterObservation = "Failed"
)

// AdapterTask contains immutable task identity and input. ID is the Run UID
// and is the adapter's idempotency key across retries and controller restarts.
type AdapterTask struct {
	ID     string
	Prompt string
}

// AdapterCredential is ephemeral launch-only credential material. Callers must
// not retain APIKey after EnsureAccepted returns.
type AdapterCredential struct {
	Type   platformv1alpha1.AgentCredentialType
	APIKey []byte
}

// AdapterEvent is an adapter-owned transcript event carried by the platform's
// generic transcript transport. Data is opaque to the controller.
type AdapterEvent struct {
	Source         string
	IdempotencyKey string
	Type           string
	Data           json.RawMessage
}

// AdapterEventSink forwards opaque adapter events for one namespaced Run.
// Permanent rejection wraps ErrAdapterEventRejected; other errors are retryable.
type AdapterEventSink interface {
	Append(context.Context, string, string, AdapterEvent) error
}

// AdapterSandbox is the backend-neutral handle exposed to adapters. Adapters
// use sandboxd and never inspect pods, containers, VMs, PIDs, or OS signals.
type AdapterSandbox struct {
	EnvironmentName string
	EnvironmentUID  types.UID
	DialProcess     func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error)
	EmitEvent       func(context.Context, AdapterEvent) error
}

// AdapterLifecycle translates one agent's execution model into normalized Run
// lifecycle events. Every operation must be idempotent. EnsureAccepted may be
// repeated after an uncertain response or environment resume; Cancel succeeds
// when work is already absent or terminal and returns
// ErrAdapterCancellationPending while its execution tree is still stopping.
type AdapterLifecycle interface {
	EnsureAccepted(context.Context, AdapterTask, AdapterSandbox, *AdapterCredential) error
	Observe(context.Context, AdapterTask, AdapterSandbox) (AdapterObservation, string, error)
	Cancel(context.Context, AdapterTask, AdapterSandbox) error
}

// RunReconciler turns a Run intent into one Environment allocation and drives
// its adapter lifecycle. sandboxd, reached through an adapter, owns all agent
// and declared-service processes inside the Environment.
type RunReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Adapters  map[string]AdapterLifecycle
	EventSink AdapterEventSink
}

// +kubebuilder:rbac:groups=swe.dev,resources=runs,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=swe.dev,resources=runs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=swe.dev,resources=runs/transcript,verbs=update
// +kubebuilder:rbac:groups=swe.dev,resources=environments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=swe.dev,resources=environments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=swe.dev,resources=projects,verbs=get;list;watch
// +kubebuilder:rbac:groups=swe.dev,resources=agentcredentialprofiles,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get

func (r *RunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var run platformv1alpha1.Run
	if err := r.Get(ctx, req.NamespacedName, &run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !run.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &run)
	}
	if terminalRunState(run.Status.State) {
		return r.cleanupTerminal(ctx, &run)
	}
	if run.Spec.Cancel && run.Status.EnvironmentRef == nil {
		ref, err := r.recoverEnvironmentReference(ctx, &run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if ref == nil {
			return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateCancelled, "Cancelled", "cancelled before allocation", false)
		}
		if ref.Ownership == platformv1alpha1.EnvironmentOwnershipClaimed {
			var recovered platformv1alpha1.Environment
			if err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: ref.Name}, &recovered); err != nil {
				return ctrl.Result{}, err
			}
			if unpromotedWarmClaim(&recovered, &run) {
				if err := r.releaseClaim(ctx, &run, &recovered); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateCancelled, "Cancelled", "cancelled before warm environment promotion", false)
			}
		}
		run.Status.EnvironmentRef = ref
		return ctrl.Result{Requeue: true}, r.setRunState(ctx, &run, platformv1alpha1.RunStateAllocating, "EnvironmentRecovered", fmt.Sprintf("recovered environment %s before cancellation", ref.Name), false)
	}
	if run.Status.EnvironmentRef == nil {
		result, done, err := r.ensureCredentialBinding(ctx, &run)
		if done || err != nil {
			return result, err
		}
	}
	if !controllerutil.ContainsFinalizer(&run, runFinalizer) {
		controllerutil.AddFinalizer(&run, runFinalizer)
		return ctrl.Result{}, r.Update(ctx, &run)
	}

	if run.Status.EnvironmentRef == nil {
		ref, err := r.recoverEnvironmentReference(ctx, &run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if ref == nil {
			ref, err = r.allocateEnvironment(ctx, &run)
		} else if ref.Ownership == platformv1alpha1.EnvironmentOwnershipClaimed {
			var recovered platformv1alpha1.Environment
			if getErr := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: ref.Name}, &recovered); getErr != nil {
				return ctrl.Result{}, getErr
			}
			if run.Spec.EnvironmentRef != "" {
				err = r.wakeExplicitClaim(ctx, &recovered)
			} else {
				err = r.promoteWarmEnvironment(ctx, &run, &recovered)
			}
		}
		if err != nil {
			if errors.Is(err, errExplicitEnvironmentClaimed) {
				return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateFailed, "EnvironmentUnavailable", err.Error(), false)
			}
			return ctrl.Result{}, err
		}
		run.Status.EnvironmentRef = ref
		return ctrl.Result{Requeue: true}, r.setRunState(ctx, &run, platformv1alpha1.RunStateAllocating, "EnvironmentAllocated", fmt.Sprintf("environment %s allocated", ref.Name), false)
	}

	env, err := r.getAllocatedEnvironment(ctx, &run)
	if err != nil {
		if apierrors.IsNotFound(err) || errors.Is(err, errAllocatedEnvironmentGone) {
			return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateFailed, "EnvironmentLost", err.Error(), false)
		}
		return ctrl.Result{}, err
	}
	if !env.Spec.Paused {
		if err := r.touchEnvironmentActivity(ctx, env); err != nil {
			return ctrl.Result{}, err
		}
	}
	if run.Spec.Cancel {
		if runMayHaveAccepted(&run) && !environmentFenced(env) {
			adapter := r.Adapters[run.Spec.Agent]
			if !environmentReachable(env) || adapter == nil {
				return r.requestEnvironmentFence(ctx, env)
			}
			if err := adapter.Cancel(ctx, adapterTask(&run), r.adapterSandbox(&run, env)); err != nil {
				if errors.Is(err, ErrAdapterCancellationPending) {
					return ctrl.Result{RequeueAfter: adapterPollInterval}, nil
				}
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateCancelled, "Cancelled", "cancellation completed", environmentReachable(env))
	}
	if env.Status.ObservedGeneration != env.Generation {
		return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateAllocating, "EnvironmentStatusStale", "environment status has not observed the current generation", false)
	}

	switch env.Status.Phase {
	case platformv1alpha1.EnvironmentPhasePaused, platformv1alpha1.EnvironmentPhaseResuming:
		return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStatePaused, "EnvironmentPaused", "managed processes stop; workspace and transcript are retained", false)
	case platformv1alpha1.EnvironmentPhaseFailed, platformv1alpha1.EnvironmentPhaseTerminated:
		message := fmt.Sprintf("environment phase is %s", env.Status.Phase)
		if condition := apiMeta.FindStatusCondition(env.Status.Conditions, platformv1alpha1.EnvironmentConditionReady); condition != nil && condition.Message != "" {
			message = condition.Reason + ": " + condition.Message
		}
		return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateFailed, "EnvironmentFailed", message, false)
	case platformv1alpha1.EnvironmentPhaseReady, platformv1alpha1.EnvironmentPhaseRunning:
		// Continue below.
	default:
		return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateAllocating, "EnvironmentNotReady", fmt.Sprintf("environment phase is %s", env.Status.Phase), false)
	}
	if !environmentReachable(env) {
		return ctrl.Result{RequeueAfter: adapterPollInterval}, r.setRunState(ctx, &run, platformv1alpha1.RunStateAllocating, "EnvironmentNotReachable", "sandboxd endpoint is not currently reachable", false)
	}

	if run.Status.State == platformv1alpha1.RunStateAllocating || run.Status.State == platformv1alpha1.RunStatePaused || run.Status.State == "" {
		return ctrl.Result{Requeue: true}, r.setRunState(ctx, &run, platformv1alpha1.RunStateEnvironmentReady, "EnvironmentReady", "sandboxd is ready", true)
	}
	adapter := r.Adapters[run.Spec.Agent]
	if adapter == nil {
		return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateFailed, "AdapterUnavailable", fmt.Sprintf("adapter %q is not registered", run.Spec.Agent), true)
	}
	if run.Status.State == platformv1alpha1.RunStateEnvironmentReady {
		if !acceptanceAttempted(&run) {
			return ctrl.Result{Requeue: true}, r.markAcceptanceAttempted(ctx, &run)
		}
		credential, reason, err := r.resolveCredential(ctx, &run)
		if err != nil {
			if reason == "ProfileNotFound" || reason == "SecretNotReady" {
				result, _, waitErr := r.waitForCredential(ctx, &run, reason, err.Error())
				return result, waitErr
			}
			if reason == "" {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, r.failCredential(ctx, &run, reason, err.Error())
		}
		if credential != nil {
			defer clear(credential.APIKey)
		}
		if err := adapter.EnsureAccepted(ctx, adapterTask(&run), r.adapterSandbox(&run, env), credential); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, r.setRunState(ctx, &run, platformv1alpha1.RunStateAdapterAccepted, "AdapterAccepted", "adapter accepted the task", true)
	}

	observation, message, err := adapter.Observe(ctx, adapterTask(&run), r.adapterSandbox(&run, env))
	if err != nil {
		return ctrl.Result{}, err
	}
	state := platformv1alpha1.RunStateAdapterAccepted
	switch observation {
	case AdapterObservationAccepted:
	case AdapterObservationRunning:
		state = platformv1alpha1.RunStateRunning
	case AdapterObservationNeedsInput:
		state = platformv1alpha1.RunStateNeedsInput
	case AdapterObservationSucceeded:
		state = platformv1alpha1.RunStateSucceeded
	case AdapterObservationFailed:
		state = platformv1alpha1.RunStateFailed
	default:
		return ctrl.Result{}, fmt.Errorf("adapter %q returned unknown observation %q", run.Spec.Agent, observation)
	}
	err = r.setRunState(ctx, &run, state, string(observation), message, true)
	if err != nil || terminalRunState(state) {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: adapterPollInterval}, nil
}

func (r *RunReconciler) apiReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client // Unit-test fallback only; managers set APIReader in SetupWithManager.
}

func (r *RunReconciler) ensureCredentialBinding(ctx context.Context, run *platformv1alpha1.Run) (ctrl.Result, bool, error) {
	if run.Spec.CredentialProfileRef == "" {
		condition := apiMeta.FindStatusCondition(run.Status.Conditions, runConditionCredentialProfileBound)
		if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "Credentialless" {
			apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: runConditionCredentialProfileBound, Status: metav1.ConditionTrue, Reason: "Credentialless", Message: "no credential profile selected", ObservedGeneration: run.Generation})
		}
		return ctrl.Result{}, false, nil
	}
	if run.Status.CredentialProfileRef != nil && run.Status.CredentialProfileRef.Name != run.Spec.CredentialProfileRef {
		return ctrl.Result{}, true, r.failCredential(ctx, run, "ProfileReplaced", "bound credential profile does not match the selected profile")
	}

	var profile platformv1alpha1.AgentCredentialProfile
	err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.CredentialProfileRef}, &profile)
	if apierrors.IsNotFound(err) {
		return r.waitForCredential(ctx, run, "ProfileNotFound", "credential profile is not ready")
	}
	if err != nil {
		return ctrl.Result{}, true, err
	}
	if run.Status.CredentialProfileRef != nil && run.Status.CredentialProfileRef.UID != profile.UID {
		return ctrl.Result{}, true, r.failCredential(ctx, run, "ProfileReplaced", "bound credential profile was replaced")
	}
	if profile.Spec.Adapter != run.Spec.Agent {
		return ctrl.Result{}, true, r.failCredential(ctx, run, "AdapterMismatch", "credential profile does not permit this adapter")
	}
	if profile.Spec.CredentialType != platformv1alpha1.AgentCredentialTypeAPIKey {
		return ctrl.Result{}, true, r.failCredential(ctx, run, "UnsupportedCredentialType", "credential profile type is unsupported")
	}
	if run.Status.CredentialProfileRef == nil {
		before := run.Status.DeepCopy()
		run.Status.CredentialProfileRef = &platformv1alpha1.RunCredentialProfileReference{Name: profile.Name, UID: profile.UID}
		apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: runConditionCredentialProfileBound, Status: metav1.ConditionTrue, Reason: "Bound", Message: "credential profile identity is bound", ObservedGeneration: run.Generation})
		if !reflect.DeepEqual(*before, run.Status) {
			return ctrl.Result{Requeue: true}, true, r.Status().Update(ctx, run)
		}
	}
	credential, reason, err := r.resolveCredential(ctx, run)
	if credential != nil {
		clear(credential.APIKey)
	}
	if err != nil {
		if reason == "SecretNotReady" {
			return r.waitForCredential(ctx, run, reason, "credential secret is not ready")
		}
		if reason == "" {
			return ctrl.Result{}, true, err
		}
		return ctrl.Result{}, true, r.failCredential(ctx, run, reason, err.Error())
	}
	condition := apiMeta.FindStatusCondition(run.Status.Conditions, runConditionCredentialProfileBound)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "Bound" {
		apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: runConditionCredentialProfileBound, Status: metav1.ConditionTrue, Reason: "Bound", Message: "credential profile identity is bound", ObservedGeneration: run.Generation})
		return ctrl.Result{Requeue: true}, true, r.Status().Update(ctx, run)
	}
	return ctrl.Result{}, false, nil
}

func (r *RunReconciler) waitForCredential(ctx context.Context, run *platformv1alpha1.Run, reason, message string) (ctrl.Result, bool, error) {
	condition := apiMeta.FindStatusCondition(run.Status.Conditions, runConditionCredentialProfileBound)
	if condition != nil && condition.Status == metav1.ConditionFalse && condition.Reason == reason {
		remaining := credentialReadyTimeout - time.Since(condition.LastTransitionTime.Time)
		if remaining <= 0 {
			return ctrl.Result{}, true, r.failCredential(ctx, run, reason, message)
		}
		return ctrl.Result{RequeueAfter: remaining}, true, nil
	}
	apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: runConditionCredentialProfileBound, Status: metav1.ConditionFalse, Reason: reason, Message: message, ObservedGeneration: run.Generation})
	return ctrl.Result{RequeueAfter: credentialReadyTimeout}, true, r.Status().Update(ctx, run)
}

func (r *RunReconciler) failCredential(ctx context.Context, run *platformv1alpha1.Run, reason, message string) error {
	apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: runConditionCredentialProfileBound, Status: metav1.ConditionFalse, Reason: reason, Message: message, ObservedGeneration: run.Generation})
	return r.setRunState(ctx, run, platformv1alpha1.RunStateFailed, reason, message, run.Status.EnvironmentRef != nil)
}

func (r *RunReconciler) resolveCredential(ctx context.Context, run *platformv1alpha1.Run) (*AdapterCredential, string, error) {
	if run.Spec.CredentialProfileRef == "" {
		return nil, "", nil
	}
	bound := run.Status.CredentialProfileRef
	if bound == nil {
		return nil, "MalformedSecret", errors.New("credential profile is not bound")
	}
	var profile platformv1alpha1.AgentCredentialProfile
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: bound.Name}, &profile); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, "ProfileNotFound", errors.New("bound credential profile is not ready")
		}
		return nil, "", err
	}
	if profile.UID != bound.UID {
		return nil, "ProfileReplaced", errors.New("bound credential profile was replaced")
	}
	if profile.Spec.Adapter != run.Spec.Agent {
		return nil, "AdapterMismatch", errors.New("credential profile does not permit this adapter")
	}
	if profile.Spec.CredentialType != platformv1alpha1.AgentCredentialTypeAPIKey {
		return nil, "UnsupportedCredentialType", errors.New("credential profile type is unsupported")
	}
	var secret corev1.Secret
	key := types.NamespacedName{Namespace: run.Namespace, Name: platformv1alpha1.AgentCredentialSecretName(profile.UID)}
	if err := r.apiReader().Get(ctx, key, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, "SecretNotReady", errors.New("credential secret is not ready")
		}
		return nil, "", err
	}
	defer func() {
		for _, value := range secret.Data {
			clear(value)
		}
	}()
	owner := metav1.GetControllerOf(&secret)
	if owner == nil || owner.APIVersion != platformv1alpha1.GroupVersion.String() || owner.Kind != "AgentCredentialProfile" || owner.Name != profile.Name || owner.UID != profile.UID {
		return nil, "ForeignSecret", errors.New("credential secret is not controlled by the bound profile")
	}
	value, ok := secret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey]
	if secret.Type != platformv1alpha1.AgentCredentialAPIKeySecretType || len(secret.Data) != 1 || !ok || len(value) == 0 || len(value) > platformv1alpha1.AgentCredentialAPIKeyMaxBytes || !utf8.Valid(value) || bytesContainNUL(value) {
		return nil, "MalformedSecret", errors.New("credential secret is malformed")
	}
	return &AdapterCredential{Type: platformv1alpha1.AgentCredentialTypeAPIKey, APIKey: append([]byte(nil), value...)}, "", nil
}

func bytesContainNUL(value []byte) bool {
	for _, b := range value {
		if b == 0 {
			return true
		}
	}
	return false
}

func (r *RunReconciler) allocateEnvironment(ctx context.Context, run *platformv1alpha1.Run) (*platformv1alpha1.RunEnvironmentReference, error) {
	if run.Spec.EnvironmentRef != "" {
		var env platformv1alpha1.Environment
		key := types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.EnvironmentRef}
		if err := r.Get(ctx, key, &env); err != nil {
			return nil, fmt.Errorf("get claimed environment %q: %w", run.Spec.EnvironmentRef, err)
		}
		if owner := metav1.GetControllerOf(&env); owner != nil {
			return nil, fmt.Errorf("environment %q is controller-owned by %s %q and cannot be claimed", env.Name, owner.Kind, owner.Name)
		}
		claim := &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID}
		if env.Status.ClaimedBy != nil && (env.Status.ClaimedBy.Name != claim.Name || env.Status.ClaimedBy.UID != claim.UID) {
			return nil, fmt.Errorf("%w: environment %q is claimed by run %s", errExplicitEnvironmentClaimed, env.Name, env.Status.ClaimedBy.Name)
		}
		if env.Status.ClaimedBy == nil {
			env.Status.ClaimedBy = claim
			if err := r.Status().Update(ctx, &env); err != nil {
				return nil, err
			}
		}
		if env.Spec.Paused {
			before := env.DeepCopy()
			env.Spec.Paused = false
			if err := r.Patch(ctx, &env, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return nil, err
			}
		}
		return &platformv1alpha1.RunEnvironmentReference{Name: env.Name, UID: env.UID, Ownership: platformv1alpha1.EnvironmentOwnershipClaimed}, nil
	}

	template, err := r.resolveTemplate(ctx, run)
	if err != nil {
		return nil, err
	}
	if template == "" {
		return nil, fmt.Errorf("run has no environment template")
	}
	if ref, err := r.claimWarmEnvironment(ctx, run, template); err != nil || ref != nil {
		return ref, err
	}
	name := "run-" + string(run.UID)
	key := types.NamespacedName{Namespace: run.Namespace, Name: name}
	var env platformv1alpha1.Environment
	err = r.Get(ctx, key, &env)
	if apierrors.IsNotFound(err) {
		env = platformv1alpha1.Environment{
			ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: name},
			Spec:       platformv1alpha1.EnvironmentSpec{ProjectRef: run.Spec.ProjectRef, TemplateRef: template},
		}
		if err := controllerutil.SetControllerReference(run, &env, r.Scheme); err != nil {
			return nil, err
		}
		if err := r.Create(ctx, &env); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return nil, err
			}
			if err := r.Get(ctx, key, &env); err != nil {
				return nil, err
			}
		}
	} else if err != nil {
		return nil, err
	}
	if !exactControllerOwner(&env, platformv1alpha1.GroupVersion.String(), "Run", run.Name, run.UID) {
		return nil, fmt.Errorf("deterministic environment %q is not owned by run UID %s", env.Name, run.UID)
	}
	return &platformv1alpha1.RunEnvironmentReference{Name: env.Name, UID: env.UID, Ownership: platformv1alpha1.EnvironmentOwnershipOwned}, nil
}

func (r *RunReconciler) getAllocatedEnvironment(ctx context.Context, run *platformv1alpha1.Run) (*platformv1alpha1.Environment, error) {
	ref := run.Status.EnvironmentRef
	var env platformv1alpha1.Environment
	if err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: ref.Name}, &env); err != nil {
		return nil, err
	}
	if env.UID != ref.UID {
		return nil, fmt.Errorf("%w: environment %q was replaced (wanted UID %s, got %s)", errAllocatedEnvironmentGone, env.Name, ref.UID, env.UID)
	}
	switch ref.Ownership {
	case platformv1alpha1.EnvironmentOwnershipOwned:
		if !exactControllerOwner(&env, platformv1alpha1.GroupVersion.String(), "Run", run.Name, run.UID) {
			return nil, fmt.Errorf("%w: environment %q is not owned by run UID %s", errAllocatedEnvironmentGone, env.Name, run.UID)
		}
	case platformv1alpha1.EnvironmentOwnershipClaimed:
		if metav1.GetControllerOf(&env) != nil || env.Status.ClaimedBy == nil || env.Status.ClaimedBy.Name != run.Name || env.Status.ClaimedBy.UID != run.UID {
			return nil, fmt.Errorf("%w: environment %q claim does not match run UID %s", errAllocatedEnvironmentGone, env.Name, run.UID)
		}
	default:
		return nil, fmt.Errorf("%w: environment %q has unknown ownership", errAllocatedEnvironmentGone, env.Name)
	}
	return &env, nil
}

// recoverEnvironmentReference closes the gap where allocation succeeded but
// its Run status update was lost. Only exact UID ownership/claims are adopted.
func (r *RunReconciler) recoverEnvironmentReference(ctx context.Context, run *platformv1alpha1.Run) (*platformv1alpha1.RunEnvironmentReference, error) {
	if run.Spec.EnvironmentRef != "" {
		var env platformv1alpha1.Environment
		if err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.EnvironmentRef}, &env); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}
		if env.Status.ClaimedBy != nil && env.Status.ClaimedBy.Name == run.Name && env.Status.ClaimedBy.UID == run.UID {
			if metav1.GetControllerOf(&env) != nil {
				return nil, nil
			}
			return &platformv1alpha1.RunEnvironmentReference{Name: env.Name, UID: env.UID, Ownership: platformv1alpha1.EnvironmentOwnershipClaimed}, nil
		}
		return nil, nil
	}
	var environments platformv1alpha1.EnvironmentList
	if err := r.List(ctx, &environments, client.InNamespace(run.Namespace)); err != nil {
		return nil, err
	}
	for i := range environments.Items {
		env := &environments.Items[i]
		if env.Status.ClaimedBy != nil && env.Status.ClaimedBy.Name == run.Name && env.Status.ClaimedBy.UID == run.UID {
			return &platformv1alpha1.RunEnvironmentReference{Name: env.Name, UID: env.UID, Ownership: platformv1alpha1.EnvironmentOwnershipClaimed}, nil
		}
	}
	var env platformv1alpha1.Environment
	if err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: "run-" + string(run.UID)}, &env); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if !exactControllerOwner(&env, platformv1alpha1.GroupVersion.String(), "Run", run.Name, run.UID) {
		return nil, nil
	}
	return &platformv1alpha1.RunEnvironmentReference{Name: env.Name, UID: env.UID, Ownership: platformv1alpha1.EnvironmentOwnershipOwned}, nil
}

func (r *RunReconciler) resolveTemplate(ctx context.Context, run *platformv1alpha1.Run) (string, error) {
	if run.Spec.TemplateRef != "" {
		return run.Spec.TemplateRef, nil
	}
	if run.Spec.ProjectRef == "" {
		return "", nil
	}
	var project platformv1alpha1.Project
	if err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.ProjectRef}, &project); err != nil {
		return "", fmt.Errorf("get project %q: %w", run.Spec.ProjectRef, err)
	}
	return project.Spec.TemplateRef, nil
}

func (r *RunReconciler) claimWarmEnvironment(ctx context.Context, run *platformv1alpha1.Run, template string) (*platformv1alpha1.RunEnvironmentReference, error) {
	var environments platformv1alpha1.EnvironmentList
	if err := r.List(ctx, &environments, client.InNamespace(run.Namespace), client.MatchingLabels{warmPoolLabel: template}); err != nil {
		return nil, fmt.Errorf("list warm environments: %w", err)
	}
	for i := range environments.Items {
		env := &environments.Items[i]
		owner := metav1.GetControllerOf(env)
		if env.Spec.TemplateRef != template || env.Spec.Paused || !platformv1alpha1.IsEnvironmentReady(env) || env.Status.ClaimedBy != nil || owner == nil || owner.Kind != "EnvironmentTemplate" || owner.Name != template {
			continue
		}
		now := metav1.Now()
		env.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID}
		env.Status.LastActiveAt = &now
		if err := r.Status().Update(ctx, env); err != nil {
			if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("claim warm environment %q: %w", env.Name, err)
		}
		if err := r.promoteWarmEnvironment(ctx, run, env); err != nil {
			return nil, err
		}
		return &platformv1alpha1.RunEnvironmentReference{Name: env.Name, UID: env.UID, Ownership: platformv1alpha1.EnvironmentOwnershipClaimed}, nil
	}
	return nil, nil
}

func (r *RunReconciler) promoteWarmEnvironment(ctx context.Context, run *platformv1alpha1.Run, env *platformv1alpha1.Environment) error {
	before := env.DeepCopy()
	delete(env.Labels, warmPoolLabel)
	owners := env.OwnerReferences[:0]
	for _, owner := range env.OwnerReferences {
		if owner.Controller != nil && *owner.Controller && owner.Kind == "EnvironmentTemplate" && owner.Name == env.Spec.TemplateRef {
			continue
		}
		owners = append(owners, owner)
	}
	env.OwnerReferences = owners
	env.Spec.ProjectRef = run.Spec.ProjectRef
	env.Spec.Paused = false
	if !reflect.DeepEqual(before.ObjectMeta, env.ObjectMeta) || !reflect.DeepEqual(before.Spec, env.Spec) {
		if err := r.Patch(ctx, env, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return fmt.Errorf("promote warm environment %q: %w", env.Name, err)
		}
	}
	// A warm pod may still represent the generic, project-less environment.
	// Withdraw readiness before recording the allocation on the Run so the
	// adapter cannot start until Environment reconciliation has applied the
	// project and republished the current sandboxd endpoint. Do this on recovery
	// too, closing a crash between promotion and the Run status write.
	if env.Status.Phase == platformv1alpha1.EnvironmentPhaseReady || env.Status.Phase == platformv1alpha1.EnvironmentPhaseRunning {
		applyEnvironmentStatus(env, platformv1alpha1.EnvironmentPhaseSetup, "", "", "SetupInProgress", "warm environment is being configured for its project", env.Status.LastActiveAt)
		if err := r.Status().Update(ctx, env); err != nil {
			return fmt.Errorf("withdraw warm environment %q readiness: %w", env.Name, err)
		}
	}
	return nil
}

func (r *RunReconciler) touchEnvironmentActivity(ctx context.Context, env *platformv1alpha1.Environment) error {
	key := client.ObjectKeyFromObject(env)
	expectedUID := env.UID
	var updated platformv1alpha1.Environment
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var current platformv1alpha1.Environment
		if err := r.Get(ctx, key, &current); err != nil {
			return err
		}
		if current.UID != expectedUID {
			return errAllocatedEnvironmentGone
		}
		now := metav1.Now()
		if current.Status.LastActiveAt != nil && now.Sub(current.Status.LastActiveAt.Time) < adapterPollInterval {
			updated = current
			return nil
		}
		current.Status.LastActiveAt = &now
		if err := r.Status().Update(ctx, &current); err != nil {
			return err
		}
		updated = current
		return nil
	})
	if err == nil {
		*env = *updated.DeepCopy()
	}
	return err
}

func (r *RunReconciler) wakeExplicitClaim(ctx context.Context, env *platformv1alpha1.Environment) error {
	if !env.Spec.Paused {
		return nil
	}
	before := env.DeepCopy()
	env.Spec.Paused = false
	return r.Patch(ctx, env, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func environmentReachable(env *platformv1alpha1.Environment) bool {
	return platformv1alpha1.IsEnvironmentReady(env) && env.Status.PodName != "" && env.Status.Endpoints.Sandboxd != ""
}

func exactControllerOwner(object metav1.Object, apiVersion, kind, name string, uid types.UID) bool {
	owner := metav1.GetControllerOf(object)
	return owner != nil && owner.APIVersion == apiVersion && owner.Kind == kind && owner.Name == name && owner.UID == uid
}

func unpromotedWarmClaim(env *platformv1alpha1.Environment, run *platformv1alpha1.Run) bool {
	template := env.Labels[warmPoolLabel]
	owner := metav1.GetControllerOf(env)
	return template != "" && env.Spec.TemplateRef == template && env.Status.ClaimedBy != nil &&
		env.Status.ClaimedBy.Name == run.Name && env.Status.ClaimedBy.UID == run.UID &&
		owner != nil && owner.APIVersion == platformv1alpha1.GroupVersion.String() && owner.Kind == "EnvironmentTemplate" && owner.Name == template
}

func environmentFenced(env *platformv1alpha1.Environment) bool {
	return env.Spec.Paused && env.Status.Phase == platformv1alpha1.EnvironmentPhasePaused && env.Status.PodName == "" && env.Status.Endpoints.Sandboxd == ""
}

func runAccepted(run *platformv1alpha1.Run) bool {
	condition := apiMeta.FindStatusCondition(run.Status.Conditions, runConditionAdapterAccepted)
	return condition != nil && condition.Status == metav1.ConditionTrue
}

func acceptanceAttempted(run *platformv1alpha1.Run) bool {
	condition := apiMeta.FindStatusCondition(run.Status.Conditions, runConditionAdapterAcceptanceAttempted)
	return condition != nil && condition.Status == metav1.ConditionTrue
}

func runMayHaveAccepted(run *platformv1alpha1.Run) bool {
	return acceptanceAttempted(run) || runAccepted(run)
}

func (r *RunReconciler) markAcceptanceAttempted(ctx context.Context, run *platformv1alpha1.Run) error {
	before := run.Status.DeepCopy()
	apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue,
		Reason: "AcceptancePending", Message: "adapter acceptance may be attempted idempotently",
		ObservedGeneration: run.Generation,
	})
	if reflect.DeepEqual(*before, run.Status) {
		return nil
	}
	return r.Status().Update(ctx, run)
}

func (r *RunReconciler) requestEnvironmentFence(ctx context.Context, env *platformv1alpha1.Environment) (ctrl.Result, error) {
	if !env.Spec.Paused {
		env.Spec.Paused = true
		if err := r.Update(ctx, env); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: adapterPollInterval}, nil
}

func (r *RunReconciler) cleanupTerminal(ctx context.Context, run *platformv1alpha1.Run) (ctrl.Result, error) {
	if run.Status.EnvironmentRef == nil {
		return ctrl.Result{}, r.setEnvironmentReadyCondition(ctx, run, false, "EnvironmentReleased", "Run has no allocated environment")
	}
	if run.Status.EnvironmentRef.Ownership == platformv1alpha1.EnvironmentOwnershipClaimed {
		condition := apiMeta.FindStatusCondition(run.Status.Conditions, runConditionEnvironmentReady)
		if condition != nil && condition.Status == metav1.ConditionFalse && condition.Reason == "EnvironmentReleased" {
			return ctrl.Result{}, r.setEnvironmentReadyCondition(ctx, run, false, "EnvironmentReleased", "claimed environment was released")
		}
	}
	env, err := r.getAllocatedEnvironment(ctx, run)
	if apierrors.IsNotFound(err) || errors.Is(err, errAllocatedEnvironmentGone) {
		return ctrl.Result{}, r.setEnvironmentReadyCondition(ctx, run, false, "EnvironmentLost", err.Error())
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if !environmentReachable(env) {
		if err := r.setEnvironmentReadyCondition(ctx, run, false, "EnvironmentNotReady", "allocated environment is not reachable"); err != nil {
			return ctrl.Result{}, err
		}
	}
	if runMayHaveAccepted(run) && !environmentFenced(env) {
		adapter := r.Adapters[run.Spec.Agent]
		if !environmentReachable(env) || adapter == nil {
			return r.requestEnvironmentFence(ctx, env)
		}
		if err := adapter.Cancel(ctx, adapterTask(run), r.adapterSandbox(run, env)); err != nil {
			if errors.Is(err, ErrAdapterCancellationPending) {
				return ctrl.Result{RequeueAfter: adapterPollInterval}, nil
			}
			return ctrl.Result{}, err
		}
	}
	if run.Status.EnvironmentRef.Ownership == platformv1alpha1.EnvironmentOwnershipOwned {
		if !env.Spec.Paused {
			env.Spec.Paused = true
			return ctrl.Result{}, r.Update(ctx, env)
		}
		if environmentFenced(env) {
			return ctrl.Result{}, r.setEnvironmentReadyCondition(ctx, run, false, "EnvironmentFenced", "owned environment is paused and fenced")
		}
		return ctrl.Result{}, nil
	}
	if err := r.releaseClaim(ctx, run, env); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.setEnvironmentReadyCondition(ctx, run, false, "EnvironmentReleased", "claimed environment was released")
}

func (r *RunReconciler) finalize(ctx context.Context, run *platformv1alpha1.Run) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(run, runFinalizer) {
		return ctrl.Result{}, nil
	}
	if run.Status.EnvironmentRef == nil {
		ref, err := r.recoverEnvironmentReference(ctx, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if ref != nil && ref.Ownership == platformv1alpha1.EnvironmentOwnershipClaimed {
			var recovered platformv1alpha1.Environment
			if err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: ref.Name}, &recovered); err != nil {
				return ctrl.Result{}, err
			}
			if unpromotedWarmClaim(&recovered, run) {
				if err := r.releaseClaim(ctx, run, &recovered); err != nil {
					return ctrl.Result{}, err
				}
				ref = nil
			}
		}
		run.Status.EnvironmentRef = ref
	}
	if run.Status.EnvironmentRef != nil {
		env, err := r.getAllocatedEnvironment(ctx, run)
		if err != nil && !apierrors.IsNotFound(err) && !errors.Is(err, errAllocatedEnvironmentGone) {
			return ctrl.Result{}, err
		}
		if err == nil {
			if runMayHaveAccepted(run) && !environmentFenced(env) {
				adapter := r.Adapters[run.Spec.Agent]
				if !environmentReachable(env) || adapter == nil {
					return r.requestEnvironmentFence(ctx, env)
				}
				if err := adapter.Cancel(ctx, adapterTask(run), r.adapterSandbox(run, env)); err != nil {
					if errors.Is(err, ErrAdapterCancellationPending) {
						return ctrl.Result{RequeueAfter: adapterPollInterval}, nil
					}
					return ctrl.Result{}, err
				}
			}
			if run.Status.EnvironmentRef.Ownership == platformv1alpha1.EnvironmentOwnershipClaimed {
				if err := r.releaseClaim(ctx, run, env); err != nil {
					return ctrl.Result{}, err
				}
			}
		}
	}
	controllerutil.RemoveFinalizer(run, runFinalizer)
	return ctrl.Result{}, r.Update(ctx, run)
}

func (r *RunReconciler) releaseClaim(ctx context.Context, run *platformv1alpha1.Run, env *platformv1alpha1.Environment) error {
	if env.Status.ClaimedBy == nil || env.Status.ClaimedBy.Name != run.Name || env.Status.ClaimedBy.UID != run.UID {
		return nil
	}
	env.Status.ClaimedBy = nil
	return r.Status().Update(ctx, env)
}

func (r *RunReconciler) setRunState(ctx context.Context, run *platformv1alpha1.Run, state platformv1alpha1.RunState, reason, message string, environmentReady bool) error {
	before := run.Status.DeepCopy()
	run.Status.State = state
	run.Status.ObservedGeneration = run.Generation
	environmentReason, environmentMessage := reason, message
	if environmentReady {
		environmentReason, environmentMessage = "EnvironmentReady", "sandboxd is ready"
	}
	apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type: runConditionEnvironmentReady, Status: boolConditionStatus(environmentReady), Reason: environmentReason, Message: environmentMessage, ObservedGeneration: run.Generation,
	})
	adapterAccepted := runAccepted(run) || state == platformv1alpha1.RunStateAdapterAccepted || state == platformv1alpha1.RunStateRunning || state == platformv1alpha1.RunStateNeedsInput || state == platformv1alpha1.RunStateSucceeded || (state == platformv1alpha1.RunStateFailed && reason == string(AdapterObservationFailed))
	apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type: runConditionAdapterAccepted, Status: boolConditionStatus(adapterAccepted), Reason: reason, Message: message, ObservedGeneration: run.Generation,
	})
	if reflect.DeepEqual(*before, run.Status) {
		return nil
	}
	return r.Status().Update(ctx, run)
}

func (r *RunReconciler) setEnvironmentReadyCondition(ctx context.Context, run *platformv1alpha1.Run, ready bool, reason, message string) error {
	before := run.Status.DeepCopy()
	apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type: runConditionEnvironmentReady, Status: boolConditionStatus(ready), Reason: reason, Message: message, ObservedGeneration: run.Generation,
	})
	if reflect.DeepEqual(*before, run.Status) {
		return nil
	}
	return r.Status().Update(ctx, run)
}

func boolConditionStatus(value bool) metav1.ConditionStatus {
	if value {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

func terminalRunState(state platformv1alpha1.RunState) bool {
	return state == platformv1alpha1.RunStateSucceeded || state == platformv1alpha1.RunStateFailed || state == platformv1alpha1.RunStateCancelled
}

func adapterTask(run *platformv1alpha1.Run) AdapterTask {
	return AdapterTask{ID: string(run.UID), Prompt: run.Spec.Prompt}
}

func (r *RunReconciler) adapterSandbox(run *platformv1alpha1.Run, env *platformv1alpha1.Environment) AdapterSandbox {
	sandbox := AdapterSandbox{EnvironmentName: env.Name, EnvironmentUID: env.UID,
		DialProcess: func(ctx context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
			current, err := r.getAllocatedEnvironment(ctx, run)
			if err != nil {
				return nil, nil, err
			}
			return (sandboxclient.Connector{Reader: r.Client}).DialProcess(ctx, current.Namespace, current.Name, current.UID)
		}}
	if r.EventSink != nil {
		sandbox.EmitEvent = func(ctx context.Context, event AdapterEvent) error {
			return r.EventSink.Append(ctx, run.Namespace, run.Name, event)
		}
	}
	return sandbox
}

// SetupWithManager registers Run watches. Owned Environments enqueue through
// Owns; claimed Environments enqueue Runs selected by their exact reference.
func (r *RunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.APIReader = mgr.GetAPIReader()
	environmentEvents := builder.WithPredicates(predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return true },
		UpdateFunc: func(update event.UpdateEvent) bool {
			oldEnvironment, oldOK := update.ObjectOld.(*platformv1alpha1.Environment)
			newEnvironment, newOK := update.ObjectNew.(*platformv1alpha1.Environment)
			return !oldOK || !newOK || runRelevantEnvironmentUpdate(oldEnvironment, newEnvironment)
		},
	})
	return ctrl.NewControllerManagedBy(mgr).
		For(&platformv1alpha1.Run{}, builder.WithPredicates()).
		Owns(&platformv1alpha1.Environment{}, environmentEvents).
		Watches(&platformv1alpha1.Environment{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, object client.Object) []ctrl.Request {
			var runs platformv1alpha1.RunList
			if err := r.List(ctx, &runs, client.InNamespace(object.GetNamespace())); err != nil {
				return nil
			}
			requests := make([]ctrl.Request, 0, 1)
			for i := range runs.Items {
				ref := runs.Items[i].Status.EnvironmentRef
				if ref != nil && ref.Name == object.GetName() && ref.UID == object.GetUID() {
					requests = append(requests, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(&runs.Items[i])})
				}
			}
			return requests
		}), environmentEvents).
		Complete(r)
}

func runRelevantEnvironmentUpdate(oldEnvironment, newEnvironment *platformv1alpha1.Environment) bool {
	oldStatus := oldEnvironment.Status.DeepCopy()
	newStatus := newEnvironment.Status.DeepCopy()
	oldStatus.LastActiveAt = nil
	newStatus.LastActiveAt = nil
	return oldEnvironment.Generation != newEnvironment.Generation ||
		oldEnvironment.UID != newEnvironment.UID ||
		!reflect.DeepEqual(oldEnvironment.DeletionTimestamp, newEnvironment.DeletionTimestamp) ||
		!reflect.DeepEqual(oldEnvironment.OwnerReferences, newEnvironment.OwnerReferences) ||
		!reflect.DeepEqual(oldEnvironment.Labels, newEnvironment.Labels) ||
		!reflect.DeepEqual(*oldStatus, *newStatus)
}
