package controllers

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/sandboxclient"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const runFinalizer = "swe.dev/run-cleanup"

const adapterPollInterval = 2 * time.Second

var errAllocatedEnvironmentGone = errors.New("allocated environment is gone or no longer claimed by this run")

const (
	runConditionEnvironmentReady           = "EnvironmentReady"
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

// AdapterSandbox is the backend-neutral handle exposed to adapters. Adapters
// use sandboxd and never inspect pods, containers, VMs, PIDs, or OS signals.
type AdapterSandbox struct {
	EnvironmentName  string
	EnvironmentUID   types.UID
	SandboxdEndpoint string
	DialProcess      func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error)
}

// AdapterLifecycle translates one agent's execution model into normalized Run
// lifecycle events. Every operation must be idempotent. EnsureAccepted may be
// repeated after an uncertain response or environment resume; Cancel succeeds
// when work is already absent or terminal.
type AdapterLifecycle interface {
	EnsureAccepted(context.Context, AdapterTask, AdapterSandbox) error
	Observe(context.Context, AdapterTask, AdapterSandbox) (AdapterObservation, string, error)
	Cancel(context.Context, AdapterTask, AdapterSandbox) error
}

// RunReconciler turns a Run intent into one Environment allocation and drives
// its adapter lifecycle. sandboxd, reached through an adapter, owns all agent
// and declared-service processes inside the Environment.
type RunReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Adapters map[string]AdapterLifecycle
}

// +kubebuilder:rbac:groups=swe.dev,resources=runs,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=swe.dev,resources=runs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=swe.dev,resources=environments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=swe.dev,resources=environments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=swe.dev,resources=projects,verbs=get;list;watch

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
			return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateCancelled, "Cancelled", "cancelled before allocation")
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
				return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateCancelled, "Cancelled", "cancelled before warm environment promotion")
			}
		}
		run.Status.EnvironmentRef = ref
		return ctrl.Result{Requeue: true}, r.setRunState(ctx, &run, platformv1alpha1.RunStateAllocating, "EnvironmentRecovered", fmt.Sprintf("recovered environment %s before cancellation", ref.Name))
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
			return ctrl.Result{}, err
		}
		run.Status.EnvironmentRef = ref
		return ctrl.Result{Requeue: true}, r.setRunState(ctx, &run, platformv1alpha1.RunStateAllocating, "EnvironmentAllocated", fmt.Sprintf("environment %s allocated", ref.Name))
	}

	env, err := r.getAllocatedEnvironment(ctx, &run)
	if err != nil {
		if apierrors.IsNotFound(err) || errors.Is(err, errAllocatedEnvironmentGone) {
			return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateFailed, "EnvironmentLost", err.Error())
		}
		return ctrl.Result{}, err
	}
	if run.Spec.Cancel {
		if runMayHaveAccepted(&run) && !environmentFenced(env) {
			adapter := r.Adapters[run.Spec.Agent]
			if !environmentReachable(env) || adapter == nil {
				return r.requestEnvironmentFence(ctx, env)
			}
			if err := adapter.Cancel(ctx, adapterTask(&run), r.adapterSandbox(&run, env)); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateCancelled, "Cancelled", "cancellation completed")
	}

	switch env.Status.Phase {
	case platformv1alpha1.EnvironmentPhasePaused, platformv1alpha1.EnvironmentPhaseResuming:
		return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStatePaused, "EnvironmentPaused", "managed processes stop; workspace and transcript are retained")
	case platformv1alpha1.EnvironmentPhaseFailed, platformv1alpha1.EnvironmentPhaseTerminated:
		return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateFailed, "EnvironmentFailed", fmt.Sprintf("environment phase is %s", env.Status.Phase))
	case platformv1alpha1.EnvironmentPhaseReady, platformv1alpha1.EnvironmentPhaseRunning:
		// Continue below.
	default:
		return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateAllocating, "EnvironmentNotReady", fmt.Sprintf("environment phase is %s", env.Status.Phase))
	}
	if !environmentReachable(env) {
		return ctrl.Result{RequeueAfter: adapterPollInterval}, r.setRunState(ctx, &run, platformv1alpha1.RunStateAllocating, "EnvironmentNotReachable", "sandboxd endpoint is not currently reachable")
	}

	if run.Status.State == platformv1alpha1.RunStateAllocating || run.Status.State == platformv1alpha1.RunStatePaused || run.Status.State == "" {
		return ctrl.Result{Requeue: true}, r.setRunState(ctx, &run, platformv1alpha1.RunStateEnvironmentReady, "EnvironmentReady", "sandboxd is ready")
	}
	adapter := r.Adapters[run.Spec.Agent]
	if adapter == nil {
		return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateFailed, "AdapterUnavailable", fmt.Sprintf("adapter %q is not registered", run.Spec.Agent))
	}
	if run.Status.State == platformv1alpha1.RunStateEnvironmentReady {
		if !acceptanceAttempted(&run) {
			return ctrl.Result{Requeue: true}, r.markAcceptanceAttempted(ctx, &run)
		}
		if err := adapter.EnsureAccepted(ctx, adapterTask(&run), r.adapterSandbox(&run, env)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, r.setRunState(ctx, &run, platformv1alpha1.RunStateAdapterAccepted, "AdapterAccepted", "adapter accepted the task")
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
		if err := r.touchEnvironmentActivity(ctx, env); err != nil {
			return ctrl.Result{}, err
		}
	case AdapterObservationNeedsInput:
		state = platformv1alpha1.RunStateNeedsInput
	case AdapterObservationSucceeded:
		state = platformv1alpha1.RunStateSucceeded
	case AdapterObservationFailed:
		state = platformv1alpha1.RunStateFailed
	default:
		return ctrl.Result{}, fmt.Errorf("adapter %q returned unknown observation %q", run.Spec.Agent, observation)
	}
	err = r.setRunState(ctx, &run, state, string(observation), message)
	if err != nil || terminalRunState(state) {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: adapterPollInterval}, nil
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
			return nil, fmt.Errorf("environment %q is claimed by run %s", env.Name, env.Status.ClaimedBy.Name)
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
		if env.Spec.TemplateRef != template || env.Spec.Paused || env.Status.Phase != platformv1alpha1.EnvironmentPhaseReady || env.Status.ClaimedBy != nil || owner == nil || owner.Kind != "EnvironmentTemplate" || owner.Name != template {
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
	if env.Status.Phase == platformv1alpha1.EnvironmentPhaseReady {
		env.Status.Phase = platformv1alpha1.EnvironmentPhaseSetup
		env.Status.PodName = ""
		env.Status.Endpoints = platformv1alpha1.EnvironmentEndpoints{}
		if err := r.Status().Update(ctx, env); err != nil {
			return fmt.Errorf("withdraw warm environment %q readiness: %w", env.Name, err)
		}
	}
	return nil
}

func (r *RunReconciler) touchEnvironmentActivity(ctx context.Context, env *platformv1alpha1.Environment) error {
	now := metav1.Now()
	env.Status.LastActiveAt = &now
	return r.Status().Update(ctx, env)
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
	return (env.Status.Phase == platformv1alpha1.EnvironmentPhaseReady || env.Status.Phase == platformv1alpha1.EnvironmentPhaseRunning) &&
		env.Status.PodName != "" && env.Status.Endpoints.Sandboxd != ""
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
		return ctrl.Result{}, nil
	}
	env, err := r.getAllocatedEnvironment(ctx, run)
	if apierrors.IsNotFound(err) || errors.Is(err, errAllocatedEnvironmentGone) {
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if runMayHaveAccepted(run) && !environmentFenced(env) {
		adapter := r.Adapters[run.Spec.Agent]
		if !environmentReachable(env) || adapter == nil {
			return r.requestEnvironmentFence(ctx, env)
		}
		if err := adapter.Cancel(ctx, adapterTask(run), r.adapterSandbox(run, env)); err != nil {
			return ctrl.Result{}, err
		}
	}
	if run.Status.EnvironmentRef.Ownership == platformv1alpha1.EnvironmentOwnershipOwned {
		if !env.Spec.Paused {
			env.Spec.Paused = true
			return ctrl.Result{}, r.Update(ctx, env)
		}
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, r.releaseClaim(ctx, run, env)
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

func (r *RunReconciler) setRunState(ctx context.Context, run *platformv1alpha1.Run, state platformv1alpha1.RunState, reason, message string) error {
	before := run.Status.DeepCopy()
	run.Status.State = state
	run.Status.ObservedGeneration = run.Generation
	environmentReady := state == platformv1alpha1.RunStateEnvironmentReady || state == platformv1alpha1.RunStateAdapterAccepted || state == platformv1alpha1.RunStateRunning || state == platformv1alpha1.RunStateNeedsInput || state == platformv1alpha1.RunStateSucceeded
	apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type: runConditionEnvironmentReady, Status: boolConditionStatus(environmentReady), Reason: reason, Message: message, ObservedGeneration: run.Generation,
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
	return AdapterSandbox{EnvironmentName: env.Name, EnvironmentUID: env.UID, SandboxdEndpoint: env.Status.Endpoints.Sandboxd,
		DialProcess: func(ctx context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
			current, err := r.getAllocatedEnvironment(ctx, run)
			if err != nil {
				return nil, nil, err
			}
			return sandboxclient.DialProcess(ctx, r.Client, current.Namespace, current.Name, current.UID)
		}}
}

// SetupWithManager registers Run watches. Owned Environments enqueue through
// Owns; claimed Environments enqueue Runs selected by their exact reference.
func (r *RunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&platformv1alpha1.Run{}, builder.WithPredicates()).
		Owns(&platformv1alpha1.Environment{}).
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
		})).
		Complete(r)
}
