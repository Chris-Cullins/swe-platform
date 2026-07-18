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
)

const runFinalizer = "swe.dev/run-cleanup"

const adapterPollInterval = 2 * time.Second

var errAllocatedEnvironmentGone = errors.New("allocated environment is gone or no longer claimed by this run")

const (
	runConditionEnvironmentReady = "EnvironmentReady"
	runConditionAdapterAccepted  = "AdapterAccepted"
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

// +kubebuilder:rbac:groups=swe.dev,resources=runs,verbs=get;list;watch;update;patch;delete
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
		return ctrl.Result{}, r.finalize(ctx, &run)
	}
	if terminalRunState(run.Status.State) {
		return ctrl.Result{}, r.cleanupTerminal(ctx, &run)
	}
	if run.Spec.Cancel && run.Status.EnvironmentRef == nil {
		ref, err := r.recoverEnvironmentReference(ctx, &run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if ref == nil {
			return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateCancelled, "Cancelled", "cancelled before allocation")
		}
		run.Status.EnvironmentRef = ref
		return ctrl.Result{Requeue: true}, r.setRunState(ctx, &run, platformv1alpha1.RunStateAllocating, "EnvironmentRecovered", fmt.Sprintf("recovered environment %s before cancellation", ref.Name))
	}
	if !controllerutil.ContainsFinalizer(&run, runFinalizer) {
		controllerutil.AddFinalizer(&run, runFinalizer)
		return ctrl.Result{}, r.Update(ctx, &run)
	}

	if run.Status.EnvironmentRef == nil {
		ref, err := r.allocateEnvironment(ctx, &run)
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
		if adapter := r.Adapters[run.Spec.Agent]; adapter != nil {
			if err := adapter.Cancel(ctx, adapterTask(&run), adapterSandbox(env)); err != nil {
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

	if run.Status.State == platformv1alpha1.RunStateAllocating || run.Status.State == platformv1alpha1.RunStatePaused || run.Status.State == "" {
		return ctrl.Result{Requeue: true}, r.setRunState(ctx, &run, platformv1alpha1.RunStateEnvironmentReady, "EnvironmentReady", "sandboxd is ready")
	}
	adapter := r.Adapters[run.Spec.Agent]
	if adapter == nil {
		return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateFailed, "AdapterUnavailable", fmt.Sprintf("adapter %q is not registered", run.Spec.Agent))
	}
	if run.Status.State == platformv1alpha1.RunStateEnvironmentReady {
		if err := adapter.EnsureAccepted(ctx, adapterTask(&run), adapterSandbox(env)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, r.setRunState(ctx, &run, platformv1alpha1.RunStateAdapterAccepted, "AdapterAccepted", "adapter accepted the task")
	}

	observation, message, err := adapter.Observe(ctx, adapterTask(&run), adapterSandbox(env))
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
		return &platformv1alpha1.RunEnvironmentReference{Name: env.Name, UID: env.UID, Ownership: platformv1alpha1.EnvironmentOwnershipClaimed}, nil
	}

	template := run.Spec.TemplateRef
	if run.Spec.ProjectRef != "" && template == "" {
		var project platformv1alpha1.Project
		if err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.ProjectRef}, &project); err != nil {
			return nil, fmt.Errorf("get project %q: %w", run.Spec.ProjectRef, err)
		}
		template = project.Spec.TemplateRef
	}
	if template == "" {
		return nil, fmt.Errorf("run has no environment template")
	}
	name := "run-" + string(run.UID)
	key := types.NamespacedName{Namespace: run.Namespace, Name: name}
	var env platformv1alpha1.Environment
	err := r.Get(ctx, key, &env)
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
	if !metav1.IsControlledBy(&env, run) {
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
	if ref.Ownership == platformv1alpha1.EnvironmentOwnershipClaimed && (env.Status.ClaimedBy == nil || env.Status.ClaimedBy.Name != run.Name || env.Status.ClaimedBy.UID != run.UID) {
		return nil, fmt.Errorf("%w: environment %q claim does not match run UID %s", errAllocatedEnvironmentGone, env.Name, run.UID)
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
			return &platformv1alpha1.RunEnvironmentReference{Name: env.Name, UID: env.UID, Ownership: platformv1alpha1.EnvironmentOwnershipClaimed}, nil
		}
		return nil, nil
	}
	var env platformv1alpha1.Environment
	if err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: "run-" + string(run.UID)}, &env); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if !metav1.IsControlledBy(&env, run) {
		return nil, nil
	}
	return &platformv1alpha1.RunEnvironmentReference{Name: env.Name, UID: env.UID, Ownership: platformv1alpha1.EnvironmentOwnershipOwned}, nil
}

func (r *RunReconciler) cleanupTerminal(ctx context.Context, run *platformv1alpha1.Run) error {
	if run.Status.EnvironmentRef == nil {
		return nil
	}
	env, err := r.getAllocatedEnvironment(ctx, run)
	if apierrors.IsNotFound(err) || errors.Is(err, errAllocatedEnvironmentGone) {
		return nil
	}
	if err != nil {
		return err
	}
	if adapter := r.Adapters[run.Spec.Agent]; adapter != nil {
		if err := adapter.Cancel(ctx, adapterTask(run), adapterSandbox(env)); err != nil {
			return err
		}
	}
	if run.Status.EnvironmentRef.Ownership == platformv1alpha1.EnvironmentOwnershipOwned {
		if !env.Spec.Paused {
			env.Spec.Paused = true
			return r.Update(ctx, env)
		}
		return nil
	}
	return r.releaseClaim(ctx, run, env)
}

func (r *RunReconciler) finalize(ctx context.Context, run *platformv1alpha1.Run) error {
	if !controllerutil.ContainsFinalizer(run, runFinalizer) {
		return nil
	}
	if run.Status.EnvironmentRef == nil {
		ref, err := r.recoverEnvironmentReference(ctx, run)
		if err != nil {
			return err
		}
		run.Status.EnvironmentRef = ref
	}
	if run.Status.EnvironmentRef != nil {
		env, err := r.getAllocatedEnvironment(ctx, run)
		if err != nil && !apierrors.IsNotFound(err) && !errors.Is(err, errAllocatedEnvironmentGone) {
			return err
		}
		if err == nil {
			if adapter := r.Adapters[run.Spec.Agent]; adapter != nil {
				if err := adapter.Cancel(ctx, adapterTask(run), adapterSandbox(env)); err != nil {
					return err
				}
			}
			if run.Status.EnvironmentRef.Ownership == platformv1alpha1.EnvironmentOwnershipClaimed {
				if err := r.releaseClaim(ctx, run, env); err != nil {
					return err
				}
			}
		}
	}
	controllerutil.RemoveFinalizer(run, runFinalizer)
	return r.Update(ctx, run)
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
	adapterAccepted := state == platformv1alpha1.RunStateAdapterAccepted || state == platformv1alpha1.RunStateRunning || state == platformv1alpha1.RunStateNeedsInput || state == platformv1alpha1.RunStateSucceeded || (state == platformv1alpha1.RunStateFailed && reason == string(AdapterObservationFailed))
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

func adapterSandbox(env *platformv1alpha1.Environment) AdapterSandbox {
	return AdapterSandbox{EnvironmentName: env.Name, EnvironmentUID: env.UID, SandboxdEndpoint: env.Status.Endpoints.Sandboxd}
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
