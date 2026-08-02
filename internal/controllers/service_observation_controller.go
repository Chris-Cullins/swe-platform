package controllers

import (
	"context"
	"errors"
	"hash/fnv"
	"sort"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	"github.com/Chris-Cullins/swe-platform/internal/sandboxclient"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
)

type ServiceObserver interface {
	ObserveServices(context.Context, lifecycle.ExecutionFence, sandboxclient.ServiceDeclarationSnapshot) (sandboxclient.ServiceObservationResult, error)
	ServiceObservationCurrent(context.Context, lifecycle.ExecutionFence, sandboxclient.ServiceDeclarationSnapshot, sandboxclient.ServiceObservationResult) (bool, error)
}

type ServiceObservationReconciler struct {
	client.Client
	APIReader client.Reader
	Scope     *tenancy.ReconcileScope
	Observer  ServiceObserver
	Now       func() time.Time
}

func (r *ServiceObservationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var env platformv1alpha1.Environment
	if err := r.reader().Get(ctx, req.NamespacedName, &env); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	var err error
	if r.Scope != nil {
		ctx, _, err = r.Scope.Begin(ctx, env.Namespace, tenancy.LifecycleActive)
	}
	if errors.Is(err, tenancy.ErrOutOfScope) {
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if !env.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	if len(env.Spec.Services) == 0 {
		if env.Status.ServiceObservations == nil {
			return ctrl.Result{}, nil
		}
		return r.publish(ctx, &env, nil, nil, sandboxclient.ServiceDeclarationSnapshot{}, nil)
	}

	snapshot := sandboxclient.CaptureServiceDeclarationSnapshot(&env)
	var records []platformv1alpha1.EnvironmentServiceObservation
	var execution *int64
	var observationResult sandboxclient.ServiceObservationResult
	suspended := env.Spec.Paused || env.Status.Lifecycle.Suspended || env.Spec.Lifecycle.Hold != nil && env.Spec.Lifecycle.Hold.Enabled
	if suspended {
		records = fixedRecords(env.Spec.Services, platformv1alpha1.EnvironmentServiceObservationUnavailable, platformv1alpha1.EnvironmentServiceReasonEnvironmentSuspended)
	} else if !platformv1alpha1.IsEnvironmentReady(&env) {
		records = fixedRecords(env.Spec.Services, platformv1alpha1.EnvironmentServiceObservationPending, platformv1alpha1.EnvironmentServiceReasonEnvironmentNotReady)
	} else {
		fence := lifecycle.CaptureExecutionFence(&env)
		callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		var observeErr error
		observationResult, observeErr = r.Observer.ObserveServices(callCtx, fence, snapshot)
		cancel()
		if observeErr != nil {
			return ctrl.Result{RequeueAfter: observationJitter(env.UID)}, nil
		}
		executionValue := fence.ExecutionGeneration()
		execution = &executionValue
		if observationResult.Failed {
			records = fixedRecords(env.Spec.Services, platformv1alpha1.EnvironmentServiceObservationUnknown, platformv1alpha1.EnvironmentServiceReasonObservationFailed)
		} else {
			if len(observationResult.Probes) != len(env.Spec.Services) {
				return ctrl.Result{RequeueAfter: observationJitter(env.UID)}, nil
			}
			revisions := make(map[string]int64, len(env.Spec.Services))
			for _, declaration := range env.Spec.Services {
				revisions[declaration.Name] = declaration.Revision
			}
			for _, probe := range observationResult.Probes {
				state, reason := platformv1alpha1.EnvironmentServiceObservationUnknown, platformv1alpha1.EnvironmentServiceReasonProbeTimedOut
				switch probe.Outcome {
				case sandboxclient.ServiceProbeConnected:
					state, reason = platformv1alpha1.EnvironmentServiceObservationHealthy, platformv1alpha1.EnvironmentServiceReasonConnectionAccepted
				case sandboxclient.ServiceProbeNotConnected:
					state, reason = platformv1alpha1.EnvironmentServiceObservationUnhealthy, platformv1alpha1.EnvironmentServiceReasonConnectionFailed
				case sandboxclient.ServiceProbeTimedOut:
				default:
					return ctrl.Result{RequeueAfter: observationJitter(env.UID)}, nil
				}
				revision, ok := revisions[probe.Name]
				if !ok {
					return ctrl.Result{RequeueAfter: observationJitter(env.UID)}, nil
				}
				records = append(records, platformv1alpha1.EnvironmentServiceObservation{Name: probe.Name, DeclarationRevision: revision, State: state, Reason: reason})
				delete(revisions, probe.Name)
			}
			if len(revisions) != 0 {
				return ctrl.Result{RequeueAfter: observationJitter(env.UID)}, nil
			}
		}
	}
	var publishable *sandboxclient.ServiceObservationResult
	if execution != nil {
		publishable = &observationResult
	}
	result, err := r.publish(ctx, &env, records, execution, snapshot, publishable)
	if err != nil {
		return result, err
	}
	if result.Requeue {
		return result, nil
	}
	result.RequeueAfter = observationJitter(env.UID)
	return result, nil
}

func (r *ServiceObservationReconciler) publish(ctx context.Context, observed *platformv1alpha1.Environment, records []platformv1alpha1.EnvironmentServiceObservation, execution *int64, snapshot sandboxclient.ServiceDeclarationSnapshot, publishable *sandboxclient.ServiceObservationResult) (ctrl.Result, error) {
	var current platformv1alpha1.Environment
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(observed), &current); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if current.UID != observed.UID || !current.DeletionTimestamp.IsZero() || current.Generation != observed.Generation {
		return ctrl.Result{Requeue: true}, nil
	}
	if len(observed.Spec.Services) != 0 && !snapshot.Matches(&current) {
		return ctrl.Result{Requeue: true}, nil
	}
	if execution != nil {
		if err := lifecycle.CaptureExecutionFence(observed).Validate(&current); err != nil || !platformv1alpha1.IsEnvironmentReady(&current) || current.Spec.Paused || current.Status.Lifecycle.Suspended || current.Spec.Lifecycle.Hold != nil && current.Spec.Lifecycle.Hold.Enabled {
			return ctrl.Result{Requeue: true}, nil
		}
		if publishable == nil {
			return ctrl.Result{Requeue: true}, nil
		}
		proofCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		stillCurrent, err := r.Observer.ServiceObservationCurrent(proofCtx, lifecycle.CaptureExecutionFence(observed), snapshot, *publishable)
		cancel()
		if err != nil || !stillCurrent {
			return ctrl.Result{Requeue: true}, nil
		}
	} else if (observed.Spec.Paused || observed.Status.Lifecycle.Suspended || observed.Spec.Lifecycle.Hold != nil && observed.Spec.Lifecycle.Hold.Enabled) != (current.Spec.Paused || current.Status.Lifecycle.Suspended || current.Spec.Lifecycle.Hold != nil && current.Spec.Lifecycle.Hold.Enabled) || platformv1alpha1.IsEnvironmentReady(observed) != platformv1alpha1.IsEnvironmentReady(&current) {
		return ctrl.Result{Requeue: true}, nil
	}
	before := current.DeepCopy()
	if records == nil {
		current.Status.ServiceObservations = nil
	} else {
		sort.Slice(records, func(i, j int) bool { return records[i].Name < records[j].Name })
		current.Status.ServiceObservations = &platformv1alpha1.EnvironmentServiceObservations{ObservedGeneration: current.Generation, ExecutionGeneration: execution, LifecycleEpoch: current.Status.Lifecycle.Epoch, HoldRevision: lifecycle.HoldPolicyRevision(&current), ObservedAt: metav1.NewTime(r.now()), Records: records}
	}
	if err := r.Status().Patch(ctx, &current, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func fixedRecords(declarations []platformv1alpha1.EnvironmentServiceDeclaration, state platformv1alpha1.EnvironmentServiceObservationState, reason platformv1alpha1.EnvironmentServiceObservationReason) []platformv1alpha1.EnvironmentServiceObservation {
	records := make([]platformv1alpha1.EnvironmentServiceObservation, len(declarations))
	for i, d := range declarations {
		records[i] = platformv1alpha1.EnvironmentServiceObservation{Name: d.Name, DeclarationRevision: d.Revision, State: state, Reason: reason}
	}
	return records
}
func observationJitter(uid types.UID) time.Duration {
	h := fnv.New32a()
	_, _ = h.Write([]byte(uid))
	return 4*time.Second + time.Duration(h.Sum32()%2001)*time.Millisecond
}
func (r *ServiceObservationReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}
func (r *ServiceObservationReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *ServiceObservationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.APIReader = mgr.GetAPIReader()
	if r.Observer == nil {
		r.Observer = sandboxclient.Connector{Reader: r.APIReader}
	}
	p := builder.WithPredicates(predicate.Funcs{CreateFunc: func(event.CreateEvent) bool { return true }, DeleteFunc: func(event.DeleteEvent) bool { return true }, GenericFunc: func(event.GenericEvent) bool { return true }, UpdateFunc: func(e event.UpdateEvent) bool {
		old, ok1 := e.ObjectOld.(*platformv1alpha1.Environment)
		new, ok2 := e.ObjectNew.(*platformv1alpha1.Environment)
		return !ok1 || !ok2 || observationRelevantEnvironmentUpdate(old, new)
	}})
	return ctrl.NewControllerManagedBy(mgr).Named("service-observation").For(&platformv1alpha1.Environment{}, p).WithOptions(controller.Options{MaxConcurrentReconciles: 4}).Complete(r)
}
