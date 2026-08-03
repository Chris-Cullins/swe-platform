package controllers

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/agent"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	"github.com/Chris-Cullins/swe-platform/internal/repositorycredential"
	"github.com/Chris-Cullins/swe-platform/internal/sandboxclient"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const runFinalizer = "swe.dev/run-cleanup"

const adapterPollInterval = 2 * time.Second
const credentialReadyTimeout = time.Minute

var errAllocatedEnvironmentGone = errors.New("allocated environment is gone or no longer claimed by this run")
var errExplicitEnvironmentClaimed = errors.New("explicit environment is already claimed")
var errExplicitEnvironmentHeld = errors.New("explicit environment is held")
var errExplicitEnvironmentSuspensionNotWakeable = errors.New("explicit environment suspension is not wakeable")
var errRepositoryCredentialInvalid = errors.New("repository credential authority is invalid")

const (
	runConditionEnvironmentReady           = "EnvironmentReady"
	runConditionCredentialProfileBound     = "CredentialProfileBound"
	runConditionAdapterAcceptanceAttempted = "AdapterAcceptanceAttempted"
	runConditionAdapterAccepted            = "AdapterAccepted"
	runConditionRepositoryCredentialReady  = "RepositoryCredentialReady"
	repositoryRefreshAnnotation            = "credentials.swe.dev/repository-refresh"
)

type repositoryRotationRecord struct {
	OldSecretUID     types.UID `json:"oldSecretUID"`
	TargetGeneration int64     `json:"targetGeneration"`
	Wake             bool      `json:"wake"`
}

func parseRepositoryRotationRecord(value string) (*repositoryRotationRecord, error) {
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	var record repositoryRotationRecord
	if value == "" || decoder.Decode(&record) != nil || record.OldSecretUID == "" || strings.TrimSpace(string(record.OldSecretUID)) != string(record.OldSecretUID) || record.TargetGeneration < 2 {
		return nil, errors.New("invalid repository credential rotation record")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("invalid repository credential rotation record")
	}
	return &record, nil
}

func repositoryRotation(run *platformv1alpha1.Run) (*repositoryRotationRecord, error) {
	return parseRepositoryRotationRecord(run.Annotations[repositoryRefreshAnnotation])
}

func (r *RunReconciler) persistRepositoryRotation(ctx context.Context, run *platformv1alpha1.Run, lease *repositorycredential.Lease, wake bool) error {
	if lease.SecretUID == "" {
		return errors.New("repository credential Secret has no UID")
	}
	record := repositoryRotationRecord{OldSecretUID: lease.SecretUID, TargetGeneration: lease.TokenGeneration + 1, Wake: wake}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if run.Annotations == nil {
		run.Annotations = map[string]string{}
	}
	run.Annotations[repositoryRefreshAnnotation] = string(data)
	return r.Update(ctx, run)
}

// RunReconciler turns a Run intent into one Environment allocation and drives
// its adapter lifecycle. sandboxd, reached through an adapter, owns all agent
// and declared-service processes inside the Environment.
type RunReconciler struct {
	client.Client
	APIReader             client.Reader
	Scheme                *runtime.Scheme
	Scope                 *tenancy.ReconcileScope
	Adapters              map[string]agent.AdapterLifecycle
	EventSink             agent.AdapterEventSink
	Connector             sandboxclient.Connector
	Metrics               *OperatorMetrics
	RepositoryCredentials repositorycredential.Provider
	Now                   func() time.Time
}

func (r *RunReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// +kubebuilder:rbac:groups=swe.dev,resources=runs,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=swe.dev,resources=runs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=swe.dev,resources=runs/transcript,verbs=update
// +kubebuilder:rbac:groups=swe.dev,resources=environments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=swe.dev,resources=environments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=swe.dev,resources=projects,verbs=get;list;watch
// +kubebuilder:rbac:groups=swe.dev,resources=agentcredentialprofiles,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;create;update;delete

func (r *RunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var run platformv1alpha1.Run
	if err := r.Get(ctx, req.NamespacedName, &run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	ctx, namespaceClaim, err := r.Scope.Begin(ctx, run.Namespace, tenancy.LifecycleActive, tenancy.LifecycleFencing)
	if err != nil {
		if errors.Is(err, tenancy.ErrOutOfScope) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if namespaceClaim.Lifecycle == tenancy.LifecycleFencing {
		if namespaceClaim.Operation != tenancy.OperationOffboarding {
			return ctrl.Result{}, nil
		}
		if run.DeletionTimestamp.IsZero() && !run.Spec.Cancel {
			before := run.DeepCopy()
			run.Spec.Cancel = true
			if err := r.Patch(ctx, &run, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, fmt.Errorf("cancel Run for Project offboarding: %w", err)
			}
			return ctrl.Result{Requeue: true}, nil
		}
	}

	if !run.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &run)
	}
	if terminalRunState(run.Status.State) {
		return r.cleanupTerminal(ctx, &run)
	}
	// Persist cleanup ownership before profile resolution or any external
	// credential side effect.
	if !controllerutil.ContainsFinalizer(&run, runFinalizer) {
		controllerutil.AddFinalizer(&run, runFinalizer)
		if err := r.Update(ctx, &run); err != nil {
			return ctrl.Result{}, err
		}
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
			currentWarmClaim, err := r.currentUnpromotedWarmClaim(ctx, &recovered, &run)
			if err != nil {
				return ctrl.Result{}, err
			}
			if recovered.Labels[warmPoolLabel] != "" && !currentWarmClaim {
				ref = nil
			} else if currentWarmClaim {
				if err := r.releaseClaim(ctx, &run, &recovered); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateCancelled, "Cancelled", "cancelled before warm environment promotion", false)
			}
		}
		if ref == nil {
			return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateCancelled, "Cancelled", "cancelled before allocation", false)
		}
		run.Status.EnvironmentRef = ref
		return ctrl.Result{Requeue: true}, r.setRunState(ctx, &run, platformv1alpha1.RunStateAllocating, "EnvironmentRecovered", fmt.Sprintf("recovered environment %s before cancellation", ref.Name), false)
	}
	if run.Status.EnvironmentRef == nil {
		if run.Spec.CredentialProfileRef != "" {
			if policy, ok := r.Adapters[run.Spec.Agent].(agent.AdapterCredentialPolicy); ok && !policy.SupportsCredentialProfiles() {
				return ctrl.Result{}, r.failCredential(ctx, &run, "CredentialProfilesUnsupported", fmt.Sprintf("adapter %q does not support credential profiles", run.Spec.Agent))
			}
		}
		result, done, err := r.ensureCredentialBinding(ctx, &run)
		if done || err != nil {
			return result, err
		}
	}
	if run.Status.EnvironmentRef == nil {
		allocatedNow := false
		ref, err := r.recoverEnvironmentReference(ctx, &run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if ref == nil {
			ref, err = r.allocateEnvironment(ctx, &run)
			allocatedNow = ref != nil && err == nil
		} else if ref.Ownership == platformv1alpha1.EnvironmentOwnershipClaimed {
			var recovered platformv1alpha1.Environment
			if getErr := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: ref.Name}, &recovered); getErr != nil {
				return ctrl.Result{}, getErr
			}
			if run.Spec.EnvironmentRef != "" {
				err = r.wakeExplicitClaim(ctx, &recovered, &run)
				if errors.Is(err, errExplicitEnvironmentHeld) || errors.Is(err, errExplicitEnvironmentSuspensionNotWakeable) {
					if releaseErr := r.releaseClaim(ctx, &run, &recovered); releaseErr != nil {
						return ctrl.Result{}, releaseErr
					}
				}
			} else if currentWarmClaim, currentErr := r.currentUnpromotedWarmClaim(ctx, &recovered, &run); currentErr != nil {
				err = currentErr
			} else if recovered.Labels[warmPoolLabel] != "" && !currentWarmClaim {
				ref = nil
			} else if currentWarmClaim {
				err = r.promoteWarmEnvironment(ctx, &run, &recovered)
			}
		}
		if ref == nil && err == nil {
			ref, err = r.allocateEnvironment(ctx, &run)
			allocatedNow = ref != nil && err == nil
		}
		if err != nil {
			if errors.Is(err, errExplicitEnvironmentClaimed) || errors.Is(err, errExplicitEnvironmentHeld) || errors.Is(err, errExplicitEnvironmentSuspensionNotWakeable) {
				return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateFailed, "EnvironmentUnavailable", err.Error(), false)
			}
			return ctrl.Result{}, err
		}
		run.Status.EnvironmentRef = ref
		if err := r.setRunState(ctx, &run, platformv1alpha1.RunStateAllocating, "EnvironmentAllocated", fmt.Sprintf("environment %s allocated", ref.Name), false); err != nil {
			return ctrl.Result{Requeue: true}, err
		}
		if allocatedNow && run.Spec.EnvironmentRef == "" {
			path := allocationOwnedCreate
			if ref.Ownership == platformv1alpha1.EnvironmentOwnershipClaimed {
				path = allocationWarmClaim
			}
			r.Metrics.observeAllocation(run.CreationTimestamp.Time, path)
		}
		return ctrl.Result{Requeue: true}, nil
	}
	repositoryRefreshAfter := time.Duration(0)
	if !run.Spec.Cancel {
		result, done, err := r.ensureRepositoryCredential(ctx, &run)
		if done || err != nil {
			return result, err
		}
		repositoryRefreshAfter = result.RequeueAfter
	}

	env, err := r.getAllocatedEnvironment(ctx, &run)
	if err != nil {
		if apierrors.IsNotFound(err) || errors.Is(err, errAllocatedEnvironmentGone) {
			if done, cleanupResult, cleanupErr := r.cleanupRepositoryCredential(ctx, &run, nil); !done || cleanupErr != nil {
				return cleanupResult, cleanupErr
			}
			return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateFailed, "EnvironmentLost", err.Error(), false)
		}
		return ctrl.Result{}, err
	}
	if run.Spec.Cancel {
		if runMayHaveAccepted(&run) && !environmentFenced(env) {
			adapter := r.Adapters[run.Spec.Agent]
			if !environmentReachable(env) || adapter == nil {
				return r.requestEnvironmentFence(ctx, env)
			}
			fenceRejections := &fenceRejectionRecorder{metrics: r.Metrics, callSite: fenceCallSiteRunCancel}
			execution, current, err := r.resolveAllocatedExecution(ctx, &run, env, fenceRejections)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !current {
				return ctrl.Result{Requeue: true}, nil
			}
			executionFence := lifecycle.CaptureExecutionFence(env)
			started := time.Now()
			cancelErr := adapter.Cancel(ctx, adapterTask(&run), r.adapterSandbox(&run, env, executionFence, fenceRejections))
			r.Metrics.observeAdapter(run.Spec.Agent, adapterOperationCancel, started, cancelErr)
			if cancelErr != nil {
				if errors.Is(cancelErr, agent.ErrAdapterCancellationPending) {
					return ctrl.Result{RequeueAfter: adapterPollInterval}, nil
				}
				return ctrl.Result{}, cancelErr
			}
			current, err = r.allocatedExecutionCurrent(ctx, &run, env, execution, fenceRejections)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !current {
				return ctrl.Result{Requeue: true}, nil
			}
		}
		return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateCancelled, "Cancelled", "cancellation completed", environmentReachable(env))
	}
	if env.Status.ObservedGeneration != env.Generation {
		if runAccepted(&run) {
			// An accepted execution remains authoritative while its Environment
			// controller classifies a new generation. In particular, legacy
			// spec-based activity must not restart adapter acceptance. Once the
			// Environment status converges, its resulting phase drives the Run.
			return ctrl.Result{RequeueAfter: adapterPollInterval}, nil
		}
		return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateAllocating, "EnvironmentStatusStale", "environment status has not observed the current generation", false)
	}

	switch env.Status.Phase {
	case platformv1alpha1.EnvironmentPhasePaused, platformv1alpha1.EnvironmentPhaseResuming:
		return ctrl.Result{RequeueAfter: repositoryRefreshAfter}, r.setRunState(ctx, &run, platformv1alpha1.RunStatePaused, "EnvironmentPaused", "managed processes stop; workspace and transcript are retained", false)
	case platformv1alpha1.EnvironmentPhaseFailed, platformv1alpha1.EnvironmentPhaseTerminated:
		message := fmt.Sprintf("environment phase is %s", env.Status.Phase)
		if condition := apiMeta.FindStatusCondition(env.Status.Conditions, platformv1alpha1.EnvironmentConditionReady); condition != nil && condition.Message != "" {
			message = condition.Reason + ": " + condition.Message
		}
		return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateFailed, "EnvironmentFailed", message, false)
	case platformv1alpha1.EnvironmentPhaseReady, platformv1alpha1.EnvironmentPhaseRunning:
		// Continue below.
	default:
		return ctrl.Result{RequeueAfter: repositoryRefreshAfter}, r.setRunState(ctx, &run, platformv1alpha1.RunStateAllocating, "EnvironmentNotReady", fmt.Sprintf("environment phase is %s", env.Status.Phase), false)
	}
	if !environmentReachable(env) {
		return ctrl.Result{RequeueAfter: adapterPollInterval}, r.setRunState(ctx, &run, platformv1alpha1.RunStateAllocating, "EnvironmentNotReachable", "sandboxd endpoint is not currently reachable", false)
	}
	if runAccepted(&run) && !acceptedEnvironmentExecutionCurrent(&run, env) && run.Status.State != platformv1alpha1.RunStateEnvironmentReady {
		return ctrl.Result{Requeue: true}, r.setRunState(ctx, &run, platformv1alpha1.RunStateEnvironmentReady, "EnvironmentExecutionChanged", "fresh environment execution requires adapter acceptance", true)
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
		executionFence := lifecycle.CaptureExecutionFence(env)
		fenceRejections := &fenceRejectionRecorder{metrics: r.Metrics, callSite: fenceCallSiteEnsureAccepted}
		execution, current, err := r.resolveAllocatedExecution(ctx, &run, env, fenceRejections)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !current {
			return ctrl.Result{Requeue: true}, nil
		}
		started := time.Now()
		repositoryEnv, lease, err := r.repositoryLaunchMaterial(ctx, &run, env)
		if err != nil {
			return ctrl.Result{}, err
		}
		if lease != nil {
			defer repositorycredential.ClearLease(lease)
		}
		defer func() {
			for _, value := range repositoryEnv {
				clear(value)
			}
		}()
		acceptErr := adapter.EnsureAccepted(ctx, adapterTask(&run), r.adapterSandbox(&run, env, executionFence, fenceRejections), &agent.AdapterLaunchMaterial{AgentCredential: credential, RepositorySecretEnv: repositoryEnv})
		r.Metrics.observeAdapter(run.Spec.Agent, adapterOperationEnsureAccepted, started, acceptErr)
		current, err = r.allocatedExecutionCurrent(ctx, &run, env, execution, fenceRejections)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !current {
			return ctrl.Result{Requeue: true}, nil
		}
		if acceptErr != nil {
			if errors.Is(acceptErr, agent.ErrAdapterTaskRejected) {
				return ctrl.Result{}, r.setRunState(ctx, &run, platformv1alpha1.RunStateFailed, "AdapterRejected", acceptErr.Error(), true)
			}
			return ctrl.Result{}, acceptErr
		}
		acceptedEpoch := executionFence.LifecycleEpoch()
		acceptedExecutionGeneration := executionFence.ExecutionGeneration()
		run.Status.AcceptedEnvironmentEpoch = &acceptedEpoch
		run.Status.AcceptedEnvironmentExecutionGeneration = &acceptedExecutionGeneration
		return ctrl.Result{Requeue: true}, r.setRunState(ctx, &run, platformv1alpha1.RunStateAdapterAccepted, "AdapterAccepted", "adapter accepted the task", true)
	}

	fenceRejections := &fenceRejectionRecorder{metrics: r.Metrics, callSite: fenceCallSiteObserve}
	execution, current, err := r.resolveAllocatedExecution(ctx, &run, env, fenceRejections)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !current {
		return ctrl.Result{Requeue: true}, nil
	}
	executionFence := lifecycle.CaptureExecutionFence(env)
	started := time.Now()
	observation, message, err := adapter.Observe(ctx, adapterTask(&run), r.adapterSandbox(&run, env, executionFence, fenceRejections))
	metricErr := err
	if metricErr == nil {
		switch observation {
		case agent.AdapterObservationAccepted, agent.AdapterObservationRunning, agent.AdapterObservationNeedsInput, agent.AdapterObservationSucceeded, agent.AdapterObservationFailed:
		default:
			metricErr = fmt.Errorf("adapter %q returned unknown observation %q", run.Spec.Agent, observation)
		}
	}
	r.Metrics.observeAdapter(run.Spec.Agent, adapterOperationObserve, started, metricErr)
	if err != nil {
		return ctrl.Result{}, err
	}
	current, err = r.allocatedExecutionCurrent(ctx, &run, env, execution, fenceRejections)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !current {
		return ctrl.Result{Requeue: true}, nil
	}
	state := platformv1alpha1.RunStateAdapterAccepted
	switch observation {
	case agent.AdapterObservationAccepted:
	case agent.AdapterObservationRunning:
		state = platformv1alpha1.RunStateRunning
	case agent.AdapterObservationNeedsInput:
		state = platformv1alpha1.RunStateNeedsInput
	case agent.AdapterObservationSucceeded:
		state = platformv1alpha1.RunStateSucceeded
	case agent.AdapterObservationFailed:
		state = platformv1alpha1.RunStateFailed
	default:
		return ctrl.Result{}, metricErr
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

func (r *RunReconciler) resolveCredential(ctx context.Context, run *platformv1alpha1.Run) (*agent.AdapterCredential, string, error) {
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
	key := types.NamespacedName{Namespace: run.Namespace, Name: platformv1alpha1.AgentCredentialSecretName(profile.UID)}
	metadata := &metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: corev1.SchemeGroupVersion.String(), Kind: "Secret"}}
	if err := r.apiReader().Get(ctx, key, metadata); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, "SecretNotReady", errors.New("credential secret is not ready")
		}
		return nil, "", err
	}
	if !exactCredentialSecretOwner(&profile, metadata) {
		return nil, "ForeignSecret", errors.New("credential secret is not controlled by the bound profile")
	}
	var secret corev1.Secret
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
	if secret.UID != metadata.UID || secret.ResourceVersion != metadata.ResourceVersion {
		return nil, "SecretNotReady", errors.New("credential secret changed during validation")
	}
	if !exactCredentialSecretOwner(&profile, &secret) {
		return nil, "ForeignSecret", errors.New("credential secret is not controlled by the bound profile")
	}
	value, ok := secret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey]
	if secret.Type != platformv1alpha1.AgentCredentialAPIKeySecretType || len(secret.Data) != 1 || !ok || len(value) == 0 || len(value) > platformv1alpha1.AgentCredentialAPIKeyMaxBytes || !utf8.Valid(value) || bytesContainNUL(value) {
		return nil, "MalformedSecret", errors.New("credential secret is malformed")
	}
	return &agent.AdapterCredential{Type: platformv1alpha1.AgentCredentialTypeAPIKey, APIKey: append([]byte(nil), value...)}, "", nil
}

func exactCredentialSecretOwner(profile *platformv1alpha1.AgentCredentialProfile, object metav1.Object) bool {
	owner := metav1.GetControllerOf(object)
	return len(object.GetOwnerReferences()) == 1 && owner != nil && owner.APIVersion == platformv1alpha1.GroupVersion.String() &&
		owner.Kind == "AgentCredentialProfile" && owner.Name == profile.Name && owner.UID == profile.UID
}

func bytesContainNUL(value []byte) bool {
	for _, b := range value {
		if b == 0 {
			return true
		}
	}
	return false
}

func (r *RunReconciler) setRepositoryCredentialCondition(ctx context.Context, run *platformv1alpha1.Run, status metav1.ConditionStatus, reason, message string) error {
	before := run.Status.DeepCopy()
	apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: runConditionRepositoryCredentialReady, Status: status, Reason: reason, Message: message, ObservedGeneration: run.Generation})
	if reflect.DeepEqual(*before, run.Status) {
		return nil
	}
	return r.Status().Update(ctx, run)
}

func (r *RunReconciler) repositoryForRun(ctx context.Context, run *platformv1alpha1.Run) (string, error) {
	if run.Status.EnvironmentRef == nil {
		return "", fmt.Errorf("%w: run has no frozen environment association", errRepositoryCredentialInvalid)
	}
	var env platformv1alpha1.Environment
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Status.EnvironmentRef.Name}, &env); err != nil {
		return "", err
	}
	if env.UID != run.Status.EnvironmentRef.UID || !env.DeletionTimestamp.IsZero() {
		return "", fmt.Errorf("%w: environment repository association is malformed", errRepositoryCredentialInvalid)
	}
	if env.Status.Provisioning == nil || env.Status.Provisioning.Project == nil || platformv1alpha1.ValidateEnvironmentProvisioningSnapshot(&env, env.Status.Provisioning) != nil || !env.Status.Provisioning.ProjectVerified {
		return "", errRepositoryCredentialPending
	}
	p := env.Status.Provisioning.Project
	var project platformv1alpha1.Project
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: p.Name}, &project); err != nil {
		return "", err
	}
	if project.UID != p.UID || !project.DeletionTimestamp.IsZero() {
		return "", fmt.Errorf("%w: environment project association was replaced", errRepositoryCredentialInvalid)
	}
	return p.Repository, nil
}

func (r *RunReconciler) readRepositoryLease(ctx context.Context, run *platformv1alpha1.Run) (*repositorycredential.Lease, error) {
	key := types.NamespacedName{Namespace: run.Namespace, Name: repositorycredential.SecretName(run.UID)}
	metadata := &metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: corev1.SchemeGroupVersion.String(), Kind: "Secret"}}
	if err := r.apiReader().Get(ctx, key, metadata); err != nil {
		return nil, err
	}
	var secret corev1.Secret
	if err := r.apiReader().Get(ctx, key, &secret); err != nil {
		return nil, err
	}
	defer func() {
		for _, value := range secret.Data {
			clear(value)
		}
	}()
	if secret.UID != metadata.UID || secret.ResourceVersion != metadata.ResourceVersion {
		return nil, errors.New("repository credential secret changed during validation")
	}
	return repositorycredential.Parse(&secret, run.Name, run.UID)
}

func (r *RunReconciler) readPendingRepositoryRevocation(ctx context.Context, run *platformv1alpha1.Run) (*repositorycredential.Lease, error) {
	key := types.NamespacedName{Namespace: run.Namespace, Name: repositorycredential.PendingRevocationSecretName(run.UID)}
	var secret corev1.Secret
	if err := r.apiReader().Get(ctx, key, &secret); err != nil {
		return nil, err
	}
	defer func() {
		for _, value := range secret.Data {
			clear(value)
		}
	}()
	return repositorycredential.ParsePendingRevocation(&secret, run.Name, run.UID)
}

func (r *RunReconciler) persistPendingRepositoryRevocation(ctx context.Context, run *platformv1alpha1.Run, source string, credential *repositorycredential.Credential, generation int64) error {
	secret, err := repositorycredential.NewPendingRevocationSecret(run.Namespace, run.Name, run.UID, string(run.Spec.RepositoryCredential), source, credential, generation, r.now())
	if err != nil {
		return err
	}
	if err = r.Create(ctx, secret); err == nil {
		return nil
	}
	persisted, readErr := r.readPendingRepositoryRevocation(ctx, run)
	if readErr != nil {
		return err
	}
	exact := persisted.Provider == string(run.Spec.RepositoryCredential) && persisted.SourceRepository == source &&
		persisted.Repository == credential.Repository && persisted.InstallationID == credential.InstallationID &&
		persisted.ExpiresAt.Equal(credential.ExpiresAt) && persisted.TokenGeneration == generation && bytes.Equal(persisted.Token, credential.Token)
	repositorycredential.ClearLease(persisted)
	if !exact {
		return err
	}
	return nil
}

func (r *RunReconciler) cleanupPendingRepositoryRevocation(ctx context.Context, run *platformv1alpha1.Run) (bool, ctrl.Result, error) {
	pending, err := r.readPendingRepositoryRevocation(ctx, run)
	if apierrors.IsNotFound(err) {
		return true, ctrl.Result{}, nil
	}
	if err != nil {
		return false, ctrl.Result{}, err
	}
	defer repositorycredential.ClearLease(pending)
	if r.RepositoryCredentials == nil && r.now().Before(pending.ExpiresAt) {
		return false, ctrl.Result{RequeueAfter: repositorycredential.DefaultRetryDelay}, r.setRepositoryCredentialCondition(ctx, run, metav1.ConditionFalse, "RevocationPending", "repository credential revocation is pending")
	}
	if r.RepositoryCredentials != nil {
		if revokeErr := r.RepositoryCredentials.Revoke(ctx, &pending.Credential); revokeErr != nil && r.now().Before(pending.ExpiresAt) {
			if statusErr := r.setRepositoryCredentialCondition(ctx, run, metav1.ConditionFalse, "RevocationPending", "repository credential revocation is pending"); statusErr != nil {
				return false, ctrl.Result{}, statusErr
			}
			return false, ctrl.Result{RequeueAfter: repositorycredential.RetryDelay(revokeErr)}, nil
		}
	}
	active, activeErr := r.readRepositoryLease(ctx, run)
	if activeErr == nil {
		sameToken := active.Provider == pending.Provider && active.SourceRepository == pending.SourceRepository &&
			active.Repository == pending.Repository && active.InstallationID == pending.InstallationID && active.ExpiresAt.Equal(pending.ExpiresAt) &&
			active.TokenGeneration == pending.TokenGeneration && bytes.Equal(active.Token, pending.Token)
		uid := active.SecretUID
		repositorycredential.ClearLease(active)
		if sameToken {
			if deleteErr := r.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: repositorycredential.SecretName(run.UID), UID: uid}}, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); deleteErr != nil && !apierrors.IsNotFound(deleteErr) {
				return false, ctrl.Result{}, deleteErr
			}
		}
	}
	uid := pending.SecretUID
	if deleteErr := r.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: repositorycredential.PendingRevocationSecretName(run.UID), UID: uid}}, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); deleteErr != nil && !apierrors.IsNotFound(deleteErr) {
		return false, ctrl.Result{}, deleteErr
	}
	return false, ctrl.Result{Requeue: true}, nil
}

func (r *RunReconciler) ensureRepositoryCredential(ctx context.Context, run *platformv1alpha1.Run) (ctrl.Result, bool, error) {
	if run.Spec.RepositoryCredential == "" {
		return ctrl.Result{}, false, r.setRepositoryCredentialCondition(ctx, run, metav1.ConditionTrue, "NotRequested", "repository credential was not requested")
	}
	if run.Spec.RepositoryCredential != platformv1alpha1.RepositoryCredentialGitHubApp {
		return ctrl.Result{}, true, r.failRepositoryCredential(ctx, run, "RepositoryUnsupported")
	}
	if done, result, err := r.cleanupPendingRepositoryRevocation(ctx, run); !done || err != nil {
		return result, true, err
	}
	if r.RepositoryCredentials == nil {
		return ctrl.Result{}, true, r.failRepositoryCredential(ctx, run, "ProviderDisabled")
	}
	repository, err := r.repositoryForRun(ctx, run)
	if err != nil {
		if errors.Is(err, errRepositoryCredentialPending) {
			return ctrl.Result{RequeueAfter: repositoryCredentialRequeueDelay}, true, r.setRepositoryCredentialCondition(ctx, run, metav1.ConditionFalse, "Issuing", "repository credential is waiting for the frozen provisioning snapshot")
		}
		if errors.Is(err, errRepositoryCredentialInvalid) {
			return ctrl.Result{}, true, r.failRepositoryCredential(ctx, run, "RepositoryUnsupported")
		}
		return ctrl.Result{}, true, err
	}
	canonicalRepository, err := r.RepositoryCredentials.CanonicalRepository(repository)
	if err != nil {
		return ctrl.Result{}, true, r.failRepositoryCredential(ctx, run, repositorycredential.Reason(err))
	}
	var rotation *repositoryRotationRecord
	if run.Annotations[repositoryRefreshAnnotation] != "" {
		rotation, err = repositoryRotation(run)
		if err != nil {
			return ctrl.Result{}, true, r.failRepositoryCredential(ctx, run, "MalformedRotationRecord")
		}
	}
	lease, err := r.readRepositoryLease(ctx, run)
	if err == nil {
		defer repositorycredential.ClearLease(lease)
		if lease.Provider != string(run.Spec.RepositoryCredential) || lease.SourceRepository != repository || lease.Repository != canonicalRepository {
			return ctrl.Result{}, true, r.failRepositoryCredential(ctx, run, "SecretChanged")
		}
		if rotation != nil {
			if lease.SecretUID != rotation.OldSecretUID {
				if lease.TokenGeneration != rotation.TargetGeneration {
					return ctrl.Result{}, true, r.failRepositoryCredential(ctx, run, "SecretChanged")
				}
				if rotation.Wake {
					var env platformv1alpha1.Environment
					if getErr := r.getExactEnvironment(ctx, run, &env); getErr != nil {
						return ctrl.Result{}, true, getErr
					}
					requestID := fmt.Sprintf("run/%s/repository-refresh/%s", run.UID, lease.SecretUID)
					wakePublished := env.Spec.Lifecycle.Wake != nil && env.Spec.Lifecycle.Wake.ID == requestID || env.Status.Lifecycle.LastWakeRequestID == requestID
					if wakePublished {
						delete(run.Annotations, repositoryRefreshAnnotation)
						if updateErr := r.Update(ctx, run); updateErr != nil {
							return ctrl.Result{}, true, updateErr
						}
						return ctrl.Result{Requeue: true}, true, nil
					}
					if explicitHoldEnabled(&env) {
						return ctrl.Result{}, false, r.setRepositoryCredentialCondition(ctx, run, metav1.ConditionTrue, "Ready", "fresh repository credential is ready while environment is held")
					}
					// A missing old Secret is not itself proof that the execution
					// which received it has stopped. Always finish the requested
					// fence before publishing the refresh wake.
					if !environmentFenced(&env) {
						result, fenceErr := r.requestEnvironmentFence(ctx, &env)
						return result, true, fenceErr
					}
					if wakeErr := lifecycle.RequestWakeForReason(ctx, r.Client, client.ObjectKeyFromObject(&env), env.UID, lifecycle.HoldPolicyRevision(&env), requestID, platformv1alpha1.EnvironmentSuspensionReasonRequested); wakeErr != nil {
						return ctrl.Result{}, true, wakeErr
					}
				}
				delete(run.Annotations, repositoryRefreshAnnotation)
				if updateErr := r.Update(ctx, run); updateErr != nil {
					return ctrl.Result{}, true, updateErr
				}
				return ctrl.Result{Requeue: true}, true, nil
			}
			if lease.TokenGeneration+1 != rotation.TargetGeneration {
				return ctrl.Result{}, true, r.failRepositoryCredential(ctx, run, "SecretChanged")
			}
			if rotation.Wake {
				if condition := apiMeta.FindStatusCondition(run.Status.Conditions, runConditionRepositoryCredentialReady); condition == nil || condition.Reason != "Refreshing" {
					return ctrl.Result{Requeue: true}, true, r.setRepositoryCredentialCondition(ctx, run, metav1.ConditionFalse, "Refreshing", "repository credential refresh is fencing the current execution")
				}
				var env platformv1alpha1.Environment
				if getErr := r.getExactEnvironment(ctx, run, &env); getErr != nil {
					return ctrl.Result{}, true, getErr
				}
				if !environmentFenced(&env) {
					result, fenceErr := r.requestEnvironmentFence(ctx, &env)
					return result, true, fenceErr
				}
			}
			if revokeErr := r.RepositoryCredentials.Revoke(ctx, &lease.Credential); revokeErr != nil && r.now().Before(lease.ExpiresAt) {
				return ctrl.Result{RequeueAfter: repositorycredential.RetryDelay(revokeErr)}, true, nil
			}
			uid := lease.SecretUID
			if deleteErr := r.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: repositorycredential.SecretName(run.UID), UID: uid}}, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); deleteErr != nil && !apierrors.IsNotFound(deleteErr) {
				return ctrl.Result{}, true, deleteErr
			}
			return ctrl.Result{Requeue: true}, true, r.setRepositoryCredentialCondition(ctx, run, metav1.ConditionFalse, "Issuing", "repository credential lease is being issued")
		}
		// A bound lease belonged to a process in an already-created execution.
		// If that execution's Pod is gone, revoke it before Environment resume so
		// the clone init container can consume a fresh unbound lease.
		if lease.EnvironmentUID != "" && run.Status.EnvironmentRef != nil {
			var env platformv1alpha1.Environment
			if getErr := r.apiReader().Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Status.EnvironmentRef.Name}, &env); getErr == nil && env.UID == run.Status.EnvironmentRef.UID {
				var pod corev1.Pod
				podErr := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envPodName(&env)}, &pod)
				if apierrors.IsNotFound(podErr) {
					provisioning, provisioningErr := repositoryProvisioning(&env)
					if provisioningErr != nil {
						return ctrl.Result{}, true, provisioningErr
					}
					provisioningCurrent := provisioning != nil && provisioning.SecretUID == lease.SecretUID &&
						provisioning.ExecutionGeneration == lease.ExecutionGeneration && lease.EnvironmentUID == env.UID
					if !provisioningCurrent {
						if persistErr := r.persistRepositoryRotation(ctx, run, lease, false); persistErr != nil {
							return ctrl.Result{}, true, persistErr
						}
						return ctrl.Result{Requeue: true}, true, nil
					}
				} else if podErr != nil {
					return ctrl.Result{}, true, podErr
				}
			} else if getErr != nil && !apierrors.IsNotFound(getErr) {
				return ctrl.Result{}, true, getErr
			}
		}
		if lease.ExpiresAt.Sub(r.now()) <= repositorycredential.RefreshMargin && run.Status.EnvironmentRef != nil {
			if persistErr := r.persistRepositoryRotation(ctx, run, lease, true); persistErr != nil {
				return ctrl.Result{}, true, persistErr
			}
			return ctrl.Result{Requeue: true}, true, nil
		}
		delay := lease.ExpiresAt.Add(-repositorycredential.RefreshMargin).Sub(r.now())
		if delay < 0 {
			delay = 0
		}
		return ctrl.Result{RequeueAfter: delay}, false, r.setRepositoryCredentialCondition(ctx, run, metav1.ConditionTrue, "Ready", "repository credential lease is ready")
	}
	if !apierrors.IsNotFound(err) {
		reason := "MalformedSecret"
		if strings.Contains(err.Error(), "foreign repository credential secret") {
			reason = "ForeignSecret"
		} else if strings.Contains(err.Error(), "changed during validation") {
			reason = "SecretChanged"
		}
		return ctrl.Result{}, true, r.failRepositoryCredential(ctx, run, reason)
	}
	condition := apiMeta.FindStatusCondition(run.Status.Conditions, runConditionRepositoryCredentialReady)
	if condition == nil || condition.Reason != "Issuing" && condition.Reason != "Refreshing" {
		return ctrl.Result{Requeue: true}, true, r.setRepositoryCredentialCondition(ctx, run, metav1.ConditionFalse, "Issuing", "repository credential lease is being issued")
	}
	credential, issueErr := r.RepositoryCredentials.Issue(ctx, canonicalRepository)
	if issueErr != nil {
		reason := repositorycredential.Reason(issueErr)
		if repositorycredential.IsRetryable(issueErr) {
			if err := r.setRepositoryCredentialCondition(ctx, run, metav1.ConditionFalse, reason, "repository credential provider is temporarily unavailable"); err != nil {
				return ctrl.Result{}, true, err
			}
			return ctrl.Result{RequeueAfter: repositorycredential.RetryDelay(issueErr)}, true, nil
		}
		return ctrl.Result{}, true, r.failRepositoryCredential(ctx, run, reason)
	}
	if credential != nil {
		defer clear(credential.Token)
	}
	generation := int64(1)
	if rotation != nil {
		generation = rotation.TargetGeneration
	}
	secret, createErr := repositorycredential.NewSecret(run.Namespace, run.Name, run.UID, string(run.Spec.RepositoryCredential), repository, credential, generation, "", 0, r.now())
	if createErr == nil {
		createErr = r.Create(ctx, secret)
	}
	if createErr != nil && secret != nil {
		persisted, readErr := r.readRepositoryLease(ctx, run)
		if readErr == nil {
			exact := persisted.Provider == string(run.Spec.RepositoryCredential) && persisted.SourceRepository == repository &&
				persisted.Repository == credential.Repository && persisted.InstallationID == credential.InstallationID &&
				persisted.ExpiresAt.Equal(credential.ExpiresAt) && persisted.TokenGeneration == generation && bytes.Equal(persisted.Token, credential.Token)
			repositorycredential.ClearLease(persisted)
			if exact {
				createErr = nil
			}
		}
	}
	if createErr != nil {
		if credential != nil {
			if revokeErr := r.RepositoryCredentials.Revoke(ctx, credential); revokeErr != nil {
				if pendingErr := r.persistPendingRepositoryRevocation(ctx, run, repository, credential, generation); pendingErr != nil {
					return ctrl.Result{}, true, r.failRepositoryCredential(ctx, run, "SecretPersistenceFailed")
				}
				if statusErr := r.setRepositoryCredentialCondition(ctx, run, metav1.ConditionFalse, "RevocationPending", "repository credential revocation is pending"); statusErr != nil {
					return ctrl.Result{}, true, statusErr
				}
				return ctrl.Result{RequeueAfter: repositorycredential.RetryDelay(revokeErr)}, true, nil
			}
		}
		if apierrors.IsAlreadyExists(createErr) {
			return ctrl.Result{}, true, r.failRepositoryCredential(ctx, run, "SecretChanged")
		}
		return ctrl.Result{}, true, r.failRepositoryCredential(ctx, run, "SecretPersistenceFailed")
	}
	return ctrl.Result{Requeue: true}, true, nil
}

func (r *RunReconciler) failRepositoryCredential(ctx context.Context, run *platformv1alpha1.Run, reason string) error {
	apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: runConditionRepositoryCredentialReady, Status: metav1.ConditionFalse, Reason: reason, Message: "repository credential is unavailable", ObservedGeneration: run.Generation})
	return r.setRunState(ctx, run, platformv1alpha1.RunStateFailed, reason, "repository credential is unavailable", false)
}

func (r *RunReconciler) getExactEnvironment(ctx context.Context, run *platformv1alpha1.Run, env *platformv1alpha1.Environment) error {
	if run.Status.EnvironmentRef == nil {
		return errors.New("run has no allocated environment")
	}
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Status.EnvironmentRef.Name}, env); err != nil {
		return err
	}
	if env.UID != run.Status.EnvironmentRef.UID {
		return errAllocatedEnvironmentGone
	}
	return nil
}

func (r *RunReconciler) repositoryLaunchMaterial(ctx context.Context, run *platformv1alpha1.Run, env *platformv1alpha1.Environment) (map[string][]byte, *repositorycredential.Lease, error) {
	if run.Spec.RepositoryCredential == "" {
		return nil, nil, nil
	}
	lease, err := r.readRepositoryLease(ctx, run)
	if err != nil {
		return nil, nil, err
	}
	if !lease.ExpiresAt.After(r.now()) || env.Status.Provisioning == nil || env.Status.Provisioning.Project == nil || lease.SourceRepository != env.Status.Provisioning.Project.Repository {
		repositorycredential.ClearLease(lease)
		return nil, nil, errors.New("repository credential does not match frozen provisioning authority")
	}
	canonical, err := r.RepositoryCredentials.CanonicalRepository(env.Status.Provisioning.Project.Repository)
	if err != nil || lease.Repository != canonical {
		repositorycredential.ClearLease(lease)
		return nil, nil, errors.New("repository credential canonical repository mismatch")
	}
	if lease.EnvironmentUID == "" || lease.EnvironmentUID != env.UID || lease.ExecutionGeneration != env.Status.ExecutionGeneration {
		repositorycredential.ClearLease(lease)
		return nil, nil, errors.New("repository credential is not bound to the exact environment execution")
	}
	source := make([]byte, len("x-access-token:")+len(lease.Token))
	copy(source, "x-access-token:")
	copy(source[len("x-access-token:"):], lease.Token)
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(source)))
	base64.StdEncoding.Encode(encoded, source)
	clear(source)
	header := append([]byte("AUTHORIZATION: basic "), encoded...)
	clear(encoded)
	return map[string][]byte{"GH_TOKEN": append([]byte(nil), lease.Token...), "GIT_CONFIG_COUNT": []byte("1"), "GIT_CONFIG_KEY_0": []byte("http.https://github.com/.extraheader"), "GIT_CONFIG_VALUE_0": header}, lease, nil
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
		if explicitHoldEnabled(&env) {
			return nil, fmt.Errorf("%w: environment %q is explicitly held at policy revision %d", errExplicitEnvironmentHeld, env.Name, lifecycle.HoldPolicyRevision(&env))
		}
		if _, err := explicitClaimWakeReason(&env); err != nil {
			return nil, err
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
		if environmentSuspended(&env) {
			if err := r.wakeExplicitClaim(ctx, &env, run); err != nil {
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
	return getAllocatedEnvironment(ctx, r.Client, run)
}

func getAllocatedEnvironment(ctx context.Context, reader client.Reader, run *platformv1alpha1.Run) (*platformv1alpha1.Environment, error) {
	ref := run.Status.EnvironmentRef
	var env platformv1alpha1.Environment
	if err := reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: ref.Name}, &env); err != nil {
		return nil, err
	}
	if env.UID != ref.UID {
		// Preserve the exact replacement object for execution-fence metric
		// classification. Other association failures below return no object:
		// they are not tuple mismatches and must not be counted as fence changes.
		return &env, fmt.Errorf("%w: environment %q was replaced (wanted UID %s, got %s)", errAllocatedEnvironmentGone, env.Name, ref.UID, env.UID)
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
			if env.Labels[warmPoolLabel] != "" {
				current, err := r.currentUnpromotedWarmClaim(ctx, env, run)
				if err != nil {
					return nil, err
				}
				if !current {
					continue
				}
			}
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
	templateName := run.Spec.TemplateRef
	if run.Spec.TemplateRef != "" {
		templateName = run.Spec.TemplateRef
	} else if run.Spec.ProjectRef != "" {
		var project platformv1alpha1.Project
		if err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.ProjectRef}, &project); err != nil {
			return "", fmt.Errorf("get project %q: %w", run.Spec.ProjectRef, err)
		}
		templateName = project.Spec.TemplateRef
	}
	if templateName == "" {
		return "", nil
	}
	var template platformv1alpha1.EnvironmentTemplate
	if err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: templateName}, &template); err != nil {
		// Preserve the existing allocation contract for a missing Template: the
		// owned Environment records the intent and reports the invalid reference.
		if !apierrors.IsNotFound(err) {
			return "", fmt.Errorf("get template %q: %w", templateName, err)
		}
		return templateName, nil
	}
	if tenancy.IsCatalogSource(&template) {
		return "", fmt.Errorf("template %q is an inert installation catalog source", templateName)
	}
	if r.Scope != nil && r.Scope.Verifier != nil {
		claim, err := r.Scope.Verifier.VerifyNamespace(ctx, run.Namespace)
		if err != nil {
			return "", err
		}
		if err := tenancy.ValidateManagedTemplate(&template, r.Scope.Verifier.Installation, claim); err != nil {
			return "", err
		}
	}
	return templateName, nil
}

func (r *RunReconciler) claimWarmEnvironment(ctx context.Context, run *platformv1alpha1.Run, template string) (*platformv1alpha1.RunEnvironmentReference, error) {
	var environments platformv1alpha1.EnvironmentList
	if err := r.List(ctx, &environments, client.InNamespace(run.Namespace), client.MatchingLabels{warmPoolLabel: template}); err != nil {
		return nil, fmt.Errorf("list warm environments: %w", err)
	}
	if len(environments.Items) == 0 {
		return nil, nil
	}
	var currentTemplate platformv1alpha1.EnvironmentTemplate
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: template}, &currentTemplate); err != nil {
		return nil, fmt.Errorf("get warm environment template %q: %w", template, err)
	}
	if !currentTemplate.DeletionTimestamp.IsZero() {
		return nil, nil
	}
	for i := range environments.Items {
		env := &environments.Items[i]
		if environmentSuspended(env) || !platformv1alpha1.IsEnvironmentReady(env) || env.Status.ClaimedBy != nil || !warmPoolMemberCurrent(env, &currentTemplate) {
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
		var confirmedTemplate platformv1alpha1.EnvironmentTemplate
		if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: template}, &confirmedTemplate); err != nil ||
			!confirmedTemplate.DeletionTimestamp.IsZero() || !warmPoolMemberCurrent(env, &confirmedTemplate) {
			if releaseErr := r.releaseClaim(ctx, run, env); releaseErr != nil {
				return nil, fmt.Errorf("release warm environment %q after template changed: %w", env.Name, releaseErr)
			}
			if err != nil && !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("confirm warm environment template %q: %w", template, err)
			}
			continue
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

func (r *RunReconciler) wakeExplicitClaim(ctx context.Context, env *platformv1alpha1.Environment, run *platformv1alpha1.Run) error {
	if explicitHoldEnabled(env) {
		return fmt.Errorf("%w: environment %q is explicitly held at policy revision %d", errExplicitEnvironmentHeld, env.Name, lifecycle.HoldPolicyRevision(env))
	}
	if !environmentSuspended(env) {
		return nil
	}
	reason, err := explicitClaimWakeReason(env)
	if err != nil {
		return err
	}
	return lifecycle.RequestWakeForReason(ctx, r.Client, client.ObjectKeyFromObject(env), env.UID, lifecycle.HoldPolicyRevision(env), "run/"+string(run.UID)+"/wake", reason)
}

func explicitClaimWakeReason(env *platformv1alpha1.Environment) (platformv1alpha1.EnvironmentSuspensionReason, error) {
	if !environmentSuspended(env) {
		return "", nil
	}
	switch env.Status.Lifecycle.SuspensionReason {
	case platformv1alpha1.EnvironmentSuspensionReasonIdle, platformv1alpha1.EnvironmentSuspensionReasonRequested:
		return env.Status.Lifecycle.SuspensionReason, nil
	default:
		return "", fmt.Errorf("%w: environment %q has suspension reason %q", errExplicitEnvironmentSuspensionNotWakeable, env.Name, env.Status.Lifecycle.SuspensionReason)
	}
}

func explicitHoldEnabled(env *platformv1alpha1.Environment) bool {
	return env.Spec.Paused || env.Spec.Lifecycle.Hold != nil && env.Spec.Lifecycle.Hold.Enabled
}

func environmentReachable(env *platformv1alpha1.Environment) bool {
	return env.Status.ExecutionGeneration > 0 && !environmentSuspended(env) && platformv1alpha1.IsEnvironmentReady(env) && env.Status.PodName != "" && env.Status.Endpoints.Sandboxd != ""
}

func exactControllerOwner(object metav1.Object, apiVersion, kind, name string, uid types.UID) bool {
	owner := metav1.GetControllerOf(object)
	return owner != nil && owner.APIVersion == apiVersion && owner.Kind == kind && owner.Name == name && owner.UID == uid
}

func (r *RunReconciler) currentUnpromotedWarmClaim(ctx context.Context, env *platformv1alpha1.Environment, run *platformv1alpha1.Run) (bool, error) {
	templateName := env.Labels[warmPoolLabel]
	if templateName == "" || env.Spec.TemplateRef != templateName || env.Status.ClaimedBy == nil ||
		env.Status.ClaimedBy.Name != run.Name || env.Status.ClaimedBy.UID != run.UID {
		return false, nil
	}
	var template platformv1alpha1.EnvironmentTemplate
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: templateName}, &template); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if !template.DeletionTimestamp.IsZero() {
		return false, nil
	}
	return warmPoolMemberCurrent(env, &template), nil
}

func environmentFenced(env *platformv1alpha1.Environment) bool {
	return environmentSuspended(env) && env.Status.Phase == platformv1alpha1.EnvironmentPhasePaused && env.Status.PodName == "" && env.Status.Endpoints.Sandboxd == ""
}

func environmentSuspended(env *platformv1alpha1.Environment) bool {
	return env.Spec.Paused || env.Status.Lifecycle.Suspended
}

func runAccepted(run *platformv1alpha1.Run) bool {
	condition := apiMeta.FindStatusCondition(run.Status.Conditions, runConditionAdapterAccepted)
	return condition != nil && condition.Status == metav1.ConditionTrue
}

func acceptedEnvironmentEpochCurrent(run *platformv1alpha1.Run, env *platformv1alpha1.Environment) bool {
	if run.Status.AcceptedEnvironmentEpoch == nil {
		// Runs accepted before the epoch fence was introduced are compatible
		// with the initial epoch. A nonzero epoch must be accepted explicitly.
		return env.Status.Lifecycle.Epoch == 0
	}
	return *run.Status.AcceptedEnvironmentEpoch == env.Status.Lifecycle.Epoch
}

func acceptedEnvironmentExecutionCurrent(run *platformv1alpha1.Run, env *platformv1alpha1.Environment) bool {
	return env.Status.ExecutionGeneration > 0 && run.Status.AcceptedEnvironmentExecutionGeneration != nil &&
		*run.Status.AcceptedEnvironmentExecutionGeneration == env.Status.ExecutionGeneration && acceptedEnvironmentEpochCurrent(run, env)
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
	if !environmentSuspended(env) {
		nextSequence := max(env.Status.Lifecycle.Epoch, env.Status.Lifecycle.LastSuspendRequestSequence) + 1
		requestID := fmt.Sprintf("environment/%s/fence/%d", env.UID, nextSequence)
		if env.Status.ClaimedBy != nil {
			requestID = fmt.Sprintf("run/%s/fence/%d", env.Status.ClaimedBy.UID, nextSequence)
		}
		if err := lifecycle.RequestSuspend(ctx, r.Client, client.ObjectKeyFromObject(env), env.UID, lifecycle.HoldPolicyRevision(env), requestID, nextSequence); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: adapterPollInterval}, nil
}

func (r *RunReconciler) cleanupTerminal(ctx context.Context, run *platformv1alpha1.Run) (ctrl.Result, error) {
	if run.Status.EnvironmentRef == nil {
		if done, result, err := r.cleanupRepositoryCredential(ctx, run, nil); !done || err != nil {
			return result, err
		}
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
		if done, result, cleanupErr := r.cleanupRepositoryCredential(ctx, run, nil); !done || cleanupErr != nil {
			return result, cleanupErr
		}
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
		fenceRejections := &fenceRejectionRecorder{metrics: r.Metrics, callSite: fenceCallSiteTerminalCleanup}
		execution, current, err := r.resolveAllocatedExecution(ctx, run, env, fenceRejections)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !current {
			return ctrl.Result{Requeue: true}, nil
		}
		executionFence := lifecycle.CaptureExecutionFence(env)
		started := time.Now()
		cancelErr := adapter.Cancel(ctx, adapterTask(run), r.adapterSandbox(run, env, executionFence, fenceRejections))
		r.Metrics.observeAdapter(run.Spec.Agent, adapterOperationCancel, started, cancelErr)
		if cancelErr != nil {
			if errors.Is(cancelErr, agent.ErrAdapterCancellationPending) {
				return ctrl.Result{RequeueAfter: adapterPollInterval}, nil
			}
			return ctrl.Result{}, cancelErr
		}
		current, err = r.allocatedExecutionCurrent(ctx, run, env, execution, fenceRejections)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !current {
			return ctrl.Result{Requeue: true}, nil
		}
	}
	if run.Spec.RepositoryCredential != "" && !environmentFenced(env) {
		return r.requestEnvironmentFence(ctx, env)
	}
	if env.Spec.Lifecycle.Suspend != nil || env.Status.Lifecycle.PendingSuspendRequestID != "" {
		return ctrl.Result{RequeueAfter: adapterPollInterval}, nil
	}
	if done, result, err := r.cleanupRepositoryCredential(ctx, run, env); !done || err != nil {
		return result, err
	}
	if run.Status.EnvironmentRef.Ownership == platformv1alpha1.EnvironmentOwnershipOwned {
		if !environmentSuspended(env) {
			return r.requestEnvironmentFence(ctx, env)
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
			currentWarmClaim, currentErr := r.currentUnpromotedWarmClaim(ctx, &recovered, run)
			if currentErr != nil {
				return ctrl.Result{}, currentErr
			}
			if recovered.Labels[warmPoolLabel] != "" && !currentWarmClaim {
				ref = nil
			} else if currentWarmClaim {
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
		if err != nil {
			if done, result, cleanupErr := r.cleanupRepositoryCredential(ctx, run, nil); !done || cleanupErr != nil {
				return result, cleanupErr
			}
		} else {
			if runMayHaveAccepted(run) && !environmentFenced(env) {
				adapter := r.Adapters[run.Spec.Agent]
				if !environmentReachable(env) || adapter == nil {
					return r.requestEnvironmentFence(ctx, env)
				}
				fenceRejections := &fenceRejectionRecorder{metrics: r.Metrics, callSite: fenceCallSiteFinalizerCleanup}
				execution, current, resolveErr := r.resolveAllocatedExecution(ctx, run, env, fenceRejections)
				if resolveErr != nil {
					return ctrl.Result{}, resolveErr
				}
				if !current {
					return ctrl.Result{Requeue: true}, nil
				}
				executionFence := lifecycle.CaptureExecutionFence(env)
				started := time.Now()
				cancelErr := adapter.Cancel(ctx, adapterTask(run), r.adapterSandbox(run, env, executionFence, fenceRejections))
				r.Metrics.observeAdapter(run.Spec.Agent, adapterOperationCancel, started, cancelErr)
				if cancelErr != nil {
					if errors.Is(cancelErr, agent.ErrAdapterCancellationPending) {
						return ctrl.Result{RequeueAfter: adapterPollInterval}, nil
					}
					return ctrl.Result{}, cancelErr
				}
				current, resolveErr = r.allocatedExecutionCurrent(ctx, run, env, execution, fenceRejections)
				if resolveErr != nil {
					return ctrl.Result{}, resolveErr
				}
				if !current {
					return ctrl.Result{Requeue: true}, nil
				}
			}
			if run.Spec.RepositoryCredential != "" && !environmentFenced(env) {
				return r.requestEnvironmentFence(ctx, env)
			}
			if env.Spec.Lifecycle.Suspend != nil || env.Status.Lifecycle.PendingSuspendRequestID != "" {
				return ctrl.Result{RequeueAfter: adapterPollInterval}, nil
			}
			if done, result, cleanupErr := r.cleanupRepositoryCredential(ctx, run, env); !done || cleanupErr != nil {
				return result, cleanupErr
			}
			if run.Status.EnvironmentRef.Ownership == platformv1alpha1.EnvironmentOwnershipClaimed {
				if err := r.releaseClaim(ctx, run, env); err != nil {
					return ctrl.Result{}, err
				}
			}
		}
	}
	if run.Status.EnvironmentRef == nil {
		if done, result, cleanupErr := r.cleanupRepositoryCredential(ctx, run, nil); !done || cleanupErr != nil {
			return result, cleanupErr
		}
	}
	controllerutil.RemoveFinalizer(run, runFinalizer)
	return ctrl.Result{}, r.Update(ctx, run)
}

func (r *RunReconciler) cleanupRepositoryCredential(ctx context.Context, run *platformv1alpha1.Run, env *platformv1alpha1.Environment) (bool, ctrl.Result, error) {
	if run.Spec.RepositoryCredential == "" {
		return true, ctrl.Result{}, nil
	}
	if env != nil && !environmentFenced(env) {
		result, err := r.requestEnvironmentFence(ctx, env)
		return false, result, err
	}
	if done, result, err := r.cleanupPendingRepositoryRevocation(ctx, run); !done || err != nil {
		return false, result, err
	}
	lease, err := r.readRepositoryLease(ctx, run)
	if apierrors.IsNotFound(err) {
		if run.Annotations[repositoryRefreshAnnotation] != "" {
			delete(run.Annotations, repositoryRefreshAnnotation)
			if updateErr := r.Update(ctx, run); updateErr != nil {
				return false, ctrl.Result{}, updateErr
			}
		}
		_ = r.setRepositoryCredentialCondition(ctx, run, metav1.ConditionTrue, "Revoked", "repository credential lifecycle is complete")
		return true, ctrl.Result{}, nil
	}
	if err != nil {
		// An expired exact secret no longer represents an active provider token.
		var secret corev1.Secret
		key := types.NamespacedName{Namespace: run.Namespace, Name: repositorycredential.SecretName(run.UID)}
		if getErr := r.apiReader().Get(ctx, key, &secret); getErr != nil {
			return false, ctrl.Result{}, getErr
		}
		source, identityErr := r.repositoryForRun(ctx, run)
		if identityErr != nil {
			if !apierrors.IsNotFound(identityErr) {
				return false, ctrl.Result{}, fmt.Errorf("repository credential cleanup identity unavailable: %w", identityErr)
			}
			source = secret.Annotations[repositorycredential.AnnotationSourceRepository]
		}
		canonical := secret.Annotations[repositorycredential.AnnotationRepository]
		if r.RepositoryCredentials != nil {
			var canonicalErr error
			canonical, canonicalErr = r.RepositoryCredentials.CanonicalRepository(source)
			if canonicalErr != nil {
				return false, ctrl.Result{}, fmt.Errorf("repository credential cleanup canonical identity unavailable: %w", canonicalErr)
			}
		}
		expires, parseErr := time.Parse(time.RFC3339Nano, secret.Annotations[repositorycredential.AnnotationExpiry])
		if !repositorycredential.ExactManagedIdentity(&secret, run.Name, run.UID, string(run.Spec.RepositoryCredential), source, canonical) {
			return false, ctrl.Result{}, errors.New("repository credential cleanup blocked by foreign Secret collision")
		}
		if parseErr != nil || r.now().Before(expires) {
			return false, ctrl.Result{}, err
		}
		uid := secret.UID
		if deleteErr := r.Delete(ctx, &secret, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); deleteErr != nil && !apierrors.IsNotFound(deleteErr) {
			return false, ctrl.Result{}, deleteErr
		}
		return true, ctrl.Result{}, nil
	}
	defer repositorycredential.ClearLease(lease)
	if r.RepositoryCredentials == nil && r.now().Before(lease.ExpiresAt) {
		_ = r.setRepositoryCredentialCondition(ctx, run, metav1.ConditionFalse, "RevocationPending", "repository credential revocation is pending")
		return false, ctrl.Result{RequeueAfter: repositorycredential.DefaultRetryDelay}, nil
	}
	if r.RepositoryCredentials != nil {
		if revokeErr := r.RepositoryCredentials.Revoke(ctx, &lease.Credential); revokeErr != nil && r.now().Before(lease.ExpiresAt) {
			_ = r.setRepositoryCredentialCondition(ctx, run, metav1.ConditionFalse, "RevocationPending", "repository credential revocation is pending")
			return false, ctrl.Result{RequeueAfter: repositorycredential.RetryDelay(revokeErr)}, nil
		}
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: repositorycredential.SecretName(run.UID), UID: lease.SecretUID}}
	uid := lease.SecretUID
	if err := r.Delete(ctx, secret, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) {
		return false, ctrl.Result{}, err
	}
	if run.Annotations[repositoryRefreshAnnotation] != "" {
		delete(run.Annotations, repositoryRefreshAnnotation)
		if err := r.Update(ctx, run); err != nil {
			return false, ctrl.Result{}, err
		}
	}
	_ = r.setRepositoryCredentialCondition(ctx, run, metav1.ConditionTrue, "Revoked", "repository credential lifecycle is complete")
	return true, ctrl.Result{}, nil
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
	adapterAccepted := runAccepted(run) || state == platformv1alpha1.RunStateAdapterAccepted || state == platformv1alpha1.RunStateRunning || state == platformv1alpha1.RunStateNeedsInput || state == platformv1alpha1.RunStateSucceeded || (state == platformv1alpha1.RunStateFailed && reason == string(agent.AdapterObservationFailed))
	if adapterAccepted && run.Status.StartedAt == nil {
		startedAt := metav1.Now()
		run.Status.StartedAt = &startedAt
	}
	if terminalRunState(state) && run.Status.FinishedAt == nil {
		finishedAt := metav1.Now()
		run.Status.FinishedAt = &finishedAt
	}
	environmentReason, environmentMessage := reason, message
	if environmentReady {
		environmentReason, environmentMessage = "EnvironmentReady", "sandboxd is ready"
	}
	apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type: runConditionEnvironmentReady, Status: boolConditionStatus(environmentReady), Reason: environmentReason, Message: environmentMessage, ObservedGeneration: run.Generation,
	})
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

func adapterTask(run *platformv1alpha1.Run) agent.AdapterTask {
	return agent.AdapterTask{ID: string(run.UID), Prompt: run.Spec.Prompt}
}

func (r *RunReconciler) adapterSandbox(run *platformv1alpha1.Run, env *platformv1alpha1.Environment, fence lifecycle.ExecutionFence, fenceRejections *fenceRejectionRecorder) agent.AdapterSandbox {
	sandbox := agent.AdapterSandbox{EnvironmentName: env.Name, EnvironmentUID: agent.EnvironmentUID(env.UID),
		DialProcess: func(ctx context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
			reader := r.apiReader()
			// Mandatory uncached pre-call association proof: a still-current
			// execution must not accept work after this Run lost its exact owned or
			// claimed allocation. The connector separately revalidates its opaque
			// complete fence before leasing a pooled physical connection.
			current, err := getAllocatedEnvironment(ctx, reader, run)
			if err != nil {
				if current != nil {
					fenceRejections.observe(fence.Validate(current))
				}
				return nil, nil, err
			}
			if err := fence.Validate(current); err != nil {
				fenceRejections.observe(err)
				return nil, nil, err
			}
			process, closeProcess, err := r.connector().DialProcess(ctx, fence)
			fenceRejections.observe(err)
			return process, closeProcess, err
		}}
	if r.EventSink != nil {
		runUID := string(run.UID)
		sandbox.EmitEvent = func(ctx context.Context, event agent.AdapterEvent) error {
			return r.EventSink.Append(ctx, run.Namespace, run.Name, runUID, event)
		}
	}
	return sandbox
}

func (r *RunReconciler) resolveAllocatedExecution(ctx context.Context, run *platformv1alpha1.Run, expected *platformv1alpha1.Environment, fenceRejections *fenceRejectionRecorder) (sandboxclient.Execution, bool, error) {
	fence := lifecycle.CaptureExecutionFence(expected)
	// Mandatory uncached pre-call association proof. ResolveExecution then
	// proves the connector-private exact Pod execution used for the later
	// post-adapter currentness comparison.
	current, err := getAllocatedEnvironment(ctx, r.apiReader(), run)
	if apierrors.IsNotFound(err) || errors.Is(err, errAllocatedEnvironmentGone) {
		if current != nil {
			fenceRejections.observe(fence.Validate(current))
		}
		return sandboxclient.Execution{}, false, nil
	}
	if err != nil {
		return sandboxclient.Execution{}, false, err
	}
	if err := fence.Validate(current); err != nil {
		fenceRejections.observe(err)
		return sandboxclient.Execution{}, false, nil
	}
	if !environmentReachable(current) {
		return sandboxclient.Execution{}, false, nil
	}
	execution, err := r.connector().ResolveExecution(ctx, fence)
	if err != nil {
		fenceRejections.observe(err)
		return sandboxclient.Execution{}, false, err
	}
	return execution, true, nil
}

func (r *RunReconciler) allocatedExecutionCurrent(ctx context.Context, run *platformv1alpha1.Run, expected *platformv1alpha1.Environment, execution sandboxclient.Execution, fenceRejections *fenceRejectionRecorder) (bool, error) {
	fence := lifecycle.CaptureExecutionFence(expected)
	// Mandatory uncached post-call association proof: adapter results cannot be
	// published after allocation moved, even when the old execution still
	// answers. ExecutionCurrent adds the exact live backend proof.
	current, err := getAllocatedEnvironment(ctx, r.apiReader(), run)
	if apierrors.IsNotFound(err) || errors.Is(err, errAllocatedEnvironmentGone) {
		if current != nil {
			fenceRejections.observe(fence.Validate(current))
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := fence.Validate(current); err != nil {
		fenceRejections.observe(err)
		return false, nil
	}
	if !environmentReachable(current) {
		return false, nil
	}
	currentExecution, err := r.connector().ExecutionCurrent(ctx, fence, execution)
	if errors.Is(err, lifecycle.ErrExecutionFenceChanged) {
		fenceRejections.observe(err)
		return false, nil
	}
	return currentExecution, err
}

func (r *RunReconciler) connector() sandboxclient.Connector {
	if r.Connector.Reader != nil {
		return r.Connector
	}
	return sandboxclient.Connector{Reader: r.apiReader()}
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
	oldSpec := oldEnvironment.Spec.DeepCopy()
	newSpec := newEnvironment.Spec.DeepCopy()
	oldSpec.Lifecycle.Activity = nil
	newSpec.Lifecycle.Activity = nil
	oldStatus := oldEnvironment.Status.DeepCopy()
	newStatus := newEnvironment.Status.DeepCopy()
	oldStatus.LastActiveAt = nil
	newStatus.LastActiveAt = nil
	oldStatus.Lifecycle.ActivityReceipts = nil
	newStatus.Lifecycle.ActivityReceipts = nil
	oldStatus.ServiceObservations = nil
	newStatus.ServiceObservations = nil
	return oldEnvironment.Generation != newEnvironment.Generation && !reflect.DeepEqual(*oldSpec, *newSpec) ||
		oldEnvironment.UID != newEnvironment.UID ||
		!reflect.DeepEqual(oldEnvironment.DeletionTimestamp, newEnvironment.DeletionTimestamp) ||
		!reflect.DeepEqual(oldEnvironment.OwnerReferences, newEnvironment.OwnerReferences) ||
		!reflect.DeepEqual(oldEnvironment.Labels, newEnvironment.Labels) ||
		!reflect.DeepEqual(*oldStatus, *newStatus)
}
