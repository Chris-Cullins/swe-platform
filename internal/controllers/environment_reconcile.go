package controllers

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
)

// Reconcile drives an Environment toward its desired state:
// pod + PVC present when active, pod deleted (PVC retained) when paused.
func (r *EnvironmentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	defer func() {
		if stderrors.Is(err, errEnvironmentIncarnationChanged) {
			result = ctrl.Result{}
			err = nil
		}
	}()

	state := &environmentReconcileState{ctx: ctx}
	if err := r.apiReader().Get(ctx, req.NamespacedName, &state.env); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	for _, phase := range environmentReconcilePhases {
		phaseLog := log.FromContext(state.ctx).WithValues("reconcilePhase", phase.name)
		phaseLog.V(1).Info("reconciling environment phase")
		outcome := phase.run(r, log.IntoContext(state.ctx, phaseLog), state)
		if outcome.err != nil || outcome.handled {
			return outcome.result, outcome.err
		}
	}
	return ctrl.Result{}, nil
}

type environmentReconcileState struct {
	ctx             context.Context
	env             platformv1alpha1.Environment
	namespaceClaim  tenancy.Claim
	project         *platformv1alpha1.Project
	projectErr      error
	template        platformv1alpha1.EnvironmentTemplate
	runtimeClassUID types.UID
	pod             *corev1.Pod
}

type environmentPhaseOutcome struct {
	handled bool
	result  ctrl.Result
	err     error
}

type environmentReconcilePhase struct {
	name string
	run  func(*EnvironmentReconciler, context.Context, *environmentReconcileState) environmentPhaseOutcome
}

func phaseContinue() environmentPhaseOutcome { return environmentPhaseOutcome{} }
func phaseHandled(result ctrl.Result, err error) environmentPhaseOutcome {
	return environmentPhaseOutcome{handled: true, result: result, err: err}
}

var environmentReconcilePhases = []environmentReconcilePhase{
	{name: "tenancy-fencing", run: reconcileEnvironmentTenancyFencing},
	{name: "deletion", run: reconcileEnvironmentDeletionGate},
	{name: "lifecycle", run: reconcileEnvironmentLifecycleGate},
	{name: "provisioning-fence", run: reconcileEnvironmentProvisioningFenceGate},
	{name: "project-resolution", run: reconcileEnvironmentProjectResolutionGate},
	{name: "suspension", run: reconcileEnvironmentSuspensionGate},
	{name: "project-validation", run: reconcileEnvironmentProjectValidationGate},
	{name: "template", run: reconcileEnvironmentTemplateGate},
	{name: "recovery", run: reconcileEnvironmentRecoveryGate},
	{name: "backend-runtime", run: reconcileEnvironmentBackendRuntimeGate},
	{name: "provisioning", run: reconcileEnvironmentProvisioningGate},
	{name: "status-idle", run: reconcileEnvironmentStatusIdleGate},
}

func reconcileEnvironmentTenancyFencing(r *EnvironmentReconciler, ctx context.Context, state *environmentReconcileState) environmentPhaseOutcome {
	env := &state.env
	leasedCtx, claim, err := r.Scope.Begin(state.ctx, env.Namespace, tenancy.LifecycleActive, tenancy.LifecycleFencing)
	if err != nil {
		if stderrors.Is(err, tenancy.ErrOutOfScope) {
			return phaseHandled(ctrl.Result{}, nil)
		}
		return phaseHandled(ctrl.Result{}, err)
	}
	state.ctx = leasedCtx
	state.namespaceClaim = claim
	ctx = log.IntoContext(leasedCtx, log.FromContext(ctx))
	if state.namespaceClaim.Lifecycle == tenancy.LifecycleFencing {
		if state.namespaceClaim.Operation != tenancy.OperationOffboarding {
			return phaseHandled(ctrl.Result{}, nil)
		}
		if env.DeletionTimestamp.IsZero() && (env.Spec.Lifecycle.Hold == nil || !env.Spec.Lifecycle.Hold.Enabled) {
			before := env.DeepCopy()
			revision := int64(1)
			if env.Spec.Lifecycle.Hold != nil {
				revision = env.Spec.Lifecycle.Hold.Revision + 1
			}
			env.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Enabled: true, Revision: revision}
			if err := r.Patch(ctx, env, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return phaseHandled(ctrl.Result{}, fmt.Errorf("fence Environment for Project offboarding: %w", err))
			}
			return phaseHandled(ctrl.Result{Requeue: true}, nil)
		}
	}
	return phaseContinue()
}

func reconcileEnvironmentDeletionGate(r *EnvironmentReconciler, ctx context.Context, state *environmentReconcileState) environmentPhaseOutcome {
	env := &state.env
	if !env.DeletionTimestamp.IsZero() {
		result, err := r.reconcileDeleting(ctx, env)
		return phaseHandled(result, err)
	}
	if !controllerutil.ContainsFinalizer(env, environmentFinalizer) {
		controllerutil.AddFinalizer(env, environmentFinalizer)
		if err := r.Update(ctx, env); err != nil {
			return phaseHandled(ctrl.Result{}, err)
		}
		return phaseHandled(ctrl.Result{Requeue: true}, nil)
	}
	return phaseContinue()
}

func reconcileEnvironmentLifecycleGate(r *EnvironmentReconciler, ctx context.Context, state *environmentReconcileState) environmentPhaseOutcome {
	env := &state.env
	if env.Spec.Paused {
		before := env.DeepCopy()
		if env.Spec.Lifecycle.Hold == nil {
			env.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Enabled: true, Revision: 1}
		} else if !env.Spec.Lifecycle.Hold.Enabled {
			env.Spec.Lifecycle.Hold.Enabled = true
			env.Spec.Lifecycle.Hold.Revision++
		}
		env.Spec.Paused = false
		if err := r.Patch(ctx, env, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return phaseHandled(ctrl.Result{}, fmt.Errorf("migrate deprecated paused intent: %w", err))
		}
		return phaseHandled(ctrl.Result{Requeue: true}, nil)
	}
	if request := env.Spec.Lifecycle.Suspend; request != nil &&
		(env.Status.Lifecycle.PendingSuspendRequestID != request.ID || env.Status.Lifecycle.LastSuspendRequestSequence != request.Sequence) &&
		(request.Sequence <= env.Status.Lifecycle.LastSuspendRequestSequence || request.ID == env.Status.Lifecycle.LastSuspendRequestID || !validLifecycleRequest(env, &request.EnvironmentLifecycleRequest)) {
		before := env.DeepCopy()
		env.Spec.Lifecycle.Suspend = nil
		if err := r.Patch(ctx, env, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return phaseHandled(ctrl.Result{}, fmt.Errorf("discard replayed suspension request: %w", err))
		}
		return phaseHandled(ctrl.Result{Requeue: true}, nil)
	}
	changed, err := r.reconcileLifecycleIntent(ctx, env)
	if err != nil {
		return phaseHandled(ctrl.Result{}, fmt.Errorf("reconcile lifecycle intent: %w", err))
	}
	if changed {
		return phaseHandled(ctrl.Result{Requeue: true}, nil)
	}
	return phaseContinue()
}

func reconcileEnvironmentProvisioningFenceGate(r *EnvironmentReconciler, ctx context.Context, state *environmentReconcileState) environmentPhaseOutcome {
	env := &state.env
	if invalidProvisioningFenceStarted(env) {
		if result, handled, err := r.reconcileInvalidProvisioningFence(ctx, env); handled || err != nil {
			return phaseHandled(result, err)
		}
	}
	return phaseContinue()
}

func reconcileEnvironmentProjectResolutionGate(r *EnvironmentReconciler, ctx context.Context, state *environmentReconcileState) environmentPhaseOutcome {
	env := &state.env
	state.project, state.projectErr = r.resolveEnvironmentProject(ctx, env)
	if state.project != nil && len(state.project.Spec.EgressAllowlist) != 0 {
		result, err := r.reconcileInvalidProvisioningConfiguration(ctx, env, unsupportedEgressAllowlistMessage(state.project.Name))
		return phaseHandled(result, err)
	}
	return phaseContinue()
}

func reconcileEnvironmentSuspensionGate(r *EnvironmentReconciler, ctx context.Context, state *environmentReconcileState) environmentPhaseOutcome {
	env := &state.env
	// Fencing must not depend on a still-readable template or successful setup.
	// Cancellation/finalization can therefore stop an execution domain even after
	// its template or Project was deleted or provisioning became permanently broken.
	if env.Status.Lifecycle.Suspended {
		result, err := r.reconcilePaused(ctx, env)
		if err != nil {
			return phaseHandled(ctrl.Result{}, r.fail(ctx, env, fmt.Errorf("pause environment: %w", err)))
		}
		return phaseHandled(result, nil)
	}
	return phaseContinue()
}

func reconcileEnvironmentProjectValidationGate(r *EnvironmentReconciler, ctx context.Context, state *environmentReconcileState) environmentPhaseOutcome {
	env := &state.env
	if state.projectErr != nil {
		var terminal *terminalEnvironmentError
		if stderrors.As(state.projectErr, &terminal) {
			result, err := r.reconcileInvalidProvisioningConfiguration(ctx, env, state.projectErr.Error())
			return phaseHandled(result, err)
		}
		return phaseHandled(ctrl.Result{}, r.fail(ctx, env, state.projectErr))
	}
	if err := validateEnvironmentProject(state.project); err != nil {
		return phaseHandled(ctrl.Result{}, r.fail(ctx, env, err))
	}
	return phaseContinue()
}

func reconcileEnvironmentTemplateGate(r *EnvironmentReconciler, ctx context.Context, state *environmentReconcileState) environmentPhaseOutcome {
	env := &state.env
	if strings.TrimSpace(env.Spec.TemplateRef) == "" {
		result, err := r.reconcileInvalidProvisioningConfiguration(ctx, env, "environment templateRef must not be blank")
		return phaseHandled(result, err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: env.Spec.TemplateRef}, &state.template); err != nil {
		wrapped := fmt.Errorf("get template %q: %w", env.Spec.TemplateRef, err)
		if errors.IsNotFound(err) {
			result, err := r.reconcileInvalidProvisioningConfiguration(ctx, env, wrapped.Error())
			return phaseHandled(result, err)
		}
		return phaseHandled(ctrl.Result{}, r.fail(ctx, env, wrapped))
	}
	tmpl := &state.template
	if tenancy.IsCatalogSource(tmpl) {
		result, err := r.reconcileInvalidProvisioningConfiguration(ctx, env, fmt.Sprintf("environment template %q is an inert installation catalog source", tmpl.Name))
		return phaseHandled(result, err)
	}
	if r.Scope != nil && r.Scope.Verifier != nil {
		if err := tenancy.ValidateManagedTemplate(tmpl, r.Scope.Verifier.Installation, state.namespaceClaim); err != nil {
			result, err := r.reconcileInvalidProvisioningConfiguration(ctx, env, err.Error())
			return phaseHandled(result, err)
		}
	}
	return phaseContinue()
}

func reconcileEnvironmentRecoveryGate(r *EnvironmentReconciler, ctx context.Context, state *environmentReconcileState) environmentPhaseOutcome {
	env := &state.env
	if result, handled, err := r.reconcilePendingPodRecovery(ctx, env); handled || err != nil {
		return phaseHandled(result, err)
	}
	return phaseContinue()
}

func reconcileEnvironmentBackendRuntimeGate(r *EnvironmentReconciler, ctx context.Context, state *environmentReconcileState) environmentPhaseOutcome {
	env, tmpl := &state.env, &state.template
	backend := platformv1alpha1.EffectiveEnvironmentBackend(env, tmpl)
	if backend != platformv1alpha1.EnvironmentBackendPod {
		result, err := r.reconcileUnsupportedBackend(ctx, env, backend)
		return phaseHandled(result, err)
	}
	if tmpl.Spec.RuntimeClass != "" {
		var runtimeClass nodev1.RuntimeClass
		if err := r.Get(ctx, types.NamespacedName{Name: tmpl.Spec.RuntimeClass}, &runtimeClass); err != nil {
			wrapped := fmt.Errorf("get RuntimeClass %q required by template %q: %w", tmpl.Spec.RuntimeClass, tmpl.Name, err)
			if errors.IsNotFound(err) {
				result, err := r.reconcileInvalidProvisioningConfiguration(ctx, env, wrapped.Error())
				return phaseHandled(result, err)
			}
			return phaseHandled(ctrl.Result{}, r.fail(ctx, env, wrapped))
		}
		state.runtimeClassUID = runtimeClass.UID
		var pod corev1.Pod
		if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: envPodName(env)}, &pod); err == nil {
			if exactControllerOwner(&pod, platformv1alpha1.GroupVersion.String(), "Environment", env.Name, env.UID) &&
				pod.Annotations[runtimeClassUIDAnnotation] != string(state.runtimeClassUID) {
				message := fmt.Sprintf("environment pod RuntimeClass %q incarnation does not match the current RuntimeClass; execution must be replaced", tmpl.Spec.RuntimeClass)
				result, err := r.reconcileInvalidProvisioningConfiguration(ctx, env, message)
				return phaseHandled(result, err)
			}
		} else if !errors.IsNotFound(err) {
			return phaseHandled(ctrl.Result{}, r.fail(ctx, env, fmt.Errorf("get environment pod before RuntimeClass validation: %w", err)))
		}
	}
	return phaseContinue()
}

func reconcileEnvironmentProvisioningGate(r *EnvironmentReconciler, ctx context.Context, state *environmentReconcileState) environmentPhaseOutcome {
	env, tmpl := &state.env, &state.template
	pvcReady, err := r.ensureWorkspacePVC(ctx, env, tmpl)
	if err != nil {
		return phaseHandled(ctrl.Result{}, r.fail(ctx, env, fmt.Errorf("ensure workspace PVC: %w", err)))
	}
	if !pvcReady {
		return phaseHandled(ctrl.Result{Requeue: true}, nil)
	}
	policyReady, err := r.ensureSandboxdNetworkPolicy(ctx, env)
	if err != nil {
		return phaseHandled(ctrl.Result{}, r.fail(ctx, env, fmt.Errorf("ensure sandboxd network policy: %w", err)))
	}
	if !policyReady {
		return phaseHandled(ctrl.Result{Requeue: true}, nil)
	}

	if env.Status.Phase == platformv1alpha1.EnvironmentPhasePaused {
		now := metav1.Now()
		env.Status.LastActiveAt = &now
		return phaseHandled(ctrl.Result{Requeue: true}, r.setPhase(ctx, env, platformv1alpha1.EnvironmentPhaseResuming, "", ""))
	}

	pod, err := r.ensurePodForProject(ctx, env, tmpl, state.project, state.runtimeClassUID)
	if stderrors.Is(err, errPodReplacing) {
		return phaseHandled(ctrl.Result{Requeue: true}, nil)
	}
	if err != nil {
		return phaseHandled(ctrl.Result{}, r.fail(ctx, env, fmt.Errorf("ensure pod: %w", err)))
	}
	if pod == nil {
		return phaseHandled(ctrl.Result{Requeue: true}, nil)
	}
	state.pod = pod
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		result, err := r.reconcileTerminalPod(ctx, env, pod)
		return phaseHandled(result, err)
	}
	return phaseContinue()
}

func reconcileEnvironmentStatusIdleGate(r *EnvironmentReconciler, ctx context.Context, state *environmentReconcileState) environmentPhaseOutcome {
	env, tmpl, pod := &state.env, &state.template, state.pod
	if err := r.syncStatus(ctx, env, pod); err != nil {
		return phaseHandled(ctrl.Result{}, err)
	}
	if pod.Status.Phase != corev1.PodRunning {
		return phaseHandled(ctrl.Result{}, nil)
	}
	result, err := r.reconcileIdle(ctx, env, tmpl)
	if errors.IsConflict(err) {
		// A concurrent claim or activity update won the optimistic pause race.
		return phaseHandled(ctrl.Result{Requeue: true}, nil)
	}
	if err != nil {
		return phaseHandled(ctrl.Result{}, r.fail(ctx, env, fmt.Errorf("reconcile idle policy: %w", err)))
	}
	return phaseHandled(result, nil)
}
