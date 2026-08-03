package controllers

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
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
	legacyMigration bool
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
	{name: "recovery-migration", run: reconcileEnvironmentRecoveryMigrationGate},
	{name: "lifecycle", run: reconcileEnvironmentLifecycleGate},
	{name: "legacy-provisioning-migration", run: reconcileEnvironmentLegacyProvisioningMigrationGate},
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
			fenceCtx, fenceClaim, fenceErr := r.Scope.BeginEnvironmentStaleProjectFence(state.ctx, env.Namespace, env.Name, env.UID, tenancy.LifecycleActive, tenancy.LifecycleFencing)
			if fenceErr != nil {
				if stderrors.Is(fenceErr, tenancy.ErrOutOfScope) {
					return phaseHandled(ctrl.Result{}, nil)
				}
				return phaseHandled(ctrl.Result{}, fenceErr)
			}
			state.ctx = fenceCtx
			state.namespaceClaim = fenceClaim
			message := fmt.Sprintf("namespace Project claim incarnation %s (%s) is missing, replaced, deleting, or not sole; execution fenced", fenceClaim.ProjectName, fenceClaim.ProjectUID)
			result, fenceErr := r.reconcileInvalidProvisioningConfiguration(fenceCtx, env, message)
			return phaseHandled(result, fenceErr)
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

func reconcileEnvironmentRecoveryMigrationGate(r *EnvironmentReconciler, ctx context.Context, state *environmentReconcileState) environmentPhaseOutcome {
	if result, handled, err := r.reconcilePodRecoveryMigration(ctx, &state.env); handled || err != nil {
		return phaseHandled(result, err)
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

func reconcileEnvironmentLegacyProvisioningMigrationGate(r *EnvironmentReconciler, ctx context.Context, state *environmentReconcileState) environmentPhaseOutcome {
	env := &state.env
	if env.Status.Provisioning != nil && env.Status.Provisioning.TemplateVerified &&
		(env.Status.Provisioning.Project == nil || env.Status.Provisioning.ProjectVerified) {
		return phaseContinue()
	}
	// Recovery deadlines and exhaustion were persisted by an earlier controller
	// version and remain authoritative even when this Environment still needs a
	// provisioning snapshot. Enforce them before migration can fence children
	// or publish a snapshot, except while suspended: lifecycle teardown and the
	// held migration path remain authoritative in that state. The normal
	// recovery phase runs later for already-provisioned Environments.
	if !env.Status.Lifecycle.Suspended {
		if result, handled, err := r.reconcilePendingPodRecovery(ctx, env); handled || err != nil {
			return phaseHandled(result, err)
		}
	}
	retained, result, handled, err := r.reconcileLegacyProvisioningMigration(ctx, env)
	state.legacyMigration = retained
	if handled || err != nil {
		return phaseHandled(result, err)
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
	// A legacy migration that has already removed its Pod and credential may
	// capture and verify current authoritative sources while held. It still
	// cannot reach backend provisioning, and the next reconcile restores Paused.
	if env.Status.Lifecycle.Suspended && (!state.legacyMigration || env.Status.Phase != platformv1alpha1.EnvironmentPhasePaused) {
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
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: env.Spec.TemplateRef}, &state.template); err != nil {
		wrapped := fmt.Errorf("get template %q: %w", env.Spec.TemplateRef, err)
		if errors.IsNotFound(err) {
			result, err := r.reconcileInvalidProvisioningConfiguration(ctx, env, wrapped.Error())
			return phaseHandled(result, err)
		}
		return phaseHandled(ctrl.Result{}, r.fail(ctx, env, wrapped))
	}
	tmpl := &state.template
	if !tmpl.DeletionTimestamp.IsZero() {
		result, err := r.reconcileInvalidProvisioningConfiguration(ctx, env, fmt.Sprintf("environment template %q is deleting", tmpl.Name))
		return phaseHandled(result, err)
	}
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
	// A source error after physical fencing is stable in Failed. Resume an active
	// migration only after all current authoritative inputs validate, immediately
	// before the one-time snapshot publication.
	if state.legacyMigration && env.Status.Provisioning == nil && !env.Status.Lifecycle.Suspended && env.Status.Phase != platformv1alpha1.EnvironmentPhaseResuming {
		if err := r.setEnvironmentStatus(ctx, env, platformv1alpha1.EnvironmentPhaseResuming, "", "", "ProvisioningMigration", "legacy execution is fenced before current provisioning sources are frozen"); err != nil {
			return phaseHandled(ctrl.Result{}, err)
		}
		return phaseHandled(ctrl.Result{Requeue: true}, nil)
	}
	if handled, err := r.publishProvisioningSnapshot(ctx, env, &state.template, state.project, state.namespaceClaim, state.legacyMigration); handled || err != nil {
		return phaseHandled(ctrl.Result{Requeue: err == nil}, err)
	}
	return phaseContinue()
}

func (r *EnvironmentReconciler) publishProvisioningSnapshot(ctx context.Context, env *platformv1alpha1.Environment, tmpl *platformv1alpha1.EnvironmentTemplate, project *platformv1alpha1.Project, claim tenancy.Claim, legacyMigration bool) (bool, error) {
	current := env.Status.Provisioning
	if current != nil {
		// A missing Project is the sole valid partial state: a warm environment may
		// be bound once after its initial snapshot. Everything else fails closed.
		if err := platformv1alpha1.ValidateEnvironmentProvisioningTemplateSnapshot(env, current); err != nil {
			_, failErr := r.reconcileInvalidProvisioningConfiguration(ctx, env, err.Error())
			return true, failErr
		}
		if !current.TemplateVerified {
			return r.verifyProvisioningSnapshot(ctx, env, tmpl, project, claim, false)
		}
		if current.Template.Name != tmpl.Name || current.Template.UID != tmpl.UID {
			_, err := r.reconcileInvalidProvisioningConfiguration(ctx, env, "provisioning snapshot template incarnation does not match the live managed template")
			return true, err
		}
		if env.Spec.ProjectRef == "" {
			if current.Project != nil {
				return true, r.fail(ctx, env, fmt.Errorf("projectless environment has a project provisioning snapshot"))
			}
			return false, nil
		}
		if project == nil {
			return false, nil
		}
		if current.Project != nil {
			if current.Project.Name != project.Name || current.Project.UID != project.UID {
				_, err := r.reconcileInvalidProvisioningConfiguration(ctx, env, "provisioning snapshot project incarnation does not match the bound Project")
				return true, err
			}
			if !current.ProjectVerified {
				return r.verifyProvisioningSnapshot(ctx, env, tmpl, project, claim, true)
			}
			return false, nil
		}
		nextValue := *current
		nextValue.Resources = make(map[string]resource.Quantity, len(current.Resources))
		for name, quantity := range current.Resources {
			nextValue.Resources[name] = quantity.DeepCopy()
		}
		next := &nextValue
		next.Project = platformv1alpha1.ResolveEnvironmentProvisioning(env, tmpl, project).Project
		next.ProjectVerified = false
		if err := platformv1alpha1.ValidateEnvironmentProvisioningTemplateSnapshot(env, next); err != nil {
			_, failErr := r.reconcileInvalidProvisioningConfiguration(ctx, env, err.Error())
			return true, failErr
		}
		currentSources, err := r.provisioningSourcesCurrent(ctx, env, tmpl, project, claim)
		if err != nil || !currentSources {
			return true, err
		}
		before := env.DeepCopy()
		env.Status.Provisioning = next
		if err := r.Status().Patch(ctx, env, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			if errors.IsConflict(err) {
				return true, nil
			}
			return true, err
		}
		return true, nil
	}
	// Legacy execution is fenced before current authoritative sources are
	// captured. Exact-owned PVC and NetworkPolicy children are retained and may
	// participate in that one-time migration; Pod and credential presence still
	// fails closed here so no snapshot can be published before teardown.
	legacyWorkspacePVCUID := types.UID("")
	legacyChildren := []struct {
		kind     string
		name     string
		object   client.Object
		retained bool
	}{
		{kind: "Pod", name: envPodName(env), object: &corev1.Pod{}},
		{kind: "PersistentVolumeClaim", name: envPVCName(env), object: &corev1.PersistentVolumeClaim{}},
		{kind: "Secret", name: envCredentialName(env), object: &corev1.Secret{}},
		{kind: "NetworkPolicy", name: envNetworkPolicyName(env), object: &networkingv1.NetworkPolicy{}},
	}
	legacyChildren[1].retained = true
	legacyChildren[3].retained = true
	for _, child := range legacyChildren {
		if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: child.name}, child.object); err == nil {
			if !exactControllerOwner(child.object, platformv1alpha1.GroupVersion.String(), "Environment", env.Name, env.UID) {
				return true, r.fail(ctx, env, &childOwnershipCollisionError{kind: child.kind, name: child.name})
			}
			if child.kind == "PersistentVolumeClaim" && !child.object.GetDeletionTimestamp().IsZero() {
				return true, r.fail(ctx, env, terminalEnvironment(fmt.Errorf("legacy workspace PVC %q is deleting; refusing provisioning migration", child.name)))
			}
			if child.kind == "PersistentVolumeClaim" {
				legacyWorkspacePVCUID = child.object.GetUID()
				if legacyWorkspacePVCUID != env.Status.ProvisioningMigrationPVCUID {
					return true, r.fail(ctx, env, terminalEnvironment(fmt.Errorf("legacy workspace PVC %q UID %q does not match migration UID %q", child.name, legacyWorkspacePVCUID, env.Status.ProvisioningMigrationPVCUID)))
				}
			}
			if !legacyMigration || !child.retained {
				_, failErr := r.reconcileInvalidProvisioningConfiguration(ctx, env, "legacy environment provisioning migration has not completed execution fencing")
				return true, failErr
			}
		} else if errors.IsNotFound(err) && legacyMigration && child.kind == "PersistentVolumeClaim" {
			return true, r.fail(ctx, env, terminalEnvironment(fmt.Errorf("legacy execution generation %d lost its retained workspace PVC; refusing replacement", env.Status.ExecutionGeneration)))
		} else if !errors.IsNotFound(err) {
			return true, err
		}
	}
	currentSources, err := r.provisioningSourcesCurrent(ctx, env, tmpl, project, claim)
	if err != nil || !currentSources {
		return true, err
	}
	before := env.DeepCopy()
	candidate := platformv1alpha1.ResolveEnvironmentProvisioning(env, tmpl, project)
	candidate.LegacyWorkspacePVCUID = env.Status.ProvisioningMigrationPVCUID
	if err := platformv1alpha1.ValidateEnvironmentProvisioningTemplateSnapshot(env, candidate); err != nil {
		_, failErr := r.reconcileInvalidProvisioningConfiguration(ctx, env, err.Error())
		return true, failErr
	}
	env.Status.Provisioning = candidate
	if err := r.Status().Patch(ctx, env, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		if errors.IsConflict(err) {
			return true, nil
		}
		return true, err
	}
	return true, nil
}

func (r *EnvironmentReconciler) verifyProvisioningSnapshot(ctx context.Context, env *platformv1alpha1.Environment, tmpl *platformv1alpha1.EnvironmentTemplate, project *platformv1alpha1.Project, claim tenancy.Claim, verifyProject bool) (bool, error) {
	current, err := r.provisioningSourcesCurrent(ctx, env, tmpl, project, claim)
	if err != nil {
		return true, err
	}
	projection := platformv1alpha1.ResolveEnvironmentProvisioning(env, tmpl, project)
	projection.LegacyWorkspacePVCUID = env.Status.Provisioning.LegacyWorkspacePVCUID
	if verifyProject {
		// Warm snapshots may remain claimable across policy-only Template edits.
		// Preserve the exact captured generation while comparing the current
		// normalized provisioning projection.
		projection.Template.Generation = env.Status.Provisioning.Template.Generation
	}
	if !current || env.Status.Provisioning.Template.Name != projection.Template.Name || env.Status.Provisioning.Template.UID != projection.Template.UID ||
		!verifyProject && env.Status.Provisioning.Template.Generation != projection.Template.Generation ||
		!platformv1alpha1.ProvisioningSnapshotsEqualIgnoringVerification(env.Status.Provisioning, projection) {
		_, failErr := r.reconcileInvalidProvisioningConfiguration(ctx, env, "pending provisioning snapshot no longer matches its exact source projection; recreate the Environment")
		return true, failErr
	}
	before := env.DeepCopy()
	env.Status.Provisioning.TemplateVerified = true
	if verifyProject {
		env.Status.Provisioning.ProjectVerified = true
	}
	if err := r.Status().Patch(ctx, env, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil && !errors.IsConflict(err) {
		return true, err
	}
	return true, nil
}

func (r *EnvironmentReconciler) provisioningSourcesCurrent(ctx context.Context, env *platformv1alpha1.Environment, tmpl *platformv1alpha1.EnvironmentTemplate, project *platformv1alpha1.Project, claim tenancy.Claim) (bool, error) {
	var confirmedEnv platformv1alpha1.Environment
	if err := r.apiReader().Get(ctx, client.ObjectKeyFromObject(env), &confirmedEnv); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if confirmedEnv.UID != env.UID || confirmedEnv.Generation != env.Generation || confirmedEnv.Spec.TemplateRef != env.Spec.TemplateRef || confirmedEnv.Spec.ProjectRef != env.Spec.ProjectRef || confirmedEnv.Spec.Backend != env.Spec.Backend {
		return false, nil
	}
	var confirmedTemplate platformv1alpha1.EnvironmentTemplate
	if err := r.apiReader().Get(ctx, client.ObjectKeyFromObject(tmpl), &confirmedTemplate); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if confirmedTemplate.UID != tmpl.UID || confirmedTemplate.Generation != tmpl.Generation || !confirmedTemplate.DeletionTimestamp.IsZero() {
		return false, nil
	}
	if tenancy.IsCatalogSource(&confirmedTemplate) {
		return false, nil
	}
	if r.Scope != nil && r.Scope.Verifier != nil && tenancy.ValidateManagedTemplate(&confirmedTemplate, r.Scope.Verifier.Installation, claim) != nil {
		return false, nil
	}
	if project == nil {
		return confirmedEnv.Spec.ProjectRef == "", nil
	}
	var confirmedProject platformv1alpha1.Project
	if err := r.apiReader().Get(ctx, client.ObjectKeyFromObject(project), &confirmedProject); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if confirmedProject.UID != project.UID || confirmedProject.Generation != project.Generation || !confirmedProject.DeletionTimestamp.IsZero() || len(confirmedProject.Spec.Repositories) != 1 || confirmedProject.Spec.Repositories[0] != project.Spec.Repositories[0] {
		return false, nil
	}
	return true, nil
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
	backend := env.Status.Provisioning.Backend
	if backend != platformv1alpha1.EnvironmentBackendPod {
		result, err := r.reconcileUnsupportedBackend(ctx, env, backend)
		return phaseHandled(result, err)
	}
	runtimeClassName := env.Status.Provisioning.RuntimeClassName
	if runtimeClassName != "" {
		var runtimeClass nodev1.RuntimeClass
		if err := r.apiReader().Get(ctx, types.NamespacedName{Name: runtimeClassName}, &runtimeClass); err != nil {
			wrapped := fmt.Errorf("get RuntimeClass %q required by template %q: %w", runtimeClassName, tmpl.Name, err)
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
				message := fmt.Sprintf("environment pod RuntimeClass %q incarnation does not match the current RuntimeClass; execution must be replaced", runtimeClassName)
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

	pod, err := r.ensurePodForProject(ctx, env, tmpl, state.project, state.runtimeClassUID, state.namespaceClaim)
	if stderrors.Is(err, errPodReplacing) {
		return phaseHandled(ctrl.Result{Requeue: true}, nil)
	}
	if stderrors.Is(err, errRepositoryCredentialPending) {
		return phaseHandled(ctrl.Result{RequeueAfter: repositoryCredentialRequeueDelay}, nil)
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
		if stderrors.Is(err, errEnvironmentExecutionChanged) {
			return phaseHandled(ctrl.Result{Requeue: true}, nil)
		}
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
