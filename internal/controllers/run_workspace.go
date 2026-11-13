package controllers

import (
	"context"
	"fmt"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ensureRunWorkspace runs only on execution paths. Cleanup retains its existing
// exact ownership authority even when storage is gone.
func (r *RunReconciler) ensureRunWorkspace(ctx context.Context, run *platformv1alpha1.Run) (bool, error) {
	var env platformv1alpha1.Environment
	ref := run.Status.EnvironmentRef
	if err := r.apiReader().Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: ref.Name}, &env); err != nil {
		return false, err
	}
	if env.UID != ref.UID || !env.DeletionTimestamp.IsZero() {
		return false, terminalEnvironment(fmt.Errorf("workspace Environment identity is unavailable"))
	}
	if ready, err := r.runWorkspaceCurrent(ctx, run, &env); err != nil || !ready {
		return false, err
	}
	if env.Status.Workspace == nil || run.Status.WorkspaceRef != nil || runMayHaveAccepted(run) || run.Status.StartedAt != nil {
		return true, nil // never backfill legacy or previously accepted Runs
	}
	if _, err := getAllocatedEnvironment(ctx, r.apiReader(), run); err != nil {
		return false, err
	}
	before := run.DeepCopy()
	run.Status.WorkspaceRef = &platformv1alpha1.RunWorkspaceReference{EnvironmentUID: env.UID, Incarnation: env.Status.Workspace.Incarnation}
	return false, r.Status().Patch(ctx, run, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

// Rechecked around execution calls without changing cleanup's authority.
func (r *RunReconciler) runWorkspaceCurrent(ctx context.Context, run *platformv1alpha1.Run, env *platformv1alpha1.Environment) (bool, error) {
	w := env.Status.Workspace
	if w == nil {
		if run.Status.WorkspaceRef != nil {
			return false, terminalEnvironment(fmt.Errorf("Run workspace proof is unavailable"))
		}
		return true, nil // legacy operation; never infer an association
	}
	if w.PVCRef == nil {
		return false, nil
	}
	var pvc corev1.PersistentVolumeClaim
	if err := r.apiReader().Get(ctx, client.ObjectKey{Namespace: env.Namespace, Name: w.PVCRef.Name}, &pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return false, terminalEnvironment(fmt.Errorf("Run workspace storage is missing"))
		}
		return false, err
	}
	if !platformv1alpha1.WorkspacePVCMatches(env, &pvc) {
		return false, terminalEnvironment(fmt.Errorf("Run workspace storage identity changed"))
	}
	if association := run.Status.WorkspaceRef; association != nil {
		if association.EnvironmentUID != env.UID || association.Incarnation != w.Incarnation {
			return false, terminalEnvironment(fmt.Errorf("Run workspace association changed"))
		}
	}
	return true, nil
}
