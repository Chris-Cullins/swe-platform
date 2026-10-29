package controlplane

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/isolation"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// FollowUpInspection is advisory storage eligibility, never a reservation or a
// promise of execution. No task, credential, filesystem or raw status text is returned.
type FollowUpInspection struct {
	Namespace                  string `json:"namespace"`
	NamespaceUID               string `json:"namespaceUID"`
	SourceName                 string `json:"sourceName"`
	SourceUID                  string `json:"sourceUID"`
	Reason                     string `json:"reason"`
	EnvironmentName            string `json:"environmentName,omitempty"`
	EnvironmentUID             string `json:"environmentUID,omitempty"`
	WorkspaceIncarnation       string `json:"workspaceIncarnation,omitempty"`
	SuspensionReason           string `json:"suspensionReason,omitempty"`
	HoldPolicyRevision         int64  `json:"holdPolicyRevision"`
	LastSuspendRequestSequence int64  `json:"lastSuspendRequestSequence"`
}

type followUpInspector interface {
	InspectFollowUp(context.Context, string, string, string, func(string) error) (FollowUpInspection, error)
}

func (s *Server) handleInspectFollowUp(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	r = r.WithContext(ctx)
	if !s.authorizeResource(w, r, ResourceAccess{Namespace: namespace, Resource: "runs", Verb: "get", Name: name}, true) {
		return
	}
	uid := strings.TrimSpace(r.Header.Get(RunUIDHeader))
	if uid == "" || len(uid) > maxRunUIDLength {
		writeProblem(w, http.StatusBadRequest, "run-uid-required", "Exact Run required", "provide an exact Run UID of at most 128 bytes")
		return
	}
	service, ok := s.resources.(followUpInspector)
	if !ok {
		writeProblem(w, http.StatusServiceUnavailable, "resource-service-unavailable", "Resource service unavailable", "inspection is not configured")
		return
	}
	result, err := service.InspectFollowUp(ctx, namespace, name, uid, func(target string) error {
		for _, verb := range []string{"get", "use"} {
			if s.access == nil || s.access.Authorize(r, ResourceAccess{Namespace: namespace, Resource: "environments", Verb: verb, Name: target}, true) != nil {
				return errForbidden
			}
		}
		return nil
	})
	if errors.Is(err, errForbidden) {
		writeProblem(w, http.StatusForbidden, "forbidden", "Access denied", "inspection requires named Environment read and execution-use permission")
		return
	}
	if errors.Is(err, errRunUIDConflict) {
		writeProblem(w, http.StatusConflict, "run-uid-conflict", "Run identity changed", "the requested Run identity is unavailable")
		return
	}
	if err != nil {
		s.writeResourceError(w, "inspect follow-up", namespace, name, err)
		return
	}
	if !s.authorizeResource(w, r, ResourceAccess{Namespace: namespace, Resource: "runs", Verb: "get", Name: name}, true) {
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *KubernetesResourceService) InspectFollowUp(ctx context.Context, namespace, name, uid string, authorizeTarget func(string) error) (FollowUpInspection, error) {
	var run platformv1alpha1.Run
	if err := s.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &run); err != nil {
		return FollowUpInspection{}, err
	}
	if string(run.UID) != uid || !run.DeletionTimestamp.IsZero() {
		return FollowUpInspection{}, errRunUIDConflict
	}
	var ns corev1.Namespace
	if err := s.Client.Get(ctx, client.ObjectKey{Name: namespace}, &ns); err != nil {
		return FollowUpInspection{}, err
	}
	result := FollowUpInspection{Namespace: namespace, NamespaceUID: string(ns.UID), SourceName: name, SourceUID: uid, Reason: "WorkspaceUnverifiable"}
	ref := run.Status.EnvironmentRef
	if ref == nil {
		return result, nil
	}
	if err := authorizeTarget(ref.Name); err != nil {
		return FollowUpInspection{}, err
	}
	var env platformv1alpha1.Environment
	if err := s.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, &env); err != nil {
		if apierrors.IsNotFound(err) {
			result.Reason = "WorkspaceUnavailable"
			return result, nil
		}
		return FollowUpInspection{}, err
	}
	if env.UID != ref.UID || !env.DeletionTimestamp.IsZero() {
		result.Reason = "WorkspaceUnavailable"
		return result, nil
	}
	// Every disclosed target identity is independently authorized and exact.
	result.EnvironmentName, result.EnvironmentUID = env.Name, string(env.UID)
	association, workspace := run.Status.WorkspaceRef, env.Status.Workspace
	if association == nil || workspace == nil || workspace.PVCRef == nil || association.EnvironmentUID != env.UID || association.Incarnation != workspace.Incarnation {
		return result, nil
	}
	result.WorkspaceIncarnation = workspace.Incarnation
	result.Reason = "SourceNotTerminal"
	switch run.Status.State {
	case platformv1alpha1.RunStateSucceeded, platformv1alpha1.RunStateFailed, platformv1alpha1.RunStateCancelled:
	default:
		return result, nil
	}
	result.Reason = "OwnedEnvironmentUnsupported"
	if ref.Ownership != platformv1alpha1.EnvironmentOwnershipClaimed || metav1.GetControllerOf(&env) != nil {
		return result, nil
	}
	result.Reason = "SourceCleanupPending"
	released := apiMeta.FindStatusCondition(run.Status.Conditions, "EnvironmentReady")
	if run.Generation <= 0 || run.Status.ObservedGeneration != run.Generation || released == nil || released.ObservedGeneration != run.Generation || released.Status != metav1.ConditionFalse || released.Reason != "EnvironmentReleased" || env.Status.Lifecycle.PendingSuspendRequestID != "" {
		return result, nil
	}
	result.Reason = "EnvironmentBusy"
	if env.Status.ClaimedBy != nil {
		return result, nil
	}
	var pvc corev1.PersistentVolumeClaim
	if err := s.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: workspace.PVCRef.Name}, &pvc); err != nil {
		if apierrors.IsNotFound(err) {
			result.Reason = "WorkspaceUnavailable"
			return result, nil
		}
		return FollowUpInspection{}, err
	}
	result.Reason = "WorkspaceUnavailable"
	if !platformv1alpha1.WorkspacePVCMatches(&env, &pvc) {
		return result, nil
	}
	result.Reason = "AuthorityUnavailable"
	if env.Generation <= 0 || env.Status.ObservedGeneration != env.Generation || !ns.DeletionTimestamp.IsZero() {
		return result, nil
	}
	if platformv1alpha1.ValidateEnvironmentProvisioningSnapshot(&env, env.Status.Provisioning) != nil {
		return result, nil
	}
	// Authorization proves the current active namespace claim, not isolation
	// selection. Pin the startup Installation identity and include it in the final
	// drift proof; never derive authority from a same-name replacement.
	identity := s.Installation
	if identity.UID == "" || identity.Key.Namespace == "" || identity.Key.Name == "" ||
		ns.Annotations[tenancy.InstallationNamespaceAnnotation] != identity.Key.Namespace ||
		ns.Annotations[tenancy.InstallationNameAnnotation] != identity.Key.Name ||
		ns.Annotations[tenancy.InstallationUIDAnnotation] != string(identity.UID) ||
		ns.Annotations[tenancy.LifecycleAnnotation] != string(tenancy.LifecycleActive) {
		return result, nil
	}
	var installation platformv1alpha1.Installation
	if err := s.Client.Get(ctx, identity.Key, &installation); err != nil {
		if apierrors.IsNotFound(err) {
			return result, nil
		}
		return FollowUpInspection{}, err
	}
	if installation.UID != identity.UID || !installation.DeletionTimestamp.IsZero() {
		return result, nil
	}
	if allowed, err := isolation.ExecutionAllowed(&installation); err != nil || !allowed {
		return result, nil
	}
	claim := tenancy.Claim{NamespaceUID: ns.UID, ProjectName: ns.Annotations[tenancy.ProjectNameAnnotation], ProjectUID: types.UID(ns.Annotations[tenancy.ProjectUIDAnnotation])}
	if claim.ProjectName == "" || claim.ProjectUID == "" {
		return result, nil
	}
	observed := []client.Object{&run, &env, &ns, &pvc, &installation}
	var template platformv1alpha1.EnvironmentTemplate
	if err := s.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: env.Status.Provisioning.Template.Name}, &template); err != nil {
		if apierrors.IsNotFound(err) {
			return result, nil
		}
		return FollowUpInspection{}, err
	}
	if template.UID != env.Status.Provisioning.Template.UID || !template.DeletionTimestamp.IsZero() || tenancy.ValidateManagedTemplate(&template, identity, claim) != nil {
		return result, nil
	}
	observed = append(observed, &template)
	if selected := env.Status.Provisioning.Project; selected != nil {
		var project platformv1alpha1.Project
		if err := s.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: selected.Name}, &project); err != nil {
			if apierrors.IsNotFound(err) {
				return result, nil
			}
			return FollowUpInspection{}, err
		}
		if project.UID != selected.UID || project.Name != claim.ProjectName || project.UID != claim.ProjectUID || !project.DeletionTimestamp.IsZero() || len(project.Spec.EgressAllowlist) != 0 {
			return result, nil
		}
		observed = append(observed, &project)
	}
	result.Reason = "EligibleActive"
	if env.Spec.Lifecycle.Hold != nil {
		result.HoldPolicyRevision = env.Spec.Lifecycle.Hold.Revision
	}
	result.LastSuspendRequestSequence = env.Status.Lifecycle.LastSuspendRequestSequence
	if env.Spec.Paused || env.Spec.Lifecycle.Hold != nil && env.Spec.Lifecycle.Hold.Enabled {
		result.Reason = "EnvironmentHeld"
	} else if env.Status.Lifecycle.Suspended {
		result.Reason = "EnvironmentHeld"
		if env.Status.Lifecycle.SuspensionReason == platformv1alpha1.EnvironmentSuspensionReasonIdle || env.Status.Lifecycle.SuspensionReason == platformv1alpha1.EnvironmentSuspensionReasonRequested {
			result.Reason = "WakeConsentRequired"
			result.SuspensionReason = string(env.Status.Lifecycle.SuspensionReason)
		}
	} else if !platformv1alpha1.IsEnvironmentReady(&env) {
		result.Reason = "AuthorityUnavailable"
	}
	// A fixed number of exact re-reads detects observed drift; it is not an
	// atomic snapshot across resources, and inspection never reserves anything.
	for _, original := range observed {
		current := original.DeepCopyObject().(client.Object)
		if err := s.Client.Get(ctx, client.ObjectKeyFromObject(original), current); err != nil {
			return FollowUpInspection{}, err
		}
		if current.GetUID() != original.GetUID() || current.GetResourceVersion() != original.GetResourceVersion() {
			return FollowUpInspection{}, apierrors.NewConflict(platformv1alpha1.GroupVersion.WithResource("runs").GroupResource(), name, errors.New("inspection authority changed"))
		}
	}
	if err := authorizeTarget(ref.Name); err != nil {
		return FollowUpInspection{}, err
	}
	return result, nil
}
