package controllers

import (
	"context"
	"encoding/hex"
	stderrors "errors"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/egresspolicy"
	"github.com/Chris-Cullins/swe-platform/internal/isolation"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
)

const (
	installationIsolationBlockedMessage = "installation isolation does not permit environment execution"
	isolationResolutionRetryDelay       = 5 * time.Second
)

var (
	errInstallationIsolationBlocked = stderrors.New(installationIsolationBlockedMessage)
	errIsolationDependenciesPending = stderrors.New("restricted isolation dependencies have not been resolved")
)

// InstallationIsolationReconciler owns the observed selection lifecycle for
// the one exact Installation loaded at operator startup. Restricted runtime
// activation is deliberately unavailable in this slice.
type InstallationIsolationReconciler struct {
	client.Client
	APIReader    client.Reader
	Installation tenancy.InstallationIdentity
	Mode         tenancy.Mode
	Namespaces   []string
}

// +kubebuilder:rbac:groups=swe.dev,resources=installations,verbs=get;list;watch
// +kubebuilder:rbac:groups=swe.dev,resources=installations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups=node.k8s.io,resources=runtimeclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses;csidrivers,verbs=get;list;watch

func (r *InstallationIsolationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if req.NamespacedName != r.Installation.Key {
		return ctrl.Result{}, nil
	}
	var installation platformv1alpha1.Installation
	if err := r.reader().Get(ctx, r.Installation.Key, &installation); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if installation.UID != r.Installation.UID || !installation.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	if installation.Spec.Isolation == nil {
		return r.reconcileLegacySelection(ctx, &installation)
	}

	selection := installation.Spec.Isolation.DeepCopy()
	if err := isolation.ValidateSelection(selection); err != nil {
		return r.reconcileBlockedSelection(ctx, &installation, nil, nil, err)
	}
	if selection.Mode == platformv1alpha1.InstallationIsolationModeUnrestrictedDevelopment {
		revision, err := (isolation.RevisionInputs{InstallationUID: installation.UID, Selection: *selection}).DeriveRevision()
		if err != nil {
			return r.reconcileBlockedSelection(ctx, &installation, nil, nil, err)
		}
		return r.reconcileUnrestrictedSelection(ctx, &installation, selection, revision)
	}

	// Publish the restricted intent and withdraw Active authority before any
	// dependency read can stall or fail. Resolution starts only after this
	// exact selection is durably observed in Fencing or Blocked.
	if !apiequality.Semantic.DeepEqual(installation.Status.ObservedIsolation, selection) ||
		installation.Status.IsolationState != platformv1alpha1.InstallationIsolationStateFencing && installation.Status.IsolationState != platformv1alpha1.InstallationIsolationStateBlocked {
		return r.reconcileBlockedSelection(ctx, &installation, nil, nil, errIsolationDependenciesPending)
	}
	resolved, identities, resolveErr := r.resolveRestrictedSelection(ctx, &installation, selection)
	return r.reconcileBlockedSelection(ctx, &installation, identities, resolved, resolveErr)
}

func (r *InstallationIsolationReconciler) reconcileLegacySelection(ctx context.Context, installation *platformv1alpha1.Installation) (ctrl.Result, error) {
	desiredLegacy := r.legacyStatus(installation, platformv1alpha1.InstallationIsolationStateLegacyUnclassified)
	if legacyCompatibleIsolationStatus(installation.Status) {
		_, err := r.updateStatus(ctx, installation, desiredLegacy)
		return ctrl.Result{}, err
	}
	desiredFencing := r.legacyStatus(installation, platformv1alpha1.InstallationIsolationStateFencing)
	changed, err := r.updateStatus(ctx, installation, desiredFencing)
	if err != nil {
		return ctrl.Result{}, err
	}
	if changed {
		return ctrl.Result{Requeue: true}, nil
	}
	fenced, err := r.allEnvironmentExecutionsFenced(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !fenced {
		return ctrl.Result{Requeue: true}, nil
	}
	changed, err = r.updateStatus(ctx, installation, desiredLegacy)
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: changed}, nil
}

func (r *InstallationIsolationReconciler) legacyStatus(installation *platformv1alpha1.Installation, state platformv1alpha1.InstallationIsolationState) platformv1alpha1.InstallationStatus {
	desired := r.baseStatus(installation, state, nil)
	readyReason := "LegacyUnclassified"
	readyMessage := "installation isolation selection is omitted"
	if state == platformv1alpha1.InstallationIsolationStateFencing {
		readyReason = "Fencing"
		readyMessage = "installation environment execution is being fenced"
	}
	r.setConditions(installation, &desired, true, "NotRequired", metav1.ConditionFalse, readyReason, readyMessage, "LegacyUnclassified", "legacy isolation is not classified for production")
	return desired
}

func (r *InstallationIsolationReconciler) reconcileUnrestrictedSelection(ctx context.Context, installation *platformv1alpha1.Installation, selection *platformv1alpha1.InstallationIsolationSpec, revision isolation.Revision) (ctrl.Result, error) {
	desiredActive := r.unrestrictedStatus(installation, platformv1alpha1.InstallationIsolationStateActive, selection, revision)
	if legacyCompatibleIsolationStatus(installation.Status) || unrestrictedAuthorityMatches(installation.Status, selection, revision) {
		_, err := r.updateStatus(ctx, installation, desiredActive)
		return ctrl.Result{}, err
	}

	desiredFencing := r.unrestrictedStatus(installation, platformv1alpha1.InstallationIsolationStateFencing, selection, revision)
	changed, err := r.updateStatus(ctx, installation, desiredFencing)
	if err != nil {
		return ctrl.Result{}, err
	}
	if changed {
		return ctrl.Result{Requeue: true}, nil
	}
	fenced, err := r.allEnvironmentExecutionsFenced(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !fenced {
		return ctrl.Result{Requeue: true}, nil
	}
	changed, err = r.updateStatus(ctx, installation, desiredActive)
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: changed}, nil
}

func (r *InstallationIsolationReconciler) unrestrictedStatus(installation *platformv1alpha1.Installation, state platformv1alpha1.InstallationIsolationState, selection *platformv1alpha1.InstallationIsolationSpec, revision isolation.Revision) platformv1alpha1.InstallationStatus {
	desired := r.baseStatus(installation, state, selection)
	desired.IsolationRevision = revision.String()
	readyStatus := metav1.ConditionFalse
	readyReason := "Fencing"
	readyMessage := "installation environment execution is being fenced"
	if state == platformv1alpha1.InstallationIsolationStateActive {
		readyStatus = metav1.ConditionTrue
		readyReason = "UnrestrictedDevelopment"
		readyMessage = "unrestricted development isolation is active"
	}
	r.setConditions(installation, &desired, true, "NotRequired", readyStatus, readyReason, readyMessage, "UnrestrictedDevelopment", "unrestricted development isolation is explicitly non-production")
	return desired
}

type restrictedIsolationIdentities struct {
	policy       *platformv1alpha1.InstallationPolicyConfigMapIdentity
	runtimeClass *platformv1alpha1.InstallationRuntimeClassIdentity
	storageClass *platformv1alpha1.InstallationStorageClassIdentity
	csiDriver    *platformv1alpha1.InstallationCSIDriverIdentity
}

func (r *InstallationIsolationReconciler) resolveRestrictedSelection(ctx context.Context, installation *platformv1alpha1.Installation, selection *platformv1alpha1.InstallationIsolationSpec) (*isolation.Revision, *restrictedIsolationIdentities, error) {
	var policyConfigMap corev1.ConfigMap
	policyKey := types.NamespacedName{Namespace: installation.Namespace, Name: selection.PolicyConfigMapName}
	if err := r.reader().Get(ctx, policyKey, &policyConfigMap); err != nil {
		return nil, nil, fmt.Errorf("resolve isolation policy ConfigMap: %w", err)
	}
	policy, err := egresspolicy.ParseConfigMap(&policyConfigMap)
	if err != nil {
		return nil, nil, fmt.Errorf("validate isolation policy ConfigMap: %w", err)
	}
	if policy.Mode != egresspolicy.ModeRestricted || policy.RestrictedProfile == nil || policy.RestrictedProfile.Name != egresspolicy.RestrictedProfileCalicoV1 {
		return nil, nil, stderrors.New("restricted installation isolation requires the canonical restricted Calico v3.32.1 policy")
	}

	var runtimeClass nodev1.RuntimeClass
	if err := r.reader().Get(ctx, types.NamespacedName{Name: selection.RuntimeClass.Name}, &runtimeClass); err != nil {
		return nil, nil, fmt.Errorf("resolve isolation RuntimeClass: %w", err)
	}
	if runtimeClass.UID == "" || !runtimeClass.DeletionTimestamp.IsZero() || runtimeClass.Handler != selection.RuntimeClass.Handler {
		return nil, nil, stderrors.New("RuntimeClass identity or handler does not match the restricted isolation selection")
	}

	var storageClass storagev1.StorageClass
	if err := r.reader().Get(ctx, types.NamespacedName{Name: selection.StorageClass.Name}, &storageClass); err != nil {
		return nil, nil, fmt.Errorf("resolve isolation StorageClass: %w", err)
	}
	if storageClass.UID == "" || !storageClass.DeletionTimestamp.IsZero() || storageClass.Provisioner != selection.StorageClass.CSIDriver {
		return nil, nil, stderrors.New("StorageClass identity or provisioner does not match the restricted isolation selection")
	}

	var csiDriver storagev1.CSIDriver
	if err := r.reader().Get(ctx, types.NamespacedName{Name: selection.StorageClass.CSIDriver}, &csiDriver); err != nil {
		return nil, nil, fmt.Errorf("resolve isolation CSIDriver: %w", err)
	}
	if csiDriver.UID == "" || !csiDriver.DeletionTimestamp.IsZero() {
		return nil, nil, stderrors.New("stable live CSIDriver identity does not match the restricted isolation selection")
	}

	inputs := isolation.RevisionInputs{
		InstallationUID: installation.UID,
		Selection:       *selection,
		PolicyConfigMap: &isolation.PolicyConfigMapIdentity{UID: policyConfigMap.UID, ContentSHA256: policy.ContentSHA256},
		RuntimeClass:    &isolation.RuntimeClassIdentity{UID: runtimeClass.UID, Handler: runtimeClass.Handler},
		StorageClass:    &isolation.StorageClassIdentity{UID: storageClass.UID, CSIDriver: storageClass.Provisioner},
		CSIDriver:       &isolation.CSIDriverIdentity{UID: csiDriver.UID},
	}
	revision, err := inputs.DeriveRevision()
	if err != nil {
		return nil, nil, fmt.Errorf("derive restricted isolation revision: %w", err)
	}
	identities := &restrictedIsolationIdentities{
		policy: &platformv1alpha1.InstallationPolicyConfigMapIdentity{
			Name: selection.PolicyConfigMapName, UID: policyConfigMap.UID,
			ContentSHA256: hex.EncodeToString(policy.ContentSHA256[:]),
		},
		runtimeClass: &platformv1alpha1.InstallationRuntimeClassIdentity{Name: runtimeClass.Name, UID: runtimeClass.UID, Handler: runtimeClass.Handler},
		storageClass: &platformv1alpha1.InstallationStorageClassIdentity{Name: storageClass.Name, UID: storageClass.UID, CSIDriver: storageClass.Provisioner},
		csiDriver:    &platformv1alpha1.InstallationCSIDriverIdentity{Name: csiDriver.Name, UID: csiDriver.UID},
	}
	return &revision, identities, nil
}

func (r *InstallationIsolationReconciler) reconcileBlockedSelection(ctx context.Context, installation *platformv1alpha1.Installation, identities *restrictedIsolationIdentities, revision *isolation.Revision, resolveErr error) (ctrl.Result, error) {
	selectionValid := isolation.ValidateSelection(installation.Spec.Isolation) == nil
	observed := installation.Spec.Isolation
	if !selectionValid {
		observed = nil
	}
	sameSelection := apiequality.Semantic.DeepEqual(installation.Status.ObservedIsolation, observed)
	desiredFencing := r.blockedStatus(installation, platformv1alpha1.InstallationIsolationStateFencing, observed, identities, revision, resolveErr)
	if !sameSelection || installation.Status.IsolationState != platformv1alpha1.InstallationIsolationStateFencing && installation.Status.IsolationState != platformv1alpha1.InstallationIsolationStateBlocked ||
		installation.Status.IsolationState == platformv1alpha1.InstallationIsolationStateBlocked && !sameIsolationDependencyAuthority(installation.Status, desiredFencing) {
		if _, err := r.updateStatus(ctx, installation, desiredFencing); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	fenced, fenceErr := r.allEnvironmentExecutionsFenced(ctx)
	state := platformv1alpha1.InstallationIsolationStateBlocked
	if fenceErr != nil || !fenced {
		state = platformv1alpha1.InstallationIsolationStateFencing
	}
	desired := r.blockedStatus(installation, state, observed, identities, revision, resolveErr)
	changed, err := r.updateStatus(ctx, installation, desired)
	if err != nil {
		return ctrl.Result{}, err
	}
	if fenceErr != nil {
		return ctrl.Result{}, fenceErr
	}
	if changed || !fenced {
		return ctrl.Result{Requeue: true}, nil
	}
	if resolveErr != nil {
		log.FromContext(ctx).Error(resolveErr, "restricted installation isolation dependencies are unresolved")
		return ctrl.Result{RequeueAfter: isolationResolutionRetryDelay}, nil
	}
	return ctrl.Result{}, nil
}

func sameIsolationDependencyAuthority(current, desired platformv1alpha1.InstallationStatus) bool {
	return current.IsolationRevision == desired.IsolationRevision &&
		apiequality.Semantic.DeepEqual(current.PolicyConfigMap, desired.PolicyConfigMap) &&
		apiequality.Semantic.DeepEqual(current.RuntimeClass, desired.RuntimeClass) &&
		apiequality.Semantic.DeepEqual(current.StorageClass, desired.StorageClass) &&
		apiequality.Semantic.DeepEqual(current.CSIDriver, desired.CSIDriver)
}

func legacyCompatibleIsolationStatus(status platformv1alpha1.InstallationStatus) bool {
	if status.ObservedIsolation != nil || status.IsolationRevision != "" || status.PolicyConfigMap != nil || status.RuntimeClass != nil || status.StorageClass != nil || status.CSIDriver != nil {
		return false
	}
	switch status.IsolationState {
	case "":
		return len(status.Conditions) == 0
	case platformv1alpha1.InstallationIsolationStateLegacyUnclassified:
		return true
	default:
		return false
	}
}

func unrestrictedAuthorityMatches(status platformv1alpha1.InstallationStatus, selection *platformv1alpha1.InstallationIsolationSpec, revision isolation.Revision) bool {
	return status.IsolationState == platformv1alpha1.InstallationIsolationStateActive &&
		apiequality.Semantic.DeepEqual(status.ObservedIsolation, selection) &&
		status.IsolationRevision == revision.String() &&
		status.PolicyConfigMap == nil && status.RuntimeClass == nil && status.StorageClass == nil && status.CSIDriver == nil
}

func (r *InstallationIsolationReconciler) blockedStatus(installation *platformv1alpha1.Installation, state platformv1alpha1.InstallationIsolationState, observed *platformv1alpha1.InstallationIsolationSpec, identities *restrictedIsolationIdentities, revision *isolation.Revision, resolveErr error) platformv1alpha1.InstallationStatus {
	desired := r.baseStatus(installation, state, observed)
	if identities != nil && revision != nil {
		desired.IsolationRevision = revision.String()
		desired.PolicyConfigMap = identities.policy
		desired.RuntimeClass = identities.runtimeClass
		desired.StorageClass = identities.storageClass
		desired.CSIDriver = identities.csiDriver
	}
	dependenciesResolved := resolveErr == nil && identities != nil && revision != nil
	dependencyReason := "DependenciesResolved"
	if !dependenciesResolved {
		dependencyReason = "DependencyResolutionFailed"
	}
	readyReason := "Fencing"
	readyMessage := "installation environment execution is being fenced"
	if state == platformv1alpha1.InstallationIsolationStateBlocked {
		readyReason = "RuntimeActivationUnavailable"
		readyMessage = "restricted runtime activation is unavailable"
	}
	r.setConditions(installation, &desired, dependenciesResolved, dependencyReason, metav1.ConditionFalse, readyReason, readyMessage, "RuntimeActivationUnavailable", "restricted production isolation is not active")
	return desired
}

func (r *InstallationIsolationReconciler) baseStatus(_ *platformv1alpha1.Installation, state platformv1alpha1.InstallationIsolationState, observed *platformv1alpha1.InstallationIsolationSpec) platformv1alpha1.InstallationStatus {
	status := platformv1alpha1.InstallationStatus{IsolationState: state}
	if observed != nil {
		status.ObservedIsolation = observed.DeepCopy()
	}
	return status
}

func (r *InstallationIsolationReconciler) setConditions(installation *platformv1alpha1.Installation, status *platformv1alpha1.InstallationStatus, dependenciesResolved bool, dependencyReason string, readyStatus metav1.ConditionStatus, readyReason, readyMessage, productionReason, productionMessage string) {
	dependencyStatus := metav1.ConditionFalse
	dependencyMessage := "restricted isolation dependencies are unresolved"
	if dependenciesResolved {
		dependencyStatus = metav1.ConditionTrue
		dependencyMessage = "isolation dependencies are resolved"
	}
	conditions := []metav1.Condition{
		{Type: platformv1alpha1.InstallationConditionIsolationDependenciesResolved, Status: dependencyStatus, Reason: dependencyReason, Message: dependencyMessage, ObservedGeneration: installation.Generation},
		{Type: platformv1alpha1.InstallationConditionIsolationReady, Status: readyStatus, Reason: readyReason, Message: readyMessage, ObservedGeneration: installation.Generation},
		{Type: platformv1alpha1.InstallationConditionProductionReady, Status: metav1.ConditionFalse, Reason: productionReason, Message: productionMessage, ObservedGeneration: installation.Generation},
	}
	status.Conditions = make([]metav1.Condition, 0, len(conditions))
	for _, condition := range conditions {
		current := apimeta.FindStatusCondition(installation.Status.Conditions, condition.Type)
		one := make([]metav1.Condition, 0, 1)
		if current != nil {
			one = append(one, *current)
		}
		apimeta.SetStatusCondition(&one, condition)
		status.Conditions = append(status.Conditions, one[0])
	}
}

func (r *InstallationIsolationReconciler) updateStatus(ctx context.Context, installation *platformv1alpha1.Installation, desired platformv1alpha1.InstallationStatus) (bool, error) {
	if apiequality.Semantic.DeepEqual(installation.Status, desired) {
		return false, nil
	}
	installation.Status = desired
	if err := r.Status().Update(ctx, installation); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("update Installation isolation status: %w", err)
	}
	return true, nil
}

func (r *InstallationIsolationReconciler) allEnvironmentExecutionsFenced(ctx context.Context) (bool, error) {
	var environments []platformv1alpha1.Environment
	switch r.Mode {
	case tenancy.ModeScoped:
		namespaces := slices.Clone(r.Namespaces)
		slices.Sort(namespaces)
		namespaces = slices.Compact(namespaces)
		for _, namespace := range namespaces {
			var list platformv1alpha1.EnvironmentList
			if err := r.reader().List(ctx, &list, client.InNamespace(namespace)); err != nil {
				return false, fmt.Errorf("list Environments while fencing installation: %w", err)
			}
			environments = append(environments, list.Items...)
		}
	case tenancy.ModeTrustedAdmin:
		var list platformv1alpha1.EnvironmentList
		if err := r.reader().List(ctx, &list); err != nil {
			return false, fmt.Errorf("list Environments while fencing installation: %w", err)
		}
		ownership := make(map[string]bool)
		for i := range list.Items {
			environment := &list.Items[i]
			owned, checked := ownership[environment.Namespace]
			if !checked {
				var err error
				owned, err = r.trustedAdminNamespaceOwned(ctx, environment.Namespace)
				if err != nil {
					return false, err
				}
				ownership[environment.Namespace] = owned
			}
			if owned {
				environments = append(environments, *environment)
			}
		}
	default:
		return false, stderrors.New("installation isolation controller has no valid tenancy mode")
	}

	for i := range environments {
		environment := &environments[i]
		if environment.Status.PodName != "" || environment.Status.Endpoints.Sandboxd != "" || platformv1alpha1.IsEnvironmentReady(environment) {
			return false, nil
		}
		for _, child := range []client.Object{
			&corev1.Pod{},
			&corev1.Secret{},
		} {
			name := envPodName(environment)
			if _, ok := child.(*corev1.Secret); ok {
				name = envCredentialName(environment)
			}
			if err := r.reader().Get(ctx, types.NamespacedName{Namespace: environment.Namespace, Name: name}, child); err == nil {
				if exactControllerOwner(child, platformv1alpha1.GroupVersion.String(), "Environment", environment.Name, environment.UID) {
					return false, nil
				}
			} else if !apierrors.IsNotFound(err) {
				return false, fmt.Errorf("prove Environment execution fencing: %w", err)
			}
		}
	}
	return true, nil
}

func (r *InstallationIsolationReconciler) trustedAdminNamespaceOwned(ctx context.Context, namespace string) (bool, error) {
	var current corev1.Namespace
	if err := r.reader().Get(ctx, types.NamespacedName{Name: namespace}, &current); err != nil {
		return false, fmt.Errorf("resolve Environment Namespace ownership while fencing installation: %w", err)
	}
	if current.UID == "" || !current.DeletionTimestamp.IsZero() {
		return false, fmt.Errorf("Environment Namespace %q has no stable live identity", namespace)
	}
	annotations := current.GetAnnotations()
	installationNamespace := annotations[tenancy.InstallationNamespaceAnnotation]
	installationName := annotations[tenancy.InstallationNameAnnotation]
	installationUID := types.UID(annotations[tenancy.InstallationUIDAnnotation])
	if installationNamespace == "" || installationName == "" || installationUID == "" {
		return false, fmt.Errorf("Environment Namespace %q has uncertain Installation ownership", namespace)
	}
	if installationNamespace != r.Installation.Key.Namespace || installationName != r.Installation.Key.Name {
		return false, nil
	}
	if installationUID != r.Installation.UID {
		return false, fmt.Errorf("Environment Namespace %q has uncertain ownership by this Installation", namespace)
	}
	return true, nil
}

func installationExecutionAllowed(ctx context.Context, reader client.Reader, identity tenancy.InstallationIdentity) (bool, error) {
	if reader == nil || identity.Key.Namespace == "" || identity.Key.Name == "" || identity.UID == "" {
		return false, stderrors.New("complete installation isolation authority is required")
	}
	var installation platformv1alpha1.Installation
	if err := reader.Get(ctx, identity.Key, &installation); err != nil {
		return false, fmt.Errorf("revalidate Installation isolation: %w", err)
	}
	if installation.UID != identity.UID || !installation.DeletionTimestamp.IsZero() {
		return false, stderrors.New("Installation isolation identity changed")
	}
	if installation.Spec.Isolation == nil {
		return legacyCompatibleIsolationStatus(installation.Status), nil
	}
	if err := isolation.ValidateSelection(installation.Spec.Isolation); err != nil {
		return false, fmt.Errorf("validate Installation isolation selection: %w", err)
	}
	switch installation.Spec.Isolation.Mode {
	case platformv1alpha1.InstallationIsolationModeUnrestrictedDevelopment:
		revision, err := (isolation.RevisionInputs{InstallationUID: installation.UID, Selection: *installation.Spec.Isolation}).DeriveRevision()
		if err != nil {
			return false, fmt.Errorf("derive Installation isolation revision: %w", err)
		}
		if legacyCompatibleIsolationStatus(installation.Status) {
			return true, nil
		}
		ready := apimeta.FindStatusCondition(installation.Status.Conditions, platformv1alpha1.InstallationConditionIsolationReady)
		return unrestrictedAuthorityMatches(installation.Status, installation.Spec.Isolation, revision) &&
			ready != nil && ready.Status == metav1.ConditionTrue && ready.Reason == "UnrestrictedDevelopment" && ready.ObservedGeneration == installation.Generation, nil
	case platformv1alpha1.InstallationIsolationModeRestrictedProductionCalicoV3_32_1:
		return false, nil
	default:
		return false, stderrors.New("unsupported Installation isolation selection")
	}
}

func installationIdentity(scope *tenancy.ReconcileScope) (tenancy.InstallationIdentity, bool) {
	if scope == nil || scope.Verifier == nil {
		return tenancy.InstallationIdentity{}, false
	}
	return scope.Verifier.Installation, true
}

func installationExecutionCurrent(ctx context.Context, reader client.Reader, scope *tenancy.ReconcileScope) (bool, error) {
	identity, configured := installationIdentity(scope)
	if !configured {
		return true, nil
	}
	return installationExecutionAllowed(ctx, reader, identity)
}

func (r *InstallationIsolationReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *InstallationIsolationReconciler) dependencyRequests(_ context.Context, _ client.Object) []reconcile.Request {
	return []reconcile.Request{{NamespacedName: r.Installation.Key}}
}

// SetupWithManager registers the exact Installation and all object families
// whose replacement or mutation can change its observed restricted identity.
func (r *InstallationIsolationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.APIReader = mgr.GetAPIReader()
	mapper := handler.EnqueueRequestsFromMapFunc(r.dependencyRequests)
	builder := ctrl.NewControllerManagedBy(mgr).
		For(&platformv1alpha1.Installation{}).
		Watches(&corev1.ConfigMap{}, mapper).
		Watches(&nodev1.RuntimeClass{}, mapper).
		Watches(&storagev1.StorageClass{}, mapper).
		Watches(&storagev1.CSIDriver{}, mapper)
	// A scoped install without onboarded Project namespaces has no permitted
	// Environment informer or execution to fence. Keep its lifecycle controller
	// active without broadening the system namespace's workload RBAC.
	if r.Mode != tenancy.ModeScoped || len(r.Namespaces) != 0 {
		builder = builder.Watches(&platformv1alpha1.Environment{}, mapper)
	}
	return builder.Complete(r)
}
