package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	nodev1 "k8s.io/api/node/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kptr "k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/egresspolicy"
	"github.com/Chris-Cullins/swe-platform/internal/isolation"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
)

func isolationTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, networkingv1.AddToScheme, nodev1.AddToScheme, storagev1.AddToScheme, platformv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	return scheme
}

func restrictedIsolationSelection() *platformv1alpha1.InstallationIsolationSpec {
	return &platformv1alpha1.InstallationIsolationSpec{
		Mode:                platformv1alpha1.InstallationIsolationModeRestrictedProductionCalicoV3_32_1,
		PolicyConfigMapName: "egress-policy",
		RuntimeClass:        &platformv1alpha1.InstallationRuntimeClassExpectation{Name: "gvisor", Handler: "runsc"},
		StorageClass:        &platformv1alpha1.InstallationStorageClassExpectation{Name: "workspace", CSIDriver: "csi.example.com"},
	}
}

func restrictedPolicyConfigMap(t *testing.T) *corev1.ConfigMap {
	t.Helper()
	config := egresspolicy.Config{
		APIVersion: egresspolicy.ConfigAPIVersion,
		Mode:       egresspolicy.ModeRestricted,
		Ceiling:    []egresspolicy.Hostname{},
		Baseline:   []egresspolicy.Hostname{},
		RestrictedProfile: &egresspolicy.RestrictedProfile{
			Name: egresspolicy.RestrictedProfileCalicoV1, ResolverIPs: []string{"10.96.0.10"},
			APIServerCIDRs: []string{"10.0.0.1/32"}, PodCIDRs: []string{"10.244.0.0/16"},
			ServiceCIDRs: []string{"10.96.0.0/12"}, NodeCIDRs: []string{"192.168.0.0/16"},
			ControlPlaneCIDRs: []string{"172.16.0.0/16"}, AdditionalDeniedCIDRs: []string{},
		},
		TLSSecretName: "egress-proxy-tls",
		ProxyImage:    "registry.example.com/swe/egress-proxy@sha256:" + strings.Repeat("a", 64),
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "egress-policy", Namespace: "system", UID: "policy-uid", Annotations: map[string]string{
			egresspolicy.ConfigContentSHA256Annotation: hex.EncodeToString(digest[:]),
		}},
		Immutable: kptr.To(true),
		Data:      map[string]string{egresspolicy.ConfigDataKey: string(raw)},
	}
}

func restrictedIsolationDependencies(t *testing.T) []client.Object {
	return []client.Object{
		restrictedPolicyConfigMap(t),
		&nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: "gvisor", UID: "runtime-uid"}, Handler: "runsc"},
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "workspace", UID: "storage-uid"}, Provisioner: "csi.example.com"},
		&storagev1.CSIDriver{ObjectMeta: metav1.ObjectMeta{Name: "csi.example.com", UID: "csi-uid"}},
	}
}

func installationIsolationReconciler(t *testing.T, installation *platformv1alpha1.Installation, objects ...client.Object) (*InstallationIsolationReconciler, client.WithWatch) {
	t.Helper()
	all := append([]client.Object{installation}, objects...)
	kubeClient := fake.NewClientBuilder().WithScheme(isolationTestScheme(t)).WithStatusSubresource(installation, &platformv1alpha1.Environment{}).WithObjects(all...).Build()
	identity := tenancy.InstallationIdentity{Key: client.ObjectKeyFromObject(installation), UID: installation.UID}
	return &InstallationIsolationReconciler{Client: kubeClient, APIReader: kubeClient, Installation: identity, Mode: tenancy.ModeScoped, Namespaces: []string{"project"}}, kubeClient
}

func TestInstallationIsolationLegacyAndUnrestrictedStatus(t *testing.T) {
	for _, test := range []struct {
		name       string
		selection  *platformv1alpha1.InstallationIsolationSpec
		wantState  platformv1alpha1.InstallationIsolationState
		wantReady  metav1.ConditionStatus
		wantReason string
	}{
		{name: "legacy omission", wantState: platformv1alpha1.InstallationIsolationStateLegacyUnclassified, wantReady: metav1.ConditionFalse, wantReason: "LegacyUnclassified"},
		{name: "explicit unrestricted development", selection: &platformv1alpha1.InstallationIsolationSpec{Mode: platformv1alpha1.InstallationIsolationModeUnrestrictedDevelopment}, wantState: platformv1alpha1.InstallationIsolationStateActive, wantReady: metav1.ConditionTrue, wantReason: "UnrestrictedDevelopment"},
	} {
		t.Run(test.name, func(t *testing.T) {
			installation := &platformv1alpha1.Installation{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "installation-uid", Generation: 2}, Spec: platformv1alpha1.InstallationSpec{Isolation: test.selection}}
			reconciler, kubeClient := installationIsolationReconciler(t, installation)
			if result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(installation)}); err != nil || result != (ctrl.Result{}) {
				t.Fatalf("Reconcile() = (%#v, %v)", result, err)
			}
			var updated platformv1alpha1.Installation
			if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(installation), &updated); err != nil {
				t.Fatal(err)
			}
			ready := apimeta.FindStatusCondition(updated.Status.Conditions, platformv1alpha1.InstallationConditionIsolationReady)
			production := apimeta.FindStatusCondition(updated.Status.Conditions, platformv1alpha1.InstallationConditionProductionReady)
			if updated.Status.IsolationState != test.wantState || ready == nil || ready.Status != test.wantReady || ready.Reason != test.wantReason {
				t.Fatalf("status = %#v", updated.Status)
			}
			if production == nil || production.Status != metav1.ConditionFalse {
				t.Fatalf("production condition = %#v", production)
			}
			if test.selection == nil {
				if updated.Status.ObservedIsolation != nil || updated.Status.IsolationRevision != "" {
					t.Fatalf("legacy status published selection identity: %#v", updated.Status)
				}
			} else if updated.Status.ObservedIsolation == nil || len(updated.Status.IsolationRevision) != 64 || production.Reason != "UnrestrictedDevelopment" {
				t.Fatalf("unrestricted status = %#v", updated.Status)
			}
		})
	}
}

func TestRestrictedInstallationFencesEnvironmentThenSettlesBlocked(t *testing.T) {
	installation := &platformv1alpha1.Installation{
		ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "installation-uid", Generation: 2},
		Spec:       platformv1alpha1.InstallationSpec{Isolation: restrictedIsolationSelection()},
	}
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "warm", Namespace: "project", UID: "environment-uid", Generation: 1, Finalizers: []string{environmentFinalizer}},
		Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "missing"},
		Status: platformv1alpha1.EnvironmentStatus{
			ObservedGeneration: 1, Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-warm",
			Endpoints:  platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.2:50051"},
			Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, Reason: "SandboxdReady", ObservedGeneration: 1}},
		},
	}
	controller := true
	owner := metav1.OwnerReference{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: environment.Name, UID: environment.UID, Controller: &controller}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envPodName(environment), Namespace: environment.Namespace, UID: "pod-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: envCredentialName(environment), Namespace: environment.Namespace, UID: "secret-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: envPVCName(environment), Namespace: environment.Namespace, UID: "pvc-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
	policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: envNetworkPolicyName(environment), Namespace: environment.Namespace, UID: "network-policy-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
	objects := append(restrictedIsolationDependencies(t), environment, pod, secret, pvc, policy)
	reconciler, kubeClient := installationIsolationReconciler(t, installation, objects...)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(installation)}

	if result, err := reconciler.Reconcile(context.Background(), request); err != nil || !result.Requeue {
		t.Fatalf("enter Fencing = (%#v, %v)", result, err)
	}
	var fencing platformv1alpha1.Installation
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(installation), &fencing); err != nil {
		t.Fatal(err)
	}
	if fencing.Status.IsolationState != platformv1alpha1.InstallationIsolationStateFencing || fencing.Status.IsolationRevision != "" || fencing.Status.PolicyConfigMap != nil || fencing.Status.RuntimeClass != nil || fencing.Status.StorageClass != nil || fencing.Status.CSIDriver != nil {
		t.Fatalf("initial Fencing status resolved dependencies: %#v", fencing.Status)
	}
	if result, err := reconciler.Reconcile(context.Background(), request); err != nil || !result.Requeue {
		t.Fatalf("resolve dependencies while Fencing = (%#v, %v)", result, err)
	}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(installation), &fencing); err != nil {
		t.Fatal(err)
	}
	if fencing.Status.IsolationState != platformv1alpha1.InstallationIsolationStateFencing || len(fencing.Status.IsolationRevision) != 64 || fencing.Status.PolicyConfigMap == nil || fencing.Status.RuntimeClass == nil || fencing.Status.StorageClass == nil || fencing.Status.CSIDriver == nil {
		t.Fatalf("Fencing status lacks resolved identity: %#v", fencing.Status)
	}

	environmentReconciler := &EnvironmentReconciler{
		Client: kubeClient, APIReader: kubeClient,
		Scope: &tenancy.ReconcileScope{Verifier: &tenancy.Verifier{Installation: reconciler.Installation}},
	}
	for step := 0; step < 4; step++ {
		var current platformv1alpha1.Environment
		if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(environment), &current); err != nil {
			t.Fatal(err)
		}
		outcome := reconcileEnvironmentInstallationIsolationGate(environmentReconciler, context.Background(), &environmentReconcileState{env: current})
		if !outcome.handled || outcome.err != nil {
			t.Fatalf("environment fence step %d = %#v", step, outcome)
		}
		if step == 0 {
			if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
				t.Fatal("Pod was deleted before readiness withdrawal")
			}
		}
	}

	if result, err := reconciler.Reconcile(context.Background(), request); err != nil || !result.Requeue {
		t.Fatalf("publish Blocked = (%#v, %v)", result, err)
	}
	if result, err := reconciler.Reconcile(context.Background(), request); err != nil || result != (ctrl.Result{}) {
		t.Fatalf("stable Blocked = (%#v, %v)", result, err)
	}
	var blocked platformv1alpha1.Installation
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(installation), &blocked); err != nil {
		t.Fatal(err)
	}
	ready := apimeta.FindStatusCondition(blocked.Status.Conditions, platformv1alpha1.InstallationConditionIsolationReady)
	if blocked.Status.IsolationState != platformv1alpha1.InstallationIsolationStateBlocked || ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "RuntimeActivationUnavailable" {
		t.Fatalf("blocked status = %#v", blocked.Status)
	}
	var fencedEnvironment platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(environment), &fencedEnvironment); err != nil {
		t.Fatal(err)
	}
	environmentReady := apimeta.FindStatusCondition(fencedEnvironment.Status.Conditions, platformv1alpha1.EnvironmentConditionReady)
	if fencedEnvironment.Status.Phase != platformv1alpha1.EnvironmentPhaseFailed || environmentReady == nil || environmentReady.Reason != "InvalidConfiguration" || environmentReady.Message != installationIsolationBlockedMessage {
		t.Fatalf("fenced Environment status = %#v", fencedEnvironment.Status)
	}
	for _, removed := range []client.Object{pod, secret} {
		if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(removed), removed); !apierrors.IsNotFound(err) {
			t.Fatalf("execution child %T remains: %v", removed, err)
		}
	}
	for _, retained := range []client.Object{pvc, policy} {
		if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(retained), retained); err != nil {
			t.Fatalf("retained child %T was removed: %v", retained, err)
		}
	}
}

func TestRestrictedSelectionPublishesFencingBeforeDependencyReads(t *testing.T) {
	unrestricted := &platformv1alpha1.InstallationIsolationSpec{Mode: platformv1alpha1.InstallationIsolationModeUnrestrictedDevelopment}
	revision, err := (isolation.RevisionInputs{InstallationUID: "installation-uid", Selection: *unrestricted}).DeriveRevision()
	if err != nil {
		t.Fatal(err)
	}
	installation := &platformv1alpha1.Installation{
		ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "installation-uid", Generation: 2},
		Spec:       platformv1alpha1.InstallationSpec{Isolation: restrictedIsolationSelection()},
		Status: platformv1alpha1.InstallationStatus{
			IsolationState:    platformv1alpha1.InstallationIsolationStateActive,
			ObservedIsolation: unrestricted,
			IsolationRevision: revision.String(),
			Conditions: []metav1.Condition{{
				Type: platformv1alpha1.InstallationConditionIsolationReady, Status: metav1.ConditionTrue,
				Reason: "UnrestrictedDevelopment", ObservedGeneration: 1,
			}},
		},
	}
	scheme := isolationTestScheme(t)
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	dependencyReads := 0
	reader := interceptor.NewClient(base, interceptor.Funcs{Get: func(ctx context.Context, delegate client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
		switch object.(type) {
		case *corev1.ConfigMap, *nodev1.RuntimeClass, *storagev1.StorageClass, *storagev1.CSIDriver:
			dependencyReads++
			return errors.New("dependency reader must not run before Fencing")
		default:
			return delegate.Get(ctx, key, object, options...)
		}
	}})
	reconciler := &InstallationIsolationReconciler{
		Client: base, APIReader: reader,
		Installation: tenancy.InstallationIdentity{Key: client.ObjectKeyFromObject(installation), UID: installation.UID},
		Mode:         tenancy.ModeScoped, Namespaces: []string{"project"},
	}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(installation)})
	if err != nil || !result.Requeue || dependencyReads != 0 {
		t.Fatalf("first restricted reconcile = (%#v, %v), dependency reads = %d", result, err, dependencyReads)
	}
	var updated platformv1alpha1.Installation
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(installation), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.IsolationState != platformv1alpha1.InstallationIsolationStateFencing || !apiequality.Semantic.DeepEqual(updated.Status.ObservedIsolation, installation.Spec.Isolation) || updated.Status.IsolationRevision != "" || updated.Status.PolicyConfigMap != nil || updated.Status.RuntimeClass != nil || updated.Status.StorageClass != nil || updated.Status.CSIDriver != nil {
		t.Fatalf("initial restricted status = %#v", updated.Status)
	}
}

func convergeRestrictedInstallationBlocked(t *testing.T, reconciler *InstallationIsolationReconciler, installation *platformv1alpha1.Installation, kubeClient client.Client) platformv1alpha1.Installation {
	t.Helper()
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(installation)}
	for step := 0; step < 3; step++ {
		result, err := reconciler.Reconcile(context.Background(), request)
		if err != nil {
			t.Fatalf("restricted convergence step %d: %v", step, err)
		}
		if step < 2 && !result.Requeue {
			t.Fatalf("restricted convergence step %d = %#v", step, result)
		}
	}
	var blocked platformv1alpha1.Installation
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(installation), &blocked); err != nil {
		t.Fatal(err)
	}
	if blocked.Status.IsolationState != platformv1alpha1.InstallationIsolationStateBlocked || blocked.Status.RuntimeClass == nil || blocked.Status.IsolationRevision == "" {
		t.Fatalf("restricted Installation did not converge Blocked: %#v", blocked.Status)
	}
	return blocked
}

func TestBlockedRestrictedDependencyAuthorityChangesReenterFencing(t *testing.T) {
	t.Run("RuntimeClass UID replacement", func(t *testing.T) {
		installation := &platformv1alpha1.Installation{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "installation-uid", Generation: 1}, Spec: platformv1alpha1.InstallationSpec{Isolation: restrictedIsolationSelection()}}
		dependencies := restrictedIsolationDependencies(t)
		reconciler, kubeClient := installationIsolationReconciler(t, installation, dependencies...)
		blocked := convergeRestrictedInstallationBlocked(t, reconciler, installation, kubeClient)
		oldRevision := blocked.Status.IsolationRevision

		var old nodev1.RuntimeClass
		if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: "gvisor"}, &old); err != nil {
			t.Fatal(err)
		}
		if err := kubeClient.Delete(context.Background(), &old); err != nil {
			t.Fatal(err)
		}
		replacement := old.DeepCopy()
		replacement.ResourceVersion = ""
		replacement.UID = "runtime-uid-2"
		if err := kubeClient.Create(context.Background(), replacement); err != nil {
			t.Fatal(err)
		}

		request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(installation)}
		if result, err := reconciler.Reconcile(context.Background(), request); err != nil || !result.Requeue {
			t.Fatalf("replacement reconcile = (%#v, %v)", result, err)
		}
		var fencing platformv1alpha1.Installation
		if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(installation), &fencing); err != nil {
			t.Fatal(err)
		}
		if fencing.Status.IsolationState != platformv1alpha1.InstallationIsolationStateFencing || fencing.Status.RuntimeClass == nil || fencing.Status.RuntimeClass.UID != replacement.UID || fencing.Status.IsolationRevision == oldRevision {
			t.Fatalf("replacement did not reenter Fencing with new authority: %#v", fencing.Status)
		}
		if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(installation), &blocked); err != nil {
			t.Fatal(err)
		}
		if blocked.Status.IsolationState != platformv1alpha1.InstallationIsolationStateBlocked || blocked.Status.RuntimeClass == nil || blocked.Status.RuntimeClass.UID != replacement.UID {
			t.Fatalf("replacement did not settle Blocked after Fencing: %#v", blocked.Status)
		}
	})

	t.Run("dependency read uncertainty", func(t *testing.T) {
		installation := &platformv1alpha1.Installation{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "installation-uid", Generation: 1}, Spec: platformv1alpha1.InstallationSpec{Isolation: restrictedIsolationSelection()}}
		reconciler, kubeClient := installationIsolationReconciler(t, installation, restrictedIsolationDependencies(t)...)
		convergeRestrictedInstallationBlocked(t, reconciler, installation, kubeClient)
		reconciler.APIReader = interceptor.NewClient(kubeClient, interceptor.Funcs{Get: func(ctx context.Context, delegate client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
			if _, ok := object.(*nodev1.RuntimeClass); ok {
				return errors.New("temporary RuntimeClass read failure")
			}
			return delegate.Get(ctx, key, object, options...)
		}})

		request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(installation)}
		if result, err := reconciler.Reconcile(context.Background(), request); err != nil || !result.Requeue {
			t.Fatalf("uncertain dependency reconcile = (%#v, %v)", result, err)
		}
		var fencing platformv1alpha1.Installation
		if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(installation), &fencing); err != nil {
			t.Fatal(err)
		}
		if fencing.Status.IsolationState != platformv1alpha1.InstallationIsolationStateFencing || fencing.Status.IsolationRevision != "" || fencing.Status.RuntimeClass != nil {
			t.Fatalf("uncertain dependency did not clear authority through Fencing: %#v", fencing.Status)
		}
		if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		var blocked platformv1alpha1.Installation
		if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(installation), &blocked); err != nil {
			t.Fatal(err)
		}
		if blocked.Status.IsolationState != platformv1alpha1.InstallationIsolationStateBlocked || blocked.Status.IsolationRevision != "" || blocked.Status.RuntimeClass != nil {
			t.Fatalf("uncertain dependency did not settle Blocked after Fencing: %#v", blocked.Status)
		}
	})
}

func TestRestrictedDependencyMismatchFailsClosedWithoutObservedIdentity(t *testing.T) {
	installation := &platformv1alpha1.Installation{
		ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "installation-uid", Generation: 1},
		Spec:       platformv1alpha1.InstallationSpec{Isolation: restrictedIsolationSelection()},
	}
	dependencies := restrictedIsolationDependencies(t)
	dependencies[1].(*nodev1.RuntimeClass).Handler = "other"
	reconciler, kubeClient := installationIsolationReconciler(t, installation, dependencies...)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(installation)}
	if result, err := reconciler.Reconcile(context.Background(), request); err != nil || !result.Requeue {
		t.Fatalf("enter Fencing = (%#v, %v)", result, err)
	}
	if result, err := reconciler.Reconcile(context.Background(), request); err != nil || !result.Requeue {
		t.Fatalf("publish dependency failure = (%#v, %v)", result, err)
	}
	if result, err := reconciler.Reconcile(context.Background(), request); err != nil || result.RequeueAfter != isolationResolutionRetryDelay {
		t.Fatalf("settle dependency failure = (%#v, %v)", result, err)
	}
	var blocked platformv1alpha1.Installation
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(installation), &blocked); err != nil {
		t.Fatal(err)
	}
	dependenciesResolved := apimeta.FindStatusCondition(blocked.Status.Conditions, platformv1alpha1.InstallationConditionIsolationDependenciesResolved)
	ready := apimeta.FindStatusCondition(blocked.Status.Conditions, platformv1alpha1.InstallationConditionIsolationReady)
	if blocked.Status.IsolationState != platformv1alpha1.InstallationIsolationStateBlocked || blocked.Status.IsolationRevision != "" || blocked.Status.PolicyConfigMap != nil || blocked.Status.RuntimeClass != nil || blocked.Status.StorageClass != nil || blocked.Status.CSIDriver != nil || dependenciesResolved == nil || dependenciesResolved.Reason != "DependencyResolutionFailed" || ready == nil || ready.Reason != "RuntimeActivationUnavailable" {
		t.Fatalf("dependency mismatch status = %#v", blocked.Status)
	}
	allowed, err := installationExecutionAllowed(context.Background(), kubeClient, reconciler.Installation)
	if err != nil || allowed {
		t.Fatalf("restricted selection allowed execution = %t, %v", allowed, err)
	}
}

func TestInstallationAPIUncertaintyStillWithdrawsEnvironmentReadiness(t *testing.T) {
	installation := &platformv1alpha1.Installation{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "installation-uid"}}
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "project", UID: "environment-uid", Generation: 1, Finalizers: []string{environmentFinalizer}},
		Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-env", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
			Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, Reason: "SandboxdReady"}}},
	}
	scheme := isolationTestScheme(t)
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(installation, environment).Build()
	uncertain := interceptor.NewClient(base, interceptor.Funcs{Get: func(ctx context.Context, delegate client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
		if _, ok := object.(*platformv1alpha1.Installation); ok {
			return errors.New("temporary API failure")
		}
		return delegate.Get(ctx, key, object, options...)
	}})
	reconciler := &EnvironmentReconciler{
		Client: base, APIReader: uncertain,
		Scope: &tenancy.ReconcileScope{Verifier: &tenancy.Verifier{Installation: tenancy.InstallationIdentity{Key: client.ObjectKeyFromObject(installation), UID: installation.UID}}},
	}
	outcome := reconcileEnvironmentInstallationIsolationGate(reconciler, context.Background(), &environmentReconcileState{env: *environment.DeepCopy()})
	if !outcome.handled || outcome.err != nil || !outcome.result.Requeue {
		t.Fatalf("uncertain isolation gate = %#v", outcome)
	}
	var updated platformv1alpha1.Environment
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(environment), &updated); err != nil {
		t.Fatal(err)
	}
	ready := apimeta.FindStatusCondition(updated.Status.Conditions, platformv1alpha1.EnvironmentConditionReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "InvalidConfiguration" || updated.Status.PodName != "" || updated.Status.Endpoints.Sandboxd != "" {
		t.Fatalf("API uncertainty did not withdraw readiness: %#v", updated.Status)
	}
}

func TestBackendCreationSourcesRevalidateInstallationIsolation(t *testing.T) {
	scheme := isolationTestScheme(t)
	installation := &platformv1alpha1.Installation{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "installation-uid"}}
	environment := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "environment", Namespace: "project", UID: "environment-uid", Generation: 1}}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(installation, environment).Build()
	identity := tenancy.InstallationIdentity{Key: client.ObjectKeyFromObject(installation), UID: installation.UID}
	reconciler := &EnvironmentReconciler{
		Client: kubeClient, APIReader: kubeClient,
		Scope: &tenancy.ReconcileScope{Verifier: &tenancy.Verifier{Reader: kubeClient, Installation: identity, Mode: tenancy.ModeScoped}},
	}

	if current, err := reconciler.backendCreationSourcesCurrent(context.Background(), environment, tenancy.Claim{}); err != nil || !current {
		t.Fatalf("legacy backend authority = (%t, %v), want current", current, err)
	}
	var restricted platformv1alpha1.Installation
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(installation), &restricted); err != nil {
		t.Fatal(err)
	}
	restricted.Spec.Isolation = restrictedIsolationSelection()
	if err := kubeClient.Update(context.Background(), &restricted); err != nil {
		t.Fatal(err)
	}
	if current, err := reconciler.backendCreationSourcesCurrent(context.Background(), environment, tenancy.Claim{}); err != nil || current {
		t.Fatalf("restricted backend authority = (%t, %v), want fenced", current, err)
	}
}

func TestTrustedAdminFenceProofIgnoresForeignInstallationEnvironment(t *testing.T) {
	installation := &platformv1alpha1.Installation{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "installation-uid"}}
	foreignInstallation := &platformv1alpha1.Installation{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "foreign-system", UID: "foreign-installation-uid"}}
	ownNamespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "own-project", UID: "own-namespace-uid", Annotations: map[string]string{
		tenancy.InstallationNamespaceAnnotation: installation.Namespace,
		tenancy.InstallationNameAnnotation:      installation.Name,
		tenancy.InstallationUIDAnnotation:       string(installation.UID),
	}}}
	foreignNamespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "foreign-project", UID: "foreign-namespace-uid", Annotations: map[string]string{
		tenancy.InstallationNamespaceAnnotation: foreignInstallation.Namespace,
		tenancy.InstallationNameAnnotation:      foreignInstallation.Name,
		tenancy.InstallationUIDAnnotation:       string(foreignInstallation.UID),
	}}}
	readyStatus := platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-ready", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, Reason: "SandboxdReady"}},
	}
	ownEnvironment := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "own", Namespace: ownNamespace.Name, UID: "own-environment-uid"}, Status: readyStatus}
	foreignEnvironment := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: foreignNamespace.Name, UID: "foreign-environment-uid"}, Status: readyStatus}
	scheme := isolationTestScheme(t)
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, foreignInstallation, ownEnvironment, foreignEnvironment).WithObjects(installation, foreignInstallation, ownNamespace, foreignNamespace, ownEnvironment, foreignEnvironment).Build()
	reconciler := &InstallationIsolationReconciler{
		Client: kubeClient, APIReader: kubeClient,
		Installation: tenancy.InstallationIdentity{Key: client.ObjectKeyFromObject(installation), UID: installation.UID},
		Mode:         tenancy.ModeTrustedAdmin,
	}

	if fenced, err := reconciler.allEnvironmentExecutionsFenced(context.Background()); err != nil || fenced {
		t.Fatalf("own Ready Environment fence proof = %t, %v", fenced, err)
	}
	if err := kubeClient.Delete(context.Background(), ownEnvironment); err != nil {
		t.Fatal(err)
	}
	if fenced, err := reconciler.allEnvironmentExecutionsFenced(context.Background()); err != nil || !fenced {
		t.Fatalf("foreign Ready Environment affected this Installation fence proof = %t, %v", fenced, err)
	}
}

func TestRestrictedBlockedToUnrestrictedFencesBeforeActive(t *testing.T) {
	installation := &platformv1alpha1.Installation{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "installation-uid", Generation: 1}, Spec: platformv1alpha1.InstallationSpec{Isolation: restrictedIsolationSelection()}}
	reconciler, kubeClient := installationIsolationReconciler(t, installation, restrictedIsolationDependencies(t)...)
	convergeRestrictedInstallationBlocked(t, reconciler, installation, kubeClient)

	var current platformv1alpha1.Installation
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(installation), &current); err != nil {
		t.Fatal(err)
	}
	current.Spec.Isolation = &platformv1alpha1.InstallationIsolationSpec{Mode: platformv1alpha1.InstallationIsolationModeUnrestrictedDevelopment}
	current.Generation++
	if err := kubeClient.Update(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	if allowed, err := installationExecutionAllowed(context.Background(), kubeClient, reconciler.Installation); err != nil || allowed {
		t.Fatalf("restricted status reopened from unrestricted spec = %t, %v", allowed, err)
	}

	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(installation)}
	if result, err := reconciler.Reconcile(context.Background(), request); err != nil || !result.Requeue {
		t.Fatalf("unrestricted Fencing transition = (%#v, %v)", result, err)
	}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(installation), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.IsolationState != platformv1alpha1.InstallationIsolationStateFencing || current.Status.ObservedIsolation == nil || current.Status.ObservedIsolation.Mode != platformv1alpha1.InstallationIsolationModeUnrestrictedDevelopment {
		t.Fatalf("unrestricted transition skipped Fencing: %#v", current.Status)
	}
	if allowed, err := installationExecutionAllowed(context.Background(), kubeClient, reconciler.Installation); err != nil || allowed {
		t.Fatalf("unrestricted Fencing permitted execution = %t, %v", allowed, err)
	}

	if result, err := reconciler.Reconcile(context.Background(), request); err != nil || !result.Requeue {
		t.Fatalf("unrestricted Active transition = (%#v, %v)", result, err)
	}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(installation), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.IsolationState != platformv1alpha1.InstallationIsolationStateActive {
		t.Fatalf("unrestricted transition did not become Active: %#v", current.Status)
	}
	if allowed, err := installationExecutionAllowed(context.Background(), kubeClient, reconciler.Installation); err != nil || !allowed {
		t.Fatalf("current unrestricted Active authority denied execution = %t, %v", allowed, err)
	}
}

func TestLegacyUnclassifiedDevelopmentAdoptionDoesNotFence(t *testing.T) {
	installation := &platformv1alpha1.Installation{
		ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "installation-uid", Generation: 2},
		Spec:       platformv1alpha1.InstallationSpec{Isolation: &platformv1alpha1.InstallationIsolationSpec{Mode: platformv1alpha1.InstallationIsolationModeUnrestrictedDevelopment}},
		Status:     platformv1alpha1.InstallationStatus{IsolationState: platformv1alpha1.InstallationIsolationStateLegacyUnclassified},
	}
	reconciler, kubeClient := installationIsolationReconciler(t, installation)
	if allowed, err := installationExecutionAllowed(context.Background(), kubeClient, reconciler.Installation); err != nil || !allowed {
		t.Fatalf("LegacyUnclassified development adoption changed behavior = %t, %v", allowed, err)
	}
	if result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(installation)}); err != nil || result != (ctrl.Result{}) {
		t.Fatalf("LegacyUnclassified development adoption = (%#v, %v)", result, err)
	}
	var active platformv1alpha1.Installation
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(installation), &active); err != nil {
		t.Fatal(err)
	}
	if active.Status.IsolationState != platformv1alpha1.InstallationIsolationStateActive {
		t.Fatalf("LegacyUnclassified development adoption did not become Active: %#v", active.Status)
	}
}

func TestIsolationDependencyMapperTargetsOnlyConfiguredInstallation(t *testing.T) {
	reconciler := &InstallationIsolationReconciler{Installation: tenancy.InstallationIdentity{Key: types.NamespacedName{Namespace: "system", Name: "main"}, UID: "uid"}}
	requests := reconciler.dependencyRequests(context.Background(), &storagev1.CSIDriver{})
	if len(requests) != 1 || requests[0].NamespacedName != reconciler.Installation.Key {
		t.Fatalf("dependency requests = %#v", requests)
	}
}

func TestInstallationWatchMappersWakeEveryEnvironmentAndWarmPool(t *testing.T) {
	scheme := isolationTestScheme(t)
	installation := &platformv1alpha1.Installation{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system"}}
	environments := []client.Object{
		&platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "one", Namespace: "project-a"}},
		&platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "two", Namespace: "project-b"}},
	}
	templates := []client.Object{
		&platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "project-a"}},
		&platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "large", Namespace: "project-b"}},
	}
	objects := append(environments, templates...)
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	environmentReconciler := &EnvironmentReconciler{Client: kubeClient}
	warmPoolReconciler := &WarmPoolReconciler{Client: kubeClient}
	if requests := environmentReconciler.installationIsolationRequests(context.Background(), installation); len(requests) != 2 {
		t.Fatalf("Installation Environment requests = %#v", requests)
	}
	if requests := warmPoolReconciler.installationTemplateRequests(context.Background(), installation); len(requests) != 2 {
		t.Fatalf("Installation Template requests = %#v", requests)
	}
}
