package controllers

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	stderrors "errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	sandboxdauth "github.com/Chris-Cullins/swe-platform/sandboxd/auth"
)

func TestEnsurePodInjectsProjectSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	project := &platformv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: platformv1alpha1.ProjectSpec{
			Repositories: []string{"https://github.com/example/repo"},
			SecretRef:    &corev1.LocalObjectReference{Name: "project-config"},
		},
	}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(project).Build(),
		Scheme: scheme,
	}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "environment-uid"},
		Spec: platformv1alpha1.EnvironmentSpec{
			ProjectRef:  project.Name,
			TemplateRef: "small",
		},
	}
	tmpl := &platformv1alpha1.EnvironmentTemplate{
		Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "example/environment:latest", Size: "small"},
	}

	pod, err := reconciler.ensurePod(context.Background(), env, tmpl)
	if err != nil {
		t.Fatalf("ensurePod() error = %v", err)
	}
	if len(pod.Spec.Containers[0].EnvFrom) != 1 {
		t.Fatalf("EnvFrom length = %d, want 1", len(pod.Spec.Containers[0].EnvFrom))
	}
	secretRef := pod.Spec.Containers[0].EnvFrom[0].SecretRef
	if secretRef == nil || secretRef.Name != "project-config" {
		t.Fatalf("SecretRef = %#v, want project-config", secretRef)
	}
	if len(pod.Spec.InitContainers) != 1 {
		t.Fatalf("InitContainers length = %d, want 1", len(pod.Spec.InitContainers))
	}
	setup := pod.Spec.InitContainers[0]
	if setup.Name != "project-setup" {
		t.Errorf("init container name = %q, want project-setup", setup.Name)
	}
	if len(setup.Env) != 1 || setup.Env[0].Name != "SWE_REPOSITORY" || setup.Env[0].Value != project.Spec.Repositories[0] {
		t.Errorf("init container Env = %#v, want SWE_REPOSITORY=%s", setup.Env, project.Spec.Repositories[0])
	}
	if len(setup.EnvFrom) != 1 || setup.EnvFrom[0].SecretRef == nil || setup.EnvFrom[0].SecretRef.Name != "project-config" {
		t.Errorf("init container EnvFrom = %#v, want project-config Secret", setup.EnvFrom)
	}
	if len(setup.VolumeMounts) != 1 || setup.VolumeMounts[0].MountPath != "/workspace" {
		t.Errorf("init container VolumeMounts = %#v, want /workspace", setup.VolumeMounts)
	}
}

func TestEnsurePodCreatesAndRotatesEphemeralSandboxdCredentials(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		Scheme: scheme,
	}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "environment-uid"},
	}
	tmpl := &platformv1alpha1.EnvironmentTemplate{
		Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "example/environment:latest", Size: "small"},
	}

	pod, err := reconciler.ensurePod(context.Background(), env, tmpl)
	if err != nil {
		t.Fatalf("ensurePod() error = %v", err)
	}
	identity := pod.Annotations[sandboxdauth.IdentityAnnotation]
	if identity == "" {
		t.Fatal("pod has no sandboxd identity")
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("environment pod must not mount a Kubernetes service account token")
	}
	credentialMount := pod.Spec.Containers[0].VolumeMounts[1]
	if credentialMount.MountPath != sandboxdCredentialMount || !credentialMount.ReadOnly {
		t.Fatalf("credential mount = %#v, want read-only non-workspace mount", credentialMount)
	}
	if pod.Spec.Volumes[1].Secret == nil || pod.Spec.Volumes[1].Secret.SecretName != envCredentialName(env) {
		t.Fatalf("credential volume = %#v", pod.Spec.Volumes[1])
	}
	if pod.Spec.Volumes[1].Secret.DefaultMode == nil || *pod.Spec.Volumes[1].Secret.DefaultMode != 0o444 {
		t.Fatalf("credential mode = %v, want readable by non-root sandboxd", pod.Spec.Volumes[1].Secret.DefaultMode)
	}

	var first corev1.Secret
	if err := reconciler.Get(context.Background(), client.ObjectKey{Namespace: env.Namespace, Name: envCredentialName(env)}, &first); err != nil {
		t.Fatal(err)
	}
	if first.Annotations[sandboxdauth.IdentityAnnotation] != identity {
		t.Fatal("Secret identity does not match pod identity")
	}
	var capabilityConfig sandboxdauth.Config
	if err := json.Unmarshal(first.Data[sandboxdauth.CapabilitiesKey], &capabilityConfig); err != nil {
		t.Fatal(err)
	}
	if len(capabilityConfig.Grants) != 1 || len(capabilityConfig.Grants[0].Capabilities) != 2 {
		t.Fatalf("capability grants = %#v, want one terminal/health grant", capabilityConfig.Grants)
	}
	block, _ := pem.Decode(first.Data[sandboxdauth.TLSCertKey])
	if block == nil {
		t.Fatal("Secret certificate is not PEM")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := certificate.VerifyHostname(identity); err != nil {
		t.Fatalf("certificate does not authenticate pod identity: %v", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	if _, err := certificate.Verify(x509.VerifyOptions{DNSName: identity, Roots: roots}); err != nil {
		t.Fatalf("certificate cannot be used as the pinned TLS identity: %v", err)
	}
	firstToken := pod.Annotations[sandboxdauth.TokenAnnotation]

	if err := reconciler.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	recreated, err := reconciler.ensurePod(context.Background(), env, tmpl)
	if err != nil {
		t.Fatalf("recreate ensurePod() error = %v", err)
	}
	var rotated corev1.Secret
	if err := reconciler.Get(context.Background(), client.ObjectKey{Namespace: env.Namespace, Name: envCredentialName(env)}, &rotated); err != nil {
		t.Fatal(err)
	}
	if recreated.Annotations[sandboxdauth.IdentityAnnotation] == identity {
		t.Fatal("pod recreation did not rotate TLS identity")
	}
	if recreated.Annotations[sandboxdauth.TokenAnnotation] == firstToken {
		t.Fatal("pod recreation did not rotate terminal capability")
	}
}

func TestSandboxdNetworkPolicyOnlyAllowsControlPlaneIngress(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := networkingv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(), Scheme: scheme,
		ControlPlaneNamespace: "platform-system", ControlPlaneName: "swe-platform", ControlPlaneInstance: "production",
	}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{
		Name: "test", Namespace: "project", UID: "environment-uid",
	}}

	ready, err := reconciler.ensureSandboxdNetworkPolicy(context.Background(), env)
	if err != nil || !ready {
		t.Fatal(err)
	}
	var policy networkingv1.NetworkPolicy
	if err := reconciler.Get(context.Background(), client.ObjectKey{Namespace: env.Namespace, Name: envNetworkPolicyName(env)}, &policy); err != nil {
		t.Fatal(err)
	}
	if len(policy.Spec.Ingress) != 1 || len(policy.Spec.Ingress[0].From) != 1 {
		t.Fatalf("unexpected ingress policy: %#v", policy.Spec.Ingress)
	}
	peer := policy.Spec.Ingress[0].From[0]
	if peer.PodSelector == nil || peer.PodSelector.MatchLabels["app.kubernetes.io/component"] != "control-plane" {
		t.Fatalf("ingress peer = %#v, want control-plane pods", peer)
	}
	if peer.NamespaceSelector == nil || peer.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != "platform-system" ||
		peer.PodSelector.MatchLabels["app.kubernetes.io/name"] != "swe-platform" ||
		peer.PodSelector.MatchLabels["app.kubernetes.io/instance"] != "production" {
		t.Fatalf("ingress peer = %#v, want this control-plane installation only", peer)
	}
	if len(policy.Spec.Ingress[0].Ports) != 1 || policy.Spec.Ingress[0].Ports[0].Port.IntVal != 50051 {
		t.Fatalf("ingress ports = %#v, want sandboxd only", policy.Spec.Ingress[0].Ports)
	}
}

func TestEnsurePodReplacesLegacyOrWrongOwnerPod(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{
		Name: "test", Namespace: "default", UID: "current-environment",
	}}
	legacyPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: envPodName(env), Namespace: env.Namespace,
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name,
			UID: "old-environment", Controller: ptr(true),
		}},
	}}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(legacyPod).Build(), Scheme: scheme,
	}

	pod, err := reconciler.ensurePod(context.Background(), env, &platformv1alpha1.EnvironmentTemplate{})
	if err != nil {
		t.Fatal(err)
	}
	if pod != nil {
		t.Fatal("ensurePod adopted an insecure pod from another environment incarnation")
	}
	var deleted corev1.Pod
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(legacyPod), &deleted); err == nil {
		t.Fatal("legacy pod was not deleted for secure recreation")
	}
}

func TestEnsurePodRetainsCurrentPodWhenSecretReadFails(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{
		Name: "test", Namespace: "default", UID: "environment-uid",
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: envPodName(env), Namespace: env.Namespace,
		Annotations: map[string]string{
			sandboxdRevisionAnnotation:      sandboxdSecurityRevision,
			sandboxdauth.IdentityAnnotation: "current.sandboxd.swe.dev",
			sandboxdauth.TrustAnnotation:    "public trust bundle",
			sandboxdauth.TokenAnnotation:    "terminal token",
		},
	}}
	if err := controllerutil.SetControllerReference(env, pod, scheme); err != nil {
		t.Fatal(err)
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	readErr := stderrors.New("transient Secret API failure")
	reconciler := &EnvironmentReconciler{
		Client: secretReadErrorClient{Client: baseClient, err: readErr}, Scheme: scheme,
	}

	got, err := reconciler.ensurePod(context.Background(), env, &platformv1alpha1.EnvironmentTemplate{})
	if got != nil || !stderrors.Is(err, readErr) {
		t.Fatalf("ensurePod() = (%#v, %v), want transient Secret error", got, err)
	}
	var retained corev1.Pod
	if err := baseClient.Get(context.Background(), client.ObjectKeyFromObject(pod), &retained); err != nil {
		t.Fatalf("current pod was deleted after transient Secret read failure: %v", err)
	}
}

type secretReadErrorClient struct {
	client.Client
	err error
}

func (c secretReadErrorClient) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if _, ok := object.(*corev1.Secret); ok {
		return c.err
	}
	return c.Client.Get(ctx, key, object, options...)
}

func TestStaleDependentsAreRemovedButForeignCollisionsArePreserved(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := networkingv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{
		Name: "test", Namespace: "default", UID: "current-environment",
	}}
	stalePVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: envPVCName(env), Namespace: env.Namespace,
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name,
			UID: "old-environment", Controller: ptr(true),
		}},
	}}
	foreignPolicy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{
		Name: envNetworkPolicyName(env), Namespace: env.Namespace,
	}}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(stalePVC, foreignPolicy).Build(), Scheme: scheme,
	}

	ready, err := reconciler.ensureWorkspacePVC(context.Background(), env, &platformv1alpha1.EnvironmentTemplate{})
	if err != nil || ready {
		t.Fatalf("stale PVC reconciliation = (%t, %v), want removed and requeue", ready, err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(stalePVC), &corev1.PersistentVolumeClaim{}); err == nil {
		t.Fatal("stale PVC from prior Environment UID was retained")
	}
	if ready, err := reconciler.ensureSandboxdNetworkPolicy(context.Background(), env); err == nil || ready {
		t.Fatalf("foreign NetworkPolicy reconciliation = (%t, %v), want collision error", ready, err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(foreignPolicy), &networkingv1.NetworkPolicy{}); err != nil {
		t.Fatal("foreign NetworkPolicy was modified or deleted")
	}
}

func TestEnvironmentDeletionStopsPodBeforeRevokingCredentials(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := networkingv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{
		Name: "test", Namespace: "default", UID: "environment-uid", Finalizers: []string{environmentFinalizer},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envPodName(env), Namespace: env.Namespace}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: envCredentialName(env), Namespace: env.Namespace}}
	policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: envNetworkPolicyName(env), Namespace: env.Namespace}}
	for _, object := range []client.Object{pod, secret, policy} {
		if err := controllerutil.SetControllerReference(env, object, scheme); err != nil {
			t.Fatal(err)
		}
	}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(env, pod, secret, policy).Build(), Scheme: scheme,
	}

	result, err := reconciler.reconcileDeleting(context.Background(), env)
	if err != nil || !result.Requeue {
		t.Fatalf("delete pod step = (%#v, %v)", result, err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(secret), &corev1.Secret{}); err != nil {
		t.Fatal("credentials were revoked before sandboxd stopped")
	}
	result, err = reconciler.reconcileDeleting(context.Background(), env)
	if err != nil || !result.Requeue {
		t.Fatalf("revoke credentials step = (%#v, %v)", result, err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(policy), &networkingv1.NetworkPolicy{}); err != nil {
		t.Fatal("network policy was removed before credentials were revoked")
	}
	result, err = reconciler.reconcileDeleting(context.Background(), env)
	if err != nil || !result.Requeue {
		t.Fatalf("remove network policy step = (%#v, %v)", result, err)
	}
	if _, err := reconciler.reconcileDeleting(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	var updated platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &updated); err != nil {
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(&updated, environmentFinalizer) {
		t.Fatal("security finalizer remained after ordered revocation")
	}
}

func TestSyncStatusReportsSetupForProjectInitialization(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "environment-uid"},
	}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build(),
		Scheme: scheme,
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "env-test", Namespace: "default"},
		Spec:       corev1.PodSpec{InitContainers: []corev1.Container{{Name: "project-setup"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}

	if err := reconciler.syncStatus(context.Background(), env, pod); err != nil {
		t.Fatalf("syncStatus() error = %v", err)
	}
	var updated platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != platformv1alpha1.EnvironmentPhaseSetup {
		t.Fatalf("Phase = %q, want Setup", updated.Status.Phase)
	}
}

func TestSyncStatusPublishesSandboxdEndpoint(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build(),
		Scheme: scheme,
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "env-test", Namespace: "default"},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			PodIP:      "10.0.0.7",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}

	if err := reconciler.syncStatus(context.Background(), env, pod); err != nil {
		t.Fatalf("syncStatus() error = %v", err)
	}
	var updated platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != platformv1alpha1.EnvironmentPhaseReady || updated.Status.Endpoints.Sandboxd != "10.0.0.7:50051" {
		t.Fatalf("Status = %#v, want Ready with sandboxd endpoint", updated.Status)
	}
}

func TestEnsurePodMarksProjectInitializationAsResume(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	project := &platformv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: platformv1alpha1.ProjectSpec{
			Repositories: []string{"https://github.com/example/repo"},
		},
	}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(project).Build(),
		Scheme: scheme,
	}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "environment-uid"},
		Spec: platformv1alpha1.EnvironmentSpec{
			ProjectRef:  project.Name,
			TemplateRef: "small",
		},
		Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseResuming},
	}
	tmpl := &platformv1alpha1.EnvironmentTemplate{
		Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "example/environment:latest", Size: "small"},
	}

	pod, err := reconciler.ensurePod(context.Background(), env, tmpl)
	if err != nil {
		t.Fatalf("ensurePod() error = %v", err)
	}
	setup := pod.Spec.InitContainers[0]
	if len(setup.Env) != 2 || setup.Env[1].Name != "SWE_RESUMING" || setup.Env[1].Value != "true" {
		t.Fatalf("init container Env = %#v, want SWE_RESUMING=true", setup.Env)
	}
}

func TestSyncStatusPreservesResumingWhilePodStarts(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Status:     platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseResuming},
	}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build(),
		Scheme: scheme,
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "env-test", Namespace: "default"},
		Spec:       corev1.PodSpec{InitContainers: []corev1.Container{{Name: "project-setup"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}

	if err := reconciler.syncStatus(context.Background(), env, pod); err != nil {
		t.Fatalf("syncStatus() error = %v", err)
	}
	var updated platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != platformv1alpha1.EnvironmentPhaseResuming {
		t.Fatalf("Phase = %q, want Resuming", updated.Status.Phase)
	}
}

func TestReconcilePausedWaitsForPodDeletion(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "environment-uid"},
		Status: platformv1alpha1.EnvironmentStatus{
			Phase:   platformv1alpha1.EnvironmentPhaseReady,
			PodName: "env-test",
		},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "env-test", Namespace: "default"}}
	if err := controllerutil.SetControllerReference(env, pod, scheme); err != nil {
		t.Fatal(err)
	}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, pod).Build(),
		Scheme: scheme,
	}

	result, err := reconciler.reconcilePaused(context.Background(), env)
	if err != nil {
		t.Fatalf("reconcilePaused() error = %v", err)
	}
	if !result.Requeue {
		t.Fatal("reconcilePaused() did not requeue after deleting the pod")
	}
	var updated platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != platformv1alpha1.EnvironmentPhaseReady {
		t.Fatalf("Phase = %q before pod deletion is observed, want Ready", updated.Status.Phase)
	}

	if _, err := reconciler.reconcilePaused(context.Background(), &updated); err != nil {
		t.Fatalf("second reconcilePaused() error = %v", err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != platformv1alpha1.EnvironmentPhasePaused || updated.Status.PodName != "" {
		t.Fatalf("Status = %#v, want Paused with no pod name", updated.Status)
	}
}

func TestReconcileIdleRequestsPauseAfterTimeout(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	lastActive := metav1.NewTime(time.Now().Add(-time.Minute))
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Status: platformv1alpha1.EnvironmentStatus{
			Phase:        platformv1alpha1.EnvironmentPhaseReady,
			PodName:      "env-test",
			LastActiveAt: &lastActive,
		},
	}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build(),
		Scheme: scheme,
	}
	tmpl := &platformv1alpha1.EnvironmentTemplate{
		Spec: platformv1alpha1.EnvironmentTemplateSpec{IdleTimeout: &metav1.Duration{Duration: 30 * time.Second}},
	}

	result, err := reconciler.reconcileIdle(context.Background(), env, tmpl)
	if err != nil {
		t.Fatalf("reconcileIdle() error = %v", err)
	}
	if !result.Requeue {
		t.Fatal("reconcileIdle() did not requeue after requesting pause")
	}
	var updated platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &updated); err != nil {
		t.Fatal(err)
	}
	if !updated.Spec.Paused || updated.Status.Phase != platformv1alpha1.EnvironmentPhaseIdle {
		t.Fatalf("Environment = %#v, want paused with Idle phase", updated)
	}
}

func TestReconcileIdleSchedulesRemainingTimeout(t *testing.T) {
	lastActive := metav1.Now()
	env := &platformv1alpha1.Environment{
		Status: platformv1alpha1.EnvironmentStatus{LastActiveAt: &lastActive},
	}
	tmpl := &platformv1alpha1.EnvironmentTemplate{
		Spec: platformv1alpha1.EnvironmentTemplateSpec{IdleTimeout: &metav1.Duration{Duration: time.Minute}},
	}
	reconciler := &EnvironmentReconciler{}

	result, err := reconciler.reconcileIdle(context.Background(), env, tmpl)
	if err != nil {
		t.Fatalf("reconcileIdle() error = %v", err)
	}
	if result.RequeueAfter <= 0 || result.RequeueAfter > time.Minute {
		t.Fatalf("RequeueAfter = %s, want remaining one-minute timeout", result.RequeueAfter)
	}
}

func TestSyncStatusRefreshesActivityWhenSetupBecomesReady(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	oldActivity := metav1.NewTime(time.Now().Add(-time.Hour))
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Status:     platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseSetup, LastActiveAt: &oldActivity},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "env-test", Namespace: "default"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning, PodIP: "10.0.0.1",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build(),
		Scheme: scheme,
	}
	if err := reconciler.syncStatus(context.Background(), env, pod); err != nil {
		t.Fatal(err)
	}
	var updated platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != platformv1alpha1.EnvironmentPhaseReady || updated.Status.LastActiveAt == nil || !updated.Status.LastActiveAt.After(oldActivity.Time) {
		t.Fatalf("status = %#v, want newly ready with refreshed activity", updated.Status)
	}
}
