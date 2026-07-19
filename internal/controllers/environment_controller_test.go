package controllers

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	stderrors "errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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
	for _, item := range pod.Spec.Volumes[1].Secret.Items {
		if item.Key == sandboxdauth.ProcessTokenKey {
			t.Fatal("private adapter process token was mounted into the environment pod")
		}
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
	if len(capabilityConfig.Grants) != 2 || len(capabilityConfig.Grants[0].Capabilities) != 2 || len(capabilityConfig.Grants[1].Capabilities) != 1 || capabilityConfig.Grants[1].Capabilities[0] != sandboxdauth.CapabilityProcess {
		t.Fatalf("capability grants = %#v, want terminal/health and distinct process grants", capabilityConfig.Grants)
	}
	if _, published := pod.Annotations[sandboxdauth.ProcessTokenKey]; published {
		t.Fatal("process token was published on pod")
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
	if string(first.Data[sandboxdauth.ProcessTokenKey]) == "" || string(first.Data[sandboxdauth.ProcessTokenKey]) == firstToken {
		t.Fatal("Secret must contain a distinct private process token")
	}
	if string(first.Data[sandboxdauth.CapabilitiesKey]) == string(first.Data[sandboxdauth.ProcessTokenKey]) ||
		strings.Contains(string(first.Data[sandboxdauth.CapabilitiesKey]), string(first.Data[sandboxdauth.ProcessTokenKey])) {
		t.Fatal("mounted capability data exposes the raw process token")
	}

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
	if len(policy.Spec.Ingress) != 1 || len(policy.Spec.Ingress[0].From) != 2 {
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
	if got := policy.Spec.Ingress[0].From[1].PodSelector.MatchLabels["app.kubernetes.io/component"]; got != "operator" {
		t.Fatalf("second ingress peer component = %q, want operator", got)
	}
}

func TestEnsurePodRefusesWrongOwnerPod(t *testing.T) {
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
	var collision *childOwnershipCollisionError
	if pod != nil || !stderrors.As(err, &collision) {
		t.Fatalf("ensurePod() = (%#v, %v), want ownership collision", pod, err)
	}
	var retained corev1.Pod
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(legacyPod), &retained); err != nil {
		t.Fatal("wrong-owner pod was modified or deleted")
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

func TestEnsurePodRetainsCurrentPodAndForeignSecretOnCollision(t *testing.T) {
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
	foreignSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: envCredentialName(env), Namespace: env.Namespace,
	}}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod, foreignSecret).Build(), Scheme: scheme,
	}

	got, err := reconciler.ensurePod(context.Background(), env, &platformv1alpha1.EnvironmentTemplate{})
	var collision *childOwnershipCollisionError
	if got != nil || !stderrors.As(err, &collision) {
		t.Fatalf("ensurePod() = (%#v, %v), want Secret ownership collision", got, err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
		t.Fatal("current pod was deleted because of a foreign Secret")
	}
	var retainedSecret corev1.Secret
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(foreignSecret), &retainedSecret); err != nil {
		t.Fatal("foreign Secret was modified or deleted")
	}
	if len(retainedSecret.Data) != 0 || len(retainedSecret.Annotations) != 0 {
		t.Fatal("foreign Secret was mutated")
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

func TestWrongOwnerAndForeignDependentsArePreserved(t *testing.T) {
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
	var collision *childOwnershipCollisionError
	if ready || !stderrors.As(err, &collision) {
		t.Fatalf("wrong-owner PVC reconciliation = (%t, %v), want collision", ready, err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(stalePVC), &corev1.PersistentVolumeClaim{}); err != nil {
		t.Fatal("wrong-owner PVC was modified or deleted")
	}
	if ready, err := reconciler.ensureSandboxdNetworkPolicy(context.Background(), env); err == nil || ready {
		t.Fatalf("foreign NetworkPolicy reconciliation = (%t, %v), want collision error", ready, err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(foreignPolicy), &networkingv1.NetworkPolicy{}); err != nil {
		t.Fatal("foreign NetworkPolicy was modified or deleted")
	}
}

func TestChildNamesAreBoundedAndScopedToEnvironmentUID(t *testing.T) {
	longName := strings.Repeat("a", 253)
	first := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: longName, UID: "first-environment-uid"}}
	second := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: longName, UID: "second-environment-uid"}}

	for _, name := range []string{envPodName(first), envPVCName(first), envCredentialName(first), envNetworkPolicyName(first)} {
		if len(name) > 63 {
			t.Errorf("child name length = %d, want at most 63: %q", len(name), name)
		}
		if problems := validation.IsDNS1123Subdomain(name); len(problems) != 0 {
			t.Errorf("child name %q is invalid: %v", name, problems)
		}
	}
	if envPodName(first) == envPodName(second) || envPVCName(first) == envPVCName(second) {
		t.Fatal("same-name Environment recreations share child names")
	}

	legacy := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: strings.Repeat("a", 63), UID: "legacy-uid"}}
	wantPodName := "env-" + legacy.Name
	if envPodName(legacy) != wantPodName || envPVCName(legacy) != wantPodName {
		t.Fatalf("63-character Environment did not retain legacy Pod/PVC name %q", wantPodName)
	}
	wantSandboxdName := wantPodName + "-sandboxd"
	if envCredentialName(legacy) != wantSandboxdName || envNetworkPolicyName(legacy) != wantSandboxdName {
		t.Fatalf("63-character Environment did not retain legacy sandboxd name %q", wantSandboxdName)
	}
	for _, name := range []string{wantPodName, wantSandboxdName} {
		if problems := validation.IsDNS1123Subdomain(name); len(problems) != 0 {
			t.Errorf("legacy child name %q is invalid: %v", name, problems)
		}
	}
}

func TestDeleteObservedChildUsesUIDPrecondition(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	replacement := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "environment", Namespace: "default", UID: "replacement-uid"}}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(replacement).Build()
	var preconditionUID *types.UID
	interceptedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Delete: func(_ context.Context, _ client.WithWatch, object client.Object, options ...client.DeleteOption) error {
			deleteOptions := &client.DeleteOptions{}
			for _, option := range options {
				option.ApplyToDelete(deleteOptions)
			}
			if deleteOptions.Preconditions != nil {
				preconditionUID = deleteOptions.Preconditions.UID
			}
			return apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, object.GetName(), stderrors.New("UID mismatch"))
		},
	})
	reconciler := &EnvironmentReconciler{Client: interceptedClient, Scheme: scheme}
	observed := replacement.DeepCopy()
	observed.UID = "observed-uid"

	if err := reconciler.deleteObservedChild(context.Background(), observed); !apierrors.IsConflict(err) {
		t.Fatalf("deleteObservedChild() error = %v, want UID precondition conflict", err)
	}
	if preconditionUID == nil || *preconditionUID != observed.UID {
		t.Fatalf("delete UID precondition = %v, want %q", preconditionUID, observed.UID)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(replacement), &corev1.Pod{}); err != nil {
		t.Fatal("replacement Pod was deleted despite UID precondition")
	}
}

func TestNewEnvironmentRefusesTerminatingPriorIncarnationPVC(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	oldEnv := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "old-uid"}}
	newEnv := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "new-uid"}}
	deletedAt := metav1.Now()
	oldPVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: envPVCName(oldEnv), Namespace: oldEnv.Namespace, DeletionTimestamp: &deletedAt, Finalizers: []string{"test/finalizer"},
	}}
	if err := controllerutil.SetControllerReference(oldEnv, oldPVC, scheme); err != nil {
		t.Fatal(err)
	}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(oldPVC).Build(), Scheme: scheme,
	}

	ready, err := reconciler.ensureWorkspacePVC(context.Background(), newEnv, &platformv1alpha1.EnvironmentTemplate{})
	var collision *childOwnershipCollisionError
	if ready || !stderrors.As(err, &collision) {
		t.Fatalf("new incarnation PVC reconciliation = (%t, %v), want ownership collision", ready, err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(oldPVC), &corev1.PersistentVolumeClaim{}); err != nil {
		t.Fatal("terminating prior-incarnation PVC was touched")
	}
}

func TestReconcileReportsStableChildOwnershipCondition(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "environment-uid", Generation: 3, Finalizers: []string{environmentFinalizer}},
		Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
	}
	template := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default"}}
	foreignPVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: envPVCName(env), Namespace: env.Namespace}}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, template, foreignPVC).Build(), Scheme: scheme,
	}

	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: env.Namespace, Name: env.Name}}
	for i := 0; i < 2; i++ {
		result, err := reconciler.Reconcile(context.Background(), request)
		if err != nil || result.Requeue || result.RequeueAfter != 0 {
			t.Fatalf("reconcile %d = (%#v, %v), want stable success without requeue", i+1, result, err)
		}
	}
	var updated platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &updated); err != nil {
		t.Fatal(err)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, "ChildOwnershipConflict")
	if updated.Status.Phase != platformv1alpha1.EnvironmentPhaseFailed || condition == nil ||
		condition.Status != metav1.ConditionTrue || condition.Reason != "ResourceCollision" || condition.ObservedGeneration != env.Generation {
		t.Fatalf("collision status = phase %q, condition %#v", updated.Status.Phase, condition)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(foreignPVC), &corev1.PersistentVolumeClaim{}); err != nil {
		t.Fatal("foreign PVC was modified or deleted")
	}
}

func TestReconcilePausedRefusesForeignPod(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "environment-uid"}}
	foreignPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envPodName(env), Namespace: env.Namespace}}
	reconciler := &EnvironmentReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(foreignPod).Build(), Scheme: scheme}

	result, err := reconciler.reconcilePaused(context.Background(), env)
	var collision *childOwnershipCollisionError
	if result.Requeue || !stderrors.As(err, &collision) {
		t.Fatalf("reconcilePaused() = (%#v, %v), want ownership collision", result, err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(foreignPod), &corev1.Pod{}); err != nil {
		t.Fatal("pause modified or deleted a foreign pod")
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
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envPodName(env), Namespace: env.Namespace, UID: "pod-uid"}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: envCredentialName(env), Namespace: env.Namespace, UID: "secret-uid"}}
	policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: envNetworkPolicyName(env), Namespace: env.Namespace, UID: "policy-uid"}}
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
			Phase: platformv1alpha1.EnvironmentPhaseReady,
		},
	}
	env.Status.PodName = envPodName(env)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envPodName(env), Namespace: "default", UID: "pod-uid"}}
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

func TestPauseFencesEnvironmentWithoutReadableTemplate(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "env-uid", Finalizers: []string{environmentFinalizer}},
		Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "deleted-template", Paused: true},
		Status:     platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseFailed, PodName: "missing-pod"},
	}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build(),
		Scheme: scheme,
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)}); err != nil {
		t.Fatalf("paused reconcile depended on deleted template: %v", err)
	}
	var fenced platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &fenced); err != nil {
		t.Fatal(err)
	}
	if fenced.Status.Phase != platformv1alpha1.EnvironmentPhasePaused || fenced.Status.PodName != "" || fenced.Status.Endpoints.Sandboxd != "" {
		t.Fatalf("fenced status = %#v", fenced.Status)
	}
}
