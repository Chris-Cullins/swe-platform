package controllers

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	stderrors "errors"
	"os"
	"os/exec"
	"path/filepath"
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
	if len(setup.Env) != 3 || setup.Env[0].Name != "SWE_REPOSITORY" || setup.Env[0].Value != project.Spec.Repositories[0] ||
		setup.Env[1].Name != "SWE_HOOK_TIMEOUT" || setup.Env[1].Value != projectHookTimeout ||
		setup.Env[2].Name != "SWE_HOOK_KILL_AFTER" || setup.Env[2].Value != hookKillAfter {
		t.Errorf("init container Env = %#v, want repository and bounded hook timeout", setup.Env)
	}
	if len(setup.EnvFrom) != 1 || setup.EnvFrom[0].SecretRef == nil || setup.EnvFrom[0].SecretRef.Name != "project-config" {
		t.Errorf("init container EnvFrom = %#v, want project-config Secret", setup.EnvFrom)
	}
	if len(setup.VolumeMounts) != 1 || setup.VolumeMounts[0].MountPath != "/workspace" {
		t.Errorf("init container VolumeMounts = %#v, want /workspace", setup.VolumeMounts)
	}
}

func TestHookRunnerBoundsAndPropagatesExecution(t *testing.T) {
	directory := t.TempDir()
	run := func(name, contents, timeout, killAfter string) (int, time.Duration) {
		t.Helper()
		hook := filepath.Join(directory, name)
		if err := os.WriteFile(hook, []byte(contents), 0o700); err != nil {
			t.Fatal(err)
		}
		command := exec.Command("/bin/sh", "-c", hookRunnerScript+"\nrun_hook \"$HOOK\"\n")
		command.Env = append(os.Environ(), "HOOK="+hook, "SWE_HOOK_TIMEOUT="+timeout, "SWE_HOOK_KILL_AFTER="+killAfter)
		started := time.Now()
		err := command.Run()
		elapsed := time.Since(started)
		if err == nil {
			return 0, elapsed
		}
		var exitError *exec.ExitError
		if !stderrors.As(err, &exitError) {
			t.Fatalf("run hook: %v", err)
		}
		return exitError.ExitCode(), elapsed
	}

	output := filepath.Join(directory, "ran")
	hook := filepath.Join(directory, "success")
	if err := os.WriteFile(hook, []byte("echo ran > \"$OUTPUT\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("/bin/sh", "-c", hookRunnerScript+"\nrun_hook \"$HOOK\"\n")
	command.Env = append(os.Environ(), "HOOK="+hook, "OUTPUT="+output, "SWE_HOOK_TIMEOUT=1s", "SWE_HOOK_KILL_AFTER=1s")
	if err := command.Run(); err != nil {
		t.Fatalf("successful hook: %v", err)
	}
	if _, err := os.Stat(output); err != nil {
		t.Fatal("successful hook did not run")
	}
	if code, _ := run("failure", "exit 7\n", "1s", "1s"); code != 7 {
		t.Fatalf("failing hook exit = %d, want 7", code)
	}
	if code, elapsed := run("timeout", "trap '' TERM\nwhile :; do sleep 1; done\n", "0.1s", "0.1s"); code != 124 || elapsed > 2*time.Second {
		t.Fatalf("timed-out hook = exit %d after %s, want exit 124 within bound", code, elapsed)
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
	container := pod.Spec.Containers[0]
	for name, probe := range map[string]*corev1.Probe{"startup": container.StartupProbe, "readiness": container.ReadinessProbe, "liveness": container.LivenessProbe} {
		if probe == nil || probe.Exec == nil || len(probe.Exec.Command) < 2 || probe.Exec.Command[1] != "healthcheck" {
			t.Errorf("%s probe = %#v, want authenticated sandboxd health RPC", name, probe)
		}
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
	if len(capabilityConfig.Grants) != 3 || len(capabilityConfig.Grants[0].Capabilities) != 2 ||
		len(capabilityConfig.Grants[1].Capabilities) != 1 || capabilityConfig.Grants[1].Capabilities[0] != sandboxdauth.CapabilityHealth ||
		len(capabilityConfig.Grants[2].Capabilities) != 1 || capabilityConfig.Grants[2].Capabilities[0] != sandboxdauth.CapabilityProcess {
		t.Fatalf("capability grants = %#v, want terminal, probe health, and distinct process grants", capabilityConfig.Grants)
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
			sandboxdRevisionAnnotation:        sandboxdSecurityRevision,
			sandboxdauth.IdentityAnnotation:   "current.sandboxd.swe.dev",
			sandboxdauth.TrustAnnotation:      "public trust bundle",
			sandboxdauth.TokenAnnotation:      "terminal token",
			sandboxdauth.SecretNameAnnotation: envCredentialName(env),
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
			sandboxdRevisionAnnotation:        sandboxdSecurityRevision,
			sandboxdauth.IdentityAnnotation:   "current.sandboxd.swe.dev",
			sandboxdauth.TrustAnnotation:      "public trust bundle",
			sandboxdauth.TokenAnnotation:      "terminal token",
			sandboxdauth.SecretNameAnnotation: envCredentialName(env),
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

func TestPodReplacementWithdrawsReadinessBeforeDeletion(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "env-uid", Generation: 3}, Status: platformv1alpha1.EnvironmentStatus{
		ObservedGeneration: 3, Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-test",
		Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue,
			ObservedGeneration: 3, Reason: "SandboxdReady", Message: "sandboxd is ready"}},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envPodName(env), Namespace: env.Namespace, UID: "old-pod",
		Annotations: map[string]string{sandboxdRevisionAnnotation: "1"}}}
	if err := controllerutil.SetControllerReference(env, pod, scheme); err != nil {
		t.Fatal(err)
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, pod).Build()
	readinessWithdrawn := false
	interceptedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Delete: func(ctx context.Context, underlying client.WithWatch, object client.Object, options ...client.DeleteOption) error {
			var current platformv1alpha1.Environment
			if err := underlying.Get(ctx, client.ObjectKeyFromObject(env), &current); err != nil {
				return err
			}
			readinessWithdrawn = !platformv1alpha1.IsEnvironmentReady(&current) && current.Status.PodName == "" && current.Status.Endpoints.Sandboxd == ""
			return underlying.Delete(ctx, object, options...)
		},
	})
	reconciler := &EnvironmentReconciler{Client: interceptedClient, Scheme: scheme}

	if _, err := reconciler.ensurePod(context.Background(), env, &platformv1alpha1.EnvironmentTemplate{}); err != nil {
		t.Fatal(err)
	}
	if !readinessWithdrawn {
		t.Fatal("pod replacement deleted the old incarnation before withdrawing readiness")
	}
}

func TestTerminatingPodCannotRemainReady(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	deletedAt := metav1.Now()
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "env-uid", Generation: 2}, Status: platformv1alpha1.EnvironmentStatus{
		ObservedGeneration: 2, Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-test",
		Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue,
			ObservedGeneration: 2, Reason: "SandboxdReady", Message: "sandboxd is ready"}},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envPodName(env), Namespace: env.Namespace, UID: "pod-uid",
		DeletionTimestamp: &deletedAt, Finalizers: []string{"test/hold"}}}
	if err := controllerutil.SetControllerReference(env, pod, scheme); err != nil {
		t.Fatal(err)
	}
	reconciler := &EnvironmentReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, pod).Build(), Scheme: scheme}
	got, err := reconciler.ensurePod(context.Background(), env, &platformv1alpha1.EnvironmentTemplate{})
	if err != nil || got != nil {
		t.Fatalf("ensurePod() = (%#v, %v), want wait for terminating pod", got, err)
	}
	var updated platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &updated); err != nil {
		t.Fatal(err)
	}
	ready := apimeta.FindStatusCondition(updated.Status.Conditions, platformv1alpha1.EnvironmentConditionReady)
	if platformv1alpha1.IsEnvironmentReady(&updated) || updated.Status.PodName != "" || updated.Status.Endpoints.Sandboxd != "" || ready == nil || ready.Reason != "PodTerminating" {
		t.Fatalf("terminating pod status = %#v", updated.Status)
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

func TestReconcileRejectsUnsupportedEffectiveBackendBeforeCreatingChildren(t *testing.T) {
	tests := []struct {
		name            string
		environment     platformv1alpha1.EnvironmentBackend
		template        platformv1alpha1.EnvironmentBackend
		wantUnsupported platformv1alpha1.EnvironmentBackend
	}{
		{name: "template backend", template: platformv1alpha1.EnvironmentBackendKubeVirt, wantUnsupported: platformv1alpha1.EnvironmentBackendKubeVirt},
		{name: "environment override", environment: platformv1alpha1.EnvironmentBackendExternalRunner, template: platformv1alpha1.EnvironmentBackendPod, wantUnsupported: platformv1alpha1.EnvironmentBackendExternalRunner},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := platformv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			env := &platformv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "environment-uid", Generation: 2, Finalizers: []string{environmentFinalizer}},
				Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "small", Backend: test.environment},
			}
			template := &platformv1alpha1.EnvironmentTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default"},
				Spec:       platformv1alpha1.EnvironmentTemplateSpec{Backend: test.template},
			}
			reconciler := &EnvironmentReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, template).Build(), Scheme: scheme}
			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)})
			if err != nil || result.Requeue || result.RequeueAfter != 0 {
				t.Fatalf("Reconcile() = (%#v, %v), want terminal success", result, err)
			}
			var updated platformv1alpha1.Environment
			if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &updated); err != nil {
				t.Fatal(err)
			}
			condition := apimeta.FindStatusCondition(updated.Status.Conditions, platformv1alpha1.EnvironmentConditionReady)
			if updated.Status.Phase != platformv1alpha1.EnvironmentPhaseFailed || condition == nil || condition.Reason != "UnsupportedBackend" || !strings.Contains(condition.Message, string(test.wantUnsupported)) {
				t.Fatalf("unsupported backend status = phase %q, condition %#v", updated.Status.Phase, condition)
			}
			var pods corev1.PodList
			var pvcs corev1.PersistentVolumeClaimList
			if err := reconciler.List(context.Background(), &pods, client.InNamespace(env.Namespace)); err != nil {
				t.Fatal(err)
			}
			if err := reconciler.List(context.Background(), &pvcs, client.InNamespace(env.Namespace)); err != nil {
				t.Fatal(err)
			}
			if len(pods.Items) != 0 || len(pvcs.Items) != 0 {
				t.Fatalf("unsupported backend created %d Pods and %d PVCs", len(pods.Items), len(pvcs.Items))
			}
		})
	}
}

func TestEffectiveEnvironmentBackendPrecedence(t *testing.T) {
	template := &platformv1alpha1.EnvironmentTemplate{Spec: platformv1alpha1.EnvironmentTemplateSpec{Backend: platformv1alpha1.EnvironmentBackendKubeVirt}}
	environment := &platformv1alpha1.Environment{}
	if got := platformv1alpha1.EffectiveEnvironmentBackend(environment, template); got != platformv1alpha1.EnvironmentBackendKubeVirt {
		t.Fatalf("template backend = %q, want kubevirt", got)
	}
	environment.Spec.Backend = platformv1alpha1.EnvironmentBackendPod
	if got := platformv1alpha1.EffectiveEnvironmentBackend(environment, template); got != platformv1alpha1.EnvironmentBackendPod {
		t.Fatalf("environment override = %q, want pod", got)
	}
	if got := platformv1alpha1.EffectiveEnvironmentBackend(&platformv1alpha1.Environment{}, &platformv1alpha1.EnvironmentTemplate{}); got != platformv1alpha1.EnvironmentBackendPod {
		t.Fatalf("empty backend default = %q, want pod", got)
	}
}

func TestUnsupportedBackendWithdrawsReadinessBeforeStoppingOwnedPod(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "environment-uid", Generation: 2},
		Status: platformv1alpha1.EnvironmentStatus{
			ObservedGeneration: 2, Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-test",
			Endpoints:  platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
			Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 2, Reason: "SandboxdReady"}},
		},
	}
	controller := true
	owner := metav1.OwnerReference{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: &controller}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envPodName(env), Namespace: env.Namespace, UID: "pod-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: envCredentialName(env), Namespace: env.Namespace, UID: "secret-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: envPVCName(env), Namespace: env.Namespace, UID: "pvc-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
	reconciler := &EnvironmentReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, pod, secret, pvc).Build(), Scheme: scheme}

	result, err := reconciler.reconcileUnsupportedBackend(context.Background(), env, platformv1alpha1.EnvironmentBackendKubeVirt)
	if err != nil || !result.Requeue {
		t.Fatalf("withdraw readiness = (%#v, %v)", result, err)
	}
	var updated platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &updated); err != nil {
		t.Fatal(err)
	}
	ready := apimeta.FindStatusCondition(updated.Status.Conditions, platformv1alpha1.EnvironmentConditionReady)
	if platformv1alpha1.IsEnvironmentReady(&updated) || updated.Status.PodName != "" || updated.Status.Endpoints.Sandboxd != "" || ready == nil || ready.Reason != "UnsupportedBackend" {
		t.Fatalf("readiness was not withdrawn first: %#v", updated.Status)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
		t.Fatal("pod was deleted before readiness withdrawal")
	}
	if _, err := reconciler.reconcileUnsupportedBackend(context.Background(), &updated, platformv1alpha1.EnvironmentBackendKubeVirt); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("owned pod still exists: %v", err)
	}
	if _, err := reconciler.reconcileUnsupportedBackend(context.Background(), &updated, platformv1alpha1.EnvironmentBackendKubeVirt); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(secret), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("credential still exists: %v", err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(pvc), &corev1.PersistentVolumeClaim{}); err != nil {
		t.Fatal("workspace PVC was not retained")
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
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test", Namespace: "default", UID: "environment-uid", Generation: 1,
			Finalizers: []string{environmentFinalizer},
		},
		Status: platformv1alpha1.EnvironmentStatus{
			ObservedGeneration: 1,
			Phase:              platformv1alpha1.EnvironmentPhaseReady,
			PodName:            "env-test",
			Endpoints:          platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
			Conditions: []metav1.Condition{{
				Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1,
				Reason: "SandboxdReady", LastTransitionTime: metav1.Now(),
			}},
		},
	}
	deletingEnv := env.DeepCopy()
	deletedAt := metav1.Now()
	deletingEnv.DeletionTimestamp = &deletedAt
	if platformv1alpha1.IsEnvironmentReady(deletingEnv) {
		t.Fatal("deleting Environment reported ready")
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envPodName(env), Namespace: env.Namespace, UID: "pod-uid"}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: envCredentialName(env), Namespace: env.Namespace, UID: "secret-uid"}}
	policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: envNetworkPolicyName(env), Namespace: env.Namespace, UID: "policy-uid"}}
	for _, object := range []client.Object{pod, secret, policy} {
		if err := controllerutil.SetControllerReference(env, object, scheme); err != nil {
			t.Fatal(err)
		}
	}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, pod, secret, policy).Build(), Scheme: scheme,
	}

	result, err := reconciler.reconcileDeleting(context.Background(), env)
	if err != nil || !result.Requeue {
		t.Fatalf("delete pod step = (%#v, %v)", result, err)
	}
	var deleting platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &deleting); err != nil {
		t.Fatal(err)
	}
	ready := apimeta.FindStatusCondition(deleting.Status.Conditions, platformv1alpha1.EnvironmentConditionReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "Deleting" || deleting.Status.PodName != "" || deleting.Status.Endpoints.Sandboxd != "" {
		t.Fatalf("status before pod deletion = %#v, want readiness and endpoint withdrawn", deleting.Status)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(secret), &corev1.Secret{}); err != nil {
		t.Fatal("credentials were revoked before sandboxd stopped")
	}
	result, err = reconciler.reconcileDeleting(context.Background(), &deleting)
	if err != nil || !result.Requeue {
		t.Fatalf("revoke credentials step = (%#v, %v)", result, err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(policy), &networkingv1.NetworkPolicy{}); err != nil {
		t.Fatal("network policy was removed before credentials were revoked")
	}
	result, err = reconciler.reconcileDeleting(context.Background(), &deleting)
	if err != nil || !result.Requeue {
		t.Fatalf("remove network policy step = (%#v, %v)", result, err)
	}
	if _, err := reconciler.reconcileDeleting(context.Background(), &deleting); err != nil {
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

	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", Generation: 4}}
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
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "environment", ImageID: "ghcr.io/example/env@sha256:0123456789abcdef",
			}},
		},
	}

	if err := reconciler.syncStatus(context.Background(), env, pod); err != nil {
		t.Fatalf("syncStatus() error = %v", err)
	}
	var updated platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != platformv1alpha1.EnvironmentPhaseReady || updated.Status.Endpoints.Sandboxd != "10.0.0.7:50051" || updated.Status.ImageID != "ghcr.io/example/env@sha256:0123456789abcdef" {
		t.Fatalf("Status = %#v, want Ready with sandboxd endpoint and immutable image ID", updated.Status)
	}
	ready := apimeta.FindStatusCondition(updated.Status.Conditions, platformv1alpha1.EnvironmentConditionReady)
	if updated.Status.ObservedGeneration != updated.Generation || ready == nil || ready.Status != metav1.ConditionTrue || ready.ObservedGeneration != updated.Generation || ready.Reason != "SandboxdReady" {
		t.Fatalf("generation-aware Ready condition = %#v, status generation = %d", ready, updated.Status.ObservedGeneration)
	}
}

func TestEnvironmentPodStateSurfacesReadinessFailures(t *testing.T) {
	waiting := func(reason, message string) corev1.ContainerState {
		return corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason, Message: message}}
	}
	terminated := func(exitCode int32, message string) corev1.ContainerState {
		return corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: exitCode, Message: message}}
	}
	for _, tc := range []struct {
		name       string
		env        platformv1alpha1.Environment
		pod        corev1.Pod
		wantPhase  platformv1alpha1.EnvironmentPhase
		wantReason string
	}{
		{
			name: "unschedulable",
			pod: corev1.Pod{Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse,
					Reason: corev1.PodReasonUnschedulable, Message: "insufficient cpu"}},
			}},
			wantPhase: platformv1alpha1.EnvironmentPhaseCreating, wantReason: "Unschedulable",
		},
		{
			name: "setup failed",
			pod: corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending,
				InitContainerStatuses: []corev1.ContainerStatus{{Name: "project-setup", State: terminated(1, "setup error")}}}},
			wantPhase: platformv1alpha1.EnvironmentPhaseFailed, wantReason: "SetupFailed",
		},
		{
			name: "setup hook timeout",
			pod: corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending,
				InitContainerStatuses: []corev1.ContainerStatus{{Name: "project-setup", State: terminated(124, "")}}}},
			wantPhase: platformv1alpha1.EnvironmentPhaseFailed, wantReason: "SetupHookTimedOut",
		},
		{
			name: "resume hook timeout",
			env:  platformv1alpha1.Environment{Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseFailed}},
			pod: corev1.Pod{
				Spec: corev1.PodSpec{InitContainers: []corev1.Container{{Env: []corev1.EnvVar{{Name: "SWE_RESUMING", Value: "true"}}}}},
				Status: corev1.PodStatus{Phase: corev1.PodPending, InitContainerStatuses: []corev1.ContainerStatus{{
					Name: "project-setup", State: waiting("CrashLoopBackOff", "retrying"), LastTerminationState: terminated(124, ""),
				}}},
			},
			wantPhase: platformv1alpha1.EnvironmentPhaseFailed, wantReason: "ResumeHookTimedOut",
		},
		{
			name: "image pull",
			pod: corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending,
				ContainerStatuses: []corev1.ContainerStatus{{Name: "environment", State: waiting("ImagePullBackOff", "image not found")}}}},
			wantPhase: platformv1alpha1.EnvironmentPhaseFailed, wantReason: "ImagePullFailed",
		},
		{
			name: "sandboxd crash loop",
			pod: corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{{Name: "environment", State: waiting("CrashLoopBackOff", "back-off restarting")}}}},
			wantPhase: platformv1alpha1.EnvironmentPhaseFailed, wantReason: "SandboxdCrashLoopBackOff",
		},
		{
			name:      "sandboxd not ready",
			pod:       corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.1"}},
			wantPhase: platformv1alpha1.EnvironmentPhaseCreating, wantReason: "SandboxdNotReady",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			phase, reason, message := environmentPodState(&tc.env, &tc.pod)
			if phase != tc.wantPhase || reason != tc.wantReason || message == "" {
				t.Fatalf("state = (%s, %s, %q), want (%s, %s, actionable message)", phase, reason, message, tc.wantPhase, tc.wantReason)
			}
		})
	}
}

func TestEnvironmentPodStateIgnoresFailureBeforeSuccessfulInitRetry(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{InitContainers: []corev1.Container{{Name: "project-setup"}}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning, PodIP: "10.0.0.1",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name:                 "project-setup",
				State:                corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 124}},
			}},
		},
	}
	phase, reason, _ := environmentPodState(&platformv1alpha1.Environment{}, pod)
	if phase != platformv1alpha1.EnvironmentPhaseReady || reason != "SandboxdReady" {
		t.Fatalf("state after successful init retry = (%s, %s), want Ready", phase, reason)
	}
}

func TestEnvironmentStatusRetriesConflictAndPreservesConditions(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", Generation: 2}, Status: platformv1alpha1.EnvironmentStatus{
		Conditions: []metav1.Condition{{Type: "Audit", Status: metav1.ConditionTrue, Reason: "Recorded", Message: "preserve me"}},
	}}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build()
	conflicts := 0
	interceptedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, underlying client.Client, subresource string, object client.Object, options ...client.SubResourceUpdateOption) error {
			if subresource == "status" && conflicts == 0 {
				conflicts++
				return apierrors.NewConflict(schema.GroupResource{Group: platformv1alpha1.GroupVersion.Group, Resource: "environments"}, object.GetName(), stderrors.New("simulated conflict"))
			}
			return underlying.SubResource(subresource).Update(ctx, object, options...)
		},
	})
	reconciler := &EnvironmentReconciler{Client: interceptedClient, Scheme: scheme}

	if err := reconciler.setEnvironmentStatus(context.Background(), env, platformv1alpha1.EnvironmentPhaseReady, "env-test", "10.0.0.1:50051", "SandboxdReady", "sandboxd is ready"); err != nil {
		t.Fatal(err)
	}
	var updated platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &updated); err != nil {
		t.Fatal(err)
	}
	if conflicts != 1 || !platformv1alpha1.IsEnvironmentReady(&updated) || apimeta.FindStatusCondition(updated.Status.Conditions, "Audit") == nil {
		t.Fatalf("status after conflict = %#v, conflicts = %d", updated.Status, conflicts)
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
	if len(setup.Env) != 4 || setup.Env[3].Name != "SWE_RESUMING" || setup.Env[3].Value != "true" {
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
	ready := apimeta.FindStatusCondition(updated.Status.Conditions, platformv1alpha1.EnvironmentConditionReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "ResumeInProgress" {
		t.Fatalf("Ready during resume = %#v, want false ResumeInProgress", ready)
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
	ready := apimeta.FindStatusCondition(updated.Status.Conditions, platformv1alpha1.EnvironmentConditionReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "Paused" {
		t.Fatalf("Ready while paused = %#v, want false Paused", ready)
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
