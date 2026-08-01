package controllers

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
	sandboxdauth "github.com/Chris-Cullins/swe-platform/sandboxd/auth"
)

func provisioningRequest(env *platformv1alpha1.Environment) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)}
}

func TestWarmPolicyEditProjectSnapshotVerification(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, networkingv1.AddToScheme, platformv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	tmpl := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default", UID: "template-uid", Generation: 1}, Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "image", Size: "small"}}
	project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid", Generation: 2}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://example.test/repo"}}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "warm", Namespace: "default", UID: "env-uid", Generation: 2, Finalizers: []string{environmentFinalizer}}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: tmpl.Name, ProjectRef: project.Name}}
	env.Status.Provisioning = platformv1alpha1.ResolveEnvironmentProvisioning(&platformv1alpha1.Environment{Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: tmpl.Name}}, tmpl, nil)
	env.Status.Provisioning.TemplateVerified = true
	tmpl.Generation++
	tmpl.Spec.IdleTimeout = &metav1.Duration{Duration: time.Hour}
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, tmpl, project).Build()
	r := &EnvironmentReconciler{Client: base, Scheme: scheme}
	request := provisioningRequest(env)
	for range 2 {
		if _, err := r.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}
	var got platformv1alpha1.Environment
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Provisioning.Project == nil || !got.Status.Provisioning.ProjectVerified || got.Status.Provisioning.Template.Generation != 1 || got.Status.Provisioning.Project.Generation != project.Generation || got.Status.Provisioning.Project.Repository != project.Spec.Repositories[0] {
		t.Fatalf("warm Project snapshot was not exactly verified: %#v", got.Status.Provisioning)
	}

	stale := env.DeepCopy()
	stale.Name, stale.UID = "warm-stale", "stale-uid"
	stale.Status.Provisioning = platformv1alpha1.ResolveEnvironmentProvisioning(&platformv1alpha1.Environment{Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: tmpl.Name}}, tmpl, nil)
	stale.Status.Provisioning.TemplateVerified = true
	stale.Status.Provisioning.Image = "old-image"
	if platformv1alpha1.ProvisioningSnapshotCurrentTemplate(stale.Status.Provisioning, tmpl) {
		t.Fatal("provisioning edit remained warm-claimable")
	}
}

func TestEnvironmentControllerStaleProjectFenceScopedReplacement(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, networkingv1.AddToScheme, platformv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	installation := &platformv1alpha1.Installation{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "installation-uid"}}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team", UID: "namespace-uid", Annotations: map[string]string{
		tenancy.InstallationNamespaceAnnotation: "system", tenancy.InstallationNameAnnotation: "main", tenancy.InstallationUIDAnnotation: "installation-uid",
		tenancy.ProjectNameAnnotation: "app", tenancy.ProjectUIDAnnotation: "original-project", tenancy.LifecycleAnnotation: string(tenancy.LifecycleActive),
	}}}
	tmpl := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "team", UID: "template-uid", Generation: 1}, Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "image", Size: "small"}}
	original := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "team", UID: "original-project", Generation: 1}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://example.test/repo"}}}
	replacement := original.DeepCopy()
	replacement.UID = "replacement-project"
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "work", Namespace: "team", UID: "env-uid", Generation: 1, Finalizers: []string{environmentFinalizer}}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: tmpl.Name, ProjectRef: original.Name}, Status: platformv1alpha1.EnvironmentStatus{ObservedGeneration: 1, Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: envPodName(&platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "work"}}), Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"}, Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1}}}}
	setTestProvisioningSnapshot(env, tmpl, original)
	controller := true
	owner := metav1.OwnerReference{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: &controller}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envPodName(env), Namespace: env.Namespace, UID: "pod-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: envCredentialName(env), Namespace: env.Namespace, UID: "secret-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: envPVCName(env), Namespace: env.Namespace, UID: "pvc-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
	policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: envNetworkPolicyName(env), Namespace: env.Namespace, UID: "policy-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(installation, namespace, tmpl, replacement, env, pod, secret, pvc, policy).Build()
	verifier := &tenancy.Verifier{Reader: base, Installation: tenancy.InstallationIdentity{Key: types.NamespacedName{Namespace: "system", Name: "main"}, UID: installation.UID}, Mode: tenancy.ModeScoped}
	r := &EnvironmentReconciler{Client: tenancy.GuardedClient{Client: base, Verifier: verifier}, APIReader: base, Scheme: scheme, Scope: &tenancy.ReconcileScope{Verifier: verifier}}
	request := provisioningRequest(env)
	for range 6 {
		if _, err := r.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}
	var got platformv1alpha1.Environment
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != platformv1alpha1.EnvironmentPhaseFailed || got.Status.PodName != "" || got.Status.Endpoints.Sandboxd != "" || !platformv1alpha1.ProvisioningSnapshotsEqual(got.Status.Provisioning, env.Status.Provisioning) {
		t.Fatalf("stale Project fence status/snapshot = %#v", got.Status)
	}
	for _, deleted := range []client.Object{pod, secret} {
		if err := base.Get(context.Background(), client.ObjectKeyFromObject(deleted), deleted); !apierrors.IsNotFound(err) {
			t.Fatalf("%T retained: %v", deleted, err)
		}
	}
	for _, retained := range []client.Object{pvc, policy} {
		if err := base.Get(context.Background(), client.ObjectKeyFromObject(retained), retained); err != nil {
			t.Fatalf("%T removed: %v", retained, err)
		}
	}
	var pods corev1.PodList
	if err := base.List(context.Background(), &pods, client.InNamespace(env.Namespace)); err != nil || len(pods.Items) != 0 {
		t.Fatalf("children created/adopted: %d, %v", len(pods.Items), err)
	}
}

func TestProvisioningSnapshotAtomicPublicationAndFailClosedInputs(t *testing.T) {
	fixture := func(t *testing.T) (*runtime.Scheme, *platformv1alpha1.Environment, *platformv1alpha1.EnvironmentTemplate) {
		t.Helper()
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
		env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "env-uid", Generation: 1, Finalizers: []string{environmentFinalizer}}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: "small"}}
		tmpl := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default", UID: "template-uid", Generation: 1}, Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "image:v1", Size: "small"}}
		return scheme, env, tmpl
	}

	t.Run("initial atomic snapshot before any child", func(t *testing.T) {
		scheme, env, tmpl := fixture(t)
		env.Spec.ProjectRef = "project"
		project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: env.Namespace, UID: "project-uid", Generation: 3}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://example.test/repository"}}}
		c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, tmpl, project).Build()
		r := &EnvironmentReconciler{Client: c, Scheme: scheme}
		for i := 0; i < 3; i++ {
			result, err := r.Reconcile(context.Background(), provisioningRequest(env))
			if err != nil || !result.Requeue {
				t.Fatalf("Reconcile(%d) = (%#v, %v)", i, result, err)
			}
		}
		var got platformv1alpha1.Environment
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
			t.Fatal(err)
		}
		if err := platformv1alpha1.ValidateEnvironmentProvisioningSnapshot(&got, got.Status.Provisioning); err != nil {
			t.Fatal(err)
		}
		want := platformv1alpha1.ResolveEnvironmentProvisioning(env, tmpl, project)
		want.TemplateVerified, want.ProjectVerified = true, true
		if !platformv1alpha1.ProvisioningSnapshotsEqual(got.Status.Provisioning, want) {
			t.Fatalf("snapshot = %#v, want %#v", got.Status.Provisioning, want)
		}
		for _, child := range []struct {
			name   string
			object client.Object
		}{{envPodName(env), &corev1.Pod{}}, {envPVCName(env), &corev1.PersistentVolumeClaim{}}, {envCredentialName(env), &corev1.Secret{}}, {envNetworkPolicyName(env), &networkingv1.NetworkPolicy{}}} {
			if err := c.Get(context.Background(), client.ObjectKey{Namespace: env.Namespace, Name: child.name}, child.object); !apierrors.IsNotFound(err) {
				t.Fatalf("%T existed before snapshot publication: %v", child.object, err)
			}
		}
	})

	t.Run("warm snapshot promotion only extends project", func(t *testing.T) {
		scheme, env, tmpl := fixture(t)
		c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, tmpl).Build()
		r := &EnvironmentReconciler{Client: c, Scheme: scheme}
		for i := 0; i < 2; i++ {
			if _, err := r.Reconcile(context.Background(), provisioningRequest(env)); err != nil {
				t.Fatal(err)
			}
		}
		var promoted platformv1alpha1.Environment
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &promoted); err != nil {
			t.Fatal(err)
		}
		original := promoted.Status.Provisioning.DeepCopy()
		promoted.Spec.ProjectRef = "project"
		if err := c.Update(context.Background(), &promoted); err != nil {
			t.Fatal(err)
		}
		project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: env.Namespace, UID: "project-uid", Generation: 1}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://example.test/repository"}}}
		if err := c.Create(context.Background(), project); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Reconcile(context.Background(), provisioningRequest(env)); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &promoted); err != nil {
			t.Fatal(err)
		}
		if promoted.Status.Provisioning.Project == nil || !equality.Semantic.DeepEqual(promoted.Status.Provisioning.Template, original.Template) {
			t.Fatalf("promotion changed template snapshot or omitted project: before=%#v after=%#v", original, promoted.Status.Provisioning)
		}
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: env.Namespace, Name: envPodName(env)}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
			t.Fatalf("project Pod existed before project snapshot extension: %v", err)
		}
	})

	t.Run("source generation race publishes nothing", func(t *testing.T) {
		scheme, env, tmpl := fixture(t)
		base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, tmpl).Build()
		reads := 0
		reader := interceptor.NewClient(base, interceptor.Funcs{Get: func(ctx context.Context, delegate client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
			if err := delegate.Get(ctx, key, object, options...); err != nil {
				return err
			}
			if template, ok := object.(*platformv1alpha1.EnvironmentTemplate); ok {
				reads++
				if reads > 1 {
					template.Generation++
				}
			}
			return nil
		}})
		r := &EnvironmentReconciler{Client: base, APIReader: reader, Scheme: scheme}
		if _, err := r.Reconcile(context.Background(), provisioningRequest(env)); err != nil {
			t.Fatal(err)
		}
		var got platformv1alpha1.Environment
		if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
			t.Fatal(err)
		}
		if got.Status.Provisioning != nil {
			t.Fatalf("snapshot published across source race: %#v", got.Status.Provisioning)
		}
		if err := base.Get(context.Background(), client.ObjectKey{Namespace: env.Namespace, Name: envPodName(env)}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
			t.Fatalf("child created across source race: %v", err)
		}
	})

	t.Run("malformed snapshot fails closed", func(t *testing.T) {
		scheme, env, tmpl := fixture(t)
		env.Status.Provisioning = &platformv1alpha1.EnvironmentProvisioningSnapshot{}
		c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, tmpl).Build()
		r := &EnvironmentReconciler{Client: c, Scheme: scheme}
		if _, err := r.Reconcile(context.Background(), provisioningRequest(env)); err != nil {
			t.Fatal(err)
		}
		var got platformv1alpha1.Environment
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
			t.Fatal(err)
		}
		if got.Status.Phase != platformv1alpha1.EnvironmentPhaseFailed {
			t.Fatalf("phase = %q", got.Status.Phase)
		}
		var pods corev1.PodList
		_ = c.List(context.Background(), &pods)
		if len(pods.Items) != 0 {
			t.Fatal("malformed snapshot created a Pod")
		}
	})

	t.Run("same-name Project UID replacement fails closed", func(t *testing.T) {
		scheme, env, tmpl := fixture(t)
		env.Spec.ProjectRef = "project"
		old := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "old-project", Generation: 1}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://example.test/old"}}}
		setTestProvisioningSnapshot(env, tmpl, old)
		replacement := old.DeepCopy()
		replacement.UID = "new-project"
		c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, tmpl, replacement).Build()
		r := &EnvironmentReconciler{Client: c, Scheme: scheme}
		if _, err := r.Reconcile(context.Background(), provisioningRequest(env)); err != nil {
			t.Fatal(err)
		}
		var got platformv1alpha1.Environment
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
			t.Fatal(err)
		}
		if got.Status.Phase != platformv1alpha1.EnvironmentPhaseFailed || got.Status.Provisioning.Project.UID != "old-project" {
			t.Fatalf("replacement was not fenced: %#v", got.Status)
		}
	})
}

func TestProvisioningSnapshotFreezesReplacementPodAndPVC(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, nodev1.AddToScheme, platformv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	disk := resource.MustParse("10Gi")
	tmpl := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default", UID: "template-uid", Generation: 1}, Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "image:frozen", Size: "tiny", RuntimeClass: "gvisor", DiskSize: &disk}}
	project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid", Generation: 1}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://example.test/frozen"}}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "default", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: tmpl.Name, ProjectRef: project.Name}}
	setTestProvisioningSnapshot(env, tmpl, project)
	gvisor := &nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: "gvisor", UID: "gvisor-uid"}, Handler: "runsc"}
	native := &nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: "native", UID: "native-uid"}, Handler: "native"}
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, tmpl, project, gvisor, native).Build()
	r := &EnvironmentReconciler{Client: base, Scheme: scheme}
	if _, err := r.ensureWorkspacePVC(context.Background(), env, tmpl); err != nil {
		t.Fatal(err)
	}
	liveDisk := resource.MustParse("80Gi")
	tmpl.Spec.Image, tmpl.Spec.Size, tmpl.Spec.RuntimeClass, tmpl.Spec.DiskSize = "image:edited", "large", "native", &liveDisk
	project.Spec.Repositories[0] = "https://example.test/edited"
	if err := base.Update(context.Background(), project); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ensureWorkspacePVC(context.Background(), env, tmpl); err != nil {
		t.Fatal(err)
	}
	pod, err := r.ensurePod(context.Background(), env, tmpl)
	if err != nil {
		t.Fatal(err)
	}
	if pod.Spec.RuntimeClassName == nil || *pod.Spec.RuntimeClassName != "gvisor" || pod.Spec.Containers[0].Image != "image:frozen" || pod.Spec.InitContainers[0].Image != "image:frozen" {
		t.Fatalf("replacement Pod did not use frozen image/runtime: %#v", pod.Spec)
	}
	if pod.Annotations[runtimeClassUIDAnnotation] != string(gvisor.UID) {
		t.Fatalf("replacement Pod RuntimeClass UID = %q, want current frozen RuntimeClass UID %q", pod.Annotations[runtimeClassUIDAnnotation], gvisor.UID)
	}
	wantResources := corev1.ResourceList{}
	for name, quantity := range env.Status.Provisioning.Resources {
		wantResources[corev1.ResourceName(name)] = quantity
	}
	if !equality.Semantic.DeepEqual(pod.Spec.Containers[0].Resources.Requests, wantResources) || !equality.Semantic.DeepEqual(pod.Spec.Containers[0].Resources.Limits, wantResources) {
		t.Fatalf("replacement resources = %#v, want %#v", pod.Spec.Containers[0].Resources, env.Status.Provisioning.Resources)
	}
	repository := ""
	for _, variable := range pod.Spec.InitContainers[0].Env {
		if variable.Name == "SWE_REPOSITORY" {
			repository = variable.Value
		}
	}
	if repository != "https://example.test/frozen" {
		t.Fatalf("SWE_REPOSITORY = %q", repository)
	}
	var pvc corev1.PersistentVolumeClaim
	if err := base.Get(context.Background(), client.ObjectKey{Namespace: env.Namespace, Name: envPVCName(env)}, &pvc); err != nil {
		t.Fatal(err)
	}
	if got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; got.Cmp(disk) != 0 {
		t.Fatalf("PVC storage = %s, want %s", got.String(), disk.String())
	}
}

func TestLegacyProvisioningMigrationOrdersFenceSnapshotAndReplacement(t *testing.T) {
	for _, held := range []bool{false, true} {
		name := "active"
		if held {
			name = "held"
		}
		t.Run(name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, networkingv1.AddToScheme, platformv1alpha1.AddToScheme} {
				if err := add(scheme); err != nil {
					t.Fatal(err)
				}
			}
			disk := resource.MustParse("10Gi")
			tmpl := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default", UID: "template-uid", Generation: 2}, Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "image:frozen", Size: "tiny", DiskSize: &disk}}
			project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid", Generation: 3}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://example.test/frozen"}}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID("env-" + name), Generation: 1, Finalizers: []string{environmentFinalizer}}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: tmpl.Name, ProjectRef: project.Name}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady, ExecutionGeneration: 1, PodName: "legacy-pod", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"}, Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue}}}}
			if held {
				env.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Enabled: true, Revision: 1}
				env.Status.Lifecycle.Suspended = true
			}
			controller := true
			owner := metav1.OwnerReference{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: &controller}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envPodName(env), Namespace: env.Namespace, UID: "old-pod", OwnerReferences: []metav1.OwnerReference{owner}}}
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: envCredentialName(env), Namespace: env.Namespace, UID: "old-secret", OwnerReferences: []metav1.OwnerReference{owner}}}
			pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: envPVCName(env), Namespace: env.Namespace, UID: "retained-pvc", OwnerReferences: []metav1.OwnerReference{owner}}, Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: disk}}}}
			policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: envNetworkPolicyName(env), Namespace: env.Namespace, UID: "retained-policy", OwnerReferences: []metav1.OwnerReference{owner}}}
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, tmpl, project, pod, secret, pvc, policy).Build()
			r := &EnvironmentReconciler{Client: c, APIReader: c, Scheme: scheme}

			sawUnverified := false
			sourcesEdited := false
			for step := 0; step < 14; step++ {
				if _, err := r.Reconcile(context.Background(), provisioningRequest(env)); err != nil {
					t.Fatalf("step %d: %v", step, err)
				}
				var current platformv1alpha1.Environment
				if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
					t.Fatal(err)
				}
				var observedPod corev1.Pod
				podErr := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &observedPod)
				podGone := apierrors.IsNotFound(podErr) || podErr == nil && observedPod.UID != pod.UID
				if podGone && current.Status.ProvisioningMigrationPVCUID != pvc.UID {
					t.Fatalf("step %d removed Pod before persisting workspace UID fence: status=%q want=%q", step, current.Status.ProvisioningMigrationPVCUID, pvc.UID)
				}
				var observedSecret corev1.Secret
				secretErr := c.Get(context.Background(), client.ObjectKeyFromObject(secret), &observedSecret)
				secretGone := apierrors.IsNotFound(secretErr) || secretErr == nil && observedSecret.UID != secret.UID
				if !podGone && secretGone {
					t.Fatalf("step %d revoked credential before Pod", step)
				}
				if current.Status.Provisioning != nil && (!podGone || !secretGone) {
					t.Fatalf("step %d published snapshot before execution fence: %#v", step, current.Status.Provisioning)
				}
				if current.Status.Provisioning != nil && (!current.Status.Provisioning.TemplateVerified || !current.Status.Provisioning.ProjectVerified) {
					sawUnverified = true
				}
				if !sourcesEdited && current.Status.Provisioning != nil && current.Status.Provisioning.TemplateVerified && current.Status.Provisioning.ProjectVerified {
					tmpl.Spec.Image = "image:edited-after-verification"
					tmpl.Generation++
					if err := c.Update(context.Background(), tmpl); err != nil {
						t.Fatal(err)
					}
					project.Spec.Repositories[0] = "https://example.test/edited-after-verification"
					project.Generation++
					if err := c.Update(context.Background(), project); err != nil {
						t.Fatal(err)
					}
					sourcesEdited = true
				}
				if !held && podErr == nil && observedPod.UID != pod.UID {
					break
				}
				if held && current.Status.Phase == platformv1alpha1.EnvironmentPhasePaused && current.Status.Provisioning != nil && current.Status.Provisioning.TemplateVerified && current.Status.Provisioning.ProjectVerified {
					break
				}
			}
			var got platformv1alpha1.Environment
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
				t.Fatal(err)
			}
			if !sawUnverified || got.Status.Provisioning == nil || !got.Status.Provisioning.TemplateVerified || !got.Status.Provisioning.ProjectVerified {
				t.Fatalf("snapshot was not published unverified then exactly verified: %#v", got.Status.Provisioning)
			}
			if got.Status.Provisioning.LegacyWorkspacePVCUID != pvc.UID {
				t.Fatalf("snapshot workspace UID = %q, want %q", got.Status.Provisioning.LegacyWorkspacePVCUID, pvc.UID)
			}
			var retained corev1.PersistentVolumeClaim
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(pvc), &retained); err != nil || retained.UID != pvc.UID {
				t.Fatalf("retained PVC changed: %#v, %v", retained.UID, err)
			}
			var replacement corev1.Pod
			err := c.Get(context.Background(), client.ObjectKey{Namespace: env.Namespace, Name: envPodName(env)}, &replacement)
			if held {
				if !apierrors.IsNotFound(err) || got.Status.Phase != platformv1alpha1.EnvironmentPhasePaused {
					t.Fatalf("held migration created Pod or did not pause: pod=%v status=%#v", err, got.Status)
				}
			} else if err != nil {
				t.Fatalf("active replacement Pod missing: %v (status %#v)", err, got.Status)
			} else if replacement.Spec.Containers[0].Image != "image:frozen" || replacement.Spec.Volumes[0].PersistentVolumeClaim.ClaimName != pvc.Name {
				t.Fatalf("replacement did not use frozen sources and retained PVC: %#v", replacement.Spec)
			}
		})
	}
}

func TestVerifiedLegacySnapshotNeverReplacesFrozenWorkspace(t *testing.T) {
	for _, test := range []struct {
		name        string
		replacement bool
		held        bool
		deleting    bool
	}{
		{name: "deleted after verification"},
		{name: "same-name UID replacement", replacement: true},
		{name: "held loss before release", held: true},
		{name: "deleting after verification", deleting: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, platformv1alpha1.AddToScheme} {
				if err := add(scheme); err != nil {
					t.Fatal(err)
				}
			}
			tmpl := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default", UID: "template-uid", Generation: 1}, Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "image", Size: "small"}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: "default", UID: "env-uid", Generation: 1}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: tmpl.Name}}
			env.Status.Provisioning = platformv1alpha1.ResolveEnvironmentProvisioning(env, tmpl, nil)
			env.Status.Provisioning.TemplateVerified = true
			env.Status.Provisioning.LegacyWorkspacePVCUID = "retained-pvc-uid"
			if test.held {
				env.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Enabled: true, Revision: 1}
				env.Status.Lifecycle.Suspended = true
				env.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
			}
			controller := true
			owner := metav1.OwnerReference{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: &controller}
			objects := []client.Object{env, tmpl}
			if test.replacement || test.deleting {
				uid := types.UID("replacement-pvc-uid")
				metadata := metav1.ObjectMeta{Name: envPVCName(env), Namespace: env.Namespace, UID: uid, OwnerReferences: []metav1.OwnerReference{owner}}
				if test.deleting {
					now := metav1.Now()
					metadata.UID = "retained-pvc-uid"
					metadata.DeletionTimestamp = &now
					metadata.Finalizers = []string{"kubernetes.io/pvc-protection"}
				}
				objects = append(objects, &corev1.PersistentVolumeClaim{ObjectMeta: metadata})
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(objects...).Build()
			r := &EnvironmentReconciler{Client: c, APIReader: c, Scheme: scheme}
			if ready, err := r.ensureWorkspacePVC(context.Background(), env, tmpl); ready || err == nil {
				t.Fatalf("frozen workspace check = ready %v, err %v", ready, err)
			}
			var pvc corev1.PersistentVolumeClaim
			err := c.Get(context.Background(), client.ObjectKey{Namespace: env.Namespace, Name: envPVCName(env)}, &pvc)
			if test.replacement || test.deleting {
				wantUID := types.UID("replacement-pvc-uid")
				if test.deleting {
					wantUID = "retained-pvc-uid"
				}
				if err != nil || pvc.UID != wantUID {
					t.Fatalf("replacement PVC was changed: uid=%q err=%v", pvc.UID, err)
				}
			} else if !apierrors.IsNotFound(err) {
				t.Fatalf("missing frozen PVC was recreated: %v", err)
			}
		})
	}
}

func TestPodCreationRevalidatesSourceIncarnationsAtBothBoundaries(t *testing.T) {
	for _, source := range []string{"Template", "Project"} {
		for _, boundary := range []string{"pre-create", "post-create"} {
			t.Run(source+"/"+boundary, func(t *testing.T) {
				scheme := runtime.NewScheme()
				for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, platformv1alpha1.AddToScheme} {
					if err := add(scheme); err != nil {
						t.Fatal(err)
					}
				}
				tmpl := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default", UID: "template-old", Generation: 1}, Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "image", Size: "small"}}
				project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-old", Generation: 1}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://example.test/repo"}}}
				env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: "default", UID: "env-uid", Generation: 1}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: tmpl.Name, ProjectRef: project.Name}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseResuming}}
				env.Status.Provisioning = platformv1alpha1.ResolveEnvironmentProvisioning(env, tmpl, project)
				env.Status.Provisioning.TemplateVerified = true
				env.Status.Provisioning.ProjectVerified = true
				base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, tmpl, project).Build()
				clientWithUID := interceptor.NewClient(base, interceptor.Funcs{Create: func(ctx context.Context, delegate client.WithWatch, object client.Object, options ...client.CreateOption) error {
					if err := delegate.Create(ctx, object, options...); err != nil {
						return err
					}
					if pod, ok := object.(*corev1.Pod); ok {
						pod.UID = "created-pod-uid"
						return delegate.Update(ctx, pod)
					}
					return nil
				}})
				reads := 0
				reader := interceptor.NewClient(base, interceptor.Funcs{Get: func(ctx context.Context, delegate client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
					if err := delegate.Get(ctx, key, object, options...); err != nil {
						return err
					}
					target := source == "Template"
					if _, ok := object.(*platformv1alpha1.Project); ok {
						target = source == "Project"
					} else if _, ok := object.(*platformv1alpha1.EnvironmentTemplate); !ok {
						target = false
					}
					if target {
						reads++
						wantRead := 1
						if boundary == "post-create" {
							wantRead = 2
						}
						if reads == wantRead {
							object.SetUID(types.UID(source + "-replacement"))
						}
					}
					return nil
				}})
				r := &EnvironmentReconciler{Client: clientWithUID, APIReader: reader, Scheme: scheme}
				if pod, err := r.ensurePodForProject(context.Background(), env, tmpl, project, "", tenancy.Claim{}); pod != nil || err == nil {
					t.Fatalf("creation across %s %s replacement = pod %#v, err %v", boundary, source, pod, err)
				}
				var pod corev1.Pod
				if err := base.Get(context.Background(), client.ObjectKey{Namespace: env.Namespace, Name: envPodName(env)}, &pod); !apierrors.IsNotFound(err) {
					t.Fatalf("Pod survived %s %s replacement: uid=%q err=%v", boundary, source, pod.UID, err)
				}
				if boundary == "post-create" {
					var secret corev1.Secret
					if err := base.Get(context.Background(), client.ObjectKey{Namespace: env.Namespace, Name: envCredentialName(env)}, &secret); err != nil {
						t.Fatal(err)
					}
					if secret.Annotations[sandboxdauth.PodUIDAnnotation] != "" {
						t.Fatalf("credential was bound before post-create authority validation: %#v", secret.Annotations)
					}
				}
			})
		}
	}
}

func TestLegacyProvisioningMigrationRefusesDeletingWorkspaceBeforeFence(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, networkingv1.AddToScheme, platformv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	now := metav1.Now()
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: "default", UID: "env-uid", Finalizers: []string{environmentFinalizer}},
		Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
		Status:     platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady, ExecutionGeneration: 1, PodName: "env-legacy"},
	}
	controller := true
	owner := metav1.OwnerReference{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: &controller}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envPodName(env), Namespace: env.Namespace, UID: "pod-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: envCredentialName(env), Namespace: env.Namespace, UID: "secret-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: envPVCName(env), Namespace: env.Namespace, UID: "pvc-uid", OwnerReferences: []metav1.OwnerReference{owner}, Finalizers: []string{"kubernetes.io/pvc-protection"}, DeletionTimestamp: &now}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, pod, secret, pvc).Build()
	r := &EnvironmentReconciler{Client: c, APIReader: c, Scheme: scheme}

	retained, _, handled, err := r.reconcileLegacyProvisioningMigration(context.Background(), env.DeepCopy())
	if err != nil || retained || !handled {
		t.Fatalf("migration result = retained %v, handled %v, err %v", retained, handled, err)
	}
	for _, child := range []client.Object{pod, secret, pvc} {
		observed := child.DeepCopyObject().(client.Object)
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(child), observed); err != nil || observed.GetUID() != child.GetUID() {
			t.Fatalf("child %T was changed before deleting-workspace refusal: uid=%q err=%v", child, observed.GetUID(), err)
		}
	}
	var got platformv1alpha1.Environment
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Provisioning != nil || got.Status.Phase != platformv1alpha1.EnvironmentPhaseFailed {
		t.Fatalf("deleting workspace did not fail closed: %#v", got.Status)
	}
}

func TestLegacyProvisioningMigrationWaitsForPendingRecovery(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	next := metav1.NewTime(now.Add(5 * time.Minute))
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: "default", UID: "env-uid", Generation: 2},
		Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
		Status: platformv1alpha1.EnvironmentStatus{
			ExecutionGeneration: 1,
			Recovery: platformv1alpha1.EnvironmentRecoveryStatus{
				Attempts: 1, ExecutionGeneration: 1, NextAttemptAt: &next,
			},
		},
	}
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: envPodName(env), Namespace: env.Namespace, UID: "legacy-pod-uid",
		OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: &controller}},
	}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, pod).Build()
	r := &EnvironmentReconciler{Client: c, APIReader: c, Scheme: scheme, Now: func() time.Time { return now }}
	state := &environmentReconcileState{env: *env.DeepCopy()}

	outcome := reconcileEnvironmentLegacyProvisioningMigrationGate(r, context.Background(), state)
	if outcome.err != nil || !outcome.handled || outcome.result.RequeueAfter != 5*time.Minute {
		t.Fatalf("migration gate = %#v, want pending recovery delay", outcome)
	}
	var got platformv1alpha1.Environment
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Provisioning != nil || got.Status.Recovery.Attempts != 1 || got.Status.Recovery.NextAttemptAt == nil || !got.Status.Recovery.NextAttemptAt.Equal(&next) {
		t.Fatalf("pending recovery was changed by provisioning migration gate: %#v", got.Status)
	}
	var retained corev1.Pod
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &retained); err != nil || retained.UID != pod.UID {
		t.Fatalf("legacy Pod was fenced before pending recovery elapsed: uid=%q err=%v", retained.UID, err)
	}

	modern := env.DeepCopy()
	modern.Status.Provisioning = &platformv1alpha1.EnvironmentProvisioningSnapshot{
		Template:         platformv1alpha1.EnvironmentProvisioningTemplate{Name: "small", UID: "template-uid", Generation: 1},
		Backend:          platformv1alpha1.EnvironmentBackendPod,
		Image:            "image",
		Size:             "small",
		Resources:        map[string]resource.Quantity{},
		DiskSize:         resource.MustParse("1Gi"),
		TemplateVerified: true,
	}
	modernState := &environmentReconcileState{env: *modern}
	modernOutcome := reconcileEnvironmentLegacyProvisioningMigrationGate(r, context.Background(), modernState)
	if modernOutcome.handled || modernOutcome.err != nil {
		t.Fatalf("verified provisioning was preempted by early recovery: %#v", modernOutcome)
	}
}

func TestSuspendedLegacyProvisioningMigrationFencesExecutionBeforeMissingPVCFailure(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, platformv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: "default", UID: "env-uid", Generation: 2},
		Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
		Status: platformv1alpha1.EnvironmentStatus{
			ExecutionGeneration: 1, Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-legacy",
			Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
			Lifecycle: platformv1alpha1.EnvironmentLifecycleStatus{Suspended: true},
			Recovery:  platformv1alpha1.EnvironmentRecoveryStatus{Attempts: 3, Exhausted: true, ExecutionGeneration: 1},
			Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue,
				ObservedGeneration: 2, Reason: "SandboxdReady"}},
		},
	}
	controller := true
	owner := metav1.OwnerReference{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: &controller}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envPodName(env), Namespace: env.Namespace, UID: "pod-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: envCredentialName(env), Namespace: env.Namespace, UID: "secret-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
	deleted := make([]string, 0, 2)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, pod, secret).
		WithInterceptorFuncs(interceptor.Funcs{Delete: func(ctx context.Context, client client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			deleted = append(deleted, obj.GetName())
			return client.Delete(ctx, obj, opts...)
		}}).Build()
	r := &EnvironmentReconciler{Client: c, APIReader: c, Scheme: scheme}

	for range 8 {
		var current platformv1alpha1.Environment
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
			t.Fatal(err)
		}
		state := &environmentReconcileState{env: current}
		outcome := reconcileEnvironmentLegacyProvisioningMigrationGate(r, context.Background(), state)
		if outcome.err != nil {
			t.Fatal(outcome.err)
		}
	}
	if len(deleted) != 2 || deleted[0] != pod.Name || deleted[1] != secret.Name {
		t.Fatalf("suspended migration deletion order = %v, want Pod then credential", deleted)
	}
	var got platformv1alpha1.Environment
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != platformv1alpha1.EnvironmentPhaseFailed || got.Status.Provisioning != nil || got.Status.ProvisioningMigrationPVCUID != "" {
		t.Fatalf("missing-PVC suspended migration status = %#v", got.Status)
	}
	before := got.DeepCopy()
	state := &environmentReconcileState{env: got}
	if outcome := reconcileEnvironmentLegacyProvisioningMigrationGate(r, context.Background(), state); outcome.err != nil || !outcome.handled {
		t.Fatalf("stable failed migration = %#v", outcome)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != before.Status.Phase || len(deleted) != 2 {
		t.Fatalf("failed suspended migration looped status or teardown: before=%#v after=%#v deletes=%v", before.Status, got.Status, deleted)
	}
}

func TestLegacyProvisioningMigrationRejectsPVCReplacementAfterUIDFence(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, networkingv1.AddToScheme, platformv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: "default", UID: "env-uid", Generation: 1, Finalizers: []string{environmentFinalizer}}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: "small"}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady, ExecutionGeneration: 1, PodName: "env-legacy"}}
	tmpl := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: env.Namespace, UID: "template-uid", Generation: 1}, Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "image", Size: "small"}}
	controller := true
	owner := metav1.OwnerReference{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: &controller}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envPodName(env), Namespace: env.Namespace, UID: "pod-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: envPVCName(env), Namespace: env.Namespace, UID: "original-pvc", OwnerReferences: []metav1.OwnerReference{owner}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, tmpl, pod, pvc).Build()
	r := &EnvironmentReconciler{Client: c, APIReader: c, Scheme: scheme}
	if _, err := r.Reconcile(context.Background(), provisioningRequest(env)); err != nil {
		t.Fatal(err)
	}
	var fenced platformv1alpha1.Environment
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &fenced); err != nil {
		t.Fatal(err)
	}
	if fenced.Status.ProvisioningMigrationPVCUID != pvc.UID {
		t.Fatalf("migration PVC UID fence = %q, want %q", fenced.Status.ProvisioningMigrationPVCUID, pvc.UID)
	}
	if err := c.Delete(context.Background(), pvc); err != nil {
		t.Fatal(err)
	}
	replacement := pvc.DeepCopy()
	replacement.ResourceVersion = ""
	replacement.UID = "replacement-pvc"
	if err := c.Create(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := r.Reconcile(context.Background(), provisioningRequest(env)); err != nil {
			t.Fatal(err)
		}
	}
	var got platformv1alpha1.Environment
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != platformv1alpha1.EnvironmentPhaseFailed || got.Status.Provisioning != nil || got.Status.ProvisioningMigrationPVCUID != "original-pvc" {
		t.Fatalf("replacement PVC gained migration authority: %#v", got.Status)
	}
}

func TestLegacyProvisioningMigrationRejectsForeignFixedNameChildren(t *testing.T) {
	for _, kind := range []string{"Pod", "PersistentVolumeClaim", "Secret", "NetworkPolicy"} {
		t.Run(kind, func(t *testing.T) {
			scheme := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, networkingv1.AddToScheme, platformv1alpha1.AddToScheme} {
				if err := add(scheme); err != nil {
					t.Fatal(err)
				}
			}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: "default", UID: "env-uid", Generation: 1, Finalizers: []string{environmentFinalizer}}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: "small"}, Status: platformv1alpha1.EnvironmentStatus{ExecutionGeneration: 1}}
			tmpl := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: env.Namespace, UID: "template-uid", Generation: 1}, Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "image", Size: "tiny"}}
			foreignOwner := true
			owner := metav1.OwnerReference{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: "other", UID: "other-uid", Controller: &foreignOwner}
			var child client.Object
			switch kind {
			case "Pod":
				child = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envPodName(env), Namespace: env.Namespace, UID: "foreign", OwnerReferences: []metav1.OwnerReference{owner}}}
			case "PersistentVolumeClaim":
				child = &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: envPVCName(env), Namespace: env.Namespace, UID: "foreign", OwnerReferences: []metav1.OwnerReference{owner}}}
			case "Secret":
				child = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: envCredentialName(env), Namespace: env.Namespace, UID: "foreign", OwnerReferences: []metav1.OwnerReference{owner}}}
			case "NetworkPolicy":
				child = &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: envNetworkPolicyName(env), Namespace: env.Namespace, UID: "foreign", OwnerReferences: []metav1.OwnerReference{owner}}}
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, tmpl, child).Build()
			r := &EnvironmentReconciler{Client: c, APIReader: c, Scheme: scheme}
			for range 3 {
				if _, err := r.Reconcile(context.Background(), provisioningRequest(env)); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(child), child); err != nil || child.GetUID() != "foreign" {
				t.Fatalf("foreign child was deleted or adopted: uid=%q err=%v", child.GetUID(), err)
			}
			var got platformv1alpha1.Environment
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
				t.Fatal(err)
			}
			if got.Status.Provisioning != nil || got.Status.Phase != platformv1alpha1.EnvironmentPhaseFailed {
				t.Fatalf("foreign child was used for migration: %#v", got.Status)
			}
			var pods corev1.PodList
			if err := c.List(context.Background(), &pods, client.InNamespace(env.Namespace)); err != nil {
				t.Fatal(err)
			}
			for i := range pods.Items {
				if exactControllerOwner(&pods.Items[i], platformv1alpha1.GroupVersion.String(), "Environment", env.Name, env.UID) {
					t.Fatalf("owned Pod created despite foreign %s", kind)
				}
			}
		})
	}
}

func TestLegacyProvisioningMigrationRejectsSameNameSourceReplacementDuringVerification(t *testing.T) {
	for _, source := range []string{"Template", "Project"} {
		t.Run(source, func(t *testing.T) {
			scheme := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, networkingv1.AddToScheme, platformv1alpha1.AddToScheme} {
				if err := add(scheme); err != nil {
					t.Fatal(err)
				}
			}
			tmpl := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default", UID: "template-old", Generation: 1}, Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "image:old", Size: "tiny"}}
			project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-old", Generation: 1}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://example.test/old"}}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: "default", UID: "env-uid", Generation: 1, Finalizers: []string{environmentFinalizer}}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: tmpl.Name, ProjectRef: project.Name}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseResuming, ExecutionGeneration: 1}}
			controller := true
			owner := metav1.OwnerReference{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: &controller}
			pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: envPVCName(env), Namespace: env.Namespace, UID: "pvc-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
			policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: envNetworkPolicyName(env), Namespace: env.Namespace, UID: "policy-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, tmpl, project, pvc, policy).Build()
			r := &EnvironmentReconciler{Client: c, APIReader: c, Scheme: scheme}
			for range 2 {
				if _, err := r.Reconcile(context.Background(), provisioningRequest(env)); err != nil {
					t.Fatal(err)
				}
			}
			var captured platformv1alpha1.Environment
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &captured); err != nil {
				t.Fatal(err)
			}
			if captured.Status.Provisioning == nil || captured.Status.Provisioning.TemplateVerified || captured.Status.Provisioning.ProjectVerified {
				t.Fatalf("migration did not stop at unverified capture: %#v", captured.Status.Provisioning)
			}

			if source == "Template" {
				if err := c.Delete(context.Background(), tmpl); err != nil {
					t.Fatal(err)
				}
				replacement := tmpl.DeepCopy()
				replacement.ResourceVersion = ""
				replacement.UID = "template-new"
				if err := c.Create(context.Background(), replacement); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := c.Delete(context.Background(), project); err != nil {
					t.Fatal(err)
				}
				replacement := project.DeepCopy()
				replacement.ResourceVersion = ""
				replacement.UID = "project-new"
				if err := c.Create(context.Background(), replacement); err != nil {
					t.Fatal(err)
				}
			}
			for attempt := 0; attempt < 6; attempt++ {
				if _, err := r.Reconcile(context.Background(), provisioningRequest(env)); err != nil {
					t.Fatal(err)
				}
				var stable platformv1alpha1.Environment
				if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &stable); err != nil {
					t.Fatal(err)
				}
				if stable.Status.Phase != platformv1alpha1.EnvironmentPhaseFailed {
					t.Fatalf("attempt %d changed failed migration status after same-name %s replacement: %#v", attempt, source, stable.Status)
				}
			}
			var got platformv1alpha1.Environment
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
				t.Fatal(err)
			}
			if got.Status.Phase != platformv1alpha1.EnvironmentPhaseFailed || got.Status.Provisioning.TemplateVerified || got.Status.Provisioning.ProjectVerified {
				t.Fatalf("same-name %s replacement gained migration authority: %#v", source, got.Status)
			}
			if err := c.Get(context.Background(), client.ObjectKey{Namespace: env.Namespace, Name: envPodName(env)}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
				t.Fatalf("same-name %s replacement created a Pod: %v", source, err)
			}
		})
	}
}
