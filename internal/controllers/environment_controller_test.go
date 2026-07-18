package controllers

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
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

func TestSyncStatusReportsSetupForProjectInitialization(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
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
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Status: platformv1alpha1.EnvironmentStatus{
			Phase:   platformv1alpha1.EnvironmentPhaseReady,
			PodName: "env-test",
		},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "env-test", Namespace: "default"}}
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
