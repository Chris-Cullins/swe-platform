package controllers

import (
	"context"
	stderrors "errors"
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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/agent"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	sandboxdauth "github.com/Chris-Cullins/swe-platform/sandboxd/auth"
)

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
		Status:     platformv1alpha1.EnvironmentStatus{ExecutionGeneration: 1},
	}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build(),
		Scheme: scheme,
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "env-test", Namespace: "default", Annotations: map[string]string{executionGenerationAnnotation: "1"}},
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

	nextRecovery := metav1.Now()
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", Generation: 4}, Status: platformv1alpha1.EnvironmentStatus{
		ExecutionGeneration: 1,
		Recovery:            platformv1alpha1.EnvironmentRecoveryStatus{Attempts: 2, Exhausted: true, ExecutionGeneration: 1, NextAttemptAt: &nextRecovery},
		PodRecoveryAttempts: 3, PodRecoveryExhausted: true, PodRecoveryUID: "legacy-pod", PodRecoveryNextAttemptAt: &nextRecovery,
	}}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build(),
		Scheme: scheme,
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "env-test", Namespace: "default", Annotations: map[string]string{executionGenerationAnnotation: "1"}},
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
	if updated.Status.Recovery.Attempts != 0 || updated.Status.Recovery.Exhausted || updated.Status.Recovery.ExecutionGeneration != 0 || updated.Status.Recovery.NextAttemptAt != nil {
		t.Fatalf("recovery budget was not reset after health-aware readiness: %#v", updated.Status)
	}
	if legacyRecoveryMeaningful(&updated.Status) {
		t.Fatalf("legacy recovery budget was not reset after health-aware readiness: %#v", updated.Status)
	}
}

func TestSyncStatusRejectsReadyObservationAfterExecutionChanges(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	nextRecovery := metav1.Now()
	stored := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "environment-uid"},
		Status: platformv1alpha1.EnvironmentStatus{
			ExecutionGeneration: 3,
			Recovery: platformv1alpha1.EnvironmentRecoveryStatus{
				Attempts: 2, ExecutionGeneration: 2, NextAttemptAt: &nextRecovery,
			},
		},
	}
	stale := stored.DeepCopy()
	stale.Status.ExecutionGeneration = 2
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "env-test", Namespace: "default", Annotations: map[string]string{executionGenerationAnnotation: "2"}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning, PodIP: "10.0.0.2",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(stored).WithObjects(stored).Build(),
		Scheme: scheme,
	}

	if err := reconciler.syncStatus(context.Background(), stale, pod); !stderrors.Is(err, errEnvironmentExecutionChanged) {
		t.Fatalf("stale ready observation error = %v, want execution-change rejection", err)
	}
	var retained platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(stored), &retained); err != nil {
		t.Fatal(err)
	}
	if retained.Status.ExecutionGeneration != 3 || retained.Status.Endpoints.Sandboxd != "" || retained.Status.Recovery.Attempts != 2 || retained.Status.Recovery.NextAttemptAt == nil || len(retained.Status.Conditions) != 0 {
		t.Fatalf("stale ready observation changed newer execution status: %#v", retained.Status)
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
		ExecutionGeneration: 1,
		Conditions:          []metav1.Condition{{Type: "Audit", Status: metav1.ConditionTrue, Reason: "Recorded", Message: "preserve me"}},
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
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "environment-uid"},
		Spec: platformv1alpha1.EnvironmentSpec{
			ProjectRef:  project.Name,
			TemplateRef: "small",
		},
		Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseResuming},
	}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(project, env).Build(),
		Scheme: scheme,
	}
	tmpl := &platformv1alpha1.EnvironmentTemplate{
		Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "example/environment:latest", Size: "small"},
	}

	pod, err := reconciler.ensurePod(context.Background(), env, tmpl)
	if err != nil {
		t.Fatalf("ensurePod() error = %v", err)
	}
	setup := pod.Spec.InitContainers[1]
	if len(setup.Env) != 3 || setup.Env[2].Name != "SWE_RESUMING" || setup.Env[2].Value != "true" {
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
		Status:     platformv1alpha1.EnvironmentStatus{ExecutionGeneration: 1, Phase: platformv1alpha1.EnvironmentPhaseResuming},
	}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build(),
		Scheme: scheme,
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "env-test", Namespace: "default", Annotations: map[string]string{executionGenerationAnnotation: "1"}},
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
		t.Fatal("reconcilePaused() did not requeue after withdrawing readiness")
	}
	var updated platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &updated); err != nil {
		t.Fatal(err)
	}
	ready := apimeta.FindStatusCondition(updated.Status.Conditions, platformv1alpha1.EnvironmentConditionReady)
	if updated.Status.Phase != platformv1alpha1.EnvironmentPhaseIdle || updated.Status.PodName != "" || ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "PauseRequested" {
		t.Fatalf("status before pod deletion = %#v, want readiness withdrawn", updated.Status)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
		t.Fatal("Pod was deleted before readiness withdrawal")
	}

	if _, err := reconciler.reconcilePaused(context.Background(), &updated); err != nil {
		t.Fatalf("second reconcilePaused() error = %v", err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("owned Pod still exists: %v", err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &updated); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcilePaused(context.Background(), &updated); err != nil {
		t.Fatalf("third reconcilePaused() error = %v", err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != platformv1alpha1.EnvironmentPhasePaused || updated.Status.PodName != "" {
		t.Fatalf("Status = %#v, want Paused with no pod name", updated.Status)
	}
	ready = apimeta.FindStatusCondition(updated.Status.Conditions, platformv1alpha1.EnvironmentConditionReady)
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
	if !updated.Status.Lifecycle.Suspended || updated.Status.Lifecycle.SuspensionReason != platformv1alpha1.EnvironmentSuspensionReasonIdle || updated.Status.Lifecycle.Epoch != 1 || updated.Status.Phase != platformv1alpha1.EnvironmentPhaseIdle {
		t.Fatalf("Environment = %#v, want paused with Idle phase", updated)
	}
}

func TestLegacyActivityWithoutExecutionGenerationFailsClosed(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	oldActivity := metav1.NewTime(time.Date(2026, time.July, 20, 11, 0, 0, 0, time.UTC))
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: "default", UID: "env-uid"},
		Spec: platformv1alpha1.EnvironmentSpec{Lifecycle: platformv1alpha1.EnvironmentLifecycleSpec{Activity: []platformv1alpha1.EnvironmentActivityRequest{{
			Source: platformv1alpha1.EnvironmentActivitySourceTerminal,
			EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{
				ID: "legacy-activity", EnvironmentUID: "env-uid", HoldPolicyRevision: 0,
			},
		}}}},
		Status: platformv1alpha1.EnvironmentStatus{
			ExecutionGeneration: 4, LastActiveAt: &oldActivity,
			Lifecycle: platformv1alpha1.EnvironmentLifecycleStatus{ActivityReceipts: []platformv1alpha1.EnvironmentActivityReceipt{{
				Source: platformv1alpha1.EnvironmentActivitySourceTerminal, RequestID: "legacy-activity",
			}}},
		},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build()
	reconciler := &EnvironmentReconciler{Client: kubeClient, Now: func() time.Time { return oldActivity.Add(time.Hour) }}

	changed, err := reconciler.reconcileLifecycleIntent(context.Background(), env)
	if err != nil || changed {
		t.Fatalf("legacy lifecycle reconcile = (%t, %v), want unchanged", changed, err)
	}
	var retained platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal(err)
	}
	if retained.Status.LastActiveAt == nil || !retained.Status.LastActiveAt.Equal(&oldActivity) ||
		len(retained.Status.Lifecycle.ActivityReceipts) != 1 || retained.Status.Lifecycle.ActivityReceipts[0].ExecutionGeneration != 0 {
		t.Fatalf("legacy activity was accepted or rewritten: %#v", retained.Status)
	}
}

func TestLifecycleHoldConsumesButRefusesOrdinaryWake(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "held", Namespace: "default", UID: "env-uid"},
		Status:     platformv1alpha1.EnvironmentStatus{ExecutionGeneration: 1},
		Spec: platformv1alpha1.EnvironmentSpec{Lifecycle: platformv1alpha1.EnvironmentLifecycleSpec{
			Hold: &platformv1alpha1.EnvironmentHoldPolicy{Enabled: true, Revision: 4},
			Wake: &platformv1alpha1.EnvironmentWakeRequest{EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "wake-1", EnvironmentUID: "env-uid", HoldPolicyRevision: 4}},
			Activity: []platformv1alpha1.EnvironmentActivityRequest{{Source: platformv1alpha1.EnvironmentActivitySourceTerminal, EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{
				ID: "activity-1", EnvironmentUID: "env-uid", HoldPolicyRevision: 4,
			}, ExecutionGeneration: 1}},
		}},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build()
	reconciler := &EnvironmentReconciler{Client: kubeClient, Now: func() time.Time { return now }}

	changed, err := reconciler.reconcileLifecycleIntent(context.Background(), env)
	if err != nil || !changed {
		t.Fatalf("first lifecycle reconcile = (%t, %v)", changed, err)
	}
	var held platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(env), &held); err != nil {
		t.Fatal(err)
	}
	if !held.Status.Lifecycle.Suspended || held.Status.Lifecycle.SuspensionReason != platformv1alpha1.EnvironmentSuspensionReasonHold || held.Status.Lifecycle.Epoch != 1 || held.Status.Lifecycle.LastWakeRequestID != "wake-1" || held.Status.LastActiveAt == nil || len(held.Status.Lifecycle.ActivityReceipts) != 1 {
		t.Fatalf("held lifecycle status = %#v", held.Status)
	}
	changed, err = reconciler.reconcileLifecycleIntent(context.Background(), &held)
	if err != nil || changed {
		t.Fatalf("idempotent lifecycle reconcile = (%t, %v)", changed, err)
	}
}

func TestLifecycleWakeIsUIDAndPolicyRevisionFenced(t *testing.T) {
	for _, test := range []struct {
		name         string
		wakeUID      types.UID
		wakeRevision int64
	}{
		{name: "replacement UID", wakeUID: "old-uid", wakeRevision: 2},
		{name: "changed hold policy", wakeUID: "env-uid", wakeRevision: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := platformv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			env := &platformv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{Name: "idle", Namespace: "default", UID: "env-uid"},
				Spec: platformv1alpha1.EnvironmentSpec{Lifecycle: platformv1alpha1.EnvironmentLifecycleSpec{
					Hold: &platformv1alpha1.EnvironmentHoldPolicy{Revision: 2},
					Wake: &platformv1alpha1.EnvironmentWakeRequest{EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "stale", EnvironmentUID: test.wakeUID, HoldPolicyRevision: test.wakeRevision}},
				}},
				Status: platformv1alpha1.EnvironmentStatus{Lifecycle: platformv1alpha1.EnvironmentLifecycleStatus{Suspended: true, SuspensionReason: platformv1alpha1.EnvironmentSuspensionReasonIdle, Epoch: 7}},
			}
			kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build()
			reconciler := &EnvironmentReconciler{Client: kubeClient}
			if _, err := reconciler.reconcileLifecycleIntent(context.Background(), env); err != nil {
				t.Fatal(err)
			}
			var retained platformv1alpha1.Environment
			if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
				t.Fatal(err)
			}
			if !retained.Status.Lifecycle.Suspended || retained.Status.Lifecycle.Epoch != 7 || retained.Status.Lifecycle.LastWakeRequestID != "" {
				t.Fatalf("stale wake changed lifecycle = %#v", retained.Status.Lifecycle)
			}
		})
	}
}

func TestLifecycleIntentDoesNotCertifyStaleBackendReadiness(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "changed", Namespace: "default", UID: "env-uid", Generation: 2},
		Spec: platformv1alpha1.EnvironmentSpec{Lifecycle: platformv1alpha1.EnvironmentLifecycleSpec{Activity: []platformv1alpha1.EnvironmentActivityRequest{{
			Source: platformv1alpha1.EnvironmentActivitySourcePortal, EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "portal-1", EnvironmentUID: "env-uid"},
		}}}},
		Status: platformv1alpha1.EnvironmentStatus{ObservedGeneration: 1, Conditions: []metav1.Condition{{
			Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1, Reason: "SandboxdReady",
		}}},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build()
	reconciler := &EnvironmentReconciler{Client: kubeClient}
	if _, err := reconciler.reconcileLifecycleIntent(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	var retained platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal(err)
	}
	ready := apimeta.FindStatusCondition(retained.Status.Conditions, platformv1alpha1.EnvironmentConditionReady)
	if retained.Status.ObservedGeneration != 1 || ready == nil || ready.ObservedGeneration != 1 || platformv1alpha1.IsEnvironmentReady(&retained) {
		t.Fatalf("lifecycle intent certified stale backend: %#v", retained.Status)
	}
}

func TestLifecycleIdleWakeIsIdempotentAndPauseIncrementsEpoch(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "idle", Namespace: "default", UID: "env-uid"},
		Spec: platformv1alpha1.EnvironmentSpec{Lifecycle: platformv1alpha1.EnvironmentLifecycleSpec{
			Wake: &platformv1alpha1.EnvironmentWakeRequest{EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "wake-1", EnvironmentUID: "env-uid"}},
		}},
		Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhasePaused, Lifecycle: platformv1alpha1.EnvironmentLifecycleStatus{Suspended: true, SuspensionReason: platformv1alpha1.EnvironmentSuspensionReasonIdle, Epoch: 1}},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build()
	reconciler := &EnvironmentReconciler{Client: kubeClient}
	if _, err := reconciler.reconcileLifecycleIntent(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	var active platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(env), &active); err != nil {
		t.Fatal(err)
	}
	if active.Status.Lifecycle.Suspended || active.Status.Lifecycle.Epoch != 1 || active.Status.Lifecycle.LastWakeRequestID != "wake-1" {
		t.Fatalf("wake status = %#v", active.Status.Lifecycle)
	}
	stale := metav1.NewTime(time.Now().Add(-time.Hour))
	active.Status.LastActiveAt = &stale
	if err := kubeClient.Status().Update(context.Background(), &active); err != nil {
		t.Fatal(err)
	}
	tmpl := &platformv1alpha1.EnvironmentTemplate{Spec: platformv1alpha1.EnvironmentTemplateSpec{IdleTimeout: &metav1.Duration{Duration: time.Minute}}}
	if _, err := reconciler.reconcileIdle(context.Background(), &active, tmpl); err != nil {
		t.Fatal(err)
	}
	var suspendedAgain platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(env), &suspendedAgain); err != nil {
		t.Fatal(err)
	}
	if !suspendedAgain.Status.Lifecycle.Suspended || suspendedAgain.Status.Lifecycle.Epoch != 2 {
		t.Fatalf("second suspension status = %#v", suspendedAgain.Status.Lifecycle)
	}
}

func TestLegacyWakeDuringRequestedFenceIsRefusedAndSuspendReplayIsIgnored(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	suspend := &platformv1alpha1.EnvironmentSuspendRequest{
		EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "suspend-1", EnvironmentUID: "env-uid"},
		Sequence:                    1,
	}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "default", UID: "env-uid", Finalizers: []string{environmentFinalizer}},
		Spec: platformv1alpha1.EnvironmentSpec{Lifecycle: platformv1alpha1.EnvironmentLifecycleSpec{
			Suspend: suspend.DeepCopy(),
			Wake:    &platformv1alpha1.EnvironmentWakeRequest{EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "wake-2", EnvironmentUID: "env-uid"}},
		}},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build()
	reconciler := &EnvironmentReconciler{Client: kubeClient, Scheme: scheme}

	if _, err := reconciler.reconcileLifecycleIntent(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	var fencing platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil {
		t.Fatal(err)
	}
	if !fencing.Status.Lifecycle.Suspended || fencing.Status.Lifecycle.LastSuspendRequestID != "suspend-1" || fencing.Status.Lifecycle.PendingSuspendRequestID != "suspend-1" || fencing.Status.Lifecycle.LastWakeRequestID != "wake-2" {
		t.Fatalf("legacy wake was not refused by requested fence: %#v", fencing.Status.Lifecycle)
	}
	fencing.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Revision: 1}
	fencing.Spec.Lifecycle.Wake.HoldPolicyRevision = 1
	if err := kubeClient.Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileLifecycleIntent(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil {
		t.Fatal(err)
	}
	if !fencing.Status.Lifecycle.Suspended || fencing.Status.Lifecycle.PendingSuspendRequestID != "suspend-1" || fencing.Status.Lifecycle.LastWakeRequestID != "wake-2" {
		t.Fatalf("policy revision preempted accepted fence: %#v", fencing.Status.Lifecycle)
	}
	fencing.Spec.Lifecycle.Suspend = nil
	if err := kubeClient.Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileLifecycleIntent(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil {
		t.Fatal(err)
	}
	if !fencing.Status.Lifecycle.Suspended || fencing.Status.Lifecycle.PendingSuspendRequestID != "suspend-1" || fencing.Status.Lifecycle.LastWakeRequestID != "wake-2" {
		t.Fatalf("spec removal preempted accepted fence: %#v", fencing.Status.Lifecycle)
	}
	fencing.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
	if err := kubeClient.Status().Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcilePaused(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)}); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil {
		t.Fatal(err)
	}
	if fencing.Spec.Lifecycle.Suspend != nil {
		t.Fatalf("acknowledged suspend remained in spec: %#v", fencing.Spec.Lifecycle.Suspend)
	}
	if _, err := reconciler.reconcileLifecycleIntent(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	var active platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(env), &active); err != nil {
		t.Fatal(err)
	}
	if !active.Status.Lifecycle.Suspended || active.Status.Lifecycle.SuspensionReason != platformv1alpha1.EnvironmentSuspensionReasonRequested || active.Status.Lifecycle.LastWakeRequestID != "wake-2" || active.Status.Lifecycle.Epoch != 1 {
		t.Fatalf("legacy wake resumed acknowledged fence: %#v", active.Status.Lifecycle)
	}
	replayed := suspend.DeepCopy()
	replayed.HoldPolicyRevision = 1
	active.Spec.Lifecycle.Suspend = replayed
	if err := kubeClient.Update(context.Background(), &active); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)}); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(env), &active); err != nil {
		t.Fatal(err)
	}
	if active.Spec.Lifecycle.Suspend != nil || !active.Status.Lifecycle.Suspended || active.Status.Lifecycle.SuspensionReason != platformv1alpha1.EnvironmentSuspensionReasonRequested || active.Status.Lifecycle.Epoch != 1 {
		t.Fatalf("replayed suspend changed lifecycle: spec=%#v status=%#v", active.Spec.Lifecycle, active.Status.Lifecycle)
	}
}

func TestConsumedSuspendSequenceRejectsABAReplay(t *testing.T) {
	for _, test := range []struct {
		name    string
		pending bool
	}{
		{name: "after newer request was acknowledged and resumed"},
		{name: "while newer request is pending", pending: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := platformv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			environment := &platformv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "default", UID: "env-uid", Finalizers: []string{environmentFinalizer}},
				Spec: platformv1alpha1.EnvironmentSpec{Lifecycle: platformv1alpha1.EnvironmentLifecycleSpec{
					Suspend: &platformv1alpha1.EnvironmentSuspendRequest{
						EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "suspend-a", EnvironmentUID: "env-uid"},
						Sequence:                    1,
					},
				}},
				Status: platformv1alpha1.EnvironmentStatus{
					Phase: platformv1alpha1.EnvironmentPhaseReady,
					Lifecycle: platformv1alpha1.EnvironmentLifecycleStatus{
						Epoch: 2, LastSuspendRequestID: "suspend-b", LastSuspendRequestSequence: 2,
					},
				},
			}
			if test.pending {
				environment.Status.Lifecycle.Suspended = true
				environment.Status.Lifecycle.SuspensionReason = platformv1alpha1.EnvironmentSuspensionReasonRequested
				environment.Status.Lifecycle.SuspensionRequestID = "suspend-b"
				environment.Status.Lifecycle.PendingSuspendRequestID = "suspend-b"
			}
			kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(environment).Build()
			reconciler := &EnvironmentReconciler{Client: kubeClient, Scheme: scheme}
			key := client.ObjectKeyFromObject(environment)

			if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			var current platformv1alpha1.Environment
			if err := kubeClient.Get(context.Background(), key, &current); err != nil {
				t.Fatal(err)
			}
			if current.Spec.Lifecycle.Suspend != nil {
				t.Fatalf("ABA replay remained in spec: %#v", current.Spec.Lifecycle.Suspend)
			}
			if current.Status.Lifecycle.LastSuspendRequestID != "suspend-b" || current.Status.Lifecycle.LastSuspendRequestSequence != 2 || current.Status.Lifecycle.PendingSuspendRequestID != environment.Status.Lifecycle.PendingSuspendRequestID || current.Status.Lifecycle.Epoch != 2 {
				t.Fatalf("ABA replay changed lifecycle status: %#v", current.Status.Lifecycle)
			}
		})
	}
}

func TestSuspendSequenceAdvancesAfterPendingFenceAcknowledgement(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "default", UID: "env-uid"},
		Spec: platformv1alpha1.EnvironmentSpec{Lifecycle: platformv1alpha1.EnvironmentLifecycleSpec{
			Suspend: &platformv1alpha1.EnvironmentSuspendRequest{
				EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "suspend-b", EnvironmentUID: "env-uid"},
				Sequence:                    2,
			},
		}},
		Status: platformv1alpha1.EnvironmentStatus{Lifecycle: platformv1alpha1.EnvironmentLifecycleStatus{
			LastSuspendRequestID: "suspend-a", LastSuspendRequestSequence: 1,
		}},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(environment).Build()
	reconciler := &EnvironmentReconciler{Client: kubeClient, Scheme: scheme}
	key := client.ObjectKeyFromObject(environment)

	if _, err := reconciler.reconcileLifecycleIntent(context.Background(), environment); err != nil {
		t.Fatal(err)
	}
	var current platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), key, &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Lifecycle.LastSuspendRequestID != "suspend-b" || current.Status.Lifecycle.LastSuspendRequestSequence != 2 || current.Status.Lifecycle.PendingSuspendRequestID != "suspend-b" {
		t.Fatalf("second request was not accepted: %#v", current.Status.Lifecycle)
	}
	current.Spec.Lifecycle.Suspend = &platformv1alpha1.EnvironmentSuspendRequest{
		EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "suspend-c", EnvironmentUID: "env-uid"},
		Sequence:                    3,
	}
	if err := kubeClient.Update(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileLifecycleIntent(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(context.Background(), key, &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Lifecycle.LastSuspendRequestID != "suspend-b" || current.Status.Lifecycle.LastSuspendRequestSequence != 2 || current.Status.Lifecycle.PendingSuspendRequestID != "suspend-b" {
		t.Fatalf("new request preempted pending fence: %#v", current.Status.Lifecycle)
	}
	current.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
	if err := kubeClient.Status().Update(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcilePaused(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(context.Background(), key, &current); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileLifecycleIntent(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(context.Background(), key, &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Lifecycle.LastSuspendRequestID != "suspend-c" || current.Status.Lifecycle.LastSuspendRequestSequence != 3 || current.Status.Lifecycle.PendingSuspendRequestID != "suspend-c" {
		t.Fatalf("new request was not accepted after acknowledgement: %#v", current.Status.Lifecycle)
	}
}

func TestReconcileIdleSchedulesRemainingTimeout(t *testing.T) {
	now := time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC)
	lastActive := metav1.NewTime(now)
	env := &platformv1alpha1.Environment{
		Status: platformv1alpha1.EnvironmentStatus{LastActiveAt: &lastActive},
	}
	tmpl := &platformv1alpha1.EnvironmentTemplate{
		Spec: platformv1alpha1.EnvironmentTemplateSpec{IdleTimeout: &metav1.Duration{Duration: time.Minute}},
	}
	reconciler := &EnvironmentReconciler{Now: func() time.Time { return now }}

	result, err := reconciler.reconcileIdle(context.Background(), env, tmpl)
	if err != nil {
		t.Fatalf("reconcileIdle() error = %v", err)
	}
	if result.RequeueAfter <= 0 || result.RequeueAfter > time.Minute {
		t.Fatalf("RequeueAfter = %s, want remaining one-minute timeout", result.RequeueAfter)
	}
}

func TestReconcileIdleProtectsExactActiveRunOwnerAndClaim(t *testing.T) {
	for _, test := range []struct {
		name    string
		claimed bool
	}{
		{name: "owned"},
		{name: "claimed", claimed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := platformv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC)
			stale := metav1.NewTime(now.Add(-time.Hour))
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "active", Namespace: "default", UID: "run-uid"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateRunning}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{LastActiveAt: &stale}}
			if test.claimed {
				env.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID}
			} else {
				env.OwnerReferences = []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true)}}
			}
			reconciler := &EnvironmentReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env, run).WithObjects(env, run).Build(), Now: func() time.Time { return now }}
			tmpl := &platformv1alpha1.EnvironmentTemplate{Spec: platformv1alpha1.EnvironmentTemplateSpec{IdleTimeout: &metav1.Duration{Duration: time.Minute}}}

			result, err := reconciler.reconcileIdle(context.Background(), env, tmpl)
			if err != nil {
				t.Fatal(err)
			}
			var retained platformv1alpha1.Environment
			if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
				t.Fatal(err)
			}
			if retained.Spec.Paused || result.RequeueAfter != time.Minute {
				t.Fatalf("active Run protection = (%#v, %#v), want unpaused one-minute recheck", retained.Spec, result)
			}
		})
	}
}

func TestIdleScopedWakeRacingSuspendRemainsRequestedAfterAcknowledgement(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "default", UID: "env-uid", Finalizers: []string{environmentFinalizer}},
		Spec: platformv1alpha1.EnvironmentSpec{Lifecycle: platformv1alpha1.EnvironmentLifecycleSpec{
			Suspend: &platformv1alpha1.EnvironmentSuspendRequest{
				EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "suspend-1", EnvironmentUID: "env-uid"},
				Sequence:                    1,
			},
			Wake: &platformv1alpha1.EnvironmentWakeRequest{
				EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "terminal-wake", EnvironmentUID: "env-uid"},
				ExpectedSuspensionReason:    platformv1alpha1.EnvironmentSuspensionReasonIdle,
			},
		}},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(environment).Build()
	reconciler := &EnvironmentReconciler{Client: kubeClient, Scheme: scheme}

	if _, err := reconciler.reconcileLifecycleIntent(context.Background(), environment); err != nil {
		t.Fatal(err)
	}
	var fencing platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(environment), &fencing); err != nil {
		t.Fatal(err)
	}
	if !fencing.Status.Lifecycle.Suspended || fencing.Status.Lifecycle.SuspensionReason != platformv1alpha1.EnvironmentSuspensionReasonRequested || fencing.Status.Lifecycle.LastWakeRequestID != "terminal-wake" || fencing.Status.Lifecycle.PendingSuspendRequestID != "suspend-1" {
		t.Fatalf("racing terminal wake changed requested fence: %#v", fencing.Status.Lifecycle)
	}
	fencing.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
	if err := kubeClient.Status().Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcilePaused(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(environment), &fencing); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileLifecycleIntent(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	var acknowledged platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(environment), &acknowledged); err != nil {
		t.Fatal(err)
	}
	if !acknowledged.Status.Lifecycle.Suspended || acknowledged.Status.Lifecycle.SuspensionReason != platformv1alpha1.EnvironmentSuspensionReasonRequested || acknowledged.Status.Lifecycle.PendingSuspendRequestID != "" || acknowledged.Status.Lifecycle.LastWakeRequestID != "terminal-wake" {
		t.Fatalf("acknowledged requested fence was woken: %#v", acknowledged.Status.Lifecycle)
	}
}

func TestRequestedScopedWakeWaitsForFenceAcknowledgementThenResumes(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "default", UID: "env-uid"},
		Spec: platformv1alpha1.EnvironmentSpec{Lifecycle: platformv1alpha1.EnvironmentLifecycleSpec{
			Suspend: &platformv1alpha1.EnvironmentSuspendRequest{
				EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "suspend-1", EnvironmentUID: "env-uid"},
				Sequence:                    1,
			},
			Wake: &platformv1alpha1.EnvironmentWakeRequest{
				EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "run-wake", EnvironmentUID: "env-uid"},
				ExpectedSuspensionReason:    platformv1alpha1.EnvironmentSuspensionReasonRequested,
			},
		}},
		Status: platformv1alpha1.EnvironmentStatus{Lifecycle: platformv1alpha1.EnvironmentLifecycleStatus{
			Suspended: true, SuspensionReason: platformv1alpha1.EnvironmentSuspensionReasonRequested, Epoch: 1,
			LastSuspendRequestID: "suspend-1", LastSuspendRequestSequence: 1, PendingSuspendRequestID: "suspend-1", SuspensionRequestID: "suspend-1",
		}},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(environment).Build()
	reconciler := &EnvironmentReconciler{Client: kubeClient, Scheme: scheme}

	if _, err := reconciler.reconcileLifecycleIntent(context.Background(), environment); err != nil {
		t.Fatal(err)
	}
	var fencing platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(environment), &fencing); err != nil {
		t.Fatal(err)
	}
	if !fencing.Status.Lifecycle.Suspended || fencing.Status.Lifecycle.PendingSuspendRequestID != "suspend-1" || fencing.Status.Lifecycle.LastWakeRequestID != "" {
		t.Fatalf("requested wake preempted cleanup fence: %#v", fencing.Status.Lifecycle)
	}
	fencing.Status.Lifecycle.PendingSuspendRequestID = ""
	fencing.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
	if err := kubeClient.Status().Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileLifecycleIntent(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	var active platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(environment), &active); err != nil {
		t.Fatal(err)
	}
	if active.Status.Lifecycle.Suspended || active.Status.Lifecycle.LastWakeRequestID != "run-wake" || active.Status.Lifecycle.PendingSuspendRequestID != "" {
		t.Fatalf("acknowledged requested suspension did not resume: %#v", active.Status.Lifecycle)
	}
}

func TestWakeAndHoldReleaseWaitForBackendFence(t *testing.T) {
	for _, test := range []struct {
		name   string
		reason platformv1alpha1.EnvironmentSuspensionReason
		spec   platformv1alpha1.EnvironmentLifecycleSpec
	}{
		{
			name:   "idle wake",
			reason: platformv1alpha1.EnvironmentSuspensionReasonIdle,
			spec: platformv1alpha1.EnvironmentLifecycleSpec{Wake: &platformv1alpha1.EnvironmentWakeRequest{
				EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "wake-1", EnvironmentUID: "env-uid"},
			}},
		},
		{
			name:   "hold release",
			reason: platformv1alpha1.EnvironmentSuspensionReasonHold,
			spec:   platformv1alpha1.EnvironmentLifecycleSpec{Hold: &platformv1alpha1.EnvironmentHoldPolicy{Revision: 2}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
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
			const oldIdentity = "old-execution.sandboxd.swe.dev"
			project := &platformv1alpha1.Project{
				ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default"},
				Spec:       platformv1alpha1.ProjectSpec{Repositories: []string{"https://github.com/example/repo"}},
			}
			template := &platformv1alpha1.EnvironmentTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default"},
				Spec:       platformv1alpha1.EnvironmentTemplateSpec{Image: "example/environment:latest", Size: "small"},
			}
			environment := &platformv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "default", UID: "env-uid", Finalizers: []string{environmentFinalizer}},
				Spec:       platformv1alpha1.EnvironmentSpec{ProjectRef: project.Name, TemplateRef: template.Name, Lifecycle: test.spec},
				Status: platformv1alpha1.EnvironmentStatus{
					ExecutionGeneration: 7,
					Phase:               platformv1alpha1.EnvironmentPhaseIdle,
					PodName:             envPodName(&platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared"}}),
					Endpoints:           platformv1alpha1.EnvironmentEndpoints{Sandboxd: "192.0.2.10:50051"},
					Lifecycle: platformv1alpha1.EnvironmentLifecycleStatus{
						Suspended: true, SuspensionReason: test.reason, Epoch: 1, ObservedHoldPolicyRevision: lifecycle.HoldPolicyRevision(&platformv1alpha1.Environment{Spec: platformv1alpha1.EnvironmentSpec{Lifecycle: test.spec}}),
					},
				},
			}
			setTestProvisioningSnapshot(environment, template, project)
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: envPodName(environment), Namespace: environment.Namespace, UID: "pod-uid",
				Annotations: map[string]string{sandboxdauth.IdentityAnnotation: oldIdentity},
			}}
			credentials := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
				Name: envCredentialName(environment), Namespace: environment.Namespace, UID: "credentials-uid",
				Annotations: map[string]string{sandboxdauth.IdentityAnnotation: oldIdentity},
			}}
			if err := controllerutil.SetControllerReference(environment, pod, scheme); err != nil {
				t.Fatal(err)
			}
			if err := controllerutil.SetControllerReference(environment, credentials, scheme); err != nil {
				t.Fatal(err)
			}
			reconciler := &EnvironmentReconciler{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(environment, pod, credentials, project, template).Build(),
				Scheme: scheme,
			}
			key := client.ObjectKeyFromObject(environment)

			if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			var fencing platformv1alpha1.Environment
			if err := reconciler.Get(context.Background(), key, &fencing); err != nil {
				t.Fatal(err)
			}
			if !fencing.Status.Lifecycle.Suspended || fencing.Status.Phase == platformv1alpha1.EnvironmentPhasePaused || fencing.Status.Lifecycle.LastWakeRequestID != "" {
				t.Fatalf("release preempted pod teardown: %#v", fencing.Status)
			}
			if fencing.Status.PodName != "" || fencing.Status.Endpoints.Sandboxd != "" {
				t.Fatalf("readiness was not withdrawn before pod teardown: %#v", fencing.Status)
			}
			if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
				t.Fatalf("pod was deleted before readiness withdrawal: %v", err)
			}
			if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
				t.Fatalf("pod was not deleted before credentials: %v", err)
			}
			if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(credentials), &corev1.Secret{}); err != nil {
				t.Fatalf("credentials revoked before pod teardown acknowledgement: %v", err)
			}
			if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(credentials), &corev1.Secret{}); !apierrors.IsNotFound(err) {
				t.Fatalf("credentials were not revoked after pod deletion: %v", err)
			}
			if err := reconciler.Get(context.Background(), key, &fencing); err != nil {
				t.Fatal(err)
			}
			if !fencing.Status.Lifecycle.Suspended || fencing.Status.Phase == platformv1alpha1.EnvironmentPhasePaused {
				t.Fatalf("release preempted credential teardown: %#v", fencing.Status)
			}
			if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			if err := reconciler.Get(context.Background(), key, &fencing); err != nil {
				t.Fatal(err)
			}
			if !fencing.Status.Lifecycle.Suspended || fencing.Status.Phase != platformv1alpha1.EnvironmentPhasePaused || fencing.Status.PodName != "" || fencing.Status.Endpoints.Sandboxd != "" {
				t.Fatalf("backend fence was not acknowledged before release: %#v", fencing.Status)
			}
			if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			if err := reconciler.Get(context.Background(), key, &fencing); err != nil {
				t.Fatal(err)
			}
			if fencing.Status.Lifecycle.Suspended {
				t.Fatalf("acknowledged backend fence did not release: %#v", fencing.Status.Lifecycle)
			}
			if test.reason == platformv1alpha1.EnvironmentSuspensionReasonIdle && fencing.Status.Lifecycle.LastWakeRequestID != "wake-1" {
				t.Fatalf("idle wake was not consumed after fence: %#v", fencing.Status.Lifecycle)
			}
			if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
				t.Fatalf("old pod survived backend fence: %v", err)
			}
			if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(credentials), &corev1.Secret{}); !apierrors.IsNotFound(err) {
				t.Fatalf("old credentials survived backend fence: %v", err)
			}

			if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			if err := reconciler.Get(context.Background(), key, &fencing); err != nil {
				t.Fatal(err)
			}
			if fencing.Status.Phase == platformv1alpha1.EnvironmentPhasePaused {
				if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
					t.Fatal(err)
				}
				if err := reconciler.Get(context.Background(), key, &fencing); err != nil {
					t.Fatal(err)
				}
			}
			if fencing.Status.Phase != platformv1alpha1.EnvironmentPhaseResuming || fencing.Status.Endpoints.Sandboxd != "" {
				t.Fatalf("released Environment skipped fenced resume: %#v", fencing.Status)
			}
			if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			var replacementPod corev1.Pod
			if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(pod), &replacementPod); err != nil {
				t.Fatal(err)
			}
			var replacementCredentials corev1.Secret
			if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(credentials), &replacementCredentials); err != nil {
				t.Fatal(err)
			}
			if replacementPod.Annotations[sandboxdauth.IdentityAnnotation] == oldIdentity || replacementCredentials.Annotations[sandboxdauth.IdentityAnnotation] == oldIdentity || replacementPod.Annotations[sandboxdauth.IdentityAnnotation] != replacementCredentials.Annotations[sandboxdauth.IdentityAnnotation] {
				t.Fatalf("replacement reused old execution identity: pod=%q credentials=%q", replacementPod.Annotations[sandboxdauth.IdentityAnnotation], replacementCredentials.Annotations[sandboxdauth.IdentityAnnotation])
			}
			if err := reconciler.Get(context.Background(), key, &fencing); err != nil {
				t.Fatal(err)
			}
			if replacementPod.Annotations[executionGenerationAnnotation] != "8" || fencing.Status.ExecutionGeneration != 8 {
				t.Fatalf("resume execution generation = status %d, Pod %q, want 8", fencing.Status.ExecutionGeneration, replacementPod.Annotations[executionGenerationAnnotation])
			}
			setup := replacementPod.Spec.InitContainers[1]
			resuming := false
			for _, variable := range setup.Env {
				if variable.Name == "SWE_RESUMING" && variable.Value == "true" {
					resuming = true
				}
			}
			if !resuming {
				t.Fatalf("replacement pod did not use resume path: %#v", setup.Env)
			}
		})
	}
}

func TestHoldAcceptsSuspendFenceAndAllowsRunCleanupInEitherOrdering(t *testing.T) {
	for _, test := range []struct {
		name            string
		requestRevision int64
		status          platformv1alpha1.EnvironmentLifecycleStatus
	}{
		{name: "enabled hold then current-revision suspend", requestRevision: 2},
		{
			name:            "accepted suspend then hold",
			requestRevision: 1,
			status: platformv1alpha1.EnvironmentLifecycleStatus{
				Suspended: true, SuspensionReason: platformv1alpha1.EnvironmentSuspensionReasonRequested, Epoch: 1,
				LastSuspendRequestID: "cleanup-fence", LastSuspendRequestSequence: 1, PendingSuspendRequestID: "cleanup-fence", SuspensionRequestID: "cleanup-fence",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := platformv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			deletedAt := metav1.Now()
			run := &platformv1alpha1.Run{
				ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "default", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &deletedAt},
				Spec:       platformv1alpha1.RunSpec{Agent: "test"},
				Status: platformv1alpha1.RunStatus{
					State: platformv1alpha1.RunStateFailed,
					EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{
						Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed,
					},
				},
			}
			environment := &platformv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "default", UID: "env-uid", Finalizers: []string{environmentFinalizer}},
				Spec: platformv1alpha1.EnvironmentSpec{Lifecycle: platformv1alpha1.EnvironmentLifecycleSpec{
					Hold: &platformv1alpha1.EnvironmentHoldPolicy{Enabled: true, Revision: 2},
					Suspend: &platformv1alpha1.EnvironmentSuspendRequest{
						EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "cleanup-fence", EnvironmentUID: "env-uid", HoldPolicyRevision: test.requestRevision},
						Sequence:                    1,
					},
				}},
				Status: platformv1alpha1.EnvironmentStatus{
					Phase:     platformv1alpha1.EnvironmentPhaseReady,
					PodName:   "env-shared",
					Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "192.0.2.10:50051"},
					ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
					Lifecycle: test.status,
				},
			}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envPodName(environment), Namespace: environment.Namespace, UID: "old-pod-uid"}}
			credentials := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: envCredentialName(environment), Namespace: environment.Namespace, UID: "old-credentials-uid"}}
			if err := controllerutil.SetControllerReference(environment, pod, scheme); err != nil {
				t.Fatal(err)
			}
			if err := controllerutil.SetControllerReference(environment, credentials, scheme); err != nil {
				t.Fatal(err)
			}
			kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment, run).WithObjects(environment, run, pod, credentials).Build()
			environmentReconciler := &EnvironmentReconciler{Client: kubeClient, Scheme: scheme}
			runReconciler := &RunReconciler{Client: kubeClient, Scheme: scheme, Adapters: map[string]agent.AdapterLifecycle{"test": &scriptedAdapter{}}}
			key := client.ObjectKeyFromObject(environment)
			reconcileEnvironment := func() {
				t.Helper()
				if _, err := environmentReconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
					t.Fatal(err)
				}
			}

			reconcileEnvironment()
			var fencing platformv1alpha1.Environment
			if err := environmentReconciler.Get(context.Background(), key, &fencing); err != nil {
				t.Fatal(err)
			}
			if !fencing.Status.Lifecycle.Suspended || fencing.Status.Lifecycle.SuspensionReason != platformv1alpha1.EnvironmentSuspensionReasonHold || fencing.Status.Lifecycle.SuspensionRequestID != "" || fencing.Status.Lifecycle.LastSuspendRequestID != "cleanup-fence" || fencing.Status.Lifecycle.PendingSuspendRequestID != "cleanup-fence" {
				t.Fatalf("hold did not retain cleanup fence receipt: %#v", fencing.Status.Lifecycle)
			}
			if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
				t.Fatalf("pod disappeared before suspend receipt persisted: %v", err)
			}
			if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(credentials), &corev1.Secret{}); err != nil {
				t.Fatalf("credentials disappeared before suspend receipt persisted: %v", err)
			}

			reconcileEnvironment()
			var withdrawing platformv1alpha1.Environment
			if err := kubeClient.Get(context.Background(), key, &withdrawing); err != nil || withdrawing.Status.PodName != "" || withdrawing.Status.Endpoints.Sandboxd != "" {
				t.Fatalf("readiness was not withdrawn before pod teardown: environment=%#v error=%v", withdrawing, err)
			}
			if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
				t.Fatalf("pod disappeared before readiness withdrawal: %v", err)
			}

			reconcileEnvironment()
			if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
				t.Fatalf("pod was not deleted first: %v", err)
			}
			if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(credentials), &corev1.Secret{}); err != nil {
				t.Fatalf("credentials were revoked before pod absence: %v", err)
			}
			if err := kubeClient.Get(context.Background(), key, &fencing); err != nil || !fencing.Status.Lifecycle.Suspended || fencing.Status.Lifecycle.PendingSuspendRequestID != "cleanup-fence" {
				t.Fatalf("pod teardown lost pending suspend receipt: environment=%#v error=%v", fencing, err)
			}

			reconcileEnvironment()
			if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(credentials), &corev1.Secret{}); !apierrors.IsNotFound(err) {
				t.Fatalf("credentials were not revoked after pod absence: %v", err)
			}
			if err := kubeClient.Get(context.Background(), key, &fencing); err != nil || !fencing.Status.Lifecycle.Suspended || fencing.Status.Phase == platformv1alpha1.EnvironmentPhasePaused || fencing.Status.Lifecycle.PendingSuspendRequestID != "cleanup-fence" {
				t.Fatalf("credential teardown prematurely acknowledged fence: environment=%#v error=%v", fencing, err)
			}

			reconcileEnvironment()
			if err := kubeClient.Get(context.Background(), key, &fencing); err != nil || fencing.Status.Phase != platformv1alpha1.EnvironmentPhasePaused || fencing.Status.Lifecycle.PendingSuspendRequestID != "cleanup-fence" || fencing.Status.PodName != "" || fencing.Status.Endpoints.Sandboxd != "" {
				t.Fatalf("physical fence was not published before request acknowledgement: environment=%#v error=%v", fencing, err)
			}

			reconcileEnvironment()
			if err := kubeClient.Get(context.Background(), key, &fencing); err != nil || fencing.Status.Lifecycle.PendingSuspendRequestID != "" || fencing.Spec.Lifecycle.Suspend == nil {
				t.Fatalf("pending suspend was not acknowledged before spec cleanup: environment=%#v error=%v", fencing, err)
			}

			reconcileEnvironment()
			if err := environmentReconciler.Get(context.Background(), key, &fencing); err != nil {
				t.Fatal(err)
			}
			if fencing.Spec.Lifecycle.Suspend != nil || fencing.Status.Lifecycle.PendingSuspendRequestID != "" || fencing.Status.Phase != platformv1alpha1.EnvironmentPhasePaused || !fencing.Status.Lifecycle.Suspended || fencing.Status.Lifecycle.SuspensionReason != platformv1alpha1.EnvironmentSuspensionReasonHold || fencing.Status.ClaimedBy == nil {
				t.Fatalf("held cleanup fence did not complete before claim release: spec=%#v status=%#v", fencing.Spec.Lifecycle, fencing.Status)
			}

			if _, err := runReconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
				t.Fatal(err)
			}
			if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(run), &platformv1alpha1.Run{}); !apierrors.IsNotFound(err) {
				t.Fatalf("deleting Run finalizer did not complete: %v", err)
			}
			if err := kubeClient.Get(context.Background(), key, &fencing); err != nil {
				t.Fatal(err)
			}
			if fencing.Status.ClaimedBy != nil {
				t.Fatalf("completed cleanup retained exact claim: %#v", fencing.Status.ClaimedBy)
			}

			fencing.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Revision: 3}
			if err := kubeClient.Update(context.Background(), &fencing); err != nil {
				t.Fatal(err)
			}
			if _, err := environmentReconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			if err := kubeClient.Get(context.Background(), key, &fencing); err != nil {
				t.Fatal(err)
			}
			if fencing.Status.Lifecycle.Suspended || fencing.Status.Lifecycle.PendingSuspendRequestID != "" || fencing.Spec.Lifecycle.Suspend != nil || fencing.Status.Lifecycle.LastSuspendRequestID != "cleanup-fence" {
				t.Fatalf("hold release replayed completed suspend: spec=%#v status=%#v", fencing.Spec.Lifecycle, fencing.Status.Lifecycle)
			}
		})
	}
}

func TestReconcileIdleRestartUsesPersistedActivityDeadline(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	started := time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC)
	lastActive := metav1.NewTime(started)
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}, Status: platformv1alpha1.EnvironmentStatus{LastActiveAt: &lastActive}}
	clientAfterRestart := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build()
	tmpl := &platformv1alpha1.EnvironmentTemplate{Spec: platformv1alpha1.EnvironmentTemplateSpec{IdleTimeout: &metav1.Duration{Duration: time.Minute}}}

	first := &EnvironmentReconciler{Client: clientAfterRestart, Now: func() time.Time { return started.Add(30 * time.Second) }}
	result, err := first.reconcileIdle(context.Background(), env.DeepCopy(), tmpl)
	if err != nil || result.RequeueAfter != 30*time.Second {
		t.Fatalf("first restarted reconcile = (%#v, %v), want persisted 30-second remainder", result, err)
	}
	second := &EnvironmentReconciler{Client: clientAfterRestart, Now: func() time.Time { return started.Add(61 * time.Second) }}
	if _, err := second.reconcileIdle(context.Background(), env.DeepCopy(), tmpl); err != nil {
		t.Fatal(err)
	}
	var paused platformv1alpha1.Environment
	if err := clientAfterRestart.Get(context.Background(), client.ObjectKeyFromObject(env), &paused); err != nil {
		t.Fatal(err)
	}
	if !paused.Status.Lifecycle.Suspended {
		t.Fatal("fresh reconciler forgot the persisted idle deadline")
	}
}

func TestReconcileIdleDoesNotTreatTerminalRunClaimAsActive(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC)
	stale := metav1.NewTime(now.Add(-time.Hour))
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "done", Namespace: "default", UID: "run-uid"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateSucceeded}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}, Status: platformv1alpha1.EnvironmentStatus{LastActiveAt: &stale, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID}}}
	reconciler := &EnvironmentReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env, run).WithObjects(env, run).Build(), Now: func() time.Time { return now }}
	tmpl := &platformv1alpha1.EnvironmentTemplate{Spec: platformv1alpha1.EnvironmentTemplateSpec{IdleTimeout: &metav1.Duration{Duration: time.Minute}}}

	if _, err := reconciler.reconcileIdle(context.Background(), env, tmpl); err != nil {
		t.Fatal(err)
	}
	var paused platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &paused); err != nil {
		t.Fatal(err)
	}
	if !paused.Status.Lifecycle.Suspended {
		t.Fatal("terminal Run claim incorrectly retained active-Run protection")
	}
}

func TestReconcileIdleClaimRaceCannotCommitPause(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC)
	stale := metav1.NewTime(now.Add(-time.Hour))
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "claiming", Namespace: "default", UID: "run-uid"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateAllocating}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{LastActiveAt: &stale}}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env, run).WithObjects(env, run).Build()
	claimed := false
	interceptedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, underlying client.Client, subresource string, object client.Object, options ...client.SubResourceUpdateOption) error {
			if !claimed {
				claimed = true
				var current platformv1alpha1.Environment
				if err := underlying.Get(ctx, client.ObjectKeyFromObject(env), &current); err != nil {
					return err
				}
				current.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID}
				if err := underlying.Status().Update(ctx, &current); err != nil {
					return err
				}
			}
			return underlying.SubResource(subresource).Update(ctx, object, options...)
		},
	})
	reconciler := &EnvironmentReconciler{Client: interceptedClient, Now: func() time.Time { return now }}
	tmpl := &platformv1alpha1.EnvironmentTemplate{Spec: platformv1alpha1.EnvironmentTemplateSpec{IdleTimeout: &metav1.Duration{Duration: time.Minute}}}

	if _, err := reconciler.reconcileIdle(context.Background(), env, tmpl); !apierrors.IsConflict(err) {
		t.Fatalf("claim race error = %v, want optimistic-lock conflict", err)
	}
	var current platformv1alpha1.Environment
	if err := baseClient.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Lifecycle.Suspended || current.Status.ClaimedBy == nil {
		t.Fatalf("claim race committed automatic pause: %#v", current)
	}
	if _, err := reconciler.reconcileIdle(context.Background(), &current, tmpl); err != nil {
		t.Fatal(err)
	}
	if err := baseClient.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Lifecycle.Suspended {
		t.Fatal("active claim was not protected after the race retry")
	}
}

func TestExplicitPauseRemainsAuthoritativeWithActiveRun(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "active", Namespace: "default", UID: "run-uid"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateRunning}}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "env-uid", Finalizers: []string{environmentFinalizer}, OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true)}}},
		Spec:       platformv1alpha1.EnvironmentSpec{Paused: true, TemplateRef: "deleted-template"},
		Status:     platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady},
	}
	reconciler := &EnvironmentReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env, run).WithObjects(env, run).Build(), Scheme: scheme}

	for range 3 {
		if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)}); err != nil {
			t.Fatal(err)
		}
	}
	var paused platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &paused); err != nil {
		t.Fatal(err)
	}
	if paused.Spec.Paused || paused.Spec.Lifecycle.Hold == nil || !paused.Spec.Lifecycle.Hold.Enabled || !paused.Status.Lifecycle.Suspended || paused.Status.Lifecycle.SuspensionReason != platformv1alpha1.EnvironmentSuspensionReasonHold || paused.Status.Phase != platformv1alpha1.EnvironmentPhasePaused {
		t.Fatalf("explicit pause was overridden by active Run protection: %#v", paused)
	}
}

func TestDeprecatedPauseMigrationCannotDisableExistingHoldPolicy(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "mixed", Namespace: "default", UID: "env-uid", Finalizers: []string{environmentFinalizer}},
		Spec: platformv1alpha1.EnvironmentSpec{Paused: true, Lifecycle: platformv1alpha1.EnvironmentLifecycleSpec{Hold: &platformv1alpha1.EnvironmentHoldPolicy{
			Enabled: false, Revision: 3,
		}}},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build()
	reconciler := &EnvironmentReconciler{Client: kubeClient}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)}); err != nil {
		t.Fatal(err)
	}
	var migrated platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(env), &migrated); err != nil {
		t.Fatal(err)
	}
	if migrated.Spec.Paused || migrated.Spec.Lifecycle.Hold == nil || !migrated.Spec.Lifecycle.Hold.Enabled || migrated.Spec.Lifecycle.Hold.Revision != 4 {
		t.Fatalf("mixed legacy pause migration = %#v", migrated.Spec)
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
		Status:     platformv1alpha1.EnvironmentStatus{ExecutionGeneration: 1, Phase: platformv1alpha1.EnvironmentPhaseSetup, LastActiveAt: &oldActivity},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "env-test", Namespace: "default", Annotations: map[string]string{executionGenerationAnnotation: "1"}},
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

func TestActivityIntentPreservesReadyGenerationAndProtectsIdle(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 21, 1, 0, 0, 0, time.UTC)
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "env-uid", Generation: 1},
		Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: "small", Lifecycle: platformv1alpha1.EnvironmentLifecycleSpec{
			Hold: &platformv1alpha1.EnvironmentHoldPolicy{Revision: 2},
		}},
	}
	environment.Status.ExecutionGeneration = 1
	applyEnvironmentStatus(environment, platformv1alpha1.EnvironmentPhaseReady, "env-test", "10.0.0.1:50051", "SandboxdReady", "ready", nil)
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(environment).Build()
	reconciler := &EnvironmentReconciler{Client: kubeClient, Scheme: scheme, Now: func() time.Time { return now }}
	key := client.ObjectKeyFromObject(environment)

	if err := lifecycle.RecordActivity(context.Background(), kubeClient, lifecycle.CaptureExecutionFence(environment), platformv1alpha1.EnvironmentActivitySourceTerminal, "terminal-2"); err != nil {
		t.Fatal(err)
	}
	var activity platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), key, &activity); err != nil {
		t.Fatal(err)
	}
	if activity.Generation != 1 || activity.Status.ObservedGeneration != activity.Generation || !platformv1alpha1.IsEnvironmentReady(&activity) {
		t.Fatalf("publishing activity made Ready status stale: generation=%d status=%#v", activity.Generation, activity.Status)
	}
	if _, err := reconciler.reconcileLifecycleIntent(context.Background(), &activity); err != nil {
		t.Fatal(err)
	}
	var consumed platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), key, &consumed); err != nil {
		t.Fatal(err)
	}
	if consumed.Status.LastActiveAt == nil || !consumed.Status.LastActiveAt.Equal(&metav1.Time{Time: now}) || activityReceipt(consumed.Status.Lifecycle.ActivityReceipts, platformv1alpha1.EnvironmentActivitySourceTerminal, 1) != "terminal-2" || consumed.Status.Lifecycle.ObservedHoldPolicyRevision != 2 {
		t.Fatalf("consumed activity = %#v", consumed.Status)
	}
	if consumed.Status.ObservedGeneration != consumed.Generation || !platformv1alpha1.IsEnvironmentReady(&consumed) {
		t.Fatalf("consuming activity disturbed Ready status: generation=%d status=%#v", consumed.Generation, consumed.Status)
	}
	template := &platformv1alpha1.EnvironmentTemplate{Spec: platformv1alpha1.EnvironmentTemplateSpec{IdleTimeout: &metav1.Duration{Duration: time.Minute}}}
	result, err := reconciler.reconcileIdle(context.Background(), &consumed, template)
	if err != nil || consumed.Status.Lifecycle.Suspended || result.RequeueAfter <= 0 {
		t.Fatalf("fresh activity idle protection = (%#v, %#v, %v)", consumed.Status.Lifecycle, result, err)
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
		Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "deleted-template", Lifecycle: platformv1alpha1.EnvironmentLifecycleSpec{Hold: &platformv1alpha1.EnvironmentHoldPolicy{Enabled: true, Revision: 1}}},
		Status: platformv1alpha1.EnvironmentStatus{
			Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "missing-pod", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
			Lifecycle:  platformv1alpha1.EnvironmentLifecycleStatus{Suspended: true, SuspensionReason: platformv1alpha1.EnvironmentSuspensionReasonHold, Epoch: 1, ObservedHoldPolicyRevision: 1},
			Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, Reason: "SandboxdReady"}},
		},
	}
	controller := true
	owner := metav1.OwnerReference{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: &controller}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envPodName(env), Namespace: env.Namespace, UID: "pod-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: envCredentialName(env), Namespace: env.Namespace, UID: "secret-uid", OwnerReferences: []metav1.OwnerReference{owner}}}
	reconciler := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, pod, secret).Build(),
		Scheme: scheme,
	}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("paused readiness withdrawal depended on deleted template: %v", err)
	}
	var withdrawing platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &withdrawing); err != nil {
		t.Fatal(err)
	}
	ready := apimeta.FindStatusCondition(withdrawing.Status.Conditions, platformv1alpha1.EnvironmentConditionReady)
	if withdrawing.Status.PodName != "" || withdrawing.Status.Endpoints.Sandboxd != "" || ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "PauseRequested" {
		t.Fatalf("pause did not withdraw readiness first: %#v", withdrawing.Status)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
		t.Fatal("paused Pod was deleted before readiness withdrawal")
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("paused Pod fence depended on deleted template: %v", err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("paused exact-owned Pod still exists: %v", err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(secret), &corev1.Secret{}); err != nil {
		t.Fatal("paused credentials were revoked before Pod absence")
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("paused credential fence depended on deleted template: %v", err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(secret), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("paused exact-owned Secret still exists: %v", err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("paused status convergence depended on deleted template: %v", err)
	}
	var fenced platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &fenced); err != nil {
		t.Fatal(err)
	}
	if fenced.Status.Phase != platformv1alpha1.EnvironmentPhasePaused || fenced.Status.PodName != "" || fenced.Status.Endpoints.Sandboxd != "" {
		t.Fatalf("fenced status = %#v", fenced.Status)
	}
}
