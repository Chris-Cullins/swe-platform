package controllers

import (
	"context"
	"errors"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
)

func TestWarmPoolReconcileCreatesMinimum(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	tmpl := &platformv1alpha1.EnvironmentTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default", UID: "template-uid"},
		Spec: platformv1alpha1.EnvironmentTemplateSpec{
			Backend:  platformv1alpha1.EnvironmentBackendPod,
			WarmPool: &platformv1alpha1.WarmPoolSpec{Min: 2},
		},
	}
	reconciler := &WarmPoolReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(tmpl).WithObjects(tmpl).Build(),
		Scheme: scheme,
	}

	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tmpl)}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var environments platformv1alpha1.EnvironmentList
	if err := reconciler.List(context.Background(), &environments, client.InNamespace("default"), client.MatchingLabels{warmPoolLabel: "small"}); err != nil {
		t.Fatal(err)
	}
	if len(environments.Items) != 2 {
		t.Fatalf("warm environments = %d, want 2", len(environments.Items))
	}
	for _, env := range environments.Items {
		if env.Spec.TemplateRef != "small" || env.Spec.Backend != "" {
			t.Errorf("environment spec = %#v, want inheritance from small template", env.Spec)
		}
		if !metav1.IsControlledBy(&env, tmpl) {
			t.Errorf("environment %q is not controlled by template", env.Name)
		}
		if env.OwnerReferences[0].BlockOwnerDeletion == nil || *env.OwnerReferences[0].BlockOwnerDeletion {
			t.Errorf("environment %q blocks template deletion", env.Name)
		}
	}
}

func TestWarmPoolDoesNotReplenishUnsupportedBackend(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	template := &platformv1alpha1.EnvironmentTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: "default", UID: "template-uid"},
		Spec: platformv1alpha1.EnvironmentTemplateSpec{
			Backend:  platformv1alpha1.EnvironmentBackendKubeVirt,
			WarmPool: &platformv1alpha1.WarmPoolSpec{Min: 2},
		},
		Status: platformv1alpha1.EnvironmentTemplateStatus{WarmPoolReady: 1},
	}
	reconciler := &WarmPoolReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(template).WithObjects(template).Build(), Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(template)}); err != nil {
		t.Fatal(err)
	}
	var environments platformv1alpha1.EnvironmentList
	if err := reconciler.List(context.Background(), &environments); err != nil {
		t.Fatal(err)
	}
	var updated platformv1alpha1.EnvironmentTemplate
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(template), &updated); err != nil {
		t.Fatal(err)
	}
	if len(environments.Items) != 0 || updated.Status.WarmPoolReady != 0 {
		t.Fatalf("unsupported warm pool created %d environments and reports %d ready", len(environments.Items), updated.Status.WarmPoolReady)
	}
}

func TestWarmPoolReconcileReportsReadyAndRemovesExcess(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	tmpl := &platformv1alpha1.EnvironmentTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default", UID: "template-uid"},
		Spec: platformv1alpha1.EnvironmentTemplateSpec{
			WarmPool: &platformv1alpha1.WarmPoolSpec{Min: 1},
		},
	}
	environments := []client.Object{
		&platformv1alpha1.Environment{
			ObjectMeta: metav1.ObjectMeta{Name: "warm-old", Namespace: "default", UID: "warm-old-uid", Labels: map[string]string{warmPoolLabel: "small"}, CreationTimestamp: metav1.Unix(1, 0)},
			Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
			Status:     platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady},
		},
		&platformv1alpha1.Environment{
			ObjectMeta: metav1.ObjectMeta{Name: "warm-new", Namespace: "default", UID: "warm-new-uid", Labels: map[string]string{warmPoolLabel: "small"}, CreationTimestamp: metav1.Unix(2, 0)},
			Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
			Status:     platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady},
		},
	}
	for _, object := range environments {
		env := object.(*platformv1alpha1.Environment)
		setWarmPoolOwner(t, scheme, tmpl, env)
		applyEnvironmentStatus(env, platformv1alpha1.EnvironmentPhaseReady, "env-"+env.Name, "10.0.0.1:50051", "SandboxdReady", "sandboxd is ready", nil)
	}
	objects := append([]client.Object{tmpl}, environments...)
	reconciler := &WarmPoolReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(tmpl).WithObjects(objects...).Build(),
		Scheme: scheme,
	}

	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tmpl)}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var updated platformv1alpha1.EnvironmentTemplate
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(tmpl), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.WarmPoolReady != 2 {
		t.Fatalf("WarmPoolReady = %d, want observed count 2", updated.Status.WarmPoolReady)
	}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "warm-new"}, &platformv1alpha1.Environment{}); err == nil {
		t.Fatal("newest excess warm environment was not deleted")
	}
}

func TestWarmPoolReconcileExcludesClaimedEnvironment(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	tmpl := &platformv1alpha1.EnvironmentTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default", UID: "template-uid"},
		Spec:       platformv1alpha1.EnvironmentTemplateSpec{WarmPool: &platformv1alpha1.WarmPoolSpec{Min: 1}},
	}
	claimed := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "warm-claimed", Namespace: "default", Labels: map[string]string{warmPoolLabel: "small"}},
		Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
		Status: platformv1alpha1.EnvironmentStatus{
			Phase:     platformv1alpha1.EnvironmentPhaseReady,
			ClaimedBy: &platformv1alpha1.RunReference{Name: "run", UID: "run-uid"},
		},
	}
	setWarmPoolOwner(t, scheme, tmpl, claimed)
	reconciler := &WarmPoolReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(tmpl, claimed).WithObjects(tmpl, claimed).Build(),
		Scheme: scheme,
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tmpl)}); err != nil {
		t.Fatal(err)
	}
	var environments platformv1alpha1.EnvironmentList
	if err := reconciler.List(context.Background(), &environments, client.InNamespace("default")); err != nil {
		t.Fatal(err)
	}
	if len(environments.Items) != 2 {
		t.Fatalf("environments = %d, want claimed plus replacement", len(environments.Items))
	}
	var retained platformv1alpha1.Environment
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(claimed), &retained); err != nil || retained.Status.ClaimedBy == nil {
		t.Fatalf("claimed environment was removed or altered: %#v, %v", retained, err)
	}
}

func TestWarmPoolKeepsStaleGenerationFailure(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	tmpl := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default", UID: "template-uid"}, Spec: platformv1alpha1.EnvironmentTemplateSpec{
		WarmPool: &platformv1alpha1.WarmPoolSpec{Min: 1},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "warm-stale", Namespace: "default", Generation: 2, Labels: map[string]string{warmPoolLabel: "small"}}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: "small"}, Status: platformv1alpha1.EnvironmentStatus{
		ObservedGeneration: 1, Phase: platformv1alpha1.EnvironmentPhaseFailed,
	}}
	setWarmPoolOwner(t, scheme, tmpl, env)
	reconciler := &WarmPoolReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(tmpl).WithObjects(tmpl, env).Build(), Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tmpl)}); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(env), &platformv1alpha1.Environment{}); err != nil {
		t.Fatalf("stale-generation failed Environment was deleted: %v", err)
	}
}

func TestWarmPoolReconcileDeletesOnlyUnclaimedUnusableEnvironment(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	tmpl := &platformv1alpha1.EnvironmentTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default", UID: "template-uid"},
		Spec:       platformv1alpha1.EnvironmentTemplateSpec{WarmPool: &platformv1alpha1.WarmPoolSpec{Min: 1}},
	}
	failed := func(name string, claim *platformv1alpha1.RunReference) *platformv1alpha1.Environment {
		return &platformv1alpha1.Environment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name + "-uid"), Labels: map[string]string{warmPoolLabel: "small"}, Annotations: map[string]string{
				warmPoolCleanupAnnotation: time.Unix(1, 0).UTC().Format(time.RFC3339Nano),
			}},
			Spec:   platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
			Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseFailed, ClaimedBy: claim},
		}
	}
	unclaimed := failed("warm-unclaimed", nil)
	claimed := failed("warm-claimed", &platformv1alpha1.RunReference{Name: "run", UID: "run-uid"})
	setWarmPoolOwner(t, scheme, tmpl, unclaimed)
	setWarmPoolOwner(t, scheme, tmpl, claimed)
	reconciler := &WarmPoolReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(tmpl, unclaimed, claimed).WithObjects(tmpl, unclaimed, claimed).Build(),
		Scheme: scheme,
		Now:    func() time.Time { return time.Unix(1, 0).Add(warmPoolCleanupGrace) },
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tmpl)}); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(unclaimed), &platformv1alpha1.Environment{}); err == nil {
		t.Fatal("unclaimed failed environment was retained")
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(claimed), &platformv1alpha1.Environment{}); err != nil {
		t.Fatalf("claimed failed environment was deleted: %v", err)
	}
	var environments platformv1alpha1.EnvironmentList
	if err := reconciler.List(context.Background(), &environments, client.InNamespace("default"), client.MatchingLabels{warmPoolLabel: "small"}); err != nil {
		t.Fatal(err)
	}
	if len(environments.Items) != 2 {
		t.Fatalf("pool environments = %d, want claimed plus replacement", len(environments.Items))
	}
}

func TestWarmPoolCleanupGracePersistsAcrossRestart(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_000, 0)
	tmpl := &platformv1alpha1.EnvironmentTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default", UID: "template-uid"},
		Spec:       platformv1alpha1.EnvironmentTemplateSpec{WarmPool: &platformv1alpha1.WarmPoolSpec{Min: 1}},
	}
	failed := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "warm-failed", Namespace: "default", UID: "failed-uid", Labels: map[string]string{warmPoolLabel: tmpl.Name}},
		Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: tmpl.Name},
		Status:     platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseFailed},
	}
	setWarmPoolOwner(t, scheme, tmpl, failed)
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(tmpl, failed).WithObjects(tmpl, failed).Build()
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tmpl)}

	first := &WarmPoolReconciler{Client: baseClient, Scheme: scheme, Now: func() time.Time { return now }}
	result, err := first.Reconcile(context.Background(), request)
	if err != nil || result.RequeueAfter != warmPoolCleanupGrace {
		t.Fatalf("first Reconcile() = (%#v, %v), want cleanup grace", result, err)
	}
	var marked platformv1alpha1.Environment
	if err := baseClient.Get(context.Background(), client.ObjectKeyFromObject(failed), &marked); err != nil {
		t.Fatal(err)
	}
	if marked.Annotations[warmPoolCleanupAnnotation] != now.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("cleanup marker = %q", marked.Annotations[warmPoolCleanupAnnotation])
	}
	var environments platformv1alpha1.EnvironmentList
	if err := baseClient.List(context.Background(), &environments, client.MatchingLabels{warmPoolLabel: tmpl.Name}); err != nil || len(environments.Items) != 2 {
		t.Fatalf("replacement did not converge during grace: count %d, error %v", len(environments.Items), err)
	}

	restartedNow := now.Add(warmPoolCleanupGrace - time.Second)
	restarted := &WarmPoolReconciler{Client: baseClient, Scheme: scheme, Now: func() time.Time { return restartedNow }}
	result, err = restarted.Reconcile(context.Background(), request)
	if err != nil || result.RequeueAfter != time.Second {
		t.Fatalf("restarted Reconcile() = (%#v, %v), want one-second remainder", result, err)
	}
	restarted.Now = func() time.Time { return now.Add(warmPoolCleanupGrace) }
	if _, err := restarted.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := baseClient.Get(context.Background(), client.ObjectKeyFromObject(failed), &platformv1alpha1.Environment{}); !apierrors.IsNotFound(err) {
		t.Fatalf("failed member survived grace: %v", err)
	}
}

func TestWarmPoolCleanupGraceResetsFutureMarker(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_500, 0)
	tmpl := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default", UID: "template-uid"}}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "warm-failed", Namespace: "default", UID: "failed-uid", Labels: map[string]string{warmPoolLabel: tmpl.Name}, Annotations: map[string]string{
			warmPoolCleanupAnnotation: now.Add(24 * time.Hour).UTC().Format(time.RFC3339Nano),
		}},
		Spec:   platformv1alpha1.EnvironmentSpec{TemplateRef: tmpl.Name},
		Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseFailed},
	}
	setWarmPoolOwner(t, scheme, tmpl, env)
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(tmpl, env).WithObjects(tmpl, env).Build()
	r := &WarmPoolReconciler{Client: baseClient, Scheme: scheme, Now: func() time.Time { return now }}
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tmpl)})
	if err != nil || result.RequeueAfter != warmPoolCleanupGrace {
		t.Fatalf("Reconcile() = (%#v, %v), want bounded grace", result, err)
	}
	var marked platformv1alpha1.Environment
	if err := baseClient.Get(context.Background(), client.ObjectKeyFromObject(env), &marked); err != nil {
		t.Fatal(err)
	}
	if marked.Annotations[warmPoolCleanupAnnotation] != now.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("future cleanup marker was not reset: %q", marked.Annotations[warmPoolCleanupAnnotation])
	}
}

func TestWarmPoolCleansPausedMemberAfterGrace(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(2_000, 0)
	tmpl := &platformv1alpha1.EnvironmentTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default", UID: "template-uid"},
		Spec:       platformv1alpha1.EnvironmentTemplateSpec{WarmPool: &platformv1alpha1.WarmPoolSpec{Min: 1}},
	}
	paused := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "warm-paused", Namespace: "default", UID: "paused-uid", Labels: map[string]string{warmPoolLabel: tmpl.Name}, Annotations: map[string]string{
			warmPoolCleanupAnnotation: now.Add(-warmPoolCleanupGrace).UTC().Format(time.RFC3339Nano),
		}},
		Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: tmpl.Name, Paused: true},
	}
	setWarmPoolOwner(t, scheme, tmpl, paused)
	r := &WarmPoolReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(tmpl).WithObjects(tmpl, paused).Build(), Scheme: scheme, Now: func() time.Time { return now }}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tmpl)}); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(paused), &platformv1alpha1.Environment{}); !apierrors.IsNotFound(err) {
		t.Fatalf("paused member survived grace: %v", err)
	}
}

func TestWarmPoolTransientFailureRecoversDuringGrace(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(2_500, 0)
	tmpl := &platformv1alpha1.EnvironmentTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default", UID: "template-uid"},
		Spec:       platformv1alpha1.EnvironmentTemplateSpec{WarmPool: &platformv1alpha1.WarmPoolSpec{Min: 1}},
	}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "warm-recovering", Namespace: "default", UID: "recovering-uid", Labels: map[string]string{warmPoolLabel: tmpl.Name}},
		Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: tmpl.Name},
		Status:     platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseFailed},
	}
	setWarmPoolOwner(t, scheme, tmpl, env)
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(tmpl, env).WithObjects(tmpl, env).Build()
	r := &WarmPoolReconciler{Client: baseClient, Scheme: scheme, Now: func() time.Time { return now }}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tmpl)}
	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	var members platformv1alpha1.EnvironmentList
	if err := baseClient.List(context.Background(), &members, client.MatchingLabels{warmPoolLabel: tmpl.Name}); err != nil {
		t.Fatal(err)
	}
	for i := range members.Items {
		if members.Items[i].Name != env.Name {
			if err := baseClient.Delete(context.Background(), &members.Items[i]); err != nil {
				t.Fatal(err)
			}
		}
	}
	var recovering platformv1alpha1.Environment
	if err := baseClient.Get(context.Background(), client.ObjectKeyFromObject(env), &recovering); err != nil {
		t.Fatal(err)
	}
	applyEnvironmentStatus(&recovering, platformv1alpha1.EnvironmentPhaseReady, "pod", "10.0.0.1:50051", "SandboxdReady", "ready", nil)
	if err := baseClient.Status().Update(context.Background(), &recovering); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := baseClient.Get(context.Background(), client.ObjectKeyFromObject(env), &recovering); err != nil {
		t.Fatal(err)
	}
	if _, marked := recovering.Annotations[warmPoolCleanupAnnotation]; marked || !platformv1alpha1.IsEnvironmentReady(&recovering) {
		t.Fatalf("recovered member retained cleanup marker or readiness was lost: %#v", recovering)
	}
}

func TestWarmPoolDeletingAndForeignMembersDoNotCount(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	tmpl := &platformv1alpha1.EnvironmentTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default", UID: "template-uid"},
		Spec:       platformv1alpha1.EnvironmentTemplateSpec{WarmPool: &platformv1alpha1.WarmPoolSpec{Min: 1}},
	}
	deletingAt := metav1.Now()
	deleting := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "warm-deleting", Namespace: "default", UID: "deleting-uid", Labels: map[string]string{warmPoolLabel: tmpl.Name}, DeletionTimestamp: &deletingAt, Finalizers: []string{"test"}},
		Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: tmpl.Name},
	}
	applyEnvironmentStatus(deleting, platformv1alpha1.EnvironmentPhaseReady, "pod", "10.0.0.1:50051", "SandboxdReady", "ready", nil)
	setWarmPoolOwner(t, scheme, tmpl, deleting)
	foreign := deleting.DeepCopy()
	foreign.Name = "warm-foreign"
	foreign.UID = "foreign-uid"
	foreign.DeletionTimestamp = nil
	foreign.Finalizers = nil
	foreign.OwnerReferences = nil
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(tmpl).WithObjects(tmpl, deleting, foreign).Build()
	r := &WarmPoolReconciler{Client: baseClient, Scheme: scheme}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tmpl)}); err != nil {
		t.Fatal(err)
	}
	var updated platformv1alpha1.EnvironmentTemplate
	if err := baseClient.Get(context.Background(), client.ObjectKeyFromObject(tmpl), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.WarmPoolReady != 0 {
		t.Fatalf("WarmPoolReady = %d, want deleting and foreign excluded", updated.Status.WarmPoolReady)
	}
	var environments platformv1alpha1.EnvironmentList
	if err := baseClient.List(context.Background(), &environments); err != nil || len(environments.Items) != 3 {
		t.Fatalf("members = %d, want deleting, foreign, and replacement: %v", len(environments.Items), err)
	}
}

func TestWarmPoolCleanupIsFencedByConcurrentClaimOrPromotion(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*platformv1alpha1.Environment)
	}{
		{name: "claim", mutate: func(env *platformv1alpha1.Environment) {
			env.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: "run", UID: "run-uid"}
		}},
		{name: "promotion", mutate: func(env *platformv1alpha1.Environment) {
			delete(env.Labels, warmPoolLabel)
			env.OwnerReferences = nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := platformv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			now := time.Unix(3_000, 0)
			tmpl := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "default", UID: "template-uid"}}
			env := &platformv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{Name: "warm-failed", Namespace: "default", UID: "env-uid", Labels: map[string]string{warmPoolLabel: tmpl.Name}, Annotations: map[string]string{
					warmPoolCleanupAnnotation: now.Add(-warmPoolCleanupGrace).UTC().Format(time.RFC3339Nano),
				}},
				Spec:   platformv1alpha1.EnvironmentSpec{TemplateRef: tmpl.Name},
				Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseFailed},
			}
			setWarmPoolOwner(t, scheme, tmpl, env)
			baseClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(tmpl, env).WithObjects(tmpl, env).Build()
			intercepted := interceptor.NewClient(baseClient, interceptor.Funcs{
				Delete: func(ctx context.Context, underlying client.WithWatch, object client.Object, options ...client.DeleteOption) error {
					deleteOptions := (&client.DeleteOptions{}).ApplyOptions(options)
					if deleteOptions.Preconditions == nil || deleteOptions.Preconditions.UID == nil || *deleteOptions.Preconditions.UID != object.GetUID() ||
						deleteOptions.Preconditions.ResourceVersion == nil || *deleteOptions.Preconditions.ResourceVersion != object.GetResourceVersion() {
						t.Fatalf("delete preconditions = %#v, want UID %q and resourceVersion %q", deleteOptions.Preconditions, object.GetUID(), object.GetResourceVersion())
					}
					var current platformv1alpha1.Environment
					if err := underlying.Get(ctx, client.ObjectKeyFromObject(object), &current); err != nil {
						return err
					}
					test.mutate(&current)
					if test.name == "claim" {
						if err := underlying.Status().Update(ctx, &current); err != nil {
							return err
						}
					} else if err := underlying.Update(ctx, &current); err != nil {
						return err
					}
					return apierrors.NewConflict(schema.GroupResource{Group: platformv1alpha1.GroupVersion.Group, Resource: "environments"}, object.GetName(), errors.New("concurrent allocation"))
				},
			})
			r := &WarmPoolReconciler{Client: intercepted, Scheme: scheme, Now: func() time.Time { return now }}
			result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tmpl)})
			if err != nil || !result.Requeue {
				t.Fatalf("Reconcile() = (%#v, %v), want conflict requeue", result, err)
			}
			if err := baseClient.Get(context.Background(), client.ObjectKeyFromObject(env), &platformv1alpha1.Environment{}); err != nil {
				t.Fatalf("concurrently allocated member was deleted: %v", err)
			}
		})
	}
}

func setWarmPoolOwner(t *testing.T, scheme *runtime.Scheme, tmpl *platformv1alpha1.EnvironmentTemplate, env *platformv1alpha1.Environment) {
	t.Helper()
	if err := controllerutil.SetControllerReference(tmpl, env, scheme, controllerutil.WithBlockOwnerDeletion(false)); err != nil {
		t.Fatal(err)
	}
}
