package controllers

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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
		if env.Spec.TemplateRef != "small" || env.Spec.Backend != platformv1alpha1.EnvironmentBackendPod {
			t.Errorf("environment spec = %#v, want small pod template", env.Spec)
		}
		if !metav1.IsControlledBy(&env, tmpl) {
			t.Errorf("environment %q is not controlled by template", env.Name)
		}
		if env.OwnerReferences[0].BlockOwnerDeletion == nil || *env.OwnerReferences[0].BlockOwnerDeletion {
			t.Errorf("environment %q blocks template deletion", env.Name)
		}
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
			ObjectMeta: metav1.ObjectMeta{Name: "warm-old", Namespace: "default", Labels: map[string]string{warmPoolLabel: "small"}, CreationTimestamp: metav1.Unix(1, 0)},
			Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
			Status:     platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady},
		},
		&platformv1alpha1.Environment{
			ObjectMeta: metav1.ObjectMeta{Name: "warm-new", Namespace: "default", Labels: map[string]string{warmPoolLabel: "small"}, CreationTimestamp: metav1.Unix(2, 0)},
			Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
			Status:     platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady},
		},
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
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: map[string]string{warmPoolLabel: "small"}},
			Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
			Status:     platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseFailed, ClaimedBy: claim},
		}
	}
	unclaimed := failed("warm-unclaimed", nil)
	claimed := failed("warm-claimed", &platformv1alpha1.RunReference{Name: "run", UID: "run-uid"})
	reconciler := &WarmPoolReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(tmpl, unclaimed, claimed).WithObjects(tmpl, unclaimed, claimed).Build(),
		Scheme: scheme,
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
