package tenancy

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

func tenancyScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func fixture(l Lifecycle) (*platformv1alpha1.Installation, *corev1.Namespace, *platformv1alpha1.Project) {
	i := &platformv1alpha1.Installation{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "i-1"}}
	operation := ""
	if l == LifecycleFencing {
		operation = OperationOffboarding
	}
	n := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team", UID: "n-1", Annotations: map[string]string{
		InstallationNamespaceAnnotation: "system", InstallationNameAnnotation: "main", InstallationUIDAnnotation: "i-1",
		ProjectNameAnnotation: "app", ProjectUIDAnnotation: "p-1", LifecycleAnnotation: string(l), LifecycleOperationAnnotation: operation,
	}}}
	p := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "team", UID: "p-1"}}
	return i, n, p
}

func verifier(t *testing.T, objects ...client.Object) (Verifier, client.Client) {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(tenancyScheme(t)).WithObjects(objects...).Build()
	return Verifier{Reader: c, Installation: InstallationIdentity{Key: types.NamespacedName{Namespace: "system", Name: "main"}, UID: "i-1"}, Mode: ModeScoped}, c
}

func TestVerifierExactClaimAndFailureFences(t *testing.T) {
	ctx := context.Background()
	i, n, p := fixture(LifecycleActive)
	v, _ := verifier(t, i, n, p)
	claim, err := v.VerifyNamespace(ctx, "team")
	if err != nil || claim.NamespaceUID != "n-1" || claim.ProjectUID != "p-1" {
		t.Fatalf("exact claim: %#v, %v", claim, err)
	}

	tests := []struct {
		name   string
		mutate func(*platformv1alpha1.Installation, *corev1.Namespace, *platformv1alpha1.Project)
		extra  bool
	}{
		{"installation UID", func(i *platformv1alpha1.Installation, _ *corev1.Namespace, _ *platformv1alpha1.Project) {
			i.UID = "i-2"
		}, false},
		{"stale project UID", func(_ *platformv1alpha1.Installation, n *corev1.Namespace, _ *platformv1alpha1.Project) {
			n.Annotations[ProjectUIDAnnotation] = "old"
		}, false},
		{"invalid lifecycle", func(_ *platformv1alpha1.Installation, n *corev1.Namespace, _ *platformv1alpha1.Project) {
			n.Annotations[LifecycleAnnotation] = "mystery"
		}, false},
		{"invalid fencing operation", func(_ *platformv1alpha1.Installation, n *corev1.Namespace, _ *platformv1alpha1.Project) {
			n.Annotations[LifecycleAnnotation] = string(LifecycleFencing)
			n.Annotations[LifecycleOperationAnnotation] = "mystery"
		}, false},
		{"operation outside fencing", func(_ *platformv1alpha1.Installation, n *corev1.Namespace, _ *platformv1alpha1.Project) {
			n.Annotations[LifecycleOperationAnnotation] = OperationOnboarding
		}, false},
		{"second project", func(*platformv1alpha1.Installation, *corev1.Namespace, *platformv1alpha1.Project) {}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i, n, p := fixture(LifecycleActive)
			tt.mutate(i, n, p)
			objs := []client.Object{i, n, p}
			if tt.extra {
				objs = append(objs, &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "team", UID: "p-2"}})
			}
			v, _ := verifier(t, objs...)
			if _, err := v.VerifyNamespace(ctx, "team"); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
	for _, l := range []Lifecycle{LifecycleFencing, LifecycleFenced} {
		t.Run(string(l), func(t *testing.T) {
			i, n, p := fixture(l)
			v, _ := verifier(t, i, n, p)
			claim, err := v.VerifyNamespace(ctx, "team")
			if err != nil || claim.Lifecycle != l {
				t.Fatalf("lifecycle should verify for explicit callers: %v", err)
			}
			if err = v.ValidateConfiguredNamespaces(ctx, []string{"team"}); err != nil {
				t.Fatalf("configured transition must survive restart: %v", err)
			}
		})
	}
	t.Run("interrupted onboarding transition", func(t *testing.T) {
		i, n, p := fixture(LifecycleFencing)
		n.Annotations[LifecycleOperationAnnotation] = OperationOnboarding
		v, _ := verifier(t, i, n, p)
		claim, err := v.VerifyNamespace(ctx, "team")
		if err != nil || claim.Operation != OperationOnboarding {
			t.Fatalf("onboarding transition = %#v, %v", claim, err)
		}
		if err := v.ValidateConfiguredNamespaces(ctx, []string{"team"}); err != nil {
			t.Fatalf("configured onboarding transition must start fail-closed: %v", err)
		}
	})
}

func TestTrustedAdminIsExplicit(t *testing.T) {
	i, n, p := fixture(LifecycleActive)
	v, c := verifier(t, i, n, p)
	v.Mode = ModeTrustedAdmin
	claim, err := v.VerifyNamespace(context.Background(), "team")
	if err != nil || claim.ProjectUID != "p-1" || claim.Lifecycle != LifecycleActive {
		t.Fatalf("trusted admin: %#v %v", claim, err)
	}
	var namespace corev1.Namespace
	if err := c.Get(context.Background(), client.ObjectKey{Name: "team"}, &namespace); err != nil {
		t.Fatal(err)
	}
	namespace.Annotations = nil
	if err := c.Update(context.Background(), &namespace); err != nil {
		t.Fatal(err)
	}
	if _, err := v.VerifyNamespace(context.Background(), "team"); err == nil {
		t.Fatal("trusted admin accepted a Namespace without this Installation's exact claim")
	}
}

func TestValidateConfiguredNamespaces(t *testing.T) {
	i, n, p := fixture(LifecycleActive)
	v, _ := verifier(t, i, n, p)
	for name, names := range map[string][]string{"system": {"system"}, "duplicate": {"team", "team"}, "blank": {" "}} {
		t.Run(name, func(t *testing.T) {
			if err := v.ValidateConfiguredNamespaces(context.Background(), names); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
	i2, n2, p2 := fixture(LifecycleActive)
	n2.Annotations[InstallationUIDAnnotation] = "stale"
	v2, _ := verifier(t, i2, n2, p2)
	if err := v2.ValidateConfiguredNamespaces(context.Background(), []string{"team"}); err == nil {
		t.Fatal("stale accepted")
	}
	i3, n3, p3 := fixture(LifecycleActive)
	delete(n3.Annotations, ProjectUIDAnnotation)
	v3, _ := verifier(t, i3, n3, p3)
	if err := v3.ValidateConfiguredNamespaces(context.Background(), []string{"team"}); err == nil {
		t.Fatal("unclaimed accepted")
	}
}

func TestGuardedClientLeaseAndClaimChanges(t *testing.T) {
	i, n, p := fixture(LifecycleActive)
	v, c := verifier(t, i, n, p)
	scope := ReconcileScope{Verifier: &v}
	leased, _, err := scope.Begin(context.Background(), "team", LifecycleActive)
	if err != nil {
		t.Fatal(err)
	}
	g := GuardedClient{Client: c, Verifier: &v}
	if err = g.Create(leased, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "ok", Namespace: "team"}}); err != nil {
		t.Fatal(err)
	}
	if err = g.Create(context.Background(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "no", Namespace: "team"}}); err == nil {
		t.Fatal("no lease accepted")
	}
	if err = g.Create(leased, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cross", Namespace: "other"}}); err == nil {
		t.Fatal("cross namespace accepted")
	}
	var current corev1.Namespace
	_ = c.Get(context.Background(), client.ObjectKey{Name: "team"}, &current)
	current.Annotations[LifecycleAnnotation] = string(LifecycleFencing)
	_ = c.Update(context.Background(), &current)
	if err = g.Create(leased, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "flip", Namespace: "team"}}); !errors.Is(err, ErrOutOfScope) {
		t.Fatalf("state flip: %v", err)
	}
	// The fake client allows replacing UID, so exercise the exact namespace-identity fence directly.
	current.UID = "n-2"
	current.Annotations[LifecycleAnnotation] = string(LifecycleActive)
	_ = c.Update(context.Background(), &current)
	if err = g.Create(leased, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "uid", Namespace: "team"}}); !errors.Is(err, ErrOutOfScope) {
		t.Fatalf("UID replacement: %v", err)
	}
}

func TestGuardedClientLeaseBindsCompleteClaim(t *testing.T) {
	tests := []struct {
		name      string
		lifecycle Lifecycle
		mutate    func(client.Client, *corev1.Namespace, *platformv1alpha1.Project)
	}{
		{
			name:      "project name and UID",
			lifecycle: LifecycleActive,
			mutate: func(c client.Client, namespace *corev1.Namespace, project *platformv1alpha1.Project) {
				if err := c.Delete(context.Background(), project); err != nil {
					t.Fatal(err)
				}
				replacement := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "replacement", Namespace: "team", UID: "p-2"}}
				if err := c.Create(context.Background(), replacement); err != nil {
					t.Fatal(err)
				}
				namespace.Annotations[ProjectNameAnnotation] = replacement.Name
				namespace.Annotations[ProjectUIDAnnotation] = string(replacement.UID)
				if err := c.Update(context.Background(), namespace); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:      "fencing operation",
			lifecycle: LifecycleFencing,
			mutate: func(c client.Client, namespace *corev1.Namespace, _ *platformv1alpha1.Project) {
				namespace.Annotations[LifecycleOperationAnnotation] = OperationOnboarding
				if err := c.Update(context.Background(), namespace); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			i, namespace, project := fixture(test.lifecycle)
			v, c := verifier(t, i, namespace, project)
			leased, _, err := (&ReconcileScope{Verifier: &v}).Begin(context.Background(), namespace.Name, test.lifecycle)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(c, namespace, project)
			guarded := GuardedClient{Client: c, Verifier: &v}
			err = guarded.Create(leased, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "stale-lease", Namespace: namespace.Name}})
			if !errors.Is(err, ErrOutOfScope) {
				t.Fatalf("changed claim accepted: %v", err)
			}
		})
	}
}

func TestValidateManagedTemplateExactOwnership(t *testing.T) {
	identity := InstallationIdentity{Key: types.NamespacedName{Namespace: "system", Name: "main"}, UID: "i-1"}
	claim := Claim{NamespaceUID: "n-1", ProjectName: "app", ProjectUID: "p-1", Lifecycle: LifecycleActive}
	template := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "team", Annotations: map[string]string{
		InstallationNamespaceAnnotation: "system",
		InstallationNameAnnotation:      "main",
		InstallationUIDAnnotation:       "i-1",
		ProjectNameAnnotation:           "app",
		ProjectUIDAnnotation:            "p-1",
		CatalogNameAnnotation:           "small",
		CatalogRevisionAnnotation:       "revision",
		CatalogSourceUIDAnnotation:      "source-uid",
	}}}
	if err := ValidateManagedTemplate(template, identity, claim); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(map[string]string){
		"installation": func(a map[string]string) { a[InstallationUIDAnnotation] = "old" },
		"project":      func(a map[string]string) { a[ProjectUIDAnnotation] = "old" },
		"source":       func(a map[string]string) { delete(a, CatalogSourceUIDAnnotation) },
		"revision":     func(a map[string]string) { delete(a, CatalogRevisionAnnotation) },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := template.DeepCopy()
			mutate(candidate.Annotations)
			if err := ValidateManagedTemplate(candidate, identity, claim); !errors.Is(err, ErrOutOfScope) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
