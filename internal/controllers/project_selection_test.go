package controllers

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	platform "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
)

func TestExactProjectSelectionBeforeCredentialsAndAllocation(t *testing.T) {
	for _, mode := range []string{"replacement", "missing", "reader-error", "explicit-template", "reclaimed-namespace"} {
		t.Run(mode, func(t *testing.T) {
			run := &platform.Run{ObjectMeta: metav1.ObjectMeta{Name: "task", Namespace: "ns", UID: "run-A"}, Spec: platform.RunSpec{ProjectRef: "repo", ProjectUID: "selected-A", Agent: "test", CredentialProfileRef: "must-not-resolve", RepositoryCredential: platform.RepositoryCredentialGitHubApp}}
			if mode == "explicit-template" {
				run.Spec.TemplateRef = "override"
			}
			project := &platform.Project{ObjectMeta: metav1.ObjectMeta{Name: "repo", Namespace: "ns", UID: "replacement-B"}, Spec: platform.ProjectSpec{TemplateRef: "replacement-default"}}
			adapter := &scriptedAdapter{}
			r := reconciler(t, adapter, run)
			if mode != "missing" {
				if err := r.Create(context.Background(), project); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "reclaimed-namespace" {
				installation := &platform.Installation{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "installation"}}
				namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns", UID: "namespace", Annotations: map[string]string{
					tenancy.InstallationNamespaceAnnotation: "system", tenancy.InstallationNameAnnotation: "main", tenancy.InstallationUIDAnnotation: "installation",
					tenancy.ProjectNameAnnotation: project.Name, tenancy.ProjectUIDAnnotation: string(project.UID), tenancy.LifecycleAnnotation: string(tenancy.LifecycleActive),
				}}}
				for _, obj := range []client.Object{installation, namespace} {
					if err := r.Create(context.Background(), obj); err != nil {
						t.Fatal(err)
					}
				}
				verifier := &tenancy.Verifier{Reader: r.Client, Installation: tenancy.InstallationIdentity{Key: client.ObjectKeyFromObject(installation), UID: installation.UID}, Mode: tenancy.ModeScoped}
				r.Scope = &tenancy.ReconcileScope{Verifier: verifier}
			}
			forbiddenReads := 0
			r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				switch obj.(type) {
				case *platform.AgentCredentialProfile, *corev1.Secret:
					forbiddenReads++
				}
				if _, ok := obj.(*platform.Project); ok && mode == "reader-error" {
					return errors.New("temporary read failure")
				}
				return c.Get(ctx, key, obj, opts...)
			}})
			if r.Scope != nil {
				r.Client = tenancy.GuardedClient{Client: r.Client, Verifier: r.Scope.Verifier}
			}
			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
			if (err != nil) != (mode == "reader-error") {
				t.Fatalf("reconcile: %v", err)
			}
			var got platform.Run
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &got); err != nil {
				t.Fatal(err)
			}
			if mode != "reader-error" && got.Status.State != platform.RunStateFailed {
				t.Fatalf("state %s", got.Status.State)
			}
			var envs platform.EnvironmentList
			if err := r.List(context.Background(), &envs); err != nil {
				t.Fatal(err)
			}
			if len(envs.Items) != 0 || got.Status.CredentialProfileRef != nil || adapter.accepted != 0 || forbiddenReads != 0 {
				t.Fatalf("selection denial allowed side effects: envs=%d reads=%d accepted=%d", len(envs.Items), forbiddenReads, adapter.accepted)
			}
		})
	}
}

func TestExactProjectDefaultUsesAuthoritativeProject(t *testing.T) {
	for _, override := range []string{"", "explicit"} {
		t.Run("override-"+override, func(t *testing.T) {
			run := &platform.Run{ObjectMeta: metav1.ObjectMeta{Name: "task", Namespace: "ns", UID: "run-A"}, Spec: platform.RunSpec{ProjectRef: "repo", ProjectUID: "A", TemplateRef: override, Agent: "test"}}
			stale := &platform.Project{ObjectMeta: metav1.ObjectMeta{Name: "repo", Namespace: "ns", UID: "A", Generation: 1}, Spec: platform.ProjectSpec{TemplateRef: "stale-default"}}
			r := reconciler(t, &scriptedAdapter{}, run, stale)
			current := stale.DeepCopy()
			current.Spec.TemplateRef = "current-default"
			current.Generation = 2
			r.APIReader = fake.NewClientBuilder().WithScheme(r.Scheme).WithObjects(current).Build()
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
				t.Fatal(err)
			}
			var env platform.Environment
			if err := r.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "run-run-A"}, &env); err != nil {
				t.Fatal(err)
			}
			want := "current-default"
			if override != "" {
				want = override
			}
			if env.Spec.TemplateRef != want || env.Spec.ProjectUID != "A" {
				t.Fatalf("allocation = %#v", env.Spec)
			}
		})
	}
}

func TestExactProjectWarmRecoveryNeverRetrofitsLegacyBinding(t *testing.T) {
	for _, mode := range []string{"legacy-bound", "wrong-bound", "unpromoted-missing", "unpromoted-current"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			run := &platform.Run{ObjectMeta: metav1.ObjectMeta{Name: "task", Namespace: "ns", UID: "run-A"}, Spec: platform.RunSpec{ProjectRef: "repo", ProjectUID: "selected-A", Agent: "test"}}
			tmpl := &platform.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "ns", UID: "template-A", Generation: 1}, Spec: platform.EnvironmentTemplateSpec{Image: "image", Size: "small"}}
			env := &platform.Environment{ObjectMeta: metav1.ObjectMeta{Name: "claimed", Namespace: "ns", UID: "env-A"}, Spec: platform.EnvironmentSpec{TemplateRef: "small", ProjectRef: "repo"}, Status: platform.EnvironmentStatus{Phase: platform.EnvironmentPhaseSetup, ClaimedBy: &platform.RunReference{Name: run.Name, UID: run.UID}}}
			if mode == "wrong-bound" {
				env.Spec.ProjectUID = "replacement-B"
			}
			if mode == "unpromoted-missing" || mode == "unpromoted-current" {
				env.Spec.ProjectRef = ""
				env.Labels = map[string]string{warmPoolLabel: "small"}
				env.OwnerReferences = []metav1.OwnerReference{{APIVersion: platform.GroupVersion.String(), Kind: "EnvironmentTemplate", Name: "small", UID: tmpl.UID, Controller: ptr(true)}}
				setTestProvisioningSnapshot(env, tmpl, nil)
			}
			objects := []client.Object{run, tmpl, env}
			if mode != "unpromoted-missing" {
				objects = append(objects, &platform.Project{ObjectMeta: metav1.ObjectMeta{Name: "repo", Namespace: "ns", UID: "selected-A"}, Spec: platform.ProjectSpec{TemplateRef: "changed-default"}})
			}
			r := reconciler(t, &scriptedAdapter{}, objects...)
			if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
				t.Fatal(err)
			}
			var got platform.Environment
			var result platform.Run
			if err := r.Get(ctx, client.ObjectKeyFromObject(env), &got); err != nil {
				t.Fatal(err)
			}
			if err := r.Get(ctx, client.ObjectKeyFromObject(run), &result); err != nil {
				t.Fatal(err)
			}
			if mode == "unpromoted-current" {
				if got.Spec.ProjectUID != "selected-A" || got.Spec.TemplateRef != "small" || result.Status.EnvironmentRef == nil || result.Status.EnvironmentRef.UID != env.UID {
					t.Fatal("warm claim lost its allocation-time template")
				}
			} else {
				if result.Status.State != platform.RunStateFailed {
					t.Fatalf("selection not rejected: %#v", result.Status)
				}
				if got.Spec.ProjectUID != env.Spec.ProjectUID || got.Spec.ProjectRef != env.Spec.ProjectRef {
					t.Fatal("bound Environment was retrofitted")
				}
				if mode == "unpromoted-missing" && got.Status.ClaimedBy != nil {
					t.Fatal("unused warm claim leaked")
				}
				if mode != "unpromoted-missing" && (result.Status.EnvironmentRef == nil || result.Status.EnvironmentRef.UID != env.UID) {
					t.Fatal("lost exact cleanup authority")
				}
			}
		})
	}
}

func TestExactProjectAllocationRecoveryAndReplacement(t *testing.T) {
	for _, warm := range []bool{false, true} {
		for _, replace := range []bool{false, true} {
			name := "cold"
			if warm {
				name = "warm"
			}
			if replace {
				name += "-replacement"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				run := &platform.Run{ObjectMeta: metav1.ObjectMeta{Name: "task", Namespace: "ns", UID: "run-A"}, Spec: platform.RunSpec{ProjectRef: "repo", ProjectUID: "selected-A", Agent: "test"}}
				project := &platform.Project{ObjectMeta: metav1.ObjectMeta{Name: "repo", Namespace: "ns", UID: "selected-A", Generation: 1}, Spec: platform.ProjectSpec{TemplateRef: "small", Repositories: []string{"https://selected.test/repo"}}}
				tmpl := &platform.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "ns", UID: "template-A", Generation: 1}, Spec: platform.EnvironmentTemplateSpec{Image: "image", Size: "small"}}
				objects := []client.Object{run, project, tmpl}
				if warm {
					objects = append(objects, &platform.Environment{ObjectMeta: metav1.ObjectMeta{Name: "warm", Namespace: "ns", UID: "warm-A", Labels: map[string]string{warmPoolLabel: "small"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: platform.GroupVersion.String(), Kind: "EnvironmentTemplate", Name: "small", UID: tmpl.UID, Controller: ptr(true)}}}, Spec: platform.EnvironmentSpec{TemplateRef: "small"}, Status: platform.EnvironmentStatus{Phase: platform.EnvironmentPhaseReady}})
				}
				r := reconciler(t, &scriptedAdapter{}, objects...)
				base := r.Client
				swapped := false
				replaceProject := func() {
					if !replace || swapped {
						return
					}
					swapped = true
					if err := base.Delete(ctx, project); err != nil {
						t.Fatal(err)
					}
					project.ResourceVersion = ""
					project.UID = "replacement-B"
					project.Spec.TemplateRef = "different"
					project.Spec.Repositories = []string{"https://replacement.test/repo"}
					if err := base.Create(ctx, project); err != nil {
						t.Fatal(err)
					}
				}
				// Replace after selection, at the cold create / warm atomic patch.
				r.Client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
					Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
						if e, ok := obj.(*platform.Environment); ok {
							replaceProject()
							e.UID = "cold-A"
						}
						return c.Create(ctx, obj, opts...)
					},
					Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
						if _, ok := obj.(*platform.Environment); ok {
							replaceProject()
						}
						return c.Patch(ctx, obj, p, opts...)
					},
				})
				if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
					t.Fatal(err)
				}
				var allocated platform.Run
				if err := base.Get(ctx, client.ObjectKeyFromObject(run), &allocated); err != nil {
					t.Fatal(err)
				}
				if allocated.Status.EnvironmentRef == nil {
					t.Fatalf("no allocation: %#v", allocated.Status)
				}
				var env platform.Environment
				if err := base.Get(ctx, types.NamespacedName{Namespace: "ns", Name: allocated.Status.EnvironmentRef.Name}, &env); err != nil {
					t.Fatal(err)
				}
				if env.Spec.ProjectRef != "repo" || env.Spec.ProjectUID != "selected-A" || env.Spec.TemplateRef != "small" {
					t.Fatalf("retargeted intent: %#v", env.Spec)
				}
				if replace {
					if err := networkingv1.AddToScheme(r.Scheme); err != nil {
						t.Fatal(err)
					}
					er := &EnvironmentReconciler{Client: base, Scheme: r.Scheme}
					for range 5 {
						if _, err := er.Reconcile(ctx, provisioningRequest(&env)); err != nil {
							t.Fatal(err)
						}
					}
					if err := base.Get(ctx, client.ObjectKeyFromObject(&env), &env); err != nil {
						t.Fatal(err)
					}
					if env.Status.Provisioning != nil && env.Status.Provisioning.Project != nil {
						t.Fatal("replacement captured")
					}
					var pods corev1.PodList
					var secrets corev1.SecretList
					var pvcs corev1.PersistentVolumeClaimList
					if err := base.List(ctx, &pods); err != nil {
						t.Fatal(err)
					}
					if err := base.List(ctx, &secrets); err != nil {
						t.Fatal(err)
					}
					if err := base.List(ctx, &pvcs); err != nil {
						t.Fatal(err)
					}
					if len(pods.Items)+len(secrets.Items)+len(pvcs.Items) != 0 {
						t.Fatal("replacement caused provisioning effects")
					}
					return
				}
				// A changed default after allocation cannot change recovered intent.
				if err := base.Get(ctx, client.ObjectKeyFromObject(project), project); err != nil {
					t.Fatal(err)
				}
				project.Spec.TemplateRef = "new-default"
				if err := base.Update(ctx, project); err != nil {
					t.Fatal(err)
				}
				// Lose the whole atomic allocation status write, not just one field.
				allocated.Status = platform.RunStatus{}
				if err := base.Status().Update(ctx, &allocated); err != nil {
					t.Fatal(err)
				}
				r.Client = base
				if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
					t.Fatal(err)
				}
				if err := base.Get(ctx, client.ObjectKeyFromObject(run), &allocated); err != nil {
					t.Fatal(err)
				}
				if allocated.Status.EnvironmentRef == nil || allocated.Status.EnvironmentRef.UID != env.UID {
					t.Fatalf("allocation was not recovered: %#v, env UID %s", allocated.Status, env.UID)
				}
				if err := base.Get(ctx, client.ObjectKeyFromObject(&env), &env); err != nil {
					t.Fatal(err)
				}
				if env.Spec.TemplateRef != "small" {
					t.Fatal("recovery re-resolved default")
				}
			})
		}
	}
}
