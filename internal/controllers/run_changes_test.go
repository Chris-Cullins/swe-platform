package controllers

import (
	"context"
	"errors"
	"testing"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type changesSinkFunc func(context.Context, string, string, string, controlplane.CaptureChangesRequest) error

type changesClaimReadFailure struct{ client.Reader }

func (r changesClaimReadFailure) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Namespace); ok {
		return context.DeadlineExceeded
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestChangesOffboardingFinalCleanupAcrossFencedTransition(t *testing.T) {
	for _, transition := range []string{"fencing", "before-capture", "after-capture", "foreign-claim"} {
		t.Run(transition, func(t *testing.T) {
			run, env := adapterRaceObjects(platformv1alpha1.EnvironmentOwnershipClaimed, platformv1alpha1.RunStateSucceeded)
			run.Spec.RepositoryCredential = "github-app"
			env.Spec.Paused = true
			env.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
			i := &platformv1alpha1.Installation{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "i"}}
			p := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "ns", UID: "p"}}
			n := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns", UID: "n", Annotations: map[string]string{tenancy.InstallationNamespaceAnnotation: "system", tenancy.InstallationNameAnnotation: "main", tenancy.InstallationUIDAnnotation: "i", tenancy.ProjectNameAnnotation: "project", tenancy.ProjectUIDAnnotation: "p", tenancy.LifecycleAnnotation: string(tenancy.LifecycleFencing), tenancy.LifecycleOperationAnnotation: tenancy.OperationOffboarding}}}
			r := reconciler(t, &scriptedAdapter{}, run, env, i, p, n)
			base := r.Client
			v := &tenancy.Verifier{Reader: base, Installation: tenancy.InstallationIdentity{Key: types.NamespacedName{Namespace: "system", Name: "main"}, UID: "i"}, Mode: tenancy.ModeScoped}
			r.Scope = &tenancy.ReconcileScope{Verifier: v}
			r.Client = tenancy.GuardedClient{Client: base, Verifier: v}
			r.Changes = changesSinkFunc(func(context.Context, string, string, string, controlplane.CaptureChangesRequest) error {
				t.Fatal("offboarding attempted denied capture")
				return nil
			})
			ctx, _, err := r.Scope.Begin(context.Background(), "ns", tenancy.LifecycleFencing)
			if err != nil {
				t.Fatal(err)
			}
			v.Reader = changesClaimReadFailure{Reader: base}
			if err := r.captureChanges(ctx, run, env, false, true); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("uncertain claim allowed skip: %v", err)
			}
			v.Reader = base
			if err := r.captureChanges(ctx, run, env, true, false); err == nil {
				t.Fatal("offboarding baseline admitted")
			}
			run.Status.State = platformv1alpha1.RunStateRunning
			if err := r.captureChanges(ctx, run, env, false, true); err == nil {
				t.Fatal("nonterminal skip admitted")
			}
			run.Status.State = platformv1alpha1.RunStateSucceeded
			if err := r.captureChanges(context.Background(), run, env, false, true); err == nil {
				t.Fatal("unproven skip admitted")
			}
			if transition != "fencing" {
				if transition == "after-capture" {
					if err := r.captureChanges(ctx, run, env, false, true); err != nil {
						t.Fatal(err)
					}
				}
				n.Annotations[tenancy.LifecycleAnnotation] = string(tenancy.LifecycleFenced)
				n.Annotations[tenancy.LifecycleOperationAnnotation] = ""
				if transition == "foreign-claim" {
					n.Annotations[tenancy.ProjectUIDAnnotation] = "replacement"
				}
				if err := base.Update(context.Background(), n); err != nil {
					t.Fatal(err)
				}
				if _, err := r.Scope.Revalidate(ctx, run.Namespace, tenancy.LifecycleFencing, tenancy.LifecycleFenced); !errors.Is(err, tenancy.ErrOutOfScope) {
					t.Fatalf("stale lease accepted: %v", err)
				}
				if transition == "foreign-claim" {
					if err := r.captureChanges(ctx, run, env, false, true); !errors.Is(err, tenancy.ErrOutOfScope) {
						t.Fatalf("foreign claim accepted: %v", err)
					}
					return
				}
				if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := r.cleanupTerminal(ctx, run); err != nil {
					t.Fatal(err)
				}
			}
			var retained platformv1alpha1.Run
			var released platformv1alpha1.Environment
			if err := base.Get(context.Background(), client.ObjectKeyFromObject(run), &retained); err != nil {
				t.Fatal(err)
			}
			if err := base.Get(context.Background(), client.ObjectKeyFromObject(env), &released); err != nil {
				t.Fatal(err)
			}
			condition := apiMeta.FindStatusCondition(retained.Status.Conditions, changesFinalCondition)
			if condition == nil || condition.Reason != "OffboardingUnavailable" || released.Status.ClaimedBy != nil {
				t.Fatalf("cleanup remained gated: condition=%+v claim=%+v", condition, released.Status.ClaimedBy)
			}
			credential := apiMeta.FindStatusCondition(retained.Status.Conditions, runConditionRepositoryCredentialReady)
			if credential == nil || credential.Reason != "Revoked" {
				t.Fatalf("credential cleanup remained gated: %+v", credential)
			}
		})
	}
}

func (f changesSinkFunc) CaptureChanges(ctx context.Context, ns, name, uid string, r controlplane.CaptureChangesRequest) error {
	return f(ctx, ns, name, uid, r)
}

func TestChangesBaselineCaptureIsDurableAndNeverReplacedOnReaccept(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}}
	r := reconciler(t, &scriptedAdapter{}, run)
	calls := 0
	r.Changes = changesSinkFunc(func(_ context.Context, ns, name, uid string, request controlplane.CaptureChangesRequest) error {
		calls++
		if ns != "ns" || name != "r" || uid != "run-uid" || !request.Baseline {
			t.Fatal("incorrect identity")
		}
		return nil
	})
	if err := r.captureChanges(context.Background(), run, nil, true, false); err != nil {
		t.Fatal(err)
	}
	var persisted platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &persisted); err != nil {
		t.Fatal(err)
	}
	if !apiMeta.IsStatusConditionTrue(persisted.Status.Conditions, changesBaselineCondition) {
		t.Fatal("baseline marker missing")
	}
	for range 3 {
		if err := r.captureChanges(context.Background(), &persisted, &platformv1alpha1.Environment{Status: platformv1alpha1.EnvironmentStatus{ExecutionGeneration: 7}}, true, false); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatal("baseline replaced on resume", calls)
	}
}

func TestChangesFinalCaptureFailureBlocksClaimRelease(t *testing.T) {
	run, env := adapterRaceObjects(platformv1alpha1.EnvironmentOwnershipClaimed, platformv1alpha1.RunStateSucceeded)
	r := reconciler(t, &scriptedAdapter{}, run, env)
	r.Changes = changesSinkFunc(func(context.Context, string, string, string, controlplane.CaptureChangesRequest) error {
		return errors.New("storage unavailable")
	})
	if _, err := r.cleanupTerminal(context.Background(), run); err == nil {
		t.Fatal("capture failure did not gate cleanup")
	}
	var current platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.ClaimedBy == nil {
		t.Fatal("released claim without retaining final outcome")
	}
}

func TestChangesTransientBaselineBlocksAcceptanceUntilRetry(t *testing.T) {
	run, env := adapterRaceObjects(platformv1alpha1.EnvironmentOwnershipClaimed, platformv1alpha1.RunStateEnvironmentReady)
	r := reconciler(t, &scriptedAdapter{}, run, env)
	fail := true
	calls := 0
	r.Changes = changesSinkFunc(func(context.Context, string, string, string, controlplane.CaptureChangesRequest) error {
		calls++
		if fail {
			return context.DeadlineExceeded
		}
		return nil
	})
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}
	for attempt := 0; attempt < 10 && calls == 0; attempt++ {
		_, err := r.Reconcile(context.Background(), request)
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	}
	var current platformv1alpha1.Run
	if err := r.Get(context.Background(), request.NamespacedName, &current); err != nil {
		t.Fatal(err)
	}
	if calls == 0 || acceptanceAttempted(&current) || apiMeta.IsStatusConditionTrue(current.Status.Conditions, changesBaselineCondition) {
		t.Fatal("transient baseline advanced acceptance or never attempted")
	}
	fail = false
	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), request.NamespacedName, &current); err != nil {
		t.Fatal(err)
	}
	if !acceptanceAttempted(&current) || !apiMeta.IsStatusConditionTrue(current.Status.Conditions, changesBaselineCondition) {
		t.Fatal("retry did not advance acceptance")
	}
}
