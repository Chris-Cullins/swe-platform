package controllers

import (
	"context"
	"errors"
	"testing"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/agent"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// Fake clients omit server-assigned UIDs. New-storage tests must model them,
// rather than weakening the production requirement for a nonempty exact UID.
func workspaceTestUIDs() interceptor.Funcs {
	return interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if obj.GetUID() == "" {
			obj.SetUID(types.UID(uuid.NewString()))
		}
		return c.Create(ctx, obj, opts...)
	}}
}

func TestWorkspaceProvisioningLostResponsesAndReplacement(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, networkingv1.AddToScheme, platformv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "new", Namespace: "team", UID: "env-a", Generation: 1, Finalizers: []string{environmentFinalizer}}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: "small"}}
	tmpl := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "team", UID: "template-a", Generation: 1}, Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "image", Size: "small"}}
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env, tmpl).Build()
	lostCreate, lostBinding := false, false
	lostResponse := errors.New("lost response")
	uidFuncs := workspaceTestUIDs()
	c := interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			err := uidFuncs.Create(ctx, c, obj, opts...)
			if _, ok := obj.(*corev1.PersistentVolumeClaim); ok && err == nil && !lostCreate {
				lostCreate = true
				return lostResponse
			}
			return err
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
			err := c.SubResource(sub).Patch(ctx, obj, p, opts...)
			if e, ok := obj.(*platformv1alpha1.Environment); ok && err == nil && e.Status.Workspace != nil && e.Status.Workspace.PVCRef != nil && !lostBinding {
				lostBinding = true
				return lostResponse
			}
			return err
		},
	})
	var originalPVC types.UID
	for range 18 {
		// A new reconciler each step exercises durable, not in-memory recovery.
		r := &EnvironmentReconciler{Client: c, APIReader: base, Scheme: scheme}
		if _, err := r.Reconcile(ctx, provisioningRequest(env)); err != nil && !errors.Is(err, lostResponse) {
			t.Fatal(err)
		}
		var current platformv1alpha1.Environment
		if err := base.Get(ctx, client.ObjectKeyFromObject(env), &current); err != nil {
			t.Fatal(err)
		}
		var pvc corev1.PersistentVolumeClaim
		if err := base.Get(ctx, client.ObjectKey{Namespace: env.Namespace, Name: envPVCName(env)}, &pvc); err == nil {
			if originalPVC == "" {
				originalPVC = pvc.UID
			} else if originalPVC != pvc.UID {
				t.Fatal("lost response replaced PVC")
			}
		}
		if current.Status.Workspace == nil || current.Status.Workspace.PVCRef == nil {
			for _, obj := range []client.Object{&corev1.Pod{}, &corev1.Secret{}} {
				name := envPodName(env)
				if _, ok := obj.(*corev1.Secret); ok {
					name = envCredentialName(env)
				}
				if err := base.Get(ctx, client.ObjectKey{Namespace: env.Namespace, Name: name}, obj); !apierrors.IsNotFound(err) {
					t.Fatalf("execution before durable binding: %T %v", obj, err)
				}
			}
		}
	}
	if !lostCreate || !lostBinding || originalPVC == "" {
		t.Fatal("did not exercise durable uncertain writes")
	}
	var current platformv1alpha1.Environment
	if err := base.Get(ctx, client.ObjectKeyFromObject(env), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Workspace.PVCRef.UID != originalPVC {
		t.Fatal("binding lost across restart")
	}
	if err := base.Get(ctx, client.ObjectKey{Namespace: env.Namespace, Name: envPodName(env)}, &corev1.Pod{}); err != nil {
		t.Fatalf("bound storage never provisioned: %v", err)
	}
	var pvc corev1.PersistentVolumeClaim
	if err := base.Get(ctx, client.ObjectKey{Namespace: env.Namespace, Name: envPVCName(env)}, &pvc); err != nil {
		t.Fatal(err)
	}
	if err := base.Delete(ctx, &pvc); err != nil {
		t.Fatal(err)
	}
	for range 6 {
		r := &EnvironmentReconciler{Client: c, APIReader: base, Scheme: scheme}
		if _, err := r.Reconcile(ctx, provisioningRequest(env)); err != nil {
			t.Fatal(err)
		}
	}
	if err := base.Get(ctx, client.ObjectKey{Namespace: env.Namespace, Name: envPVCName(env)}, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
		t.Fatalf("missing bound PVC recreated: %v", err)
	}
	if err := base.Get(ctx, client.ObjectKey{Namespace: env.Namespace, Name: envPodName(env)}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("execution not fenced: %v", err)
	}
	// Even a replacement with the correct marker and owner cannot inherit a bound UID.
	pvc.ResourceVersion = ""
	pvc.UID = "replacement"
	if err := base.Create(ctx, &pvc); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(ctx, client.ObjectKeyFromObject(env), &current); err != nil {
		t.Fatal(err)
	}
	r := &EnvironmentReconciler{Client: c, APIReader: base, Scheme: scheme}
	if ready, err := r.ensureWorkspacePVC(ctx, &current, tmpl); ready || err == nil {
		t.Fatalf("replacement accepted: %t %v", ready, err)
	}
}

func TestRunWorkspaceAssociationAndExecutionFence(t *testing.T) {
	for _, scenario := range []string{"new", "legacy", "previously-attempted", "missing", "replaced", "foreign", "deleting", "lost-association-response", "lost-during-acceptance", "cancel-missing", "terminal-missing"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			run, env := adapterRaceObjects(platformv1alpha1.EnvironmentOwnershipClaimed, platformv1alpha1.RunStateEnvironmentReady)
			incarnation := "15bd9bf1-eef4-4e49-a6ae-02cd99ca9695"
			env.Status.Workspace = &platformv1alpha1.EnvironmentWorkspace{Incarnation: incarnation, Backend: platformv1alpha1.EnvironmentBackendPod, PVCRef: &platformv1alpha1.WorkspacePVCReference{Name: "disk", UID: "disk-a"}}
			pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "disk", Namespace: env.Namespace, UID: "disk-a", Annotations: map[string]string{platformv1alpha1.WorkspaceIncarnationAnnotation: incarnation}, OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: ptr(true)}}}}
			switch scenario {
			case "legacy":
				env.Status.Workspace = nil
			case "previously-attempted":
				run.Status.Conditions = []metav1.Condition{{Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue}}
			case "replaced":
				pvc.UID = "different-disk"
			case "foreign":
				pvc.OwnerReferences[0].UID = "different-owner"
			case "deleting":
				now := metav1.Now()
				pvc.DeletionTimestamp, pvc.Finalizers = &now, []string{"test"}
			case "cancel-missing":
				run.Spec.Cancel = true
			case "terminal-missing":
				run.Status.State = platformv1alpha1.RunStateSucceeded
			}
			adapter := &scriptedAdapter{observations: []agent.AdapterObservation{agent.AdapterObservationRunning}}
			objects := []client.Object{run, env}
			if scenario != "missing" && scenario != "cancel-missing" && scenario != "terminal-missing" {
				objects = append(objects, pvc)
			}
			r := reconciler(t, adapter, objects...)
			base := r.Client.(client.WithWatch)
			lostResponse := errors.New("association response lost")
			lost := false
			if scenario == "lost-association-response" {
				r.Client = interceptor.NewClient(base, interceptor.Funcs{SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
					err := c.SubResource(sub).Patch(ctx, obj, patch, opts...)
					if current, ok := obj.(*platformv1alpha1.Run); ok && err == nil && current.Status.WorkspaceRef != nil && !lost {
						lost = true
						return lostResponse
					}
					return err
				}})
			}
			adapter.onAccept = func() {
				var current platformv1alpha1.Run
				if err := base.Get(ctx, client.ObjectKeyFromObject(run), &current); err != nil {
					t.Fatal(err)
				}
				if scenario != "legacy" && scenario != "previously-attempted" && (current.Status.WorkspaceRef == nil || current.Status.WorkspaceRef.Incarnation != incarnation || current.Status.WorkspaceRef.EnvironmentUID != env.UID) {
					t.Fatal("adapter invoked without durable exact association")
				}
				if scenario == "lost-during-acceptance" {
					if err := base.Delete(ctx, pvc); err != nil {
						t.Fatal(err)
					}
				}
			}
			var got platformv1alpha1.Run
			for range 6 {
				_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
				var terminal *terminalEnvironmentError
				if err != nil && !errors.Is(err, lostResponse) && !(scenario == "lost-during-acceptance" && errors.As(err, &terminal)) {
					t.Fatal(err)
				}
				if err := base.Get(ctx, client.ObjectKeyFromObject(run), &got); err != nil {
					t.Fatal(err)
				}
				if scenario == "lost-during-acceptance" && adapter.accepted > 0 {
					if runAccepted(&got) || got.Status.StartedAt != nil {
						t.Fatal("stale acceptance published after storage disappeared")
					}
					break
				}
			}
			switch scenario {
			case "new", "lost-association-response":
				if got.Status.WorkspaceRef == nil || adapter.accepted != 1 || got.Status.State != platformv1alpha1.RunStateRunning || scenario == "lost-association-response" && !lost {
					t.Fatalf("association/acceptance not recovered: %#v accepts=%d lost=%t", got.Status, adapter.accepted, lost)
				}
			case "legacy", "previously-attempted":
				if got.Status.WorkspaceRef != nil || got.Status.State != platformv1alpha1.RunStateRunning {
					t.Fatalf("legacy execution backfilled or blocked: %#v", got.Status)
				}
			case "missing", "replaced", "foreign", "deleting":
				if adapter.accepted != 0 || got.Status.State != platformv1alpha1.RunStateFailed || got.Status.WorkspaceRef != nil {
					t.Fatalf("invalid storage accepted: %#v", got.Status)
				}
			case "cancel-missing", "terminal-missing":
				var current platformv1alpha1.Environment
				if err := base.Get(ctx, client.ObjectKeyFromObject(env), &current); err != nil {
					t.Fatal(err)
				}
				if current.Status.ClaimedBy != nil || got.Status.WorkspaceRef != nil || adapter.accepted != 0 {
					t.Fatal("storage proof blocked cleanup or backfilled terminal provenance")
				}
			case "lost-during-acceptance":
				if adapter.accepted != 1 {
					t.Fatal("race never reached adapter")
				}
			}
		})
	}
}
