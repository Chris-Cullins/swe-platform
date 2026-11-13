package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/isolation"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func followUpFixture() (*platformv1alpha1.Run, *platformv1alpha1.Environment, *corev1.PersistentVolumeClaim, *platformv1alpha1.EnvironmentTemplate) {
	incarnation := "15bd9bf1-eef4-4e49-a6ae-02cd99ca9695"
	tmpl := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "team", UID: "template-a", Generation: 1}, Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "image"}}
	tmpl.Annotations = map[string]string{
		tenancy.InstallationNamespaceAnnotation: "system", tenancy.InstallationNameAnnotation: "main", tenancy.InstallationUIDAnnotation: "installation-a",
		tenancy.ProjectNameAnnotation: "project", tenancy.ProjectUIDAnnotation: "project-a", tenancy.CatalogNameAnnotation: "small", tenancy.CatalogRevisionAnnotation: "revision", tenancy.CatalogSourceUIDAnnotation: "catalog-a",
	}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "retained", Namespace: "team", UID: "environment-a", Generation: 3}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: tmpl.Name}, Status: platformv1alpha1.EnvironmentStatus{ObservedGeneration: 3, ExecutionGeneration: 2, Workspace: &platformv1alpha1.EnvironmentWorkspace{Incarnation: incarnation, Backend: platformv1alpha1.EnvironmentBackendPod, PVCRef: &platformv1alpha1.WorkspacePVCReference{Name: "disk", UID: "disk-a"}}, Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, ObservedGeneration: 3}}}}
	env.Status.Provisioning = platformv1alpha1.ResolveEnvironmentProvisioning(env, tmpl, nil)
	env.Status.Provisioning.TemplateVerified = true
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "source", Namespace: "team", UID: "run-a", Generation: 2}, Spec: platformv1alpha1.RunSpec{Prompt: "sensitive-prompt", CredentialProfileRef: "sensitive-profile"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateFailed, ObservedGeneration: 2, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: env.Name, UID: env.UID, Ownership: platformv1alpha1.EnvironmentOwnershipClaimed}, WorkspaceRef: &platformv1alpha1.RunWorkspaceReference{EnvironmentUID: env.UID, Incarnation: incarnation}, Conditions: []metav1.Condition{{Type: "EnvironmentReady", Status: metav1.ConditionFalse, Reason: "EnvironmentReleased", ObservedGeneration: 2, Message: "sensitive-message"}}}}
	controller := true
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "disk", Namespace: "team", UID: "disk-a", Annotations: map[string]string{platformv1alpha1.WorkspaceIncarnationAnnotation: incarnation}, OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: &controller}}}}
	return run, env, pvc, tmpl
}

func followUpAuthority() []client.Object {
	return []client.Object{
		&platformv1alpha1.Installation{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "installation-a", Generation: 1}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team", UID: "namespace-a", Annotations: map[string]string{
			tenancy.InstallationNamespaceAnnotation: "system", tenancy.InstallationNameAnnotation: "main", tenancy.InstallationUIDAnnotation: "installation-a",
			tenancy.ProjectNameAnnotation: "project", tenancy.ProjectUIDAnnotation: "project-a", tenancy.LifecycleAnnotation: string(tenancy.LifecycleActive),
		}}},
		&platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "team", UID: "project-a"}},
	}
}

func followUpService(c client.WithWatch) *KubernetesResourceService {
	return &KubernetesResourceService{Client: c, Installation: tenancy.InstallationIdentity{Key: types.NamespacedName{Namespace: "system", Name: "main"}, UID: "installation-a"}}
}

func TestFollowUpInspectionClassifications(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		mutate     func(*platformv1alpha1.Run, *platformv1alpha1.Environment, *corev1.PersistentVolumeClaim, *platformv1alpha1.EnvironmentTemplate)
	}{
		{"failed", "EligibleActive", nil},
		{"succeeded", "EligibleActive", func(r *platformv1alpha1.Run, _ *platformv1alpha1.Environment, _ *corev1.PersistentVolumeClaim, _ *platformv1alpha1.EnvironmentTemplate) {
			r.Status.State = platformv1alpha1.RunStateSucceeded
		}},
		{"cancelled", "EligibleActive", func(r *platformv1alpha1.Run, _ *platformv1alpha1.Environment, _ *corev1.PersistentVolumeClaim, _ *platformv1alpha1.EnvironmentTemplate) {
			r.Status.State = platformv1alpha1.RunStateCancelled
		}},
		{"unknown", "SourceNotTerminal", func(r *platformv1alpha1.Run, _ *platformv1alpha1.Environment, _ *corev1.PersistentVolumeClaim, _ *platformv1alpha1.EnvironmentTemplate) {
			r.Status.State = "Future"
		}},
		{"legacy", "WorkspaceUnverifiable", func(r *platformv1alpha1.Run, _ *platformv1alpha1.Environment, _ *corev1.PersistentVolumeClaim, _ *platformv1alpha1.EnvironmentTemplate) {
			r.Status.WorkspaceRef = nil
		}},
		{"unbound", "WorkspaceUnverifiable", func(_ *platformv1alpha1.Run, e *platformv1alpha1.Environment, _ *corev1.PersistentVolumeClaim, _ *platformv1alpha1.EnvironmentTemplate) {
			e.Status.Workspace.PVCRef = nil
		}},
		{"stale-source", "SourceCleanupPending", func(r *platformv1alpha1.Run, _ *platformv1alpha1.Environment, _ *corev1.PersistentVolumeClaim, _ *platformv1alpha1.EnvironmentTemplate) {
			r.Status.ObservedGeneration--
		}},
		{"stale-cleanup", "SourceCleanupPending", func(r *platformv1alpha1.Run, _ *platformv1alpha1.Environment, _ *corev1.PersistentVolumeClaim, _ *platformv1alpha1.EnvironmentTemplate) {
			r.Status.Conditions[0].ObservedGeneration--
		}},
		{"pending-suspend", "SourceCleanupPending", func(_ *platformv1alpha1.Run, e *platformv1alpha1.Environment, _ *corev1.PersistentVolumeClaim, _ *platformv1alpha1.EnvironmentTemplate) {
			e.Status.Lifecycle.PendingSuspendRequestID = "pending"
		}},
		{"owned", "OwnedEnvironmentUnsupported", func(r *platformv1alpha1.Run, _ *platformv1alpha1.Environment, _ *corev1.PersistentVolumeClaim, _ *platformv1alpha1.EnvironmentTemplate) {
			r.Status.EnvironmentRef.Ownership = platformv1alpha1.EnvironmentOwnershipOwned
		}},
		{"busy", "EnvironmentBusy", func(_ *platformv1alpha1.Run, e *platformv1alpha1.Environment, _ *corev1.PersistentVolumeClaim, _ *platformv1alpha1.EnvironmentTemplate) {
			e.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: "other", UID: "other-uid"}
		}},
		{"replaced-environment", "WorkspaceUnavailable", func(_ *platformv1alpha1.Run, e *platformv1alpha1.Environment, _ *corev1.PersistentVolumeClaim, _ *platformv1alpha1.EnvironmentTemplate) {
			e.UID = "replacement"
		}},
		{"replaced-pvc", "WorkspaceUnavailable", func(_ *platformv1alpha1.Run, _ *platformv1alpha1.Environment, p *corev1.PersistentVolumeClaim, _ *platformv1alpha1.EnvironmentTemplate) {
			p.UID = "replacement"
		}},
		{"foreign-pvc", "WorkspaceUnavailable", func(_ *platformv1alpha1.Run, _ *platformv1alpha1.Environment, p *corev1.PersistentVolumeClaim, _ *platformv1alpha1.EnvironmentTemplate) {
			p.OwnerReferences[0].UID = "foreign"
		}},
		{"deleting-pvc", "WorkspaceUnavailable", func(_ *platformv1alpha1.Run, _ *platformv1alpha1.Environment, p *corev1.PersistentVolumeClaim, _ *platformv1alpha1.EnvironmentTemplate) {
			now := metav1.Now()
			p.DeletionTimestamp = &now
			p.Finalizers = []string{"test"}
		}},
		{"stale-environment", "AuthorityUnavailable", func(_ *platformv1alpha1.Run, e *platformv1alpha1.Environment, _ *corev1.PersistentVolumeClaim, _ *platformv1alpha1.EnvironmentTemplate) {
			e.Status.ObservedGeneration--
		}},
		{"replaced-template", "AuthorityUnavailable", func(_ *platformv1alpha1.Run, _ *platformv1alpha1.Environment, _ *corev1.PersistentVolumeClaim, t *platformv1alpha1.EnvironmentTemplate) {
			t.UID = "replacement"
		}},
		{"held", "EnvironmentHeld", func(_ *platformv1alpha1.Run, e *platformv1alpha1.Environment, _ *corev1.PersistentVolumeClaim, _ *platformv1alpha1.EnvironmentTemplate) {
			e.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Enabled: true, Revision: 4}
		}},
		{"requested", "WakeConsentRequired", func(_ *platformv1alpha1.Run, e *platformv1alpha1.Environment, _ *corev1.PersistentVolumeClaim, _ *platformv1alpha1.EnvironmentTemplate) {
			e.Status.Lifecycle.Suspended = true
			e.Status.Lifecycle.SuspensionReason = platformv1alpha1.EnvironmentSuspensionReasonRequested
		}},
		{"idle", "WakeConsentRequired", func(_ *platformv1alpha1.Run, e *platformv1alpha1.Environment, _ *corev1.PersistentVolumeClaim, _ *platformv1alpha1.EnvironmentTemplate) {
			e.Status.Lifecycle.Suspended = true
			e.Status.Lifecycle.SuspensionReason = platformv1alpha1.EnvironmentSuspensionReasonIdle
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run, env, pvc, tmpl := followUpFixture()
			if tc.mutate != nil {
				tc.mutate(run, env, pvc, tmpl)
			}
			scheme := resourceScheme(t)
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, env, pvc, tmpl).WithObjects(followUpAuthority()...).Build()
			gets := 0
			c := interceptor.NewClient(base, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				gets++
				if _, secret := obj.(*corev1.Secret); secret {
					t.Fatal("inspection read Secret")
				}
				return c.Get(ctx, key, obj, opts...)
			}, Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
				t.Fatal("inspection created resource")
				return nil
			}, Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
				t.Fatal("inspection wrote resource")
				return nil
			}})
			got, err := followUpService(c).InspectFollowUp(context.Background(), "team", "source", "run-a", func(name string) error {
				if name != "retained" {
					t.Fatal("wrong authorization target")
				}
				return nil
			})
			if err != nil || got.Reason != tc.want || gets > 14 {
				t.Fatalf("reason=%s reads=%d err=%v", got.Reason, gets, err)
			}
			data, _ := json.Marshal(got)
			for _, secret := range []string{"sensitive", "disk-a", "image"} {
				if strings.Contains(string(data), secret) {
					t.Fatalf("private field in inspection: %s", data)
				}
			}
			if tc.name == "replaced-environment" && got.EnvironmentUID != "" {
				t.Fatal("replacement identity disclosed")
			}
		})
	}
}

type followUpAccess struct {
	deny  string
	calls []ResourceAccess
}

func (a *followUpAccess) Authorize(_ *http.Request, x ResourceAccess, _ bool) error {
	a.calls = append(a.calls, x)
	if a.deny == x.Verb+" "+x.Resource {
		return errForbidden
	}
	return nil
}

func TestFollowUpInspectionAuthorizationAndIdentity(t *testing.T) {
	for _, deny := range []string{"get runs", "get environments", "use environments", ""} {
		t.Run(deny, func(t *testing.T) {
			run, env, pvc, tmpl := followUpFixture()
			scheme := resourceScheme(t)
			_ = corev1.AddToScheme(scheme)
			base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, env, pvc, tmpl).WithObjects(followUpAuthority()...).Build()
			gets := 0
			c := interceptor.NewClient(base, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				gets++
				if deny != "" {
					if _, ok := obj.(*platformv1alpha1.Environment); ok {
						t.Fatal("unauthorized target read")
					}
				}
				return c.Get(ctx, key, obj, opts...)
			}})
			a := &followUpAccess{deny: deny}
			s := NewServer(nil, ServerOptions{Access: a, Resources: followUpService(c)})
			r := httptest.NewRequest("GET", "/api/v1/namespaces/team/runs/source/follow-up", nil)
			r.Header.Set(RunUIDHeader, "run-a")
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if deny != "" {
				if w.Code != 403 || strings.Contains(w.Body.String(), "retained") || strings.Contains(w.Body.String(), "environment-a") {
					t.Fatalf("denial leaked data: %d %s", w.Code, w.Body.String())
				}
				if deny == "get runs" && gets != 0 {
					t.Fatal("unauthorized source read")
				}
				return
			}
			if w.Code != 200 {
				t.Fatalf("allowed inspection: %d %s", w.Code, w.Body.String())
			}
			for _, call := range a.calls {
				if call.Resource == "environments" && call.Name != "retained" {
					t.Fatal("unnamed authorization")
				}
			}
			r.Header.Set(RunUIDHeader, "replacement")
			w = httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != 409 {
				t.Fatalf("stale UID accepted: %d", w.Code)
			}
		})
	}
}

func TestFollowUpInspectionDetectsDrift(t *testing.T) {
	for _, kind := range []string{"namespace", "run", "environment", "pvc", "installation"} {
		t.Run(kind, func(t *testing.T) {
			run, env, pvc, tmpl := followUpFixture()
			scheme := resourceScheme(t)
			_ = corev1.AddToScheme(scheme)
			base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, env, pvc, tmpl).WithObjects(followUpAuthority()...).Build()
			seen := map[string]int{}
			c := interceptor.NewClient(base, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				k := ""
				switch obj.(type) {
				case *corev1.Namespace:
					k = "namespace"
				case *platformv1alpha1.Run:
					k = "run"
				case *platformv1alpha1.Environment:
					k = "environment"
				case *corev1.PersistentVolumeClaim:
					k = "pvc"
				case *platformv1alpha1.Installation:
					k = "installation"
				}
				seen[k]++
				if k == kind && seen[k] == 2 {
					obj.SetUID("replacement")
				}
				return nil
			}})
			got, err := followUpService(c).InspectFollowUp(context.Background(), "team", "source", "run-a", func(string) error { return nil })
			if err == nil || got.Reason != "" {
				t.Fatal(fmt.Sprintf("%s drift returned eligibility", kind))
			}
		})
	}
}

func TestFollowUpInspectionInstallationAuthority(t *testing.T) {
	for _, scenario := range []string{"active", "fencing", "restricted", "stale-condition", "wrong-revision", "replacement", "revoked-claim", "fencing-claim", "reclaimed-project"} {
		t.Run(scenario, func(t *testing.T) {
			run, env, pvc, tmpl := followUpFixture()
			authority := followUpAuthority()
			installation := authority[0].(*platformv1alpha1.Installation)
			ns := authority[1].(*corev1.Namespace)
			project := authority[2].(*platformv1alpha1.Project)
			installation.Spec.Isolation = &platformv1alpha1.InstallationIsolationSpec{Mode: platformv1alpha1.InstallationIsolationModeUnrestrictedDevelopment}
			if scenario == "replacement" {
				installation.UID = "valid-replacement"
				ns.Annotations[tenancy.InstallationUIDAnnotation] = string(installation.UID)
				tmpl.Annotations[tenancy.InstallationUIDAnnotation] = string(installation.UID)
			}
			revision, err := (isolation.RevisionInputs{InstallationUID: installation.UID, Selection: *installation.Spec.Isolation}).DeriveRevision()
			if err != nil {
				t.Fatal(err)
			}
			installation.Status = platformv1alpha1.InstallationStatus{IsolationState: platformv1alpha1.InstallationIsolationStateActive, ObservedIsolation: installation.Spec.Isolation.DeepCopy(), IsolationRevision: revision.String(), Conditions: []metav1.Condition{{Type: platformv1alpha1.InstallationConditionIsolationReady, Status: metav1.ConditionTrue, Reason: "UnrestrictedDevelopment", ObservedGeneration: installation.Generation}}}
			switch scenario {
			case "fencing":
				installation.Status.IsolationState = platformv1alpha1.InstallationIsolationStateFencing
			case "restricted":
				installation.Spec.Isolation = &platformv1alpha1.InstallationIsolationSpec{Mode: platformv1alpha1.InstallationIsolationModeRestrictedProductionCalicoV3_32_1, PolicyConfigMapName: "policy", RuntimeClass: &platformv1alpha1.InstallationRuntimeClassExpectation{Name: "gvisor", Handler: "runsc"}, StorageClass: &platformv1alpha1.InstallationStorageClassExpectation{Name: "disk", CSIDriver: "csi.example.com"}}
				if err := isolation.ValidateSelection(installation.Spec.Isolation); err != nil {
					t.Fatalf("invalid restricted fixture: %v", err)
				}
				installation.Status.IsolationState = platformv1alpha1.InstallationIsolationStateBlocked
			case "stale-condition":
				installation.Status.Conditions[0].ObservedGeneration--
			case "wrong-revision":
				installation.Status.IsolationRevision = "sha256:" + strings.Repeat("0", 64)
			case "revoked-claim":
				delete(ns.Annotations, tenancy.InstallationUIDAnnotation)
			case "fencing-claim":
				ns.Annotations[tenancy.LifecycleAnnotation] = string(tenancy.LifecycleFencing)
				ns.Annotations[tenancy.LifecycleOperationAnnotation] = tenancy.OperationOffboarding
			case "reclaimed-project":
				project.UID = "new-project"
				ns.Annotations[tenancy.ProjectUIDAnnotation] = string(project.UID)
			}
			scheme := resourceScheme(t)
			_ = corev1.AddToScheme(scheme)
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, env, pvc, tmpl).WithObjects(authority...).Build()
			service := followUpService(c)
			result, err := service.InspectFollowUp(context.Background(), "team", "source", "run-a", func(string) error { return nil })
			want := "AuthorityUnavailable"
			if scenario == "active" {
				want = "EligibleActive"
			}
			if err != nil || result.Reason != want {
				t.Fatalf("inspection=%#v err=%v", result, err)
			}
			// Exercise the production middleware as well: revoked/replaced/fencing
			// namespace authority stays an authorization denial, never a disclosure.
			access := TenancyAccessController{Access: &followUpAccess{}, Verifier: &tenancy.Verifier{Reader: c, Installation: service.Installation, Mode: tenancy.ModeScoped}, Namespaces: map[string]struct{}{"team": {}}}
			server := NewServer(nil, ServerOptions{Access: access, Resources: service})
			req := httptest.NewRequest("GET", "/api/v1/namespaces/team/runs/source/follow-up", nil)
			req.Header.Set(RunUIDHeader, "run-a")
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, req)
			if scenario == "replacement" || scenario == "revoked-claim" || scenario == "fencing-claim" {
				if response.Code != 403 || strings.Contains(response.Body.String(), "environment-a") {
					t.Fatalf("revoked tenancy disclosed inspection: %d", response.Code)
				}
			} else if response.Code != 200 || !strings.Contains(response.Body.String(), want) {
				t.Fatalf("middleware result=%d want=%s", response.Code, want)
			}
		})
	}
}
