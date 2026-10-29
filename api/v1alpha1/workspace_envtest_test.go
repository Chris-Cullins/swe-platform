package v1alpha1

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"strings"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Uses the same disposable API server as Project selection, and installs the
// actual rendered policy. Impersonated clients have RBAC permission deliberately:
// these assertions exercise admission, not accidental RBAC denial.
func testWorkspaceAdmission(t *testing.T, config *rest.Config, admin client.Client, scheme *runtime.Scheme) {
	t.Helper()
	ctx := context.Background()
	if err := admissionv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "workspace-proof", Annotations: map[string]string{"swe.dev/installation-namespace": "system", "swe.dev/installation-name": "proof-swe-platform"}}}
	if err := admin.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
	rendered, err := exec.Command("helm", "template", "proof", "../../charts/swe-platform", "--namespace", "system", "--set", "tenancy.mode=scoped", "--show-only", "templates/workspace-proof-policy.yaml").Output()
	if err != nil {
		t.Fatal(err)
	}
	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(rendered), 4096)
	for {
		var raw runtime.RawExtension
		if err := decoder.Decode(&raw); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if len(raw.Raw) == 0 {
			continue
		}
		obj, _, err := serializer.NewCodecFactory(scheme).UniversalDeserializer().Decode(raw.Raw, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := admin.Create(ctx, obj.(client.Object)); err != nil {
			t.Fatal(err)
		}
	}
	actor := func(name string) client.Client {
		cfg := rest.CopyConfig(config)
		cfg.Impersonate = rest.ImpersonationConfig{UserName: name, Groups: []string{"system:masters"}}
		c, err := client.New(cfg, client.Options{Scheme: scheme})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	operator := actor("system:serviceaccount:system:proof-swe-platform")
	outsider := actor("system:serviceaccount:system:other-operator")
	gateway := actor("system:serviceaccount:system:proof-swe-platform-control-plane")
	incarnation := "15bd9bf1-eef4-4e49-a6ae-02cd99ca9695"
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "storage", Namespace: ns.Name, Annotations: map[string]string{WorkspaceIncarnationAnnotation: incarnation}}, Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}}}}
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := outsider.Create(ctx, pvc.DeepCopy(), client.DryRunAll)
		if apierrors.IsForbidden(err) {
			break
		}
		if err != nil || time.Now().After(deadline) {
			t.Fatalf("workspace policy did not reject unauthorized marker: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := operator.Create(ctx, pvc); err != nil {
		t.Fatal(err)
	}
	changedPVC := pvc.DeepCopy()
	changedPVC.Annotations[WorkspaceIncarnationAnnotation] = "changed"
	if err := operator.Update(ctx, changedPVC); !apierrors.IsForbidden(err) {
		t.Fatalf("marker changed: %v", err)
	}
	env := &Environment{ObjectMeta: metav1.ObjectMeta{Name: "new", Namespace: ns.Name}, Spec: EnvironmentSpec{TemplateRef: "small"}}
	if err := admin.Create(ctx, env); err != nil {
		t.Fatal(err)
	}
	tmpl := &EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", UID: "template-uid", Generation: 1}, Spec: EnvironmentTemplateSpec{Image: "image"}}
	env.Status.Provisioning = ResolveEnvironmentProvisioning(env, tmpl, nil)
	env.Status.Workspace = &EnvironmentWorkspace{Incarnation: incarnation, Backend: EnvironmentBackendPod}
	for _, c := range []client.Client{outsider, gateway} {
		if err := c.Status().Update(ctx, env.DeepCopy()); !apierrors.IsForbidden(err) {
			t.Fatalf("non-operator proof publication: %v", err)
		}
	}
	if err := operator.Status().Update(ctx, env); err != nil {
		t.Fatal(err)
	}
	env.Status.Workspace.PVCRef = &WorkspacePVCReference{Name: pvc.Name, UID: pvc.UID}
	if err := operator.Status().Update(ctx, env); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Environment){
		func(e *Environment) { e.Status.Workspace.Incarnation = "411c28f0-9c3b-4658-ab1b-4dddb5161224" },
		func(e *Environment) { e.Status.Workspace.PVCRef.UID = "replacement" },
		func(e *Environment) { e.Status.Workspace.PVCRef = nil },
		func(e *Environment) { e.Status.Workspace = nil },
		func(e *Environment) { e.Status = EnvironmentStatus{} },
	} {
		candidate := env.DeepCopy()
		mutate(candidate)
		if err := operator.Status().Update(ctx, candidate); !apierrors.IsInvalid(err) {
			t.Fatalf("immutable proof mutation accepted: %v", err)
		}
	}
	legacy := &Environment{ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: ns.Name}, Spec: env.Spec}
	if err := admin.Create(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	legacy.Status.Provisioning = ResolveEnvironmentProvisioning(legacy, tmpl, nil)
	if err := operator.Status().Update(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	legacy.Status.Workspace = &EnvironmentWorkspace{Incarnation: incarnation, Backend: EnvironmentBackendPod}
	if err := operator.Status().Update(ctx, legacy); !apierrors.IsInvalid(err) {
		t.Fatalf("legacy proof retrofit: %v", err)
	}
	run := &Run{ObjectMeta: metav1.ObjectMeta{Name: "source", Namespace: ns.Name}, Spec: RunSpec{EnvironmentRef: env.Name, Agent: "fake", Prompt: "keyless"}}
	if err := admin.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	run.Status.EnvironmentRef = &RunEnvironmentReference{Name: env.Name, UID: env.UID, Ownership: EnvironmentOwnershipClaimed}
	run.Status.WorkspaceRef = &RunWorkspaceReference{EnvironmentUID: env.UID, Incarnation: incarnation}
	for _, c := range []client.Client{outsider, gateway} {
		if err := c.Status().Update(ctx, run.DeepCopy()); !apierrors.IsForbidden(err) {
			t.Fatalf("non-operator association: %v", err)
		}
	}
	if err := operator.Status().Update(ctx, run); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Run){
		func(r *Run) { r.Status.WorkspaceRef.EnvironmentUID = "other" },
		func(r *Run) { r.Status.WorkspaceRef = nil },
		func(r *Run) { r.Status = RunStatus{} },
	} {
		candidate := run.DeepCopy()
		mutate(candidate)
		if err := operator.Status().Update(ctx, candidate); !apierrors.IsInvalid(err) {
			t.Fatalf("immutable association mutation accepted: %v", err)
		}
	}
	for _, state := range []RunState{RunStateSucceeded, RunStateFailed, RunStateCancelled, RunStateRunning, RunStatePaused} {
		old := &Run{ObjectMeta: metav1.ObjectMeta{Name: "old-" + string(state), Namespace: ns.Name}, Spec: run.Spec}
		old.Name = "old-" + strings.ToLower(string(state))
		if err := admin.Create(ctx, old); err != nil {
			t.Fatal(err)
		}
		old.Status.State = state
		if state == RunStatePaused {
			old.Status.Conditions = []metav1.Condition{{Type: "AdapterAccepted", Status: metav1.ConditionTrue, Reason: "Accepted", Message: "accepted", LastTransitionTime: metav1.Now()}}
		}
		old.Status.EnvironmentRef = run.Status.EnvironmentRef
		if err := operator.Status().Update(ctx, old); err != nil {
			t.Fatal(err)
		}
		old.Status.WorkspaceRef = run.Status.WorkspaceRef
		if err := operator.Status().Update(ctx, old); !apierrors.IsInvalid(err) {
			t.Fatalf("old Run backfill accepted: %v", err)
		}
	}
}
