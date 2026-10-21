package v1alpha1

import (
	"context"
	"os"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// This uses the generated installed schemas and actual API admission, not the
// fake client's permissive update semantics. It never selects an existing cluster.
func TestProjectSelectionAPIServer(t *testing.T) {
	if os.Getenv("SWE_TEST_PROJECT_APISERVER") != "1" {
		t.Skip("set SWE_TEST_PROJECT_APISERVER=1 for disposable API-server admission")
	}
	env := &envtest.Environment{UseExistingCluster: ptr.To(false), DownloadBinaryAssets: true, DownloadBinaryAssetsVersion: "1.35.0", BinaryAssetsDirectory: t.TempDir(), CRDDirectoryPaths: []string{"../../config/crd/bases"}, ErrorIfCRDPathMissing: true}
	env.ControlPlane.GetAPIServer().Configure().Set("advertise-address", "127.0.0.1")
	config, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "selection"}}
	if err := c.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"Run", "Environment"} {
		t.Run(kind, func(t *testing.T) {
			for _, test := range []struct {
				name                             string
				oldProject, oldUID, project, uid string
				create, allowed                  bool
			}{
				{name: "legacy-create", create: true, project: "p", allowed: true},
				{name: "exact-create", create: true, project: "p", uid: "opaque-selected-A", allowed: true},
				{name: "uid-without-project", create: true, uid: "A"},
				{name: "uid-whitespace", create: true, project: "p", uid: " A"},
				{name: "uid-too-long", create: true, project: "p", uid: strings.Repeat("a", 129)},
				{name: "unchanged", oldProject: "p", oldUID: "A", project: "p", uid: "A", allowed: true},
				{name: "removed", oldProject: "p", oldUID: "A", project: "p"},
				{name: "changed", oldProject: "p", oldUID: "A", project: "p", uid: "B"},
				{name: "legacy-retrofit", oldProject: "p", project: "p", uid: "A"},
				{name: "atomic-binding", project: "p", uid: "A", allowed: kind == "Environment"},
				{name: "legacy-unchanged", oldProject: "p", project: "p", allowed: true},
			} {
				t.Run(test.name, func(t *testing.T) {
					obj := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": GroupVersion.String(), "kind": kind, "metadata": map[string]interface{}{"name": strings.ToLower(kind) + "-" + test.name, "namespace": ns.Name}, "spec": map[string]interface{}{"templateRef": "small"}}}
					spec := obj.Object["spec"].(map[string]interface{})
					if kind == "Run" {
						spec["agent"] = "test"
						spec["prompt"] = "keyless admission"
					}
					set := func(p, u string) {
						delete(spec, "projectRef")
						delete(spec, "projectUID")
						if p != "" {
							spec["projectRef"] = p
						}
						if u != "" {
							spec["projectUID"] = u
						}
					}
					if !test.create {
						set(test.oldProject, test.oldUID)
						if err := c.Create(ctx, obj); err != nil {
							t.Fatal(err)
						}
						// Create decodes the server object into a fresh map.
						spec = obj.Object["spec"].(map[string]interface{})
					}
					set(test.project, test.uid)
					var err error
					if test.create {
						err = c.Create(ctx, obj, &client.CreateOptions{FieldValidation: metav1.FieldValidationStrict})
					} else {
						err = c.Update(ctx, obj)
					}
					if test.allowed && err != nil {
						t.Fatal(err)
					}
					if !test.allowed && !apierrors.IsInvalid(err) {
						t.Fatalf("expected admission rejection, got %v", err)
					}
					if test.allowed {
						if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
							t.Fatal(err)
						}
						uid, _, _ := unstructured.NestedString(obj.Object, "spec", "projectUID")
						if uid != test.uid {
							t.Fatalf("expectation pruned: %q", uid)
						}
					}
				})
			}
		})
	}
	// The exclusive existing-Environment mode must not acquire Project semantics.
	run := &Run{ObjectMeta: metav1.ObjectMeta{Name: "exclusive", Namespace: ns.Name}, Spec: RunSpec{EnvironmentRef: "existing", ProjectUID: "A", Agent: "test", Prompt: "test"}}
	if err := c.Create(ctx, run); !apierrors.IsInvalid(err) {
		t.Fatalf("exclusive selector accepted: %v", err)
	}
	// Upgrade safety: strict transport rejects an expectation unknown to the
	// installed schema instead of pruning it and acknowledging name-only intent.
	unknown := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": GroupVersion.String(), "kind": "Run", "metadata": map[string]interface{}{"name": "unknown-expectation", "namespace": ns.Name}, "spec": map[string]interface{}{"projectRef": "p", "futureProjectUID": "A", "agent": "test", "prompt": "test"}}}
	if err := c.Create(ctx, unknown, &client.CreateOptions{FieldValidation: metav1.FieldValidationStrict}); !apierrors.IsBadRequest(err) {
		t.Fatalf("strict unknown selection accepted: %v", err)
	}
}
