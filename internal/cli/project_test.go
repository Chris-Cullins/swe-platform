package cli

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
)

func projectScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := networkingv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := rbacv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}
func projectClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(projectScheme(t)).WithStatusSubresource(&platformv1alpha1.Project{}, &platformv1alpha1.EnvironmentTemplate{}, &platformv1alpha1.Run{}, &platformv1alpha1.Environment{}).WithObjects(objs...).Build()
}
func install() *platformv1alpha1.Installation {
	return &platformv1alpha1.Installation{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "i-1", Annotations: map[string]string{tenancy.OperatorServiceAccountAnnotation: "operator", tenancy.OperatorClusterRoleAnnotation: "operator-role", tenancy.ControlPlaneServiceAccountAnnotation: "control", tenancy.ControlPlaneClusterRoleAnnotation: "control-role"}}}
}
func claim(l tenancy.Lifecycle, op string) map[string]string {
	return map[string]string{tenancy.InstallationNamespaceAnnotation: "system", tenancy.InstallationNameAnnotation: "main", tenancy.InstallationUIDAnnotation: "i-1", tenancy.ProjectNameAnnotation: "app", tenancy.ProjectUIDAnnotation: "p-1", tenancy.LifecycleAnnotation: string(l), tenancy.LifecycleOperationAnnotation: op}
}
func quotaArgs() []string {
	r := make([]string, len(quotaKeys))
	for i, k := range quotaKeys {
		r[i] = string(k) + "=1"
	}
	return r
}
func options() onboardOptions {
	return onboardOptions{namespace: "team", systemNamespace: "system", installation: "main", repository: "https://example/repo", defaultTemplate: "small", templates: []string{"small"}, quota: quotaArgs()}
}
func source(rev, image string) *platformv1alpha1.EnvironmentTemplate {
	return &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "system", UID: "source-1", Annotations: map[string]string{tenancy.CatalogSourceAnnotation: "true", tenancy.InstallationNamespaceAnnotation: "system", tenancy.InstallationNameAnnotation: "main", tenancy.InstallationUIDAnnotation: "i-1", tenancy.CatalogNameAnnotation: "small", tenancy.CatalogRevisionAnnotation: rev}}, Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: image, Size: "small"}}
}
func projectFixture(l tenancy.Lifecycle, op string) (*corev1.Namespace, *platformv1alpha1.Project) {
	n := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team", UID: "n-1", Annotations: claim(l, op)}}
	p := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "team", UID: "p-1", Annotations: claim(l, op)}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://example/repo"}, TemplateRef: "small", ChangesWorkflow: platformv1alpha1.ChangesWorkflowBranchPR}}
	return n, p
}

func TestParseOnboardRequiresCompletePositiveQuota(t *testing.T) {
	o := options()
	hard, err := parseOnboard(o)
	if err != nil || len(hard) != len(quotaKeys) {
		t.Fatalf("complete quota: %v %#v", err, hard)
	}
	o.quota[0] = strings.Split(o.quota[0], "=")[0] + "=0"
	if _, err = parseOnboard(o); err == nil {
		t.Fatal("zero quota accepted")
	}
	o = options()
	o.quota = o.quota[:len(o.quota)-1]
	if _, err = parseOnboard(o); err == nil {
		t.Fatal("incomplete quota accepted")
	}
}

func TestOnboardSyncsExactClaimTemplateAndBaseline(t *testing.T) {
	n, p := projectFixture(tenancy.LifecycleFencing, "onboarding")
	c := projectClient(t, install(), n, p, source("r1", "image:v1"))
	var out bytes.Buffer
	if err := onboardProject(context.Background(), c, "app", options(), &out); err != nil {
		t.Fatal(err)
	}
	var ns corev1.Namespace
	_ = c.Get(context.Background(), client.ObjectKey{Name: "team"}, &ns)
	if ns.Annotations[tenancy.LifecycleAnnotation] != "active" || ns.Annotations[tenancy.ProjectUIDAnnotation] != "p-1" {
		t.Fatalf("claim not activated: %#v", ns.Annotations)
	}
	var tmpl platformv1alpha1.EnvironmentTemplate
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "team", Name: "small"}, &tmpl); err != nil {
		t.Fatal(err)
	}
	if tmpl.Spec.Image != "image:v1" || tmpl.Annotations[tenancy.CatalogSourceUIDAnnotation] != "source-1" {
		t.Fatalf("template: %#v", tmpl)
	}
	var sa corev1.ServiceAccount
	_ = c.Get(context.Background(), client.ObjectKey{Namespace: "team", Name: tenancy.EnvironmentServiceAccount}, &sa)
	if sa.AutomountServiceAccountToken == nil || *sa.AutomountServiceAccountToken {
		t.Fatal("SA token automount enabled")
	}
	var np networkingv1.NetworkPolicy
	_ = c.Get(context.Background(), client.ObjectKey{Namespace: "team", Name: tenancy.BaselineIngressPolicy}, &np)
	if !reflect.DeepEqual(np.Spec.PolicyTypes, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}) || len(np.Spec.Egress) != 0 {
		t.Fatalf("network policy: %#v", np.Spec)
	}
	var rb rbacv1.RoleBinding
	_ = c.Get(context.Background(), client.ObjectKey{Namespace: "team", Name: tenancy.OperatorRoleBinding}, &rb)
	if rb.RoleRef.Kind != "ClusterRole" || rb.RoleRef.Name != "operator-role" || len(rb.Subjects) != 1 || rb.Subjects[0].Namespace != "system" || rb.Subjects[0].Name != "operator" {
		t.Fatalf("role binding: %#v", rb)
	}
	var rq corev1.ResourceQuota
	_ = c.Get(context.Background(), client.ObjectKey{Namespace: "team", Name: tenancy.BaselineResourceQuota}, &rq)
	hard, _ := parseOnboard(options())
	if !reflect.DeepEqual(rq.Spec.Hard, hard) {
		t.Fatalf("quota differs: %#v", rq.Spec.Hard)
	}
}

func TestTemplateDriftSyncPreservesUIDAndStatus(t *testing.T) {
	n, p := projectFixture(tenancy.LifecycleActive, "")
	dst := source("old", "old")
	dst.Namespace = "team"
	dst.UID = "template-1"
	dst.Annotations = annotations(tenancy.InstallationIdentity{Key: types.NamespacedName{Namespace: "system", Name: "main"}, UID: "i-1"}, "app")
	dst.Annotations[tenancy.ProjectUIDAnnotation] = "p-1"
	dst.Annotations[tenancy.CatalogNameAnnotation] = "small"
	dst.Annotations[tenancy.CatalogRevisionAnnotation] = "old"
	dst.Annotations[tenancy.CatalogSourceUIDAnnotation] = "replaced-source"
	dst.Status.WarmPoolReady = 7
	c := projectClient(t, install(), n, p, source("r2", "new"), dst)
	if err := onboardProject(context.Background(), c, "app", options(), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var got platformv1alpha1.EnvironmentTemplate
	_ = c.Get(context.Background(), client.ObjectKey{Namespace: "team", Name: "small"}, &got)
	if got.UID != "template-1" || got.Status.WarmPoolReady != 7 || got.Spec.Image != "new" || got.Annotations[tenancy.CatalogRevisionAnnotation] != "r2" {
		t.Fatalf("drift sync: %#v", got)
	}
}

func TestDeletedCatalogSourceRetainsManagedCopy(t *testing.T) {
	id := tenancy.InstallationIdentity{Key: types.NamespacedName{Namespace: "system", Name: "main"}, UID: "i-1"}
	dst := source("r1", "retained")
	dst.Namespace = "team"
	dst.UID = "template-1"
	dst.Annotations = annotations(id, "app")
	dst.Annotations[tenancy.ProjectUIDAnnotation] = "p-1"
	dst.Annotations[tenancy.CatalogNameAnnotation] = "small"
	dst.Annotations[tenancy.CatalogRevisionAnnotation] = "r1"
	dst.Annotations[tenancy.CatalogSourceUIDAnnotation] = "deleted-source"
	c := projectClient(t, dst)
	if err := syncTemplates(context.Background(), c, id, "p-1", "app", "team", []string{"small"}); err == nil {
		t.Fatal("sync without a catalog source succeeded")
	}
	var retained platformv1alpha1.EnvironmentTemplate
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "team", Name: "small"}, &retained); err != nil {
		t.Fatalf("managed copy was deleted with its source: %v", err)
	}
	if retained.UID != "template-1" || retained.Spec.Image != "retained" {
		t.Fatalf("managed copy changed after source deletion: %#v", retained)
	}
}

func TestTemplateCollisionsAndAdoptionFences(t *testing.T) {
	id := tenancy.InstallationIdentity{Key: types.NamespacedName{Namespace: "system", Name: "main"}, UID: "i-1"}
	src := source("r1", "new")
	for name, mutate := range map[string]func(*platformv1alpha1.EnvironmentTemplate){"unowned": func(d *platformv1alpha1.EnvironmentTemplate) { d.Annotations = nil }, "stale installation": func(d *platformv1alpha1.EnvironmentTemplate) {
		d.Annotations[tenancy.InstallationUIDAnnotation] = "old"
	}} {
		t.Run(name, func(t *testing.T) {
			d := source("r1", "old")
			d.Namespace = "team"
			d.UID = "dst"
			d.Annotations = annotations(id, "app")
			d.Annotations[tenancy.ProjectUIDAnnotation] = "p-1"
			d.Annotations[tenancy.CatalogNameAnnotation] = "small"
			d.Annotations[tenancy.CatalogSourceUIDAnnotation] = "source-1"
			mutate(d)
			c := projectClient(t, src, d)
			if err := syncTemplates(context.Background(), c, id, "p-1", "app", "team", []string{"small"}); err == nil {
				t.Fatal("collision accepted")
			}
		})
	}
	n, p := projectFixture(tenancy.LifecycleActive, "")
	n.Annotations = nil
	c := projectClient(t, install(), n, p, src)
	if err := onboardProject(context.Background(), c, "app", options(), &bytes.Buffer{}); err == nil {
		t.Fatal("adoption without --adopt accepted")
	}
	n, p = projectFixture(tenancy.LifecycleActive, "")
	n.Annotations, p.Annotations = nil, nil
	c = projectClient(t, install(), n, p, src)
	o := options()
	o.adopt = true
	if err := onboardProject(context.Background(), c, "app", o, &bytes.Buffer{}); err != nil {
		t.Fatalf("explicit adoption failed: %v", err)
	}
	n, p = projectFixture(tenancy.LifecycleActive, "")
	p2 := p.DeepCopy()
	p2.Name = "other"
	p2.UID = "p-2"
	c = projectClient(t, install(), n, p, p2, src)
	o = options()
	o.adopt = true
	if err := onboardProject(context.Background(), c, "app", o, &bytes.Buffer{}); err == nil {
		t.Fatal("multiple projects accepted")
	}
	n, p = projectFixture(tenancy.LifecycleActive, "")
	n.Annotations[tenancy.InstallationUIDAnnotation] = "other"
	c = projectClient(t, install(), n, p, src)
	if err := onboardProject(context.Background(), c, "app", options(), &bytes.Buffer{}); err == nil {
		t.Fatal("conflicting namespace claim accepted")
	}
}

func TestOnboardingRefusesPartialClaimsAndProjectReplacement(t *testing.T) {
	t.Run("partial Namespace authority", func(t *testing.T) {
		n, p := projectFixture(tenancy.LifecycleActive, "")
		n.Annotations = map[string]string{tenancy.InstallationNameAnnotation: "main"}
		p.Annotations = nil
		c := projectClient(t, install(), n, p, source("r1", "v"))
		o := options()
		o.adopt = true
		if err := onboardProject(context.Background(), c, "app", o, &bytes.Buffer{}); err == nil {
			t.Fatal("partial Namespace claim was overwritten")
		}
	})

	t.Run("foreign Project authority", func(t *testing.T) {
		n, p := projectFixture(tenancy.LifecycleActive, "")
		n.Annotations = nil
		p.Annotations[tenancy.InstallationUIDAnnotation] = "foreign"
		c := projectClient(t, install(), n, p, source("r1", "v"))
		o := options()
		o.adopt = true
		if err := onboardProject(context.Background(), c, "app", o, &bytes.Buffer{}); err == nil {
			t.Fatal("foreign Project claim was overwritten")
		}
	})

	t.Run("same-name Project replacement", func(t *testing.T) {
		n, _ := projectFixture(tenancy.LifecycleActive, "")
		replacement := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "team", UID: "p-2", Annotations: claim(tenancy.LifecycleActive, "")}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://example/repo"}, TemplateRef: "small", ChangesWorkflow: platformv1alpha1.ChangesWorkflowBranchPR}}
		replacement.Annotations[tenancy.ProjectUIDAnnotation] = "p-2"
		c := projectClient(t, install(), n, replacement, source("r1", "v"))
		if err := onboardProject(context.Background(), c, "app", options(), &bytes.Buffer{}); err == nil {
			t.Fatal("same-name replacement Project was accepted")
		}
	})

	t.Run("missing claimed Project", func(t *testing.T) {
		n, _ := projectFixture(tenancy.LifecycleActive, "")
		c := projectClient(t, install(), n, source("r1", "v"))
		if err := onboardProject(context.Background(), c, "app", options(), &bytes.Buffer{}); err == nil {
			t.Fatal("missing claimed Project was recreated")
		}
	})
}

func TestInterruptedOnboardingRequiresMatchingOperation(t *testing.T) {
	for _, op := range []string{"onboarding", "offboarding"} {
		t.Run(op, func(t *testing.T) {
			n, p := projectFixture(tenancy.LifecycleFencing, op)
			c := projectClient(t, install(), n, p, source("r1", "v"))
			err := onboardProject(context.Background(), c, "app", options(), &bytes.Buffer{})
			if (op == "onboarding") != (err == nil) {
				t.Fatalf("operation %s: %v", op, err)
			}
		})
	}
}

func TestOffboardingPublishesIntentsAndTimeoutKeepsResources(t *testing.T) {
	n, p := projectFixture(tenancy.LifecycleActive, "")
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "team", UID: "r-1"}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "team", UID: "e-1"}}
	c := projectClient(t, install(), n, p, run, env)
	err := offboardProject(context.Background(), c, "app", "team", "system", "main", time.Nanosecond, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("expected timeout: %v", err)
	}
	var gotRun platformv1alpha1.Run
	_ = c.Get(context.Background(), client.ObjectKey{Namespace: "team", Name: "run"}, &gotRun)
	var gotEnv platformv1alpha1.Environment
	_ = c.Get(context.Background(), client.ObjectKey{Namespace: "team", Name: "env"}, &gotEnv)
	if !gotRun.Spec.Cancel || gotEnv.Spec.Lifecycle.Hold == nil || !gotEnv.Spec.Lifecycle.Hold.Enabled {
		t.Fatalf("intents missing: %#v %#v", gotRun.Spec, gotEnv.Spec)
	}
	var gotNS corev1.Namespace
	_ = c.Get(context.Background(), client.ObjectKey{Name: "team"}, &gotNS)
	if gotNS.Annotations[tenancy.LifecycleAnnotation] != "fencing" {
		t.Fatal("timeout did not retain fencing")
	}
}

func TestOffboardingDrainedAndIdentityFences(t *testing.T) {
	n, p := projectFixture(tenancy.LifecycleFencing, "offboarding")
	c := projectClient(t, install(), n, p)
	if err := offboardProject(context.Background(), c, "app", "team", "system", "main", time.Second, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var got corev1.Namespace
	_ = c.Get(context.Background(), client.ObjectKey{Name: "team"}, &got)
	if got.Annotations[tenancy.LifecycleAnnotation] != "fenced" {
		t.Fatal("not fenced")
	}
	var still platformv1alpha1.Project
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "team", Name: "app"}, &still); err != nil {
		t.Fatalf("project deleted: %v", err)
	}
	t.Run("namespace UID", func(t *testing.T) {
		n, p := projectFixture(tenancy.LifecycleActive, "")
		n.UID = "n-2"
		c := projectClient(t, install(), n, p)
		id := tenancy.InstallationIdentity{Key: types.NamespacedName{Namespace: "system", Name: "main"}, UID: "i-1"}
		// Offboarding captures the live UID itself. checkNamespace is the exact
		// boundary which detects replacement after that capture.
		if err := checkNamespace(context.Background(), c, "team", "n-1", id, "app"); err == nil {
			t.Fatal("replacement UID accepted")
		}
	})
	t.Run("project UID", func(t *testing.T) {
		n, p := projectFixture(tenancy.LifecycleActive, "")
		n.Annotations[tenancy.ProjectUIDAnnotation] = "wrong"
		c := projectClient(t, install(), n, p)
		if err := offboardProject(context.Background(), c, "app", "team", "system", "main", time.Nanosecond, &bytes.Buffer{}); err == nil {
			t.Fatal("conflict accepted")
		}
	})
}
