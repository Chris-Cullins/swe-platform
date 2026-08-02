package controllers

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/controlplaneclient"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	"github.com/Chris-Cullins/swe-platform/internal/sandboxclient"
	"github.com/Chris-Cullins/swe-platform/internal/serviceconfig"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

type declaredConnectorFake struct {
	file       sandboxclient.WorkspaceServicesFile
	readErr    error
	reads      []lifecycle.ExecutionFence
	reconciles []declaredReconcileCall
	reconcile  func(declaredReconcileCall) error
}

type declaredReconcileCall struct {
	fence                 lifecycle.ExecutionFence
	decls                 []platformv1alpha1.EnvironmentServiceDeclaration
	routes                []platformv1alpha1.EnvironmentPortalRoute
	intent, routeRevision uint64
	specs                 []*sandboxdv1.ManagedServiceSpec
}

func (f *declaredConnectorFake) ReadWorkspaceServices(_ context.Context, fence lifecycle.ExecutionFence) (sandboxclient.WorkspaceServicesFile, error) {
	f.reads = append(f.reads, fence)
	return f.file, f.readErr
}
func (f *declaredConnectorFake) ReconcileRepositoryServices(_ context.Context, fence lifecycle.ExecutionFence, d []platformv1alpha1.EnvironmentServiceDeclaration, routes []platformv1alpha1.EnvironmentPortalRoute, intent, routeRevision uint64, specs []*sandboxdv1.ManagedServiceSpec) error {
	call := declaredReconcileCall{
		fence: fence, decls: append([]platformv1alpha1.EnvironmentServiceDeclaration(nil), d...),
		routes: append([]platformv1alpha1.EnvironmentPortalRoute(nil), routes...), intent: intent, routeRevision: routeRevision, specs: specs,
	}
	f.reconciles = append(f.reconciles, call)
	if f.reconcile != nil {
		return f.reconcile(call)
	}
	return nil
}

type declaredRoutesFake struct {
	route controlplaneclient.PortalRoute
	hook  func()
	err   error
	calls int
}

func (f *declaredRoutesFake) GetPortalRoute(context.Context, string, string, string) (controlplaneclient.PortalRoute, error) {
	f.calls++
	if f.hook != nil {
		f.hook()
	}
	return f.route, f.err
}

func declaredEnvironment() *platformv1alpha1.Environment {
	return &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "ns", UID: types.UID("env-uid"), Generation: 4}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: "default"}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady, ObservedGeneration: 4, ExecutionGeneration: 2, Lifecycle: platformv1alpha1.EnvironmentLifecycleStatus{Epoch: 3}, Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 4}}}}
}

func declaredClient(t *testing.T, env *platformv1alpha1.Environment) client.WithWatch {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build()
}

func runDeclared(t *testing.T, c client.Client, connector *declaredConnectorFake, routes *declaredRoutesFake) ctrl.Result {
	t.Helper()
	result, err := (&DeclaredServiceReconciler{Client: c, APIReader: c, Connector: connector, Routes: routes}).Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "ns", Name: "env"}})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestDeclaredServiceReconcilerConvergesThenLaunchesFreshGeneration(t *testing.T) {
	env := declaredEnvironment()
	c := declaredClient(t, env)
	connector := &declaredConnectorFake{file: sandboxclient.WorkspaceServicesFile{Data: []byte("version: 1\nservices:\n  web:\n    command: [server, --direct]\n    port: 8080\n")}}
	routes := &declaredRoutesFake{}
	runDeclared(t, c, connector, routes)
	if len(connector.reconciles) != 1 || len(connector.reconciles[0].specs) != 0 || connector.reconciles[0].intent != 5 || connector.reconciles[0].routeRevision != 0 || routes.calls != 0 {
		t.Fatalf("first declaration reconcile did not fence the old set: %#v", connector.reconciles)
	}
	var current platformv1alpha1.Environment
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
		t.Fatal(err)
	}
	if len(current.Spec.Services) != 1 {
		t.Fatalf("services = %#v", current.Spec.Services)
	}
	d := current.Spec.Services[0]
	if d.Source != platformv1alpha1.EnvironmentServiceSourceRepository || d.Name != "web" || d.TargetPort != 8080 || d.Revision != 1 {
		t.Fatalf("canonical declaration = %#v", d)
	}
	current.Generation++
	current.Status.ObservedGeneration = current.Generation
	current.Status.Conditions[0].ObservedGeneration = current.Generation
	current.Status.PortalRoutes = []platformv1alpha1.EnvironmentPortalRoute{{Name: d.Name, Active: true, DeclarationInstanceID: d.InstanceID, DeclarationRevision: d.Revision, Generation: 9}}
	if err := c.Update(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
		t.Fatal(err)
	}
	current.Status.ObservedGeneration = current.Generation
	current.Status.Conditions[0].ObservedGeneration = current.Generation
	current.Status.PortalRoutes = []platformv1alpha1.EnvironmentPortalRoute{{Name: d.Name, Active: true, DeclarationInstanceID: d.InstanceID, DeclarationRevision: d.Revision, Generation: 9}}
	if err := c.Status().Update(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	routes.route = controlplaneclient.PortalRoute{URL: "https://web.example", EnvironmentUID: string(current.UID), Service: d.Name, DeclarationInstanceID: d.InstanceID, Revision: d.Revision, RouteGeneration: 9}
	runDeclared(t, c, connector, routes)
	if len(connector.reconciles) != 2 {
		t.Fatalf("reconcile calls = %d", len(connector.reconciles))
	}
	call := connector.reconciles[1]
	if call.intent != uint64(current.Generation) || call.routeRevision != 9 || call.fence.EnvironmentUID() != current.UID || len(call.specs) != 1 || len(call.routes) != 1 {
		t.Fatalf("call = %#v", call)
	}
	spec := call.specs[0]
	if !reflect.DeepEqual(spec.Spec.Argv, []string{"server", "--direct"}) || !reflect.DeepEqual(spec.Spec.Env, map[string]string{"PORT": "8080", "PUBLIC_URL": "https://web.example"}) || spec.Spec.EnvMode != sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_INHERIT {
		t.Fatalf("managed spec = %#v", spec)
	}
}

func TestDeclaredServiceReconcilerPublishesEmptySetForDisabledPortalAuthority(t *testing.T) {
	env := declaredEnvironment()
	declaration := platformv1alpha1.EnvironmentServiceDeclaration{
		Name: "web", Source: platformv1alpha1.EnvironmentServiceSourceRepository, InstanceID: "abcdefghijklmnopqrst", Revision: 1,
		Protocol: platformv1alpha1.EnvironmentServiceProtocolHTTP, Visibility: platformv1alpha1.EnvironmentServiceVisibilityProject, Readiness: platformv1alpha1.EnvironmentServiceReadinessTCPConnect, TargetPort: 8080,
		Launch: &platformv1alpha1.EnvironmentServiceLaunch{Argv: []platformv1alpha1.EnvironmentServiceLaunchArgument{"server"}},
	}
	env.Spec.Services = []platformv1alpha1.EnvironmentServiceDeclaration{declaration}
	env.Status.NextPortalRouteGeneration = 10
	env.Status.PortalRoutes = []platformv1alpha1.EnvironmentPortalRoute{{Name: declaration.Name, Active: false, DeclarationInstanceID: declaration.InstanceID, DeclarationRevision: declaration.Revision, Generation: 10}}
	c := declaredClient(t, env)
	connector := &declaredConnectorFake{file: sandboxclient.WorkspaceServicesFile{Data: []byte("version: 1\nservices:\n  web:\n    command: [server]\n    port: 8080\n")}}
	routes := &declaredRoutesFake{route: controlplaneclient.PortalRoute{Disabled: true, EnvironmentUID: string(env.UID), Service: declaration.Name, DeclarationInstanceID: declaration.InstanceID, Revision: declaration.Revision, RouteGeneration: 10}}
	runDeclared(t, c, connector, routes)
	if len(connector.reconciles) != 1 {
		t.Fatalf("reconcile calls = %d", len(connector.reconciles))
	}
	call := connector.reconciles[0]
	if len(call.specs) != 0 || len(call.routes) != 1 || call.routes[0].Active || call.routeRevision != 10 {
		t.Fatalf("disabled route reconcile = %#v", call)
	}
}

func TestDeclaredServiceReconcilerMissingMalformedAndCollision(t *testing.T) {
	api := platformv1alpha1.EnvironmentServiceDeclaration{Name: "api", Source: platformv1alpha1.EnvironmentServiceSourceAPI, InstanceID: "api-instance-abcdefghijkl", Revision: 1, TargetPort: 9000}
	repo := platformv1alpha1.EnvironmentServiceDeclaration{Name: "old", Source: platformv1alpha1.EnvironmentServiceSourceRepository, InstanceID: "repo-instance-abcdefghijk", Revision: 1, TargetPort: 9001, Launch: &platformv1alpha1.EnvironmentServiceLaunch{Argv: []platformv1alpha1.EnvironmentServiceLaunchArgument{"old"}}}
	for _, tc := range []struct {
		name     string
		file     sandboxclient.WorkspaceServicesFile
		services []platformv1alpha1.EnvironmentServiceDeclaration
		want     int
		launch   bool
	}{
		{"missing removes repository", sandboxclient.WorkspaceServicesFile{Missing: true}, []platformv1alpha1.EnvironmentServiceDeclaration{api, repo}, 1, true},
		{"malformed preserves", sandboxclient.WorkspaceServicesFile{Data: []byte("services: [")}, []platformv1alpha1.EnvironmentServiceDeclaration{api, repo}, 2, false},
		{"API collision preserves", sandboxclient.WorkspaceServicesFile{Data: []byte("version: 1\nservices:\n  api:\n    command: [x]\n")}, []platformv1alpha1.EnvironmentServiceDeclaration{api}, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := declaredEnvironment()
			env.Spec.Services = tc.services
			c := declaredClient(t, env)
			connector := &declaredConnectorFake{file: tc.file}
			routes := &declaredRoutesFake{}
			runDeclared(t, c, connector, routes)
			var got platformv1alpha1.Environment
			_ = c.Get(context.Background(), client.ObjectKeyFromObject(env), &got)
			wantReconciles := 0
			if tc.launch {
				wantReconciles = 1
			}
			if len(got.Spec.Services) != tc.want || routes.calls != 0 || len(connector.reconciles) != wantReconciles {
				t.Fatalf("services=%#v routes=%d launches=%d", got.Spec.Services, routes.calls, len(connector.reconciles))
			}
			if tc.launch {
				if len(connector.reconciles[0].specs) != 0 || connector.reconciles[0].intent != uint64(env.Generation+1) {
					t.Fatalf("empty managed set not sent: %#v", connector.reconciles)
				}
			}
		})
	}
}

func TestDeclaredServiceReconcilerCombinedCapacityDoesNotPatchOrReconcile(t *testing.T) {
	env := declaredEnvironment()
	api := platformv1alpha1.EnvironmentServiceDeclaration{Name: "api", Source: platformv1alpha1.EnvironmentServiceSourceAPI, InstanceID: "api-instance-abcdefghijkl", Revision: 1, TargetPort: 9000}
	env.Spec.Services = []platformv1alpha1.EnvironmentServiceDeclaration{api}
	base := declaredClient(t, env)
	patches := 0
	c := interceptor.NewClient(base, interceptor.Funcs{Patch: func(ctx context.Context, underlying client.WithWatch, object client.Object, patch client.Patch, options ...client.PatchOption) error {
		patches++
		return underlying.Patch(ctx, object, patch, options...)
	}})
	var file strings.Builder
	file.WriteString("version: 1\nservices:\n")
	for i := 0; i < platformv1alpha1.EnvironmentServiceMaxDeclarations; i++ {
		fmt.Fprintf(&file, "  repo-%02d: {command: [serve]}\n", i)
	}
	connector := &declaredConnectorFake{file: sandboxclient.WorkspaceServicesFile{Data: []byte(file.String())}}
	routes := &declaredRoutesFake{}
	for range 2 {
		result := runDeclared(t, c, connector, routes)
		if result.RequeueAfter != declaredServiceInterval {
			t.Fatalf("overflow requeue = %s, want %s", result.RequeueAfter, declaredServiceInterval)
		}
	}
	var current platformv1alpha1.Environment
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
		t.Fatal(err)
	}
	if patches != 0 || len(connector.reconciles) != 0 || routes.calls != 0 || !reflect.DeepEqual(current.Spec.Services, []platformv1alpha1.EnvironmentServiceDeclaration{api}) {
		t.Fatalf("overflow mutated intent: patches=%d reconciles=%d routes=%d services=%#v", patches, len(connector.reconciles), routes.calls, current.Spec.Services)
	}
}

func TestDeclaredServiceReconcilerFencesOldSetBeforeRemovalDespiteRouteFailure(t *testing.T) {
	env := declaredEnvironment()
	removed := platformv1alpha1.EnvironmentServiceDeclaration{
		Name: "removed", Source: platformv1alpha1.EnvironmentServiceSourceRepository, InstanceID: "removed-instance-abcdef", Revision: 1,
		Protocol: platformv1alpha1.EnvironmentServiceProtocolHTTP, Visibility: platformv1alpha1.EnvironmentServiceVisibilityProject, Readiness: platformv1alpha1.EnvironmentServiceReadinessTCPConnect, TargetPort: 8080,
		Launch: &platformv1alpha1.EnvironmentServiceLaunch{Argv: []platformv1alpha1.EnvironmentServiceLaunchArgument{"old"}},
	}
	retained := platformv1alpha1.EnvironmentServiceDeclaration{
		Name: "retained", Source: platformv1alpha1.EnvironmentServiceSourceRepository, InstanceID: "retained-instance-abcdef", Revision: 1,
		Protocol: platformv1alpha1.EnvironmentServiceProtocolHTTP, Visibility: platformv1alpha1.EnvironmentServiceVisibilityProject, Readiness: platformv1alpha1.EnvironmentServiceReadinessTCPConnect, TargetPort: 8081,
		Launch: &platformv1alpha1.EnvironmentServiceLaunch{Argv: []platformv1alpha1.EnvironmentServiceLaunchArgument{"keep"}},
	}
	env.Spec.Services = []platformv1alpha1.EnvironmentServiceDeclaration{removed, retained}
	c := declaredClient(t, env)
	connector := &declaredConnectorFake{file: sandboxclient.WorkspaceServicesFile{Data: []byte("version: 1\nservices:\n  retained:\n    command: [keep]\n    port: 8081\n")}}
	connector.reconcile = func(call declaredReconcileCall) error {
		var current platformv1alpha1.Environment
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(current.Spec.Services, []platformv1alpha1.EnvironmentServiceDeclaration{removed, retained}) {
			t.Fatalf("durable intent changed before old process set was fenced: %#v", current.Spec.Services)
		}
		return nil
	}
	routes := &declaredRoutesFake{}
	runDeclared(t, c, connector, routes)
	if len(connector.reconciles) != 1 || connector.reconciles[0].intent != uint64(env.Generation+1) || connector.reconciles[0].routeRevision != 0 || len(connector.reconciles[0].specs) != 0 {
		t.Fatalf("old process fence = %#v", connector.reconciles)
	}
	var current platformv1alpha1.Environment
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(current.Spec.Services, []platformv1alpha1.EnvironmentServiceDeclaration{retained}) {
		t.Fatalf("converged services = %#v", current.Spec.Services)
	}
	// The fake client does not advance generation for spec writes. Model the API
	// server before proving that a retained-service discovery outage cannot
	// bypass the already accepted empty future intent.
	current.Generation++
	if err := c.Update(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
		t.Fatal(err)
	}
	current.Status.ObservedGeneration = current.Generation
	current.Status.Conditions[0].ObservedGeneration = current.Generation
	if err := c.Status().Update(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	connector.reconciles = nil
	routes.err = errors.New("portal discovery unavailable")
	runDeclared(t, c, connector, routes)
	if routes.calls != 1 || len(connector.reconciles) != 0 {
		t.Fatalf("failed discovery changed fenced process intent: routes=%d reconciles=%#v", routes.calls, connector.reconciles)
	}
}

func TestDeclaredServiceReconcilerRefusesGenerationOverflowBeforeMutation(t *testing.T) {
	env := declaredEnvironment()
	env.Generation = math.MaxInt64
	env.Status.ObservedGeneration = env.Generation
	env.Status.Conditions[0].ObservedGeneration = env.Generation
	c := declaredClient(t, env)
	connector := &declaredConnectorFake{file: sandboxclient.WorkspaceServicesFile{Data: []byte("version: 1\nservices:\n  web:\n    command: [serve]\n")}}
	runDeclared(t, c, connector, &declaredRoutesFake{})
	var current platformv1alpha1.Environment
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
		t.Fatal(err)
	}
	if len(current.Spec.Services) != 0 || len(connector.reconciles) != 0 {
		t.Fatalf("generation-exhausted mutation: services=%#v reconciles=%#v", current.Spec.Services, connector.reconciles)
	}
}

func TestDeclaredServiceReconcilerDiscardsPostRouteRaces(t *testing.T) {
	mutations := map[string]func(*platformv1alpha1.Environment){
		"declaration revision": func(e *platformv1alpha1.Environment) { e.Spec.Services[0].Revision++ },
		"execution generation": func(e *platformv1alpha1.Environment) { e.Status.ExecutionGeneration++ },
		"epoch":                func(e *platformv1alpha1.Environment) { e.Status.Lifecycle.Epoch++ },
		"hold": func(e *platformv1alpha1.Environment) {
			e.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Enabled: true, Revision: 1}
		},
		"route tombstone":  func(e *platformv1alpha1.Environment) { e.Status.PortalRoutes[0].Active = false },
		"route generation": func(e *platformv1alpha1.Environment) { e.Status.PortalRoutes[0].Generation++ },
		"replacement UID":  func(e *platformv1alpha1.Environment) { e.UID = "replacement" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			env := declaredEnvironment()
			d := platformv1alpha1.EnvironmentServiceDeclaration{
				Name: "web", Source: platformv1alpha1.EnvironmentServiceSourceRepository, InstanceID: "instance-abcdefghijklmnop", Revision: 2, TargetPort: 8080,
				Protocol: platformv1alpha1.EnvironmentServiceProtocolHTTP, Visibility: platformv1alpha1.EnvironmentServiceVisibilityProject, Readiness: platformv1alpha1.EnvironmentServiceReadinessTCPConnect,
				Launch: &platformv1alpha1.EnvironmentServiceLaunch{Argv: []platformv1alpha1.EnvironmentServiceLaunchArgument{"serve"}},
			}
			env.Spec.Services = []platformv1alpha1.EnvironmentServiceDeclaration{d}
			env.Status.PortalRoutes = []platformv1alpha1.EnvironmentPortalRoute{{Name: "web", Active: true, DeclarationInstanceID: d.InstanceID, DeclarationRevision: 2, Generation: 7}}
			c := declaredClient(t, env)
			connector := &declaredConnectorFake{file: sandboxclient.WorkspaceServicesFile{Data: []byte("version: 1\nservices:\n  web:\n    command: [serve]\n    port: 8080\n")}}
			routes := &declaredRoutesFake{route: controlplaneclient.PortalRoute{URL: "https://web", EnvironmentUID: string(env.UID), Service: "web", DeclarationInstanceID: d.InstanceID, Revision: 2, RouteGeneration: 7}}
			routes.hook = func() {
				var current platformv1alpha1.Environment
				if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
					t.Fatal(err)
				}
				mutated := current.DeepCopy()
				mutate(mutated)
				if !reflect.DeepEqual(current.Spec, mutated.Spec) || current.UID != mutated.UID {
					if err := c.Update(context.Background(), mutated); err != nil {
						t.Fatal(err)
					}
				}
				if !reflect.DeepEqual(current.Status, mutated.Status) {
					var statusCurrent platformv1alpha1.Environment
					if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &statusCurrent); err != nil {
						t.Fatal(err)
					}
					statusCurrent.Status = mutated.Status
					if err := c.Status().Update(context.Background(), &statusCurrent); err != nil {
						t.Fatal(err)
					}
				}
			}
			runDeclared(t, c, connector, routes)
			if len(connector.reconciles) != 0 {
				t.Fatalf("stale route launched: %#v", connector.reconciles)
			}
		})
	}
}

func TestConvergeRepositoryDeclarationsLifecycle(t *testing.T) {
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{UID: "uid"}}
	want := []serviceconfig.Declaration{{Name: "api", Argv: []string{"serve"}}}
	first, collision, err := convergeRepositoryDeclarations(env, want)
	if err != nil || collision != "" || len(first) != 1 || first[0].Revision != 1 || first[0].TargetPort < 49152 || len(first[0].InstanceID) < 20 {
		t.Fatalf("first = %#v, collision=%q err=%v", first, collision, err)
	}
	for _, c := range first[0].InstanceID {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			t.Fatalf("instance ID %q is not CRD-valid lowercase alphanumeric", first[0].InstanceID)
		}
	}
	env.Spec.Services = first
	second, _, _ := convergeRepositoryDeclarations(env, want)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("idempotent convergence changed: %#v", second)
	}
	want[0].Argv = []string{"serve", "--new"}
	changed, _, _ := convergeRepositoryDeclarations(env, want)
	if changed[0].Revision != 2 || changed[0].InstanceID != first[0].InstanceID || changed[0].TargetPort != first[0].TargetPort {
		t.Fatalf("changed = %#v", changed[0])
	}
	removed, _, _ := convergeRepositoryDeclarations(env, nil)
	if len(removed) != 0 {
		t.Fatalf("removed = %#v", removed)
	}
}

func TestConvergeRepositoryDeclarationsPreservesAPIAndRejectsCollisions(t *testing.T) {
	api := platformv1alpha1.EnvironmentServiceDeclaration{Name: "api", InstanceID: "abcdefghijklmnopqrst", Revision: 1, Source: platformv1alpha1.EnvironmentServiceSourceAPI, TargetPort: 8000}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{UID: "uid"}, Spec: platformv1alpha1.EnvironmentSpec{Services: []platformv1alpha1.EnvironmentServiceDeclaration{api}}}
	_, collision, _ := convergeRepositoryDeclarations(env, []serviceconfig.Declaration{{Name: "api", Argv: []string{"x"}}})
	if collision != "api" {
		t.Fatalf("API collision = %q", collision)
	}
	p := int32(9000)
	_, collision, _ = convergeRepositoryDeclarations(env, []serviceconfig.Declaration{{Name: "one", Argv: []string{"x"}, Port: &p}, {Name: "two", Argv: []string{"x"}, Port: &p}})
	if collision == "" {
		t.Fatal("explicit repository port collision accepted")
	}
	result, collision, _ := convergeRepositoryDeclarations(env, []serviceconfig.Declaration{{Name: "repo", Argv: []string{"x"}, Port: &p}})
	if collision != "" || len(result) != 2 || !reflect.DeepEqual(result[0], api) {
		t.Fatalf("API preservation result=%#v collision=%q", result, collision)
	}
}

func TestConvergeRepositoryDeclarationsEnforcesCombinedCapacity(t *testing.T) {
	api := platformv1alpha1.EnvironmentServiceDeclaration{Name: "api", InstanceID: "abcdefghijklmnopqrst", Revision: 1, Source: platformv1alpha1.EnvironmentServiceSourceAPI, TargetPort: 8000}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{UID: "uid"}, Spec: platformv1alpha1.EnvironmentSpec{Services: []platformv1alpha1.EnvironmentServiceDeclaration{api}}}
	desired := make([]serviceconfig.Declaration, platformv1alpha1.EnvironmentServiceMaxDeclarations)
	for i := range desired {
		desired[i] = serviceconfig.Declaration{Name: fmt.Sprintf("repo-%02d", i), Argv: []string{"serve"}}
	}
	boundary, collision, err := convergeRepositoryDeclarations(env, desired[:platformv1alpha1.EnvironmentServiceMaxDeclarations-1])
	if err != nil || collision != "" || len(boundary) != platformv1alpha1.EnvironmentServiceMaxDeclarations {
		t.Fatalf("combined boundary result=%d collision=%q err=%v", len(boundary), collision, err)
	}
	result, collision, err := convergeRepositoryDeclarations(env, desired)
	if err == nil || collision != "" || result != nil || !strings.Contains(err.Error(), "exceed the Environment limit of 32") {
		t.Fatalf("combined overflow result=%#v collision=%q err=%v", result, collision, err)
	}
}

func TestConvergeRepositoryDeclarationsTreatsLegacyEmptySourceAsAPI(t *testing.T) {
	legacy := platformv1alpha1.EnvironmentServiceDeclaration{Name: "legacy", InstanceID: "abcdefghijklmnopqrst", Revision: 3, TargetPort: 4321}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{UID: "uid"}, Spec: platformv1alpha1.EnvironmentSpec{Services: []platformv1alpha1.EnvironmentServiceDeclaration{legacy}}}
	result, collision, err := convergeRepositoryDeclarations(env, nil)
	if err != nil || collision != "" || !reflect.DeepEqual(result, []platformv1alpha1.EnvironmentServiceDeclaration{legacy}) {
		t.Fatalf("legacy API result=%#v collision=%q err=%v", result, collision, err)
	}
	_, collision, err = convergeRepositoryDeclarations(env, []serviceconfig.Declaration{{Name: "legacy", Argv: []string{"serve"}}})
	if err != nil || collision != "legacy" {
		t.Fatalf("legacy API collision=%q err=%v", collision, err)
	}
}

func TestConvergeRepositoryDeclarationsRejectsRetainedAndExplicitPortCollision(t *testing.T) {
	port := int32(50000)
	existing := platformv1alpha1.EnvironmentServiceDeclaration{
		Name: "retained", InstanceID: "abcdefghijklmnopqrst", Revision: 1,
		Source:     platformv1alpha1.EnvironmentServiceSourceRepository,
		Launch:     &platformv1alpha1.EnvironmentServiceLaunch{Argv: []platformv1alpha1.EnvironmentServiceLaunchArgument{"serve"}},
		TargetPort: port,
	}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{UID: "uid"}, Spec: platformv1alpha1.EnvironmentSpec{Services: []platformv1alpha1.EnvironmentServiceDeclaration{existing}}}
	_, collision, err := convergeRepositoryDeclarations(env, []serviceconfig.Declaration{
		{Name: "explicit", Argv: []string{"serve"}, Port: &port},
		{Name: "retained", Argv: []string{"serve"}},
	})
	if err != nil || collision == "" {
		t.Fatalf("collision=%q err=%v, want retained/explicit collision", collision, err)
	}
}
