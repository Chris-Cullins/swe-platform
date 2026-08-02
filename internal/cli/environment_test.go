package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
)

func TestSetEnvironmentHoldUsesMonotonicIdempotentPolicy(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{}
	environment.Name = "shared"
	environment.Namespace = "ns"
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
	key := types.NamespacedName{Namespace: environment.Namespace, Name: environment.Name}

	for _, step := range []struct {
		enabled  bool
		revision int64
	}{
		{enabled: true, revision: 1},
		{enabled: true, revision: 1},
		{enabled: false, revision: 2},
		{enabled: false, revision: 2},
		{enabled: true, revision: 3},
	} {
		revision, err := setEnvironmentHold(context.Background(), kube, key, step.enabled)
		if err != nil || revision != step.revision {
			t.Fatalf("setEnvironmentHold(%t) = revision %d, error %v; want %d", step.enabled, revision, err, step.revision)
		}
		var current platformv1alpha1.Environment
		if err := kube.Get(context.Background(), key, &current); err != nil {
			t.Fatal(err)
		}
		if current.Spec.Paused || current.Spec.Lifecycle.Hold == nil || current.Spec.Lifecycle.Hold.Enabled != step.enabled || current.Spec.Lifecycle.Hold.Revision != step.revision {
			t.Fatalf("hold policy after enabled=%t: spec=%#v", step.enabled, current.Spec)
		}
	}
}

func TestSetEnvironmentHoldRetriesConflictAndPreservesConcurrentLifecycleIntents(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{}
	environment.Name = "shared"
	environment.Namespace = "ns"
	environment.UID = "env-uid"
	environment.Spec.Lifecycle.Wake = &platformv1alpha1.EnvironmentWakeRequest{EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "wake-1", EnvironmentUID: environment.UID}}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
	key := types.NamespacedName{Namespace: environment.Namespace, Name: environment.Name}
	patches := 0
	kube := interceptor.NewClient(base, interceptor.Funcs{Patch: func(ctx context.Context, underlying client.WithWatch, object client.Object, patch client.Patch, options ...client.PatchOption) error {
		patches++
		if patches == 1 {
			var concurrent platformv1alpha1.Environment
			if err := underlying.Get(ctx, key, &concurrent); err != nil {
				return err
			}
			concurrent.Spec.Lifecycle.Suspend = &platformv1alpha1.EnvironmentSuspendRequest{
				EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "suspend-concurrent", EnvironmentUID: concurrent.UID},
				Sequence:                    1,
			}
			if err := underlying.Update(ctx, &concurrent); err != nil {
				return err
			}
			return apierrors.NewConflict(schema.GroupResource{Group: platformv1alpha1.GroupVersion.Group, Resource: "environments"}, object.GetName(), errors.New("simulated hold conflict"))
		}
		return underlying.Patch(ctx, object, patch, options...)
	}})

	revision, err := setEnvironmentHold(context.Background(), kube, key, true)
	if err != nil || revision != 1 || patches != 2 {
		t.Fatalf("conflicting hold = revision %d, patches %d, error %v", revision, patches, err)
	}
	var current platformv1alpha1.Environment
	if err := base.Get(context.Background(), key, &current); err != nil {
		t.Fatal(err)
	}
	if current.Spec.Lifecycle.Hold == nil || !current.Spec.Lifecycle.Hold.Enabled || current.Spec.Lifecycle.Hold.Revision != 1 ||
		current.Spec.Lifecycle.Wake == nil || current.Spec.Lifecycle.Wake.ID != "wake-1" ||
		current.Spec.Lifecycle.Suspend == nil || current.Spec.Lifecycle.Suspend.ID != "suspend-concurrent" {
		t.Fatalf("concurrent lifecycle intent was lost: %#v", current.Spec.Lifecycle)
	}
}

func TestSetEnvironmentHoldRejectsSameNameReplacementIncarnation(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{}
	environment.Name = "shared"
	environment.Namespace = "ns"
	environment.UID = "env-uid-old"
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
	key := types.NamespacedName{Namespace: environment.Namespace, Name: environment.Name}
	patches := 0
	kube := interceptor.NewClient(base, interceptor.Funcs{Patch: func(ctx context.Context, underlying client.WithWatch, object client.Object, patch client.Patch, options ...client.PatchOption) error {
		patches++
		if patches == 1 {
			// Between the first read and the retry, the original incarnation is
			// deleted and a same-name replacement is created with a new UID.
			var original platformv1alpha1.Environment
			if err := underlying.Get(ctx, key, &original); err != nil {
				return err
			}
			if err := underlying.Delete(ctx, &original); err != nil {
				return err
			}
			replacement := &platformv1alpha1.Environment{}
			replacement.Name = environment.Name
			replacement.Namespace = environment.Namespace
			replacement.UID = "env-uid-new"
			if err := underlying.Create(ctx, replacement); err != nil {
				return err
			}
			return apierrors.NewConflict(schema.GroupResource{Group: platformv1alpha1.GroupVersion.Group, Resource: "environments"}, object.GetName(), errors.New("simulated hold conflict"))
		}
		return underlying.Patch(ctx, object, patch, options...)
	}})

	revision, err := setEnvironmentHold(context.Background(), kube, key, true)
	if err == nil || revision != 0 {
		t.Fatalf("replacement incarnation = revision %d, error %v; want failure", revision, err)
	}
	if !errors.Is(err, lifecycle.ErrEnvironmentIncarnationChanged) {
		t.Fatalf("error = %v; want incarnation-changed error", err)
	}
	if patches != 1 {
		t.Fatalf("replacement incarnation patched %d times; want 1 (no retry patch)", patches)
	}
	var current platformv1alpha1.Environment
	if err := base.Get(context.Background(), key, &current); err != nil {
		t.Fatal(err)
	}
	if current.UID != "env-uid-new" || current.Spec.Lifecycle.Hold != nil {
		t.Fatalf("replacement incarnation was mutated: uid=%q spec=%#v", current.UID, current.Spec.Lifecycle)
	}
}

func TestSetEnvironmentHoldFailsClosedDuringLegacyPauseMigration(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "release", true: "hold"}[enabled], func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := platformv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			environment := &platformv1alpha1.Environment{}
			environment.Name = "shared"
			environment.Namespace = "ns"
			environment.Spec.Paused = true
			kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
			key := types.NamespacedName{Namespace: environment.Namespace, Name: environment.Name}

			revision, err := setEnvironmentHold(context.Background(), kube, key, enabled)
			if err == nil || revision != 0 {
				t.Fatalf("legacy pause = revision %d, error %v", revision, err)
			}
			var current platformv1alpha1.Environment
			if err := kube.Get(context.Background(), key, &current); err != nil {
				t.Fatal(err)
			}
			if !current.Spec.Paused || current.Spec.Lifecycle.Hold != nil {
				t.Fatalf("legacy spec was changed by CLI: %#v", current.Spec)
			}
		})
	}
}

func TestRootIncludesEnvironmentHoldCommands(t *testing.T) {
	root := NewRootCommand()
	for _, args := range [][]string{
		{"environment", "hold"},
		{"environment", "release"},
		{"environment", "services", "list"},
		{"environment", "services", "declare"},
		{"environment", "services", "update"},
		{"environment", "services", "remove"},
	} {
		command, _, err := root.Find(args)
		if err != nil || command == root {
			t.Fatalf("root.Find(%v) = command %q, error %v", args, command.Name(), err)
		}
	}
}

func TestEnvironmentServiceDeclarationsAreRevisionedAndIdempotent(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{}
	environment.Name = "shared"
	environment.Namespace = "ns"
	environment.UID = "env-uid"
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
	key := client.ObjectKeyFromObject(environment)

	web, err := writeEnvironmentService(context.Background(), kube, key, "web", 3000, false)
	if err != nil || !validServiceInstanceID(web.InstanceID) || web != desiredEnvironmentService("web", 3000, 1, web.InstanceID) {
		t.Fatalf("declare web = %#v, %v", web, err)
	}
	web, err = writeEnvironmentService(context.Background(), kube, key, "web", 3000, false)
	if err != nil || web.Revision != 1 {
		t.Fatalf("idempotent declare web = %#v, %v", web, err)
	}
	if _, err := writeEnvironmentService(context.Background(), kube, key, "web", 3001, false); err == nil || !strings.Contains(err.Error(), "use update") {
		t.Fatalf("different declare error = %v", err)
	}
	if _, err := writeEnvironmentService(context.Background(), kube, key, "alias", 3000, false); err != nil {
		t.Fatalf("declare duplicate-port alias: %v", err)
	}
	instanceID := web.InstanceID
	web, err = writeEnvironmentService(context.Background(), kube, key, "web", 3001, true)
	if err != nil || web != desiredEnvironmentService("web", 3001, 2, instanceID) {
		t.Fatalf("update web = %#v, %v", web, err)
	}
	web, err = writeEnvironmentService(context.Background(), kube, key, "web", 3001, true)
	if err != nil || web.Revision != 2 {
		t.Fatalf("idempotent update web = %#v, %v", web, err)
	}

	var output bytes.Buffer
	if err := listEnvironmentServices(context.Background(), kube, key, &output); err != nil {
		t.Fatal(err)
	}
	want := "NAME\tREVISION\tPROTOCOL\tTARGET-PORT\tVISIBILITY\tREADINESS\tSTATE\tREASON\tOBSERVED-AT\tFRESHNESS\n" +
		"alias\t1\tHTTP\t3000\tProject\tTCPConnect\t-\t-\t-\tNO-OBSERVATION\n" +
		"web\t2\tHTTP\t3001\tProject\tTCPConnect\t-\t-\t-\tNO-OBSERVATION\n"
	if output.String() != want {
		t.Fatalf("service list:\n%s\nwant:\n%s", output.String(), want)
	}

	if err := removeEnvironmentService(context.Background(), kube, key, "web"); err != nil {
		t.Fatal(err)
	}
	if err := removeEnvironmentService(context.Background(), kube, key, "web"); err != nil {
		t.Fatalf("idempotent remove: %v", err)
	}
	web, err = writeEnvironmentService(context.Background(), kube, key, "web", 3002, false)
	if err != nil || web.Revision != 1 {
		t.Fatalf("same-name re-declare = %#v, %v", web, err)
	}
}

func TestEnvironmentServiceUpdateBackfillsLegacyInstanceID(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{
		Services: []platformv1alpha1.EnvironmentServiceDeclaration{desiredEnvironmentService("web", 3000, 4)},
	}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
	service, err := writeEnvironmentService(context.Background(), kube, client.ObjectKeyFromObject(environment), "web", 3000, true)
	if err != nil {
		t.Fatal(err)
	}
	if service.Revision != 5 || !validServiceInstanceID(service.InstanceID) || service.TargetPort != 3000 {
		t.Fatalf("legacy backfill = %#v", service)
	}
	stableID := service.InstanceID
	service, err = writeEnvironmentService(context.Background(), kube, client.ObjectKeyFromObject(environment), "web", 3000, true)
	if err != nil || service.Revision != 5 || service.InstanceID != stableID {
		t.Fatalf("idempotent migrated update = %#v, %v", service, err)
	}
}

func TestServiceListCurrentRequiresExecutionAndWallClockFreshness(t *testing.T) {
	now := time.Unix(1000, 0)
	execution := int64(4)
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "n", UID: "u", Generation: 2}, Spec: platformv1alpha1.EnvironmentSpec{Services: []platformv1alpha1.EnvironmentServiceDeclaration{desiredEnvironmentService("web", 3000, 1)}}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady, ObservedGeneration: 2, ExecutionGeneration: execution, Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 2}}, ServiceObservations: &platformv1alpha1.EnvironmentServiceObservations{ObservedGeneration: 2, ExecutionGeneration: &execution, ObservedAt: metav1.NewTime(now.Add(-time.Second)), Records: []platformv1alpha1.EnvironmentServiceObservation{{Name: "web", DeclarationRevision: 1, State: platformv1alpha1.EnvironmentServiceObservationHealthy}}}}}
	scheme := runtime.NewScheme()
	_ = platformv1alpha1.AddToScheme(scheme)
	list := func(at time.Time) string {
		var out bytes.Buffer
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(env.DeepCopy()).Build()
		if err := listEnvironmentServicesAt(context.Background(), c, types.NamespacedName{Namespace: "n", Name: "e"}, &out, at); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	if !strings.Contains(list(now), "\tCURRENT\n") {
		t.Fatal("exact fresh execution was not current")
	}
	deleting := metav1.NewTime(now)
	env.DeletionTimestamp = &deleting
	env.Finalizers = []string{"test"}
	if !strings.Contains(list(now), "\tSTALE\n") {
		t.Fatal("deleting environment retained a current observation")
	}
	env.DeletionTimestamp = nil
	env.Finalizers = nil
	if !strings.Contains(list(now.Add(serviceObservationMaxAge+time.Second)), "\tSTALE\n") {
		t.Fatal("old observation remained current")
	}
	*env.Status.ServiceObservations.ExecutionGeneration = 3
	if !strings.Contains(list(now), "\tSTALE\n") {
		t.Fatal("old execution remained current")
	}
	env.Status.ServiceObservations.ExecutionGeneration = nil
	env.Status.ServiceObservations.Records[0].State = platformv1alpha1.EnvironmentServiceObservationPending
	env.Status.Conditions = nil
	if !strings.Contains(list(now), "\tCURRENT\n") {
		t.Fatal("fresh pending classification was not current")
	}
	env.Status.Conditions = []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 2}}
	if !strings.Contains(list(now), "\tSTALE\n") {
		t.Fatal("pending classification remained current after readiness")
	}
}

func TestEnvironmentServiceUpdateRetriesConflictAndPreservesConcurrentDeclarations(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{}
	environment.Name = "shared"
	environment.Namespace = "ns"
	environment.UID = "env-uid"
	environment.Spec.Services = []platformv1alpha1.EnvironmentServiceDeclaration{desiredEnvironmentService("web", 3000, 1)}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
	key := client.ObjectKeyFromObject(environment)
	patches := 0
	kube := interceptor.NewClient(base, interceptor.Funcs{Patch: func(ctx context.Context, underlying client.WithWatch, object client.Object, patch client.Patch, options ...client.PatchOption) error {
		patches++
		if patches == 1 {
			var concurrent platformv1alpha1.Environment
			if err := underlying.Get(ctx, key, &concurrent); err != nil {
				return err
			}
			concurrent.Spec.Services = append(concurrent.Spec.Services, desiredEnvironmentService("api", 4000, 1))
			if err := underlying.Update(ctx, &concurrent); err != nil {
				return err
			}
			return apierrors.NewConflict(schema.GroupResource{Group: platformv1alpha1.GroupVersion.Group, Resource: "environments"}, object.GetName(), errors.New("simulated service conflict"))
		}
		return underlying.Patch(ctx, object, patch, options...)
	}})

	service, err := writeEnvironmentService(context.Background(), kube, key, "web", 3001, true)
	if err != nil || service.Revision != 2 || patches != 2 {
		t.Fatalf("conflicting update = %#v, patches %d, error %v", service, patches, err)
	}
	var current platformv1alpha1.Environment
	if err := base.Get(context.Background(), key, &current); err != nil {
		t.Fatal(err)
	}
	if len(current.Spec.Services) != 2 || environmentServiceIndex(current.Spec.Services, "api") < 0 || current.Spec.Services[environmentServiceIndex(current.Spec.Services, "web")].TargetPort != 3001 {
		t.Fatalf("concurrent declaration was lost: %#v", current.Spec.Services)
	}
}

func TestEnvironmentServiceDeclareEnforcesBound(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{}
	environment.Name = "shared"
	environment.Namespace = "ns"
	environment.UID = "env-uid"
	for i := 0; i < platformv1alpha1.EnvironmentServiceMaxDeclarations; i++ {
		environment.Spec.Services = append(environment.Spec.Services, desiredEnvironmentService(fmt.Sprintf("service-%02d", i), int32(3000+i), 1))
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()

	_, err := writeEnvironmentService(context.Background(), kube, client.ObjectKeyFromObject(environment), "one-too-many", 4000, false)
	if err == nil || !strings.Contains(err.Error(), "maximum of 32") {
		t.Fatalf("bounded declare error = %v", err)
	}
	var current platformv1alpha1.Environment
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(environment), &current); err != nil {
		t.Fatal(err)
	}
	if len(current.Spec.Services) != platformv1alpha1.EnvironmentServiceMaxDeclarations {
		t.Fatalf("service count = %d, want %d", len(current.Spec.Services), platformv1alpha1.EnvironmentServiceMaxDeclarations)
	}
}

func TestEnvironmentServiceMutationRejectsSameNameReplacement(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{}
	environment.Name = "shared"
	environment.Namespace = "ns"
	environment.UID = "env-uid-old"
	environment.Spec.Services = []platformv1alpha1.EnvironmentServiceDeclaration{desiredEnvironmentService("web", 3000, 1)}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment).Build()
	key := client.ObjectKeyFromObject(environment)
	patches := 0
	kube := interceptor.NewClient(base, interceptor.Funcs{Patch: func(ctx context.Context, underlying client.WithWatch, object client.Object, patch client.Patch, options ...client.PatchOption) error {
		patches++
		if patches == 1 {
			var original platformv1alpha1.Environment
			if err := underlying.Get(ctx, key, &original); err != nil {
				return err
			}
			if err := underlying.Delete(ctx, &original); err != nil {
				return err
			}
			replacement := &platformv1alpha1.Environment{}
			replacement.Name, replacement.Namespace, replacement.UID = environment.Name, environment.Namespace, "env-uid-new"
			if err := underlying.Create(ctx, replacement); err != nil {
				return err
			}
			return apierrors.NewConflict(schema.GroupResource{Group: platformv1alpha1.GroupVersion.Group, Resource: "environments"}, object.GetName(), errors.New("simulated replacement conflict"))
		}
		return underlying.Patch(ctx, object, patch, options...)
	}})

	_, err := writeEnvironmentService(context.Background(), kube, key, "web", 3001, true)
	if !errors.Is(err, lifecycle.ErrEnvironmentIncarnationChanged) || patches != 1 {
		t.Fatalf("replacement update = patches %d, error %v", patches, err)
	}
	var current platformv1alpha1.Environment
	if err := base.Get(context.Background(), key, &current); err != nil {
		t.Fatal(err)
	}
	if current.UID != "env-uid-new" || len(current.Spec.Services) != 0 {
		t.Fatalf("replacement was mutated: uid=%q services=%#v", current.UID, current.Spec.Services)
	}
}

func TestValidateEnvironmentServiceInput(t *testing.T) {
	for _, test := range []struct {
		name string
		port uint32
		ok   bool
	}{
		{name: "web", port: 1, ok: true},
		{name: "web-api", port: 65535, ok: true},
		{name: "Web", port: 8080},
		{name: "web.example", port: 8080},
		{name: "web", port: 0},
		{name: "web", port: platformv1alpha1.EnvironmentServiceControlPort},
		{name: "web", port: 65536},
	} {
		err := validateEnvironmentServiceInput(test.name, test.port)
		if (err == nil) != test.ok {
			t.Errorf("validateEnvironmentServiceInput(%q, %d) = %v, want ok=%t", test.name, test.port, err, test.ok)
		}
	}
}
