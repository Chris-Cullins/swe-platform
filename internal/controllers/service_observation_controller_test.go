package controllers

import (
	"context"
	"errors"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	"github.com/Chris-Cullins/swe-platform/internal/sandboxclient"
)

type fakeServiceObserver struct {
	calls        int
	currentCalls int
	result       sandboxclient.ServiceObservationResult
	err          error
	current      *bool
	currentErr   error
	after        func()
}

func (o *fakeServiceObserver) ObserveServices(context.Context, lifecycle.ExecutionFence, sandboxclient.ServiceDeclarationSnapshot) (sandboxclient.ServiceObservationResult, error) {
	o.calls++
	if o.after != nil {
		o.after()
	}
	return o.result, o.err
}

func (o *fakeServiceObserver) ServiceObservationCurrent(context.Context, lifecycle.ExecutionFence, sandboxclient.ServiceDeclarationSnapshot, sandboxclient.ServiceObservationResult) (bool, error) {
	o.currentCalls++
	if o.currentErr != nil {
		return false, o.currentErr
	}
	if o.current != nil {
		return *o.current, nil
	}
	// Current defaults true for concise controller-only tests; connector tests
	// exercise the opaque production proof independently.
	return true, nil
}

func TestServiceObservationFinalBackendProofFailureDiscardsResult(t *testing.T) {
	env := readyObservationEnvironment()
	current := false
	o := &fakeServiceObserver{current: &current, result: sandboxclient.ServiceObservationResult{Probes: []sandboxclient.ServiceProbeResult{{Name: "z", Outcome: sandboxclient.ServiceProbeConnected}, {Name: "a", Outcome: sandboxclient.ServiceProbeConnected}}}}
	c := fake.NewClientBuilder().WithScheme(observationTestScheme(t)).WithStatusSubresource(env).WithObjects(env).Build()
	result, err := (&ServiceObservationReconciler{Client: c, Observer: o}).Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)})
	if err != nil || !result.Requeue || o.currentCalls != 1 {
		t.Fatalf("result = %#v, error = %v, current calls = %d", result, err, o.currentCalls)
	}
	var got platformv1alpha1.Environment
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(env), &got)
	if got.Status.ServiceObservations != nil {
		t.Fatalf("failed final proof published %#v", got.Status.ServiceObservations)
	}
}

func TestServiceObservationStatusConflictDiscardsResult(t *testing.T) {
	env := readyObservationEnvironment()
	base := fake.NewClientBuilder().WithScheme(observationTestScheme(t)).WithStatusSubresource(env).WithObjects(env).Build()
	conflicting := &observationConflictClient{Client: base}
	o := &fakeServiceObserver{result: sandboxclient.ServiceObservationResult{Probes: []sandboxclient.ServiceProbeResult{{Name: "z", Outcome: sandboxclient.ServiceProbeConnected}, {Name: "a", Outcome: sandboxclient.ServiceProbeConnected}}}}
	result, err := (&ServiceObservationReconciler{Client: conflicting, APIReader: base, Observer: o}).Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)})
	if err != nil || !result.Requeue || conflicting.patches != 1 {
		t.Fatalf("result = %#v, error = %v, patches = %d", result, err, conflicting.patches)
	}
	var got platformv1alpha1.Environment
	_ = base.Get(context.Background(), client.ObjectKeyFromObject(env), &got)
	if got.Status.ServiceObservations != nil {
		t.Fatalf("conflicting result published %#v", got.Status.ServiceObservations)
	}
}

func TestServiceObservationReconcileClassifiesAndSorts(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = platformv1alpha1.AddToScheme(scheme)
	env := readyObservationEnvironment()
	o := &fakeServiceObserver{result: sandboxclient.ServiceObservationResult{Probes: []sandboxclient.ServiceProbeResult{{Name: "z", Outcome: sandboxclient.ServiceProbeNotConnected}, {Name: "a", Outcome: sandboxclient.ServiceProbeConnected}}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build()
	r := &ServiceObservationReconciler{Client: c, Observer: o, Now: func() time.Time { return time.Unix(100, 0) }}
	key := types.NamespacedName{Namespace: env.Namespace, Name: env.Name}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	var got platformv1alpha1.Environment
	_ = c.Get(context.Background(), key, &got)
	if o.calls != 1 || got.Status.ServiceObservations == nil || got.Status.ServiceObservations.ExecutionGeneration == nil || len(got.Status.ServiceObservations.Records) != 2 || got.Status.ServiceObservations.Records[0].Name != "a" || got.Status.ServiceObservations.Records[0].State != platformv1alpha1.EnvironmentServiceObservationHealthy || got.Status.ServiceObservations.Records[1].State != platformv1alpha1.EnvironmentServiceObservationUnhealthy {
		t.Fatalf("result = %#v calls=%d", got.Status.ServiceObservations, o.calls)
	}
}

func TestServiceObservationReconcileDoesNotProbeUnavailableStates(t *testing.T) {
	for name, mutate := range map[string]func(*platformv1alpha1.Environment){
		"paused": func(e *platformv1alpha1.Environment) { e.Spec.Paused = true },
		"enabled hold": func(e *platformv1alpha1.Environment) {
			e.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Enabled: true, Revision: 1}
		},
		"suspended": func(e *platformv1alpha1.Environment) { e.Status.Lifecycle.Suspended = true },
		"unready":   func(e *platformv1alpha1.Environment) { e.Status.Phase = ""; e.Status.Conditions = nil },
	} {
		t.Run(name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = platformv1alpha1.AddToScheme(scheme)
			env := readyObservationEnvironment()
			mutate(env)
			o := &fakeServiceObserver{}
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build()
			r := &ServiceObservationReconciler{Client: c, Observer: o}
			key := types.NamespacedName{Namespace: env.Namespace, Name: env.Name}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			if o.calls != 0 {
				t.Fatal("observer invoked")
			}
			var got platformv1alpha1.Environment
			_ = c.Get(context.Background(), key, &got)
			if got.Status.ServiceObservations.ExecutionGeneration != nil {
				t.Fatal("unavailable result has execution")
			}
		})
	}
}

func TestServiceObservationDeletingNeverProbesOrWrites(t *testing.T) {
	scheme := observationTestScheme(t)
	env := readyObservationEnvironment()
	now := metav1.Now()
	env.DeletionTimestamp = &now
	env.Finalizers = []string{"retain"}
	o := &fakeServiceObserver{}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build()
	r := &ServiceObservationReconciler{Client: c, Observer: o}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)}); err != nil {
		t.Fatal(err)
	}
	var got platformv1alpha1.Environment
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
		t.Fatal(err)
	}
	if o.calls != 0 || got.Status.ServiceObservations != nil {
		t.Fatalf("deleting observation = %#v, calls = %d", got.Status.ServiceObservations, o.calls)
	}
}

func TestServiceObservationMapsTimeoutAndProvenFailure(t *testing.T) {
	for name, observer := range map[string]*fakeServiceObserver{
		"probe timeout":  {result: sandboxclient.ServiceObservationResult{Probes: []sandboxclient.ServiceProbeResult{{Name: "z", Outcome: sandboxclient.ServiceProbeTimedOut}, {Name: "a", Outcome: sandboxclient.ServiceProbeTimedOut}}}},
		"proven failure": {result: sandboxclient.ServiceObservationResult{Failed: true}},
	} {
		t.Run(name, func(t *testing.T) {
			env := readyObservationEnvironment()
			c := fake.NewClientBuilder().WithScheme(observationTestScheme(t)).WithStatusSubresource(env).WithObjects(env).Build()
			r := &ServiceObservationReconciler{Client: c, Observer: observer}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)}); err != nil {
				t.Fatal(err)
			}
			var got platformv1alpha1.Environment
			_ = c.Get(context.Background(), client.ObjectKeyFromObject(env), &got)
			want := platformv1alpha1.EnvironmentServiceReasonProbeTimedOut
			if name == "proven failure" {
				want = platformv1alpha1.EnvironmentServiceReasonObservationFailed
			}
			if got.Status.ServiceObservations == nil || len(got.Status.ServiceObservations.Records) != 2 {
				t.Fatalf("observations = %#v", got.Status.ServiceObservations)
			}
			for _, record := range got.Status.ServiceObservations.Records {
				if record.State != platformv1alpha1.EnvironmentServiceObservationUnknown || record.Reason != want {
					t.Fatalf("record = %#v, want Unknown/%s", record, want)
				}
			}
		})
	}
}

func TestServiceObservationRemovalClearsEnvelopeAndStops(t *testing.T) {
	env := readyObservationEnvironment()
	env.Spec.Services = nil
	env.Status.ServiceObservations = &platformv1alpha1.EnvironmentServiceObservations{ObservedGeneration: env.Generation, ObservedAt: metav1.Now(), Records: []platformv1alpha1.EnvironmentServiceObservation{{Name: "removed", DeclarationRevision: 1}}}
	o := &fakeServiceObserver{}
	c := fake.NewClientBuilder().WithScheme(observationTestScheme(t)).WithStatusSubresource(env).WithObjects(env).Build()
	result, err := (&ServiceObservationReconciler{Client: c, Observer: o}).Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)})
	if err != nil {
		t.Fatal(err)
	}
	var got platformv1alpha1.Environment
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(env), &got)
	if result.Requeue || result.RequeueAfter != 0 || o.calls != 0 || got.Status.ServiceObservations != nil {
		t.Fatalf("result = %#v, calls = %d, observations = %#v", result, o.calls, got.Status.ServiceObservations)
	}
}

func TestServiceObservationPostRPCRacesAreDiscarded(t *testing.T) {
	for name, mutate := range map[string]func(context.Context, client.Client, *platformv1alpha1.Environment) error{
		"metadata and declaration snapshot": func(ctx context.Context, c client.Client, env *platformv1alpha1.Environment) error {
			var current platformv1alpha1.Environment
			if err := c.Get(ctx, client.ObjectKeyFromObject(env), &current); err != nil {
				return err
			}
			current.Spec.Services[0].Revision++
			current.Generation++
			return c.Update(ctx, &current)
		},
		"execution generation": statusObservationMutation(func(env *platformv1alpha1.Environment) { env.Status.ExecutionGeneration++ }),
		"lifecycle epoch":      statusObservationMutation(func(env *platformv1alpha1.Environment) { env.Status.Lifecycle.Epoch++ }),
		"readiness":            statusObservationMutation(func(env *platformv1alpha1.Environment) { env.Status.Conditions = nil }),
		"hold revision": func(ctx context.Context, c client.Client, env *platformv1alpha1.Environment) error {
			var current platformv1alpha1.Environment
			if err := c.Get(ctx, client.ObjectKeyFromObject(env), &current); err != nil {
				return err
			}
			current.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Revision: 1}
			current.Generation++
			return c.Update(ctx, &current)
		},
	} {
		t.Run(name, func(t *testing.T) {
			env := readyObservationEnvironment()
			c := fake.NewClientBuilder().WithScheme(observationTestScheme(t)).WithStatusSubresource(env).WithObjects(env).Build()
			o := &fakeServiceObserver{result: sandboxclient.ServiceObservationResult{Probes: []sandboxclient.ServiceProbeResult{{Name: "z", Outcome: sandboxclient.ServiceProbeConnected}, {Name: "a", Outcome: sandboxclient.ServiceProbeConnected}}}}
			var mutationErr error
			o.after = func() { mutationErr = mutate(context.Background(), c, env) }
			result, err := (&ServiceObservationReconciler{Client: c, APIReader: c, Observer: o}).Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)})
			if err != nil || mutationErr != nil || !result.Requeue {
				t.Fatalf("result = %#v, reconcile error = %v, mutation error = %v", result, err, mutationErr)
			}
			var got platformv1alpha1.Environment
			_ = c.Get(context.Background(), client.ObjectKeyFromObject(env), &got)
			if got.Status.ServiceObservations != nil {
				t.Fatalf("stale result published: %#v", got.Status.ServiceObservations)
			}
		})
	}
}

func TestServiceObservationSameNameReplacementCannotInherit(t *testing.T) {
	env := readyObservationEnvironment()
	c := fake.NewClientBuilder().WithScheme(observationTestScheme(t)).WithStatusSubresource(env).WithObjects(env).Build()
	o := &fakeServiceObserver{result: sandboxclient.ServiceObservationResult{Probes: []sandboxclient.ServiceProbeResult{{Name: "z", Outcome: sandboxclient.ServiceProbeConnected}, {Name: "a", Outcome: sandboxclient.ServiceProbeConnected}}}}
	var mutationErr error
	o.after = func() {
		current := env.DeepCopy()
		mutationErr = c.Delete(context.Background(), current)
		if mutationErr == nil {
			replacement := readyObservationEnvironment()
			replacement.UID = "replacement"
			replacement.ResourceVersion = ""
			mutationErr = c.Create(context.Background(), replacement)
		}
	}
	result, err := (&ServiceObservationReconciler{Client: c, APIReader: c, Observer: o}).Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)})
	if err != nil || mutationErr != nil || !result.Requeue {
		t.Fatalf("result = %#v, reconcile error = %v, mutation error = %v", result, err, mutationErr)
	}
	var got platformv1alpha1.Environment
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(env), &got)
	if got.UID != "replacement" || got.Status.ServiceObservations != nil {
		t.Fatalf("replacement = UID %q, observations %#v", got.UID, got.Status.ServiceObservations)
	}
}

func TestObservationJitterBoundAndTransportDoesNotPublish(t *testing.T) {
	for _, uid := range []string{"", "a", "different"} {
		d := observationJitter(types.UID(uid))
		if d < 4*time.Second || d > 6*time.Second {
			t.Fatalf("jitter %v", d)
		}
	}
}

func readyObservationEnvironment() *platformv1alpha1.Environment {
	e := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "n", UID: "u", Generation: 2}, Spec: platformv1alpha1.EnvironmentSpec{Services: []platformv1alpha1.EnvironmentServiceDeclaration{{Name: "z", Revision: 2, TargetPort: 3000}, {Name: "a", Revision: 1, TargetPort: 3000}}}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady, ObservedGeneration: 2, ExecutionGeneration: 3, Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 2}}}}
	return e
}

func observationTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func statusObservationMutation(mutate func(*platformv1alpha1.Environment)) func(context.Context, client.Client, *platformv1alpha1.Environment) error {
	return func(ctx context.Context, c client.Client, env *platformv1alpha1.Environment) error {
		var current platformv1alpha1.Environment
		if err := c.Get(ctx, client.ObjectKeyFromObject(env), &current); err != nil {
			return err
		}
		mutate(&current)
		return c.Status().Update(ctx, &current)
	}
}

type observationConflictClient struct {
	client.Client
	patches int
}

func (c *observationConflictClient) Status() client.SubResourceWriter {
	return &observationConflictWriter{SubResourceWriter: c.Client.Status(), client: c}
}

type observationConflictWriter struct {
	client.SubResourceWriter
	client *observationConflictClient
}

func (w *observationConflictWriter) Patch(_ context.Context, object client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
	w.client.patches++
	return apierrors.NewConflict(schema.GroupResource{Group: platformv1alpha1.GroupVersion.Group, Resource: "environments"}, object.GetName(), errors.New("concurrent status update"))
}
