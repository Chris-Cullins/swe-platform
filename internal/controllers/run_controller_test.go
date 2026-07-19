package controllers

import (
	"context"
	"errors"
	"sync"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

type scriptedAdapter struct {
	observations        []AdapterObservation
	accepted, cancelled int
	onCancel            func()
}

type failAcceptedStatusClient struct {
	client.Client
	fail bool
}

func (c *failAcceptedStatusClient) Status() client.SubResourceWriter {
	return &failAcceptedStatusWriter{SubResourceWriter: c.Client.Status(), client: c}
}

type failAcceptedStatusWriter struct {
	client.SubResourceWriter
	client *failAcceptedStatusClient
}

func (w *failAcceptedStatusWriter) Update(ctx context.Context, object client.Object, opts ...client.SubResourceUpdateOption) error {
	if run, ok := object.(*platformv1alpha1.Run); ok && run.Status.State == platformv1alpha1.RunStateAdapterAccepted && w.client.fail {
		w.client.fail = false
		return errors.New("simulated lost acceptance status update")
	}
	return w.SubResourceWriter.Update(ctx, object, opts...)
}

// foregroundAdapter models a CLI agent whose managed process exit is the task
// outcome.
type foregroundAdapter struct{ process AdapterObservation }

func (*foregroundAdapter) EnsureAccepted(context.Context, AdapterTask, AdapterSandbox) error {
	return nil
}
func (a *foregroundAdapter) Observe(context.Context, AdapterTask, AdapterSandbox) (AdapterObservation, string, error) {
	return a.process, "managed process state", nil
}
func (*foregroundAdapter) Cancel(context.Context, AdapterTask, AdapterSandbox) error { return nil }

// serviceAdapter models a long-lived agent service: task events change while
// the service process remains running, so service exit is not task completion.
type serviceAdapter struct {
	serviceRunning bool
	event          AdapterObservation
}

func (a *serviceAdapter) EnsureAccepted(context.Context, AdapterTask, AdapterSandbox) error {
	a.serviceRunning = true
	return nil
}
func (a *serviceAdapter) Observe(context.Context, AdapterTask, AdapterSandbox) (AdapterObservation, string, error) {
	return a.event, "service task event", nil
}
func (a *serviceAdapter) Cancel(context.Context, AdapterTask, AdapterSandbox) error {
	a.serviceRunning = false
	return nil
}

func (a *scriptedAdapter) EnsureAccepted(context.Context, AdapterTask, AdapterSandbox) error {
	a.accepted++
	return nil
}
func (a *scriptedAdapter) Cancel(context.Context, AdapterTask, AdapterSandbox) error {
	a.cancelled++
	if a.onCancel != nil {
		a.onCancel()
	}
	return nil
}
func (a *scriptedAdapter) Observe(context.Context, AdapterTask, AdapterSandbox) (AdapterObservation, string, error) {
	o := a.observations[0]
	if len(a.observations) > 1 {
		a.observations = a.observations[1:]
	}
	return o, string(o), nil
}

func runScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func reconciler(t *testing.T, adapter AdapterLifecycle, objects ...client.Object) *RunReconciler {
	t.Helper()
	s := runScheme(t)
	for _, object := range objects {
		env, ok := object.(*platformv1alpha1.Environment)
		if ok && (env.Status.Phase == platformv1alpha1.EnvironmentPhaseReady || env.Status.Phase == platformv1alpha1.EnvironmentPhaseRunning) {
			applyEnvironmentStatus(env, env.Status.Phase, env.Status.PodName, env.Status.Endpoints.Sandboxd, "SandboxdReady", "sandboxd is ready", env.Status.LastActiveAt)
		}
	}
	// Most lifecycle tests predate the ownership/endpoint security fences. Give
	// their intentionally valid fixtures the exact current Run owner and a
	// reachable endpoint; mismatch tests construct their reconciler directly.
	for _, object := range objects {
		run, ok := object.(*platformv1alpha1.Run)
		if !ok || run.Status.EnvironmentRef == nil || run.Status.EnvironmentRef.Ownership != platformv1alpha1.EnvironmentOwnershipOwned {
			continue
		}
		for _, candidate := range objects {
			env, ok := candidate.(*platformv1alpha1.Environment)
			if !ok || env.Name != run.Status.EnvironmentRef.Name || env.UID != run.Status.EnvironmentRef.UID {
				continue
			}
			env.OwnerReferences = []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true)}}
			if env.Status.Phase == platformv1alpha1.EnvironmentPhaseReady || env.Status.Phase == platformv1alpha1.EnvironmentPhaseRunning {
				env.Status.PodName = "env-" + env.Name
				env.Status.Endpoints.Sandboxd = "10.0.0.1:50051"
			}
		}
	}
	c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&platformv1alpha1.Run{}, &platformv1alpha1.Environment{}).WithObjects(objects...).Build()
	return &RunReconciler{Client: c, Scheme: s, Adapters: map[string]AdapterLifecycle{"test": adapter}}
}

func reconcileRun(t *testing.T, r *RunReconciler, name string) platformv1alpha1.Run {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: name}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: name}, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestAllocationIsDeterministicAndRecoversBeforeStatus(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test"}}
	// This models a successful create followed by a lost Run status update.
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "run-run-uid", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: "r", UID: run.UID, Controller: ptr(true)}}}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: "small"}}
	r := reconciler(t, &scriptedAdapter{}, run, env)
	for range 3 {
		reconcileRun(t, r, run.Name)
	}
	var environments platformv1alpha1.EnvironmentList
	if err := r.List(context.Background(), &environments, client.InNamespace("ns")); err != nil {
		t.Fatal(err)
	}
	if len(environments.Items) != 1 {
		t.Fatalf("environments = %d, want 1", len(environments.Items))
	}
	got := reconcileRun(t, r, run.Name)
	if got.Status.EnvironmentRef == nil || got.Status.EnvironmentRef.Name != "run-run-uid" || got.Status.EnvironmentRef.UID != "env-uid" {
		t.Fatalf("reference = %#v", got.Status.EnvironmentRef)
	}
	if !metav1.IsControlledBy(&environments.Items[0], run) {
		t.Fatal("environment lacks Run controller owner")
	}
}

func TestRepeatedAllocationCreatesOneOwnedEnvironment(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test"}}
	r := reconciler(t, &scriptedAdapter{}, run)
	for range 2 {
		ref, err := r.allocateEnvironment(context.Background(), run)
		if err != nil {
			t.Fatal(err)
		}
		if ref.Name != "run-run-uid" {
			t.Fatalf("name = %q", ref.Name)
		}
	}
	var environments platformv1alpha1.EnvironmentList
	if err := r.List(context.Background(), &environments); err != nil {
		t.Fatal(err)
	}
	if len(environments.Items) != 1 || !metav1.IsControlledBy(&environments.Items[0], run) {
		t.Fatalf("environments = %#v", environments.Items)
	}
}

func TestRunClaimsAndRecoversWarmEnvironment(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", ProjectRef: "project", Agent: "test"}}
	warm := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "warm-small-1",
			Namespace: "ns",
			UID:       "warm-uid",
			Labels:    map[string]string{warmPoolLabel: "small"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "EnvironmentTemplate", Name: "small", UID: "template-uid", Controller: ptr(true),
			}},
		},
		Spec:   platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
		Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady},
	}
	r := reconciler(t, &scriptedAdapter{}, run, warm)
	ref, err := r.allocateEnvironment(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Name != warm.Name || ref.UID != warm.UID || ref.Ownership != platformv1alpha1.EnvironmentOwnershipClaimed {
		t.Fatalf("reference = %#v", ref)
	}
	var claimed platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(warm), &claimed); err != nil {
		t.Fatal(err)
	}
	if claimed.Status.ClaimedBy == nil || claimed.Status.ClaimedBy.UID != run.UID || claimed.Status.LastActiveAt == nil || claimed.Status.Phase != platformv1alpha1.EnvironmentPhaseSetup || claimed.Status.PodName != "" || claimed.Status.Endpoints.Sandboxd != "" {
		t.Fatalf("claim status = %#v", claimed.Status)
	}
	if claimed.Spec.ProjectRef != run.Spec.ProjectRef || claimed.Labels[warmPoolLabel] != "" || metav1.GetControllerOf(&claimed) != nil {
		t.Fatalf("promoted environment = %#v", claimed)
	}
	recovered, err := r.recoverEnvironmentReference(context.Background(), run)
	if err != nil || recovered == nil || recovered.UID != warm.UID || recovered.Ownership != platformv1alpha1.EnvironmentOwnershipClaimed {
		t.Fatalf("recovered = %#v, error = %v", recovered, err)
	}
}

func TestWarmPromotionWithdrawsReadinessBeforeAdapterAcceptance(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", ProjectRef: "project", Agent: "test"}}
	warm := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "warm-small-1", Namespace: "ns", UID: "warm-uid", Labels: map[string]string{warmPoolLabel: "small"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "EnvironmentTemplate", Name: "small", UID: "template-uid", Controller: ptr(true)}},
		},
		Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
		Status: platformv1alpha1.EnvironmentStatus{
			Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-warm-small-1", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		},
	}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, warm)
	got := reconcileRun(t, r, run.Name)
	if got.Status.State != platformv1alpha1.RunStateAllocating || got.Status.EnvironmentRef == nil {
		t.Fatalf("Run status = %#v, want allocated but not ready", got.Status)
	}
	got = reconcileRun(t, r, run.Name)
	if got.Status.State != platformv1alpha1.RunStateAllocating || adapter.accepted != 0 {
		t.Fatalf("state = %s, adapter accepts = %d", got.Status.State, adapter.accepted)
	}
}

func TestClaimsAreExclusiveUIDFencedAndReleasedSafely(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "new"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: "shared", Agent: "test"}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env"}, Status: platformv1alpha1.EnvironmentStatus{ClaimedBy: &platformv1alpha1.RunReference{Name: "r", UID: "old"}}}
	r := reconciler(t, &scriptedAdapter{}, run, env)
	if _, err := r.allocateEnvironment(context.Background(), run); err == nil {
		t.Fatal("stale same-name claim was accepted")
	}
	var stored platformv1alpha1.Environment
	_ = r.Get(context.Background(), client.ObjectKeyFromObject(env), &stored)
	if err := r.releaseClaim(context.Background(), run, &stored); err != nil {
		t.Fatal(err)
	}
	_ = r.Get(context.Background(), client.ObjectKeyFromObject(env), &stored)
	if stored.Status.ClaimedBy == nil || stored.Status.ClaimedBy.UID != "old" {
		t.Fatal("non-matching claim was released")
	}
	stored.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: "r", UID: "new"}
	if err := r.Status().Update(context.Background(), &stored); err != nil {
		t.Fatal(err)
	}
	if err := r.releaseClaim(context.Background(), run, &stored); err != nil {
		t.Fatal(err)
	}
	_ = r.Get(context.Background(), client.ObjectKeyFromObject(env), &stored)
	if stored.Status.ClaimedBy != nil {
		t.Fatal("matching claim was not released")
	}
}

func TestConcurrentClaimHasOneWinner(t *testing.T) {
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env"}}
	runA := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns", UID: "a-uid"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: env.Name, Agent: "test"}}
	runB := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns", UID: "b-uid"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: env.Name, Agent: "test"}}
	r := reconciler(t, &scriptedAdapter{}, env, runA, runB)
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, run := range []*platformv1alpha1.Run{runA, runB} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := r.allocateEnvironment(context.Background(), run)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successful claims = %d, want 1", successes)
	}
	var stored platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.ClaimedBy == nil || (stored.Status.ClaimedBy.UID != runA.UID && stored.Status.ClaimedBy.UID != runB.UID) {
		t.Fatalf("claim = %#v", stored.Status.ClaimedBy)
	}
}

func TestExplicitClaimContentionFailsPermanently(t *testing.T) {
	loser := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "loser", Namespace: "ns", UID: "loser-uid"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: "shared", Agent: "test"}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
		ClaimedBy: &platformv1alpha1.RunReference{Name: "winner", UID: "winner-uid"},
	}}
	r := reconciler(t, &scriptedAdapter{}, loser, env)
	reconcileRun(t, r, loser.Name) // finalizer
	failed := reconcileRun(t, r, loser.Name)
	if failed.Status.State != platformv1alpha1.RunStateFailed || failed.Status.EnvironmentRef != nil {
		t.Fatalf("contending Run status = %#v", failed.Status)
	}
	var released platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &released); err != nil {
		t.Fatal(err)
	}
	released.Status.ClaimedBy = nil
	if err := r.Status().Update(context.Background(), &released); err != nil {
		t.Fatal(err)
	}
	reconcileRun(t, r, loser.Name)
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &released); err != nil {
		t.Fatal(err)
	}
	if released.Status.ClaimedBy != nil {
		t.Fatalf("failed loser later claimed released Environment: %#v", released.Status.ClaimedBy)
	}
}

func TestCancelBeforeAllocationCreatesNothing(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", Cancel: true}}
	r := reconciler(t, &scriptedAdapter{}, run)
	got := reconcileRun(t, r, "r")
	var list platformv1alpha1.EnvironmentList
	_ = r.List(context.Background(), &list)
	if got.Status.State != platformv1alpha1.RunStateCancelled || len(list.Items) != 0 {
		t.Fatalf("state=%s environments=%d", got.Status.State, len(list.Items))
	}
}

func TestCancelRecoversAllocationCreatedBeforeStatus(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", Cancel: true}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "run-uid", Namespace: "ns", UID: "euid", OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true)}}}}
	r := reconciler(t, &scriptedAdapter{}, run, env)
	got := reconcileRun(t, r, run.Name)
	if got.Status.EnvironmentRef == nil || got.Status.EnvironmentRef.UID != env.UID || got.Status.State != platformv1alpha1.RunStateAllocating {
		t.Fatalf("status = %#v, want recovered allocation", got.Status)
	}
	got = reconcileRun(t, r, run.Name)
	if got.Status.State != platformv1alpha1.RunStateCancelled {
		t.Fatalf("state = %s, want Cancelled", got.Status.State)
	}
}

func TestTerminalCleanupPausesOwnedEnvironment(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid"}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateSucceeded, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "euid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "euid"}}
	a := &scriptedAdapter{}
	r := reconciler(t, a, run, env)
	reconcileRun(t, r, "r")
	var got platformv1alpha1.Environment
	_ = r.Get(context.Background(), client.ObjectKeyFromObject(env), &got)
	if !got.Spec.Paused || a.cancelled != 0 {
		t.Fatalf("paused=%v cancels=%d", got.Spec.Paused, a.cancelled)
	}
}

func TestAdapterShapesPauseResumeAndStatus(t *testing.T) {
	for _, tc := range []struct {
		name         string
		observations []AdapterObservation
		want         []platformv1alpha1.RunState
	}{
		{"foreground-process", []AdapterObservation{AdapterObservationRunning, AdapterObservationSucceeded}, []platformv1alpha1.RunState{platformv1alpha1.RunStateRunning, platformv1alpha1.RunStateSucceeded}},
		{"foreground-process-failure", []AdapterObservation{AdapterObservationFailed}, []platformv1alpha1.RunState{platformv1alpha1.RunStateFailed}},
		{"service-events", []AdapterObservation{AdapterObservationRunning, AdapterObservationNeedsInput}, []platformv1alpha1.RunState{platformv1alpha1.RunStateRunning, platformv1alpha1.RunStateNeedsInput}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateAllocating, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "euid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "euid"}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady}}
			a := &scriptedAdapter{observations: tc.observations}
			r := reconciler(t, a, run, env)
			if got := reconcileRun(t, r, "r"); got.Status.State != platformv1alpha1.RunStateEnvironmentReady {
				t.Fatal(got.Status.State)
			}
			if got := reconcileRun(t, r, "r"); got.Status.State != platformv1alpha1.RunStateEnvironmentReady || !acceptanceAttempted(&got) {
				t.Fatalf("acceptance marker state=%s attempted=%t", got.Status.State, acceptanceAttempted(&got))
			}
			if got := reconcileRun(t, r, "r"); got.Status.State != platformv1alpha1.RunStateAdapterAccepted {
				t.Fatal(got.Status.State)
			}
			for _, want := range tc.want {
				if got := reconcileRun(t, r, "r"); got.Status.State != want {
					t.Fatalf("state=%s want=%s", got.Status.State, want)
				}
			}
			if tc.name == "service-events" {
				_ = r.Get(context.Background(), client.ObjectKeyFromObject(env), env)
				env.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
				_ = r.Status().Update(context.Background(), env)
				if got := reconcileRun(t, r, "r"); got.Status.State != platformv1alpha1.RunStatePaused {
					t.Fatal(got.Status.State)
				}
				env.Status.Phase = platformv1alpha1.EnvironmentPhaseReady
				_ = r.Status().Update(context.Background(), env)
				if got := reconcileRun(t, r, "r"); got.Status.State != platformv1alpha1.RunStateEnvironmentReady {
					t.Fatal(got.Status.State)
				}
				if got := reconcileRun(t, r, "r"); got.Status.State != platformv1alpha1.RunStateAdapterAccepted || a.accepted != 2 {
					t.Fatalf("resume state=%s accepts=%d", got.Status.State, a.accepted)
				}
			}
		})
	}
}

func TestDifferentAdapterShapesDriveSameLifecycleContract(t *testing.T) {
	readyRun := func(name string) *platformv1alpha1.Run {
		return &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID(name), Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateEnvironmentReady, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e-" + name, UID: types.UID("e-" + name), Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
	}
	readyEnvironment := func(run *platformv1alpha1.Run) *platformv1alpha1.Environment {
		return &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: run.Status.EnvironmentRef.Name, Namespace: "ns", UID: run.Status.EnvironmentRef.UID}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady}}
	}

	foregroundRun := readyRun("foreground")
	foreground := &foregroundAdapter{process: AdapterObservationRunning}
	foregroundReconciler := reconciler(t, foreground, foregroundRun, readyEnvironment(foregroundRun))
	reconcileRun(t, foregroundReconciler, foregroundRun.Name) // acceptance attempt marker
	reconcileRun(t, foregroundReconciler, foregroundRun.Name) // acceptance
	if got := reconcileRun(t, foregroundReconciler, foregroundRun.Name); got.Status.State != platformv1alpha1.RunStateRunning {
		t.Fatalf("foreground state = %s", got.Status.State)
	}
	foreground.process = AdapterObservationSucceeded
	if got := reconcileRun(t, foregroundReconciler, foregroundRun.Name); got.Status.State != platformv1alpha1.RunStateSucceeded {
		t.Fatalf("foreground terminal state = %s", got.Status.State)
	}

	serviceRun := readyRun("service")
	service := &serviceAdapter{event: AdapterObservationRunning}
	serviceReconciler := reconciler(t, service, serviceRun, readyEnvironment(serviceRun))
	reconcileRun(t, serviceReconciler, serviceRun.Name) // acceptance attempt marker
	reconcileRun(t, serviceReconciler, serviceRun.Name) // task acknowledgement
	reconcileRun(t, serviceReconciler, serviceRun.Name) // running event
	service.event = AdapterObservationNeedsInput
	if got := reconcileRun(t, serviceReconciler, serviceRun.Name); got.Status.State != platformv1alpha1.RunStateNeedsInput || !service.serviceRunning {
		t.Fatalf("service state = %s, serviceRunning = %v", got.Status.State, service.serviceRunning)
	}
}

func TestNonterminalAdapterObservationSchedulesPolling(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateAdapterAccepted, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "euid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "euid"}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady}}
	r := reconciler(t, &foregroundAdapter{process: AdapterObservationRunning}, run, env)
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != adapterPollInterval {
		t.Fatalf("RequeueAfter = %s, want %s", result.RequeueAfter, adapterPollInterval)
	}
}

func TestEnvironmentReachableRequiresCurrentGenerationReady(t *testing.T) {
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Generation: 2}, Status: platformv1alpha1.EnvironmentStatus{
		ObservedGeneration: 1,
		Phase:              platformv1alpha1.EnvironmentPhaseReady,
		PodName:            "env-test",
		Endpoints:          platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		Conditions: []metav1.Condition{{
			Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue,
			ObservedGeneration: 1, Reason: "SandboxdReady", Message: "stale readiness",
		}},
	}}
	if environmentReachable(env) {
		t.Fatal("stale-generation Ready condition was accepted")
	}
	applyEnvironmentStatus(env, platformv1alpha1.EnvironmentPhaseReady, env.Status.PodName, env.Status.Endpoints.Sandboxd, "SandboxdReady", "current readiness", nil)
	if !environmentReachable(env) {
		t.Fatal("current-generation Ready condition was rejected")
	}
}

func TestRunDoesNotFailOnStaleEnvironmentFailure(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStateAllocating,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "env-uid", Generation: 2}, Status: platformv1alpha1.EnvironmentStatus{
		ObservedGeneration: 1, Phase: platformv1alpha1.EnvironmentPhaseFailed,
	}}
	r := reconciler(t, &scriptedAdapter{}, run, env)
	got := reconcileRun(t, r, run.Name)
	if got.Status.State != platformv1alpha1.RunStateAllocating {
		t.Fatalf("Run state = %s, stale Environment failure became terminal", got.Status.State)
	}
}

func TestEnvironmentReadyConditionIsIndependentFromTaskOutcome(t *testing.T) {
	for _, tc := range []struct {
		name         string
		runState     platformv1alpha1.RunState
		envPhase     platformv1alpha1.EnvironmentPhase
		adapter      AdapterLifecycle
		cancel       bool
		accepted     bool
		wantReady    metav1.ConditionStatus
		wantRunState platformv1alpha1.RunState
	}{
		{
			name: "adapter failure leaves ready Environment", runState: platformv1alpha1.RunStateAdapterAccepted,
			envPhase: platformv1alpha1.EnvironmentPhaseReady, adapter: &scriptedAdapter{observations: []AdapterObservation{AdapterObservationFailed}},
			accepted: true, wantReady: metav1.ConditionTrue, wantRunState: platformv1alpha1.RunStateFailed,
		},
		{
			name: "unavailable adapter leaves ready Environment", runState: platformv1alpha1.RunStateEnvironmentReady,
			envPhase: platformv1alpha1.EnvironmentPhaseReady, wantReady: metav1.ConditionTrue, wantRunState: platformv1alpha1.RunStateFailed,
		},
		{
			name: "Environment failure clears readiness", runState: platformv1alpha1.RunStateAdapterAccepted,
			envPhase: platformv1alpha1.EnvironmentPhaseFailed, adapter: &scriptedAdapter{}, accepted: true, wantReady: metav1.ConditionFalse, wantRunState: platformv1alpha1.RunStateFailed,
		},
		{
			name: "cancellation leaves reachable Environment ready", runState: platformv1alpha1.RunStateAdapterAccepted,
			envPhase: platformv1alpha1.EnvironmentPhaseReady, adapter: &scriptedAdapter{}, cancel: true, accepted: true,
			wantReady: metav1.ConditionTrue, wantRunState: platformv1alpha1.RunStateCancelled,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test", Cancel: tc.cancel}, Status: platformv1alpha1.RunStatus{
				State:          tc.runState,
				EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
				Conditions:     []metav1.Condition{{Type: runConditionEnvironmentReady, Status: metav1.ConditionTrue}},
			}}
			if tc.accepted {
				run.Status.Conditions = append(run.Status.Conditions, metav1.Condition{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue})
			}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
				Phase: tc.envPhase, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
			}}
			if tc.envPhase == platformv1alpha1.EnvironmentPhaseReady {
				env.Status.PodName = "env-shared"
				env.Status.Endpoints.Sandboxd = "10.0.0.1:50051"
			}
			r := reconciler(t, tc.adapter, run, env)
			if tc.adapter == nil {
				r.Adapters = map[string]AdapterLifecycle{}
			}
			got := reconcileRun(t, r, run.Name)
			condition := apiMeta.FindStatusCondition(got.Status.Conditions, runConditionEnvironmentReady)
			if got.Status.State != tc.wantRunState || condition == nil || condition.Status != tc.wantReady {
				t.Fatalf("Run status = %#v, EnvironmentReady = %#v", got.Status, condition)
			}
			if tc.wantReady == metav1.ConditionTrue {
				cleaned := reconcileRun(t, r, run.Name)
				condition = apiMeta.FindStatusCondition(cleaned.Status.Conditions, runConditionEnvironmentReady)
				if condition == nil || condition.Status != metav1.ConditionFalse {
					t.Fatalf("EnvironmentReady after terminal claim release = %#v", condition)
				}
				var released platformv1alpha1.Environment
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &released); err != nil || released.Status.ClaimedBy != nil {
					t.Fatalf("terminal claimed Environment = %#v, error = %v", released, err)
				}
			}
		})
	}
}

func TestExplicitPausedClaimWakesDuringAllocationAndRecovery(t *testing.T) {
	for _, tc := range []struct {
		name     string
		recovery bool
	}{
		{name: "allocation"},
		{name: "claim-before-status recovery", recovery: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: "shared", Agent: "test"}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{Paused: true}}
			if tc.recovery {
				env.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID}
				run.Finalizers = []string{runFinalizer}
			}
			r := reconciler(t, &scriptedAdapter{}, run, env)
			var ref *platformv1alpha1.RunEnvironmentReference
			var err error
			if tc.recovery {
				got := reconcileRun(t, r, run.Name)
				ref = got.Status.EnvironmentRef
			} else {
				ref, err = r.allocateEnvironment(context.Background(), run)
			}
			if err != nil || ref == nil || ref.Ownership != platformv1alpha1.EnvironmentOwnershipClaimed {
				t.Fatalf("reference = %#v, error = %v", ref, err)
			}
			var got platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
				t.Fatal(err)
			}
			if got.Spec.Paused || got.Status.ClaimedBy == nil || got.Status.ClaimedBy.UID != run.UID {
				t.Fatalf("claimed environment = %#v", got)
			}
		})
	}
}

func TestCancellationWhilePausedRequiresNoAdapterRPC(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test", Cancel: true}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStatePaused,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{Paused: true}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhasePaused, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
	}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	if got := reconcileRun(t, r, run.Name); got.Status.State != platformv1alpha1.RunStateCancelled {
		t.Fatalf("state = %s, want Cancelled", got.Status.State)
	}
	reconcileRun(t, r, run.Name)
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal("claimed Environment was deleted")
	}
	if retained.Status.ClaimedBy != nil || adapter.cancelled != 0 {
		t.Fatalf("claim = %#v, adapter cancellations = %d", retained.Status.ClaimedBy, adapter.cancelled)
	}
}

func TestLostAcceptanceStatusCancelsBeforeClaimRelease(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		name := "cancel"
		if deleting {
			name = "delete"
		}
		t.Run(name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
				State:          platformv1alpha1.RunStateEnvironmentReady,
				EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
				Conditions:     []metav1.Condition{{Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue}},
			}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
				Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-shared", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
				ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
			}}
			adapter := &scriptedAdapter{}
			r := reconciler(t, adapter, run, env)
			fault := &failAcceptedStatusClient{Client: r.Client, fail: true}
			r.Client = fault
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err == nil {
				t.Fatal("acceptance status update unexpectedly succeeded")
			}
			if adapter.accepted != 1 {
				t.Fatalf("acceptance calls = %d, want 1", adapter.accepted)
			}
			var storedRun platformv1alpha1.Run
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &storedRun); err != nil {
				t.Fatal(err)
			}
			if storedRun.Status.State != platformv1alpha1.RunStateEnvironmentReady || !acceptanceAttempted(&storedRun) || runAccepted(&storedRun) {
				t.Fatalf("stored status after lost update = %#v", storedRun.Status)
			}
			if deleting {
				if err := r.Delete(context.Background(), &storedRun); err != nil {
					t.Fatal(err)
				}
			} else {
				storedRun.Spec.Cancel = true
				if err := r.Update(context.Background(), &storedRun); err != nil {
					t.Fatal(err)
				}
			}
			adapter.onCancel = func() {
				var current platformv1alpha1.Environment
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil || current.Status.ClaimedBy == nil {
					t.Fatal("claim was released before uncertain acceptance was cancelled")
				}
			}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
				t.Fatal(err)
			}
			if !deleting {
				reconcileRun(t, r, run.Name)
			}
			var retained platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
				t.Fatal(err)
			}
			if adapter.cancelled == 0 || retained.Status.ClaimedBy != nil {
				t.Fatalf("cancellations = %d, claim = %#v", adapter.cancelled, retained.Status.ClaimedBy)
			}
		})
	}
}

func TestAcceptedUnreachableClaimIsFencedBeforeCancellationCleanup(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test", Cancel: true}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStateAdapterAccepted,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhaseFailed, PodName: "env-shared", ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
	}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("fence request = (%#v, %v)", result, err)
	}
	var fencing platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil {
		t.Fatal(err)
	}
	if !fencing.Spec.Paused || fencing.Status.ClaimedBy == nil || adapter.cancelled != 0 {
		t.Fatalf("fencing Environment = %#v, cancellations = %d", fencing, adapter.cancelled)
	}
	fencing.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
	fencing.Status.PodName = ""
	fencing.Status.Endpoints = platformv1alpha1.EnvironmentEndpoints{}
	if err := r.Status().Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if got := reconcileRun(t, r, run.Name); got.Status.State != platformv1alpha1.RunStateCancelled {
		t.Fatalf("state after fence = %s", got.Status.State)
	}
	reconcileRun(t, r, run.Name)
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil {
		t.Fatal(err)
	}
	if fencing.Status.ClaimedBy != nil || adapter.cancelled != 0 {
		t.Fatalf("post-fence claim = %#v, cancellations = %d", fencing.Status.ClaimedBy, adapter.cancelled)
	}
}

func TestAcceptedUnreachableOwnedFinalizerWaitsForFence(t *testing.T) {
	now := metav1.Now()
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "owned", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{
		APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true),
	}}}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseCreating}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	result, err := r.finalize(context.Background(), run)
	if err != nil || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("fence request = (%#v, %v)", result, err)
	}
	var fencing platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil {
		t.Fatal(err)
	}
	var retainedRun platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &retainedRun); err != nil || !controllerutil.ContainsFinalizer(&retainedRun, runFinalizer) || !fencing.Spec.Paused {
		t.Fatal("owned Run finalizer or fence request was not retained")
	}
	fencing.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
	if err := r.Status().Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if _, err := r.finalize(context.Background(), &retainedRun); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil || adapter.cancelled != 0 || !exactControllerOwner(&fencing, platformv1alpha1.GroupVersion.String(), "Run", run.Name, run.UID) {
		t.Fatalf("owned Environment after fenced finalization = %#v, cancellations = %d", fencing, adapter.cancelled)
	}
}

func TestAcceptedUnreachableOwnedTerminalCleanupWaitsForFence(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStateSucceeded,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "owned", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{
		APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true),
	}}}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseFailed, PodName: "env-owned"}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("terminal fence request = (%#v, %v)", result, err)
	}
	var fencing platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil || !fencing.Spec.Paused || adapter.cancelled != 0 {
		t.Fatalf("terminal fencing Environment = %#v, cancellations = %d, error = %v", fencing, adapter.cancelled, err)
	}
	fencing.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
	fencing.Status.PodName = ""
	if err := r.Status().Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil || !fencing.Spec.Paused || adapter.cancelled != 0 {
		t.Fatalf("terminal cleanup after fence = %#v, cancellations = %d, error = %v", fencing, adapter.cancelled, err)
	}
}

func TestFinalizeClaimedEnvironmentOrdersCancellationBeforeRelease(t *testing.T) {
	now := metav1.Now()
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-shared", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
	}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	adapter.onCancel = func() {
		var current platformv1alpha1.Environment
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil || current.Status.ClaimedBy == nil {
			t.Fatal("claim was released before adapter cancellation")
		}
	}
	if result, err := r.finalize(context.Background(), run); err != nil || result.RequeueAfter != 0 {
		t.Fatalf("finalize = (%#v, %v)", result, err)
	}
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal("claimed Environment was deleted")
	}
	if retained.Status.ClaimedBy != nil || adapter.cancelled != 1 {
		t.Fatalf("claim = %#v, cancellations = %d", retained.Status.ClaimedBy, adapter.cancelled)
	}
	var deletedRun platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &deletedRun); err == nil {
		if controllerutil.ContainsFinalizer(&deletedRun, runFinalizer) {
			t.Fatal("Run finalizer was not removed")
		}
	} else if !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
}

func TestFinalizeOwnedEnvironmentLeavesGCReference(t *testing.T) {
	now := metav1.Now()
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "owned", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{
		APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true),
	}}}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-owned", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"}}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	if _, err := r.finalize(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal("owned Environment was directly deleted instead of left to GC")
	}
	if !metav1.IsControlledBy(&retained, run) || adapter.cancelled != 1 {
		t.Fatalf("owner = %#v, cancellations = %d", metav1.GetControllerOf(&retained), adapter.cancelled)
	}
}

func TestFinalizePausedAndTransientlyUnreachableAcceptedClaims(t *testing.T) {
	for _, tc := range []struct {
		name        string
		phase       platformv1alpha1.EnvironmentPhase
		specPaused  bool
		wantRequeue bool
		wantClaim   bool
	}{
		{name: "paused is fenced", phase: platformv1alpha1.EnvironmentPhasePaused, specPaused: true},
		{name: "stale paused status is not fenced", phase: platformv1alpha1.EnvironmentPhasePaused, wantRequeue: true, wantClaim: true},
		{name: "setup is ambiguous", phase: platformv1alpha1.EnvironmentPhaseSetup, wantRequeue: true, wantClaim: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := metav1.Now()
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
				EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
				Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
			}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{Paused: tc.specPaused}, Status: platformv1alpha1.EnvironmentStatus{Phase: tc.phase, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID}}}
			adapter := &scriptedAdapter{}
			r := reconciler(t, adapter, run, env)
			result, err := r.finalize(context.Background(), run)
			if err != nil || (result.RequeueAfter != 0) != tc.wantRequeue {
				t.Fatalf("finalize = (%#v, %v)", result, err)
			}
			var retained platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
				t.Fatal(err)
			}
			if (retained.Status.ClaimedBy != nil) != tc.wantClaim || adapter.cancelled != 0 {
				t.Fatalf("claim = %#v, cancellations = %d", retained.Status.ClaimedBy, adapter.cancelled)
			}
			if tc.wantRequeue {
				var retainedRun platformv1alpha1.Run
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &retainedRun); err != nil || !controllerutil.ContainsFinalizer(&retainedRun, runFinalizer) {
					t.Fatal("ambiguous accepted work did not retain Run finalizer")
				}
			}
		})
	}
}

func TestMissingAdapterCleanupFallsBackToBackendFence(t *testing.T) {
	now := metav1.Now()
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "removed-adapter"}, Status: platformv1alpha1.RunStatus{
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-shared", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
	}}
	r := reconciler(t, &scriptedAdapter{}, run, env)
	r.Adapters = map[string]AdapterLifecycle{}
	result, err := r.finalize(context.Background(), run)
	if err != nil || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("missing-adapter fence = (%#v, %v)", result, err)
	}
	var fencing platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil {
		t.Fatal(err)
	}
	var retainedRun platformv1alpha1.Run
	if !fencing.Spec.Paused || fencing.Status.ClaimedBy == nil || r.Get(context.Background(), client.ObjectKeyFromObject(run), &retainedRun) != nil || !controllerutil.ContainsFinalizer(&retainedRun, runFinalizer) {
		t.Fatal("missing adapter released ownership before backend fencing")
	}
	fencing.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
	fencing.Status.PodName = ""
	fencing.Status.Endpoints = platformv1alpha1.EnvironmentEndpoints{}
	if err := r.Status().Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if _, err := r.finalize(context.Background(), &retainedRun); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil || fencing.Status.ClaimedBy != nil {
		t.Fatalf("fenced missing-adapter claim = %#v, error = %v", fencing.Status.ClaimedBy, err)
	}
}

func TestFinalizeRecoversLostClaimStatusAndOnlyReleasesMatchingUID(t *testing.T) {
	for _, tc := range []struct {
		name      string
		claimUID  types.UID
		wantClaim bool
	}{
		{name: "matching recovery", claimUID: "run-uid"},
		{name: "same-name replacement claim", claimUID: "other-run", wantClaim: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := metav1.Now()
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "test", EnvironmentRef: "shared"}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{Paused: true}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhasePaused, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: tc.claimUID}}}
			r := reconciler(t, &scriptedAdapter{}, run, env)
			if _, err := r.finalize(context.Background(), run); err != nil {
				t.Fatal(err)
			}
			var retained platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
				t.Fatal(err)
			}
			if (retained.Status.ClaimedBy != nil) != tc.wantClaim {
				t.Fatalf("claim = %#v", retained.Status.ClaimedBy)
			}
			if !retained.Spec.Paused {
				t.Fatal("deletion recovery unexpectedly woke the paused Environment")
			}
		})
	}
}

func TestCancelRecoveryDoesNotWakePausedClaim(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test", EnvironmentRef: "shared", Cancel: true}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{Paused: true}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhasePaused, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
	}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	reconcileRun(t, r, run.Name)
	if got := reconcileRun(t, r, run.Name); got.Status.State != platformv1alpha1.RunStateCancelled {
		t.Fatalf("state = %s, want Cancelled", got.Status.State)
	}
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal(err)
	}
	if !retained.Spec.Paused || adapter.cancelled != 0 {
		t.Fatalf("paused = %t, cancellations = %d", retained.Spec.Paused, adapter.cancelled)
	}
}

func TestFinalizeRecoversOwnedEnvironmentWithLostRunStatus(t *testing.T) {
	now := metav1.Now()
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "test", TemplateRef: "small"}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "run-run-uid", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{
		APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true),
	}}}, Spec: platformv1alpha1.EnvironmentSpec{Paused: true}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhasePaused}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	if _, err := r.finalize(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal("owned Environment should remain for API-server GC")
	}
	if !exactControllerOwner(&retained, platformv1alpha1.GroupVersion.String(), "Run", run.Name, run.UID) || adapter.cancelled != 0 {
		t.Fatalf("owner = %#v, cancellations = %d", metav1.GetControllerOf(&retained), adapter.cancelled)
	}
}

func TestCleanupRecoveryDoesNotPromoteWarmClaim(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		name := "cancel"
		if deleting {
			name = "delete"
		}
		t.Run(name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test", TemplateRef: "small", ProjectRef: "project", Cancel: !deleting}}
			if deleting {
				now := metav1.Now()
				run.DeletionTimestamp = &now
			}
			warm := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "warm-small-1", Namespace: "ns", UID: "warm-uid", Labels: map[string]string{warmPoolLabel: "small"}, OwnerReferences: []metav1.OwnerReference{{
				APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "EnvironmentTemplate", Name: "small", UID: "template-uid", Controller: ptr(true),
			}}}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: "small"}, Status: platformv1alpha1.EnvironmentStatus{
				Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-warm-small-1", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
				ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
			}}
			adapter := &scriptedAdapter{}
			r := reconciler(t, adapter, run, warm)
			if deleting {
				if _, err := r.finalize(context.Background(), run); err != nil {
					t.Fatal(err)
				}
			} else {
				reconcileRun(t, r, run.Name)
				reconcileRun(t, r, run.Name)
			}
			var retained platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(warm), &retained); err != nil {
				t.Fatal(err)
			}
			owner := metav1.GetControllerOf(&retained)
			if retained.Labels[warmPoolLabel] != "small" || owner == nil || owner.Kind != "EnvironmentTemplate" || owner.Name != "small" ||
				retained.Spec.ProjectRef != "" || retained.Status.ClaimedBy != nil || retained.Status.Phase != platformv1alpha1.EnvironmentPhaseReady || retained.Status.PodName == "" || retained.Status.Endpoints.Sandboxd == "" || adapter.cancelled != 0 {
				t.Fatalf("warm Environment was promoted during cleanup recovery: %#v, cancellations = %d", retained, adapter.cancelled)
			}
		})
	}
}

func TestOwnershipMismatchNeverCancelsOrMutatesEnvironment(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStateSucceeded,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "owned", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{
		APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: "other", UID: run.UID, Controller: ptr(true),
	}}}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-owned", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"}}}
	s := runScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&platformv1alpha1.Run{}, &platformv1alpha1.Environment{}).WithObjects(run, env).Build()
	adapter := &scriptedAdapter{}
	r := &RunReconciler{Client: c, Scheme: s, Adapters: map[string]AdapterLifecycle{"test": adapter}}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal(err)
	}
	if retained.Spec.Paused || adapter.cancelled != 0 || metav1.GetControllerOf(&retained).Name != "other" {
		t.Fatalf("environment mutated or cancelled: %#v, cancellations = %d", retained, adapter.cancelled)
	}
}
