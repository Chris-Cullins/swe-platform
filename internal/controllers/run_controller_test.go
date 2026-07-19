package controllers

import (
	"context"
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

type scriptedAdapter struct {
	observations        []AdapterObservation
	accepted, cancelled int
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
	if !got.Spec.Paused || a.cancelled != 1 {
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
