package controllers

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/agent"
)

func testOperatorMetrics(t *testing.T) (*OperatorMetrics, *prometheus.Registry) {
	t.Helper()
	registry := prometheus.NewRegistry()
	metrics := NewOperatorMetrics(registry, map[string]agent.AdapterLifecycle{"test": &scriptedAdapter{}})
	return metrics, registry
}

type dialAfterMutationAdapter struct {
	mutate func()
}

func (a *dialAfterMutationAdapter) EnsureAccepted(ctx context.Context, _ agent.AdapterTask, sandbox agent.AdapterSandbox, _ *agent.AdapterCredential) error {
	a.mutate()
	_, _, err := sandbox.DialProcess(ctx)
	return err
}

func (*dialAfterMutationAdapter) Observe(context.Context, agent.AdapterTask, agent.AdapterSandbox) (agent.AdapterObservation, string, error) {
	return agent.AdapterObservationRunning, "running", nil
}

func (*dialAfterMutationAdapter) Cancel(context.Context, agent.AdapterTask, agent.AdapterSandbox) error {
	return nil
}

func TestOperatorMetricsObserveRealAllocationHitAndMiss(t *testing.T) {
	for _, test := range []struct {
		name     string
		warm     bool
		wantPath string
		outcome  string
	}{
		{name: "owned create misses warm pool", wantPath: allocationOwnedCreate, outcome: "miss"},
		{name: "warm claim hits warm pool", warm: true, wantPath: allocationWarmClaim, outcome: "hit"},
	} {
		t.Run(test.name, func(t *testing.T) {
			createdAt := metav1.NewTime(time.Now().Add(-time.Second))
			run := &platformv1alpha1.Run{
				ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, CreationTimestamp: createdAt},
				Spec:       platformv1alpha1.RunSpec{TemplateRef: "small", ProjectRef: "project", Agent: "test"},
			}
			template := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "ns", UID: "template-uid"}}
			objects := []client.Object{run, template}
			if test.warm {
				warm := &platformv1alpha1.Environment{
					ObjectMeta: metav1.ObjectMeta{
						Name: "warm-small-1", Namespace: "ns", UID: "warm-uid", Labels: map[string]string{warmPoolLabel: "small"},
						OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "EnvironmentTemplate", Name: template.Name, UID: template.UID, Controller: ptr(true)}},
					},
					Spec:   platformv1alpha1.EnvironmentSpec{TemplateRef: template.Name},
					Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady},
				}
				objects = append(objects, warm)
			}
			r := reconciler(t, &scriptedAdapter{}, objects...)
			metrics, _ := testOperatorMetrics(t)
			r.Metrics = metrics

			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
				t.Fatal(err)
			}
			if got := testutil.ToFloat64(metrics.warmPoolAllocations.WithLabelValues(test.outcome)); got != 1 {
				t.Fatalf("warm pool %s = %v, want 1", test.outcome, got)
			}
			if got := histogramSampleCount(t, metrics.runAllocations.WithLabelValues(test.wantPath)); got != 1 {
				t.Fatalf("allocation histogram %s count = %d, want 1", test.wantPath, got)
			}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
				t.Fatal(err)
			}
			if got := histogramSampleCount(t, metrics.runAllocations.WithLabelValues(test.wantPath)); got != 1 {
				t.Fatalf("allocation was recounted on reconcile: %d", got)
			}
		})
	}
}

func TestOperatorMetricsObserveAdapterOutcomesOnCalls(t *testing.T) {
	tests := []struct {
		name      string
		state     platformv1alpha1.RunState
		configure func(*platformv1alpha1.Run, *scriptedAdapter)
		operation string
		outcome   string
	}{
		{name: "accept success", state: platformv1alpha1.RunStateEnvironmentReady, operation: adapterOperationEnsureAccepted, outcome: adapterOutcomeSuccess},
		{name: "accept rejected", state: platformv1alpha1.RunStateEnvironmentReady, operation: adapterOperationEnsureAccepted, outcome: adapterOutcomeRejected, configure: func(_ *platformv1alpha1.Run, adapter *scriptedAdapter) {
			adapter.acceptErr = agent.ErrAdapterTaskRejected
		}},
		{name: "observe error", state: platformv1alpha1.RunStateRunning, operation: adapterOperationObserve, outcome: adapterOutcomeError, configure: func(_ *platformv1alpha1.Run, adapter *scriptedAdapter) {
			adapter.observeErr = errors.New("rpc unavailable")
		}},
		{name: "invalid observation", state: platformv1alpha1.RunStateRunning, operation: adapterOperationObserve, outcome: adapterOutcomeError, configure: func(_ *platformv1alpha1.Run, adapter *scriptedAdapter) {
			adapter.observations = []agent.AdapterObservation{"Unknown"}
		}},
		{name: "cancel pending", state: platformv1alpha1.RunStateRunning, operation: adapterOperationCancel, outcome: adapterOutcomePending, configure: func(run *platformv1alpha1.Run, adapter *scriptedAdapter) {
			run.Spec.Cancel = true
			adapter.cancelErr = agent.ErrAdapterCancellationPending
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run, env := adapterRaceObjects(platformv1alpha1.EnvironmentOwnershipOwned, test.state)
			adapter := &scriptedAdapter{observations: []agent.AdapterObservation{agent.AdapterObservationRunning}}
			if test.state == platformv1alpha1.RunStateEnvironmentReady {
				setAcceptanceAttempted(run)
			} else {
				epoch, generation := int64(0), int64(1)
				run.Status.AcceptedEnvironmentEpoch = &epoch
				run.Status.AcceptedEnvironmentExecutionGeneration = &generation
				setAdapterAccepted(run)
			}
			if test.configure != nil {
				test.configure(run, adapter)
			}
			r := reconciler(t, adapter, run, env)
			metrics, _ := testOperatorMetrics(t)
			r.Metrics = metrics

			_, _ = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
			if got := testutil.ToFloat64(metrics.adapterOperations.WithLabelValues("test", test.operation, test.outcome)); got != 1 {
				t.Fatalf("adapter %s/%s = %v, want 1", test.operation, test.outcome, got)
			}
			if got := histogramSampleCount(t, metrics.adapterOperationDuration.WithLabelValues("test", test.operation, test.outcome)); got != 1 {
				t.Fatalf("adapter duration %s/%s count = %d, want 1", test.operation, test.outcome, got)
			}
		})
	}
}

func TestOperatorMetricsCountPostCallFenceRejection(t *testing.T) {
	run, env := adapterRaceObjects(platformv1alpha1.EnvironmentOwnershipOwned, platformv1alpha1.RunStateEnvironmentReady)
	setAcceptanceAttempted(run)
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	metrics, _ := testOperatorMetrics(t)
	r.Metrics = metrics
	adapter.onAccept = func() {
		var current platformv1alpha1.Environment
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
			t.Fatal(err)
		}
		current.Status.ExecutionGeneration++
		if err := r.Status().Update(context.Background(), &current); err != nil {
			t.Fatal(err)
		}
	}

	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil || !result.Requeue {
		t.Fatalf("stale acceptance = (%#v, %v)", result, err)
	}
	if got := testutil.ToFloat64(metrics.executionFenceRejections.WithLabelValues("execution_generation", fenceCallSiteEnsureAccepted)); got != 1 {
		t.Fatalf("fence rejection count = %v, want 1", got)
	}
}

func TestOperatorMetricsCountEnvironmentReplacementFenceRejection(t *testing.T) {
	run, env := adapterRaceObjects(platformv1alpha1.EnvironmentOwnershipOwned, platformv1alpha1.RunStateEnvironmentReady)
	setAcceptanceAttempted(run)
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	metrics, _ := testOperatorMetrics(t)
	r.Metrics = metrics
	adapter.onAccept = func() {
		var current platformv1alpha1.Environment
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
			t.Fatal(err)
		}
		if err := r.Delete(context.Background(), &current); err != nil {
			t.Fatal(err)
		}
		replacement := current.DeepCopy()
		replacement.ResourceVersion = ""
		replacement.UID = "replacement-uid"
		replacement.Status = platformv1alpha1.EnvironmentStatus{}
		if err := r.Create(context.Background(), replacement); err != nil {
			t.Fatal(err)
		}
	}

	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil || !result.Requeue {
		t.Fatalf("replacement acceptance = (%#v, %v)", result, err)
	}
	if got := testutil.ToFloat64(metrics.executionFenceRejections.WithLabelValues("environment_uid", fenceCallSiteEnsureAccepted)); got != 1 {
		t.Fatalf("environment UID fence rejection count = %v, want 1", got)
	}
	var retained platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &retained); err != nil {
		t.Fatal(err)
	}
	if retained.Status.State != platformv1alpha1.RunStateEnvironmentReady {
		t.Fatalf("stale acceptance changed Run state to %s", retained.Status.State)
	}
}

func TestOperatorMetricsCountStaleDialOnlyOnce(t *testing.T) {
	run, env := adapterRaceObjects(platformv1alpha1.EnvironmentOwnershipOwned, platformv1alpha1.RunStateEnvironmentReady)
	setAcceptanceAttempted(run)
	var r *RunReconciler
	adapter := &dialAfterMutationAdapter{mutate: func() {
		var current platformv1alpha1.Environment
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
			t.Fatal(err)
		}
		current.Status.ExecutionGeneration++
		if err := r.Status().Update(context.Background(), &current); err != nil {
			t.Fatal(err)
		}
	}}
	r = reconciler(t, adapter, run, env)
	metrics, _ := testOperatorMetrics(t)
	r.Metrics = metrics

	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil || !result.Requeue {
		t.Fatalf("stale dial acceptance = (%#v, %v)", result, err)
	}
	if got := testutil.ToFloat64(metrics.executionFenceRejections.WithLabelValues("execution_generation", fenceCallSiteEnsureAccepted)); got != 1 {
		t.Fatalf("stale dial fence rejection count = %v, want exactly 1", got)
	}
}

func TestOperatorMetricsDoNotCountAssociationFailureAsFenceRejection(t *testing.T) {
	run, env := adapterRaceObjects(platformv1alpha1.EnvironmentOwnershipOwned, platformv1alpha1.RunStateEnvironmentReady)
	setAcceptanceAttempted(run)
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	metrics, _ := testOperatorMetrics(t)
	r.Metrics = metrics
	var live platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &live); err != nil {
		t.Fatal(err)
	}
	live.OwnerReferences = nil
	live.Status.ExecutionGeneration++
	r.APIReader = fake.NewClientBuilder().WithScheme(r.Scheme).WithObjects(&live).Build()

	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil || !result.Requeue || adapter.accepted != 0 {
		t.Fatalf("association failure = (%#v, %v), adapter calls %d", result, err, adapter.accepted)
	}
	for _, component := range fenceComponents {
		if got := testutil.ToFloat64(metrics.executionFenceRejections.WithLabelValues(component, fenceCallSiteEnsureAccepted)); got != 0 {
			t.Fatalf("association failure counted as %s fence rejection: %v", component, got)
		}
	}
}

func TestOperatorMetricsCountRecoveryTransitionsOnce(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	due := metav1.NewTime(now.Add(-time.Second))
	pending := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "ns", UID: "pending-uid"}, Status: platformv1alpha1.EnvironmentStatus{PodRecoveryUID: "dead", PodRecoveryNextAttemptAt: &due}}
	exhausted := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "exhausted", Namespace: "ns", UID: "exhausted-uid"}, Status: platformv1alpha1.EnvironmentStatus{PodRecoveryAttempts: podRecoveryLimit, PodRecoveryUID: "previous"}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "env-exhausted", Namespace: "ns", UID: "terminal"}, Status: corev1.PodStatus{Phase: corev1.PodFailed}}
	metrics, _ := testOperatorMetrics(t)
	r := &EnvironmentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(pending, exhausted).WithObjects(pending, exhausted).Build(),
		Now:    func() time.Time { return now }, Metrics: metrics,
	}
	var currentPending platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(pending), &currentPending); err != nil {
		t.Fatal(err)
	}
	if _, handled, err := r.reconcilePendingPodRecovery(context.Background(), &currentPending); err != nil || !handled {
		t.Fatalf("recovery attempt = handled %t, error %v", handled, err)
	}
	if _, err := r.reconcileTerminalPod(context.Background(), exhausted, pod); err != nil {
		t.Fatal(err)
	}
	var current platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(exhausted), &current); err != nil {
		t.Fatal(err)
	}
	if _, handled, err := r.reconcilePendingPodRecovery(context.Background(), &current); err != nil || !handled {
		t.Fatalf("latched exhaustion = handled %t, error %v", handled, err)
	}
	if got := testutil.ToFloat64(metrics.podRecoveryTransitions.WithLabelValues("attempt")); got != 1 {
		t.Fatalf("recovery attempts = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.podRecoveryTransitions.WithLabelValues("exhausted")); got != 1 {
		t.Fatalf("recovery exhaustion = %v, want 1", got)
	}
}

func TestOperatorMetricsCountLifecycleTransitionsOnce(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "held", Namespace: "ns", UID: "env-uid"},
		Spec: platformv1alpha1.EnvironmentSpec{Lifecycle: platformv1alpha1.EnvironmentLifecycleSpec{
			Hold: &platformv1alpha1.EnvironmentHoldPolicy{Enabled: true, Revision: 1},
		}},
		Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhasePaused},
	}
	metrics, _ := testOperatorMetrics(t)
	r := &EnvironmentReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build(), Metrics: metrics}
	changed, err := r.reconcileLifecycleIntent(context.Background(), env)
	if err != nil || !changed {
		t.Fatalf("suspend = (%t, %v)", changed, err)
	}
	var current platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
		t.Fatal(err)
	}
	if changed, err := r.reconcileLifecycleIntent(context.Background(), &current); err != nil || changed {
		t.Fatalf("reobserved suspend = (%t, %v)", changed, err)
	}
	current.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Revision: 2}
	if err := r.Update(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	if changed, err := r.reconcileLifecycleIntent(context.Background(), &current); err != nil || !changed {
		t.Fatalf("resume = (%t, %v)", changed, err)
	}
	if got := testutil.ToFloat64(metrics.lifecycleTransitions.WithLabelValues("suspend", string(platformv1alpha1.EnvironmentSuspensionReasonHold))); got != 1 {
		t.Fatalf("suspensions = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.lifecycleTransitions.WithLabelValues("resume", string(platformv1alpha1.EnvironmentSuspensionReasonHold))); got != 1 {
		t.Fatalf("resumes = %v, want 1", got)
	}
}

func TestOperatorMetricsHaveFixedCardinalityAndNoIdentityLabels(t *testing.T) {
	metrics, registry := testOperatorMetrics(t)
	metrics.observeAdapter("namespace/run/user@example.com", adapterOperationObserve, time.Now(), nil)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 7 {
		t.Fatalf("metric family count = %d, want 7", len(families))
	}
	allowedLabels := map[string]map[string]bool{
		"path":       {allocationOwnedCreate: true, allocationWarmClaim: true},
		"outcome":    {"hit": true, "miss": true, adapterOutcomeSuccess: true, adapterOutcomePending: true, adapterOutcomeRejected: true, adapterOutcomeError: true},
		"component":  {"environment_uid": true, "execution_generation": true, "lifecycle_epoch": true, "hold_revision": true},
		"call_site":  {fenceCallSiteEnsureAccepted: true, fenceCallSiteObserve: true, fenceCallSiteRunCancel: true, fenceCallSiteTerminalCleanup: true, fenceCallSiteFinalizerCleanup: true},
		"adapter":    {"test": true},
		"operation":  {adapterOperationEnsureAccepted: true, adapterOperationObserve: true, adapterOperationCancel: true},
		"transition": {"attempt": true, "exhausted": true, "suspend": true, "resume": true},
		"reason":     {string(platformv1alpha1.EnvironmentSuspensionReasonHold): true, string(platformv1alpha1.EnvironmentSuspensionReasonIdle): true, string(platformv1alpha1.EnvironmentSuspensionReasonRequested): true},
	}
	for _, family := range families {
		if !strings.HasPrefix(family.GetName(), operatorMetricsNamespace+"_") {
			t.Fatalf("unexpected metric family %q", family.GetName())
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				values, ok := allowedLabels[label.GetName()]
				if !ok {
					t.Fatalf("unbounded label name %q on %s", label.GetName(), family.GetName())
				}
				if !values[label.GetValue()] {
					t.Fatalf("unbounded label value %s=%q on %s", label.GetName(), label.GetValue(), family.GetName())
				}
			}
		}
	}
	if got := testutil.ToFloat64(metrics.adapterOperations.WithLabelValues("test", adapterOperationObserve, adapterOutcomeSuccess)); got != 0 {
		t.Fatalf("unregistered adapter changed registered series: %v", got)
	}
}

func histogramSampleCount(t *testing.T, observer prometheus.Observer) uint64 {
	t.Helper()
	metric, ok := observer.(prometheus.Metric)
	if !ok {
		t.Fatal("observer does not implement prometheus.Metric")
	}
	dtoMetric := &dto.Metric{}
	if err := metric.Write(dtoMetric); err != nil {
		t.Fatal(err)
	}
	return dtoMetric.GetHistogram().GetSampleCount()
}

func setAcceptanceAttempted(run *platformv1alpha1.Run) {
	apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue, Reason: "AcceptancePending"})
}

func setAdapterAccepted(run *platformv1alpha1.Run) {
	apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue, Reason: "AdapterAccepted"})
}
