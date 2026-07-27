package controllers

import (
	"errors"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/agent"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
)

const operatorMetricsNamespace = "swe_operator"

const (
	allocationOwnedCreate = "owned_create"
	allocationWarmClaim   = "warm_claim"

	adapterOperationEnsureAccepted = "ensure_accepted"
	adapterOperationObserve        = "observe"
	adapterOperationCancel         = "cancel"

	adapterOutcomeSuccess  = "success"
	adapterOutcomePending  = "pending"
	adapterOutcomeRejected = "rejected"
	adapterOutcomeError    = "error"

	fenceCallSiteEnsureAccepted   = "ensure_accepted"
	fenceCallSiteObserve          = "observe"
	fenceCallSiteRunCancel        = "run_cancel"
	fenceCallSiteTerminalCleanup  = "terminal_cleanup"
	fenceCallSiteFinalizerCleanup = "finalizer_cleanup"
)

var fenceCallSites = []string{
	fenceCallSiteEnsureAccepted,
	fenceCallSiteObserve,
	fenceCallSiteRunCancel,
	fenceCallSiteTerminalCleanup,
	fenceCallSiteFinalizerCleanup,
}

var fenceComponents = []string{"environment_uid", "execution_generation", "lifecycle_epoch", "hold_revision"}

var lifecycleReasons = []platformv1alpha1.EnvironmentSuspensionReason{
	platformv1alpha1.EnvironmentSuspensionReasonHold,
	platformv1alpha1.EnvironmentSuspensionReasonIdle,
	platformv1alpha1.EnvironmentSuspensionReasonRequested,
}

// OperatorMetrics owns bounded-cardinality platform metrics exposed through
// controller-runtime's existing operator metrics registry.
type OperatorMetrics struct {
	registeredAdapters       map[string]struct{}
	runAllocations           *prometheus.HistogramVec
	warmPoolAllocations      *prometheus.CounterVec
	executionFenceRejections *prometheus.CounterVec
	adapterOperations        *prometheus.CounterVec
	adapterOperationDuration *prometheus.HistogramVec
	podRecoveryTransitions   *prometheus.CounterVec
	lifecycleTransitions     *prometheus.CounterVec
}

type fenceRejectionRecorder struct {
	metrics  *OperatorMetrics
	callSite string
	once     sync.Once
}

func (r *fenceRejectionRecorder) observe(err error) {
	if r == nil || !errors.Is(err, lifecycle.ErrExecutionFenceChanged) {
		return
	}
	r.once.Do(func() { r.metrics.observeFenceRejection(err, r.callSite) })
}

// NewOperatorMetrics registers one process-owned collector set. Tests should
// pass a fresh registry; the operator passes controller-runtime's registry.
func NewOperatorMetrics(registerer prometheus.Registerer, adapters map[string]agent.AdapterLifecycle) *OperatorMetrics {
	m := &OperatorMetrics{
		registeredAdapters: make(map[string]struct{}, len(adapters)),
		runAllocations: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: operatorMetricsNamespace, Name: "run_allocation_duration_seconds",
			Help:    "Time from Run creation to durable Environment allocation publication by allocation path.",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600},
		}, []string{"path"}),
		warmPoolAllocations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: operatorMetricsNamespace, Name: "warm_pool_allocations_total",
			Help: "Automatic Run allocation outcomes against the warm pool.",
		}, []string{"outcome"}),
		executionFenceRejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: operatorMetricsNamespace, Name: "execution_fence_rejections_total",
			Help: "Stale operator results rejected by changed execution-fence component and closed call site.",
		}, []string{"component", "call_site"}),
		adapterOperations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: operatorMetricsNamespace, Name: "adapter_operations_total",
			Help: "Adapter lifecycle calls by registered adapter, operation, and outcome.",
		}, []string{"adapter", "operation", "outcome"}),
		adapterOperationDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: operatorMetricsNamespace, Name: "adapter_operation_duration_seconds",
			Help:    "Adapter lifecycle call duration by registered adapter, operation, and outcome.",
			Buckets: prometheus.DefBuckets,
		}, []string{"adapter", "operation", "outcome"}),
		podRecoveryTransitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: operatorMetricsNamespace, Name: "pod_recovery_transitions_total",
			Help: "Durably published pod recovery attempt and exhaustion transitions.",
		}, []string{"transition"}),
		lifecycleTransitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: operatorMetricsNamespace, Name: "environment_lifecycle_transitions_total",
			Help: "Durably published Environment suspension and resume transitions by fixed reason.",
		}, []string{"transition", "reason"}),
	}
	registerer.MustRegister(
		m.runAllocations, m.warmPoolAllocations, m.executionFenceRejections,
		m.adapterOperations, m.adapterOperationDuration, m.podRecoveryTransitions,
		m.lifecycleTransitions,
	)
	for _, path := range []string{allocationOwnedCreate, allocationWarmClaim} {
		m.runAllocations.WithLabelValues(path)
	}
	for _, outcome := range []string{"hit", "miss"} {
		m.warmPoolAllocations.WithLabelValues(outcome)
	}
	for _, component := range fenceComponents {
		for _, callSite := range fenceCallSites {
			m.executionFenceRejections.WithLabelValues(component, callSite)
		}
	}
	for adapter := range adapters {
		m.registeredAdapters[adapter] = struct{}{}
		for _, operation := range []string{adapterOperationEnsureAccepted, adapterOperationObserve, adapterOperationCancel} {
			for _, outcome := range []string{adapterOutcomeSuccess, adapterOutcomePending, adapterOutcomeRejected, adapterOutcomeError} {
				m.adapterOperations.WithLabelValues(adapter, operation, outcome)
				m.adapterOperationDuration.WithLabelValues(adapter, operation, outcome)
			}
		}
	}
	for _, transition := range []string{"attempt", "exhausted"} {
		m.podRecoveryTransitions.WithLabelValues(transition)
	}
	for _, transition := range []string{"suspend", "resume"} {
		for _, reason := range lifecycleReasons {
			m.lifecycleTransitions.WithLabelValues(transition, string(reason))
		}
	}
	return m
}

func (m *OperatorMetrics) observeAllocation(createdAt time.Time, path string) {
	if m == nil {
		return
	}
	duration := time.Since(createdAt).Seconds()
	if duration < 0 {
		duration = 0
	}
	m.runAllocations.WithLabelValues(path).Observe(duration)
	if path == allocationWarmClaim {
		m.warmPoolAllocations.WithLabelValues("hit").Inc()
	} else {
		m.warmPoolAllocations.WithLabelValues("miss").Inc()
	}
}

func (m *OperatorMetrics) observeAdapter(adapter, operation string, started time.Time, err error) {
	if m == nil {
		return
	}
	if _, ok := m.registeredAdapters[adapter]; !ok {
		return
	}
	outcome := adapterOutcomeSuccess
	switch {
	case operation == adapterOperationEnsureAccepted && errors.Is(err, agent.ErrAdapterTaskRejected):
		outcome = adapterOutcomeRejected
	case operation == adapterOperationCancel && errors.Is(err, agent.ErrAdapterCancellationPending):
		outcome = adapterOutcomePending
	case err != nil:
		outcome = adapterOutcomeError
	}
	m.adapterOperations.WithLabelValues(adapter, operation, outcome).Inc()
	m.adapterOperationDuration.WithLabelValues(adapter, operation, outcome).Observe(time.Since(started).Seconds())
}

func (m *OperatorMetrics) observeFenceRejection(err error, callSite string) {
	if m == nil || !errors.Is(err, lifecycle.ErrExecutionFenceChanged) {
		return
	}
	component := ""
	switch {
	case errors.Is(err, lifecycle.ErrEnvironmentIncarnationChanged):
		component = "environment_uid"
	case errors.Is(err, lifecycle.ErrExecutionGenerationChanged):
		component = "execution_generation"
	case errors.Is(err, lifecycle.ErrLifecycleEpochChanged):
		component = "lifecycle_epoch"
	case errors.Is(err, lifecycle.ErrHoldPolicyChanged):
		component = "hold_revision"
	}
	if component != "" {
		m.executionFenceRejections.WithLabelValues(component, callSite).Inc()
	}
}

func (m *OperatorMetrics) observePodRecovery(transition string) {
	if m != nil {
		m.podRecoveryTransitions.WithLabelValues(transition).Inc()
	}
}

func (m *OperatorMetrics) observeLifecycle(transition string, reason platformv1alpha1.EnvironmentSuspensionReason) {
	if m == nil {
		return
	}
	for _, supported := range lifecycleReasons {
		if reason == supported {
			m.lifecycleTransitions.WithLabelValues(transition, string(reason)).Inc()
			return
		}
	}
}
