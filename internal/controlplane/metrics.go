package controlplane

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const metricsNamespace = "swe_control_plane"

// Metrics owns the control plane's bounded-cardinality operational metrics.
// Each process uses one instance and one registry so tests and embedded users do
// not share global collector state.
type Metrics struct {
	transcriptAppends         *prometheus.CounterVec
	transcriptAppendLatency   *prometheus.HistogramVec
	transcriptSubscribers     prometheus.Gauge
	transcriptDeliveries      *prometheus.CounterVec
	transcriptDeliveryLag     *prometheus.HistogramVec
	transcriptCleanups        *prometheus.CounterVec
	transcriptCleanupLatency  *prometheus.HistogramVec
	transcriptReclaimedEvents prometheus.Counter
	transcriptReclaimedBytes  prometheus.Counter
	terminalLeaseGrants       *prometheus.CounterVec
	terminalRevocations       *prometheus.CounterVec
	tokenReviews              *prometheus.CounterVec
	tokenReviewLatency        *prometheus.HistogramVec
	subjectAccessReviews      *prometheus.CounterVec
	subjectAccessLatency      *prometheus.HistogramVec
}

// NewMetrics registers the control-plane collectors with registerer.
func NewMetrics(registerer prometheus.Registerer) *Metrics {
	m := &Metrics{
		transcriptAppends: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "transcript_appends_total",
			Help: "Transcript append attempts by outcome.",
		}, []string{"outcome"}),
		transcriptAppendLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "transcript_append_duration_seconds",
			Help:    "Transcript store append latency by outcome.",
			Buckets: prometheus.DefBuckets,
		}, []string{"outcome"}),
		transcriptSubscribers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Name: "transcript_sse_subscribers",
			Help: "Current admitted transcript SSE subscribers.",
		}),
		transcriptDeliveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "transcript_sse_deliveries_total",
			Help: "Transcript SSE delivery outcomes by delivery kind.",
		}, []string{"kind", "outcome"}),
		transcriptDeliveryLag: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "transcript_sse_delivery_lag_seconds",
			Help:    "Time from transcript event creation to successful SSE delivery.",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600},
		}, []string{"kind"}),
		transcriptCleanups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "transcript_cleanup_total",
			Help: "Exact transcript cleanup attempts by bounded outcome.",
		}, []string{"outcome"}),
		transcriptCleanupLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "transcript_cleanup_duration_seconds",
			Help:    "Exact transcript cutoff, drain, and store deletion latency by bounded outcome.",
			Buckets: prometheus.DefBuckets,
		}, []string{"outcome"}),
		transcriptReclaimedEvents: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "transcript_cleanup_reclaimed_events_total",
			Help: "Transcript events reclaimed by exact cleanup.",
		}),
		transcriptReclaimedBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "transcript_cleanup_reclaimed_bytes_total",
			Help: "Transcript bytes reclaimed by exact cleanup.",
		}),
		terminalLeaseGrants: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "terminal_lease_grants_total",
			Help: "Terminal lease grant attempts by outcome.",
		}, []string{"outcome"}),
		terminalRevocations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "terminal_lease_revocations_total",
			Help: "Granted terminal leases revoked by the control plane.",
		}, []string{"reason"}),
		tokenReviews: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "token_reviews_total",
			Help: "Kubernetes TokenReview outcomes.",
		}, []string{"outcome"}),
		tokenReviewLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "token_review_duration_seconds",
			Help:    "Kubernetes TokenReview latency by outcome.",
			Buckets: prometheus.DefBuckets,
		}, []string{"outcome"}),
		subjectAccessReviews: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "subject_access_reviews_total",
			Help: "Kubernetes SubjectAccessReview outcomes.",
		}, []string{"outcome"}),
		subjectAccessLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "subject_access_review_duration_seconds",
			Help:    "Kubernetes SubjectAccessReview latency by outcome.",
			Buckets: prometheus.DefBuckets,
		}, []string{"outcome"}),
	}
	registerer.MustRegister(
		m.transcriptAppends, m.transcriptAppendLatency, m.transcriptSubscribers,
		m.transcriptDeliveries, m.transcriptDeliveryLag, m.transcriptCleanups,
		m.transcriptCleanupLatency, m.transcriptReclaimedEvents, m.transcriptReclaimedBytes,
		m.terminalLeaseGrants,
		m.terminalRevocations, m.tokenReviews, m.tokenReviewLatency,
		m.subjectAccessReviews, m.subjectAccessLatency,
	)
	for _, outcome := range []string{"committed", "replayed", "rejected", "error"} {
		m.transcriptAppends.WithLabelValues(outcome)
		m.transcriptAppendLatency.WithLabelValues(outcome)
	}
	for _, labels := range [][2]string{
		{"replay", "delivered"}, {"replay", "error"},
		{"live", "delivered"}, {"live", "error"}, {"live", "dropped"},
		{"gap", "delivered"}, {"gap", "error"},
	} {
		m.transcriptDeliveries.WithLabelValues(labels[0], labels[1])
	}
	for _, kind := range []string{"replay", "live"} {
		m.transcriptDeliveryLag.WithLabelValues(kind)
	}
	for _, outcome := range []string{"committed", "already_absent", "rejected", "error"} {
		m.transcriptCleanups.WithLabelValues(outcome)
		m.transcriptCleanupLatency.WithLabelValues(outcome)
	}
	for _, outcome := range []string{"granted", "failed"} {
		m.terminalLeaseGrants.WithLabelValues(outcome)
	}
	for _, reason := range []string{"run_association_changed", "environment_changed", "execution_changed", "hold_policy_changed"} {
		m.terminalRevocations.WithLabelValues(reason)
	}
	for _, outcome := range []string{"authenticated", "denied", "error"} {
		m.tokenReviews.WithLabelValues(outcome)
		m.tokenReviewLatency.WithLabelValues(outcome)
	}
	for _, outcome := range []string{"allowed", "denied", "error"} {
		m.subjectAccessReviews.WithLabelValues(outcome)
		m.subjectAccessLatency.WithLabelValues(outcome)
	}
	return m
}

func (m *Metrics) observeCleanup(start time.Time, outcome string, result DeleteTranscriptResult) {
	if m == nil {
		return
	}
	m.transcriptCleanups.WithLabelValues(outcome).Inc()
	m.transcriptCleanupLatency.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
	m.transcriptReclaimedEvents.Add(float64(result.ReclaimedEvents))
	m.transcriptReclaimedBytes.Add(float64(result.ReclaimedBytes))
}

// MetricsHandler exposes only the Prometheus scrape route. It deliberately
// does not reuse the application handler or its authenticated API routes.
func MetricsHandler(gatherer prometheus.Gatherer) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	return mux
}

func (m *Metrics) observeAppend(start time.Time, outcome string) {
	if m == nil {
		return
	}
	m.transcriptAppends.WithLabelValues(outcome).Inc()
	m.transcriptAppendLatency.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
}

func (m *Metrics) observeDelivery(kind, outcome string, event *TranscriptEvent) {
	if m == nil {
		return
	}
	m.transcriptDeliveries.WithLabelValues(kind, outcome).Inc()
	if outcome == "delivered" && event != nil {
		lag := time.Since(event.CreatedAt).Seconds()
		if lag < 0 {
			lag = 0
		}
		m.transcriptDeliveryLag.WithLabelValues(kind).Observe(lag)
	}
}

func (m *Metrics) observeReview(kind, outcome string, start time.Time) {
	if m == nil {
		return
	}
	if kind == "token" {
		m.tokenReviews.WithLabelValues(outcome).Inc()
		m.tokenReviewLatency.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
		return
	}
	m.subjectAccessReviews.WithLabelValues(outcome).Inc()
	m.subjectAccessLatency.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
}
