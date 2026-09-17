package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/erikyryan/desafio-go/internal/domain/wager"
)

// Metrics implements app.Metrics, sqs.ConsumerMetrics and workers.OutboxMetrics.
type Metrics struct {
	registry *prometheus.Registry

	transactions        *prometheus.CounterVec
	replays             *prometheus.CounterVec
	conflicts           *prometheus.CounterVec
	concurrencyConflict prometheus.Counter
	processing          *prometheus.HistogramVec
	reconciliation      prometheus.Counter
	pendingRetries      prometheus.Counter

	messages        *prometheus.CounterVec
	duplicates      prometheus.Counter
	messageRetries  prometheus.Counter
	dlq             *prometheus.CounterVec
	outboxPublished prometheus.Counter
	outboxRetries   prometheus.Counter
	outboxLag       prometheus.Gauge
}

// NewMetrics registers every collector on a private registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := &Metrics{
		registry: reg,
		transactions: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "wager_transactions_total", Help: "Transactions by source, kind, status and failure code."},
			[]string{"source", "kind", "status", "failure_code"}),
		replays:             prometheus.NewCounterVec(prometheus.CounterOpts{Name: "wager_idempotent_replays_total", Help: "Duplicate submissions answered from the stored result."}, []string{"source"}),
		conflicts:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "wager_idempotency_conflicts_total", Help: "Submissions rejected for key/payload conflicts."}, []string{"source"}),
		concurrencyConflict: prometheus.NewCounter(prometheus.CounterOpts{Name: "wager_concurrency_conflicts_total", Help: "Optimistic version checks that failed."}),
		processing: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "wager_processing_duration_seconds", Help: "Use case latency.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}}, []string{"source"}),
		reconciliation:  prometheus.NewCounter(prometheus.CounterOpts{Name: "wallet_reconciliation_divergences_total", Help: "Reconciliations whose stored balance differed from the ledger."}),
		pendingRetries:  prometheus.NewCounter(prometheus.CounterOpts{Name: "wager_pending_reference_retries_total", Help: "Retries of reversals waiting for their reference."}),
		messages:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "sqs_messages_processed_total", Help: "Consumed messages by outcome."}, []string{"outcome"}),
		duplicates:      prometheus.NewCounter(prometheus.CounterOpts{Name: "sqs_messages_duplicate_total", Help: "Redeliveries deduplicated by the inbox."}),
		messageRetries:  prometheus.NewCounter(prometheus.CounterOpts{Name: "sqs_messages_retry_total", Help: "Messages left for redelivery after a transient failure."}),
		dlq:             prometheus.NewCounterVec(prometheus.CounterOpts{Name: "sqs_messages_dlq_total", Help: "Messages sent to the DLQ by the consumer."}, []string{"reason"}),
		outboxPublished: prometheus.NewCounter(prometheus.CounterOpts{Name: "outbox_published_total", Help: "Events published."}),
		outboxRetries:   prometheus.NewCounter(prometheus.CounterOpts{Name: "outbox_publish_retries_total", Help: "Publish attempts that failed and were rescheduled."}),
		outboxLag:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "outbox_lag_seconds", Help: "Age of the oldest unpublished event."}),
	}
	reg.MustRegister(m.transactions, m.replays, m.conflicts, m.concurrencyConflict, m.processing, m.reconciliation, m.pendingRetries,
		m.messages, m.duplicates, m.messageRetries, m.dlq, m.outboxPublished, m.outboxRetries, m.outboxLag)
	return m
}

// Handler serves the registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// Registry exposes the registry for tests.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

func (m *Metrics) TransactionOutcome(source string, kind wager.Kind, status wager.Status, code wager.FailureCode) {
	m.transactions.WithLabelValues(source, string(kind), string(status), string(code)).Inc()
}
func (m *Metrics) IdempotentReplay(source string)    { m.replays.WithLabelValues(source).Inc() }
func (m *Metrics) IdempotencyConflict(source string) { m.conflicts.WithLabelValues(source).Inc() }
func (m *Metrics) ConcurrencyConflict()              { m.concurrencyConflict.Inc() }
func (m *Metrics) ProcessingDuration(source string, d time.Duration) {
	m.processing.WithLabelValues(source).Observe(d.Seconds())
}
func (m *Metrics) ReconciliationDivergence()       { m.reconciliation.Inc() }
func (m *Metrics) PendingReferenceRetry()          { m.pendingRetries.Inc() }
func (m *Metrics) MessageProcessed(outcome string) { m.messages.WithLabelValues(outcome).Inc() }
func (m *Metrics) MessageDuplicate()               { m.duplicates.Inc() }
func (m *Metrics) MessageRetry()                   { m.messageRetries.Inc() }
func (m *Metrics) MessageDLQ(reason string)        { m.dlq.WithLabelValues(reason).Inc() }
func (m *Metrics) OutboxPublished()                { m.outboxPublished.Inc() }
func (m *Metrics) OutboxRetry()                    { m.outboxRetries.Inc() }
func (m *Metrics) OutboxLag(d time.Duration)       { m.outboxLag.Set(d.Seconds()) }
