package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics for declaration processor observability.
var (
	DeclarationsProcessedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "declaration_declarations_processed_total",
		Help: "Inbound messages successfully persisted and declaration.created published",
	})
	DeclarationsIdempotentTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "declaration_declarations_idempotent_total",
		Help: "Inbound messages skipped as duplicate source_event_id (idempotent replay)",
	})
	DeclarationsFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "declaration_declarations_failed_total",
		Help: "Inbound message processing failures (offset not committed for that message)",
	})
	ProcessingSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "declaration_processing_seconds",
		Help:    "Wall time to process one inbound message (persist + publish + outbox update)",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	})
)
