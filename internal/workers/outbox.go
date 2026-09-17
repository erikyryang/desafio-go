// Package workers holds the background loops: outbox publisher and pending
// reference resolver. Each loop stops when its context is cancelled and
// reports completion through Run returning.
package workers

import (
	"context"
	"log/slog"
	"time"

	"github.com/erikyryan/desafio-go/internal/app"
	"github.com/erikyryan/desafio-go/internal/faults"
)

// Publisher sends one outbox record to the event destination.
type Publisher interface {
	Publish(ctx context.Context, rec app.OutboxRecord) error
}

// OutboxMetrics is the instrumentation emitted by the publisher.
type OutboxMetrics interface {
	OutboxPublished()
	OutboxRetry()
	OutboxLag(d time.Duration)
}

// OutboxConfig tunes the publisher.
type OutboxConfig struct {
	Owner          string        // instance id recorded on the lease
	PollInterval   time.Duration // wait when the outbox is empty
	BatchSize      int
	Lease          time.Duration // abandoned leases are reclaimed after this
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// OutboxPublisher drains the outbox. Several instances may run concurrently:
// claims use FOR UPDATE SKIP LOCKED plus a lease, so rows are never published
// by two publishers at once, and an expired lease (crash after claim) makes
// the row available again. A crash between SendMessage and MarkPublished
// yields a republication with the same eventId (consumers deduplicate).
type OutboxPublisher struct {
	uow       app.UnitOfWork
	publisher Publisher
	metrics   OutboxMetrics
	log       *slog.Logger
	cfg       OutboxConfig
	faults    *faults.Injector
	clock     app.Clock
}

// NewOutboxPublisher wires the worker.
func NewOutboxPublisher(uow app.UnitOfWork, publisher Publisher, metrics OutboxMetrics, log *slog.Logger, cfg OutboxConfig, inj *faults.Injector, clock app.Clock) *OutboxPublisher {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = time.Minute
	}
	if clock == nil {
		clock = time.Now
	}
	return &OutboxPublisher{uow: uow, publisher: publisher, metrics: metrics, log: log.With("component", "outbox-publisher", "owner", cfg.Owner), cfg: cfg, faults: inj, clock: clock}
}

// Run loops until ctx is cancelled.
func (p *OutboxPublisher) Run(ctx context.Context) {
	p.log.Info("outbox publisher started")
	defer p.log.Info("outbox publisher stopped")
	for ctx.Err() == nil {
		n, err := p.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			p.log.Warn("outbox iteration failed", "error", err.Error())
		}
		if n >= p.cfg.BatchSize {
			continue // more work is likely waiting
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.cfg.PollInterval):
		}
	}
}

// RunOnce claims and publishes one batch, returning how many rows were claimed.
func (p *OutboxPublisher) RunOnce(ctx context.Context) (int, error) {
	now := p.clock()
	var records []app.OutboxRecord
	err := p.uow.Do(ctx, func(ctx context.Context, st app.Store) error {
		var err error
		records, err = st.Outbox().Claim(ctx, p.cfg.Owner, p.cfg.Lease, p.cfg.BatchSize, now)
		if err != nil {
			return err
		}
		lag, err := st.Outbox().OldestPendingAge(ctx, now)
		if err == nil {
			p.metrics.OutboxLag(lag)
		}
		return err
	})
	if err != nil {
		return 0, err
	}
	for _, rec := range records {
		if ctx.Err() != nil {
			return len(records), ctx.Err()
		}
		p.publishOne(ctx, rec)
	}
	return len(records), nil
}

func (p *OutboxPublisher) publishOne(ctx context.Context, rec app.OutboxRecord) {
	log := p.log.With("eventId", rec.EventID, "eventType", rec.EventType, "aggregateId", rec.AggregateID, "correlationId", rec.CorrelationID, "attempt", rec.Attempts+1)
	pubCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err := p.publisher.Publish(pubCtx, rec)
	cancel()
	// Bookkeeping must not be lost to shutdown cancellation.
	bookCtx, cancelBook := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancelBook()
	if err != nil {
		p.metrics.OutboxRetry()
		next := p.clock().Add(backoff(p.cfg.InitialBackoff, p.cfg.MaxBackoff, rec.Attempts+1))
		log.Warn("publish failed, rescheduled", "error", err.Error(), "nextAttemptAt", next)
		if err := p.uow.Do(bookCtx, func(ctx context.Context, st app.Store) error {
			return st.Outbox().Reschedule(ctx, rec.EventID, next, truncate(err.Error(), 500))
		}); err != nil {
			log.Error("reschedule failed; lease will expire", "error", err.Error())
		}
		return
	}
	p.faults.Crash(faults.OutboxAfterPublish)
	if err := p.uow.Do(bookCtx, func(ctx context.Context, st app.Store) error {
		return st.Outbox().MarkPublished(ctx, rec.EventID, p.clock())
	}); err != nil {
		log.Error("mark published failed; event may be republished with the same eventId", "error", err.Error())
		return
	}
	p.metrics.OutboxPublished()
	log.Info("event published")
}

func backoff(initial, max time.Duration, attempt int) time.Duration {
	d := initial
	for i := 1; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	return d
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
