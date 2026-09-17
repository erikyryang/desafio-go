package workers

import (
	"context"
	"log/slog"
	"time"

	"github.com/erikyryan/desafio-go/internal/app"
)

// PendingConfig tunes the pending reference resolver.
type PendingConfig struct {
	PollInterval time.Duration
	BatchSize    int
}

// PendingReferenceWorker retries reversals parked as PENDING_REFERENCE. The
// schedule (next_attempt_at, attempts) lives in the database, so retries
// survive restarts and any instance may pick a due row.
type PendingReferenceWorker struct {
	wagers *app.WagerService
	log    *slog.Logger
	cfg    PendingConfig
}

// NewPendingReferenceWorker wires the worker.
func NewPendingReferenceWorker(wagers *app.WagerService, log *slog.Logger, cfg PendingConfig) *PendingReferenceWorker {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	return &PendingReferenceWorker{wagers: wagers, log: log.With("component", "pending-reference-worker"), cfg: cfg}
}

// Run loops until ctx is cancelled.
func (w *PendingReferenceWorker) Run(ctx context.Context) {
	w.log.Info("pending reference worker started")
	defer w.log.Info("pending reference worker stopped")
	for ctx.Err() == nil {
		n, err := w.wagers.ResolvePendingReferences(ctx, w.cfg.BatchSize)
		if err != nil && ctx.Err() == nil {
			w.log.Warn("pending reference iteration failed", "error", err.Error())
		}
		if n >= w.cfg.BatchSize {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(w.cfg.PollInterval):
		}
	}
}
