package workers

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// Runner is a background loop that returns when its context is cancelled.
type Runner interface {
	Run(ctx context.Context)
}

// Supervisor starts runners with a shared cancellable context and waits for
// all of them on Stop, bounded by the caller's deadline.
type Supervisor struct {
	log     *slog.Logger
	mu      sync.Mutex
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	names   []string
	started bool
}

// NewSupervisor creates an empty supervisor.
func NewSupervisor(log *slog.Logger) *Supervisor { return &Supervisor{log: log} }

// Start launches each runner in its own goroutine.
func (s *Supervisor) Start(ctx context.Context, runners map[string]Runner) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return
	}
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.cancel = cancel
	s.started = true
	for name, r := range runners {
		s.names = append(s.names, name)
		s.wg.Add(1)
		go func(name string, r Runner) {
			defer s.wg.Done()
			defer func() {
				if p := recover(); p != nil {
					s.log.Error("worker panicked", "worker", name, "panic", p)
				}
			}()
			r.Run(runCtx)
		}(name, r)
	}
}

// Stop cancels the workers and waits for them to finish or ctx to expire.
func (s *Supervisor) Stop(ctx context.Context) error {
	s.mu.Lock()
	cancel := s.cancel
	s.mu.Unlock()
	if cancel == nil {
		return nil
	}
	start := time.Now()
	cancel()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		s.log.Info("workers stopped", "workers", s.names, "durationMs", time.Since(start).Milliseconds())
		return nil
	case <-ctx.Done():
		return errors.New("workers did not stop before the shutdown deadline")
	}
}

// Wait blocks until every worker has returned (test helper).
func (s *Supervisor) Wait() { s.wg.Wait() }
