package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// ReadinessCheck verifies one dependency.
type ReadinessCheck interface {
	Name() string
	Check(ctx context.Context) error
}

// Router assembles the HTTP mux with authentication and authorization.
func Router(h *Handlers, verifier TokenVerifier, checks []ReadinessCheck, metrics http.Handler, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	// Public endpoints.
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
	})
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		result := map[string]string{}
		status := http.StatusOK
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, c := range checks {
			wg.Add(1)
			go func(c ReadinessCheck) {
				defer wg.Done()
				err := c.Check(ctx)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					result[c.Name()] = "DOWN: " + err.Error()
					status = http.StatusServiceUnavailable
				} else {
					result[c.Name()] = "UP"
				}
			}(c)
		}
		wg.Wait()
		overall := "UP"
		if status != http.StatusOK {
			overall = "DOWN"
		}
		writeJSON(w, status, map[string]any{"status": overall, "checks": result})
	})
	if metrics != nil {
		mux.Handle("GET /metrics", metrics)
	}

	authed := func(next http.HandlerFunc, mws ...func(http.Handler) http.Handler) http.Handler {
		return chain(next, append([]func(http.Handler) http.Handler{func(n http.Handler) http.Handler { return requireAuth(verifier, n) }}, mws...)...)
	}
	// Wallet operations: internal service only.
	mux.Handle("POST /wallets", authed(h.openWallet, requireInternal))
	mux.Handle("GET /wallets/{walletId}", authed(h.getWallet, requireInternal))
	mux.Handle("GET /wallets/{walletId}/ledger", authed(h.listLedger, requireInternal))
	mux.Handle("POST /wallets/{walletId}/reconciliation", authed(h.reconcile, requireInternal))
	// Provider operations.
	mux.Handle("POST /wagering/transactions", authed(h.submit, requireProvider))
	mux.Handle("GET /wagering/transactions/{transactionId}", authed(h.getTransaction, requireProviderOrInternal))
	mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", authed(h.getProviderTransaction, requireProviderOrInternal))

	return chain(mux, func(n http.Handler) http.Handler { return withRecovery(log, n) }, withCorrelation,
		func(n http.Handler) http.Handler { return withLogging(log, n) }, withTimeout)
}

// Server wraps http.Server with graceful shutdown.
type Server struct {
	srv *http.Server
	log *slog.Logger
}

// NewServer builds the server for addr.
func NewServer(addr string, handler http.Handler, log *slog.Logger) *Server {
	return &Server{log: log, srv: &http.Server{
		Addr: addr, Handler: handler,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
	}}
}

// Start begins serving in the background; errors other than a clean close
// are reported through errCh.
func (s *Server) Start(errCh chan<- error) {
	go func() {
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("http server stopped", "error", err)
			if errCh != nil {
				errCh <- err
			}
		}
	}()
}

// Stop refuses new connections and waits for in-flight requests until ctx expires.
func (s *Server) Stop(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// Addr returns the configured address.
func (s *Server) Addr() string { return s.srv.Addr }
