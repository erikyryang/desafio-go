// Package httpapi exposes the use cases over HTTP (net/http, Go 1.22 patterns).
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/erikyryan/desafio-go/internal/adapters/auth"
)

type ctxKey int

const (
	ctxIdentity ctxKey = iota
	ctxCorrelationID
)

// HeaderCorrelationID propagates a caller-supplied correlation id.
const HeaderCorrelationID = "X-Correlation-ID"

// TokenVerifier is the authentication dependency of the API.
type TokenVerifier interface {
	Verify(ctx context.Context, rawToken string) (auth.Identity, error)
}

func identityFrom(ctx context.Context) auth.Identity {
	id, _ := ctx.Value(ctxIdentity).(auth.Identity)
	return id
}

func correlationFrom(ctx context.Context) string {
	s, _ := ctx.Value(ctxCorrelationID).(string)
	return s
}

// withCorrelation assigns/propagates the correlation id.
func withCorrelation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cid := strings.TrimSpace(r.Header.Get(HeaderCorrelationID))
		if cid == "" || len(cid) > 128 {
			cid = uuid.NewString()
		}
		w.Header().Set(HeaderCorrelationID, cid)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxCorrelationID, cid)))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// withLogging writes one JSON access log line per request (no bodies, no tokens).
func withLogging(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		log.InfoContext(r.Context(), "http request",
			"method", r.Method, "path", r.URL.Path, "status", rec.status,
			"durationMs", time.Since(start).Milliseconds(), "correlationId", correlationFrom(r.Context()),
			"clientId", identityFrom(r.Context()).ClientID)
	})
}

// requestTimeout bounds every business request so that a stalled dependency
// surfaces as 503 instead of a hanging connection.
const requestTimeout = 10 * time.Second

func withTimeout(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// withRecovery converts panics into 500 responses.
func withRecovery(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if p := recover(); p != nil {
				log.ErrorContext(r.Context(), "panic in handler", "panic", p, "path", r.URL.Path)
				writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "unexpected error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// requireAuth validates the bearer token and stores the identity.
func requireAuth(v TokenVerifier, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		if !strings.HasPrefix(strings.ToLower(h), "bearer ") {
			w.Header().Set("WWW-Authenticate", `Bearer realm="wager"`)
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "missing bearer token")
			return
		}
		id, err := v.Verify(r.Context(), strings.TrimSpace(h[7:]))
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="wager", error="invalid_token"`)
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "invalid or expired token")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxIdentity, id)))
	})
}

// requireInternal restricts the endpoint to the internal service role.
func requireInternal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !identityFrom(r.Context()).IsInternal() {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "internal role required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// requireProviderOrInternal admits providers (with a providerId claim) and the internal service.
func requireProviderOrInternal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := identityFrom(r.Context())
		if !id.IsInternal() && !id.IsProvider() {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "provider or internal role required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// requireProvider admits only providers.
func requireProvider(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !identityFrom(r.Context()).IsProvider() {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "provider role required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func chain(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}
