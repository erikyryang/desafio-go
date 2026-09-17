package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/erikyryan/desafio-go/internal/app"
	"github.com/erikyryan/desafio-go/internal/domain/money"
	"github.com/erikyryan/desafio-go/internal/domain/wager"
)

const maxBodyBytes = 64 << 10

// ErrorBody is the JSON shape of every error response.
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	if status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "1")
	}
	writeJSON(w, status, ErrorBody{Code: code, Message: message})
}

// decodeJSON reads a bounded JSON body.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, "application/json") {
		return errors.New("Content-Type must be application/json")
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		return errors.New("body too large or unreadable")
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return errors.New("malformed JSON body: " + sanitizeJSONError(err))
	}
	return nil
}

func sanitizeJSONError(err error) string {
	msg := err.Error()
	if len(msg) > 200 {
		msg = msg[:200]
	}
	return msg
}

// writeAppError maps application errors to HTTP statuses:
//
//	400 INVALID_INPUT           validation failures (correctable input)
//	401 UNAUTHENTICATED         missing/invalid/expired token
//	403 FORBIDDEN               identity not allowed for the resource
//	404 NOT_FOUND               wallet/transaction unknown (or invisible to the caller)
//	409 CONFLICT                idempotency key/payload conflicts, wallet already exists
//	422 (result body)           business rejection, handled by the submit handler
//	503 UNAVAILABLE             transient infrastructure failure (Retry-After: 1)
//	500 INTERNAL_ERROR          anything else
func writeAppError(w http.ResponseWriter, log *slog.Logger, r *http.Request, err error) {
	switch {
	case errors.Is(err, app.ErrValidation), errors.Is(err, money.ErrInvalidAmount), errors.Is(err, money.ErrInvalidCurrency),
		errors.Is(err, money.ErrScaleExceeded), errors.Is(err, money.ErrNegative), errors.Is(err, money.ErrOverflow),
		errors.Is(err, wager.ErrInvalidArgument):
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", err.Error())
	case errors.Is(err, app.ErrWalletNotFound):
		writeError(w, http.StatusNotFound, "WALLET_NOT_FOUND", "wallet not found")
	case errors.Is(err, app.ErrTransactionNotFound):
		writeError(w, http.StatusNotFound, "TRANSACTION_NOT_FOUND", "transaction not found")
	case errors.Is(err, app.ErrForbidden):
		writeError(w, http.StatusForbidden, "FORBIDDEN", "not allowed for this identity")
	case errors.Is(err, app.ErrWalletAlreadyExists):
		writeError(w, http.StatusConflict, "WALLET_ALREADY_EXISTS", "a wallet already exists for this player and currency")
	case errors.Is(err, app.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "IDEMPOTENCY_CONFLICT", "idempotency key already used with a different payload")
	case errors.Is(err, app.ErrExternalIDConflict):
		writeError(w, http.StatusConflict, "EXTERNAL_ID_CONFLICT", "externalTransactionId already used with another idempotency key")
	case errors.Is(err, app.ErrUnavailable), errors.Is(err, context.DeadlineExceeded):
		log.WarnContext(r.Context(), "dependency unavailable", "error", err.Error(), "correlationId", correlationFrom(r.Context()))
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "dependency temporarily unavailable, retry later")
	default:
		log.ErrorContext(r.Context(), "unhandled error", "error", err.Error(), "correlationId", correlationFrom(r.Context()))
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "unexpected error")
	}
}
