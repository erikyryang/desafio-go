package httpapi

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/erikyryan/desafio-go/internal/app"
	"github.com/erikyryan/desafio-go/internal/domain/money"
	"github.com/erikyryan/desafio-go/internal/domain/wager"
)

// Handlers groups the business endpoints.
type Handlers struct {
	wallets *app.WalletService
	wagers  *app.WagerService
	log     *slog.Logger
}

// NewHandlers wires the use cases.
func NewHandlers(wallets *app.WalletService, wagers *app.WagerService, log *slog.Logger) *Handlers {
	return &Handlers{wallets: wallets, wagers: wagers, log: log}
}

// --- wallets -----------------------------------------------------------------

type openWalletRequest struct {
	PlayerID       string       `json:"playerId"`
	InitialBalance *money.Money `json:"initialBalance"`
}

// WalletResponse is the JSON view of a wallet.
type WalletResponse struct {
	ID        uuid.UUID   `json:"id"`
	PlayerID  uuid.UUID   `json:"playerId"`
	Balance   money.Money `json:"balance"`
	Version   int64       `json:"version"`
	CreatedAt time.Time   `json:"createdAt"`
	UpdatedAt time.Time   `json:"updatedAt"`
}

func walletResponse(w *wager.Wallet) WalletResponse {
	return WalletResponse{ID: w.ID(), PlayerID: w.PlayerID(), Balance: w.Balance(), Version: w.Version(), CreatedAt: w.CreatedAt(), UpdatedAt: w.UpdatedAt()}
}

func (h *Handlers) openWallet(w http.ResponseWriter, r *http.Request) {
	var req openWalletRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", err.Error())
		return
	}
	playerID, err := uuid.Parse(req.PlayerID)
	if err != nil || playerID == uuid.Nil {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", "playerId must be a UUID")
		return
	}
	if req.InitialBalance == nil {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", "initialBalance is required")
		return
	}
	wallet, err := h.wallets.OpenWallet(r.Context(), app.OpenWalletCommand{
		PlayerID: playerID, InitialBalance: *req.InitialBalance, CorrelationID: correlationFrom(r.Context()),
	})
	if err != nil {
		writeAppError(w, h.log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, walletResponse(wallet))
}

func pathUUID(r *http.Request, name string) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue(name))
	return id, err == nil && id != uuid.Nil
}

func (h *Handlers) getWallet(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(r, "walletId")
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", "walletId must be a UUID")
		return
	}
	wallet, err := h.wallets.GetWallet(r.Context(), id)
	if err != nil {
		writeAppError(w, h.log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, walletResponse(wallet))
}

// LedgerEntryResponse is the JSON view of a ledger entry.
type LedgerEntryResponse struct {
	ID            uuid.UUID   `json:"id"`
	WalletID      uuid.UUID   `json:"walletId"`
	TransactionID uuid.UUID   `json:"transactionId"`
	Direction     string      `json:"direction"`
	Money         money.Money `json:"money"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	CreatedAt     time.Time   `json:"createdAt"`
}

// LedgerPageResponse is one page of the ledger.
type LedgerPageResponse struct {
	Entries    []LedgerEntryResponse `json:"entries"`
	NextCursor string                `json:"nextCursor,omitempty"`
}

func (h *Handlers) listLedger(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(r, "walletId")
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", "walletId must be a UUID")
		return
	}
	limit := 50
	if s := r.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 || n > 200 {
			writeError(w, http.StatusBadRequest, "INVALID_INPUT", "limit must be between 1 and 200")
			return
		}
		limit = n
	}
	page, err := h.wallets.ListLedger(r.Context(), id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeAppError(w, h.log, r, err)
		return
	}
	resp := LedgerPageResponse{Entries: make([]LedgerEntryResponse, 0, len(page.Entries)), NextCursor: app.EncodeCursor(page.NextCursor)}
	for _, e := range page.Entries {
		resp.Entries = append(resp.Entries, LedgerEntryResponse{
			ID: e.ID(), WalletID: e.WalletID(), TransactionID: e.TransactionID(), Direction: string(e.Direction()),
			Money: e.Amount(), BalanceBefore: e.BalanceBefore(), BalanceAfter: e.BalanceAfter(), CreatedAt: e.CreatedAt(),
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

// ReconciliationResponse is the JSON view of a reconciliation.
type ReconciliationResponse struct {
	WalletID          uuid.UUID   `json:"walletId"`
	StoredBalance     money.Money `json:"storedBalance"`
	CalculatedBalance money.Money `json:"calculatedBalance"`
	Difference        money.Money `json:"difference"`
	Consistent        bool        `json:"consistent"`
	CheckedEntries    int64       `json:"checkedEntries"`
}

func (h *Handlers) reconcile(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(r, "walletId")
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", "walletId must be a UUID")
		return
	}
	rep, err := h.wallets.Reconcile(r.Context(), id)
	if err != nil {
		writeAppError(w, h.log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, ReconciliationResponse{
		WalletID: rep.WalletID, StoredBalance: rep.StoredBalance, CalculatedBalance: rep.CalculatedBalance,
		Difference: rep.Difference, Consistent: rep.Consistent, CheckedEntries: rep.CheckedEntries,
	})
}

// --- wagering ------------------------------------------------------------------

type rawMoney struct {
	Amount   *string `json:"amount"`
	Currency string  `json:"currency"`
}

// SubmitRequest is the body of POST /wagering/transactions.
type SubmitRequest struct {
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	PlayerID                       string    `json:"playerId"`
	WalletID                       string    `json:"walletId"`
	RoundID                        string    `json:"roundId"`
	GameID                         string    `json:"gameId"`
	Kind                           string    `json:"kind"`
	Money                          *rawMoney `json:"money"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId,omitempty"`
}

// SubmitResponse is the outcome returned to the provider.
type SubmitResponse struct {
	TransactionID    uuid.UUID    `json:"transactionId"`
	Status           string       `json:"status"`
	Balance          *money.Money `json:"balance,omitempty"`
	FailureCode      string       `json:"failureCode,omitempty"`
	IdempotentReplay bool         `json:"idempotentReplay"`
}

func statusOf(s wager.Status) int {
	switch s {
	case wager.StatusProcessed:
		return http.StatusOK
	case wager.StatusRejected:
		return http.StatusUnprocessableEntity
	default: // PENDING_REFERENCE: accepted, follow up through GET
		return http.StatusAccepted
	}
}

func (h *Handlers) submit(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", "Idempotency-Key header is required")
		return
	}
	if len(key) > 256 {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", "Idempotency-Key is too long")
		return
	}
	var req SubmitRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", err.Error())
		return
	}
	if req.Money == nil || req.Money.Amount == nil {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", "money.amount and money.currency are required (amount as a decimal string)")
		return
	}
	id := identityFrom(r.Context())
	if req.ProviderID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", "providerId is required")
		return
	}
	if req.ProviderID != id.ProviderID {
		// The authenticated identity decides which provider may be used.
		writeError(w, http.StatusForbidden, "FORBIDDEN", "providerId does not match the authenticated provider")
		return
	}
	res, err := h.wagers.Submit(r.Context(), app.SubmitCommand{
		IdempotencyKey: key, ProviderID: req.ProviderID, ExternalTransactionID: req.ExternalTransactionID,
		PlayerID: req.PlayerID, WalletID: req.WalletID, RoundID: req.RoundID, GameID: req.GameID, Kind: req.Kind,
		Amount: *req.Money.Amount, Currency: req.Money.Currency,
		ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
		Source:                         app.SourceHTTP, CorrelationID: correlationFrom(r.Context()), CausationID: correlationFrom(r.Context()),
	})
	if err != nil {
		writeAppError(w, h.log, r, err)
		return
	}
	writeJSON(w, statusOf(res.Status), SubmitResponse{
		TransactionID: res.TransactionID, Status: string(res.Status), Balance: res.Balance,
		FailureCode: string(res.FailureCode), IdempotentReplay: res.IdempotentReplay,
	})
}

// TransactionResponse is the detailed view of a transaction.
type TransactionResponse struct {
	TransactionID                  uuid.UUID    `json:"transactionId"`
	Origin                         string       `json:"origin"`
	Kind                           string       `json:"kind"`
	Status                         string       `json:"status"`
	WalletID                       uuid.UUID    `json:"walletId"`
	PlayerID                       uuid.UUID    `json:"playerId"`
	Money                          money.Money  `json:"money"`
	ProviderID                     string       `json:"providerId,omitempty"`
	ExternalTransactionID          string       `json:"externalTransactionId,omitempty"`
	RoundID                        string       `json:"roundId,omitempty"`
	GameID                         string       `json:"gameId,omitempty"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         *uuid.UUID   `json:"referenceTransactionId,omitempty"`
	FailureCode                    string       `json:"failureCode,omitempty"`
	Balance                        *money.Money `json:"balance,omitempty"`
	Attempts                       int          `json:"attempts"`
	NextAttemptAt                  *time.Time   `json:"nextAttemptAt,omitempty"`
	CreatedAt                      time.Time    `json:"createdAt"`
	UpdatedAt                      time.Time    `json:"updatedAt"`
	ProcessedAt                    *time.Time   `json:"processedAt,omitempty"`
}

func transactionResponse(t *wager.WagerTransaction) TransactionResponse {
	resp := TransactionResponse{
		TransactionID: t.ID(), Origin: string(t.Origin()), Kind: string(t.Kind()), Status: string(t.Status()),
		WalletID: t.WalletID(), PlayerID: t.PlayerID(), Money: t.Amount(), ProviderID: t.ProviderID(),
		ExternalTransactionID: t.ExternalTransactionID(), RoundID: t.RoundID(), GameID: t.GameID(),
		ReferenceExternalTransactionID: t.ReferenceExternalTransactionID(), FailureCode: string(t.FailureCode()),
		Balance: t.BalanceAfter(), Attempts: t.Attempts(), CreatedAt: t.CreatedAt(), UpdatedAt: t.UpdatedAt(),
	}
	if id := t.ReferenceTransactionID(); id != uuid.Nil {
		resp.ReferenceTransactionID = &id
	}
	if ts := t.NextAttemptAt(); !ts.IsZero() {
		resp.NextAttemptAt = &ts
	}
	if ts := t.ProcessedAt(); !ts.IsZero() {
		resp.ProcessedAt = &ts
	}
	return resp
}

func (h *Handlers) getTransaction(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(r, "transactionId")
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", "transactionId must be a UUID")
		return
	}
	t, err := h.wagers.GetTransaction(r.Context(), identityFrom(r.Context()).Actor(), id)
	if err != nil {
		writeAppError(w, h.log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, transactionResponse(t))
}

func (h *Handlers) getProviderTransaction(w http.ResponseWriter, r *http.Request) {
	providerID := r.PathValue("providerId")
	externalID := r.PathValue("externalTransactionId")
	if providerID == "" || externalID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", "providerId and externalTransactionId are required")
		return
	}
	t, err := h.wagers.GetTransactionByExternalID(r.Context(), identityFrom(r.Context()).Actor(), providerID, externalID)
	if err != nil {
		if errors.Is(err, app.ErrForbidden) {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "providers may only read their own transactions")
			return
		}
		writeAppError(w, h.log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, transactionResponse(t))
}
