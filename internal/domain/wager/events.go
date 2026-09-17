package wager

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/erikyryan/desafio-go/internal/domain/money"
)

// Event types published through the transactional outbox.
const (
	EventWagerTransactionProcessed        = "WagerTransactionProcessed"
	EventWagerTransactionRejected         = "WagerTransactionRejected"
	EventWagerTransactionPendingReference = "WagerTransactionPendingReference"
	EventWalletBalanceChanged             = "WalletBalanceChanged"
)

// Aggregate types used in envelopes.
const (
	AggregateWagerTransaction = "WagerTransaction"
	AggregateWallet           = "Wallet"
)

// Event is the integration event envelope. Type and Version are fixed by the
// constructor of each concrete payload.
type Event struct {
	EventID       uuid.UUID
	EventType     string
	AggregateType string
	AggregateID   uuid.UUID
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
	Version       int
	Data          any
}

// EventContext carries tracing identifiers into event constructors.
type EventContext struct {
	CorrelationID string
	CausationID   string
}

// TransactionEventData is the payload shared by the transaction events.
// Fields that do not apply to internal operations are omitted from JSON.
type TransactionEventData struct {
	TransactionID                  uuid.UUID    `json:"transactionId"`
	Origin                         Origin       `json:"origin"`
	Kind                           Kind         `json:"kind"`
	Status                         Status       `json:"status"`
	WalletID                       uuid.UUID    `json:"walletId"`
	PlayerID                       uuid.UUID    `json:"playerId"`
	Money                          money.Money  `json:"money"`
	ProviderID                     string       `json:"providerId,omitempty"`
	ExternalTransactionID          string       `json:"externalTransactionId,omitempty"`
	RoundID                        string       `json:"roundId,omitempty"`
	GameID                         string       `json:"gameId,omitempty"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         *uuid.UUID   `json:"referenceTransactionId,omitempty"`
	Balance                        *money.Money `json:"balance,omitempty"`
	FailureCode                    FailureCode  `json:"failureCode,omitempty"`
	ProcessedAt                    *time.Time   `json:"processedAt,omitempty"`
	NextAttemptAt                  *time.Time   `json:"nextAttemptAt,omitempty"`
}

// BalanceChangedData is the payload of WalletBalanceChanged.
type BalanceChangedData struct {
	WalletID      uuid.UUID   `json:"walletId"`
	PlayerID      uuid.UUID   `json:"playerId"`
	TransactionID uuid.UUID   `json:"transactionId"`
	Direction     Direction   `json:"direction"`
	Money         money.Money `json:"money"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	WalletVersion int64       `json:"walletVersion"`
	OccurredAt    time.Time   `json:"occurredAt"`
}

func transactionData(t *WagerTransaction) TransactionEventData {
	d := TransactionEventData{
		TransactionID: t.id, Origin: t.origin, Kind: t.kind, Status: t.status,
		WalletID: t.walletID, PlayerID: t.playerID, Money: t.amount,
		ProviderID: t.providerID, ExternalTransactionID: t.externalTransactionID,
		RoundID: t.roundID, GameID: t.gameID,
		ReferenceExternalTransactionID: t.referenceExternalTransactionID,
		Balance:                        t.balanceAfter,
		FailureCode:                    t.failureCode,
	}
	if t.referenceTransactionID != uuid.Nil {
		r := t.referenceTransactionID
		d.ReferenceTransactionID = &r
	}
	if !t.processedAt.IsZero() {
		p := t.processedAt
		d.ProcessedAt = &p
	}
	if !t.nextAttemptAt.IsZero() {
		n := t.nextAttemptAt
		d.NextAttemptAt = &n
	}
	return d
}

func newEvent(id uuid.UUID, eventType, aggType string, aggID uuid.UUID, ctx EventContext, now time.Time, data any) (Event, error) {
	if id == uuid.Nil {
		return Event{}, fmt.Errorf("%w: event id is required", ErrInvalidArgument)
	}
	if now.IsZero() {
		return Event{}, fmt.Errorf("%w: timestamp is required", ErrInvalidArgument)
	}
	return Event{
		EventID: id, EventType: eventType, AggregateType: aggType, AggregateID: aggID,
		CorrelationID: ctx.CorrelationID, CausationID: ctx.CausationID,
		OccurredAt: now.UTC(), Version: 1, Data: data,
	}, nil
}

// NewWagerTransactionProcessed builds the event for a PROCESSED transaction.
func NewWagerTransactionProcessed(id uuid.UUID, t *WagerTransaction, ctx EventContext, now time.Time) (Event, error) {
	if t.status != StatusProcessed {
		return Event{}, fmt.Errorf("%w: transaction is %s", ErrInvalidArgument, t.status)
	}
	return newEvent(id, EventWagerTransactionProcessed, AggregateWagerTransaction, t.id, ctx, now, transactionData(t))
}

// NewWagerTransactionRejected builds the event for a REJECTED transaction.
func NewWagerTransactionRejected(id uuid.UUID, t *WagerTransaction, ctx EventContext, now time.Time) (Event, error) {
	if t.status != StatusRejected {
		return Event{}, fmt.Errorf("%w: transaction is %s", ErrInvalidArgument, t.status)
	}
	return newEvent(id, EventWagerTransactionRejected, AggregateWagerTransaction, t.id, ctx, now, transactionData(t))
}

// NewWagerTransactionPendingReference builds the event for a parked reversal.
func NewWagerTransactionPendingReference(id uuid.UUID, t *WagerTransaction, ctx EventContext, now time.Time) (Event, error) {
	if t.status != StatusPendingReference {
		return Event{}, fmt.Errorf("%w: transaction is %s", ErrInvalidArgument, t.status)
	}
	return newEvent(id, EventWagerTransactionPendingReference, AggregateWagerTransaction, t.id, ctx, now, transactionData(t))
}

// NewWalletBalanceChanged builds the event for an effective balance change.
func NewWalletBalanceChanged(id uuid.UUID, w *Wallet, e *LedgerEntry, ctx EventContext, now time.Time) (Event, error) {
	if w.id != e.walletID {
		return Event{}, fmt.Errorf("%w: entry belongs to another wallet", ErrInvalidArgument)
	}
	return newEvent(id, EventWalletBalanceChanged, AggregateWallet, w.id, ctx, now, BalanceChangedData{
		WalletID: w.id, PlayerID: w.playerID, TransactionID: e.transactionID, Direction: e.direction,
		Money: e.amount, BalanceBefore: e.balanceBefore, BalanceAfter: e.balanceAfter,
		WalletVersion: w.version, OccurredAt: e.createdAt,
	})
}
