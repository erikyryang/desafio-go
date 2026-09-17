// Package app contains the use cases shared by the HTTP API and the SQS
// consumer, and the ports they need from infrastructure.
package app

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/erikyryan/desafio-go/internal/domain/money"
	"github.com/erikyryan/desafio-go/internal/domain/wager"
)

// Infrastructure error classes. Adapters translate driver errors into these
// so that use cases stay independent of pgx/aws.
var (
	ErrNotFound               = errors.New("app: not found")
	ErrUniqueViolation        = errors.New("app: unique violation")
	ErrConcurrentModification = errors.New("app: concurrent modification")
	// ErrUnavailable marks transient infrastructure failures (connection lost,
	// timeout, serialization failure). Callers should retry; HTTP maps it to 503.
	ErrUnavailable = errors.New("app: dependency unavailable")
)

// Application errors returned to transports.
var (
	ErrValidation            = errors.New("app: invalid input")
	ErrWalletNotFound        = errors.New("app: wallet not found")
	ErrTransactionNotFound   = errors.New("app: transaction not found")
	ErrWalletAlreadyExists   = errors.New("app: wallet already exists for player and currency")
	ErrIdempotencyConflict   = errors.New("app: idempotency key reused with a different payload")
	ErrExternalIDConflict    = errors.New("app: externalTransactionId already used with another idempotency key")
	ErrForbidden             = errors.New("app: forbidden")
	ErrTransactionNotPending = errors.New("app: transaction is not pending")
)

// Store groups the repositories bound to one SQL transaction.
type Store interface {
	Wallets() WalletRepository
	Transactions() TransactionRepository
	Ledger() LedgerRepository
	Outbox() OutboxRepository
	Inbox() InboxRepository
}

// UnitOfWork runs fn inside a single database transaction. The transaction is
// committed when fn returns nil and rolled back otherwise.
type UnitOfWork interface {
	Do(ctx context.Context, fn func(ctx context.Context, s Store) error) error
	// DoReadOnlySnapshot runs fn in a REPEATABLE READ, read-only transaction so
	// that every query observes the same snapshot (used by reconciliation).
	DoReadOnlySnapshot(ctx context.Context, fn func(ctx context.Context, s Store) error) error
}

// WalletRepository persists the wallet aggregate.
type WalletRepository interface {
	Create(ctx context.Context, w *wager.Wallet) error
	Get(ctx context.Context, id uuid.UUID) (*wager.Wallet, error)
	// GetForUpdate acquires the per-wallet row lock for the transaction.
	GetForUpdate(ctx context.Context, id uuid.UUID) (*wager.Wallet, error)
	// Update persists balance/version/updatedAt with an optimistic check on
	// expectedVersion; ErrConcurrentModification when no row matched.
	Update(ctx context.Context, w *wager.Wallet, expectedVersion int64) error
}

// TransactionRepository persists wager transactions.
type TransactionRepository interface {
	Insert(ctx context.Context, t *wager.WagerTransaction) error
	Update(ctx context.Context, t *wager.WagerTransaction) error
	GetByID(ctx context.Context, id uuid.UUID) (*wager.WagerTransaction, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*wager.WagerTransaction, error)
	GetByExternalID(ctx context.Context, providerID, externalTransactionID string) (*wager.WagerTransaction, error)
	// HasProcessedReversal reports whether a PROCESSED REFUND/ROLLBACK already
	// references the transaction.
	HasProcessedReversal(ctx context.Context, referenceID uuid.UUID) (bool, error)
	// ListDuePendingReferences returns ids of PENDING_REFERENCE transactions
	// whose nextAttemptAt <= now.
	ListDuePendingReferences(ctx context.Context, now time.Time, limit int) ([]uuid.UUID, error)
	// GetForUpdateSkipLocked locks the row for the current transaction; it
	// returns ErrNotFound when the row is missing or locked elsewhere.
	GetForUpdateSkipLocked(ctx context.Context, id uuid.UUID) (*wager.WagerTransaction, error)
}

// LedgerPage is one page of ledger entries.
type LedgerPage struct {
	Entries    []*wager.LedgerEntry
	NextCursor int64 // sequence of the last entry; 0 when there is no next page
}

// LedgerTotals is the aggregation used by reconciliation.
type LedgerTotals struct {
	Credits money.Money
	Debits  money.Money
	Count   int64
}

// LedgerRepository persists append-only ledger entries.
type LedgerRepository interface {
	Insert(ctx context.Context, e *wager.LedgerEntry) error
	// List returns entries with sequence > afterSequence ordered by sequence.
	List(ctx context.Context, walletID uuid.UUID, afterSequence int64, limit int) (LedgerPage, error)
	Totals(ctx context.Context, walletID uuid.UUID, currency money.Currency) (LedgerTotals, error)
}

// OutboxRecord is a claimed outbox row ready to publish.
type OutboxRecord struct {
	EventID       uuid.UUID
	EventType     string
	AggregateType string
	AggregateID   uuid.UUID
	CorrelationID string
	Payload       []byte // immutable snapshot of the full envelope
	OccurredAt    time.Time
	Attempts      int
}

// OutboxRepository persists integration events in the same transaction as
// the domain changes and hands them to publishers.
type OutboxRepository interface {
	Insert(ctx context.Context, events ...wager.Event) error
	// Claim leases up to limit unpublished, due, unleased rows for owner.
	Claim(ctx context.Context, owner string, lease time.Duration, limit int, now time.Time) ([]OutboxRecord, error)
	MarkPublished(ctx context.Context, eventID uuid.UUID, now time.Time) error
	// Reschedule releases the lease and sets the next attempt after a failure.
	Reschedule(ctx context.Context, eventID uuid.UUID, nextAttemptAt time.Time, lastError string) error
	// OldestPendingAge returns how long the oldest unpublished event has waited.
	OldestPendingAge(ctx context.Context, now time.Time) (time.Duration, error)
}

// InboxRepository deduplicates consumer messages.
type InboxRepository interface {
	// Record inserts (consumer, messageId, hash). It returns inserted=false and
	// the stored hash when the message was already completed.
	Record(ctx context.Context, consumer, messageID, payloadHash string, now time.Time) (inserted bool, storedHash string, err error)
}

// Metrics is the subset of instrumentation the use cases emit.
type Metrics interface {
	TransactionOutcome(source string, kind wager.Kind, status wager.Status, failureCode wager.FailureCode)
	IdempotentReplay(source string)
	IdempotencyConflict(source string)
	ConcurrencyConflict()
	ProcessingDuration(source string, d time.Duration)
	ReconciliationDivergence()
	PendingReferenceRetry()
}

// NopMetrics discards everything.
type NopMetrics struct{}

func (NopMetrics) TransactionOutcome(string, wager.Kind, wager.Status, wager.FailureCode) {}
func (NopMetrics) IdempotentReplay(string)                                                {}
func (NopMetrics) IdempotencyConflict(string)                                             {}
func (NopMetrics) ConcurrencyConflict()                                                   {}
func (NopMetrics) ProcessingDuration(string, time.Duration)                               {}
func (NopMetrics) ReconciliationDivergence()                                              {}
func (NopMetrics) PendingReferenceRetry()                                                 {}

// Clock and IDs are injected so tests can control them.
type Clock func() time.Time

// IDGenerator returns time-ordered unique ids (UUID v7).
type IDGenerator func() uuid.UUID

// Actor is the authenticated identity performing a query.
type Actor struct {
	// Internal grants access to every wallet and transaction.
	Internal bool
	// ProviderID limits access to that provider's transactions.
	ProviderID string
}

// CanSeeTransaction implements provider isolation.
func (a Actor) CanSeeTransaction(t *wager.WagerTransaction) bool {
	if a.Internal {
		return true
	}
	return a.ProviderID != "" && t.IsExternal() && t.ProviderID() == a.ProviderID
}
