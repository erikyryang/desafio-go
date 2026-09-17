package wager

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/erikyryan/desafio-go/internal/domain/money"
)

// Kind of a wager transaction.
type Kind string

const (
	KindOpening  Kind = "OPENING" // internal only: initial wallet credit
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

// ParseExternalKind accepts the five external kinds; OPENING is rejected.
func ParseExternalKind(s string) (Kind, error) {
	switch Kind(s) {
	case KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return Kind(s), nil
	case KindOpening:
		return "", fmt.Errorf("%w: kind OPENING is reserved for internal use", ErrInvalidArgument)
	}
	return "", fmt.Errorf("%w: unknown kind %q", ErrInvalidArgument, s)
}

// ParseKind accepts any persisted kind.
func ParseKind(s string) (Kind, error) {
	if Kind(s) == KindOpening {
		return KindOpening, nil
	}
	return ParseExternalKind(s)
}

// IsReversal reports whether the kind references another transaction.
func (k Kind) IsReversal() bool { return k == KindRefund || k == KindRollback }

// Status of a wager transaction.
type Status string

const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

// ParseStatus validates a persisted status.
func ParseStatus(s string) (Status, error) {
	switch Status(s) {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return Status(s), nil
	}
	return "", fmt.Errorf("%w: unknown status %q", ErrInvalidArgument, s)
}

// IsTerminal reports whether no further transition is allowed.
func (s Status) IsTerminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

// Origin distinguishes internal (OPENING) from provider-originated operations.
type Origin string

const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"
)

// ExternalRequest carries the provider-facing identifiers of an operation.
type ExternalRequest struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	PlayerID                       uuid.UUID
	WalletID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Amount                         money.Money
	ReferenceExternalTransactionID string
}

// WagerTransaction records one financial operation and its outcome.
type WagerTransaction struct {
	id     uuid.UUID
	origin Origin
	kind   Kind
	status Status

	walletID uuid.UUID
	playerID uuid.UUID
	amount   money.Money

	providerID                     string
	externalTransactionID          string
	idempotencyKey                 string
	payloadHash                    string
	roundID                        string
	gameID                         string
	referenceExternalTransactionID string
	referenceTransactionID         uuid.UUID // resolved internal reference

	failureCode  FailureCode
	balanceAfter *money.Money // balance observed when the outcome was recorded

	attempts      int
	nextAttemptAt time.Time
	createdAt     time.Time
	updatedAt     time.Time
	processedAt   time.Time
}

// NewExternalTransaction validates an operation received from a provider and
// creates it in PENDING. The amount policy per kind is enforced here:
// BET/WIN/REFUND/ROLLBACK require > 0, LOSS requires exactly 0.
func NewExternalTransaction(id uuid.UUID, req ExternalRequest, now time.Time) (*WagerTransaction, error) {
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: id is required", ErrInvalidArgument)
	}
	if req.ProviderID == "" || req.ExternalTransactionID == "" || req.IdempotencyKey == "" || req.PayloadHash == "" {
		return nil, fmt.Errorf("%w: providerId, externalTransactionId, idempotencyKey and payloadHash are required", ErrInvalidArgument)
	}
	if req.PlayerID == uuid.Nil || req.WalletID == uuid.Nil {
		return nil, fmt.Errorf("%w: playerId and walletId are required", ErrInvalidArgument)
	}
	if req.RoundID == "" || req.GameID == "" {
		return nil, fmt.Errorf("%w: roundId and gameId are required", ErrInvalidArgument)
	}
	if _, err := ParseExternalKind(string(req.Kind)); err != nil {
		return nil, err
	}
	if !req.Amount.IsValid() {
		return nil, fmt.Errorf("%w: money is required", ErrInvalidArgument)
	}
	if req.Amount.IsNegative() {
		return nil, fmt.Errorf("%w: money must not be negative", ErrInvalidArgument)
	}
	switch req.Kind {
	case KindLoss:
		if !req.Amount.IsZero() {
			return nil, fmt.Errorf("%w: LOSS requires money.amount \"0.00\"", ErrInvalidArgument)
		}
	default:
		if !req.Amount.IsPositive() {
			return nil, fmt.Errorf("%w: %s requires a positive amount", ErrInvalidArgument, req.Kind)
		}
	}
	if req.Kind.IsReversal() && req.ReferenceExternalTransactionID == "" {
		return nil, fmt.Errorf("%w: %s requires referenceExternalTransactionId", ErrInvalidArgument, req.Kind)
	}
	if !req.Kind.IsReversal() && req.ReferenceExternalTransactionID != "" {
		return nil, fmt.Errorf("%w: %s must not carry referenceExternalTransactionId", ErrInvalidArgument, req.Kind)
	}
	if now.IsZero() {
		return nil, fmt.Errorf("%w: timestamp is required", ErrInvalidArgument)
	}
	now = now.UTC()
	return &WagerTransaction{
		id: id, origin: OriginExternal, kind: req.Kind, status: StatusPending,
		walletID: req.WalletID, playerID: req.PlayerID, amount: req.Amount,
		providerID: req.ProviderID, externalTransactionID: req.ExternalTransactionID,
		idempotencyKey: req.IdempotencyKey, payloadHash: req.PayloadHash,
		roundID: req.RoundID, gameID: req.GameID,
		referenceExternalTransactionID: req.ReferenceExternalTransactionID,
		createdAt:                      now, updatedAt: now,
	}, nil
}

// NewOpeningTransaction creates the internal OPENING credit of a wallet.
func NewOpeningTransaction(id uuid.UUID, walletID, playerID uuid.UUID, amount money.Money, now time.Time) (*WagerTransaction, error) {
	if id == uuid.Nil || walletID == uuid.Nil || playerID == uuid.Nil {
		return nil, fmt.Errorf("%w: ids are required", ErrInvalidArgument)
	}
	if !amount.IsValid() || !amount.IsPositive() {
		return nil, fmt.Errorf("%w: OPENING requires a positive amount", ErrInvalidArgument)
	}
	if now.IsZero() {
		return nil, fmt.Errorf("%w: timestamp is required", ErrInvalidArgument)
	}
	now = now.UTC()
	return &WagerTransaction{
		id: id, origin: OriginInternal, kind: KindOpening, status: StatusPending,
		walletID: walletID, playerID: playerID, amount: amount,
		createdAt: now, updatedAt: now,
	}, nil
}

// Snapshot is the persisted state used by RehydrateTransaction.
type Snapshot struct {
	ID                             uuid.UUID
	Origin                         Origin
	Kind                           Kind
	Status                         Status
	WalletID                       uuid.UUID
	PlayerID                       uuid.UUID
	Amount                         money.Money
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	RoundID                        string
	GameID                         string
	ReferenceExternalTransactionID string
	ReferenceTransactionID         uuid.UUID
	FailureCode                    FailureCode
	BalanceAfter                   *money.Money
	Attempts                       int
	NextAttemptAt                  time.Time
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
	ProcessedAt                    time.Time
}

// RehydrateTransaction rebuilds a transaction from storage without
// re-running validations that depend on the original request context.
func RehydrateTransaction(s Snapshot) (*WagerTransaction, error) {
	if s.ID == uuid.Nil || s.WalletID == uuid.Nil || s.PlayerID == uuid.Nil {
		return nil, fmt.Errorf("%w: ids are required", ErrInvalidArgument)
	}
	if _, err := ParseKind(string(s.Kind)); err != nil {
		return nil, err
	}
	if _, err := ParseStatus(string(s.Status)); err != nil {
		return nil, err
	}
	if !s.Amount.IsValid() {
		return nil, fmt.Errorf("%w: invalid amount", ErrInvalidArgument)
	}
	switch s.Origin {
	case OriginInternal, OriginExternal:
	default:
		return nil, fmt.Errorf("%w: origin %q", ErrInvalidArgument, s.Origin)
	}
	return &WagerTransaction{
		id: s.ID, origin: s.Origin, kind: s.Kind, status: s.Status,
		walletID: s.WalletID, playerID: s.PlayerID, amount: s.Amount,
		providerID: s.ProviderID, externalTransactionID: s.ExternalTransactionID,
		idempotencyKey: s.IdempotencyKey, payloadHash: s.PayloadHash,
		roundID: s.RoundID, gameID: s.GameID,
		referenceExternalTransactionID: s.ReferenceExternalTransactionID,
		referenceTransactionID:         s.ReferenceTransactionID,
		failureCode:                    s.FailureCode, balanceAfter: s.BalanceAfter,
		attempts: s.Attempts, nextAttemptAt: s.NextAttemptAt,
		createdAt: s.CreatedAt, updatedAt: s.UpdatedAt, processedAt: s.ProcessedAt,
	}, nil
}

// Snapshot exports the state for persistence.
func (t *WagerTransaction) Snapshot() Snapshot {
	return Snapshot{
		ID: t.id, Origin: t.origin, Kind: t.kind, Status: t.status,
		WalletID: t.walletID, PlayerID: t.playerID, Amount: t.amount,
		ProviderID: t.providerID, ExternalTransactionID: t.externalTransactionID,
		IdempotencyKey: t.idempotencyKey, PayloadHash: t.payloadHash,
		RoundID: t.roundID, GameID: t.gameID,
		ReferenceExternalTransactionID: t.referenceExternalTransactionID,
		ReferenceTransactionID:         t.referenceTransactionID,
		FailureCode:                    t.failureCode, BalanceAfter: t.balanceAfter,
		Attempts: t.attempts, NextAttemptAt: t.nextAttemptAt,
		CreatedAt: t.createdAt, UpdatedAt: t.updatedAt, ProcessedAt: t.processedAt,
	}
}

func (t *WagerTransaction) ID() uuid.UUID                 { return t.id }
func (t *WagerTransaction) Origin() Origin                { return t.origin }
func (t *WagerTransaction) Kind() Kind                    { return t.kind }
func (t *WagerTransaction) Status() Status                { return t.status }
func (t *WagerTransaction) WalletID() uuid.UUID           { return t.walletID }
func (t *WagerTransaction) PlayerID() uuid.UUID           { return t.playerID }
func (t *WagerTransaction) Amount() money.Money           { return t.amount }
func (t *WagerTransaction) ProviderID() string            { return t.providerID }
func (t *WagerTransaction) ExternalTransactionID() string { return t.externalTransactionID }
func (t *WagerTransaction) IdempotencyKey() string        { return t.idempotencyKey }
func (t *WagerTransaction) PayloadHash() string           { return t.payloadHash }
func (t *WagerTransaction) RoundID() string               { return t.roundID }
func (t *WagerTransaction) GameID() string                { return t.gameID }
func (t *WagerTransaction) ReferenceExternalTransactionID() string {
	return t.referenceExternalTransactionID
}
func (t *WagerTransaction) ReferenceTransactionID() uuid.UUID { return t.referenceTransactionID }
func (t *WagerTransaction) FailureCode() FailureCode          { return t.failureCode }
func (t *WagerTransaction) BalanceAfter() *money.Money        { return t.balanceAfter }
func (t *WagerTransaction) Attempts() int                     { return t.attempts }
func (t *WagerTransaction) NextAttemptAt() time.Time          { return t.nextAttemptAt }
func (t *WagerTransaction) CreatedAt() time.Time              { return t.createdAt }
func (t *WagerTransaction) UpdatedAt() time.Time              { return t.updatedAt }
func (t *WagerTransaction) ProcessedAt() time.Time            { return t.processedAt }

// IsExternal reports whether the operation came from a provider.
func (t *WagerTransaction) IsExternal() bool { return t.origin == OriginExternal }

// Matches reports whether a replay carries the same idempotency key and
// canonical payload hash as the stored transaction.
func (t *WagerTransaction) Matches(idempotencyKey, payloadHash string) (sameKey, sameHash bool) {
	return t.idempotencyKey == idempotencyKey, t.payloadHash == payloadHash
}

func (t *WagerTransaction) transition(to Status, now time.Time) error {
	if t.status.IsTerminal() {
		return fmt.Errorf("%w: %s is terminal (wanted %s)", ErrInvalidTransition, t.status, to)
	}
	if now.IsZero() {
		return fmt.Errorf("%w: timestamp is required", ErrInvalidArgument)
	}
	allowed := map[Status][]Status{
		StatusPending:          {StatusProcessed, StatusRejected, StatusFailed, StatusPendingReference},
		StatusPendingReference: {StatusProcessed, StatusRejected, StatusFailed, StatusPendingReference},
	}
	for _, s := range allowed[t.status] {
		if s == to {
			t.status = to
			t.updatedAt = now.UTC()
			return nil
		}
	}
	return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.status, to)
}

// MarkProcessed records success. balanceAfter is the wallet balance observed
// at processing time (for LOSS it equals the unchanged balance); reference is
// the resolved internal reference for reversals (uuid.Nil otherwise).
func (t *WagerTransaction) MarkProcessed(balanceAfter money.Money, reference uuid.UUID, now time.Time) error {
	if !balanceAfter.IsValid() {
		return fmt.Errorf("%w: balanceAfter is required", ErrInvalidArgument)
	}
	if t.kind.IsReversal() && reference == uuid.Nil {
		return fmt.Errorf("%w: reversal requires a resolved reference", ErrInvalidArgument)
	}
	if err := t.transition(StatusProcessed, now); err != nil {
		return err
	}
	b := balanceAfter
	t.balanceAfter = &b
	t.referenceTransactionID = reference
	t.processedAt = now.UTC()
	return nil
}

// MarkRejected records a definitive business rejection. balanceAfter may be
// nil when the wallet could not be resolved.
func (t *WagerTransaction) MarkRejected(code FailureCode, balanceAfter *money.Money, now time.Time) error {
	if code == "" {
		return fmt.Errorf("%w: failure code is required", ErrInvalidArgument)
	}
	if err := t.transition(StatusRejected, now); err != nil {
		return err
	}
	t.failureCode = code
	if balanceAfter != nil {
		b := *balanceAfter
		t.balanceAfter = &b
	}
	t.processedAt = now.UTC()
	return nil
}

// MarkFailed records a permanent infrastructure failure for audit.
func (t *WagerTransaction) MarkFailed(code FailureCode, now time.Time) error {
	if code == "" {
		return fmt.Errorf("%w: failure code is required", ErrInvalidArgument)
	}
	if err := t.transition(StatusFailed, now); err != nil {
		return err
	}
	t.failureCode = code
	t.processedAt = now.UTC()
	return nil
}

// MarkPendingReference parks a reversal whose reference is not available yet.
// It is also used by the retry worker to reschedule (attempts increments).
func (t *WagerTransaction) MarkPendingReference(nextAttemptAt, now time.Time) error {
	if !t.kind.IsReversal() {
		return fmt.Errorf("%w: only reversals wait for references", ErrInvalidTransition)
	}
	if nextAttemptAt.IsZero() {
		return fmt.Errorf("%w: nextAttemptAt is required", ErrInvalidArgument)
	}
	if err := t.transition(StatusPendingReference, now); err != nil {
		return err
	}
	t.attempts++
	t.nextAttemptAt = nextAttemptAt.UTC()
	return nil
}
