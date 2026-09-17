package wager

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/erikyryan/desafio-go/internal/domain/money"
)

// Direction of a ledger movement.
type Direction string

const (
	DirectionDebit  Direction = "DEBIT"
	DirectionCredit Direction = "CREDIT"
)

// ParseDirection validates a persisted direction.
func ParseDirection(s string) (Direction, error) {
	switch Direction(s) {
	case DirectionDebit, DirectionCredit:
		return Direction(s), nil
	}
	return "", fmt.Errorf("%w: direction %q", ErrInvalidArgument, s)
}

// LedgerEntry is an immutable, append-only record of a balance change.
type LedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	createdAt     time.Time
	sequence      int64 // assigned by the database; 0 before persistence
}

// NewLedgerEntry validates balanceAfter = balanceBefore ± amount.
func NewLedgerEntry(id, walletID, transactionID uuid.UUID, direction Direction, amount, before, after money.Money, now time.Time) (*LedgerEntry, error) {
	if id == uuid.Nil || walletID == uuid.Nil || transactionID == uuid.Nil {
		return nil, fmt.Errorf("%w: ids are required", ErrInvalidLedgerEntry)
	}
	if !amount.IsValid() || !before.IsValid() || !after.IsValid() {
		return nil, fmt.Errorf("%w: uninitialized money", ErrInvalidLedgerEntry)
	}
	if !amount.SameCurrency(before) || !amount.SameCurrency(after) {
		return nil, fmt.Errorf("%w: currency mismatch", ErrInvalidLedgerEntry)
	}
	if !amount.IsPositive() {
		return nil, fmt.Errorf("%w: amount must be positive", ErrInvalidLedgerEntry)
	}
	if before.IsNegative() || after.IsNegative() {
		return nil, fmt.Errorf("%w: balances must be non-negative", ErrInvalidLedgerEntry)
	}
	var expected money.Money
	var err error
	switch direction {
	case DirectionCredit:
		expected, err = before.Add(amount)
	case DirectionDebit:
		expected, err = before.Sub(amount)
	default:
		return nil, fmt.Errorf("%w: direction %q", ErrInvalidLedgerEntry, direction)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidLedgerEntry, err)
	}
	if !expected.Equal(after) {
		return nil, fmt.Errorf("%w: balanceAfter %s != %s %s %s", ErrInvalidLedgerEntry, after, before, direction, amount)
	}
	if now.IsZero() {
		return nil, fmt.Errorf("%w: timestamp is required", ErrInvalidLedgerEntry)
	}
	return &LedgerEntry{id: id, walletID: walletID, transactionID: transactionID, direction: direction, amount: amount, balanceBefore: before, balanceAfter: after, createdAt: now.UTC()}, nil
}

// RehydrateLedgerEntry rebuilds an entry from storage, re-validating the
// arithmetic invariant.
func RehydrateLedgerEntry(id, walletID, transactionID uuid.UUID, direction Direction, amount, before, after money.Money, createdAt time.Time, sequence int64) (*LedgerEntry, error) {
	e, err := NewLedgerEntry(id, walletID, transactionID, direction, amount, before, after, createdAt)
	if err != nil {
		return nil, err
	}
	e.sequence = sequence
	return e, nil
}

func (e *LedgerEntry) ID() uuid.UUID              { return e.id }
func (e *LedgerEntry) WalletID() uuid.UUID        { return e.walletID }
func (e *LedgerEntry) TransactionID() uuid.UUID   { return e.transactionID }
func (e *LedgerEntry) Direction() Direction       { return e.direction }
func (e *LedgerEntry) Amount() money.Money        { return e.amount }
func (e *LedgerEntry) BalanceBefore() money.Money { return e.balanceBefore }
func (e *LedgerEntry) BalanceAfter() money.Money  { return e.balanceAfter }
func (e *LedgerEntry) CreatedAt() time.Time       { return e.createdAt }
func (e *LedgerEntry) Sequence() int64            { return e.sequence }
