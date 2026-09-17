package wager

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/erikyryan/desafio-go/internal/domain/money"
)

// Wallet is the aggregate root of the financial model. Balance changes only
// through Credit/Debit, which also produce the matching ledger entry; the
// application layer persists both in the same SQL transaction.
type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	currency  money.Currency
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// NewWallet creates a wallet with zero balance and version 1.
func NewWallet(id, playerID uuid.UUID, currency money.Currency, now time.Time) (*Wallet, error) {
	if id == uuid.Nil || playerID == uuid.Nil {
		return nil, fmt.Errorf("%w: wallet and player ids are required", ErrInvalidArgument)
	}
	zero, err := money.Zero(currency)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	if now.IsZero() {
		return nil, fmt.Errorf("%w: timestamp is required", ErrInvalidArgument)
	}
	now = now.UTC()
	return &Wallet{id: id, playerID: playerID, currency: currency, balance: zero, version: 1, createdAt: now, updatedAt: now}, nil
}

// RehydrateWallet rebuilds a wallet from persisted state without applying
// any movement or emitting events.
func RehydrateWallet(id, playerID uuid.UUID, balance money.Money, version int64, createdAt, updatedAt time.Time) (*Wallet, error) {
	if id == uuid.Nil || playerID == uuid.Nil {
		return nil, fmt.Errorf("%w: wallet and player ids are required", ErrInvalidArgument)
	}
	if !balance.IsValid() {
		return nil, fmt.Errorf("%w: invalid balance", ErrInvalidArgument)
	}
	if balance.IsNegative() {
		return nil, fmt.Errorf("%w: negative persisted balance", ErrInvalidArgument)
	}
	if version < 1 {
		return nil, fmt.Errorf("%w: version must be >= 1", ErrInvalidArgument)
	}
	return &Wallet{id: id, playerID: playerID, currency: balance.Currency(), balance: balance, version: version, createdAt: createdAt.UTC(), updatedAt: updatedAt.UTC()}, nil
}

func (w *Wallet) ID() uuid.UUID            { return w.id }
func (w *Wallet) PlayerID() uuid.UUID      { return w.playerID }
func (w *Wallet) Currency() money.Currency { return w.currency }
func (w *Wallet) Balance() money.Money     { return w.balance }
func (w *Wallet) Version() int64           { return w.version }
func (w *Wallet) CreatedAt() time.Time     { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time     { return w.updatedAt }

// BelongsTo reports whether the wallet is owned by the player.
func (w *Wallet) BelongsTo(playerID uuid.UUID) bool { return w.playerID == playerID }

func (w *Wallet) checkMovement(amount money.Money) error {
	if !amount.IsValid() {
		return fmt.Errorf("%w: invalid amount", ErrInvalidArgument)
	}
	if amount.Currency() != w.currency {
		return fmt.Errorf("%w: wallet %s, movement %s", ErrCurrencyMismatch, w.currency, amount.Currency())
	}
	if !amount.IsPositive() {
		return fmt.Errorf("%w: movement must be positive", ErrInvalidArgument)
	}
	return nil
}

// Credit adds amount to the balance and returns the ledger entry describing
// the movement. The wallet version is incremented.
func (w *Wallet) Credit(entryID, transactionID uuid.UUID, amount money.Money, now time.Time) (*LedgerEntry, error) {
	if err := w.checkMovement(amount); err != nil {
		return nil, err
	}
	before := w.balance
	after, err := before.Add(amount)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	entry, err := NewLedgerEntry(entryID, w.id, transactionID, DirectionCredit, amount, before, after, now)
	if err != nil {
		return nil, err
	}
	w.apply(after, now)
	return entry, nil
}

// Debit subtracts amount from the balance. It fails with
// ErrInsufficientBalance when the result would be negative.
func (w *Wallet) Debit(entryID, transactionID uuid.UUID, amount money.Money, now time.Time) (*LedgerEntry, error) {
	if err := w.checkMovement(amount); err != nil {
		return nil, err
	}
	before := w.balance
	after, err := before.Sub(amount)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	if after.IsNegative() {
		return nil, fmt.Errorf("%w: balance %s, debit %s", ErrInsufficientBalance, before, amount)
	}
	entry, err := NewLedgerEntry(entryID, w.id, transactionID, DirectionDebit, amount, before, after, now)
	if err != nil {
		return nil, err
	}
	w.apply(after, now)
	return entry, nil
}

// CanDebit reports whether a debit of amount keeps the balance >= 0.
func (w *Wallet) CanDebit(amount money.Money) bool {
	if err := w.checkMovement(amount); err != nil {
		return false
	}
	c, err := w.balance.Cmp(amount)
	return err == nil && c >= 0
}

func (w *Wallet) apply(after money.Money, now time.Time) {
	w.balance = after
	w.version++
	w.updatedAt = now.UTC()
}

// Open applies the initial credit of a freshly created wallet. Unlike
// Credit, it keeps version 1: the opening balance is part of creation, not a
// later change. It is only valid on a wallet that never moved.
func (w *Wallet) Open(entryID, transactionID uuid.UUID, amount money.Money, now time.Time) (*LedgerEntry, error) {
	if w.version != 1 || !w.balance.IsZero() {
		return nil, fmt.Errorf("%w: wallet already opened", ErrInvalidTransition)
	}
	if err := w.checkMovement(amount); err != nil {
		return nil, err
	}
	entry, err := NewLedgerEntry(entryID, w.id, transactionID, DirectionCredit, amount, w.balance, amount, now)
	if err != nil {
		return nil, err
	}
	w.balance = amount
	w.updatedAt = now.UTC()
	return entry, nil
}
