package app

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/erikyryan/desafio-go/internal/domain/money"
	"github.com/erikyryan/desafio-go/internal/domain/wager"
)

// memStore is a single-goroutine in-memory Store for use case unit tests. It
// mimics the constraints the schema enforces (unique key, unique external id,
// one processed reversal per reference) but not row locking.
type memStore struct {
	mu      sync.Mutex
	wallets map[uuid.UUID]*wager.Wallet
	txs     map[uuid.UUID]*wager.WagerTransaction
	ledger  []*wager.LedgerEntry
	events  []wager.Event
	inbox   map[string]string
}

func newMemStore() *memStore {
	return &memStore{wallets: map[uuid.UUID]*wager.Wallet{}, txs: map[uuid.UUID]*wager.WagerTransaction{}, inbox: map[string]string{}}
}

func (m *memStore) Do(ctx context.Context, fn func(context.Context, Store) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return fn(ctx, m)
}
func (m *memStore) DoReadOnlySnapshot(ctx context.Context, fn func(context.Context, Store) error) error {
	return m.Do(ctx, fn)
}
func (m *memStore) Wallets() WalletRepository           { return memWallets{m} }
func (m *memStore) Transactions() TransactionRepository { return memTxs{m} }
func (m *memStore) Ledger() LedgerRepository            { return memLedger{m} }
func (m *memStore) Outbox() OutboxRepository            { return memOutbox{m} }
func (m *memStore) Inbox() InboxRepository              { return memInbox{m} }

type memWallets struct{ *memStore }
type memTxs struct{ *memStore }
type memLedger struct{ *memStore }
type memOutbox struct{ *memStore }
type memInbox struct{ *memStore }

func clone(w *wager.Wallet) *wager.Wallet {
	c, _ := wager.RehydrateWallet(w.ID(), w.PlayerID(), w.Balance(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	return c
}

func (m memWallets) Create(_ context.Context, w *wager.Wallet) error {
	for _, e := range m.wallets {
		if e.PlayerID() == w.PlayerID() && e.Currency() == w.Currency() {
			return ErrUniqueViolation
		}
	}
	m.wallets[w.ID()] = clone(w)
	return nil
}
func (m memWallets) Get(_ context.Context, id uuid.UUID) (*wager.Wallet, error) {
	w, ok := m.wallets[id]
	if !ok {
		return nil, ErrNotFound
	}
	return clone(w), nil
}
func (m memWallets) GetForUpdate(ctx context.Context, id uuid.UUID) (*wager.Wallet, error) {
	return m.Get(ctx, id)
}
func (m memWallets) Update(_ context.Context, w *wager.Wallet, expected int64) error {
	cur, ok := m.wallets[w.ID()]
	if !ok || cur.Version() != expected {
		return ErrConcurrentModification
	}
	m.wallets[w.ID()] = clone(w)
	return nil
}

func cloneTx(t *wager.WagerTransaction) *wager.WagerTransaction {
	c, _ := wager.RehydrateTransaction(t.Snapshot())
	return c
}
func (m memTxs) Insert(_ context.Context, t *wager.WagerTransaction) error {
	for _, e := range m.txs {
		if t.IsExternal() && (e.IdempotencyKey() == t.IdempotencyKey() || (e.ProviderID() == t.ProviderID() && e.ExternalTransactionID() == t.ExternalTransactionID())) {
			return ErrUniqueViolation
		}
		if t.Kind() == wager.KindOpening && e.Kind() == wager.KindOpening && e.WalletID() == t.WalletID() {
			return ErrUniqueViolation
		}
	}
	m.txs[t.ID()] = cloneTx(t)
	return nil
}
func (m memTxs) Update(_ context.Context, t *wager.WagerTransaction) error {
	if _, ok := m.txs[t.ID()]; !ok {
		return ErrNotFound
	}
	m.txs[t.ID()] = cloneTx(t)
	return nil
}
func (m memTxs) GetByID(_ context.Context, id uuid.UUID) (*wager.WagerTransaction, error) {
	t, ok := m.txs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneTx(t), nil
}
func (m memTxs) GetByIdempotencyKey(_ context.Context, key string) (*wager.WagerTransaction, error) {
	for _, t := range m.txs {
		if t.IdempotencyKey() == key {
			return cloneTx(t), nil
		}
	}
	return nil, ErrNotFound
}
func (m memTxs) GetByExternalID(_ context.Context, p, e string) (*wager.WagerTransaction, error) {
	for _, t := range m.txs {
		if t.ProviderID() == p && t.ExternalTransactionID() == e {
			return cloneTx(t), nil
		}
	}
	return nil, ErrNotFound
}
func (m memTxs) HasProcessedReversal(_ context.Context, ref uuid.UUID) (bool, error) {
	for _, t := range m.txs {
		if t.ReferenceTransactionID() == ref && t.Status() == wager.StatusProcessed && t.Kind().IsReversal() {
			return true, nil
		}
	}
	return false, nil
}
func (m memTxs) ListDuePendingReferences(_ context.Context, now time.Time, limit int) ([]uuid.UUID, error) {
	var ids []uuid.UUID
	for _, t := range m.txs {
		if t.Status() == wager.StatusPendingReference && !t.NextAttemptAt().After(now) {
			ids = append(ids, t.ID())
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	if len(ids) > limit {
		ids = ids[:limit]
	}
	return ids, nil
}
func (m memTxs) GetForUpdateSkipLocked(ctx context.Context, id uuid.UUID) (*wager.WagerTransaction, error) {
	return m.GetByID(ctx, id)
}

func (m memLedger) Insert(_ context.Context, e *wager.LedgerEntry) error {
	for _, x := range m.ledger {
		if x.WalletID() == e.WalletID() && x.TransactionID() == e.TransactionID() {
			return ErrUniqueViolation
		}
	}
	m.ledger = append(m.ledger, e)
	return nil
}
func (m memLedger) List(_ context.Context, walletID uuid.UUID, after int64, limit int) (LedgerPage, error) {
	var page LedgerPage
	for i, e := range m.ledger {
		if e.WalletID() == walletID && int64(i+1) > after {
			page.Entries = append(page.Entries, e)
			if len(page.Entries) == limit {
				page.NextCursor = int64(i + 1)
				break
			}
		}
	}
	return page, nil
}
func (m memLedger) Totals(_ context.Context, walletID uuid.UUID, c money.Currency) (LedgerTotals, error) {
	t := LedgerTotals{Credits: money.MustZero(c), Debits: money.MustZero(c)}
	for _, e := range m.ledger {
		if e.WalletID() != walletID {
			continue
		}
		t.Count++
		if e.Direction() == wager.DirectionCredit {
			t.Credits, _ = t.Credits.Add(e.Amount())
		} else {
			t.Debits, _ = t.Debits.Add(e.Amount())
		}
	}
	return t, nil
}
func (m memOutbox) Insert(_ context.Context, events ...wager.Event) error {
	m.events = append(m.events, events...)
	return nil
}
func (m memOutbox) Claim(context.Context, string, time.Duration, int, time.Time) ([]OutboxRecord, error) {
	return nil, nil
}
func (m memOutbox) MarkPublished(context.Context, uuid.UUID, time.Time) error      { return nil }
func (m memOutbox) Reschedule(context.Context, uuid.UUID, time.Time, string) error { return nil }
func (m memOutbox) OldestPendingAge(context.Context, time.Time) (time.Duration, error) {
	return 0, nil
}
func (m memInbox) Record(_ context.Context, consumer, id, hash string, _ time.Time) (bool, string, error) {
	if h, ok := m.inbox[consumer+"/"+id]; ok {
		return false, h, nil
	}
	m.inbox[consumer+"/"+id] = hash
	return true, hash, nil
}
