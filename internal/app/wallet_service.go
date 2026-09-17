package app

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/erikyryan/desafio-go/internal/domain/money"
	"github.com/erikyryan/desafio-go/internal/domain/wager"
)

// WalletService implements wallet opening, reads and reconciliation.
type WalletService struct {
	uow     UnitOfWork
	clock   Clock
	newID   IDGenerator
	metrics Metrics
	log     *slog.Logger
}

// NewWalletService wires the use case.
func NewWalletService(uow UnitOfWork, clock Clock, newID IDGenerator, metrics Metrics, log *slog.Logger) *WalletService {
	if metrics == nil {
		metrics = NopMetrics{}
	}
	return &WalletService{uow: uow, clock: clock, newID: newID, metrics: metrics, log: log}
}

// OpenWalletCommand is the input of OpenWallet.
type OpenWalletCommand struct {
	PlayerID       uuid.UUID
	InitialBalance money.Money
	CorrelationID  string
}

// OpenWallet creates the wallet and, for a positive initial balance, the
// OPENING transaction, its ledger entry and the outbox events in one commit.
func (s *WalletService) OpenWallet(ctx context.Context, cmd OpenWalletCommand) (*wager.Wallet, error) {
	if cmd.PlayerID == uuid.Nil {
		return nil, fmt.Errorf("%w: playerId is required", ErrValidation)
	}
	if !cmd.InitialBalance.IsValid() {
		return nil, fmt.Errorf("%w: initialBalance is required", ErrValidation)
	}
	if cmd.InitialBalance.IsNegative() {
		return nil, fmt.Errorf("%w: initialBalance must not be negative", ErrValidation)
	}
	now := s.clock()
	w, err := wager.NewWallet(s.newID(), cmd.PlayerID, cmd.InitialBalance.Currency(), now)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrValidation, err)
	}
	err = s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		if cmd.InitialBalance.IsZero() {
			if err := st.Wallets().Create(ctx, w); err != nil {
				return mapWalletCreate(err)
			}
			return nil
		}
		opening, err := wager.NewOpeningTransaction(s.newID(), w.ID(), w.PlayerID(), cmd.InitialBalance, now)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrValidation, err)
		}
		entry, err := w.Open(s.newID(), opening.ID(), cmd.InitialBalance, now)
		if err != nil {
			return err
		}
		if err := opening.MarkProcessed(w.Balance(), uuid.Nil, now); err != nil {
			return err
		}
		if err := st.Wallets().Create(ctx, w); err != nil {
			return mapWalletCreate(err)
		}
		if err := st.Transactions().Insert(ctx, opening); err != nil {
			return err
		}
		if err := st.Ledger().Insert(ctx, entry); err != nil {
			return err
		}
		evCtx := wager.EventContext{CorrelationID: cmd.CorrelationID, CausationID: opening.ID().String()}
		processed, err := wager.NewWagerTransactionProcessed(s.newID(), opening, evCtx, now)
		if err != nil {
			return err
		}
		changed, err := wager.NewWalletBalanceChanged(s.newID(), w, entry, evCtx, now)
		if err != nil {
			return err
		}
		return st.Outbox().Insert(ctx, processed, changed)
	})
	if err != nil {
		return nil, err
	}
	s.metrics.TransactionOutcome("internal", wager.KindOpening, wager.StatusProcessed, "")
	s.log.InfoContext(ctx, "wallet opened", "walletId", w.ID(), "playerId", w.PlayerID(), "correlationId", cmd.CorrelationID)
	return w, nil
}

func mapWalletCreate(err error) error {
	if errors.Is(err, ErrUniqueViolation) {
		return ErrWalletAlreadyExists
	}
	return err
}

// GetWallet reads a wallet.
func (s *WalletService) GetWallet(ctx context.Context, id uuid.UUID) (*wager.Wallet, error) {
	var w *wager.Wallet
	err := s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		var err error
		w, err = st.Wallets().Get(ctx, id)
		return err
	})
	if errors.Is(err, ErrNotFound) {
		return nil, ErrWalletNotFound
	}
	return w, err
}

// Ledger cursor encoding: opaque base64url of "seq:<sequence>".
const cursorPrefix = "seq:"

// EncodeCursor turns a sequence into an opaque cursor.
func EncodeCursor(seq int64) string {
	if seq <= 0 {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString([]byte(cursorPrefix + strconv.FormatInt(seq, 10)))
}

// DecodeCursor parses an opaque cursor; "" means start.
func DecodeCursor(c string) (int64, error) {
	if c == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil || !strings.HasPrefix(string(raw), cursorPrefix) {
		return 0, fmt.Errorf("%w: invalid cursor", ErrValidation)
	}
	n, err := strconv.ParseInt(strings.TrimPrefix(string(raw), cursorPrefix), 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%w: invalid cursor", ErrValidation)
	}
	return n, nil
}

// ListLedger pages through the wallet ledger in insertion order.
func (s *WalletService) ListLedger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) (LedgerPage, error) {
	after, err := DecodeCursor(cursor)
	if err != nil {
		return LedgerPage{}, err
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	var page LedgerPage
	err = s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		if _, err := st.Wallets().Get(ctx, walletID); err != nil {
			return err
		}
		var err error
		page, err = st.Ledger().List(ctx, walletID, after, limit)
		return err
	})
	if errors.Is(err, ErrNotFound) {
		return LedgerPage{}, ErrWalletNotFound
	}
	return page, err
}

// ReconciliationReport compares the stored balance with the ledger.
type ReconciliationReport struct {
	WalletID          uuid.UUID
	StoredBalance     money.Money
	CalculatedBalance money.Money
	Difference        money.Money
	Consistent        bool
	CheckedEntries    int64
}

// Reconcile rebuilds the balance from the ledger (opening included) inside a
// single snapshot and reports the difference (stored - calculated). It never
// changes the balance.
func (s *WalletService) Reconcile(ctx context.Context, walletID uuid.UUID) (*ReconciliationReport, error) {
	var report *ReconciliationReport
	err := s.uow.DoReadOnlySnapshot(ctx, func(ctx context.Context, st Store) error {
		w, err := st.Wallets().Get(ctx, walletID)
		if err != nil {
			return err
		}
		totals, err := st.Ledger().Totals(ctx, walletID, w.Currency())
		if err != nil {
			return err
		}
		calculated, err := totals.Credits.Sub(totals.Debits)
		if err != nil {
			return err
		}
		diff, err := w.Balance().Sub(calculated)
		if err != nil {
			return err
		}
		report = &ReconciliationReport{
			WalletID: walletID, StoredBalance: w.Balance(), CalculatedBalance: calculated,
			Difference: diff, Consistent: diff.IsZero(), CheckedEntries: totals.Count,
		}
		return nil
	})
	if errors.Is(err, ErrNotFound) {
		return nil, ErrWalletNotFound
	}
	if err != nil {
		return nil, err
	}
	if !report.Consistent {
		s.metrics.ReconciliationDivergence()
		s.log.ErrorContext(ctx, "reconciliation divergence", "walletId", walletID,
			"storedBalance", report.StoredBalance.Amount(), "calculatedBalance", report.CalculatedBalance.Amount(),
			"difference", report.Difference.Amount(), "checkedEntries", report.CheckedEntries)
	}
	return report, nil
}
