package wager

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/erikyryan/desafio-go/internal/domain/money"
)

var now = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func brl(s string) money.Money { return money.MustParse(s, "BRL") }

func newTestWallet(t *testing.T, balance string) *Wallet {
	t.Helper()
	w, err := NewWallet(uuid.New(), uuid.New(), "BRL", now)
	if err != nil {
		t.Fatal(err)
	}
	if balance != "0.00" {
		if _, err := w.Open(uuid.New(), uuid.New(), brl(balance), now); err != nil {
			t.Fatal(err)
		}
	}
	return w
}

func TestNewWalletInvariants(t *testing.T) {
	if _, err := NewWallet(uuid.Nil, uuid.New(), "BRL", now); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("nil id: %v", err)
	}
	if _, err := NewWallet(uuid.New(), uuid.New(), "br", now); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("bad currency: %v", err)
	}
	w := newTestWallet(t, "0.00")
	if w.Version() != 1 || !w.Balance().IsZero() {
		t.Errorf("initial state: v=%d b=%s", w.Version(), w.Balance())
	}
}

func TestOpenKeepsVersionOne(t *testing.T) {
	w := newTestWallet(t, "1000.00")
	if w.Version() != 1 || w.Balance().Amount() != "1000.00" {
		t.Errorf("after open: v=%d b=%s", w.Version(), w.Balance())
	}
	if _, err := w.Open(uuid.New(), uuid.New(), brl("1.00"), now); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("second open: %v", err)
	}
}

func TestDebitCredit(t *testing.T) {
	w := newTestWallet(t, "100.00")
	tx := uuid.New()
	e, err := w.Debit(uuid.New(), tx, brl("80.00"), now)
	if err != nil {
		t.Fatal(err)
	}
	if e.Direction() != DirectionDebit || e.BalanceBefore().Amount() != "100.00" || e.BalanceAfter().Amount() != "20.00" {
		t.Errorf("entry: %+v", e)
	}
	if w.Version() != 2 || w.Balance().Amount() != "20.00" {
		t.Errorf("wallet: v=%d b=%s", w.Version(), w.Balance())
	}
	if _, err := w.Debit(uuid.New(), uuid.New(), brl("80.00"), now); !errors.Is(err, ErrInsufficientBalance) {
		t.Errorf("overdraft: %v", err)
	}
	if w.Version() != 2 {
		t.Error("failed debit must not bump version")
	}
	if _, err := w.Credit(uuid.New(), uuid.New(), brl("5.00"), now); err != nil || w.Balance().Amount() != "25.00" || w.Version() != 3 {
		t.Errorf("credit: %v %s v=%d", err, w.Balance(), w.Version())
	}
}

func TestMovementValidation(t *testing.T) {
	w := newTestWallet(t, "100.00")
	if _, err := w.Credit(uuid.New(), uuid.New(), money.MustParse("1.00", "USD"), now); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("usd: %v", err)
	}
	if _, err := w.Credit(uuid.New(), uuid.New(), brl("0.00"), now); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("zero: %v", err)
	}
	if _, err := w.Debit(uuid.New(), uuid.New(), money.Money{}, now); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("uninitialized: %v", err)
	}
	if w.CanDebit(brl("100.01")) || !w.CanDebit(brl("100.00")) {
		t.Error("CanDebit")
	}
}

func TestRehydrateWallet(t *testing.T) {
	w, err := RehydrateWallet(uuid.New(), uuid.New(), brl("10.00"), 7, now, now)
	if err != nil || w.Version() != 7 {
		t.Fatalf("rehydrate: %v", err)
	}
	if _, err := RehydrateWallet(uuid.New(), uuid.New(), brl("-1.00"), 1, now, now); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("negative: %v", err)
	}
	if _, err := RehydrateWallet(uuid.New(), uuid.New(), brl("1.00"), 0, now, now); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("version 0: %v", err)
	}
}

func TestLedgerEntryInvariant(t *testing.T) {
	id, wid, tid := uuid.New(), uuid.New(), uuid.New()
	if _, err := NewLedgerEntry(id, wid, tid, DirectionCredit, brl("5.00"), brl("10.00"), brl("15.00"), now); err != nil {
		t.Errorf("valid credit: %v", err)
	}
	if _, err := NewLedgerEntry(id, wid, tid, DirectionDebit, brl("5.00"), brl("10.00"), brl("15.00"), now); !errors.Is(err, ErrInvalidLedgerEntry) {
		t.Errorf("wrong arithmetic accepted")
	}
	if _, err := NewLedgerEntry(id, wid, tid, DirectionDebit, brl("15.00"), brl("10.00"), brl("-5.00"), now); !errors.Is(err, ErrInvalidLedgerEntry) {
		t.Errorf("negative after accepted")
	}
	if _, err := NewLedgerEntry(id, wid, tid, Direction("X"), brl("5.00"), brl("10.00"), brl("15.00"), now); !errors.Is(err, ErrInvalidLedgerEntry) {
		t.Errorf("bad direction accepted")
	}
	if _, err := NewLedgerEntry(id, wid, tid, DirectionCredit, money.MustParse("5.00", "USD"), brl("10.00"), brl("15.00"), now); !errors.Is(err, ErrInvalidLedgerEntry) {
		t.Errorf("mixed currency accepted")
	}
}
