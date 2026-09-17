package wager

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/erikyryan/desafio-go/internal/domain/money"
)

func request(kind Kind, amount string) ExternalRequest {
	r := ExternalRequest{
		ProviderID: "provider-a", ExternalTransactionID: "t-1", IdempotencyKey: "provider-a:t-1", PayloadHash: "h",
		PlayerID: uuid.New(), WalletID: uuid.New(), RoundID: "r-1", GameID: "g", Kind: kind, Amount: brl(amount),
	}
	if kind.IsReversal() {
		r.ReferenceExternalTransactionID = "t-0"
	}
	return r
}

func TestAmountPolicyPerKind(t *testing.T) {
	cases := []struct {
		kind   Kind
		amount string
		ok     bool
	}{
		{KindBet, "1.00", true}, {KindBet, "0.00", false},
		{KindWin, "1.00", true}, {KindWin, "0.00", false},
		{KindLoss, "0.00", true}, {KindLoss, "1.00", false},
		{KindRefund, "1.00", true}, {KindRefund, "0.00", false},
		{KindRollback, "1.00", true}, {KindRollback, "0.00", false},
	}
	for _, c := range cases {
		_, err := NewExternalTransaction(uuid.New(), request(c.kind, c.amount), now)
		if (err == nil) != c.ok {
			t.Errorf("%s %s: err=%v ok=%v", c.kind, c.amount, err, c.ok)
		}
	}
	if _, err := NewExternalTransaction(uuid.New(), request(KindBet, "-1.00"), now); err == nil {
		t.Error("negative accepted")
	}
}

func TestOpeningRejectedExternally(t *testing.T) {
	if _, err := ParseExternalKind("OPENING"); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("OPENING accepted: %v", err)
	}
	if _, err := NewExternalTransaction(uuid.New(), request(KindOpening, "1.00"), now); err == nil {
		t.Error("OPENING transaction created from external request")
	}
}

func TestReferenceRequirement(t *testing.T) {
	r := request(KindRefund, "1.00")
	r.ReferenceExternalTransactionID = ""
	if _, err := NewExternalTransaction(uuid.New(), r, now); err == nil {
		t.Error("REFUND without reference accepted")
	}
	r = request(KindBet, "1.00")
	r.ReferenceExternalTransactionID = "x"
	if _, err := NewExternalTransaction(uuid.New(), r, now); err == nil {
		t.Error("BET with reference accepted")
	}
}

func TestStateMachine(t *testing.T) {
	tx, _ := NewExternalTransaction(uuid.New(), request(KindBet, "1.00"), now)
	if tx.Status() != StatusPending {
		t.Fatalf("initial %s", tx.Status())
	}
	if err := tx.MarkPendingReference(now.Add(time.Second), now); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("BET cannot wait for reference: %v", err)
	}
	if err := tx.MarkProcessed(brl("9.00"), uuid.Nil, now); err != nil {
		t.Fatal(err)
	}
	if tx.BalanceAfter().Amount() != "9.00" || tx.ProcessedAt().IsZero() {
		t.Error("processed snapshot")
	}
	for _, f := range []func() error{
		func() error { return tx.MarkRejected(FailureInsufficientBalance, nil, now) },
		func() error { return tx.MarkFailed(FailureInfrastructure, now) },
		func() error { return tx.MarkProcessed(brl("1.00"), uuid.Nil, now) },
	} {
		if err := f(); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("terminal transition allowed: %v", err)
		}
	}

	ref, _ := NewExternalTransaction(uuid.New(), request(KindRefund, "1.00"), now)
	if err := ref.MarkPendingReference(now.Add(time.Second), now); err != nil || ref.Attempts() != 1 {
		t.Fatalf("park: %v attempts=%d", err, ref.Attempts())
	}
	if err := ref.MarkPendingReference(now.Add(2*time.Second), now); err != nil || ref.Attempts() != 2 {
		t.Fatalf("re-park: %v attempts=%d", err, ref.Attempts())
	}
	if err := ref.MarkProcessed(brl("1.00"), uuid.Nil, now); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("reversal processed without reference: %v", err)
	}
	if err := ref.MarkRejected(FailureReferenceNotFound, nil, now); err != nil || ref.Status() != StatusRejected {
		t.Errorf("reject: %v", err)
	}
	if err := ref.MarkRejected("", nil, now); err == nil {
		t.Error("empty code accepted")
	}
}

func TestRehydrateDoesNotReapply(t *testing.T) {
	tx, _ := NewExternalTransaction(uuid.New(), request(KindWin, "5.00"), now)
	_ = tx.MarkProcessed(brl("15.00"), uuid.Nil, now)
	re, err := RehydrateTransaction(tx.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if re.Status() != StatusProcessed || !re.BalanceAfter().Equal(brl("15.00")) || re.ID() != tx.ID() {
		t.Error("rehydrated state differs")
	}
	s := tx.Snapshot()
	s.Status = "WEIRD"
	if _, err := RehydrateTransaction(s); err == nil {
		t.Error("invalid status accepted")
	}
	s = tx.Snapshot()
	s.Amount = money.Money{}
	if _, err := RehydrateTransaction(s); err == nil {
		t.Error("invalid money accepted")
	}
}

func TestOpeningTransaction(t *testing.T) {
	op, err := NewOpeningTransaction(uuid.New(), uuid.New(), uuid.New(), brl("100.00"), now)
	if err != nil {
		t.Fatal(err)
	}
	if op.Origin() != OriginInternal || op.IsExternal() || op.ProviderID() != "" || op.IdempotencyKey() != "" || op.RoundID() != "" {
		t.Error("opening must carry no external metadata")
	}
	if _, err := NewOpeningTransaction(uuid.New(), uuid.New(), uuid.New(), brl("0.00"), now); err == nil {
		t.Error("zero opening accepted")
	}
}

func TestEventsCarryTypeAndVersion(t *testing.T) {
	w := newTestWallet(t, "100.00")
	op, _ := NewOpeningTransaction(uuid.New(), w.ID(), w.PlayerID(), brl("100.00"), now)
	_ = op.MarkProcessed(w.Balance(), uuid.Nil, now)
	ev, err := NewWagerTransactionProcessed(uuid.New(), op, EventContext{CorrelationID: "c"}, now)
	if err != nil || ev.EventType != EventWagerTransactionProcessed || ev.Version != 1 || ev.AggregateID != op.ID() {
		t.Fatalf("processed event: %v %+v", err, ev)
	}
	if _, err := NewWagerTransactionRejected(uuid.New(), op, EventContext{}, now); err == nil {
		t.Error("rejected event built from processed transaction")
	}
	entry, _ := w.Credit(uuid.New(), op.ID(), brl("1.00"), now)
	bc, err := NewWalletBalanceChanged(uuid.New(), w, entry, EventContext{}, now)
	if err != nil {
		t.Fatal(err)
	}
	data := bc.Data.(BalanceChangedData)
	if data.WalletVersion != 2 || data.BalanceAfter.Amount() != "101.00" || data.Direction != DirectionCredit {
		t.Errorf("balance changed payload: %+v", data)
	}
	other := newTestWallet(t, "0.00")
	if _, err := NewWalletBalanceChanged(uuid.New(), other, entry, EventContext{}, now); err == nil {
		t.Error("entry of another wallet accepted")
	}
}

func TestFailureCodeClassification(t *testing.T) {
	if FailureInsufficientBalance.IsCorrectable() || !FailureCurrencyMismatch.IsCorrectable() {
		t.Error("classification")
	}
	for _, c := range []FailureCode{FailureInsufficientBalance, FailureReversalInsufficientBalance, FailureReferenceNotFound} {
		if strings.TrimSpace(string(c)) == "" {
			t.Error("empty code")
		}
	}
}
