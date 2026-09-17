package wager

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func processed(t *testing.T, kind Kind, amount string, wallet, player uuid.UUID, round string) *WagerTransaction {
	t.Helper()
	r := request(kind, amount)
	r.ExternalTransactionID, r.IdempotencyKey = "ref-"+string(kind), "k-"+string(kind)
	r.WalletID, r.PlayerID, r.RoundID = wallet, player, round
	tx, err := NewExternalTransaction(uuid.New(), r, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.MarkProcessed(brl("0.00"), uuid.Nil, now); err != nil {
		t.Fatal(err)
	}
	return tx
}

func reversal(t *testing.T, kind Kind, amount string, wallet, player uuid.UUID, round string) *WagerTransaction {
	t.Helper()
	r := request(kind, amount)
	r.WalletID, r.PlayerID, r.RoundID = wallet, player, round
	tx, err := NewExternalTransaction(uuid.New(), r, now)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func TestValidateReference(t *testing.T) {
	wallet, player := uuid.New(), uuid.New()
	bet := processed(t, KindBet, "10.00", wallet, player, "r-1")
	win := processed(t, KindWin, "10.00", wallet, player, "r-1")

	cases := []struct {
		name    string
		op      *WagerTransaction
		ref     *WagerTransaction
		revd    bool
		code    FailureCode
		wantErr error
	}{
		{"refund of bet ok", reversal(t, KindRefund, "10.00", wallet, player, "r-1"), bet, false, "", nil},
		{"rollback of bet ok", reversal(t, KindRollback, "10.00", wallet, player, "r-1"), bet, false, "", nil},
		{"rollback of win ok", reversal(t, KindRollback, "10.00", wallet, player, "r-1"), win, false, "", nil},
		{"refund of win", reversal(t, KindRefund, "10.00", wallet, player, "r-1"), win, false, FailureReferenceKindMismatch, ErrReferenceKind},
		{"partial", reversal(t, KindRefund, "5.00", wallet, player, "r-1"), bet, false, FailureReferenceAmountMismatch, ErrReferenceAmount},
		{"other round", reversal(t, KindRefund, "10.00", wallet, player, "r-2"), bet, false, FailureReferenceMismatch, ErrReferenceMismatch},
		{"other wallet", reversal(t, KindRefund, "10.00", uuid.New(), player, "r-1"), bet, false, FailureReferenceMismatch, ErrReferenceMismatch},
		{"already reversed", reversal(t, KindRollback, "10.00", wallet, player, "r-1"), bet, true, FailureReferenceAlreadyReversed, ErrAlreadyReversed},
	}
	for _, c := range cases {
		code, err := ValidateReference(c.op, c.ref, c.revd)
		if code != c.code || !errors.Is(err, c.wantErr) {
			t.Errorf("%s: code=%s err=%v (want %s / %v)", c.name, code, err, c.code, c.wantErr)
		}
	}

	pending := reversal(t, KindRefund, "10.00", wallet, player, "r-1") // still PENDING
	if _, err := ValidateReference(reversal(t, KindRollback, "10.00", wallet, player, "r-1"), pending, false); !errors.Is(err, ErrReferencePending) {
		t.Errorf("pending reference: %v", err)
	}
	rejected := reversal(t, KindBet, "10.00", wallet, player, "r-1")
	_ = rejected.MarkRejected(FailureInsufficientBalance, nil, now)
	if code, _ := ValidateReference(reversal(t, KindRefund, "10.00", wallet, player, "r-1"), rejected, false); code != FailureReferenceNotProcessed {
		t.Errorf("rejected reference: %s", code)
	}
	otherProvider := reversal(t, KindRefund, "10.00", wallet, player, "r-1")
	otherProvider.providerID = "provider-b"
	if code, _ := ValidateReference(otherProvider, bet, false); code != FailureReferenceMismatch {
		t.Errorf("other provider: %s", code)
	}
}

func TestReversalDirection(t *testing.T) {
	cases := []struct {
		op, ref Kind
		dir     Direction
		ok      bool
	}{
		{KindRefund, KindBet, DirectionCredit, true},
		{KindRollback, KindBet, DirectionCredit, true},
		{KindRollback, KindWin, DirectionDebit, true},
		{KindRollback, KindRefund, DirectionDebit, true},
		{KindRefund, KindWin, "", false},
		{KindRollback, KindLoss, "", false},
	}
	for _, c := range cases {
		d, err := ReversalDirection(c.op, c.ref)
		if (err == nil) != c.ok || d != c.dir {
			t.Errorf("%s of %s: %s %v", c.op, c.ref, d, err)
		}
	}
}
