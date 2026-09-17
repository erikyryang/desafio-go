package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/erikyryan/desafio-go/internal/domain/money"
	"github.com/erikyryan/desafio-go/internal/domain/wager"
)

type fixture struct {
	t       *testing.T
	store   *memStore
	wallets *WalletService
	wagers  *WagerService
	now     time.Time
	policy  PendingReferencePolicy
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{t: t, store: newMemStore(), now: time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC),
		policy: PendingReferencePolicy{InitialBackoff: time.Second, MaxBackoff: 8 * time.Second, MaxAttempts: 3, TTL: time.Hour}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	clock := func() time.Time { return f.now }
	f.wallets = NewWalletService(f.store, clock, uuid.New, nil, log)
	f.wagers = NewWagerService(f.store, clock, uuid.New, nil, log, f.policy)
	return f
}

func (f *fixture) openWallet(balance string) *wager.Wallet {
	f.t.Helper()
	w, err := f.wallets.OpenWallet(context.Background(), OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: money.MustParse(balance, "BRL")})
	if err != nil {
		f.t.Fatal(err)
	}
	return w
}

func (f *fixture) cmd(w *wager.Wallet, kind, ext, amount string) SubmitCommand {
	return SubmitCommand{IdempotencyKey: "provider-a:" + ext, ProviderID: "provider-a", ExternalTransactionID: ext,
		PlayerID: w.PlayerID().String(), WalletID: w.ID().String(), RoundID: "round-1", GameID: "game", Kind: kind,
		Amount: amount, Currency: "BRL", Source: SourceHTTP}
}

func (f *fixture) submit(c SubmitCommand) *SubmitResult {
	f.t.Helper()
	res, err := f.wagers.Submit(context.Background(), c)
	if err != nil {
		f.t.Fatalf("submit %s %s: %v", c.Kind, c.ExternalTransactionID, err)
	}
	return res
}

func (f *fixture) balance(w *wager.Wallet) string {
	f.t.Helper()
	cur, err := f.wallets.GetWallet(context.Background(), w.ID())
	if err != nil {
		f.t.Fatal(err)
	}
	return cur.Balance().Amount()
}

func (f *fixture) eventTypes() []string {
	var out []string
	for _, e := range f.store.events {
		out = append(out, e.EventType)
	}
	return out
}

func TestOpenWalletPositiveAndZero(t *testing.T) {
	f := newFixture(t)
	w := f.openWallet("1000.00")
	if w.Version() != 1 || w.Balance().Amount() != "1000.00" {
		t.Fatalf("wallet %s v%d", w.Balance(), w.Version())
	}
	if len(f.store.ledger) != 1 || len(f.store.txs) != 1 {
		t.Fatalf("opening must create one ledger entry and one OPENING transaction: %d/%d", len(f.store.ledger), len(f.store.txs))
	}
	if got := f.eventTypes(); len(got) != 2 || got[0] != wager.EventWagerTransactionProcessed || got[1] != wager.EventWalletBalanceChanged {
		t.Fatalf("events %v", got)
	}
	for _, tx := range f.store.txs {
		if tx.Kind() != wager.KindOpening || tx.Status() != wager.StatusProcessed || tx.IsExternal() {
			t.Errorf("opening tx %+v", tx.Snapshot())
		}
	}
	// Zero balance: no OPENING, ledger nor events.
	before := len(f.store.events)
	z := f.openWallet("0.00")
	if len(f.store.ledger) != 1 || len(f.store.txs) != 1 || len(f.store.events) != before || z.Version() != 1 {
		t.Error("zero opening must not create ledger/transaction/events")
	}
	// Duplicate (player, currency) conflicts.
	_, err := f.wallets.OpenWallet(context.Background(), OpenWalletCommand{PlayerID: w.PlayerID(), InitialBalance: money.MustParse("1.00", "BRL")})
	if !errors.Is(err, ErrWalletAlreadyExists) {
		t.Errorf("duplicate wallet: %v", err)
	}
	if _, err := f.wallets.OpenWallet(context.Background(), OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: money.MustParse("-1.00", "BRL")}); !errors.Is(err, ErrValidation) {
		t.Errorf("negative opening: %v", err)
	}
}

func TestBetWinLossRules(t *testing.T) {
	f := newFixture(t)
	w := f.openWallet("100.00")

	bet := f.submit(f.cmd(w, "BET", "bet-1", "80.00"))
	if bet.Status != wager.StatusProcessed || bet.Balance.Amount() != "20.00" {
		t.Fatalf("bet: %+v", bet)
	}
	rej := f.submit(f.cmd(w, "BET", "bet-2", "80.00"))
	if rej.Status != wager.StatusRejected || rej.FailureCode != wager.FailureInsufficientBalance || rej.Balance.Amount() != "20.00" {
		t.Fatalf("insufficient: %+v", rej)
	}
	win := f.submit(f.cmd(w, "WIN", "win-1", "5.50"))
	if win.Status != wager.StatusProcessed || win.Balance.Amount() != "25.50" {
		t.Fatalf("win: %+v", win)
	}
	entries := len(f.store.ledger)
	versionBefore := f.store.wallets[w.ID()].Version()
	loss := f.submit(f.cmd(w, "LOSS", "loss-1", "0.00"))
	if loss.Status != wager.StatusProcessed || loss.Balance.Amount() != "25.50" {
		t.Fatalf("loss: %+v", loss)
	}
	if len(f.store.ledger) != entries || f.store.wallets[w.ID()].Version() != versionBefore {
		t.Error("LOSS must not create ledger entries nor bump the version")
	}
	types := f.eventTypes()
	if types[len(types)-1] != wager.EventWagerTransactionProcessed {
		t.Errorf("LOSS must emit WagerTransactionProcessed only: %v", types)
	}
	for _, c := range []SubmitCommand{f.cmd(w, "LOSS", "loss-2", "1.00"), f.cmd(w, "BET", "bet-3", "0.00"), f.cmd(w, "OPENING", "op-1", "1.00"), f.cmd(w, "WIN", "win-2", "-1.00"), f.cmd(w, "WIN", "win-3", "1.001")} {
		if _, err := f.wagers.Submit(context.Background(), c); !errors.Is(err, ErrValidation) {
			t.Errorf("%s %s should be a validation error: %v", c.Kind, c.Amount, err)
		}
	}
	if len(f.store.txs) != 5 { // opening + 4 handled
		t.Errorf("validation errors must not persist transactions: %d", len(f.store.txs))
	}
}

func TestCurrencyAndPlayerMismatchAreRejected(t *testing.T) {
	f := newFixture(t)
	w := f.openWallet("100.00")
	c := f.cmd(w, "BET", "usd-1", "1.00")
	c.Currency = "USD"
	if res := f.submit(c); res.Status != wager.StatusRejected || res.FailureCode != wager.FailureCurrencyMismatch {
		t.Errorf("usd: %+v", res)
	}
	c = f.cmd(w, "BET", "other-player", "1.00")
	c.PlayerID = uuid.NewString()
	if res := f.submit(c); res.Status != wager.StatusRejected || res.FailureCode != wager.FailureWalletPlayerMismatch {
		t.Errorf("player: %+v", res)
	}
	c = f.cmd(w, "BET", "no-wallet", "1.00")
	c.WalletID = uuid.NewString()
	if _, err := f.wagers.Submit(context.Background(), c); !errors.Is(err, ErrWalletNotFound) {
		t.Errorf("unknown wallet: %v", err)
	}
	if f.balance(w) != "100.00" {
		t.Error("rejections must not move money")
	}
}

func TestIdempotencyReplayAndConflicts(t *testing.T) {
	f := newFixture(t)
	w := f.openWallet("100.00")
	first := f.submit(f.cmd(w, "BET", "bet-1", "10.00"))
	f.submit(f.cmd(w, "WIN", "win-1", "50.00")) // wallet moves on

	replay := f.cmd(w, "BET", "bet-1", "10") // "10" normalizes to "10.00": same hash
	replay.Source = SourceSQS
	res := f.submit(replay)
	if !res.IdempotentReplay || res.TransactionID != first.TransactionID || res.Balance.Amount() != "90.00" {
		t.Fatalf("replay must return the original outcome: %+v", res)
	}
	if f.balance(w) != "140.00" {
		t.Error("replay moved money")
	}

	changed := f.cmd(w, "BET", "bet-1", "11.00")
	if _, err := f.wagers.Submit(context.Background(), changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Errorf("same key different payload: %v", err)
	}
	otherKey := f.cmd(w, "BET", "bet-1", "10.00")
	otherKey.IdempotencyKey = "another-key"
	if _, err := f.wagers.Submit(context.Background(), otherKey); !errors.Is(err, ErrExternalIDConflict) {
		t.Errorf("same operation other key: %v", err)
	}
	reusedKey := f.cmd(w, "BET", "bet-9", "10.00")
	reusedKey.IdempotencyKey = "provider-a:bet-1"
	if _, err := f.wagers.Submit(context.Background(), reusedKey); !errors.Is(err, ErrIdempotencyConflict) {
		t.Errorf("key reused for another operation: %v", err)
	}
	if f.balance(w) != "140.00" {
		t.Error("conflicts moved money")
	}
}

func TestRefundAndRollback(t *testing.T) {
	f := newFixture(t)
	w := f.openWallet("100.00")
	f.submit(f.cmd(w, "BET", "bet-1", "30.00"))
	f.submit(f.cmd(w, "WIN", "win-1", "10.00")) // 80.00

	ref := f.cmd(w, "REFUND", "refund-1", "30.00")
	ref.ReferenceExternalTransactionID = "bet-1"
	if res := f.submit(ref); res.Status != wager.StatusProcessed || res.Balance.Amount() != "110.00" {
		t.Fatalf("refund: %+v", res)
	}
	// Second reversal of the same bet (any kind) is rejected.
	rb := f.cmd(w, "ROLLBACK", "rollback-1", "30.00")
	rb.ReferenceExternalTransactionID = "bet-1"
	if res := f.submit(rb); res.Status != wager.StatusRejected || res.FailureCode != wager.FailureReferenceAlreadyReversed {
		t.Fatalf("double reversal: %+v", res)
	}
	// Rollback of the WIN debits.
	rbw := f.cmd(w, "ROLLBACK", "rollback-2", "10.00")
	rbw.ReferenceExternalTransactionID = "win-1"
	if res := f.submit(rbw); res.Status != wager.StatusProcessed || res.Balance.Amount() != "100.00" {
		t.Fatalf("rollback win: %+v", res)
	}
	// Rollback of the REFUND (debit 30) is allowed once.
	rbr := f.cmd(w, "ROLLBACK", "rollback-3", "30.00")
	rbr.ReferenceExternalTransactionID = "refund-1"
	if res := f.submit(rbr); res.Status != wager.StatusProcessed || res.Balance.Amount() != "70.00" {
		t.Fatalf("rollback refund: %+v", res)
	}
	// Reversal that would overdraw: distinct failure code.
	f.submit(f.cmd(w, "WIN", "win-big", "500.00")) // 570
	f.submit(f.cmd(w, "BET", "bet-big", "560.00")) // 10
	rbb := f.cmd(w, "ROLLBACK", "rollback-4", "500.00")
	rbb.ReferenceExternalTransactionID = "win-big"
	if res := f.submit(rbb); res.Status != wager.StatusRejected || res.FailureCode != wager.FailureReversalInsufficientBalance {
		t.Fatalf("reversal overdraft: %+v", res)
	}
	// Partial and kind mismatches.
	part := f.cmd(w, "REFUND", "refund-2", "1.00")
	part.ReferenceExternalTransactionID = "bet-big"
	if res := f.submit(part); res.FailureCode != wager.FailureReferenceAmountMismatch {
		t.Errorf("partial: %+v", res)
	}
	kind := f.cmd(w, "REFUND", "refund-3", "500.00")
	kind.ReferenceExternalTransactionID = "win-big"
	if res := f.submit(kind); res.FailureCode != wager.FailureReferenceKindMismatch {
		t.Errorf("refund of win: %+v", res)
	}
	// Reference that was rejected.
	f.submit(f.cmd(w, "BET", "bet-rej", "999.00"))
	rr := f.cmd(w, "REFUND", "refund-4", "999.00")
	rr.ReferenceExternalTransactionID = "bet-rej"
	if res := f.submit(rr); res.FailureCode != wager.FailureReferenceNotProcessed {
		t.Errorf("rejected reference: %+v", res)
	}
	// Ledger reconciles.
	rep, err := f.wallets.Reconcile(context.Background(), w.ID())
	if err != nil || !rep.Consistent || rep.StoredBalance.Amount() != "10.00" {
		t.Fatalf("reconcile: %v %+v", err, rep)
	}
}

func TestPendingReferenceResolvedByWorker(t *testing.T) {
	f := newFixture(t)
	w := f.openWallet("100.00")
	ref := f.cmd(w, "REFUND", "refund-1", "30.00")
	ref.ReferenceExternalTransactionID = "bet-1"
	res := f.submit(ref)
	if res.Status != wager.StatusPendingReference || res.Balance != nil {
		t.Fatalf("early refund: %+v", res)
	}
	if got := f.eventTypes(); got[len(got)-1] != wager.EventWagerTransactionPendingReference {
		t.Errorf("events %v", got)
	}
	// Replay of the pending operation reports the pending state.
	if again := f.submit(ref); !again.IdempotentReplay || again.Status != wager.StatusPendingReference {
		t.Fatalf("pending replay: %+v", again)
	}
	// Worker before due time does nothing.
	if n, _ := f.wagers.ResolvePendingReferences(context.Background(), 10); n != 0 {
		t.Fatalf("resolved before due: %d", n)
	}
	f.now = f.now.Add(2 * time.Second)
	if n, _ := f.wagers.ResolvePendingReferences(context.Background(), 10); n != 1 {
		t.Fatalf("expected one rescheduled: %d", n)
	}
	tx, _ := f.wagers.GetTransaction(context.Background(), Actor{Internal: true}, res.TransactionID)
	if tx.Status() != wager.StatusPendingReference || tx.Attempts() != 2 {
		t.Fatalf("after retry: %s attempts=%d", tx.Status(), tx.Attempts())
	}
	// Reference arrives; next due retry resolves it.
	f.submit(f.cmd(w, "BET", "bet-1", "30.00"))
	f.now = f.now.Add(10 * time.Second)
	if n, _ := f.wagers.ResolvePendingReferences(context.Background(), 10); n != 1 {
		t.Fatal("expected resolution")
	}
	tx, _ = f.wagers.GetTransaction(context.Background(), Actor{Internal: true}, res.TransactionID)
	if tx.Status() != wager.StatusProcessed || tx.BalanceAfter().Amount() != "100.00" || tx.ReferenceTransactionID() == uuid.Nil {
		t.Fatalf("resolved: %s %v", tx.Status(), tx.BalanceAfter())
	}
	if f.balance(w) != "100.00" {
		t.Error("balance after refund")
	}
}

func TestPendingReferenceExpires(t *testing.T) {
	f := newFixture(t)
	w := f.openWallet("100.00")
	ref := f.cmd(w, "ROLLBACK", "rb-1", "30.00")
	ref.ReferenceExternalTransactionID = "missing"
	res := f.submit(ref)
	for i := 0; i < f.policy.MaxAttempts; i++ {
		f.now = f.now.Add(f.policy.MaxBackoff)
		f.wagers.ResolvePendingReferences(context.Background(), 10)
	}
	tx, _ := f.wagers.GetTransaction(context.Background(), Actor{Internal: true}, res.TransactionID)
	if tx.Status() != wager.StatusRejected || tx.FailureCode() != wager.FailureReferenceNotFound {
		t.Fatalf("expired: %s %s attempts=%d", tx.Status(), tx.FailureCode(), tx.Attempts())
	}
	if got := f.eventTypes(); got[len(got)-1] != wager.EventWagerTransactionRejected {
		t.Errorf("events %v", got)
	}
	if f.balance(w) != "100.00" {
		t.Error("balance moved")
	}
}

func TestProviderIsolationOnReads(t *testing.T) {
	f := newFixture(t)
	w := f.openWallet("100.00")
	res := f.submit(f.cmd(w, "BET", "bet-1", "1.00"))
	if _, err := f.wagers.GetTransaction(context.Background(), Actor{ProviderID: "provider-b"}, res.TransactionID); !errors.Is(err, ErrTransactionNotFound) {
		t.Errorf("provider-b sees provider-a transaction: %v", err)
	}
	if _, err := f.wagers.GetTransactionByExternalID(context.Background(), Actor{ProviderID: "provider-b"}, "provider-a", "bet-1"); !errors.Is(err, ErrForbidden) {
		t.Errorf("provider-b by external id: %v", err)
	}
	if _, err := f.wagers.GetTransaction(context.Background(), Actor{ProviderID: "provider-a"}, res.TransactionID); err != nil {
		t.Errorf("owner: %v", err)
	}
	if _, err := f.wagers.GetTransaction(context.Background(), Actor{Internal: true}, res.TransactionID); err != nil {
		t.Errorf("internal: %v", err)
	}
	var opening uuid.UUID
	for _, tx := range f.store.txs {
		if tx.Kind() == wager.KindOpening {
			opening = tx.ID()
		}
	}
	if _, err := f.wagers.GetTransaction(context.Background(), Actor{ProviderID: "provider-a"}, opening); !errors.Is(err, ErrTransactionNotFound) {
		t.Errorf("provider sees internal opening: %v", err)
	}
}

func TestPayloadHashNormalization(t *testing.T) {
	base := wager.ExternalRequest{ProviderID: "p", ExternalTransactionID: "e", PlayerID: uuid.New(), WalletID: uuid.New(),
		RoundID: "r", GameID: "g", Kind: wager.KindBet, Amount: money.MustParse("25", "BRL")}
	same := base
	same.Amount = money.MustParse("25.00", "BRL")
	same.IdempotencyKey = "different-key-is-excluded"
	if PayloadHash(base) != PayloadHash(same) {
		t.Error("equivalent amounts / keys must hash identically")
	}
	diff := base
	diff.Amount = money.MustParse("25.01", "BRL")
	if PayloadHash(base) == PayloadHash(diff) {
		t.Error("different amount must change the hash")
	}
	rev := base
	rev.Kind, rev.ReferenceExternalTransactionID = wager.KindRefund, "x"
	if PayloadHash(base) == PayloadHash(rev) {
		t.Error("reference must be part of the hash")
	}
	if len(PayloadHash(base)) != 64 {
		t.Error("sha256 hex expected")
	}
}

func TestCursorAndBackoff(t *testing.T) {
	if c := EncodeCursor(42); c == "" || c == "42" {
		t.Error("cursor must be opaque")
	}
	if n, err := DecodeCursor(EncodeCursor(42)); err != nil || n != 42 {
		t.Errorf("roundtrip: %d %v", n, err)
	}
	if _, err := DecodeCursor("not-a-cursor"); !errors.Is(err, ErrValidation) {
		t.Error("garbage cursor accepted")
	}
	p := PendingReferencePolicy{InitialBackoff: time.Second, MaxBackoff: 5 * time.Second}
	for attempt, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 5 * time.Second, 10: 5 * time.Second} {
		if got := p.NextBackoff(attempt); got != want {
			t.Errorf("attempt %d: %s want %s", attempt, got, want)
		}
	}
}
