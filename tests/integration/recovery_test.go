//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/erikyryan/desafio-go/internal/app"
	"github.com/erikyryan/desafio-go/internal/domain/wager"
	"github.com/erikyryan/desafio-go/internal/workers"
)

var fastPolicy = app.PendingReferencePolicy{InitialBackoff: 300 * time.Millisecond, MaxBackoff: 300 * time.Millisecond, MaxAttempts: 4, TTL: time.Hour}

func TestReversalBeforeReferenceIsResolvedByAnotherInstance(t *testing.T) {
	a := newServices(t, fastPolicy)
	w := a.openWallet(t, "100.00")
	refund := cmd(w, "REFUND", "early-refund-"+runID, "40.00")
	refund.ReferenceExternalTransactionID = "late-bet-" + runID
	res := a.submit(t, refund)
	if res.Status != wager.StatusPendingReference {
		t.Fatalf("expected PENDING_REFERENCE: %+v", res)
	}
	a.pool.Close() // instance A dies

	b := newServices(t, fastPolicy)
	worker := workers.NewPendingReferenceWorker(b.wagers, logger, workers.PendingConfig{PollInterval: 100 * time.Millisecond, BatchSize: 10})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { worker.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	// Still pending while the reference is missing (attempts grow, status unchanged).
	eventually(t, 5*time.Second, func() bool {
		tx, _ := b.wagers.GetTransaction(context.Background(), app.Actor{Internal: true}, res.TransactionID)
		return tx.Attempts() >= 2 && tx.Status() == wager.StatusPendingReference
	}, "worker retrying")
	b.submit(t, cmd(w, "BET", "late-bet-"+runID, "40.00"))
	eventually(t, 5*time.Second, func() bool {
		st, _ := txStatus(t, res.TransactionID)
		return st == wager.StatusProcessed
	}, "refund resolved")
	if got := b.balance(t, w.ID()); got.Balance().Amount() != "100.00" || got.Version() != 3 {
		t.Fatalf("balance %s v%d", got.Balance(), got.Version())
	}
	// The replay of the original request returns the final outcome.
	if again := b.submit(t, refund); !again.IdempotentReplay || again.Status != wager.StatusProcessed || again.Balance.Amount() != "100.00" {
		t.Fatalf("replay: %+v", again)
	}
	assertReconciled(t, b, w.ID())
}

func TestReversalExpiresAsRejected(t *testing.T) {
	s := newServices(t, fastPolicy)
	w := s.openWallet(t, "100.00")
	rb := cmd(w, "ROLLBACK", "expire-"+runID, "40.00")
	rb.ReferenceExternalTransactionID = "never-" + runID
	res := s.submit(t, rb)
	worker := workers.NewPendingReferenceWorker(s.wagers, logger, workers.PendingConfig{PollInterval: 100 * time.Millisecond, BatchSize: 10})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { worker.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	eventually(t, 10*time.Second, func() bool {
		st, _ := txStatus(t, res.TransactionID)
		return st == wager.StatusRejected
	}, "expired")
	if _, code := txStatus(t, res.TransactionID); code != wager.FailureReferenceNotFound {
		t.Fatalf("code %s", code)
	}
	var events int
	pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='WagerTransactionRejected'`, res.TransactionID).Scan(&events)
	if events != 1 {
		t.Fatalf("rejected events: %d", events)
	}
	// Late reference after expiry: the rejection is final, the bet itself is fine.
	s.submit(t, cmd(w, "BET", "never-"+runID, "40.00"))
	if st, _ := txStatus(t, res.TransactionID); st != wager.StatusRejected {
		t.Fatal("terminal state changed")
	}
	if got := s.balance(t, w.ID()); got.Balance().Amount() != "60.00" {
		t.Fatalf("balance %s", got.Balance())
	}
}

func TestPendingRowsAreClaimedByOneInstanceOnly(t *testing.T) {
	s := newServices(t, fastPolicy)
	w := s.openWallet(t, "100.00")
	s.submit(t, cmd(w, "BET", "claim-bet-"+runID, "10.00"))
	const n = 10
	ids := make([]uuid.UUID, n)
	for i := range ids {
		r := cmd(w, "ROLLBACK", uuidName("claim-rb", i), "10.00")
		r.ReferenceExternalTransactionID = "claim-bet-" + runID
		ids[i] = s.submit(t, r).TransactionID
		_ = ids
	}
	// Reference exists now: exactly one rollback succeeds, the others are rejected as already reversed.
	time.Sleep(fastPolicy.InitialBackoff + 50*time.Millisecond)
	inst := instances(t, 3)
	var wg sync.WaitGroup
	for _, in := range inst {
		wg.Add(1)
		go func(in *services) {
			defer wg.Done()
			in.wagers = app.NewWagerService(in.uow, in.clock.Now, uuid.New, nil, logger, fastPolicy)
			for i := 0; i < 5; i++ {
				in.wagers.ResolvePendingReferences(context.Background(), 4)
			}
		}(in)
	}
	wg.Wait()
	var processed, rejected int
	pool.QueryRow(context.Background(), `SELECT COUNT(*) FILTER (WHERE status='PROCESSED'), COUNT(*) FILTER (WHERE status='REJECTED') FROM wager_transactions WHERE wallet_id=$1 AND kind='ROLLBACK'`, w.ID()).Scan(&processed, &rejected)
	if processed != 1 || rejected != n-1 {
		t.Fatalf("processed=%d rejected=%d", processed, rejected)
	}
	if got := s.balance(t, w.ID()); got.Balance().Amount() != "100.00" {
		t.Fatalf("balance %s", got.Balance())
	}
	assertReconciled(t, s, w.ID())
}

func uuidName(prefix string, i int) string {
	return prefix + "-" + runID + "-" + uuid.NewString()[:4] + string(rune('a'+i))
}

func TestRestartPreservesIdempotencyPendingAndConsistency(t *testing.T) {
	a := newServices(t, fastPolicy)
	w := a.openWallet(t, "500.00")
	bet := a.submit(t, cmd(w, "BET", "restart-bet-"+runID, "100.00"))
	a.submit(t, cmd(w, "WIN", "restart-win-"+runID, "30.00"))
	rb := cmd(w, "ROLLBACK", "restart-rb-"+runID, "7.00")
	rb.ReferenceExternalTransactionID = "restart-future-" + runID
	pending := a.submit(t, rb)
	a.pool.Close() // "restart": every in-memory state is gone

	b := newServices(t, fastPolicy)
	replay := b.submit(t, cmd(w, "BET", "restart-bet-"+runID, "100.00"))
	if !replay.IdempotentReplay || replay.TransactionID != bet.TransactionID || replay.Balance.Amount() != "400.00" {
		t.Fatalf("idempotency lost after restart: %+v", replay)
	}
	if st, _ := txStatus(t, pending.TransactionID); st != wager.StatusPendingReference {
		t.Fatalf("pending lost: %s", st)
	}
	b.submit(t, cmd(w, "WIN", "restart-future-"+runID, "7.00"))
	eventually(t, 5*time.Second, func() bool {
		b.wagers.ResolvePendingReferences(context.Background(), 10)
		st, _ := txStatus(t, pending.TransactionID)
		return st == wager.StatusProcessed
	}, "pending resumed by the new instance")
	got := b.balance(t, w.ID())
	credits, debits, _ := ledgerStats(t, w.ID())
	if got.Balance().Minor() != credits-debits || got.Balance().Amount() != "430.00" {
		t.Fatalf("balance %s vs ledger %d-%d", got.Balance(), credits, debits)
	}
	assertReconciled(t, b, w.ID())
}
