//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/erikyryan/desafio-go/internal/app"
	"github.com/erikyryan/desafio-go/internal/domain/wager"
)

// instances builds n independent services, each with its own pool.
func instances(t *testing.T, n int) []*services {
	out := make([]*services, n)
	for i := range out {
		out[i] = newServices(t, app.PendingReferencePolicy{})
	}
	return out
}

func TestSameBetFiftyTimesInParallel(t *testing.T) {
	inst := instances(t, 3)
	w := inst[0].openWallet(t, "100.00")
	c := cmd(w, "BET", "dup-"+runID, "10.00")
	var wg sync.WaitGroup
	results := make([]*app.SubmitResult, 50)
	errs := make([]error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = inst[i%3].wagers.Submit(context.Background(), c)
		}(i)
	}
	wg.Wait()
	replays := 0
	var id uuid.UUID
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("submit %d: %v", i, errs[i])
		}
		if id == uuid.Nil {
			id = results[i].TransactionID
		}
		if results[i].TransactionID != id || results[i].Status != wager.StatusProcessed || results[i].Balance.Amount() != "90.00" {
			t.Fatalf("result %d: %+v", i, results[i])
		}
		if results[i].IdempotentReplay {
			replays++
		}
	}
	if replays != 49 {
		t.Fatalf("expected 49 replays, got %d", replays)
	}
	if got := inst[1].balance(t, w.ID()); got.Balance().Amount() != "90.00" || got.Version() != 2 {
		t.Fatalf("balance %s v%d", got.Balance(), got.Version())
	}
	if _, debits, n := ledgerStats(t, w.ID()); debits != 1000 || n != 2 {
		t.Fatalf("ledger: debits=%d entries=%d", debits, n)
	}
	assertReconciled(t, inst[2], w.ID())
}

func TestTwoBetsOfEightyOnOneHundred(t *testing.T) {
	inst := instances(t, 3)
	w := inst[0].openWallet(t, "100.00")
	a, b := cmd(w, "BET", "eighty-a-"+runID, "80.00"), cmd(w, "BET", "eighty-b-"+runID, "80.00")
	run := func() (ra, rb *app.SubmitResult) {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); ra = inst[1].submit(t, a) }()
		go func() { defer wg.Done(); rb = inst[2].submit(t, b) }()
		wg.Wait()
		return
	}
	ra, rb := run()
	statuses := map[wager.Status]int{ra.Status: 1}
	statuses[rb.Status]++
	if statuses[wager.StatusProcessed] != 1 || statuses[wager.StatusRejected] != 1 {
		t.Fatalf("outcomes: %+v %+v", ra, rb)
	}
	for _, r := range []*app.SubmitResult{ra, rb} {
		if r.Status == wager.StatusRejected && r.FailureCode != wager.FailureInsufficientBalance {
			t.Fatalf("rejected with %s", r.FailureCode)
		}
		if r.Balance.Amount() != "20.00" {
			t.Fatalf("balance in result %+v", r)
		}
	}
	// Resending both must not change anything.
	ra2, rb2 := run()
	if ra2.Status != ra.Status || rb2.Status != rb.Status || !ra2.IdempotentReplay || !rb2.IdempotentReplay {
		t.Fatalf("resend changed outcome: %+v %+v", ra2, rb2)
	}
	if got := inst[0].balance(t, w.ID()); got.Balance().Amount() != "20.00" {
		t.Fatalf("final balance %s", got.Balance())
	}
	if _, debits, n := ledgerStats(t, w.ID()); debits != 8000 || n != 2 {
		t.Fatalf("ledger: debits=%d entries=%d", debits, n)
	}
	assertReconciled(t, inst[0], w.ID())
}

func TestDistinctWalletsProgressInParallel(t *testing.T) {
	inst := instances(t, 3)
	const wallets, bets = 12, 8
	ws := make([]*wager.Wallet, wallets)
	for i := range ws {
		ws[i] = inst[i%3].openWallet(t, "1000.00")
	}
	var wg sync.WaitGroup
	for i, w := range ws {
		for j := 0; j < bets; j++ {
			wg.Add(1)
			go func(i, j int, w *wager.Wallet) {
				defer wg.Done()
				c := cmd(w, "BET", fmt.Sprintf("par-%s-%d-%d", runID, i, j), "10.00")
				if res, err := inst[(i+j)%3].wagers.Submit(context.Background(), c); err != nil || res.Status != wager.StatusProcessed {
					t.Errorf("wallet %d bet %d: %v %+v", i, j, err, res)
				}
			}(i, j, w)
		}
	}
	wg.Wait()
	for _, w := range ws {
		if got := inst[0].balance(t, w.ID()); got.Balance().Amount() != "920.00" || got.Version() != 1+bets {
			t.Fatalf("wallet %s: %s v%d", w.ID(), got.Balance(), got.Version())
		}
		assertReconciled(t, inst[0], w.ID())
	}
}
