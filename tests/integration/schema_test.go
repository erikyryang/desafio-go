//go:build integration

package integration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/erikyryan/desafio-go/internal/adapters/postgres"
	"github.com/erikyryan/desafio-go/internal/app"
	"github.com/erikyryan/desafio-go/internal/domain/wager"
)

func TestMigrationsUpDownUp(t *testing.T) {
	ctx := context.Background()
	url, err := createDatabase(ctx, "wager_mig_"+runID)
	if err != nil {
		t.Fatal(err)
	}
	defer dropDatabase(ctx, "wager_mig_"+runID)
	if err := postgres.MigrateUp(url); err != nil {
		t.Fatal(err)
	}
	if v, dirty, _ := postgres.MigrationVersion(url); v != 1 || dirty {
		t.Fatalf("version %d dirty=%v", v, dirty)
	}
	if err := postgres.MigrateDown(url, 0); err != nil {
		t.Fatal(err)
	}
	if v, _, _ := postgres.MigrationVersion(url); v != 0 {
		t.Fatalf("after down: %d", v)
	}
	if err := postgres.MigrateUp(url); err != nil {
		t.Fatal(err)
	}
	if err := postgres.MigrateUp(url); err != nil { // idempotent
		t.Fatal(err)
	}
}

func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func TestSchemaConstraints(t *testing.T) {
	ctx := context.Background()
	s := servicesFor(pool, app.PendingReferencePolicy{})
	w := s.openWallet(t, "100.00")
	bet := s.submit(t, cmd(w, "BET", "c-bet", "10.00"))

	t.Run("negative balance rejected by CHECK", func(t *testing.T) {
		_, err := pool.Exec(ctx, `UPDATE wallets SET balance_minor = -1 WHERE id = $1`, w.ID())
		if sqlState(err) != "23514" {
			t.Fatalf("expected check violation, got %v", err)
		}
	})
	t.Run("ledger is immutable", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `UPDATE wallet_ledger_entries SET amount_minor = 1 WHERE wallet_id = $1`, w.ID()); sqlState(err) != "23001" {
			t.Fatalf("update: %v", err)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`, w.ID()); sqlState(err) != "23001" {
			t.Fatalf("delete: %v", err)
		}
	})
	t.Run("ledger unique per wallet and transaction", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before_minor, balance_after_minor, created_at)
			VALUES ($1, $2, $3, 'DEBIT', 1000, 'BRL', 9000, 8000, now())`, uuid.New(), w.ID(), bet.TransactionID)
		if sqlState(err) != "23505" {
			t.Fatalf("expected unique violation, got %v", err)
		}
		_, err = pool.Exec(ctx, `INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before_minor, balance_after_minor, created_at)
			VALUES ($1, $2, $3, 'DEBIT', 1000, 'BRL', 9000, 7000, now())`, uuid.New(), w.ID(), uuid.New())
		if sqlState(err) != "23503" && sqlState(err) != "23514" { // FK on transaction or arithmetic check
			t.Fatalf("expected constraint violation, got %v", err)
		}
	})
	t.Run("terminal transactions are immutable and never deleted", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'X' WHERE id = $1`, bet.TransactionID); sqlState(err) != "23001" {
			t.Fatalf("update terminal: %v", err)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM wager_transactions WHERE id = $1`, bet.TransactionID); sqlState(err) != "23001" {
			t.Fatalf("delete: %v", err)
		}
	})
	t.Run("one OPENING per wallet", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency, balance_after_minor, created_at, updated_at, processed_at)
			VALUES ($1, 'INTERNAL', 'OPENING', 'PROCESSED', $2, $3, 100, 'BRL', 100, now(), now(), now())`, uuid.New(), w.ID(), w.PlayerID())
		if sqlState(err) != "23505" {
			t.Fatalf("expected unique violation, got %v", err)
		}
	})
	t.Run("internal operation cannot carry provider fields", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency, provider_id, balance_after_minor, created_at, updated_at)
			VALUES ($1, 'INTERNAL', 'OPENING', 'PROCESSED', $2, $3, 100, 'BRL', 'p', 100, now(), now())`, uuid.New(), uuid.New(), w.PlayerID())
		if sqlState(err) != "23514" && sqlState(err) != "23503" {
			t.Fatalf("expected check violation, got %v", err)
		}
	})
	t.Run("one processed reversal per reference", func(t *testing.T) {
		ins := func(ext string) error {
			_, err := pool.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency, provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id, reference_external_transaction_id, reference_transaction_id, balance_after_minor, created_at, updated_at, processed_at)
				VALUES ($1, 'EXTERNAL', 'REFUND', 'PROCESSED', $2, $3, 1000, 'BRL', 'provider-a', $4, $4, 'h', 'round-1', 'game', 'c-bet', $5, 100, now(), now(), now())`,
				uuid.New(), w.ID(), w.PlayerID(), ext, bet.TransactionID)
			return err
		}
		if err := ins("rev-1-" + runID); err != nil {
			t.Fatal(err)
		}
		if err := ins("rev-2-" + runID); sqlState(err) != "23505" || !strings.Contains(err.Error(), "one_reversal_per_reference") {
			t.Fatalf("expected partial unique violation, got %v", err)
		}
	})
	t.Run("wallet unique per player and currency", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at) VALUES ($1, $2, 'BRL', 0, 1, now(), now())`, uuid.New(), w.PlayerID())
		if sqlState(err) != "23505" {
			t.Fatalf("expected unique violation, got %v", err)
		}
	})
	t.Run("outbox payload immutable", func(t *testing.T) {
		_, err := pool.Exec(ctx, `UPDATE outbox_events SET payload = '{}'::jsonb WHERE aggregate_id = $1`, bet.TransactionID)
		if sqlState(err) != "23001" {
			t.Fatalf("expected restrict violation, got %v", err)
		}
	})
}

func TestFinancialAtomicity(t *testing.T) {
	s := servicesFor(pool, app.PendingReferencePolicy{})
	w := s.openWallet(t, "100.00")
	boom := errors.New("crash before commit")
	c := cmd(w, "BET", "atomic-1", "10.00")
	err := s.uow.Do(context.Background(), func(ctx context.Context, st app.Store) error {
		if _, err := s.wagers.SubmitIn(ctx, st, c); err != nil {
			return err
		}
		return boom // interruption after every write, before commit
	})
	if !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if got := s.balance(t, w.ID()); got.Balance().Amount() != "100.00" || got.Version() != 1 {
		t.Fatalf("partial write survived: %s v%d", got.Balance(), got.Version())
	}
	if _, _, n := ledgerStats(t, w.ID()); n != 1 {
		t.Fatalf("ledger entries: %d", n)
	}
	var outbox int
	pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1`, w.ID()).Scan(&outbox)
	if outbox != 1 { // only the opening
		t.Fatalf("outbox rows: %d", outbox)
	}
	// The same operation can then be applied normally.
	if res := s.submit(t, c); res.Status != wager.StatusProcessed || res.IdempotentReplay {
		t.Fatalf("after rollback: %+v", res)
	}
	assertReconciled(t, s, w.ID())
	_ = time.Now
}
