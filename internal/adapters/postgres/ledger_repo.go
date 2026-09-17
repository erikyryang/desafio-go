package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/erikyryan/desafio-go/internal/app"
	"github.com/erikyryan/desafio-go/internal/domain/money"
	"github.com/erikyryan/desafio-go/internal/domain/wager"
)

type ledgerRepo struct{ tx pgx.Tx }

func (r *ledgerRepo) Insert(ctx context.Context, e *wager.LedgerEntry) error {
	_, err := r.tx.Exec(ctx, `INSERT INTO wallet_ledger_entries
		(id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before_minor, balance_after_minor, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()), e.Amount().Minor(), string(e.Amount().Currency()),
		e.BalanceBefore().Minor(), e.BalanceAfter().Minor(), e.CreatedAt())
	return translate(err)
}

func (r *ledgerRepo) List(ctx context.Context, walletID uuid.UUID, afterSequence int64, limit int) (app.LedgerPage, error) {
	rows, err := r.tx.Query(ctx, `SELECT id, wallet_id, transaction_id, direction, amount_minor, currency,
		balance_before_minor, balance_after_minor, created_at, sequence
		FROM wallet_ledger_entries WHERE wallet_id = $1 AND sequence > $2 ORDER BY sequence LIMIT $3`,
		walletID, afterSequence, limit+1)
	if err != nil {
		return app.LedgerPage{}, translate(err)
	}
	defer rows.Close()
	var page app.LedgerPage
	for rows.Next() {
		var (
			id, wID, txID      uuid.UUID
			dir, currency      string
			amt, before, after int64
			createdAt          time.Time
			seq                int64
		)
		if err := rows.Scan(&id, &wID, &txID, &dir, &amt, &currency, &before, &after, &createdAt, &seq); err != nil {
			return app.LedgerPage{}, translate(err)
		}
		c := money.Currency(currency)
		amount, _ := money.FromMinor(amt, c)
		bBefore, _ := money.FromMinor(before, c)
		bAfter, _ := money.FromMinor(after, c)
		entry, err := wager.RehydrateLedgerEntry(id, wID, txID, wager.Direction(dir), amount, bBefore, bAfter, createdAt, seq)
		if err != nil {
			return app.LedgerPage{}, err
		}
		page.Entries = append(page.Entries, entry)
	}
	if err := rows.Err(); err != nil {
		return app.LedgerPage{}, translate(err)
	}
	if len(page.Entries) > limit {
		page.Entries = page.Entries[:limit]
		page.NextCursor = page.Entries[limit-1].Sequence()
	}
	return page, nil
}

func (r *ledgerRepo) Totals(ctx context.Context, walletID uuid.UUID, currency money.Currency) (app.LedgerTotals, error) {
	var credits, debits, count int64
	err := r.tx.QueryRow(ctx, `SELECT
		COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'CREDIT'), 0)::BIGINT,
		COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'DEBIT'), 0)::BIGINT,
		COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&credits, &debits, &count)
	if err != nil {
		return app.LedgerTotals{}, translate(err)
	}
	c, err := money.FromMinor(credits, currency)
	if err != nil {
		return app.LedgerTotals{}, err
	}
	d, err := money.FromMinor(debits, currency)
	if err != nil {
		return app.LedgerTotals{}, err
	}
	return app.LedgerTotals{Credits: c, Debits: d, Count: count}, nil
}
