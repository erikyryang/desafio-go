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

type walletRepo struct{ tx pgx.Tx }

const walletColumns = `id, player_id, currency, balance_minor, version, created_at, updated_at`

func scanWallet(row pgx.Row) (*wager.Wallet, error) {
	var (
		id, playerID        uuid.UUID
		currency            string
		balanceMinor        int64
		version             int64
		createdAt, updateAt time.Time
	)
	if err := row.Scan(&id, &playerID, &currency, &balanceMinor, &version, &createdAt, &updateAt); err != nil {
		return nil, translate(err)
	}
	balance, err := money.FromMinor(balanceMinor, money.Currency(currency))
	if err != nil {
		return nil, err
	}
	return wager.RehydrateWallet(id, playerID, balance, version, createdAt, updateAt)
}

func (r *walletRepo) Create(ctx context.Context, w *wager.Wallet) error {
	_, err := r.tx.Exec(ctx, `INSERT INTO wallets (`+walletColumns+`) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), string(w.Currency()), w.Balance().Minor(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	return translate(err)
}

func (r *walletRepo) Get(ctx context.Context, id uuid.UUID) (*wager.Wallet, error) {
	return scanWallet(r.tx.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1`, id))
}

func (r *walletRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*wager.Wallet, error) {
	return scanWallet(r.tx.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR UPDATE`, id))
}

// Update writes the new balance/version only when the stored version still
// equals expectedVersion (optimistic guard on top of the row lock).
func (r *walletRepo) Update(ctx context.Context, w *wager.Wallet, expectedVersion int64) error {
	tag, err := r.tx.Exec(ctx,
		`UPDATE wallets SET balance_minor = $1, version = $2, updated_at = $3 WHERE id = $4 AND version = $5`,
		w.Balance().Minor(), w.Version(), w.UpdatedAt(), w.ID(), expectedVersion)
	if err != nil {
		return translate(err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrConcurrentModification
	}
	return nil
}
