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

type transactionRepo struct{ tx pgx.Tx }

const transactionColumns = `id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
	provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
	reference_external_transaction_id, reference_transaction_id, failure_code, balance_after_minor,
	attempts, next_attempt_at, created_at, updated_at, processed_at`

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullUUID(id uuid.UUID) *uuid.UUID {
	if id == uuid.Nil {
		return nil
	}
	return &id
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}

func scanTransaction(row pgx.Row) (*wager.WagerTransaction, error) {
	var (
		s                                                                      wager.Snapshot
		origin, kind, status, currency                                         string
		amountMinor                                                            int64
		providerID, externalID, key, hash, roundID, gameID, refExtID, failCode *string
		refID                                                                  *uuid.UUID
		balanceAfter                                                           *int64
		nextAttemptAt, processedAt                                             *time.Time
	)
	err := row.Scan(&s.ID, &origin, &kind, &status, &s.WalletID, &s.PlayerID, &amountMinor, &currency,
		&providerID, &externalID, &key, &hash, &roundID, &gameID, &refExtID, &refID, &failCode, &balanceAfter,
		&s.Attempts, &nextAttemptAt, &s.CreatedAt, &s.UpdatedAt, &processedAt)
	if err != nil {
		return nil, translate(err)
	}
	s.Origin, s.Kind, s.Status = wager.Origin(origin), wager.Kind(kind), wager.Status(status)
	if s.Amount, err = money.FromMinor(amountMinor, money.Currency(currency)); err != nil {
		return nil, err
	}
	s.ProviderID, s.ExternalTransactionID, s.IdempotencyKey, s.PayloadHash = deref(providerID), deref(externalID), deref(key), deref(hash)
	s.RoundID, s.GameID, s.ReferenceExternalTransactionID = deref(roundID), deref(gameID), deref(refExtID)
	s.ReferenceTransactionID = deref(refID)
	s.FailureCode = wager.FailureCode(deref(failCode))
	if balanceAfter != nil {
		b, err := money.FromMinor(*balanceAfter, money.Currency(currency))
		if err != nil {
			return nil, err
		}
		s.BalanceAfter = &b
	}
	s.NextAttemptAt, s.ProcessedAt = deref(nextAttemptAt), deref(processedAt)
	return wager.RehydrateTransaction(s)
}

func (r *transactionRepo) Insert(ctx context.Context, t *wager.WagerTransaction) error {
	s := t.Snapshot()
	var balanceAfter *int64
	if s.BalanceAfter != nil {
		v := s.BalanceAfter.Minor()
		balanceAfter = &v
	}
	_, err := r.tx.Exec(ctx, `INSERT INTO wager_transactions (`+transactionColumns+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)`,
		s.ID, string(s.Origin), string(s.Kind), string(s.Status), s.WalletID, s.PlayerID, s.Amount.Minor(), string(s.Amount.Currency()),
		nullString(s.ProviderID), nullString(s.ExternalTransactionID), nullString(s.IdempotencyKey), nullString(s.PayloadHash),
		nullString(s.RoundID), nullString(s.GameID), nullString(s.ReferenceExternalTransactionID), nullUUID(s.ReferenceTransactionID),
		nullString(string(s.FailureCode)), balanceAfter, s.Attempts, nullTime(s.NextAttemptAt), s.CreatedAt, s.UpdatedAt, nullTime(s.ProcessedAt))
	return translate(err)
}

// Update persists the mutable outcome fields of a non-terminal transaction.
func (r *transactionRepo) Update(ctx context.Context, t *wager.WagerTransaction) error {
	s := t.Snapshot()
	var balanceAfter *int64
	if s.BalanceAfter != nil {
		v := s.BalanceAfter.Minor()
		balanceAfter = &v
	}
	tag, err := r.tx.Exec(ctx, `UPDATE wager_transactions SET status = $2, reference_transaction_id = $3, failure_code = $4,
		balance_after_minor = $5, attempts = $6, next_attempt_at = $7, updated_at = $8, processed_at = $9 WHERE id = $1`,
		s.ID, string(s.Status), nullUUID(s.ReferenceTransactionID), nullString(string(s.FailureCode)), balanceAfter,
		s.Attempts, nullTime(s.NextAttemptAt), s.UpdatedAt, nullTime(s.ProcessedAt))
	if err != nil {
		return translate(err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrNotFound
	}
	return nil
}

func (r *transactionRepo) GetByID(ctx context.Context, id uuid.UUID) (*wager.WagerTransaction, error) {
	return scanTransaction(r.tx.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions WHERE id = $1`, id))
}

func (r *transactionRepo) GetByIdempotencyKey(ctx context.Context, key string) (*wager.WagerTransaction, error) {
	return scanTransaction(r.tx.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions WHERE idempotency_key = $1`, key))
}

func (r *transactionRepo) GetByExternalID(ctx context.Context, providerID, externalID string) (*wager.WagerTransaction, error) {
	return scanTransaction(r.tx.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions
		WHERE provider_id = $1 AND external_transaction_id = $2`, providerID, externalID))
}

func (r *transactionRepo) HasProcessedReversal(ctx context.Context, referenceID uuid.UUID) (bool, error) {
	var exists bool
	err := r.tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM wager_transactions
		WHERE reference_transaction_id = $1 AND status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK'))`, referenceID).Scan(&exists)
	return exists, translate(err)
}

func (r *transactionRepo) ListDuePendingReferences(ctx context.Context, now time.Time, limit int) ([]uuid.UUID, error) {
	rows, err := r.tx.Query(ctx, `SELECT id FROM wager_transactions
		WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= $1 ORDER BY next_attempt_at LIMIT $2`, now, limit)
	if err != nil {
		return nil, translate(err)
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, translate(err)
		}
		ids = append(ids, id)
	}
	return ids, translate(rows.Err())
}

func (r *transactionRepo) GetForUpdateSkipLocked(ctx context.Context, id uuid.UUID) (*wager.WagerTransaction, error) {
	return scanTransaction(r.tx.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions
		WHERE id = $1 FOR UPDATE SKIP LOCKED`, id))
}
