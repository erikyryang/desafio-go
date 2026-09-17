// Package postgres implements the application ports with pgx and explicit SQL.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/puddle/v2"

	"github.com/erikyryan/desafio-go/internal/app"
)

// NewPool opens a connection pool and verifies connectivity.
func NewPool(ctx context.Context, databaseURL string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.HealthCheckPeriod = 15 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// UnitOfWork implements app.UnitOfWork over a pgx pool.
type UnitOfWork struct {
	pool *pgxpool.Pool
}

// NewUnitOfWork wires the pool.
func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork { return &UnitOfWork{pool: pool} }

// Do runs fn in a READ COMMITTED transaction. Row locks (SELECT ... FOR
// UPDATE) provide the per-wallet serialization; the wallet UPDATE also checks
// the expected version, so a lost update is impossible even without the lock.
func (u *UnitOfWork) Do(ctx context.Context, fn func(ctx context.Context, s app.Store) error) error {
	return u.run(ctx, pgx.TxOptions{}, fn)
}

// DoReadOnlySnapshot runs fn in a REPEATABLE READ, READ ONLY transaction.
func (u *UnitOfWork) DoReadOnlySnapshot(ctx context.Context, fn func(ctx context.Context, s app.Store) error) error {
	return u.run(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, fn)
}

func (u *UnitOfWork) run(ctx context.Context, opts pgx.TxOptions, fn func(ctx context.Context, s app.Store) error) (err error) {
	tx, err := u.pool.BeginTx(ctx, opts)
	if err != nil {
		return translate(err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
			panic(p)
		}
		if err != nil {
			// Rollback must survive a cancelled context so the connection is reusable.
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	if err = fn(ctx, &store{tx: tx}); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return translate(err)
	}
	return nil
}

// store binds every repository to one transaction.
type store struct{ tx pgx.Tx }

func (s *store) Wallets() app.WalletRepository           { return &walletRepo{tx: s.tx} }
func (s *store) Transactions() app.TransactionRepository { return &transactionRepo{tx: s.tx} }
func (s *store) Ledger() app.LedgerRepository            { return &ledgerRepo{tx: s.tx} }
func (s *store) Outbox() app.OutboxRepository            { return &outboxRepo{tx: s.tx} }
func (s *store) Inbox() app.InboxRepository              { return &inboxRepo{tx: s.tx} }

// translate maps pgx/pgconn errors into the application error classes.
func translate(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return app.ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return fmt.Errorf("%w: %s", app.ErrUniqueViolation, pgErr.ConstraintName)
		case "40001", "40P01": // serialization failure, deadlock detected: retryable
			return fmt.Errorf("%w: %s", app.ErrUnavailable, pgErr.Message)
		case "57P01", "57P02", "57P03", "53300": // admin shutdown, crash shutdown, cannot connect now, too many connections
			return fmt.Errorf("%w: %s", app.ErrUnavailable, pgErr.Message)
		}
		if len(pgErr.Code) >= 2 && pgErr.Code[:2] == "08" { // connection exception class
			return fmt.Errorf("%w: %s", app.ErrUnavailable, pgErr.Message)
		}
		return err
	}
	if IsTransient(err) {
		return fmt.Errorf("%w: %v", app.ErrUnavailable, err)
	}
	return err
}

// IsTransient reports whether err looks like a temporary infrastructure failure.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, app.ErrUnavailable) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return true
	}
	if pgconn.SafeToRetry(err) {
		return true
	}
	return errors.Is(err, puddle.ErrClosedPool)
}
