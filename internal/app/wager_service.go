package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/erikyryan/desafio-go/internal/domain/money"
	"github.com/erikyryan/desafio-go/internal/domain/wager"
)

// Sources of a submission, used for metrics and logs.
const (
	SourceHTTP = "http"
	SourceSQS  = "sqs"
)

// PendingReferencePolicy controls retries of reversals waiting for their reference.
type PendingReferencePolicy struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	MaxAttempts    int
	TTL            time.Duration
}

// NextBackoff returns the exponential delay for the given attempt (1-based).
func (p PendingReferencePolicy) NextBackoff(attempt int) time.Duration {
	d := p.InitialBackoff
	for i := 1; i < attempt && d < p.MaxBackoff; i++ {
		d *= 2
	}
	if d > p.MaxBackoff {
		d = p.MaxBackoff
	}
	return d
}

// WagerService processes provider operations.
type WagerService struct {
	uow     UnitOfWork
	clock   Clock
	newID   IDGenerator
	metrics Metrics
	log     *slog.Logger
	pending PendingReferencePolicy
}

// NewWagerService wires the use case.
func NewWagerService(uow UnitOfWork, clock Clock, newID IDGenerator, metrics Metrics, log *slog.Logger, pending PendingReferencePolicy) *WagerService {
	if metrics == nil {
		metrics = NopMetrics{}
	}
	return &WagerService{uow: uow, clock: clock, newID: newID, metrics: metrics, log: log, pending: pending}
}

// SubmitCommand is the transport-independent input of Submit. Money fields
// are raw strings so that validation (and its error messages) is centralized.
type SubmitCommand struct {
	IdempotencyKey                 string
	ProviderID                     string
	ExternalTransactionID          string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           string
	Amount                         string
	Currency                       string
	ReferenceExternalTransactionID string

	Source        string
	CorrelationID string
	CausationID   string
}

// SubmitResult is what the transports return to the provider.
type SubmitResult struct {
	TransactionID    uuid.UUID
	Status           wager.Status
	Balance          *money.Money
	FailureCode      wager.FailureCode
	IdempotentReplay bool
}

func resultOf(t *wager.WagerTransaction, replay bool) *SubmitResult {
	return &SubmitResult{TransactionID: t.ID(), Status: t.Status(), Balance: t.BalanceAfter(), FailureCode: t.FailureCode(), IdempotentReplay: replay}
}

// toRequest validates the command into a domain request.
func (c SubmitCommand) toRequest() (wager.ExternalRequest, error) {
	var req wager.ExternalRequest
	if c.IdempotencyKey == "" {
		return req, fmt.Errorf("%w: idempotency key is required", ErrValidation)
	}
	kind, err := wager.ParseExternalKind(c.Kind)
	if err != nil {
		return req, fmt.Errorf("%w: %v", ErrValidation, err)
	}
	playerID, err := uuid.Parse(c.PlayerID)
	if err != nil || playerID == uuid.Nil {
		return req, fmt.Errorf("%w: playerId must be a UUID", ErrValidation)
	}
	walletID, err := uuid.Parse(c.WalletID)
	if err != nil || walletID == uuid.Nil {
		return req, fmt.Errorf("%w: walletId must be a UUID", ErrValidation)
	}
	amount, err := money.ParseNonNegative(c.Amount, c.Currency)
	if err != nil {
		return req, fmt.Errorf("%w: money: %v", ErrValidation, err)
	}
	req = wager.ExternalRequest{
		ProviderID: c.ProviderID, ExternalTransactionID: c.ExternalTransactionID,
		IdempotencyKey: c.IdempotencyKey, PlayerID: playerID, WalletID: walletID,
		RoundID: c.RoundID, GameID: c.GameID, Kind: kind, Amount: amount,
		ReferenceExternalTransactionID: c.ReferenceExternalTransactionID,
	}
	req.PayloadHash = PayloadHash(req)
	return req, nil
}

// Submit processes the operation in its own transaction.
func (s *WagerService) Submit(ctx context.Context, cmd SubmitCommand) (*SubmitResult, error) {
	var res *SubmitResult
	err := s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		r, err := s.SubmitIn(ctx, st, cmd)
		res = r
		return err
	})
	return res, err
}

// SubmitIn processes the operation inside the caller's transaction so that
// the SQS consumer can commit the inbox record atomically with the outcome.
//
// Flow: validate -> fast replay lookup -> lock wallet (FOR UPDATE) ->
// authoritative replay lookup -> apply kind rules -> persist transaction,
// ledger, wallet (optimistic version check) and outbox events.
func (s *WagerService) SubmitIn(ctx context.Context, st Store, cmd SubmitCommand) (*SubmitResult, error) {
	start := s.clock()
	req, err := cmd.toRequest()
	if err != nil {
		return nil, err
	}
	now := s.clock()
	tx, err := wager.NewExternalTransaction(s.newID(), req, now)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrValidation, err)
	}
	log := s.log.With("providerId", req.ProviderID, "externalTransactionId", req.ExternalTransactionID,
		"walletId", req.WalletID, "kind", req.Kind, "correlationId", cmd.CorrelationID, "source", cmd.Source)

	// Fast path: replays never wait for the wallet lock.
	if res, err := s.lookupReplay(ctx, st, req, cmd.Source, log); res != nil || err != nil {
		return res, err
	}
	w, err := st.Wallets().GetForUpdate(ctx, req.WalletID)
	if errors.Is(err, ErrNotFound) {
		return nil, ErrWalletNotFound
	}
	if err != nil {
		return nil, err
	}
	// Authoritative check under the wallet lock: concurrent duplicates of the
	// same wallet are serialized here, so the first writer wins and the others
	// observe its committed row.
	if res, err := s.lookupReplay(ctx, st, req, cmd.Source, log); res != nil || err != nil {
		return res, err
	}

	evCtx := wager.EventContext{CorrelationID: cmd.CorrelationID, CausationID: cmd.CausationID}
	res, err := s.process(ctx, st, w, tx, evCtx, now)
	if err != nil {
		if errors.Is(err, ErrUniqueViolation) {
			// Lost a race we could not serialize (e.g. same key, different wallet).
			s.metrics.IdempotencyConflict(cmd.Source)
			return nil, ErrIdempotencyConflict
		}
		return nil, err
	}
	s.metrics.TransactionOutcome(cmd.Source, tx.Kind(), tx.Status(), tx.FailureCode())
	s.metrics.ProcessingDuration(cmd.Source, s.clock().Sub(start))
	log.InfoContext(ctx, "wager transaction handled", "transactionId", tx.ID(), "status", tx.Status(), "failureCode", tx.FailureCode())
	return res, nil
}

// lookupReplay returns the persisted outcome when the operation was seen
// before, or a conflict when key/payload disagree.
func (s *WagerService) lookupReplay(ctx context.Context, st Store, req wager.ExternalRequest, source string, log *slog.Logger) (*SubmitResult, error) {
	existing, err := st.Transactions().GetByIdempotencyKey(ctx, req.IdempotencyKey)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	if existing == nil {
		// Under READ COMMITTED a concurrent commit may land between the two
		// lookups, so a row found here can still be a legitimate replay.
		existing, err = st.Transactions().GetByExternalID(ctx, req.ProviderID, req.ExternalTransactionID)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		if existing == nil {
			return nil, nil
		}
	}
	sameKey, sameHash := existing.Matches(req.IdempotencyKey, req.PayloadHash)
	switch {
	case !sameKey:
		// Same operation submitted with another key: never re-apply.
		s.metrics.IdempotencyConflict(source)
		log.WarnContext(ctx, "externalTransactionId reused with another idempotency key", "transactionId", existing.ID())
		return nil, ErrExternalIDConflict
	case !sameHash:
		s.metrics.IdempotencyConflict(source)
		log.WarnContext(ctx, "idempotency key reused with a different payload", "transactionId", existing.ID())
		return nil, ErrIdempotencyConflict
	}
	s.metrics.IdempotentReplay(source)
	log.InfoContext(ctx, "idempotent replay", "transactionId", existing.ID(), "status", existing.Status())
	return resultOf(existing, true), nil
}

// process applies the kind-specific rules to a locked wallet and persists
// the outcome. It is shared by the synchronous path and the pending worker.
func (s *WagerService) process(ctx context.Context, st Store, w *wager.Wallet, tx *wager.WagerTransaction, evCtx wager.EventContext, now time.Time) (*SubmitResult, error) {
	if !w.BelongsTo(tx.PlayerID()) {
		return s.reject(ctx, st, w, tx, wager.FailureWalletPlayerMismatch, evCtx, now)
	}
	if tx.Amount().Currency() != w.Currency() {
		return s.reject(ctx, st, w, tx, wager.FailureCurrencyMismatch, evCtx, now)
	}
	switch tx.Kind() {
	case wager.KindBet:
		if !w.CanDebit(tx.Amount()) {
			return s.reject(ctx, st, w, tx, wager.FailureInsufficientBalance, evCtx, now)
		}
		return s.move(ctx, st, w, tx, wager.DirectionDebit, uuid.Nil, evCtx, now)
	case wager.KindWin:
		return s.move(ctx, st, w, tx, wager.DirectionCredit, uuid.Nil, evCtx, now)
	case wager.KindLoss:
		return s.processWithoutMovement(ctx, st, w, tx, evCtx, now)
	case wager.KindRefund, wager.KindRollback:
		return s.processReversal(ctx, st, w, tx, evCtx, now)
	}
	return nil, fmt.Errorf("%w: unsupported kind %s", ErrValidation, tx.Kind())
}

func (s *WagerService) processReversal(ctx context.Context, st Store, w *wager.Wallet, tx *wager.WagerTransaction, evCtx wager.EventContext, now time.Time) (*SubmitResult, error) {
	ref, err := st.Transactions().GetByExternalID(ctx, tx.ProviderID(), tx.ReferenceExternalTransactionID())
	if errors.Is(err, ErrNotFound) {
		return s.park(ctx, st, tx, evCtx, now)
	}
	if err != nil {
		return nil, err
	}
	reversed, err := st.Transactions().HasProcessedReversal(ctx, ref.ID())
	if err != nil {
		return nil, err
	}
	code, err := wager.ValidateReference(tx, ref, reversed)
	if errors.Is(err, wager.ErrReferencePending) {
		return s.park(ctx, st, tx, evCtx, now)
	}
	if code != "" {
		return s.reject(ctx, st, w, tx, code, evCtx, now)
	}
	if err != nil {
		return nil, err
	}
	dir, err := wager.ReversalDirection(tx.Kind(), ref.Kind())
	if err != nil {
		return s.reject(ctx, st, w, tx, wager.FailureReferenceKindMismatch, evCtx, now)
	}
	if dir == wager.DirectionDebit && !w.CanDebit(tx.Amount()) {
		return s.reject(ctx, st, w, tx, wager.FailureReversalInsufficientBalance, evCtx, now)
	}
	return s.move(ctx, st, w, tx, dir, ref.ID(), evCtx, now)
}

// park persists (or reschedules) a reversal as PENDING_REFERENCE.
func (s *WagerService) park(ctx context.Context, st Store, tx *wager.WagerTransaction, evCtx wager.EventContext, now time.Time) (*SubmitResult, error) {
	isNew := tx.Status() == wager.StatusPending
	if !isNew {
		s.metrics.PendingReferenceRetry()
	}
	if err := tx.MarkPendingReference(now.Add(s.pending.NextBackoff(tx.Attempts()+1)), now); err != nil {
		return nil, err
	}
	if isNew {
		if err := st.Transactions().Insert(ctx, tx); err != nil {
			return nil, err
		}
		ev, err := wager.NewWagerTransactionPendingReference(s.newID(), tx, evCtx, now)
		if err != nil {
			return nil, err
		}
		if err := st.Outbox().Insert(ctx, ev); err != nil {
			return nil, err
		}
	} else if err := st.Transactions().Update(ctx, tx); err != nil {
		return nil, err
	}
	return resultOf(tx, false), nil
}

func (s *WagerService) persistOutcome(ctx context.Context, st Store, tx *wager.WagerTransaction) error {
	if tx.Attempts() == 0 {
		return st.Transactions().Insert(ctx, tx)
	}
	return st.Transactions().Update(ctx, tx)
}

func (s *WagerService) reject(ctx context.Context, st Store, w *wager.Wallet, tx *wager.WagerTransaction, code wager.FailureCode, evCtx wager.EventContext, now time.Time) (*SubmitResult, error) {
	balance := w.Balance()
	if err := tx.MarkRejected(code, &balance, now); err != nil {
		return nil, err
	}
	if err := s.persistOutcome(ctx, st, tx); err != nil {
		return nil, err
	}
	ev, err := wager.NewWagerTransactionRejected(s.newID(), tx, evCtx, now)
	if err != nil {
		return nil, err
	}
	if err := st.Outbox().Insert(ctx, ev); err != nil {
		return nil, err
	}
	return resultOf(tx, false), nil
}

func (s *WagerService) processWithoutMovement(ctx context.Context, st Store, w *wager.Wallet, tx *wager.WagerTransaction, evCtx wager.EventContext, now time.Time) (*SubmitResult, error) {
	if err := tx.MarkProcessed(w.Balance(), uuid.Nil, now); err != nil {
		return nil, err
	}
	if err := s.persistOutcome(ctx, st, tx); err != nil {
		return nil, err
	}
	ev, err := wager.NewWagerTransactionProcessed(s.newID(), tx, evCtx, now)
	if err != nil {
		return nil, err
	}
	if err := st.Outbox().Insert(ctx, ev); err != nil {
		return nil, err
	}
	return resultOf(tx, false), nil
}

// move applies a debit or credit and persists ledger, wallet, transaction and events.
func (s *WagerService) move(ctx context.Context, st Store, w *wager.Wallet, tx *wager.WagerTransaction, dir wager.Direction, ref uuid.UUID, evCtx wager.EventContext, now time.Time) (*SubmitResult, error) {
	expectedVersion := w.Version()
	var entry *wager.LedgerEntry
	var err error
	switch dir {
	case wager.DirectionDebit:
		entry, err = w.Debit(s.newID(), tx.ID(), tx.Amount(), now)
	case wager.DirectionCredit:
		entry, err = w.Credit(s.newID(), tx.ID(), tx.Amount(), now)
	}
	if err != nil {
		return nil, err
	}
	if err := tx.MarkProcessed(w.Balance(), ref, now); err != nil {
		return nil, err
	}
	if err := s.persistOutcome(ctx, st, tx); err != nil {
		return nil, err
	}
	if err := st.Ledger().Insert(ctx, entry); err != nil {
		return nil, err
	}
	if err := st.Wallets().Update(ctx, w, expectedVersion); err != nil {
		if errors.Is(err, ErrConcurrentModification) {
			s.metrics.ConcurrencyConflict()
		}
		return nil, err
	}
	processed, err := wager.NewWagerTransactionProcessed(s.newID(), tx, evCtx, now)
	if err != nil {
		return nil, err
	}
	changed, err := wager.NewWalletBalanceChanged(s.newID(), w, entry, evCtx, now)
	if err != nil {
		return nil, err
	}
	if err := st.Outbox().Insert(ctx, processed, changed); err != nil {
		return nil, err
	}
	return resultOf(tx, false), nil
}

// GetTransaction reads a transaction enforcing provider isolation.
func (s *WagerService) GetTransaction(ctx context.Context, actor Actor, id uuid.UUID) (*wager.WagerTransaction, error) {
	var t *wager.WagerTransaction
	err := s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		var err error
		t, err = st.Transactions().GetByID(ctx, id)
		return err
	})
	if errors.Is(err, ErrNotFound) {
		return nil, ErrTransactionNotFound
	}
	if err != nil {
		return nil, err
	}
	if !actor.CanSeeTransaction(t) {
		// Do not leak existence to other providers.
		return nil, ErrTransactionNotFound
	}
	return t, nil
}

// GetTransactionByExternalID reads a provider transaction by its external id.
func (s *WagerService) GetTransactionByExternalID(ctx context.Context, actor Actor, providerID, externalID string) (*wager.WagerTransaction, error) {
	if !actor.Internal && actor.ProviderID != providerID {
		return nil, ErrForbidden
	}
	var t *wager.WagerTransaction
	err := s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		var err error
		t, err = st.Transactions().GetByExternalID(ctx, providerID, externalID)
		return err
	})
	if errors.Is(err, ErrNotFound) {
		return nil, ErrTransactionNotFound
	}
	if err != nil {
		return nil, err
	}
	if !actor.CanSeeTransaction(t) {
		return nil, ErrTransactionNotFound
	}
	return t, nil
}

// ResolvePendingReferences is one worker iteration: it retries due
// PENDING_REFERENCE transactions, each in its own transaction, and returns
// how many were handled (processed, rejected or rescheduled).
func (s *WagerService) ResolvePendingReferences(ctx context.Context, batch int) (int, error) {
	var ids []uuid.UUID
	err := s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		var err error
		ids, err = st.Transactions().ListDuePendingReferences(ctx, s.clock(), batch)
		return err
	})
	if err != nil {
		return 0, err
	}
	handled := 0
	for _, id := range ids {
		if ctx.Err() != nil {
			return handled, ctx.Err()
		}
		if err := s.resolveOne(ctx, id); err != nil {
			s.log.ErrorContext(ctx, "pending reference retry failed", "transactionId", id, "error", err)
			if errors.Is(err, ErrUnavailable) {
				return handled, err
			}
			continue
		}
		handled++
	}
	return handled, nil
}

func (s *WagerService) resolveOne(ctx context.Context, id uuid.UUID) error {
	return s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		now := s.clock()
		tx, err := st.Transactions().GetForUpdateSkipLocked(ctx, id)
		if errors.Is(err, ErrNotFound) {
			return nil // taken by another instance
		}
		if err != nil {
			return err
		}
		if tx.Status() != wager.StatusPendingReference || tx.NextAttemptAt().After(now) {
			return nil
		}
		w, err := st.Wallets().GetForUpdate(ctx, tx.WalletID())
		if err != nil {
			return err
		}
		evCtx := wager.EventContext{CorrelationID: tx.ID().String(), CausationID: tx.ID().String()}
		expired := tx.Attempts() >= s.pending.MaxAttempts || now.Sub(tx.CreatedAt()) >= s.pending.TTL
		res, err := s.retryReversal(ctx, st, w, tx, evCtx, now, expired)
		if err != nil {
			return err
		}
		s.metrics.TransactionOutcome("worker", tx.Kind(), res.Status, res.FailureCode)
		s.log.InfoContext(ctx, "pending reference retried", "transactionId", tx.ID(), "walletId", tx.WalletID(),
			"providerId", tx.ProviderID(), "status", res.Status, "failureCode", res.FailureCode, "attempts", tx.Attempts())
		return nil
	})
}

// retryReversal re-runs the reversal rules; when the reference is still
// missing it reschedules or, once expired, rejects with REFERENCE_NOT_FOUND.
func (s *WagerService) retryReversal(ctx context.Context, st Store, w *wager.Wallet, tx *wager.WagerTransaction, evCtx wager.EventContext, now time.Time, expired bool) (*SubmitResult, error) {
	ref, err := st.Transactions().GetByExternalID(ctx, tx.ProviderID(), tx.ReferenceExternalTransactionID())
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	stillPending := ref == nil
	if ref != nil {
		reversed, err := st.Transactions().HasProcessedReversal(ctx, ref.ID())
		if err != nil {
			return nil, err
		}
		code, verr := wager.ValidateReference(tx, ref, reversed)
		switch {
		case errors.Is(verr, wager.ErrReferencePending):
			stillPending = true
		case code != "":
			return s.reject(ctx, st, w, tx, code, evCtx, now)
		case verr != nil:
			return nil, verr
		default:
			return s.process(ctx, st, w, tx, evCtx, now)
		}
	}
	if stillPending && expired {
		return s.reject(ctx, st, w, tx, wager.FailureReferenceNotFound, evCtx, now)
	}
	return s.park(ctx, st, tx, evCtx, now)
}
