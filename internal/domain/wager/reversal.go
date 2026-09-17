package wager

import (
	"fmt"

	"github.com/google/uuid"
)

// allowedReferenceKinds lists which kinds each reversal may reference.
var allowedReferenceKinds = map[Kind][]Kind{
	KindRefund:   {KindBet},
	KindRollback: {KindBet, KindWin, KindRefund},
}

// ReversalDirection returns the ledger direction produced by applying op
// against its (already validated) reference: a REFUND credits, a ROLLBACK
// performs the movement opposite to the reference.
func ReversalDirection(op Kind, reference Kind) (Direction, error) {
	switch op {
	case KindRefund:
		if reference == KindBet {
			return DirectionCredit, nil
		}
	case KindRollback:
		switch reference {
		case KindBet:
			return DirectionCredit, nil
		case KindWin, KindRefund:
			return DirectionDebit, nil
		}
	}
	return "", fmt.Errorf("%w: %s of %s", ErrReferenceKind, op, reference)
}

// ValidateReference checks that ref may be reversed by op. It returns the
// failure code to persist when the combination is rejected.
//
//   - REFUND may only reference a BET; ROLLBACK may reference BET, WIN or REFUND.
//   - Provider, player, wallet, currency and round must match.
//   - Amounts must be equal (no partial reversals).
//   - A reference with status PENDING/PENDING_REFERENCE is reported as
//     ErrReferencePending (caller keeps waiting); REJECTED/FAILED references
//     are rejected with REFERENCE_NOT_PROCESSED.
//   - alreadyReversed signals that the reference already has a PROCESSED
//     reversal; the caller obtains it from the repository under the wallet
//     lock and the database enforces the same rule with a partial unique index.
func ValidateReference(op, ref *WagerTransaction, alreadyReversed bool) (FailureCode, error) {
	if op == nil || ref == nil {
		return "", fmt.Errorf("%w: nil transaction", ErrInvalidArgument)
	}
	if !op.kind.IsReversal() {
		return "", fmt.Errorf("%w: %s is not a reversal", ErrInvalidArgument, op.kind)
	}
	if ref.id == op.id {
		return FailureReferenceMismatch, fmt.Errorf("%w: self reference", ErrReferenceMismatch)
	}
	switch ref.status {
	case StatusProcessed:
	case StatusPending, StatusPendingReference:
		return "", ErrReferencePending
	default:
		return FailureReferenceNotProcessed, fmt.Errorf("%w: reference status %s", ErrReferenceNotProcessd, ref.status)
	}
	kindOK := false
	for _, k := range allowedReferenceKinds[op.kind] {
		if ref.kind == k {
			kindOK = true
			break
		}
	}
	if !kindOK {
		return FailureReferenceKindMismatch, fmt.Errorf("%w: %s cannot reference %s", ErrReferenceKind, op.kind, ref.kind)
	}
	if ref.providerID != op.providerID || ref.playerID != op.playerID || ref.walletID != op.walletID ||
		ref.roundID != op.roundID || ref.amount.Currency() != op.amount.Currency() {
		return FailureReferenceMismatch, fmt.Errorf("%w: provider/player/wallet/currency/round differ", ErrReferenceMismatch)
	}
	if !ref.amount.Equal(op.amount) {
		return FailureReferenceAmountMismatch, fmt.Errorf("%w: reference %s, reversal %s", ErrReferenceAmount, ref.amount, op.amount)
	}
	if alreadyReversed {
		return FailureReferenceAlreadyReversed, fmt.Errorf("%w: %s", ErrAlreadyReversed, ref.id)
	}
	return "", nil
}

// ResolvedReference bundles the reference id with the resulting direction.
type ResolvedReference struct {
	ID        uuid.UUID
	Direction Direction
}
