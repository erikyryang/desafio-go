// Package wager holds the financial domain: wallets, ledger entries, wager
// transactions, reversal rules and domain events. It has no dependency on
// Fx, HTTP, SQS or persistence libraries.
package wager

import "errors"

// Domain errors. They are classifiable with errors.Is; the application layer
// maps them to failure codes, HTTP statuses and message outcomes.
var (
	ErrInvalidArgument      = errors.New("wager: invalid argument")
	ErrInsufficientBalance  = errors.New("wager: insufficient balance")
	ErrCurrencyMismatch     = errors.New("wager: currency mismatch")
	ErrPlayerMismatch       = errors.New("wager: wallet does not belong to player")
	ErrInvalidTransition    = errors.New("wager: invalid state transition")
	ErrInvalidLedgerEntry   = errors.New("wager: invalid ledger entry")
	ErrReferenceNotFound    = errors.New("wager: reference transaction not found")
	ErrReferencePending     = errors.New("wager: reference transaction still pending")
	ErrReferenceNotProcessd = errors.New("wager: reference transaction not processed")
	ErrReferenceMismatch    = errors.New("wager: reference does not match operation")
	ErrReferenceKind        = errors.New("wager: reference kind not allowed for reversal")
	ErrReferenceAmount      = errors.New("wager: reversal amount differs from reference")
	ErrAlreadyReversed      = errors.New("wager: reference already reversed")
)

// FailureCode is a stable, documented code persisted on rejected/failed
// transactions and returned to providers.
type FailureCode string

const (
	// Definitive business rejections.
	FailureInsufficientBalance         FailureCode = "INSUFFICIENT_BALANCE"          // BET without funds
	FailureReversalInsufficientBalance FailureCode = "REVERSAL_INSUFFICIENT_BALANCE" // ROLLBACK of WIN/REFUND that would go negative
	FailureReferenceNotFound           FailureCode = "REFERENCE_NOT_FOUND"           // reference never arrived (TTL / attempts exhausted)
	FailureReferenceNotProcessed       FailureCode = "REFERENCE_NOT_PROCESSED"       // reference ended REJECTED or FAILED
	FailureReferenceMismatch           FailureCode = "REFERENCE_MISMATCH"            // provider/player/wallet/currency/round differ
	FailureReferenceKindMismatch       FailureCode = "REFERENCE_KIND_MISMATCH"       // e.g. REFUND of a WIN
	FailureReferenceAmountMismatch     FailureCode = "REFERENCE_AMOUNT_MISMATCH"     // partial reversal
	FailureReferenceAlreadyReversed    FailureCode = "REFERENCE_ALREADY_REVERSED"    // second successful reversal
	FailureCurrencyMismatch            FailureCode = "CURRENCY_MISMATCH"             // money currency != wallet currency
	FailureWalletPlayerMismatch        FailureCode = "WALLET_PLAYER_MISMATCH"        // wallet belongs to another player
	// Permanent infrastructure failure recorded for audit.
	FailureInfrastructure FailureCode = "INFRASTRUCTURE_FAILURE"
)

// IsCorrectable reports whether the provider could fix the input and resend
// with a new externalTransactionId (true) or whether the rejection reflects a
// definitive financial outcome (false).
func (c FailureCode) IsCorrectable() bool {
	switch c {
	case FailureCurrencyMismatch, FailureWalletPlayerMismatch, FailureReferenceMismatch,
		FailureReferenceKindMismatch, FailureReferenceAmountMismatch, FailureReferenceNotFound:
		return true
	}
	return false
}
