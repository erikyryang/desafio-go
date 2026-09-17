package app

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/erikyryan/desafio-go/internal/domain/wager"
)

// PayloadHash computes the deterministic hash used to detect idempotency
// conflicts. Algorithm: SHA-256 over the canonical JSON (keys sorted, no
// whitespace, strings escaped by encoding/json) of the business fields:
//
//	externalTransactionId, gameId, kind, money{amount,currency}, playerId,
//	providerId, roundId, walletId and, for reversals only,
//	referenceExternalTransactionId.
//
// Normalizations applied before hashing: UUIDs are lower-cased canonical
// strings, money.amount is rendered with exactly two decimals ("25" and
// "25.00" hash identically) and the currency is upper-case. The idempotency
// key, message ids, correlation ids and other transport metadata are
// excluded, so HTTP and SQS produce the same hash for the same operation.
func PayloadHash(req wager.ExternalRequest) string {
	fields := map[string]any{
		"externalTransactionId": req.ExternalTransactionID,
		"gameId":                req.GameID,
		"kind":                  string(req.Kind),
		"money":                 map[string]string{"amount": req.Amount.Amount(), "currency": req.Amount.Currency().String()},
		"playerId":              req.PlayerID.String(),
		"providerId":            req.ProviderID,
		"roundId":               req.RoundID,
		"walletId":              req.WalletID.String(),
	}
	if req.ReferenceExternalTransactionID != "" {
		fields["referenceExternalTransactionId"] = req.ReferenceExternalTransactionID
	}
	b, err := json.Marshal(fields) // encoding/json sorts map keys
	if err != nil {
		panic(fmt.Sprintf("canonical json: %v", err)) // unreachable: only strings/maps
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
