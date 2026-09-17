package sqs

import (
	"strings"
	"testing"
)

const validBody = `{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z",
 "data":{"providerId":"provider-a","externalTransactionId":"transaction-123","idempotencyKey":"provider-a:transaction-123",
 "playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37","roundId":"round-987",
 "gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}}`

func TestParseEnvelope(t *testing.T) {
	env, cmd, err := parseEnvelope(validBody)
	if err != nil {
		t.Fatal(err)
	}
	if env.MessageID != "msg-123" || cmd.IdempotencyKey != "provider-a:transaction-123" || cmd.Amount != "25.00" || cmd.Kind != "BET" {
		t.Errorf("parsed: %+v %+v", env, cmd)
	}
	bad := []string{
		`{`,
		strings.Replace(validBody, `"messageId":"msg-123",`, "", 1),
		strings.Replace(validBody, "WagerTransactionRequested", "Other", 1),
		strings.Replace(validBody, `"amount":"25.00"`, `"amount":25.00`, 1),
		strings.Replace(validBody, `"idempotencyKey":"provider-a:transaction-123",`, "", 1),
	}
	for _, b := range bad {
		if _, _, err := parseEnvelope(b); err == nil {
			t.Errorf("accepted invalid body: %s", b)
		}
	}
}
