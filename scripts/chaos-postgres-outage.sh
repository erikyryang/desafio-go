#!/usr/bin/env bash
# Temporary PostgreSQL outage: requests answer 503 (Retry-After), readiness
# goes DOWN, nothing is lost; after the outage the same request succeeds and
# the SQS message left in the queue is processed.
source "$(dirname "$0")/lib.sh"
docker compose start app1 app2 app3 >/dev/null 2>&1; wait_healthy app1 app2 app3
INT=$(tok internal-service internal-service-secret); PA=$(tok provider-a provider-a-secret)
read -r WID PID <<<"$(open_wallet "$APP1" "$INT" 100.00)"
EXT="outage-$(date +%s)"
say "pausing postgres"
docker compose pause postgres >/dev/null
echo "readiness: $(curl -s -o /dev/null -w '%{http_code}' "$APP1/health/ready")  (expected 503)"
echo -n "submit during outage: "; curl -s -w ' %{http_code}\n' -X POST "$APP1/wagering/transactions" -H "Authorization: Bearer $PA" -H 'Content-Type: application/json' -H "Idempotency-Key: provider-a:$EXT" \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$EXT\",\"playerId\":\"$PID\",\"walletId\":\"$WID\",\"roundId\":\"r\",\"gameId\":\"g\",\"kind\":\"BET\",\"money\":{\"amount\":\"10.00\",\"currency\":\"BRL\"}}"
send_message "msg-$EXT-sqs" "$WID" "$PID" "$EXT-sqs" 5.00
sleep 3
say "unpausing postgres"
docker compose unpause postgres >/dev/null
for i in $(seq 1 30); do [ "$(curl -s -o /dev/null -w '%{http_code}' "$APP1/health/ready")" = 200 ] && break; sleep 1; done
echo "readiness: $(curl -s -o /dev/null -w '%{http_code}' "$APP1/health/ready")"
echo -n "same submit after outage: "; submit "$APP1" "$PA" "$WID" "$PID" BET "$EXT" 10.00; echo
for i in $(seq 1 60); do [ "$(psql_q "SELECT COUNT(*) FROM wager_transactions WHERE external_transaction_id='$EXT-sqs' AND status='PROCESSED'")" = 1 ] && break; sleep 1; done
echo "sqs message sent during outage: $(psql_q "SELECT status FROM wager_transactions WHERE external_transaction_id='$EXT-sqs'") (expected PROCESSED)"
echo "balance: $(wallet_balance "$APP1" "$INT" "$WID") (expected 85.00 v3)"
