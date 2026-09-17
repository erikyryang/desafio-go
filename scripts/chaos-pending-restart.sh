#!/usr/bin/env bash
# Scenario 7/8: a REFUND arrives before its BET on app1, app1 is killed with
# SIGKILL, the BET arrives on app2 and a surviving instance resolves the
# pending reversal from the durable schedule in the database.
source "$(dirname "$0")/lib.sh"
docker compose start app1 app2 app3 >/dev/null 2>&1; wait_healthy app1 app2 app3
INT=$(tok internal-service internal-service-secret); PA=$(tok provider-a provider-a-secret)
read -r WID PID <<<"$(open_wallet "$APP1" "$INT" 100.00)"
EXT="late-bet-$(date +%s)"
say "REFUND before its reference on app1"
submit "$APP1" "$PA" "$WID" "$PID" REFUND "refund-$EXT" 40.00 "$EXT"; echo
say "killing app1 (SIGKILL)"
docker compose kill -s SIGKILL app1 >/dev/null
sleep 3
echo "pending transaction in the database:"
psql_q "SELECT id, status, attempts, next_attempt_at FROM wager_transactions WHERE external_transaction_id='refund-$EXT'"
say "BET arrives on app2"
submit "$APP2" "$PA" "$WID" "$PID" BET "$EXT" 40.00; echo
say "waiting for another instance to resolve the pending REFUND"
for i in $(seq 1 30); do [ "$(psql_q "SELECT status FROM wager_transactions WHERE external_transaction_id='refund-$EXT'")" = PROCESSED ] && break; sleep 1; done
psql_q "SELECT id, status, attempts, reference_transaction_id, balance_after_minor FROM wager_transactions WHERE external_transaction_id='refund-$EXT'"
docker compose logs app2 app3 --since 60s 2>/dev/null | grep '"pending reference retried"' | grep PROCESSED | cut -c1-260 || true
say "replay of the original REFUND request on app3 returns the final outcome"
submit http://localhost:8083 "$PA" "$WID" "$PID" REFUND "refund-$EXT" 40.00 "$EXT"; echo
docker compose start app1 >/dev/null
echo "balance: $(wallet_balance "$APP2" "$INT" "$WID") (expected 100.00 v3)"
curl -sf -X POST "$APP2/wallets/$WID/reconciliation" -H "Authorization: Bearer $INT"; echo
