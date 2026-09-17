#!/usr/bin/env bash
# Scenario 5: a consumer is killed after committing the message handling and
# before deleting the message from SQS. The redelivery must be deduplicated by
# the inbox and the wallet must be debited exactly once.
source "$(dirname "$0")/lib.sh"
docker compose start app1 app2 app3 >/dev/null 2>&1; wait_healthy app1 app2 app3

say "stopping app2/app3 so a single (crashing) consumer receives the message"
docker compose stop app2 app3 >/dev/null
INT=$(tok internal-service internal-service-secret)
read -r WID PID <<<"$(open_wallet "$APP1" "$INT" 100.00)"
echo "wallet $WID balance: $(wallet_balance "$APP1" "$INT" "$WID")"
docker compose stop app1 >/dev/null

say "starting a consumer with FAULT_INJECT=consumer-after-commit"
docker rm -f wager-chaos >/dev/null 2>&1 || true
docker compose run -d --no-deps --name wager-chaos -e FAULT_INJECT=consumer-after-commit -e INSTANCE_ID=chaos app1 >/dev/null
sleep 4
EXT="crash-$(date +%s)"
send_message "msg-$EXT" "$WID" "$PID" "$EXT" 30.00
say "waiting for the crash"
# (a long poll left open by a stopped instance may hold the message invisible for up to 30s)
for i in $(seq 1 70); do docker ps -a --format '{{.Names}} {{.Status}}' | grep wager-chaos | grep -q Exited && break; sleep 1; done
docker logs wager-chaos 2>&1 | grep -E 'message processed|fault injection' | cut -c1-220 || true
echo "balance after crash (committed): $(psql_q "SELECT balance_minor FROM wallets WHERE id='$WID'") minor units"
echo "message still in the queue (in flight): $(awslocal sqs get-queue-attributes --queue-url "$QUEUE_URL" --attribute-names ApproximateNumberOfMessagesNotVisible | jsonget 'd["Attributes"]["ApproximateNumberOfMessagesNotVisible"]')"
docker rm -f wager-chaos >/dev/null

say "restarting app1/app2/app3; the broker redelivers after the 30s visibility timeout"
docker compose start app1 app2 app3 >/dev/null; wait_healthy app1 app2 app3
for i in $(seq 1 45); do docker compose logs app1 app2 app3 --no-log-prefix --since 60s 2>/dev/null | grep -q '"duplicate message ignored"' && break; sleep 2; done
docker compose logs app1 app2 app3 --since 90s 2>/dev/null | grep -E 'duplicate message ignored' | cut -c1-200 || true
say "final state"
echo "balance: $(wallet_balance "$APP1" "$INT" "$WID")  (expected 70.00 v2)"
echo "ledger debits: $(psql_q "SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id='$WID' AND direction='DEBIT'") (expected 1)"
curl -sf -X POST "$APP1/wallets/$WID/reconciliation" -H "Authorization: Bearer $INT"; echo
