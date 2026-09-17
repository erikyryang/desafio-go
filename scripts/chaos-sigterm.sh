#!/usr/bin/env bash
# Graceful shutdown: SIGTERM while messages are being consumed. The instance
# stops polling, drains in-flight work and exits; nothing is lost or duplicated.
source "$(dirname "$0")/lib.sh"
docker compose start app1 app2 app3 >/dev/null 2>&1; wait_healthy app1 app2 app3
INT=$(tok internal-service internal-service-secret)
read -r WID PID <<<"$(open_wallet "$APP1" "$INT" 1000.00)"
say "enqueueing 30 bets and sending SIGTERM to app1 while they are consumed"
for i in $(seq 1 30); do send_message "sig-$WID-$i" "$WID" "$PID" "sig-$WID-$i" 1.00 & done; wait
docker compose kill -s SIGTERM app1
docker compose logs app1 --since 30s --no-log-prefix 2>/dev/null | grep -E 'consumer stopped|workers stopped|http server stopped|postgres pool closed' | cut -c1-200 || true
say "remaining messages are consumed by app2/app3"
for i in $(seq 1 60); do [ "$(psql_q "SELECT COUNT(*) FROM wager_transactions WHERE wallet_id='$WID' AND kind='BET' AND status='PROCESSED'")" = 30 ] && break; sleep 1; done
docker compose start app1 >/dev/null
echo "processed bets: $(psql_q "SELECT COUNT(*) FROM wager_transactions WHERE wallet_id='$WID' AND kind='BET' AND status='PROCESSED'") (expected 30)"
echo "balance: $(wallet_balance "$APP2" "$INT" "$WID") (expected 970.00 v31)"
curl -sf -X POST "$APP2/wallets/$WID/reconciliation" -H "Authorization: Bearer $INT"; echo
