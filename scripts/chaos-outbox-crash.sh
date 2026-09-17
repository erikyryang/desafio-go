#!/usr/bin/env bash
# Scenario 6: a publisher crashes between SendMessage and marking the outbox
# row as published. Another instance republishes after the lease expires with
# the same eventId; the FIFO broker deduplicates it.
source "$(dirname "$0")/lib.sh"
docker compose start app1 app2 app3 >/dev/null 2>&1; wait_healthy app1 app2 app3

say "stopping all instances so only the crashing publisher runs"
docker compose stop app1 app2 app3 >/dev/null
docker rm -f wager-chaos >/dev/null 2>&1 || true
docker compose run -d --no-deps --name wager-chaos -p 8090:8081 -e FAULT_INJECT=outbox-after-publish -e INSTANCE_ID=chaos -e CONSUMER_ENABLED=false app1 >/dev/null
for i in $(seq 1 20); do curl -sf localhost:8090/health/ready >/dev/null && break; sleep 1; done
INT=$(tok internal-service internal-service-secret)
say "opening a wallet (two outbox events) on the crashing instance"
read -r WID PID <<<"$(open_wallet http://localhost:8090 "$INT" 50.00)"
# (a long poll left open by a stopped instance may hold the message invisible for up to 30s)
for i in $(seq 1 70); do docker ps -a --format '{{.Names}} {{.Status}}' | grep wager-chaos | grep -q Exited && break; sleep 1; done
docker logs wager-chaos 2>&1 | grep -E 'fault injection' | cut -c1-200 || true
docker rm -f wager-chaos >/dev/null
say "outbox after the crash (published_at NULL, lease held by 'chaos')"
psql_q "SELECT id, event_type, attempts, locked_by, published_at IS NOT NULL AS published FROM outbox_events WHERE aggregate_id='$WID' OR aggregate_id IN (SELECT id FROM wager_transactions WHERE wallet_id='$WID')"
say "starting app1; it reclaims the abandoned lease after OUTBOX_LEASE (30s) and republishes with the same eventId"
docker compose start app1 app2 app3 >/dev/null; wait_healthy app1
for i in $(seq 1 45); do [ "$(psql_q "SELECT COUNT(*) FROM outbox_events WHERE (aggregate_id='$WID' OR aggregate_id IN (SELECT id FROM wager_transactions WHERE wallet_id='$WID')) AND published_at IS NULL")" = 0 ] && break; sleep 2; done
psql_q "SELECT id, event_type, attempts, locked_by, published_at IS NOT NULL AS published FROM outbox_events WHERE aggregate_id='$WID' OR aggregate_id IN (SELECT id FROM wager_transactions WHERE wallet_id='$WID')"
docker compose logs app1 app2 app3 --since 90s 2>/dev/null | grep -E '"event published"' | grep -E "$WID" | cut -c1-240 || true
echo "(the FIFO events queue deduplicates the republished eventId within 5 minutes)"
