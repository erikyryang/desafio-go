#!/usr/bin/env bash
# Shared helpers for the demo scripts.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

KC="${KEYCLOAK_URL:-http://localhost:8080}"
APP1="${APP1_URL:-http://localhost:8081}"
APP2="${APP2_URL:-http://localhost:8082}"
QUEUE_URL="http://localhost:4566/000000000000/wager-transactions.fifo"

tok() { curl -sf -X POST "$KC/realms/wager/protocol/openid-connect/token" -d grant_type=client_credentials -d "client_id=$1" -d "client_secret=$2" | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])'; }
jsonget() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
psql_q() { docker compose exec -T postgres psql -U wager -d wager -Atc "$1"; }
awslocal() { docker compose exec -T localstack awslocal "$@"; }
say() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

open_wallet() { # $1=base url $2=internal token $3=amount -> prints wallet id and player id
  curl -sf -X POST "$1/wallets" -H "Authorization: Bearer $2" -H 'Content-Type: application/json' \
    -d "{\"playerId\":\"$(python3 -c 'import uuid;print(uuid.uuid4())')\",\"initialBalance\":{\"amount\":\"$3\",\"currency\":\"BRL\"}}" \
    | jsonget 'd["id"]+" "+d["playerId"]'
}

wallet_balance() { curl -sf "$1/wallets/$3" -H "Authorization: Bearer $2" | jsonget 'd["balance"]["amount"]+" v"+str(d["version"])'; }

submit() { # $1=base url $2=provider token $3=wallet $4=player $5=kind $6=ext id $7=amount [$8=reference]
  local ref=""
  [ -n "${8:-}" ] && ref=",\"referenceExternalTransactionId\":\"$8\""
  curl -s -X POST "$1/wagering/transactions" -H "Authorization: Bearer $2" -H 'Content-Type: application/json' -H "Idempotency-Key: provider-a:$6" \
    -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$6\",\"playerId\":\"$4\",\"walletId\":\"$3\",\"roundId\":\"round-1\",\"gameId\":\"game\",\"kind\":\"$5\",\"money\":{\"amount\":\"$7\",\"currency\":\"BRL\"}$ref}"
}

send_message() { # $1=message id $2=wallet $3=player $4=ext id $5=amount
  local body="{\"messageId\":\"$1\",\"type\":\"WagerTransactionRequested\",\"occurredAt\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"data\":{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$4\",\"idempotencyKey\":\"provider-a:$4\",\"playerId\":\"$3\",\"walletId\":\"$2\",\"roundId\":\"round-1\",\"gameId\":\"game\",\"kind\":\"BET\",\"money\":{\"amount\":\"$5\",\"currency\":\"BRL\"}}}"
  awslocal sqs send-message --queue-url "$QUEUE_URL" --message-group-id "$2" --message-deduplication-id "$1-$(date +%s%N)" --message-body "$body" >/dev/null
}

wait_healthy() { # $@ = services
  for i in $(seq 1 60); do
    local ok=1
    for s in "$@"; do docker compose ps --format '{{.Health}}' "$s" | grep -q healthy || ok=0; done
    [ $ok = 1 ] && return 0
    sleep 2
  done
  echo "services not healthy: $*" >&2; return 1
}
