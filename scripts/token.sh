#!/usr/bin/env bash
# Prints an access token for a Keycloak client: scripts/token.sh provider-a provider-a-secret
set -euo pipefail
KC="${KEYCLOAK_URL:-http://localhost:8080}"
curl -sf -X POST "$KC/realms/wager/protocol/openid-connect/token" \
  -d grant_type=client_credentials -d "client_id=${1:?client id}" -d "client_secret=${2:?client secret}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])'
