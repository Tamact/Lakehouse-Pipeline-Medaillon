#!/usr/bin/env bash
# =============================================================================
#  Declenche une ingestion NiFi via le endpoint HandleHttpRequest (port 9080).
#  Usage :
#    trigger_ingest.sh [snapshot_date] [domains] [cart_limit]
#  Exemples :
#    trigger_ingest.sh                         # aujourd'hui, 3 domaines
#    trigger_ingest.sh 2026-04-15 products,users,carts 3
# =============================================================================
set -euo pipefail

SNAPSHOT_DATE="${1:-$(date +%F)}"
DOMAINS="${2:-products,users,carts}"
CART_LIMIT="${3:-0}"
NIFI_INGEST_URL="${NIFI_INGEST_URL:-http://localhost:9080/ingest}"

echo "-> POST ${NIFI_INGEST_URL}?snapshot_date=${SNAPSHOT_DATE}&domains=${DOMAINS}&cart_limit=${CART_LIMIT}"
curl -sS -X POST \
  --get "${NIFI_INGEST_URL}" \
  --data-urlencode "snapshot_date=${SNAPSHOT_DATE}" \
  --data-urlencode "domains=${DOMAINS}" \
  --data-urlencode "cart_limit=${CART_LIMIT}" \
  -w "\nHTTP %{http_code}\n"
