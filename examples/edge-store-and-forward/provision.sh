#!/usr/bin/env bash
# Load the store's workflows, credential and schedule into the store's SQLite
# database through the temporary `store-provision` node (loopback only).
# Re-runnable: each run uploads the sequence as a new version and replaces the
# cron schedule (its id is remembered in .provisioned-cron-id).
#
# Required env: STORE_API_KEY STORE_ID POS_EXPORT_URL HQ_URL HQ_TENANT
#               HQ_SEQUENCE_ID HQ_FORWARDER_KEY
# Optional:     STORE_TENANT (default: store) STORE_TZ (default: UTC)
#               EOD_CRON (default: "0 30 23 * * * *" = 23:30 daily)
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
: "${STORE_API_KEY:?}" "${STORE_ID:?}" "${POS_EXPORT_URL:?}" "${HQ_URL:?}" \
  "${HQ_TENANT:?}" "${HQ_SEQUENCE_ID:?}" "${HQ_FORWARDER_KEY:?}"
TENANT="${STORE_TENANT:-store}"
TZ_NAME="${STORE_TZ:-UTC}"
CRON="${EOD_CRON:-0 30 23 * * * *}"
API="http://127.0.0.1:18080/api/v1"
H=(-H "x-api-key: $STORE_API_KEY" -H "x-tenant-id: $TENANT" -H "content-type: application/json")

for _ in $(seq 1 30); do curl -fsS "http://127.0.0.1:18080/health/ready" >/dev/null 2>&1 && break; sleep 1; done

# 1. Credential for the HQ forwarder key (encrypted at rest with STORE_ENCRYPTION_KEY).
jq -n --arg v "$HQ_FORWARDER_KEY" --arg t "$TENANT" \
  '{id:"hq-forwarder", name:"HQ forwarder key", kind:"api_key", value:$v, tenant_id:$t}' |
  curl -fsS -X POST "$API/credentials" "${H[@]}" -d @- >/dev/null ||
  jq -n --arg v "$HQ_FORWARDER_KEY" '{value:$v}' |
  curl -fsS -X PATCH "$API/credentials/hq-forwarder" "${H[@]}" -d @- >/dev/null

# 2. The forwarding workflow, with this store's values substituted.
seq_json="$(sed -e "s|__POS_EXPORT_URL__|$POS_EXPORT_URL|g" -e "s|__HQ_URL__|$HQ_URL|g" \
  -e "s|__HQ_TENANT__|$HQ_TENANT|g" -e "s|__HQ_SEQUENCE_ID__|$HQ_SEQUENCE_ID|g" \
  -e "s|__STORE_ID__|$STORE_ID|g" "$here/sequences/store-eod-forward.json")"
seq_id="$(echo "$seq_json" | jq --arg id "$(uuidgen | tr 'A-Z' 'a-z')" --arg t "$TENANT" \
  --argjson v "$(date +%s)" \
  '. + {id:$id, tenant_id:$t, namespace:"default", version:$v, created_at:(now|todate)}' |
  curl -fsS -X POST "$API/sequences" "${H[@]}" -d @- | jq -r .id)"
echo "sequence store-eod-forward -> $seq_id"

# 3. Nightly schedule (the only way work enters an edge node: it has no API).
state="$here/.provisioned-cron-id"
body="$(jq -n --arg t "$TENANT" --arg s "$seq_id" --arg c "$CRON" --arg z "$TZ_NAME" \
  '{tenant_id:$t, namespace:"default", sequence_id:$s, cron_expr:$c, timezone:$z, overlap_policy:"skip"}')"
# A schedule is bound to one sequence id, so re-provisioning replaces it.
if [ -s "$state" ]; then
  curl -fsS -X DELETE "$API/cron/$(cat "$state")" "${H[@]}" >/dev/null || true
fi
curl -fsS -X POST "$API/cron" "${H[@]}" -d "$body" | jq -r .id > "$state"
echo "cron $(cat "$state") -> store-eod-forward $seq_id ($CRON $TZ_NAME)"
