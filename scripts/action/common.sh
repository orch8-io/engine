#!/usr/bin/env bash
# Shared helpers for the Orch8 GitHub Action (action.yml) diff/preview modes.
# Sourced, not executed. Requires: bash, jq, curl, python3, orch8 on PATH.
#
# Inputs (environment):
#   ORCH8_URL         API base URL including /api/v1
#   ORCH8_API_KEY     API key (sent as x-api-key)
#   ORCH8_TENANT_ID   tenant (sent as x-tenant-id)
#   ORCH8_STATE_DIR   scratch directory shared between action steps
#                     (defaults to $RUNNER_TEMP/orch8-action)

set -euo pipefail

ORCH8_STATE_DIR="${ORCH8_STATE_DIR:-${RUNNER_TEMP:-/tmp}/orch8-action}"
mkdir -p "$ORCH8_STATE_DIR"
# One sequence id per line; consumed by cleanup.sh.
ORCH8_CREATED_IDS="$ORCH8_STATE_DIR/created-sequence-ids"
touch "$ORCH8_CREATED_IDS"

orch8_log() { printf '%s\n' "$*" >&2; }

orch8_mask_secrets() {
  if [[ -n "${ORCH8_API_KEY:-}" ]]; then
    echo "::add-mask::${ORCH8_API_KEY}"
  fi
}

orch8_uuid() {
  python3 -c 'import uuid; print(uuid.uuid4())'
}

# Ephemeral namespace: orch8-pr-<pr>-<run_id>-<attempt> (or run-scoped
# when not triggered by a pull request).
orch8_ephemeral_namespace() {
  local pr="${ORCH8_PR_NUMBER:-}"
  local run="${GITHUB_RUN_ID:-local}"
  local attempt="${GITHUB_RUN_ATTEMPT:-1}"
  if [[ -n "$pr" ]]; then
    printf 'orch8-pr-%s-%s-%s' "$pr" "$run" "$attempt"
  else
    printf 'orch8-run-%s-%s' "$run" "$attempt"
  fi
}

# orch8_stamp SRC DEST NAMESPACE TENANT VERSION
# Stamps the server-assigned identity fields the way `orch8 sequence apply`
# does (id, version, created_at) and forces tenant/namespace so the copy
# lands in the ephemeral namespace instead of the real one.
orch8_stamp() {
  local src="$1" dest="$2" namespace="$3" tenant="$4" version="$5"
  local id ts
  id="$(orch8_uuid)"
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  jq --arg id "$id" --arg ns "$namespace" --arg tenant "$tenant" \
     --argjson ver "$version" --arg ts "$ts" \
     '. + {id: $id, namespace: $ns, tenant_id: $tenant, version: $ver, created_at: $ts}' \
     "$src" > "$dest"
}

# orch8_create FILE -> prints created sequence id; records it for cleanup.
orch8_create() {
  local file="$1" out id
  out="$(orch8 --output json sequence create --file "$file")"
  id="$(jq -r '.id // empty' <<<"$out")"
  if [[ -z "$id" ]]; then
    orch8_log "sequence create returned no id: $out"
    return 1
  fi
  printf '%s\n' "$id" >> "$ORCH8_CREATED_IDS"
  printf '%s' "$id"
}

# orch8_api METHOD PATH [JSON_BODY] -> response body on stdout; non-2xx fails.
orch8_api() {
  local method="$1" path="$2" body="${3:-}"
  local url="${ORCH8_URL%/}${path}"
  local args=(-sS --fail-with-body -X "$method" -H "accept: application/json")
  if [[ -n "${ORCH8_API_KEY:-}" ]]; then args+=(-H "x-api-key: ${ORCH8_API_KEY}"); fi
  if [[ -n "${ORCH8_TENANT_ID:-}" ]]; then args+=(-H "x-tenant-id: ${ORCH8_TENANT_ID}"); fi
  if [[ -n "$body" ]]; then args+=(-H "content-type: application/json" --data "$body"); fi
  curl "${args[@]}" "$url"
}

orch8_set_output() {
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    printf '%s=%s\n' "$1" "$2" >> "$GITHUB_OUTPUT"
  fi
}

# Escape a value for a Markdown table cell.
orch8_md_cell() {
  local v="${1//|/\\|}"
  v="${v//$'\n'/ }"
  printf '%s' "$v"
}

# orch8_upsert_pr_comment MARKER BODY_FILE
# Updates the PR comment containing MARKER in place, or creates one.
# Needs GH_TOKEN, GITHUB_REPOSITORY, ORCH8_PR_NUMBER.
orch8_upsert_pr_comment() {
  local marker="$1" body_file="$2" existing
  if [[ -z "${ORCH8_PR_NUMBER:-}" ]]; then
    orch8_log "not a pull_request event; skipping PR comment"
    return 0
  fi
  existing="$(gh api --paginate "repos/${GITHUB_REPOSITORY}/issues/${ORCH8_PR_NUMBER}/comments" \
    --jq '.[] | [.id, .body] | @json' \
    | MARKER="$marker" jq -r 'select(.[1] | contains(env.MARKER)) | .[0]' \
    | head -n1)"
  if [[ -n "$existing" ]]; then
    gh api -X PATCH "repos/${GITHUB_REPOSITORY}/issues/comments/${existing}" \
      -F "body=@${body_file}" --jq '.html_url'
  else
    gh api -X POST "repos/${GITHUB_REPOSITORY}/issues/${ORCH8_PR_NUMBER}/comments" \
      -F "body=@${body_file}" --jq '.html_url'
  fi
}

# Render a preflight report (JSON) as Markdown lines.
orch8_render_preflight() {
  local report="$1"
  jq -r '
    "**Preflight:** `\(.overall // "unknown")`",
    "",
    "| Check | Status | Summary |",
    "|---|---|---|",
    (.checks // [] | .[] |
      "| `\(.id)` | \(.status) | \((.summary // "") | gsub("\\|"; "\\|") | gsub("\n"; " "))\(if ((.findings // []) | length) > 0 then " (\((.findings | length)) finding(s))" else "" end) |")
  ' "$report"
}
