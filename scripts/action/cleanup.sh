#!/usr/bin/env bash
# Teardown for diff/preview modes: delete every sequence the run created.
# Runs under `if: always()`; never fails the job, only warns.

set -uo pipefail
# shellcheck source=scripts/action/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
set +e
orch8_mask_secrets
if [[ -n "${ORCH8_PREVIEW_TENANT_ID:-}" ]]; then
  export ORCH8_TENANT_ID="$ORCH8_PREVIEW_TENANT_ID"
fi

deleted=0
while IFS= read -r id; do
  [[ -z "$id" ]] && continue
  if orch8_api DELETE "/sequences/${id}" >/dev/null; then
    deleted=$((deleted + 1))
  else
    orch8_log "::warning::could not delete ephemeral sequence ${id}; remove it manually"
  fi
done < "$ORCH8_CREATED_IDS"
: > "$ORCH8_CREATED_IDS"
orch8_log "deleted ${deleted} ephemeral sequence(s)"
exit 0
