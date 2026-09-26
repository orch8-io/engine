#!/usr/bin/env bash
# mode=preview: publish the head sequence into an ephemeral namespace, run the
# server-side preflight against the stored copy, and run the offline contract
# suites with `orch8 test run`. Teardown happens in cleanup.sh (if: always()).
#
# Isolation note: the namespace is a logical scope inside the tenant, not a
# separate tenant. Set ORCH8_PREVIEW_TENANT_ID to use a different tenant id,
# which only works if the API key is allowed to act for that tenant.
#
# Environment:
#   ORCH8_SEQUENCE_PATH      sequence JSON path
#   ORCH8_CONTRACTS          glob of contract suites (e.g. "sequences/**/*.contracts.json")
#   ORCH8_PREVIEW_TENANT_ID  optional tenant override
# plus the variables documented in common.sh.

set -euo pipefail
# shellcheck source=scripts/action/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
orch8_mask_secrets

path="${ORCH8_SEQUENCE_PATH:?ORCH8_SEQUENCE_PATH is required}"
if [[ -n "${ORCH8_PREVIEW_TENANT_ID:-}" ]]; then
  export ORCH8_TENANT_ID="$ORCH8_PREVIEW_TENANT_ID"
fi
tenant="${ORCH8_TENANT_ID:-default}"
namespace="$(orch8_ephemeral_namespace)"
state="$ORCH8_STATE_DIR"
junit_dir="$state/junit"
mkdir -p "$junit_dir"
orch8_set_output namespace "$namespace"
orch8_set_output junit-dir "$junit_dir"
status=0

orch8_stamp "$path" "$state/preview.json" "$namespace" "$tenant" 1
seq_id="$(orch8_create "$state/preview.json")"
orch8_log "created preview sequence ${seq_id} in ${tenant}/${namespace}"

preflight_json="$state/preview-preflight.json"
if orch8 --output json sequence preflight --id "$seq_id" > "$preflight_json"; then
  :
else
  status=1
fi
overall="$(jq -r '.overall // "error"' "$preflight_json" 2>/dev/null || echo error)"
orch8_set_output preflight-overall "$overall"
orch8_log "server-side preflight: ${overall}"

# Contract suites run fully offline: handlers are mocked and time is virtual.
# They do not execute against the preview namespace.
ran=0
if [[ -n "${ORCH8_CONTRACTS:-}" ]]; then
  # python3 glob gives `**` recursion independent of the bash version.
  suites=()
  while IFS= read -r suite; do
    suites+=("$suite")
  done < <(python3 -c 'import glob, sys; print("\n".join(sorted(glob.glob(sys.argv[1], recursive=True))))' "$ORCH8_CONTRACTS")
  for suite in ${suites[@]+"${suites[@]}"}; do
    [[ -z "$suite" ]] && continue
    ran=$((ran + 1))
    report="$junit_dir/$(basename "${suite%.json}").xml"
    orch8_log "orch8 test run ${suite}"
    if ! orch8 test run "$suite" --report junit > "$report"; then
      orch8_log "::error file=${suite}::contract suite failed"
      status=1
    fi
  done
  if [[ "$ran" -eq 0 ]]; then
    orch8_log "::warning::no contract suites matched '${ORCH8_CONTRACTS}'"
  fi
fi
orch8_set_output contract-suites "$ran"

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    echo "### Orch8 preview: \`${path}\`"
    echo
    echo "Namespace \`${namespace}\` in tenant \`${tenant}\` (torn down after the run)."
    echo
    if [[ "$overall" != "error" ]]; then orch8_render_preflight "$preflight_json"; fi
    echo
    echo "Contract suites run (offline): ${ran}"
  } >> "$GITHUB_STEP_SUMMARY"
fi
exit "$status"
