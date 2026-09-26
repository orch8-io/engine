#!/usr/bin/env bash
# mode=diff: semantic diff (base vs head) + preflight, posted as one PR
# comment that is updated in place on every push.
#
# Environment:
#   ORCH8_SEQUENCE_PATH   sequence JSON path in the repository (head version)
#   ORCH8_BASE_REF        git revision holding the baseline (default: PR base sha)
#   ORCH8_FAIL_ON         none|informational|behavioral|side_effect_risk|incompatible
#   ORCH8_FAIL_ON_PREFLIGHT  true|false
#   GH_TOKEN, GITHUB_REPOSITORY, ORCH8_PR_NUMBER  for the PR comment
#   ORCH8_COMMENT         true|false
# plus the variables documented in common.sh.

set -euo pipefail
# shellcheck source=scripts/action/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
orch8_mask_secrets

path="${ORCH8_SEQUENCE_PATH:?ORCH8_SEQUENCE_PATH is required}"
base_ref="${ORCH8_BASE_REF:-}"
fail_on="${ORCH8_FAIL_ON:-incompatible}"
fail_on_preflight="${ORCH8_FAIL_ON_PREFLIGHT:-true}"
tenant="${ORCH8_TENANT_ID:-default}"
namespace="$(orch8_ephemeral_namespace)"
state="$ORCH8_STATE_DIR"
orch8_set_output namespace "$namespace"

marker="<!-- orch8-sequence-diff:${path} -->"
body="$state/diff-comment.md"
diff_json="$state/diff.json"
preflight_json="$state/preflight.json"
max_severity="none"
diff_note=""

severity_rank() {
  case "$1" in
    informational) echo 1 ;;
    behavioral) echo 2 ;;
    side_effect_risk) echo 3 ;;
    incompatible) echo 4 ;;
    *) echo 0 ;;
  esac
}

# ---- Head copy in the ephemeral namespace ---------------------------------
orch8_stamp "$path" "$state/head.json" "$namespace" "$tenant" 2

# ---- Baseline ---------------------------------------------------------------
have_base=false
if [[ -z "$base_ref" ]]; then
  diff_note="No base revision was provided (not a pull_request event?), so no semantic diff was computed."
elif ! git cat-file -e "${base_ref}^{commit}" 2>/dev/null; then
  diff_note="Base revision \`${base_ref}\` is not available in the checkout. Use \`actions/checkout\` with \`fetch-depth: 0\`."
elif ! git cat-file -e "${base_ref}:${path}" 2>/dev/null; then
  diff_note="\`${path}\` does not exist at the base revision (new file), so there is nothing to diff against."
else
  git show "${base_ref}:${path}" > "$state/base.raw.json"
  orch8_stamp "$state/base.raw.json" "$state/base.json" "$namespace" "$tenant" 1
  have_base=true
fi

if [[ "$have_base" == true ]]; then
  base_id="$(orch8_create "$state/base.json")"
  head_id="$(orch8_create "$state/head.json")"
  orch8_api POST /sequences/releases/diff \
    "$(jq -nc --arg b "$base_id" --arg c "$head_id" \
        '{baseline_sequence_id: $b, candidate_sequence_id: $c}')" > "$diff_json"
  max_severity="$(jq -r '.max_severity // "none"' "$diff_json")"
fi
orch8_set_output max-severity "$max_severity"

# ---- Preflight on the head draft -------------------------------------------
preflight_rc=0
orch8 --output json sequence preflight --file "$state/head.json" > "$preflight_json" \
  || preflight_rc=$?
if jq -e '.overall' "$preflight_json" >/dev/null 2>&1; then
  preflight_overall="$(jq -r '.overall' "$preflight_json")"
else
  preflight_overall="error"
fi
orch8_set_output preflight-overall "$preflight_overall"

# ---- Render -----------------------------------------------------------------
{
  echo "$marker"
  echo "### Orch8 sequence diff: \`${path}\`"
  echo
  if [[ "$have_base" == true ]]; then
    count="$(jq '.entries | length' "$diff_json")"
    echo "**Max severity:** \`${max_severity}\` · **Changes:** ${count} · base \`${base_ref:0:12}\`"
    echo
    if [[ "$count" -gt 0 ]]; then
      jq -r '
        "| Severity | Category | Block | Summary |",
        "|---|---|---|---|",
        (.entries[] |
          "| \(.severity) | `\(.category)` | \(if .block_id then "`\(.block_id)`" else "-" end) | \(.summary | gsub("\\|"; "\\|") | gsub("\n"; " ")) |")
      ' "$diff_json"
      echo
    else
      echo "No semantic changes."
      echo
    fi
    if jq -e '(.candidate_lint // []) | length > 0' "$diff_json" >/dev/null; then
      echo "<details><summary>Candidate lint</summary>"
      echo
      jq -r '.candidate_lint[] | "- " + .' "$diff_json"
      echo
      echo "</details>"
      echo
    fi
  else
    echo "> ${diff_note}"
    echo
  fi
  if [[ "$preflight_overall" == "error" ]]; then
    echo "**Preflight:** request failed (exit ${preflight_rc}). See the action log."
  else
    orch8_render_preflight "$preflight_json"
  fi
  echo
  echo "<sub>Computed in the ephemeral namespace \`${namespace}\` (deleted after the run).</sub>"
} > "$body"

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  cat "$body" >> "$GITHUB_STEP_SUMMARY"
fi
if [[ "${ORCH8_COMMENT:-true}" == "true" ]]; then
  orch8_upsert_pr_comment "$marker" "$body" || orch8_log "::warning::could not post PR comment"
fi

# ---- Verdict ----------------------------------------------------------------
status=0
if [[ "$fail_on" != "none" && "$have_base" == true ]] \
   && (( $(severity_rank "$max_severity") >= $(severity_rank "$fail_on") )) \
   && (( $(severity_rank "$max_severity") > 0 )); then
  orch8_log "::error::semantic diff severity ${max_severity} reaches fail-on threshold ${fail_on}"
  status=1
fi
if [[ "$fail_on_preflight" == "true" && "$preflight_overall" != "pass" && "$preflight_overall" != "warning" ]]; then
  orch8_log "::error::preflight overall is ${preflight_overall}"
  status=1
fi
exit "$status"
