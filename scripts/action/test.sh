#!/usr/bin/env bash
# Offline smoke test for the action scripts. Stubs `orch8`, `curl`, and `gh`
# on PATH, builds a throwaway git repo with a base and head sequence, and runs
# diff.sh, preview.sh, and cleanup.sh end to end.
#   bash scripts/action/test.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
bin="$work/bin"
mkdir -p "$bin"
calls="$work/calls.log"
: > "$calls"

cat > "$bin/orch8" <<'EOF'
#!/usr/bin/env bash
echo "orch8 $*" >> "$STUB_CALLS"
args=" $* "
if [[ "$args" == *" sequence create "* ]]; then
  n=$(grep -c "sequence create" "$STUB_CALLS")
  printf '{"id":"00000000-0000-4000-8000-00000000000%s"}\n' "$n"
elif [[ "$args" == *" sequence preflight "* ]]; then
  echo '{"sequence_name":"x","sequence_version":2,"overall":"warning","checks":[{"id":"definition_valid","status":"pass","summary":"ok"},{"id":"handlers_have_workers","status":"warning","summary":"no worker | for charge","findings":[{"message":"m"}]}],"generated_at":"2026-01-01T00:00:00Z"}'
elif [[ "$args" == *" test run "* ]]; then
  echo '<testsuites/>'
fi
EOF

cat > "$bin/curl" <<'EOF'
#!/usr/bin/env bash
echo "curl $*" >> "$STUB_CALLS"
if [[ " $* " == *"/sequences/releases/diff"* ]]; then
  echo '{"entries":[{"category":"handler_changed","severity":"behavioral","block_id":"charge","summary":"handler a -> b"}],"max_severity":"behavioral"}'
fi
EOF

cat > "$bin/gh" <<'EOF'
#!/usr/bin/env bash
echo "gh $*" >> "$STUB_CALLS"
if [[ " $* " == *"/issues/7/comments --jq"* || "$*" == *"--paginate"* ]]; then
  printf '%s\n' '[11,"unrelated"]' '[42,"<!-- orch8-sequence-diff:seq.json -->\nold"]'
else
  echo "https://example.invalid/comment"
fi
EOF
chmod +x "$bin"/*

repo="$work/repo"
mkdir -p "$repo"
cd "$repo"
git init -q
git config user.email t@example.invalid
git config user.name t
echo '{"name":"s","blocks":[{"type":"step","id":"charge","handler":"a","params":{}}]}' > seq.json
echo '{"cases":[]}' > seq.contracts.json
git add . && git commit -qm base
base="$(git rev-parse HEAD)"
echo '{"name":"s","blocks":[{"type":"step","id":"charge","handler":"b","params":{}}]}' > seq.json

export PATH="$bin:$PATH" STUB_CALLS="$calls"
export ORCH8_URL=http://orch8.invalid/api/v1 ORCH8_API_KEY=k ORCH8_TENANT_ID=acme
export GITHUB_REPOSITORY=o/r GITHUB_RUN_ID=99 GITHUB_RUN_ATTEMPT=1 GH_TOKEN=t
export ORCH8_PR_NUMBER=7 ORCH8_SEQUENCE_PATH=seq.json ORCH8_BASE_REF="$base"
export RUNNER_TEMP="$work/tmp" GITHUB_OUTPUT="$work/out" GITHUB_STEP_SUMMARY="$work/summary"
mkdir -p "$RUNNER_TEMP"

fail() { echo "FAIL: $*" >&2; exit 1; }

bash "$here/diff.sh" > /dev/null
grep -q "max-severity=behavioral" "$GITHUB_OUTPUT" || fail "max-severity output"
grep -q "preflight-overall=warning" "$GITHUB_OUTPUT" || fail "preflight output"
grep -q "namespace=orch8-pr-7-99-1" "$GITHUB_OUTPUT" || fail "namespace output"
grep -q "PATCH repos/o/r/issues/comments/42" "$calls" || fail "comment not updated in place"
# shellcheck disable=SC2016 # literal Markdown backticks
grep -q '| behavioral | `handler_changed` | `charge` | handler a -> b |' "$work/tmp/orch8-action/diff-comment.md" || fail "diff table"
grep -q 'no worker \\| for charge' "$work/tmp/orch8-action/diff-comment.md" || fail "pipe escaping"
jq -e '.namespace == "orch8-pr-7-99-1" and .tenant_id == "acme" and .version == 2' \
  "$work/tmp/orch8-action/head.json" >/dev/null || fail "stamping"
[[ "$(wc -l < "$work/tmp/orch8-action/created-sequence-ids")" -eq 2 ]] || fail "created ids"

ORCH8_FAIL_ON=behavioral bash "$here/diff.sh" > /dev/null && fail "fail-on behavioral should fail"

bash "$here/cleanup.sh"
[[ "$(grep -c -- '-X DELETE' "$calls")" -eq 4 ]] || fail "cleanup deletes"

# New file: no baseline, no diff API call.
git rm -q --cached seq.json 2>/dev/null || true
before="$(grep -c releases/diff "$calls")"
ORCH8_BASE_REF="$base" ORCH8_SEQUENCE_PATH=new.json bash -c 'cp seq.json new.json; bash "$0/diff.sh"' "$here" > /dev/null
[[ "$(grep -c releases/diff "$calls")" -eq "$before" ]] || fail "new file should skip diff"
grep -q "does not exist at the base revision" "$work/tmp/orch8-action/diff-comment.md" || fail "new-file note"
bash "$here/cleanup.sh"

ORCH8_CONTRACTS='**/*.contracts.json' bash "$here/preview.sh" > /dev/null
grep -q "orch8 test run seq.contracts.json --report junit" "$calls" || fail "test run"
grep -q "contract-suites=1" "$GITHUB_OUTPUT" || fail "suite count"
[[ -f "$work/tmp/orch8-action/junit/seq.contracts.xml" ]] || fail "junit file"
bash "$here/cleanup.sh"

echo "action script tests passed"
