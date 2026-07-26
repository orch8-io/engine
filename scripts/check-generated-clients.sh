#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
gate_dir="$(mktemp -d)"
trap 'rm -rf "$gate_dir"' EXIT

cd "$repo_root"
cargo run --quiet -p orch8-api --example client_contract -- rust > "$gate_dir/client.rs"
rustc --edition=2024 "$gate_dir/client.rs" -o "$gate_dir/rust-client"
"$gate_dir/rust-client"

cargo run --quiet -p orch8-api --example client_contract -- javascript > "$gate_dir/client.js"
node --check "$gate_dir/client.js"
node "$gate_dir/client.js"

actual="$(cargo run --quiet -p orch8-api --example client_contract -- fingerprint)"
expected="$(tr -d '[:space:]' < orch8-api/openapi.sha256)"
if [[ "$actual" != "$expected" ]]; then
  echo "OpenAPI drift detected. Regenerate representative clients and update orch8-api/openapi.sha256." >&2
  echo "expected: $expected" >&2
  echo "actual:   $actual" >&2
  exit 1
fi
