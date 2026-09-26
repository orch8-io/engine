#!/usr/bin/env bash
# Build the orch8-wasm playground module and copy the wasm-bindgen output
# into the website (default: ../web/public/wasm/orch8 next to this repo).
#
# Requirements:
#   rustup target add wasm32-unknown-unknown
#   cargo install wasm-bindgen-cli --version <wasm-bindgen version in Cargo.lock>
#   (optional) wasm-opt from binaryen — used when found on PATH
#
# Usage: scripts/build-wasm-playground.sh [OUT_DIR]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${1:-$REPO_ROOT/../web/public/wasm/orch8}"
TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"

want_version="$(awk '/^name = "wasm-bindgen"$/ { getline; gsub(/version = |"/, ""); print; exit }' "$REPO_ROOT/Cargo.lock")"
if ! command -v wasm-bindgen >/dev/null 2>&1; then
  echo "wasm-bindgen not found; install with: cargo install wasm-bindgen-cli --version $want_version" >&2
  exit 1
fi
have_version="$(wasm-bindgen --version | awk '{ print $2 }')"
if [ "$have_version" != "$want_version" ]; then
  echo "wasm-bindgen CLI $have_version does not match the crate version $want_version in Cargo.lock" >&2
  exit 1
fi

cargo build --manifest-path "$REPO_ROOT/Cargo.toml" -p orch8-wasm \
  --target wasm32-unknown-unknown --release

mkdir -p "$OUT_DIR"
wasm-bindgen --target web --no-typescript --out-dir "$OUT_DIR" \
  "$TARGET_DIR/wasm32-unknown-unknown/release/orch8_wasm.wasm"

if command -v wasm-opt >/dev/null 2>&1; then
  wasm-opt -Oz --enable-bulk-memory --enable-nontrapping-float-to-int \
    -o "$OUT_DIR/orch8_wasm_bg.wasm" "$OUT_DIR/orch8_wasm_bg.wasm"
fi

echo "wrote $OUT_DIR:"
ls -l "$OUT_DIR"
