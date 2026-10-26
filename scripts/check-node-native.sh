#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
native="${1:?pass the same-commit Node native library}"
work="$(mktemp -d "${TMPDIR:-/tmp}/orch8-node-runtime.XXXXXX")"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/source" "$work/dist" "$work/runtime"
cp "$root/packages/node-native/"{package.json,index.js,index.d.ts,binding.js,host-errors.js,README.md} "$work/source/"
cp "$native" "$work/source/orch8_engine_native.node"
# Exercise the actual npm distribution, so forgetting a required helper in the
# package's explicit files list fails before any callback test can pass.
(cd "$work/source" && npm pack --ignore-scripts --silent --pack-destination "$work/dist")
tar -xzf "$work/dist/"*.tgz -C "$work/runtime"
test -s "$work/runtime/package/index.d.ts"
cp -R "$root/packages/node-native/test" "$work/runtime/package/test"
node --test "$work/runtime/package/test/"*.test.js
