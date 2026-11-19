#!/bin/sh
set -eu

example=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
workspace=$(mktemp -d)
trap 'rm -rf "$workspace"' EXIT
cp "$example"/sequence.json "$example"/fallback.json "$example"/worker.py "$example"/worker.ts "$workspace/"
cd "$workspace"
ORCH8_RELEASE_BEFORE_EFFECT=1 "${ORCH8_BIN:-orch8}" dev --once --tenant-id default --api-key "" "${1:-sequence.json}" \
  --port "${ORCH8_SANDBOX_PORT:-8080}" \
  --worker 'node --experimental-strip-types worker.ts' \
  --worker 'python3 worker.py'
