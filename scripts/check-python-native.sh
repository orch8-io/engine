#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
native="${1:?pass the same-commit Python native library}"
work="$(mktemp -d "${TMPDIR:-/tmp}/orch8-python-runtime.XXXXXX")"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/orch8_engine"
cp "$root/packages/python-native/python/orch8_engine/"{__init__.py,__init__.pyi,py.typed} "$work/orch8_engine/"
suffix="$(python3 -c 'from importlib.machinery import EXTENSION_SUFFIXES; print(EXTENSION_SUFFIXES[0])')"
cp "$native" "$work/orch8_engine/_native$suffix"
PYTHONPATH="$work${PYTHONPATH:+:$PYTHONPATH}" python3 -m unittest discover \
  -s "$root/packages/python-native/tests" -p 'test_*.py' -v
