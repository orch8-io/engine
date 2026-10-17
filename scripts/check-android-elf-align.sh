#!/usr/bin/env bash
# Assert that built Android shared libraries have PT_LOAD segments aligned to
# at least 16 KB (required for 16 KB page-size devices / Google Play).
# Usage: scripts/check-android-elf-align.sh <path/to/lib.so> [...]
# Only 64-bit libraries (arm64-v8a, x86_64) are enforced; 32-bit ones are skipped.
set -euo pipefail

READELF="${READELF:-}"
if [[ -z "$READELF" ]]; then
  for c in llvm-readelf readelf greadelf; do
    if command -v "$c" >/dev/null 2>&1; then READELF="$c"; break; fi
  done
fi
[[ -n "$READELF" ]] || { echo "No readelf available." >&2; exit 1; }
(( $# > 0 )) || { echo "usage: $0 lib.so..." >&2; exit 2; }

fail=0
for so in "$@"; do
  hdr="$("$READELF" -h "$so")"
  if grep -q 'Class:.*ELF32' <<<"$hdr"; then
    echo "skip (32-bit): $so"; continue
  fi
  bad=0
  while read -r align; do
    if (( align < 16384 )); then bad=1; fi
  done < <("$READELF" -lW "$so" | awk '$1=="LOAD"{print $NF}')
  if (( bad )); then
    echo "FAIL: $so has a PT_LOAD segment aligned below 16 KB" >&2; fail=1
  else
    echo "ok: $so"
  fi
done
exit $fail
