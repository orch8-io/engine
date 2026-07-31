#!/usr/bin/env bash
set -euo pipefail

xcframework="${1:?usage: check-xcframework.sh <path-to-xcframework>}"

if [[ ! -d "$xcframework" || ! -f "$xcframework/Info.plist" ]]; then
  echo "Invalid XCFramework: $xcframework" >&2
  exit 1
fi

# CocoaPods requires all static-library slices in an XCFramework to have the
# same basename. A device liborch8_mobile.a paired with a simulator
# liborch8_mobile-sim.a is rejected even though SwiftPM can resolve it.
libraries=()
while IFS= read -r library; do
  libraries+=("$library")
done < <(find "$xcframework" -type f -name '*.a' -print | sort)

if [[ "${#libraries[@]}" -ne 2 ]]; then
  echo "Expected two static-library slices, found ${#libraries[@]}." >&2
  exit 1
fi

for library in "${libraries[@]}"; do
  if [[ "$(basename "$library")" != "liborch8_mobile.a" ]]; then
    echo "XCFramework slice has an incompatible basename: $library" >&2
    exit 1
  fi

  highest_min_version="$(otool -l "$library" | awk '
    $1 == "minos" {
      split($2, parts, ".")
      numeric = parts[1] * 1000 + parts[2]
      if (numeric > highest) {
        highest = numeric
        version = $2
      }
    }
    END { print version }
  ')"
  if [[ "$highest_min_version" != "16.0" ]]; then
    echo "Unexpected highest deployment target in $library: $highest_min_version" >&2
    exit 1
  fi
done

echo "XCFramework slices use liborch8_mobile.a and target iOS 16.0."
