#!/usr/bin/env bash
# Embed the repository license at the conventional AAR metadata path.
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: $0 <aar-path> <license-path>" >&2
    exit 2
fi

aar_path="$1"
license_path="$2"

[[ -f "$aar_path" ]] || { echo "AAR not found: $aar_path" >&2; exit 1; }
[[ -f "$license_path" ]] || { echo "license not found: $license_path" >&2; exit 1; }

license_dir="$(mktemp -d "${TMPDIR:-/tmp}/orch8-aar-license.XXXXXX")"
trap 'rm -rf -- "$license_dir"' EXIT
mkdir -p "$license_dir/META-INF"
cp "$license_path" "$license_dir/META-INF/LICENSE"

aar_dir="$(cd "$(dirname "$aar_path")" && pwd)"
aar_path="${aar_dir}/$(basename "$aar_path")"
(
    cd "$license_dir"
    zip -q "$aar_path" META-INF/LICENSE
)
unzip -Z1 "$aar_path" | grep -Fx 'META-INF/LICENSE'
