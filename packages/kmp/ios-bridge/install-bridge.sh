#!/usr/bin/env bash
# Install the Orch8 KMP iOS bridge (Orch8KmpBridge.swift) into an iOS app.
#
# Usage:
#   install-bridge.sh <destination-dir> [shared-framework-module] [version]
#
#   destination-dir          folder inside your iOS app target, e.g. iosApp/iosApp
#   shared-framework-module  the Kotlin framework your app imports (default: Orch8Kmp).
#                            If your shared KMP module depends on io.orch8:orch8-kmp
#                            and exports it, pass that module's framework baseName
#                            (e.g. Shared).
#   version                  engine/SDK version (default: 0.7.1). Must match the
#                            io.orch8:orch8-kmp and Orch8Mobile versions you use.
#
# The file is downloaded from the engine GitHub release for that version and
# its SHA-256 is verified against the published .sha256 asset, so the bridge
# always matches the protocol version of the Kotlin library.
#
# Why this is a file and not a Swift package or pod: the bridge implements
# Kotlin protocols (Orch8JsonBridgeFactory) that live in *your* shared
# framework, whose module name is only known in your project. A prebuilt
# package would need its own copy of the Kotlin framework and runtime, and its
# types would not be the ones your shared code checks for.
set -euo pipefail

dest="${1:?usage: install-bridge.sh <destination-dir> [shared-framework-module] [version]}"
module="${2:-Orch8Kmp}"
version="${3:-0.7.1}"
version="${version#v}"
tag="v${version}"

if [[ ! "$module" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
  echo "error: '${module}' is not a valid Swift module name" >&2
  exit 1
fi

base="https://github.com/orch8-io/engine/releases/download/${tag}"
asset="Orch8KmpBridge-${tag}.swift"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

curl -fsSL -o "$tmp/$asset" "$base/$asset"
curl -fsSL -o "$tmp/$asset.sha256" "$base/$asset.sha256"
expected="$(awk '{print $1}' "$tmp/$asset.sha256")"
if command -v sha256sum >/dev/null 2>&1; then
  actual="$(sha256sum "$tmp/$asset" | awk '{print $1}')"
else
  actual="$(shasum -a 256 "$tmp/$asset" | awk '{print $1}')"
fi
if [[ "$expected" != "$actual" ]]; then
  echo "error: checksum mismatch for $asset (expected $expected, got $actual)" >&2
  exit 1
fi

if [[ "$module" != "Orch8Kmp" ]]; then
  sed "s/^import Orch8Kmp$/import ${module}/" "$tmp/$asset" > "$tmp/Orch8KmpBridge.swift"
else
  cp "$tmp/$asset" "$tmp/Orch8KmpBridge.swift"
fi

mkdir -p "$dest"
cp "$tmp/Orch8KmpBridge.swift" "$dest/Orch8KmpBridge.swift"
echo "Installed Orch8KmpBridge.swift (${tag}, import ${module}) into ${dest}."
echo "Add it to your app target, and add the Orch8Mobile Swift package (exact ${version}) or pod 'Orch8Mobile', '${version}'."
