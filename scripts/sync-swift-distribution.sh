#!/usr/bin/env bash
# Prepare a release of github.com/orch8-io/orch8-mobile-swift (the Swift
# package and CocoaPod `Orch8Mobile`) from an engine GitHub release.
#
# Usage:
#   scripts/sync-swift-distribution.sh <swift-repo-checkout> <version> <xcframework-zip> <bindings-zip> <out-dir>
#
#   <version>          e.g. 0.7.2 (a leading v is stripped)
#   <xcframework-zip>  Orch8Mobile-v<version>.xcframework.zip from the engine release
#   <bindings-zip>     orch8-bindings-v<version>.zip from the engine release
#   <out-dir>          receives Orch8MobileCocoaPods-<version>.zip
#
# It rewrites the checkout in place (no git operations):
#   - Package.swift: binaryTarget url + checksum -> the engine release asset
#   - Orch8Mobile.podspec: s.version
#   - Sources/Orch8Mobile: packages/swift sources at this engine commit, with
#     Orch8MobileBindings.swift taken from the release's generated bindings so
#     the Swift side always matches the FFI checksums of the binary
#   - README.md, CHANGELOG.md, LICENSE from packages/swift
# and builds the CocoaPods source archive the podspec downloads
# (`:http => .../releases/download/<version>/Orch8MobileCocoaPods-<version>.zip`).
set -euo pipefail

if [[ $# -ne 5 ]]; then
  sed -n '2,22p' "$0" >&2
  exit 2
fi

repo="$(cd "$1" && pwd)"
version="${2#v}"
xcframework_zip="$(cd "$(dirname "$3")" && pwd)/$(basename "$3")"
bindings_zip="$(cd "$(dirname "$4")" && pwd)/$(basename "$4")"
mkdir -p "$5"
out_dir="$(cd "$5" && pwd)"
engine_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
swift_pkg="${engine_root}/packages/swift"
tag="v${version}"

if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "error: '${version}' is not a semantic version" >&2
  exit 1
fi
for f in "$repo/Package.swift" "$repo/Orch8Mobile.podspec" "$xcframework_zip" "$bindings_zip"; do
  [[ -f "$f" ]] || { echo "error: missing $f" >&2; exit 1; }
done

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'; else shasum -a 256 "$1" | awk '{print $1}'; fi
}

# SwiftPM's binaryTarget checksum is the SHA-256 of the zip
# (`swift package compute-checksum` computes exactly this).
checksum="$(sha256 "$xcframework_zip")"
url="https://github.com/orch8-io/engine/releases/download/${tag}/Orch8Mobile-${tag}.xcframework.zip"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

unzip -q "$bindings_zip" -d "$work/bindings"
generated_bindings="$work/bindings/bindings/swift/Orch8Mobile.swift"
[[ -f "$generated_bindings" ]] || { echo "error: $bindings_zip has no bindings/swift/Orch8Mobile.swift" >&2; exit 1; }

# -- Package.swift ------------------------------------------------------------
python3 - "$repo/Package.swift" "$url" "$checksum" <<'PY'
import re, sys
path, url, checksum = sys.argv[1:]
text = open(path).read()
text, n_url = re.subn(
    r'(url:\s*")https://github\.com/orch8-io/engine/releases/download/[^"]+(")', rf'\g<1>{url}\g<2>', text)
text, n_sum = re.subn(r'(checksum:\s*")[0-9a-f]{64}(")', rf'\g<1>{checksum}\g<2>', text)
if n_url != 1 or n_sum != 1:
    sys.exit(f"error: expected exactly one binaryTarget url and checksum in {path} (found {n_url}/{n_sum})")
open(path, "w").write(text)
PY

# -- Sources + docs -----------------------------------------------------------
rm -rf "$repo/Sources/Orch8Mobile"
mkdir -p "$repo/Sources/Orch8Mobile"
cp "$swift_pkg"/Sources/Orch8Mobile/*.swift "$repo/Sources/Orch8Mobile/"
if ! cmp -s "$generated_bindings" "$repo/Sources/Orch8Mobile/Orch8MobileBindings.swift"; then
  echo "::warning::packages/swift/Sources/Orch8Mobile/Orch8MobileBindings.swift differs from the bindings generated for ${tag}; publishing the generated file."
fi
cp "$generated_bindings" "$repo/Sources/Orch8Mobile/Orch8MobileBindings.swift"
if ! grep -Fq "orch8MobileVersion = \"${version}\"" "$repo/Sources/Orch8Mobile/Orch8Mobile.swift"; then
  echo "error: packages/swift/Sources/Orch8Mobile/Orch8Mobile.swift does not declare orch8MobileVersion = \"${version}\"" >&2
  exit 1
fi
for doc in README.md CHANGELOG.md LICENSE; do
  cp "$swift_pkg/$doc" "$repo/$doc"
done

# -- Podspec -------------------------------------------------------------------
cp "$swift_pkg/Orch8Mobile.podspec" "$repo/Orch8Mobile.podspec"
python3 - "$repo/Orch8Mobile.podspec" "$version" <<'PY'
import re, sys
path, version = sys.argv[1:]
text = open(path).read()
text, n = re.subn(r"(s\.version\s*=\s*')[^']+(')", rf"\g<1>{version}\g<2>", text)
if n != 1:
    sys.exit(f"error: expected exactly one s.version in {path}")
open(path, "w").write(text)
PY

# -- CocoaPods source archive -------------------------------------------------
pod_root="$work/pod"
mkdir -p "$pod_root/Sources"
cp "$repo/LICENSE" "$pod_root/LICENSE"
cp -R "$repo/Sources/Orch8Mobile" "$pod_root/Sources/Orch8Mobile"
unzip -q "$xcframework_zip" -d "$pod_root"
[[ -d "$pod_root/Orch8Mobile.xcframework" ]] || { echo "error: $xcframework_zip has no Orch8Mobile.xcframework/" >&2; exit 1; }
"$engine_root/scripts/check-xcframework.sh" "$pod_root/Orch8Mobile.xcframework"
pod_zip="$out_dir/Orch8MobileCocoaPods-${version}.zip"
rm -f "$pod_zip"
(cd "$pod_root" && zip -qry "$pod_zip" LICENSE Sources Orch8Mobile.xcframework)
sha256 "$pod_zip" > "$pod_zip.sha256"

echo "Swift package: ${url}"
echo "Swift package checksum: ${checksum}"
echo "CocoaPods archive: ${pod_zip}"
