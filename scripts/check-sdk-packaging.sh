#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

./scripts/check-sdk-versions.sh

sdk_version="$(awk -F= '/^VERSION_NAME=/{print $2; exit}' packages/android/gradle.properties)"

required_files=(
  packages/flutter/CHANGELOG.md
  packages/flutter/LICENSE
  packages/flutter/README.md
  packages/flutter/ios/orch8_flutter/Package.swift
  packages/flutter/ios/orch8_flutter/Sources/orch8_flutter/Orch8FlutterPlugin.swift
  packages/react-native/CHANGELOG.md
  packages/react-native/LICENSE
  packages/react-native/README.md
  packages/swift/CHANGELOG.md
  packages/swift/LICENSE
  packages/swift/Orch8Mobile.podspec
  packages/swift/README.md
  packages/swift/Sources/Orch8Mobile/Orch8MobileBindings.swift
)

for file in "${required_files[@]}"; do
  if [[ ! -s "$file" ]]; then
    echo "Missing SDK distribution file: $file" >&2
    exit 1
  fi
done

if grep -R -Fq 'project(":orch8-mobile")' packages/flutter packages/react-native; then
  echo 'Wrapper still contains an unresolvable local :orch8-mobile dependency.' >&2
  exit 1
fi

for gradle_file in \
  packages/flutter/android/build.gradle.kts \
  packages/react-native/android/build.gradle.kts; do
  grep -Fq 'https://raw.githubusercontent.com/orch8-io/maven/main' "$gradle_file"
  # Either a literal pin or the package.json-driven pin (whose version
  # check-sdk-versions.sh already enforced above).
  grep -Fq "implementation(\"io.orch8:orch8-mobile:${sdk_version}\")" "$gradle_file" ||
    grep -Fq 'implementation("io.orch8:orch8-mobile:$orch8NativeVersion")' "$gradle_file"
done

grep -Fq "url: \"https://github.com/orch8-io/engine/releases/download/v${sdk_version}/Orch8Mobile-v${sdk_version}.xcframework.zip\"" packages/swift/Package.swift
# The checksum is produced by the release pipeline; only assert it is a SHA-256.
grep -Eq 'checksum: "[0-9a-f]{64}"' packages/swift/Package.swift
grep -Fq "\"Orch8Mobile\", \"${sdk_version}\"" packages/react-native/react-native-orch8.podspec ||
  grep -Fq 's.dependency     "Orch8Mobile", orch8_native_version' packages/react-native/react-native-orch8.podspec
grep -Fq "'Orch8Mobile', '${sdk_version}'" packages/flutter/ios/orch8_flutter.podspec

# All Apple-facing packages and native builds must advertise the binary's
# actual minimum deployment target.
grep -Fq '.iOS(.v16)' packages/swift/Package.swift
grep -Fq '.iOS(.v16)' packages/flutter/ios/orch8_flutter/Package.swift
grep -Fq ":ios, '16.0'" packages/swift/Orch8Mobile.podspec
grep -Fq ":ios, '16.0'" packages/flutter/ios/orch8_flutter.podspec
grep -Fq 'ios: "16.0"' packages/react-native/react-native-orch8.podspec
grep -Fq 'IPHONEOS_DEPLOYMENT_TARGET: "16.0"' .github/workflows/mobile.yml
grep -Fq 'IPHONEOS_DEPLOYMENT_TARGET: "16.0"' .github/workflows/release.yml
grep -Fq 'export IPHONEOS_DEPLOYMENT_TARGET="16.0"' scripts/build-xcframework.sh
grep -Fq 'link-arg=-mios-version-min=16.0' .cargo/config.toml
grep -Fq 'link-arg=-mios-simulator-version-min=16.0' .cargo/config.toml

# CocoaPods requires every static-library slice in an XCFramework to use the
# same binary name. Keep the simulator archive in its own directory instead.
grep -Fq -- '-output build/ios-simulator/liborch8_mobile.a' .github/workflows/mobile.yml
grep -Fq -- '-output build/ios-simulator/liborch8_mobile.a' .github/workflows/release.yml
grep -Fq -- '-output "${BUILD}/simulator/liborch8_mobile.a"' scripts/build-xcframework.sh

# Panic strategy: the mobile profile must unwind so UniFFI can convert a Rust
# panic in a handler/callback into an error instead of aborting the host app.
mobile_profile="$(awk '/^\[profile\.mobile-release\]/{f=1;next} /^\[/{f=0} f' Cargo.toml)"
if grep -Eq '^[[:space:]]*panic[[:space:]]*=[[:space:]]*"abort"' <<<"$mobile_profile"; then
  echo 'profile.mobile-release must not set panic = "abort" (host app would crash on a Rust panic).' >&2
  exit 1
fi
grep -Eq '^[[:space:]]*panic[[:space:]]*=[[:space:]]*"unwind"' <<<"$mobile_profile"

# Android 16 KB page-size support: linker flag for 64-bit targets + JNA >= 5.17.0.
for t in aarch64-linux-android x86_64-linux-android; do
  awk -v t="[target.$t]" '$0==t{f=1;next} /^\[/{f=0} f' .cargo/config.toml |
    grep -Fq 'max-page-size=16384'
done
for f in packages/android/orch8-mobile/build.gradle.kts packages/kmp/build.gradle.kts; do
  grep -Eq 'net\.java\.dev\.jna:jna:5\.(1[7-9]|[2-9][0-9])\.' "$f"
done

ruby -c packages/swift/Orch8Mobile.podspec >/dev/null
ruby -c packages/flutter/ios/orch8_flutter.podspec >/dev/null
ruby -c packages/react-native/react-native-orch8.podspec >/dev/null

echo 'SDK package dependency contracts are resolvable and version-aligned.'
