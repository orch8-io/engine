#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

./scripts/check-sdk-versions.sh

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
  grep -Fq 'implementation("io.orch8:orch8-mobile:0.7.1")' "$gradle_file"
done

grep -Fq 'url: "https://github.com/orch8-io/engine/releases/download/v0.7.1/Orch8Mobile-v0.7.1.xcframework.zip"' packages/swift/Package.swift
grep -Fq 'checksum: "0a83ce860c5b41bb7d5dcd9401e4466eb48fc0513658194293a4ca691b3f61d5"' packages/swift/Package.swift
grep -Fq '"Orch8Mobile", "0.7.1"' packages/react-native/react-native-orch8.podspec
grep -Fq "'Orch8Mobile', '0.7.1'" packages/flutter/ios/orch8_flutter.podspec

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

ruby -c packages/swift/Orch8Mobile.podspec >/dev/null
ruby -c packages/flutter/ios/orch8_flutter.podspec >/dev/null
ruby -c packages/react-native/react-native-orch8.podspec >/dev/null

echo 'SDK package dependency contracts are resolvable and version-aligned.'
