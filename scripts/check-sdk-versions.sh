#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

sdk_version="$(awk -F= '/^VERSION_NAME=/{print $2; exit}' packages/android/gradle.properties)"
if [[ -z "$sdk_version" ]]; then
  echo "Could not read VERSION_NAME from packages/android/gradle.properties." >&2
  exit 1
fi

flutter_swift_path="packages/flutter/ios/orch8_flutter/Sources/orch8_flutter/Orch8FlutterPlugin.swift"
if [[ ! -f "$flutter_swift_path" ]]; then
  flutter_swift_path="packages/flutter/ios/Classes/Orch8FlutterPlugin.swift"
fi

version_checks=(
  "packages/android/gradle.properties|VERSION_NAME=$sdk_version"
  "packages/flutter/android/src/main/kotlin/io/orch8/flutter/Orch8FlutterPlugin.kt|?: \"$sdk_version\""
  "$flutter_swift_path|?? \"$sdk_version\""
  "packages/flutter/ios/orch8_flutter.podspec|s.version          = '$sdk_version'"
  "packages/flutter/lib/orch8_flutter.dart|this.sdkVersion = '$sdk_version'"
  "packages/flutter/pubspec.yaml|version: $sdk_version"
  "packages/react-native/android/src/main/java/io/orch8/reactnative/Orch8Module.kt|?: \"$sdk_version\""
  "packages/react-native/ios/Orch8Module.swift|?? \"$sdk_version\""
  "packages/react-native/package.json|\"version\": \"$sdk_version\""
  "packages/swift/Sources/Orch8Mobile/Orch8Mobile.swift|orch8MobileVersion = \"$sdk_version\""
)

failed=0
for check in "${version_checks[@]}"; do
  file="${check%%|*}"
  expected="${check#*|}"
  if ! grep -Fq "$expected" "$file"; then
    echo "SDK version drift: $file does not declare SDK version $sdk_version." >&2
    failed=1
  fi
done

if (( failed != 0 )); then
  exit 1
fi

echo "All mobile SDK packages consistently declare version $sdk_version."
