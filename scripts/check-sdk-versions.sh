#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

workspace_version="$(awk -F'"' '/^\[workspace\.package\]/{found=1; next} /^\[/{found=0} found && /^version = /{print $2; exit}' Cargo.toml)"
if [[ -z "$workspace_version" ]]; then
  echo "Could not read [workspace.package] version from Cargo.toml." >&2
  exit 1
fi

version_checks=(
  "packages/android/gradle.properties|VERSION_NAME=$workspace_version"
  "packages/flutter/android/build.gradle.kts|implementation(\"io.orch8:orch8-mobile:$workspace_version\")"
  "packages/flutter/android/src/main/kotlin/io/orch8/flutter/Orch8FlutterPlugin.kt|?: \"$workspace_version\""
  "packages/flutter/ios/Classes/Orch8FlutterPlugin.swift|?? \"$workspace_version\""
  "packages/flutter/ios/orch8_flutter.podspec|s.version          = '$workspace_version'"
  "packages/flutter/lib/orch8_flutter.dart|this.sdkVersion = '$workspace_version'"
  "packages/flutter/pubspec.yaml|version: $workspace_version"
  "packages/react-native/android/src/main/java/io/orch8/reactnative/Orch8Module.kt|?: \"$workspace_version\""
  "packages/react-native/ios/Orch8Module.swift|?? \"$workspace_version\""
  "packages/react-native/package.json|\"version\": \"$workspace_version\""
  "packages/swift/Sources/Orch8Mobile/Orch8Mobile.swift|orch8MobileVersion = \"$workspace_version\""
)

failed=0
for check in "${version_checks[@]}"; do
  file="${check%%|*}"
  expected="${check#*|}"
  if ! grep -Fq "$expected" "$file"; then
    echo "SDK version drift: $file does not declare workspace version $workspace_version." >&2
    failed=1
  fi
done

if (( failed != 0 )); then
  exit 1
fi

echo "All SDKs match workspace version $workspace_version."
