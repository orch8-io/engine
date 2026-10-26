#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
build="$root/target/apple-bridge-lifecycle"
rn="$root/packages/react-native/ios/NativeEngineLifecycle.swift"
flutter="$root/packages/flutter/ios/orch8_flutter/Sources/orch8_flutter/NativeEngineLifecycle.swift"
mkdir -p "$build/module-cache"
cmp "$rn" "$flutter"
for platform in react-native flutter; do
  controller_source="$rn"
  if [[ "$platform" == flutter ]]; then controller_source="$flutter"; fi
  swiftc -warnings-as-errors -swift-version 5 -module-cache-path "$build/module-cache" -parse-as-library \
    "$controller_source" "$root/tests/apple-bridge-lifecycle/main.swift" -o "$build/$platform-tests"
  "$build/$platform-tests"
done

if [[ "$#" == 2 ]]; then
  ffi_headers="$1"
  native_dir="$2"
  native_work="$(mktemp -d "$build/native.XXXXXX")"
  trap 'rm -rf "$native_work"' EXIT
  mkdir -p "$native_work/ffi" "$native_work/runtime"
  cp "$ffi_headers/Orch8MobileFFI.h" "$native_work/ffi/"
  cp "$ffi_headers/Orch8MobileFFI.modulemap" "$native_work/ffi/module.modulemap"
  cp "$native_dir/liborch8_mobile.dylib" "$native_work/runtime/"
  xcrun install_name_tool -id '@rpath/liborch8_mobile.dylib' "$native_work/runtime/liborch8_mobile.dylib"
  codesign --force --sign - "$native_work/runtime/liborch8_mobile.dylib"
  sdk_path="$(xcrun --sdk macosx --show-sdk-path)"
  host_arch="$(uname -m)"
  swiftc -warnings-as-errors -swift-version 5 -strict-concurrency=complete \
    -module-cache-path "$build/module-cache" -emit-module -emit-library -parse-as-library \
    -module-name Orch8Mobile -target "$host_arch-apple-macos13.0" -sdk "$sdk_path" \
    -I "$native_work/ffi" -L "$native_work/runtime" -lorch8_mobile \
    -Xlinker -rpath -Xlinker "$native_work/runtime" \
    -emit-module-path "$native_work/Orch8Mobile.swiftmodule" -o "$native_work/libOrch8Mobile.dylib" \
    "$root"/packages/swift/Sources/Orch8Mobile/*.swift
  for platform in react-native flutter; do
    controller_source="$rn"
    if [[ "$platform" == flutter ]]; then controller_source="$flutter"; fi
    swiftc -warnings-as-errors -swift-version 5 -module-cache-path "$build/module-cache" \
      -parse-as-library -target "$host_arch-apple-macos13.0" -sdk "$sdk_path" \
      -I "$native_work" -I "$native_work/ffi" -L "$native_work" -lOrch8Mobile \
      -Xlinker -rpath -Xlinker "$native_work" \
      "$controller_source" "$root/tests/apple-bridge-lifecycle/native.swift" -o "$native_work/$platform-native-tests"
    "$native_work/$platform-native-tests"
  done
elif [[ "$#" != 0 ]]; then
  echo 'Usage: check-apple-bridge-lifecycle.sh [generated-swift-headers current-native-library-dir]' >&2
  exit 2
fi
