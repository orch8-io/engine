#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
headers="${1:?pass same-commit generated Swift headers}"
native="${2:?pass same-commit native library directory}"
fixture="${3:?pass the Rust-produced four-scope fixture directory}"
build="$root/target/react-native-scopes"
mkdir -p "$build/module-cache"
work="$(mktemp -d "$build/native.XXXXXX")"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/ffi" "$work/runtime"
cp "$headers/Orch8MobileFFI.h" "$work/ffi/"
cp "$headers/Orch8MobileFFI.modulemap" "$work/ffi/module.modulemap"
install_runtime() {
  cp "$1" "$work/runtime/liborch8_mobile.dylib"
  xcrun install_name_tool -id '@rpath/liborch8_mobile.dylib' "$work/runtime/liborch8_mobile.dylib"
  codesign --force --sign - "$work/runtime/liborch8_mobile.dylib"
}
install_runtime "$native/liborch8_mobile.dylib"
sdk="$(xcrun --sdk macosx --show-sdk-path)"
arch="$(uname -m)"
flags=(-warnings-as-errors -swift-version 5 -module-cache-path "$build/module-cache" -target "$arch-apple-macos13.0" -sdk "$sdk")
swiftc "${flags[@]}" -emit-module -emit-library -parse-as-library -module-name Orch8Mobile \
  -I "$work/ffi" -L "$work/runtime" -lorch8_mobile -Xlinker -rpath -Xlinker "$work/runtime" \
  -emit-module-path "$work/Orch8Mobile.swiftmodule" -o "$work/libOrch8Mobile.dylib" \
  "$root"/packages/swift/Sources/Orch8Mobile/*.swift
swiftc "${flags[@]}" -parse-as-library -I "$work" -I "$work/ffi" -L "$work" -lOrch8Mobile \
  -Xlinker -rpath -Xlinker "$work" \
  "$root/packages/react-native/ios/NativeEngineLifecycle.swift" \
  "$root/packages/react-native/ios/PendingHandlerCalls.swift" \
  "$root/packages/react-native/ios/ScopedBridge.swift" \
  "$root/tests/react-native-scopes/main.swift" -o "$work/tests"
if [[ "$#" == 4 ]]; then
  install_runtime "$4"
  if "$work/tests" "$fixture" "$work" > "$work/previous.log" 2>&1; then
    echo 'New bindings unexpectedly accepted the old flat-error native library.' >&2
    exit 1
  fi
  if ! grep -q 'mobile_callback_contract_v2' "$work/previous.log"; then
    cat "$work/previous.log" >&2
    echo 'Previous native library failed for an unexpected reason.' >&2
    exit 1
  fi
  echo 'Previous flat-error native library refused by the callback-contract initialization guard.'
  install_runtime "$native/liborch8_mobile.dylib"
fi
if ! "$work/tests" "$fixture" "$work" > "$work/output.log" 2>&1; then
  cat "$work/output.log" >&2
  exit 1
fi
cat "$work/output.log"
if grep -q 'panicked at' "$work/output.log"; then
  echo 'Unexpected native panic during scoped bridge validation.' >&2
  exit 1
fi
