#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
framework_dir="${1:?pass the directory containing Orch8Kmp.framework}"
ffi_headers="${2:?pass the generated FFI header/module.modulemap directory}"
module_dir="${3:-$repo_root/target/kmp-ios-swift-module}"
sdk_path="$(xcrun --sdk iphonesimulator --show-sdk-path)"
mkdir -p "$module_dir"

# Compile actual SDK sources against generated headers; no shape stubs. The
# separate Swift simulator tests validate the linked native SDK implementation.
xcrun --sdk iphonesimulator swiftc -warnings-as-errors -emit-module -parse-as-library \
  -module-name Orch8Mobile -target arm64-apple-ios16.0-simulator -sdk "$sdk_path" \
  -I "$ffi_headers" -emit-module-path "$module_dir/Orch8Mobile.swiftmodule" \
  "$repo_root"/packages/swift/Sources/Orch8Mobile/*.swift

xcrun --sdk iphonesimulator swiftc -warnings-as-errors -typecheck \
  "$repo_root/packages/kmp/ios-bridge/Orch8KmpBridge.swift" \
  -target arm64-apple-ios16.0-simulator -sdk "$sdk_path" \
  -I "$module_dir" -I "$ffi_headers" -F "$framework_dir"
