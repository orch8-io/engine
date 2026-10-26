#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
framework_dir="${1:?pass directory containing native-host Orch8Kmp.framework}"
ffi_headers="${2:?pass generated Swift FFI headers directory}"
native_dir="${3:?pass current native host library directory}"
fixture_dir="${4:?pass exported scoped fixture directory}"
mkdir -p "$repo_root/target"
work_dir="$(mktemp -d "$repo_root/target/kmp-scoped-host.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT
runtime_dir="$work_dir/native"
mkdir -p "$work_dir/ffi" "$runtime_dir"
# Cargo debug dylibs carry an absolute producer-workspace install name. CI
# downloads only the artifact. Relocate and ad-hoc sign a private copy so the
# test cannot silently load a library left in the producer's build directory.
cp "$native_dir/liborch8_mobile.dylib" "$runtime_dir/"
xcrun install_name_tool -id '@rpath/liborch8_mobile.dylib' "$runtime_dir/liborch8_mobile.dylib"
codesign --force --sign - "$runtime_dir/liborch8_mobile.dylib"
cp "$ffi_headers/Orch8MobileFFI.h" "$work_dir/ffi/"
cp "$ffi_headers/Orch8MobileFFI.modulemap" "$work_dir/ffi/module.modulemap"
sdk_path="$(xcrun --sdk macosx --show-sdk-path)"
host_arch="$(uname -m)"

# Build actual SDK sources and import the real Kotlin/Native framework. No
# protocol shape stubs, generated mock records or alternate bridge implementation.
xcrun --sdk macosx swiftc -warnings-as-errors -strict-concurrency=complete \
  -emit-module -emit-library -parse-as-library -module-name Orch8Mobile \
  -target "$host_arch-apple-macos13.0" -sdk "$sdk_path" -I "$work_dir/ffi" \
  -L "$runtime_dir" -lorch8_mobile -Xlinker -rpath -Xlinker "$runtime_dir" \
  -emit-module-path "$work_dir/Orch8Mobile.swiftmodule" -o "$work_dir/libOrch8Mobile.dylib" \
  "$repo_root"/packages/swift/Sources/Orch8Mobile/*.swift

xcrun --sdk macosx swiftc -warnings-as-errors -strict-concurrency=complete \
  -target "$host_arch-apple-macos13.0" -sdk "$sdk_path" \
  -I "$work_dir" -I "$work_dir/ffi" -F "$framework_dir" \
  -L "$work_dir" -lOrch8Mobile -framework Orch8Kmp -lc++ \
  -Xlinker -rpath -Xlinker "$work_dir" -Xlinker -rpath -Xlinker "$runtime_dir" \
  "$repo_root/packages/kmp/ios-bridge/Orch8KmpBridge.swift" \
  "$repo_root/scripts/fixtures/kmp-scoped-host.swift" -o "$work_dir/scoped-kmp-host"
"$work_dir/scoped-kmp-host" "$fixture_dir"
