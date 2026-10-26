#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
fixtures="${1:?pass downloaded Apple framework fixture directory}"
ffi_headers="${2:?pass current generated Swift FFI headers directory}"
react_package="$root/packages/react-native/node_modules/react-native"
build="$root/target/apple-native-adapters"
mkdir -p "$build/ffi" "$build/sdk" "$build/module-cache" "$build/headers/RCTDeprecation"
cp "$ffi_headers/Orch8MobileFFI.h" "$build/ffi/"
cp "$ffi_headers/Orch8MobileFFI.modulemap" "$build/ffi/module.modulemap"
# These are the real companion pod headers shipped in the React Native package.
cp "$react_package/ReactApple/Libraries/RCTFoundation/RCTDeprecation/Exported/RCTDeprecation.h" "$build/headers/RCTDeprecation/"
ln -sfn "$react_package/ReactCommon/yoga/yoga" "$build/headers/yoga"
react_slice="$fixtures/react/React.xcframework/ios-arm64_x86_64-simulator"
dependencies_slice="$fixtures/react-dependencies/packages/react-native/third-party/ReactNativeDependencies.xcframework/ios-arm64_x86_64-simulator"
flutter_slice="$fixtures/flutter/Flutter.xcframework/ios-arm64_x86_64-simulator"
sdk_path="$(xcrun --sdk iphonesimulator --show-sdk-path)"
common=(-warnings-as-errors -swift-version 5 -module-cache-path "$build/module-cache"
  -target arm64-apple-ios16.0-simulator -sdk "$sdk_path" -I "$build/ffi")
swiftc "${common[@]}" -emit-module -parse-as-library -module-name Orch8Mobile \
  -emit-module-path "$build/sdk/Orch8Mobile.swiftmodule" "$root"/packages/swift/Sources/Orch8Mobile/*.swift
react_flags=(-I "$build/headers" -I "$react_package/ReactCommon" -I "$react_package/ReactCommon/jsitooling"
  -I "$react_slice/React.framework/Headers" -F "$react_slice" -F "$dependencies_slice")
# Published React headers have nullability and incomplete-umbrella warnings.
# Keep our Swift warnings fatal; disable only these upstream header diagnostics.
swiftc "${common[@]}" -Xcc -Wno-nullability -Xcc -Wno-nullability-completeness \
  -Xcc -Wno-incomplete-umbrella -typecheck -I "$build/sdk" "${react_flags[@]}" "$root"/packages/react-native/ios/*.swift
swiftc "${common[@]}" -typecheck -I "$build/sdk" -F "$flutter_slice" \
  "$root"/packages/flutter/ios/orch8_flutter/Sources/orch8_flutter/*.swift
xcrun --sdk iphonesimulator clang -fsyntax-only -fobjc-arc -fmodules \
  -Wno-nullability -Wno-nullability-completeness -Wno-incomplete-umbrella \
  "-fmodules-cache-path=$build/module-cache" -target arm64-apple-ios16.0-simulator -isysroot "$sdk_path" \
  "${react_flags[@]}" "$root/packages/react-native/ios/Orch8Module.m"
echo 'React Native and Flutter Apple adapters compile against the real iOS 16 SDK and frameworks.'
