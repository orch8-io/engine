#!/usr/bin/env bash
set -euo pipefail

# Build Orch8Mobile.xcframework from Rust sources.
# Usage: ./scripts/build-xcframework.sh [--release]
#
# Requires: Xcode CLI tools, Rust targets installed:
#   rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios

PROFILE="debug"
CARGO_FLAGS=""
if [[ "${1:-}" == "--release" ]]; then
    PROFILE="mobile-release"
    CARGO_FLAGS="--profile mobile-release"
fi

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="${ROOT}/target/xcframework-build"
rm -rf "${BUILD}"
mkdir -p "${BUILD}/headers"

echo "==> Building iOS targets (${PROFILE})…"
for TARGET in aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios; do
    echo "    ${TARGET}"
    cargo build ${CARGO_FLAGS} --target "${TARGET}" -p orch8-mobile
done

echo "==> Generating Swift bindings…"
cargo build -p orch8-mobile
cargo run -p orch8-mobile --features bindgen --bin uniffi-bindgen -- \
    generate --library "target/debug/liborch8_mobile.dylib" \
    --language swift --out-dir "${BUILD}/headers"

echo "==> Creating fat simulator library…"
lipo -create \
    "target/aarch64-apple-ios-sim/${PROFILE}/liborch8_mobile.a" \
    "target/x86_64-apple-ios/${PROFILE}/liborch8_mobile.a" \
    -output "${BUILD}/liborch8_mobile-sim.a"

# Rename modulemap for Xcode compatibility.
cp "${BUILD}/headers/Orch8MobileFFI.modulemap" "${BUILD}/headers/module.modulemap"

echo "==> Creating XCFramework…"
rm -rf "${ROOT}/packages/swift/Orch8Mobile.xcframework"
xcodebuild -create-xcframework \
    -library "target/aarch64-apple-ios/${PROFILE}/liborch8_mobile.a" \
    -headers "${BUILD}/headers" \
    -library "${BUILD}/liborch8_mobile-sim.a" \
    -headers "${BUILD}/headers" \
    -output "${ROOT}/packages/swift/Orch8Mobile.xcframework"

# Copy generated Swift source alongside the package.
cp "${BUILD}/headers/Orch8Mobile.swift" \
   "${ROOT}/packages/swift/Sources/Orch8Mobile/Orch8MobileGenerated.swift" 2>/dev/null || true

echo "==> Done: packages/swift/Orch8Mobile.xcframework"
