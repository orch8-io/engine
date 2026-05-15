#!/usr/bin/env bash
set -euo pipefail

# Build Orch8Mobile AAR from Rust sources.
# Usage: ./scripts/build-android-aar.sh [--release]
#
# Requires:
#   - Android NDK (set ANDROID_NDK_HOME or uses default path)
#   - Rust targets: rustup target add \
#       aarch64-linux-android armv7-linux-androideabi x86_64-linux-android

PROFILE="debug"
CARGO_FLAGS=""
if [[ "${1:-}" == "--release" ]]; then
    PROFILE="mobile-release"
    CARGO_FLAGS="--profile mobile-release"
fi

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
AAR_PROJECT="${ROOT}/packages/android"
JNILIBS="${AAR_PROJECT}/orch8-mobile/src/main/jniLibs"

NDK="${ANDROID_NDK_HOME:-${ANDROID_HOME:-$HOME/Library/Android/sdk}/ndk/27.2.12479018}"
if [[ ! -d "${NDK}" ]]; then
    echo "ERROR: Android NDK not found at ${NDK}"
    echo "Set ANDROID_NDK_HOME or install NDK via Android Studio."
    exit 1
fi

TOOLCHAIN="${NDK}/toolchains/llvm/prebuilt"
case "$(uname -s)" in
    Darwin) HOST_TAG="darwin-x86_64" ;;
    Linux)  HOST_TAG="linux-x86_64" ;;
    *)      echo "Unsupported OS"; exit 1 ;;
esac
BIN="${TOOLCHAIN}/${HOST_TAG}/bin"

declare -A TARGET_MAP=(
    ["aarch64-linux-android"]="arm64-v8a"
    ["armv7-linux-androideabi"]="armeabi-v7a"
    ["x86_64-linux-android"]="x86_64"
)

declare -A CC_MAP=(
    ["aarch64-linux-android"]="aarch64-linux-android24-clang"
    ["armv7-linux-androideabi"]="armv7a-linux-androideabi24-clang"
    ["x86_64-linux-android"]="x86_64-linux-android24-clang"
)

rm -rf "${JNILIBS}"
mkdir -p "${JNILIBS}"

echo "==> Building Android targets (${PROFILE})…"
for TARGET in "${!TARGET_MAP[@]}"; do
    ABI="${TARGET_MAP[$TARGET]}"
    CC="${BIN}/${CC_MAP[$TARGET]}"
    AR="${BIN}/llvm-ar"

    echo "    ${TARGET} → ${ABI}"
    CARGO_TARGET_UPPER="$(echo "${TARGET}" | tr '[:lower:]-' '[:upper:]_')"

    CC="${CC}" AR="${AR}" \
    CARGO_TARGET_${CARGO_TARGET_UPPER}_LINKER="${CC}" \
    cargo build ${CARGO_FLAGS} --target "${TARGET}" -p orch8-mobile

    mkdir -p "${JNILIBS}/${ABI}"
    cp "target/${TARGET}/${PROFILE}/liborch8_mobile.so" "${JNILIBS}/${ABI}/"
done

echo "==> Generating Kotlin bindings…"
cargo build -p orch8-mobile
cargo run -p orch8-mobile --features bindgen --bin uniffi-bindgen -- \
    generate --library "target/debug/liborch8_mobile.dylib" \
    --language kotlin --out-dir "${AAR_PROJECT}/orch8-mobile/src/main/java"

echo "==> Assembling AAR…"
cd "${AAR_PROJECT}"
./gradlew :orch8-mobile:assembleRelease

AAR_PATH="$(find "${AAR_PROJECT}" -name "*.aar" -path "*release*" | head -1)"
if [[ -n "${AAR_PATH}" ]]; then
    echo "==> Done: ${AAR_PATH#$ROOT/}"
else
    echo "==> AAR assembled (check orch8-mobile/build/outputs/aar/)"
fi
