#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
destination="${1:?pass the artifact output directory}"
version="${2:-}"
version="${version#v}"
case "${3:-$(uname -m)}" in
  x64|amd64|x86_64) architecture=x64; platform=linux/amd64; target=x86_64-unknown-linux-musl ;;
  arm64|aarch64) architecture=arm64; platform=linux/arm64; target=aarch64-unknown-linux-musl ;;
  *) echo 'expected x64 or arm64' >&2; exit 2 ;;
esac
rust="$(awk -F'"' '/^channel = / {print $2; exit}' "$root/rust-toolchain.toml")"
test -n "$rust"
node_image="node:24-alpine3.23"
if ! timeout 20 docker info --format '{{.ServerVersion}}' >/dev/null; then
  echo "Docker did not respond before the Node musl build could start" >&2
  exit 1
fi
work="$(mktemp -d "${TMPDIR:-/tmp}/orch8-node-musl.XXXXXX")"
trap 'rm -rf "$work"' EXIT
work="$(cd "$work" && pwd -P)"
mkdir -p "$work/tarballs" "$destination"

docker run --rm --platform "$platform" -v "$root:/src:ro" -v "$work:/out" -w /src \
  -e "RUSTUP_TOOLCHAIN=$rust" -e CARGO_TARGET_DIR=/tmp/orch8-target \
  -e "ORCH8_NATIVE_TARGET=$target" \
  -e 'RUSTFLAGS=-C target-feature=-crt-static' -e CARGO_PROFILE_RELEASE_STRIP=symbols \
  "rust:$rust-alpine3.23" sh -ec '
    apk add --no-cache binutils build-base cmake perl pkgconf
    export LC_ALL=C
    target="$(rustc -vV | sed -n "s/^host: //p")"
    test "$target" = "$ORCH8_NATIVE_TARGET"
    cargo build --locked --release --manifest-path packages/node-native/Cargo.toml --target "$target"
    addon="$CARGO_TARGET_DIR/$target/release/liborch8_node.so"
    readelf --dynamic "$addon" > /tmp/orch8-addon-abi
    # libgcc can expose GNU version names on musl, so check the libc SONAME.
    if ! grep -Fq "Shared library: [libc.musl-${target%%-*}.so.1]" /tmp/orch8-addon-abi ||
       grep -Eq "Shared library: \[(libc\.so\.6|ld-linux[^]]*)\]" /tmp/orch8-addon-abi; then
      grep "NEEDED" /tmp/orch8-addon-abi >&2
      echo "The addon is linked against an unexpected C runtime" >&2
      exit 1
    fi
    cp "$addon" /out/addon.node
  '

docker run --rm --platform "$platform" -v "$root:/src:ro" -v "$work:/out" \
  -e "ORCH8_NATIVE_VERSION=$version" -e "ORCH8_NATIVE_ARCH=$architecture" \
  "$node_image" sh -ec '
    node -e "require(\"node:assert/strict\").equal(process.arch, process.env.ORCH8_NATIVE_ARCH)"
    node --test /src/scripts/pack-node-native-musl.test.cjs
    node /src/scripts/pack-node-native-musl.cjs /out/addon.node /tmp/node-packages "$ORCH8_NATIVE_VERSION"
    for package in /tmp/node-packages/wrapper /tmp/node-packages/addon; do
      (cd "$package" && npm pack --ignore-scripts --pack-destination /out/tarballs)
    done
    node -p "require(\"/tmp/node-packages/wrapper/package.json\").version" > /out/version
  '

# A new runtime image with no toolchain, GNU compatibility packages, network,
# or access to the built library other than through the installed tarballs.
docker run --rm --platform "$platform" --network none -v "$work/tarballs:/tarballs:ro" \
  -e "ORCH8_NATIVE_ARCH=$architecture" \
  -v "$root/scripts/test-node-native-musl.cjs:/smoke.cjs:ro" "$node_image" sh -ec '
    if command -v cc || command -v rustc || apk info -e gcompat || apk info -e libc6-compat; then
      echo "Unexpected compiler or glibc compatibility layer" >&2
      exit 1
    fi
    mkdir /tmp/consumer
    cd /tmp/consumer
    cp /smoke.cjs ./smoke.cjs
    npm install --offline --ignore-scripts --no-audit --no-fund /tarballs/*.tgz
    node smoke.cjs
  '

asset="orch8-engine-native-$(cat "$work/version")-linux-$architecture-musl.tar.gz"
tar -czf "$destination/$asset" -C "$work/tarballs" .
(cd "$destination" && sha256sum "$asset" > "$asset.sha256")
