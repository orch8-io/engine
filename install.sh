#!/bin/sh
set -e

REPO="orch8-io/engine"
BINARY_NAMES="orch8-server orch8"
INSTALL_DIR=""

die() {
    echo "error: $1" >&2
    exit 1
}

detect_os() {
    case "$(uname -s)" in
        Linux)  echo "linux" ;;
        Darwin) echo "darwin" ;;
        *)      die "unsupported operating system: $(uname -s)" ;;
    esac
}

detect_arch() {
    case "$(uname -m)" in
        x86_64)          echo "x86_64" ;;
        amd64)           echo "x86_64" ;;
        aarch64|arm64)   echo "aarch64" ;;
        *)               die "unsupported architecture: $(uname -m)" ;;
    esac
}

build_target() {
    OS="$1"
    ARCH="$2"
    case "${OS}-${ARCH}" in
        linux-x86_64)   echo "x86_64-unknown-linux-gnu" ;;
        linux-aarch64)  echo "aarch64-unknown-linux-gnu" ;;
        darwin-x86_64)  echo "x86_64-apple-darwin" ;;
        darwin-aarch64) echo "aarch64-apple-darwin" ;;
        *)              die "no release target for ${OS}-${ARCH}" ;;
    esac
}

fetch_latest_version() {
    URL="https://api.github.com/repos/${REPO}/releases/latest"
    VERSION="$(curl -fsSL "$URL" | grep '"tag_name"' | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')"
    if [ -z "$VERSION" ]; then
        die "failed to fetch latest release version from GitHub API"
    fi
    echo "$VERSION"
}

pick_install_dir() {
    if [ "$(id -u)" = "0" ]; then
        echo "/usr/local/bin"
    elif mkdir -p /usr/local/bin 2>/dev/null && [ -w /usr/local/bin ]; then
        echo "/usr/local/bin"
    else
        echo "${HOME}/.local/bin"
    fi
}

verify_checksum() {
    ARCHIVE="$1"
    CHECKSUM_FILE="$2"
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum --check "$CHECKSUM_FILE"
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 --check "$CHECKSUM_FILE"
    else
        die "neither sha256sum nor shasum found; cannot verify checksum"
    fi
}

main() {
    OS="$(detect_os)"
    ARCH="$(detect_arch)"
    TARGET="$(build_target "$OS" "$ARCH")"

    if [ -z "$VERSION" ] || [ "$VERSION" = "latest" ]; then
        VERSION="$(fetch_latest_version)"
    fi

    # Strip leading 'v' for archive name construction, then re-add
    VERSION_TAG="$VERSION"
    case "$VERSION_TAG" in
        v*) ;;
        *)  VERSION_TAG="v${VERSION_TAG}" ;;
    esac

    ARCHIVE="orch8-${VERSION_TAG}-${TARGET}.tar.gz"
    CHECKSUM="orch8-${VERSION_TAG}-${TARGET}.tar.gz.sha256"
    BASE_URL="https://github.com/${REPO}/releases/download/${VERSION_TAG}"

    TMPDIR="$(mktemp -d)"
    trap 'rm -rf "$TMPDIR"' EXIT

    echo "downloading orch8 ${VERSION_TAG} for ${TARGET}..."
    curl -fsSL "${BASE_URL}/${ARCHIVE}" -o "${TMPDIR}/${ARCHIVE}"
    curl -fsSL "${BASE_URL}/${CHECKSUM}" -o "${TMPDIR}/${CHECKSUM}"

    echo "verifying checksum..."
    cd "$TMPDIR"
    verify_checksum "$ARCHIVE" "$CHECKSUM"
    cd - >/dev/null

    echo "extracting..."
    tar -xzf "${TMPDIR}/${ARCHIVE}" -C "$TMPDIR"

    INSTALL_DIR="$(pick_install_dir)"
    mkdir -p "$INSTALL_DIR"

    for BIN in $BINARY_NAMES; do
        BIN_PATH="$(find "$TMPDIR" -type f -name "$BIN" | head -n 1)"
        if [ -z "$BIN_PATH" ]; then
            echo "warning: binary '$BIN' not found in archive, skipping"
            continue
        fi
        chmod +x "$BIN_PATH"
        if [ -w "$INSTALL_DIR" ]; then
            cp "$BIN_PATH" "${INSTALL_DIR}/${BIN}"
        else
            sudo cp "$BIN_PATH" "${INSTALL_DIR}/${BIN}"
        fi
        echo "installed ${INSTALL_DIR}/${BIN}"
    done

    echo ""
    echo "orch8 ${VERSION_TAG} installed successfully."
    echo ""

    case ":${PATH}:" in
        *:"${INSTALL_DIR}":*) ;;
        *)
            echo "note: ${INSTALL_DIR} is not in your PATH."
            echo "      add the following to your shell profile:"
            echo ""
            echo "      export PATH=\"${INSTALL_DIR}:\$PATH\""
            echo ""
            ;;
    esac

    echo "quick start:"
    echo ""
    echo "  start the engine:"
    echo "    orch8-server"
    echo ""
    echo "  run a sequence:"
    echo "    orch8 run <sequence-file>"
    echo ""
    echo "  documentation:"
    echo "    https://orch8.io/docs"
    echo ""
}

main
