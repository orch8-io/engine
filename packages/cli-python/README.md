# orch8-cli

Install the verified native CLI launcher with Python 3.10+ and pipx:

```sh
pipx install orch8-cli
orch8 --version
```

On first use, the launcher downloads the latest GitHub release, verifies its
SHA-256 checksum, and runs the native client. The Python package version does
not pin the native release. Select a release when invoking the launcher:

```sh
ORCH8_VERSION=0.7.1 orch8 --version
```

Binaries are cached in `~/.cache/orch8/<release-tag>/`. A cached release can be
reused, but the default `latest` selection still needs GitHub access to resolve
the tag; pin `ORCH8_VERSION` for repeatable use. `ORCH8_REPOSITORY` selects a
different GitHub release repository. Supported targets are Linux x86_64/aarch64,
macOS x86_64/arm64, and Windows AMD64.

The launcher provides `orch8`; install `orch8-server` separately for local
hosting. See the [project installation guide](../../README.md#install) and
[CLI quick starts](../../docs/quick-starts/README.md).
