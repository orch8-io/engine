# @orch8/cli

Install the native Orch8 CLI without compiling Rust. Requires Node.js 18+.

```sh
npm install --global @orch8/cli
orch8 --version
```

The postinstall hook downloads the latest GitHub release for the current
platform and verifies its SHA-256 checksum. The npm package version does not
pin the downloaded binary. To select a specific release:

```sh
ORCH8_VERSION=0.7.1 npm install --global @orch8/cli
```

Supported targets are Linux and macOS on x64/arm64, and Windows x64. GitHub
network access and `tar` (Unix) or PowerShell (Windows) are required for
installation. `ORCH8_REPOSITORY` selects a different release repository;
`ORCH8_SKIP_BINARY_INSTALL=1` skips the download for packaging checks.

The shim installs the `orch8` client; install `orch8-server` separately when
hosting an engine. See the [project installation guide](../../README.md#install)
and [CLI quick starts](../../docs/quick-starts/README.md).
