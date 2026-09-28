# Mobile releasing

How a tagged engine release reaches the mobile package channels, which
secrets each channel needs, and how to recover a partial release. For
installing the SDK, see [Mobile SDK](MOBILE_SDK.md#10-minute-install).

## Pipeline

Pushing a `vX.Y.Z` tag runs `.github/workflows/release.yml`:

1. `ios`, `android`, `bindings` build `liborch8_mobile` for every target and
   generate the UniFFI Swift and Kotlin bindings.
2. `xcframework`, `aar`, `bindings-archive` package them. The GitHub release
   gets `Orch8Mobile-vX.Y.Z.xcframework.zip`, `orch8-mobile-vX.Y.Z.aar`,
   `orch8-bindings-vX.Y.Z.zip` and `Orch8KmpBridge-vX.Y.Z.swift`, each with a
   `.sha256`.
3. `mobile-distribution` calls `.github/workflows/mobile-distribution.yml`,
   which publishes from those release assets (never from a rebuild):

| Job | Publishes | Secret | Runner |
|---|---|---|---|
| `swift-package` | Commit to `orch8-io/orch8-mobile-swift` `main` (Package.swift url + checksum, podspec version, Sources with the release's generated bindings), tag `X.Y.Z`, GitHub release with `Orch8MobileCocoaPods-X.Y.Z.zip` | `ORCH8_MOBILE_SWIFT_TOKEN` | macOS |
| `cocoapods` | `pod trunk push Orch8Mobile.podspec` from the `X.Y.Z` tag of orch8-mobile-swift | `COCOAPODS_TRUNK_TOKEN` | macOS |
| `maven` | `io/orch8/orch8-mobile/X.Y.Z/` (AAR + POM + checksums) and `maven-metadata.xml` in `orch8-io/maven` | `ORCH8_MAVEN_TOKEN` | Linux |
| `kmp` | `io.orch8:orch8-kmp` (Android + iOS variants, Gradle module metadata) in `orch8-io/maven` | `ORCH8_MAVEN_TOKEN` | macOS |

Ordering: `cocoapods` runs after `swift-package`, because the podspec
downloads its source archive from the orch8-mobile-swift release. `kmp` runs
after `maven`, because `orch8-kmp`'s Android variant depends on
`io.orch8:orch8-mobile` at the same version; the KMP build resolves it from
the local `orch8-io/maven` checkout, so it does not wait for
raw.githubusercontent.com caches.

The JVM target of `packages/kmp` runs the common tests but is not published:
it has no packaged host library.

The npm wrappers (`@orch8.io/expo` in `orch8-io/sdk-expo`,
`@orch8.io/react-native-orch8`) and `orch8_flutter` are released from their
own repositories after these channels are live, with their native pin
(`orch8NativeVersion` in the Expo `package.json`, `Orch8Mobile` in the
podspecs, `io.orch8:orch8-mobile` in Gradle) bumped to `X.Y.Z`.

## Secrets

Add these as repository (or `release` environment) secrets on `orch8-io/engine`.
Each one is optional: when it is missing, its job logs a notice and skips, so
forks and dry runs stay green.

| Secret | What it must be |
|---|---|
| `ORCH8_MOBILE_SWIFT_TOKEN` | Fine-grained PAT (or GitHub App token) with **Contents: read and write** on `orch8-io/orch8-mobile-swift`. Used to push `main`, push the tag, and create the release. If `main` is branch-protected, allow this token to bypass, since SwiftPM needs the tag on the commit that carries the new checksum. |
| `COCOAPODS_TRUNK_TOKEN` | A CocoaPods trunk session token of an `Orch8Mobile` owner. Create one with `pod trunk register <email> 'Orch8' --description='engine CI'`, confirm the email, then copy the `password` for `trunk.cocoapods.org` from `~/.netrc`. Sessions expire; `pod trunk me` shows the expiry. |
| `ORCH8_MAVEN_TOKEN` | Fine-grained PAT with **Contents: read and write** on `orch8-io/maven`. Used by both the `maven` and `kmp` jobs. |

`HOMEBREW_TAP_TOKEN` and `NPM_TOKEN` (CLI channels) are unrelated to mobile.

## Guarantees

- **Immutable versions.** `scripts/publish-maven-aar.py` refuses to replace
  an existing Maven version with different bytes and is a no-op for identical
  bytes. The `kmp` job skips when `io/orch8/orch8-kmp/X.Y.Z` exists. The
  Swift job never moves an existing tag. The CocoaPods job skips when trunk
  already lists the version.
- **Verified inputs.** Every downloaded release asset is checked against its
  `.sha256`. `scripts/sync-swift-distribution.sh` derives the SwiftPM
  checksum from the XCFramework zip itself and runs
  `scripts/check-xcframework.sh` on the CocoaPods archive.
- **Bindings match the binary.** The Swift package publishes the bindings
  generated in the same release run as the XCFramework. If the checked-in
  `packages/swift/Sources/Orch8Mobile/Orch8MobileBindings.swift` differs, the
  job warns and publishes the generated file.
- **Reproducible POM.** The `orch8-mobile` POM lists the `implementation(...)`
  dependencies of `packages/android/orch8-mobile/build.gradle.kts` as runtime
  dependencies. For `0.7.1` the generated POM is byte-identical to the one
  already published.

## Recovering a partial release

Everything is idempotent, so re-run the whole distribution:

**Actions → Mobile distribution → Run workflow**, `tag: vX.Y.Z`.

Already-published channels are detected and skipped. To publish a single
channel by hand from a checkout of the tag:

```bash
# Maven (orch8-mobile)
gh release download vX.Y.Z --repo orch8-io/engine --pattern 'orch8-mobile-vX.Y.Z.aar*'
git clone https://github.com/orch8-io/maven
scripts/publish-maven-aar.py --repo maven --version X.Y.Z --aar orch8-mobile-vX.Y.Z.aar

# orch8-mobile-swift working tree + CocoaPods archive (macOS)
gh release download vX.Y.Z --repo orch8-io/engine \
  --pattern 'Orch8Mobile-vX.Y.Z.xcframework.zip' --pattern 'orch8-bindings-vX.Y.Z.zip'
git clone https://github.com/orch8-io/orch8-mobile-swift
scripts/sync-swift-distribution.sh orch8-mobile-swift X.Y.Z \
  Orch8Mobile-vX.Y.Z.xcframework.zip orch8-bindings-vX.Y.Z.zip dist

# orch8-kmp (macOS with Android SDK + Xcode)
cd packages/kmp
ORCH8_MOBILE_VERSION=vX.Y.Z gradle publishAllPublicationsToOrch8DistRepository \
  -Porch8.kmp.targets=android,ios -Porch8.dist.repo=/path/to/maven
```

Review the resulting diffs, then commit, tag and push them yourself.

## Checks before tagging

- `scripts/check-sdk-versions.sh`: every mobile package, including
  `packages/kmp/gradle.properties`, declares the same `VERSION_NAME`.
- `scripts/check-sdk-packaging.sh`: wrapper podspecs and Gradle files point
  at resolvable, version-aligned native packages.
- `packages/swift/Sources/Orch8Mobile/Orch8Mobile.swift` declares
  `orch8MobileVersion = "X.Y.Z"`; `sync-swift-distribution.sh` fails otherwise.
