#!/usr/bin/env bash
# Behavioral/static regression tests for the release and migration guard fixes.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_contains() {
    local file="$1"
    local pattern="$2"
    grep -qF -- "$pattern" "$repo_root/$file" \
        || fail "$file is missing required guard: $pattern"
}

assert_not_contains() {
    local file="$1"
    local pattern="$2"
    if grep -qF -- "$pattern" "$repo_root/$file"; then
        fail "$file contains forbidden release behavior: $pattern"
    fi
}

assert_count() {
    local file="$1"
    local pattern="$2"
    local expected="$3"
    local actual
    actual="$(awk -v pattern="$pattern" 'index($0, pattern) { count++ } END { print count + 0 }' "$repo_root/$file")"
    [[ "$actual" == "$expected" ]] \
        || fail "$file contains $actual occurrences of $pattern, expected $expected"
}

expect_failure() {
    local label="$1"
    shift
    if "$@" > /dev/null 2>&1; then
        fail "$label unexpectedly succeeded"
    fi
}

lock_package_version() {
    local lockfile="$1"
    local package="$2"
    awk -v package="$package" '
        /^\[\[package\]\]$/ { in_package = 0 }
        $0 == "name = \"" package "\"" { in_package = 1; next }
        in_package && /^version = "/ {
            gsub(/^version = "|"$/, "")
            print
            exit
        }
    ' "$lockfile"
}

bash -n "$repo_root/scripts/check-migration-immutability.sh"
bash -n "$repo_root/scripts/check-destructive-migrations.sh"
bash -n "$repo_root/scripts/embed-aar-license.sh"

workspace_version="$(awk -F'"' '/^\[workspace\.package\]/{f=1; next} /^\[/{f=0} f && /^version/{print $2; exit}' "$repo_root/Cargo.toml")"
[[ -n "$workspace_version" ]] || fail "could not read the workspace version"
fuzz_timeout="$(awk '/^  fuzz-smoke:/{f=1; next} f && /timeout-minutes:/{print $2; exit}' "$repo_root/.github/workflows/ci.yml")"
[[ "$fuzz_timeout" == "20" ]] || fail "fuzz smoke timeout is $fuzz_timeout minutes, expected 20"
for package in orch8-engine orch8-publisher orch8-storage orch8-types; do
    fuzz_version="$(lock_package_version "$repo_root/fuzz/Cargo.lock" "$package")"
    [[ "$fuzz_version" == "$workspace_version" ]] \
        || fail "fuzz/Cargo.lock has ${package} ${fuzz_version}, expected ${workspace_version}"
done
assert_contains docs/MOBILE_SDK.md "\`${workspace_version}\`"

# Exercise migration guards in an isolated tagged repository.
fixture="$(mktemp -d "${TMPDIR:-/tmp}/orch8-migration-guards.XXXXXX")"
trap 'rm -rf -- "$fixture"' EXIT
mkdir -p "$fixture/migrations" "$fixture/scripts"
cp "$repo_root/scripts/check-migration-immutability.sh" "$fixture/scripts/"
cp "$repo_root/scripts/check-destructive-migrations.sh" "$fixture/scripts/"

# Prove the shipped AAR helper embeds the exact repository license bytes.
cp "$repo_root/LICENSE" "$fixture/aar-payload"
(
    cd "$fixture"
    zip -q sample.aar aar-payload
)
"$repo_root/scripts/embed-aar-license.sh" "$fixture/sample.aar" "$repo_root/LICENSE" > /dev/null
unzip -p "$fixture/sample.aar" META-INF/LICENSE | cmp -s - "$repo_root/LICENSE" \
    || fail "AAR license contents do not match LICENSE"

(
    cd "$fixture"
    git init -q
    git config user.email test@orch8.local
    git config user.name "orch8 guard test"
    echo 'CREATE TABLE shipped(id INTEGER);' > migrations/001_shipped.sql
    git add migrations scripts
    git commit -qm baseline
    git tag v0.1.0

    ./scripts/check-migration-immutability.sh > /dev/null

    echo '-- edited' >> migrations/001_shipped.sql
    expect_failure "edited shipped migration" ./scripts/check-migration-immutability.sh
    git restore migrations/001_shipped.sql

    rm migrations/001_shipped.sql
    expect_failure "deleted shipped migration" ./scripts/check-migration-immutability.sh
    git restore migrations/001_shipped.sql

    echo 'DROP TABLE shipped;' > migrations/002_destructive.sql
    expect_failure "unmarked destructive migration" ./scripts/check-destructive-migrations.sh
    {
        echo '-- allow-destructive: fixture proves reviewed escape hatch'
        echo 'DROP TABLE shipped;'
    } > migrations/002_destructive.sql
    ./scripts/check-destructive-migrations.sh > /dev/null
)

# A tagless CI checkout must fail closed rather than silently skipping.
tagless="$fixture/tagless"
mkdir -p "$tagless/migrations" "$tagless/scripts"
cp "$repo_root/scripts/check-migration-immutability.sh" "$tagless/scripts/"
cp "$repo_root/scripts/check-destructive-migrations.sh" "$tagless/scripts/"
(
    cd "$tagless"
    git init -q
    git config user.email test@orch8.local
    git config user.name "orch8 guard test"
    echo 'SELECT 1;' > migrations/001.sql
    git add .
    git commit -qm tagless
    expect_failure "tagless immutability check under CI" env CI=1 ./scripts/check-migration-immutability.sh
    expect_failure "tagless destructive check under CI" env CI=1 ./scripts/check-destructive-migrations.sh
)

# Release/CI policy is declarative; pin the safety-critical workflow clauses.
assert_contains .github/workflows/release.yml "Verify tag & CI"
assert_contains .github/workflows/release.yml "if [[ \"\$TAG_VERSION\" != \"\$CRATE_VERSION\" ]]"
assert_contains .github/workflows/release.yml 'needs: [verify]'
assert_contains .github/workflows/release.yml 'provenance: true'
assert_contains .github/workflows/release.yml 'Smoke test pushed image'
assert_contains .github/workflows/release.yml "if [[ \"\$TAG\" == *-* ]]"
assert_contains .github/workflows/release.yml "prerelease: \${{ contains(github.ref_name, '-') }}"
assert_contains .github/workflows/release.yml "if: \${{ !contains(github.ref_name, '-') }}"
assert_contains .github/workflows/release.yml 'Verify Cloud management surface'
assert_contains .github/workflows/release.yml 'os: ubuntu-22.04'
assert_contains orch8-grpc/build.rs '.protoc_arg("--experimental_allow_proto3_optional")'
assert_contains .github/workflows/release.yml 'sh ./install.sh --dir "$INSTALL_DIR" --version "$GITHUB_REF_NAME"'
assert_contains .github/workflows/release.yml 'cp LICENSE "$DIR/"'
assert_contains .github/workflows/release.yml 'cp LICENSE docker-ctx/'
assert_contains .github/workflows/release.yml 'test "$IMAGE" -f /usr/share/licenses/orch8/LICENSE'
assert_contains .github/workflows/release.yml 'docker pull --platform "$PLATFORM" "$IMAGE"'
assert_contains .github/workflows/release.yml 'docker run --platform "$PLATFORM" --rm --entrypoint test'
assert_contains .github/workflows/release.yml 'docker run --platform "$PLATFORM" -d --name orch8-release-smoke'
assert_contains .github/workflows/release.yml 'docker buildx imagetools create --tag "ghcr.io/orch8-io/engine:${TAG}"'
assert_not_contains .github/workflows/release.yml 'docker manifest create'
assert_count .github/workflows/release.yml 'docker/setup-buildx-action@b5ca514318bd6ebac0fb2aedd5d36ec1b5c232a2' 2
assert_contains .github/workflows/release.yml 'cp LICENSE build/Orch8Mobile.xcframework/LICENSE'
assert_contains .github/workflows/release.yml 'bash ../../scripts/embed-aar-license.sh'
assert_contains .github/workflows/release.yml 'cp LICENSE bindings/LICENSE'
assert_contains .github/workflows/mobile.yml 'cp LICENSE build/Orch8Mobile.xcframework/LICENSE'
assert_contains .github/workflows/mobile.yml 'bash ../../scripts/embed-aar-license.sh'
assert_contains .github/workflows/mobile.yml 'publish_version:'
assert_contains .github/workflows/mobile.yml "startsWith(github.ref, 'refs/tags/v') || inputs.publish_version != ''"
assert_contains .github/workflows/mobile.yml 'ORCH8_MOBILE_VERSION: ${{ inputs.publish_version || github.ref_name }}'
assert_contains .github/workflows/mobile.yml 'requested_version="${ORCH8_MOBILE_VERSION#v}"'
assert_contains .github/workflows/mobile.yml 'does not match VERSION_NAME=$checked_in_version'
assert_not_contains .github/workflows/mobile.yml 'gh release create'
assert_not_contains .github/workflows/mobile.yml 'gh release upload'
assert_contains packages/flutter/.github/workflows/publish.yml 'workflow_dispatch:'
assert_contains packages/flutter/.github/workflows/publish.yml 'https://pub.dev/api/packages/${PACKAGE}/versions/${VERSION}'
assert_contains packages/flutter/.github/workflows/publish.yml "if: needs.check.outputs.exists != 'true'"
assert_contains packages/react-native/.github/workflows/publish.yml 'workflow_dispatch:'
assert_contains packages/react-native/.github/workflows/publish.yml 'npm view "${PACKAGE}@${VERSION}" version'
assert_contains packages/react-native/.github/workflows/publish.yml "if: steps.registry.outputs.exists != 'true'"
android_version_file="$repo_root/packages/android/orch8-mobile/build.gradle.kts"
release_version_line="$(grep -nF 'providers.environmentVariable("ORCH8_MOBILE_VERSION")' "$android_version_file" | cut -d: -f1)"
fallback_version_line="$(grep -nF 'providers.gradleProperty("VERSION_NAME")' "$android_version_file" | cut -d: -f1)"
[[ -n "$release_version_line" && -n "$fallback_version_line" && "$release_version_line" -lt "$fallback_version_line" ]] \
    || fail "ORCH8_MOBILE_VERSION must take precedence over the checked-in VERSION_NAME"
assert_contains packages/android/orch8-mobile/build.gradle.kts 'name.set("Business Source License 1.1")'
assert_not_contains packages/android/orch8-mobile/build.gradle.kts 'Apache License 2.0'
assert_contains .github/workflows/ci.yml 'Verify Cloud management surface'
assert_contains .github/workflows/ci.yml 'sqlite-smoke:'
assert_contains .github/workflows/ci.yml 'fuzz-smoke:'
assert_contains .github/workflows/ci.yml 'mobile-sync-smoke:'
assert_contains Dockerfile.release "addr=\\\"\${ORCH8_HTTP_ADDR:-127.0.0.1:8080}\\\""
assert_contains Dockerfile.release 'COPY LICENSE /usr/share/licenses/orch8/LICENSE'
assert_contains Dockerfile.release 'LABEL org.opencontainers.image.licenses="BUSL-1.1"'
assert_contains scripts/embed-aar-license.sh "unzip -Z1 \"\$aar_path\" | grep -Fx 'META-INF/LICENSE'"

echo "OK: release and migration review-fix guards passed."
