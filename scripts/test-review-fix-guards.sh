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

expect_failure() {
    local label="$1"
    shift
    if "$@" > /dev/null 2>&1; then
        fail "$label unexpectedly succeeded"
    fi
}

bash -n "$repo_root/scripts/check-migration-immutability.sh"
bash -n "$repo_root/scripts/check-destructive-migrations.sh"

# Exercise migration guards in an isolated tagged repository.
fixture="$(mktemp -d "${TMPDIR:-/tmp}/orch8-migration-guards.XXXXXX")"
trap 'rm -rf -- "$fixture"' EXIT
mkdir -p "$fixture/migrations" "$fixture/scripts"
cp "$repo_root/scripts/check-migration-immutability.sh" "$fixture/scripts/"
cp "$repo_root/scripts/check-destructive-migrations.sh" "$fixture/scripts/"
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
assert_contains .github/workflows/ci.yml 'sqlite-smoke:'
assert_contains .github/workflows/ci.yml 'fuzz-smoke:'
assert_contains .github/workflows/ci.yml 'mobile-sync-smoke:'
assert_contains Dockerfile.release "addr=\\\"\${ORCH8_HTTP_ADDR:-127.0.0.1:8080}\\\""

echo "OK: release and migration review-fix guards passed."
