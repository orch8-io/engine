#!/usr/bin/env bash
# Behavioral regression tests for migration safety guards.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"

fail() {
    echo "FAIL: $*" >&2
    exit 1
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
bash -n "$repo_root/scripts/embed-aar-license.sh"

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

echo "OK: migration review-fix guards passed."
