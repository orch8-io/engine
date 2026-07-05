#!/usr/bin/env bash
# check-migration-immutability.sh — fail if a migration file that shipped in
# a prior release has been edited.
#
# sqlx::migrate! records each migration's checksum in `_sqlx_migrations` the
# first time it runs. Editing an already-shipped migration file changes that
# checksum, so any deployment that already applied the old bytes fails to
# boot with `VersionMismatch` on its next upgrade -- an outage that requires
# manual `_sqlx_migrations` surgery to recover from. This has happened once
# already (migrations/010_add_concurrency_and_idempotency.sql, see git log).
#
# This script diffs every tracked migration file against the most recent
# release tag reachable from HEAD. A migration that already existed at that
# tag must be byte-identical today; new migrations (not present at the tag)
# are unrestricted. Run in CI on every PR.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

latest_tag="$(git describe --tags --abbrev=0 2>/dev/null || true)"
if [[ -z "$latest_tag" ]]; then
    echo "No git tags found -- skipping migration immutability check."
    exit 0
fi

echo "Checking migrations/ against tag $latest_tag ..."

failed=0
while IFS= read -r -d '' file; do
    if git cat-file -e "$latest_tag:$file" 2>/dev/null; then
        if ! git diff --quiet "$latest_tag" -- "$file"; then
            echo "FAIL: $file has changed since $latest_tag"
            echo "  A migration that already shipped must never be edited --"
            echo "  ship the change as a new, separately-numbered migration."
            git diff "$latest_tag" -- "$file"
            failed=1
        fi
    fi
done < <(find migrations -maxdepth 1 -name '*.sql' -print0 | sort -z)

if [[ "$failed" -ne 0 ]]; then
    echo
    echo "One or more already-released migrations were modified. See above."
    exit 1
fi

echo "OK: no released migration files were modified."
