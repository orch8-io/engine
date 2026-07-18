#!/usr/bin/env bash
# check-destructive-migrations.sh — fail if a NEW forward migration contains
# destructive DDL without an explicit, in-file justification.
#
# Destructive statements (DROP TABLE, DROP COLUMN, ALTER TABLE ... DROP)
# silently discard production data on upgrade. Occasionally they are
# justified -- dropping a table that never carried production data, swapping
# a mis-scoped constraint -- so the escape hatch is a marker comment in the
# migration file itself, reviewed alongside the statement:
#
#   -- allow-destructive: <reason>
#
# Only migrations that have NOT shipped yet (absent from the most recent
# release tag) are scanned. Shipped migrations are frozen byte-for-byte by
# check-migration-immutability.sh -- even adding a marker comment would
# change the sqlx checksum and break upgrades with VersionMismatch -- which
# is why the pre-existing DROPs in 026/030/034 legitimately carry no marker.
# Rollback scripts under migrations/down/ are out of scope: they drop by
# design.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

latest_tag="$(git describe --tags --abbrev=0 2>/dev/null || true)"
if [[ -z "$latest_tag" ]]; then
    if [[ -n "${CI:-}" ]]; then
        echo "FAIL: no git tag reachable from HEAD -- cannot tell new migrations" >&2
        echo "  from shipped ones. Fetch tags in the checkout step (fetch-depth: 0)." >&2
        exit 1
    fi
    echo "No git tags found -- skipping destructive migration lint."
    exit 0
fi

pattern='DROP[[:space:]]+TABLE|DROP[[:space:]]+COLUMN|ALTER[[:space:]]+TABLE[^;]*DROP'

failed=0
while IFS= read -r -d '' file; do
    # Already shipped -> frozen by the immutability check; skip.
    if git cat-file -e "$latest_tag:$file" 2>/dev/null; then
        continue
    fi
    if grep -qiE "$pattern" "$file" && ! grep -q -- '-- allow-destructive:' "$file"; then
        echo "FAIL: $file contains destructive DDL without an allowlist marker:"
        grep -niE "$pattern" "$file" | sed 's/^/  /'
        echo "  Add a '-- allow-destructive: <reason>' comment justifying the data loss."
        failed=1
    fi
done < <(find migrations -maxdepth 1 -name '*.sql' -print0 | sort -z)

if [[ "$failed" -ne 0 ]]; then
    echo
    echo "One or more new migrations contain unmarked destructive DDL. See above."
    exit 1
fi

echo "OK: no unmarked destructive DDL in new migrations."
