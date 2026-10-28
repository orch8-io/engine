# Explicit legacy completion decisions

The new Operator-only HTTP operation accepts one confirmed legacy completion
without inventing an original authenticated claimant. Its immutable accepted
outcome retains a versioned decision containing approval ID, verified actor,
tenant/task/exact claim, opaque backend evidence reference, output digest and
legacy/missing/unsecured history classification. It accepts epoch-zero history
as epoch zero. Known authenticated issuers require ordinary reporting or a
separate explicit handoff; they cannot be reclassified as legacy.

The decision is part of the same transaction as the accepted body, effect
settlement, hot completion and lifecycle event. Rejection rolls all of these
back. Stored-key Operator capability, tenant, revocation and expiry are read
under a key lock at acceptance. A real PostgreSQL lock-wait regression found
that checking expiry in the SELECT predicate before waiting could approve an
expired key. Time is now read after the row lock is acquired. Root identity is
derived from the existing private verified-root marker, never insecure Admin
mode or request JSON.

A partial unique index permanently binds each approval UUID to one report.
A four-backend regression demonstrated two tasks with matching worker label and
epoch could otherwise consume the same acknowledgement. Conflicts now roll the
second task back and return a conflict. Identical concurrent retries share one
receipt and one completion event. Changed approval, evidence reference or output
does not overwrite the first result.

Reconciled records use `json_text_reconciled_v1`; old readers cannot silently
ignore a decision. Current readers require the codec and decision together,
validate scope/version, and reject unknown principal scope. PostgreSQL migration
141 and SQLite schema 71 install the codec/index guard; PostgreSQL downgrade
refuses retained decisions. The validator preserves migration 138's lossless
JSON text validation, including NUL and float representations. The matrix caught
an initially copied older JSONB validator that rejected plain PostgreSQL NUL
output; both migration directions now preserve the current JSON validator.

Verification includes authenticated HTTP cases on plain/encrypted SQLite and
PostgreSQL, role/tenant/forgery rejection, expired-key lock waits, rollback after
insert failure, permanent approval binding, epoch zero, native application and
replay after hot/native cleanup. Codec unit cases cover future versions/scope
and missing or downgraded metadata. A file-backed SQLite case exercises upgrade
from the preceding guard and reopening the retained decision. Final production verification passes 899 distinct selected cases: 368 API units,
261 storage units, 45 reconciliation HTTP cases, 153 admissions, 56 issued claims,
15 migration checks and one file-backed upgrade/reopen case. Strict all-target
checks across eight affected consumers, standalone mobile compilation, generated
Rust/JavaScript clients, the OpenAPI fingerprint, formatting and a fresh server
build pass.

This is the first functional explicit completion path, not closure of the legacy
protocol gate. Existing unattributed callback compatibility remains. Failures,
late reports without a held task, independent review/read of already accepted old
facts after approver-key retirement, approved continuing handoff/revocation, complete coordinator/database
write enforcement and functional claim-bound artifacts remain open alongside
all original distributed/mobile/embedded review requirements. This work does
not authorize merging the draft or publishing a release.
