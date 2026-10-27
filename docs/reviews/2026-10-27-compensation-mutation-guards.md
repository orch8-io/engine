# Compensation progress keeps its admitted identity

A malformed compare-and-swap update could serialize a foreign tenant into a run
whose indexed tenant remained unchanged. It could also change continuity,
physical source or creation identity, or write a skipped/regressed version.
Validated readers then refused the inconsistent record, leaving recovery stuck.
The adversarial tenant mutation was accepted on plain and encrypted PostgreSQL
and SQLite before the fix; concurrent valid claims already had one winner.

Both backends now validate the caller's tenant and require the next version to
advance by exactly one within the shared signed storage range. Their atomic
UPDATE also binds continuity, saved source instance and exact saved JSON creation
time. A stale version or changed persisted identity writes nothing. Invalid
scope/revision inputs return a constraint error. The existing primary-key and
version predicates keep competing claims mutually exclusive without a separate
read/compare/write window.

Only immutable metadata participates in these added comparisons. Compensation
parameter encryption can use fresh ciphertext on each update; normal encrypted
progress continues to work. The storage trait documents the mutation contract.
No schema or dependency change is needed.

HTTP version advancement now respects the SQL-storable range. A restored run at
the final supported revision returns a clear 409 on claim and remains unchanged,
instead of reaching a storage error and returning 500. That failure was reproduced
before the fix.

The final isolated Rust matrix passes 86 cases: 56 compensation storage cases
across PostgreSQL, SQLite and encryption wrappers; 14 continuity cases; and 16
HTTP safety cases. Mutation coverage includes tenant, continuity, source and
creation identity; unchanged, skipped and regressed revisions; unsigned overflow
and signed storage exhaustion; unchanged persisted records after rejection;
concurrent valid claims; stale writers; and decrypted parameter preservation.

Invocation-specific immutable rule/parameter binding, independent offline-owner
acknowledgement and explicit archival outcomes remain required review work.

All 221 lifecycle/tenant-isolation and 129 invariant/compensation/runtime E2E
cases pass against the exact isolated server and PostgreSQL. Strict all-target
Clippy passes across storage, engine, API, gRPC, CLI and mobile. Formatting,
TypeScript typecheck and documentation checks pass. The prerequisite recovery
and source-fence PRs and this change still require their own CI/merge proofs.
