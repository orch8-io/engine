# Complete effect evidence for long-running executions

Receipt lists are capped at 10,000 rows. Safety verdicts must cover the complete
ledger rather than infer safety from a prefix. Regressions reproduced false Pass
results in guard reports and manual invariant evaluation, and a compatible handoff
preview despite late unresolved receipts.

SQLite and PostgreSQL now compute complete duplicate/state counts within one SQL
statement per query. Duplicate witnesses, unresolved receipts and unknown-request
witnesses are bounded to 16 entries each. Plain/encrypted wrappers forward the
operations; unsupported custom adapters fail explicitly. Missing/unrecognized
receipt states cannot silently become safe evidence.

Guard reports preserve atomic duplicate refusal evidence, including an earlier
Prepared attempt. Manual receipt-rule evaluation uses complete backend evidence
and caches queries by kind within each batch. Other invariant rules retain their
existing supplied evidence. All verdicts are computed before journal appends.

Handoff compatibility uses the full unresolved count. Preview responses add
`unresolved_effect_count` and `unresolved_effects_truncated`; `unresolved_effects`
is a bounded sample. Preview hashes bind the count and sample. Admission rebuilds
the preview, so late uncertainty invalidates an earlier safe preview.

All 54 targeted tests passed on an isolated checkout based on main: 28 storage
backend cases, 19 engine guard cases, and 7 real HTTP cases. Strict all-target
Clippy for storage/engine/API, formatting, documentation and whitespace checks
passed. Tests cover histories
above 10,000 receipts, late duplicate/unknown effects, state/kind/destination and
tenant isolation, proposal identity replacement, bounded evidence, successful
resolution/compensation, and uncertainty after a valid handoff preview.

The aggregate fingerprint binds counts and sampled witnesses; it is not a digest
of every raw receipt. Queries provide point-in-time evidence, not serialization
with all future effect writes. Remaining capped consumers (receipt display,
compensation planning, extracted fixtures and policy definition enumeration) are
separate follow-up work. This change does not include the pending source-cut,
mobile scope/archive, or migration 129 implementation.
