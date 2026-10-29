# Atomic operator effect resolution

The existing `POST /continuity/effects/{id}/resolve` changed an effect receipt
with a standalone state CAS, then separately appended provenance. An injected
provenance insertion failure reproduced an error response with the receipt
already Committed and its provider confirmation changed. Retrying could not
recover the original transition because the receipt had moved on.

The endpoint now constructs its existing digest/signature boundary before one
storage transaction. Both PostgreSQL and SQLite compare the entire expected
receipt, preserve immutable receipt identity/request fields, check the current
continuity epoch, and append against the exact chain predecessor. Receipt
settlement and provenance insert commit together. PostgreSQL locks continuity
before the receipt, matching dispatch/handoff order; SQLite starts an immediate
transaction. Conflicting receipt evidence or ownership epochs require explicit
reload. Only an optimistic chain-head conflict retries automatically, up to eight
attempts, retaining the same receipt snapshot and proposed result throughout.

This also prevents a concurrent provider confirmation from being overwritten
when the state string stays unchanged. A consumed predecessor or uniqueness
collision after the receipt update rolls back the whole operation. Encrypted
wrappers forward this metadata transaction; no application body is introduced.
The request/response format and existing provenance digest/signing format remain
unchanged. No schema migration is needed.

Verification passes 22 API evidence cases, 14 continuity storage cases and 16 matrix cases across
plain/encrypted SQLite/PostgreSQL. The API fault-injection regression failed
before the repair and passes afterwards; it also retries after removing the
fault and verifies the resulting chain. Matrix cases cover injected insert
failure, protected receipt identity, incorrect digest/tenant/epoch, concurrent
same-state provider evidence, competing resolutions of two receipts in one
chain, a primary-key collision after the receipt UPDATE, and a database trigger
suppressing that UPDATE. The latter reproduced an Applied result with only
provenance written; the transaction now requires exactly one receipt update.
A clock-skew regression also reproduced refusal of a receipt written by an
ahead replica; resolution now preserves the stored timestamp floor and still
commits a valid provenance chain.

Strict all-target Clippy passes for all eight affected storage consumers.
The standalone mobile library check, generated Rust/JavaScript client contract,
OpenAPI fingerprint, formatting and diff checks pass.

This repairs a prerequisite of explicit backend reconciliation. It does not
create a durable operator approval or transfer a worker's original identity.
Legacy/unattributed worker reports still need a separate retained decision and
functional reconciliation path. Handoff/revocation, the complete coordinator
write floor, claim-bound artifact reads, late outcomes and every other open
item in the distributed/mobile/embedded ledger remain required. This is not a
completion or merge-ready claim.
