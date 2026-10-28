# Recorded HTTP worker reporter enforcement

An authenticated HTTP regression demonstrated a second stored Worker key in the
same tenant could heartbeat a lease issued to the original key by repeating its
worker label and claim epoch. The response was 200. This bypassed the durable
issuer identity retained by the preceding issuance checkpoint.

HTTP completion, failure, heartbeat, release and artifact upload now check the
retained issuer before mutation. Completion/failure checks run before accepted
or resolution receipt lookup and integration, including when hot task history
is already collected. A different authenticated principal receives 403; the
original reporter remains functional. This does not replace tenant, current
lease, activation/effect or continuity ownership fences.

The storage classifier distinguishes a verified match, a recorded mismatch and
unattributed history. Explicitly unsecured development mode matches only another
unsecured issuer and never inherits an authenticated issuer. Legacy/missing
journals remain explicitly unattributed; the classifier never backfills an
original owner from the current caller.

## Required continuation before merge

This is partial HTTP enforcement, not the completed reporter protocol. Existing
unattributed history still follows the pre-journal transport behavior. A durable,
explicit backend/operator reconciliation or approved handoff protocol must
replace that compatibility gap without permanently stranding offline reporters.
It must preserve original unknown provenance and retain approval/revocation
receipts independently of hot/native history.

gRPC unary and streaming mutations and reconnect payload adoption still need
the same original reporter enforcement; stream-generated internal requests must
retain the authenticated actor rather than default to unsecured identity.
Reconnect selection must avoid another principal's old rows consuming the
adoption window or disclosing payloads. Claim-bound artifact downloads and the
full database/coordinator write floor remain required. Older writers can still
omit issuance evidence; schema140 alone cannot complete rolling authorization.

The full distributed/mobile/embedded review ledger remains authoritative. No
merge or release is justified by this partial guard.

Verification: 413 selected API unit/HTTP cases pass, including a real two-key
transport regression on SQLite and PostgreSQL. It checks heartbeat, completion,
failure, release and upload rejection for the second key, a successful upload
and completion by the original key, and replay by both keys after hot cleanup.
All 40 issued-claim cases pass on plain/encrypted SQLite and PostgreSQL. New
classifier checks preserve Legacy attribution, deny authenticated/unsecured
identity transfer and continue matching original evidence after hot cleanup.

Strict all-target checks pass across eight affected Rust packages.
Generated Rust/JavaScript client contracts and the unchanged API fingerprint pass.
