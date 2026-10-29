# Lossless native JSON through worker recovery

The worker review exposed a durable-report/native-application mismatch. Accepted
reports preserved a NUL-containing delegation result, while unencrypted
PostgreSQL could not put it in native JSONB context. JSONB also normalizes JSON
numbers, invalidating exact payload fingerprints for values such as negative
zero, integral floats and large floating-point values.

## Storage contract

PostgreSQL migration 138 adds explicit, nullable encoding metadata to worker
inputs/context/output/checkpoints, native outputs, engine checkpoints and captured
source snapshots. Existing unencoded rows remain readable. Ordinary JSON values
keep their indexed JSONB representation. Values needing a lossless representation
are serialized as JSON text inside the existing JSONB field and decoded before
being returned through the storage API. Public worker/API/capsule values do not
acquire storage envelopes.

Native contexts keep the runtime section structured for scheduler and ownership
predicates. Application data, config and audit sections independently opt into
JSON text through an engine-owned root marker. No key in application data is
reserved. Context merges decode under the native row lock, merge the logical
value, then encode the full context inside the same transaction. Existing context
CAS and original-claim checks remain in force. Ordinary owned context sections
and snapshots are moved into the encoder without an additional payload copy.

Admission hashes compare the decoded logical/ciphertext task spec, rather than
SQL transport encoding. New attempts, checkpoint updates, hot completion and
output copies retain their per-field codec metadata. Current-output projections,
output batches/history, native checkpoint readers, source-cut replay and portable
restoration use the same logical values. Small externalized values that need
lossless transport use the existing compressed-byte codec instead of JSONB.

Database guards validate the codec/body pairing, reject unknown or malformed
encodings and preserve admitted payload/captured-snapshot codec identity. A
current storage connection explicitly advertises `json_text_v1`. Already-live
older connections cannot claim, rewrite or delete encoded records, including
clearing their encoding metadata. This is an encoding-reader compatibility
floor; it does not authenticate database principals or complete every older
coordinator/worker protocol gate in the original review.

Rollback locks all affected native/journal tables before checking eligibility,
requires read-committed visibility and refuses to discard encoded values or
accepted-result evidence. The historical source-cut rollback regression now
reverses migration 138 before rebuilding older source-cut tables and reapplies
it before invoking the current writer.

## Evidence and remaining work

The unchanged original NUL application regression now passes on plain/encrypted
PostgreSQL and SQLite. A new matrix scenario verifies exact serialized values
through admission, claim, checkpoint, hot completion, native application, output
projection and context merge. Another verifies context/config/audit, engine
checkpoints, externalized payloads, captured snapshots, replay and abort.

The expanded storage run passed 643 top-level cases: 260 storage units, 153
admission/outcome contracts, 185 source cuts, 8 capsule output imports, 6 current
output projections, 4 externalized snapshot cases, 12 atomic SLA/output cases
and 15 migration structure checks. Nested subprocess unit output is not counted
again. Additional native-encoding tests verify empty rollback/reapply, refusal to
downgrade live data, malformed body rejection, immutable admission codecs and
legacy-session mutation/claim/delete refusal.

Final strict seven-package checks passed after the connection floor and
allocation refinements. The complete affected verification checkpoint passed
4,329 distinct selected cases across storage, engine, HTTP/gRPC, mobile, embedded
and cross-runtime runs. The exact CI-failing TypeScript ForEach case also passed
against the current rebuilt server. A gRPC compensation fixture was corrected
to use a genuinely admitted dispatch and its immutable effect binding; all 14
gRPC E2E cases then passed, including factual acceptance without forward
propagation under compensation. This remains draft review work, not a completion
or merge claim. The distributed/mobile/embedded review ledger and
worker failure-resolution follow-up retain the full remaining scope.
