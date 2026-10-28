# Durable worker result application

This checkpoint advances the distributed/mobile/embedded review; it does not
complete that review or make the draft ready to merge.

## Contract

Acceptance and application are separate durable facts. Acceptance commits the
original result, original effect settlement, exact winning claim and attempt
evidence together. Application commits the native output, merged context, exact
node completion, native transition and an immutable application receipt together.
A lost response never authorizes replacing the accepted result.

HTTP and gRPC now use the shared storage application path instead of selecting a
current node by block name. Application checks tenant, physical instance,
sequence, run, execution mode, continuity ID/epoch/runtime and original node
activation (or flat-step start baseline). A changed or ambiguous invocation is
retained for reconciliation. It is not advanced using the latest block alias.

The merge starts from fresh logical context, checks its configured size and
compares the entire stored context inside the transaction. A context race causes
a bounded reload/remerge. Encryption wrappers compare the logical snapshot but
carry the actual stored snapshot into that comparison; new output is always
sealed, including authentic application ciphertext. Paused parents remain paused.
A running scheduler claim is deferred rather than invalidated.

## Recovery and retention

Both continuous scheduling and explicit mobile/embedded ticks drain accepted
results independently of worker retries and hot task history. The small durable
queue uses a monotonically increasing turn, not device wall time. Deferred and
malformed legacy entries rotate, so they do not permanently hide younger results.
Application removes its queue entry in the same transaction as its receipt.

PostgreSQL uses nonblocking namespace acquisition and skips busy owner, native,
node and queue locks. A suspended scanner must not hold up scheduling. SQLite
uses its existing immediate writer transaction. Namespace/owner-before-native
ordering matches admission and source capture.

Migration 136 and SQLite schema 67 install application receipts, recovery
scheduling and database guards. Journal insertion itself enqueues recovery, so a
supported older writer cannot omit the queue. Unapplied results retain native
history and refuse captured/sealed capsule cuts, including through raw older
write paths. Physical-instance lookups have an index independent of tenant-leading
indexes. Downgrade refuses pending or applied result evidence.

A controlled PostgreSQL downgrade race reproduced deletion of a recovery entry
that committed while rollback waited on the journal table. Rollback now locks the
journal before its eligibility check, and refuses repeatable-read/serializable
snapshots that cannot provide fresh statement visibility. The regression observes
the actual waiting lock before allowing acceptance to commit.

These guards do not establish the complete rolling coordinator/worker protocol
floor. An older coordinator's native application path still needs quarantine or
an enforced protocol version; legacy application without a receipt cannot be
backfilled by guessing from a current node or output.

## Compatibility findings

The published checkpoint's CI found an outdated gRPC expectation: an exact
winning completion replay must acknowledge the first result, rather than reject
a completed lease. The E2E assertion now checks that replay preserves the first
body and that another claim cannot inherit its acknowledgement.

The rolling-upgrade fixture also tried to erase immutable fields on a currently
admitted task. It now constructs a historical pre-admission snapshot in its
scratch database, retains the original bound invocation and restores every
immutable-evidence trigger before commit. Production guards remain enabled.
That corrected fixture exposed a real recovery defect: normal source checking
refused a missing hot effect ID before the reaper could restore it. The recovery
path may preview that field only while checking the retained invocation and
original receipt/request/destination proof. Ordinary mutations retain their
strict hot-field checks; the immutable invocation is not rewritten.

## Remaining review gates

- Atomically retain failure/uncertainty evidence with lease resolution before a
  retry removes hot history; preserve subsequent factual success evidence.
- Purpose-bound delegation admission and application, including the first
  accepted delegation metadata, mailbox outcomes and local parent resumption.
- Durable issued-claim/reporter principal scope after hot/native cleanup.
- Explicit reconciliation/disposition for superseded or uncertain legacy facts,
  and a complete rolling protocol floor.
- App/backend acknowledged archive with bounded payload compaction and retained
  deduplication/outcome metadata; key rotation and backup compatibility.
- Continue the original review matrix, including account/catalog isolation,
  public SDK integration, current-run recovery under manual rescheduling,
  platform runtime parity and independent offline ownership.

Verification logs and exact source manifests are kept with the local checkpoint.
The distributed/mobile review remains the complete scope and completion ledger.

## Verification checkpoint

Across the affected verification runs, 3,677 distinct test cases passed:
481 storage contracts (including 144 admission/outcome, 111 invocation, 181
source-cut, 20 resolution, 10 GC and 15 migration cases), 2,145 engine units,
30 distributed engine scenarios, 149 gRPC, 134 HTTP completion-related cases,
701 mobile library cases, 20 embedded facade cases and 17 selected cross-runtime
E2E cases. Backend-matrix cases require PostgreSQL and include encrypted/plain
PostgreSQL and SQLite. E2E cases exercise both database backends.

The earlier broad verification precedes the final recovery lock and rollback
fixes. Final changed paths were rechecked with the storage matrix, distributed
engine scenarios, gRPC, HTTP and repaired distributed API E2E suite; the final
rollback change reran all 144 admission/outcome and 15 migration cases. This is
selected affected coverage, not a claim that every project/E2E test has run on
this checkpoint. The published revision still requires its own CI result.
