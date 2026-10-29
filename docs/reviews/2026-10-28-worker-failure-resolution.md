# Worker failure resolution and local delegation follow-up

This checkpoint continues the distributed/mobile/embedded review and does not
make draft PR 365 ready to merge.

## Failure evidence and uncertainty

Lease failure, expiry, timeout and release now settle their original effect in
the same database transaction as the exact claim CAS, native action, retry
reservation and immutable resolution journal. Losing a claim or failing any
subsequent write rolls back the effect settlement. A forged holder annotation
cannot replace the worker actually stored on the winning task.

Retained reports bind tenant, physical instance, task, claim epoch, actual holder,
original invocation and effect proof. The report records whether it came from
strong admission or a legacy binding. It survives hot-task deletion and native
history cleanup. HTTP and gRPC acknowledge an original winning failure after
retry/cleanup, without changing its first reason or retryability. Another tenant,
worker or generation cannot inherit that acknowledgement.

Pending work is certifiably abandoned only if it has never been claimed. A
checkpointed activity requeued as pending still has uncertainty. New attempts
clear the previous activity's checkpoint and sequence. A missing original effect
receipt never authorizes stranded recovery to create a replacement effect.

PostgreSQL migration 137 / SQLite schema 68 retain resolution evidence and guard
permanent task identity and monotonic claims. A deterministic PostgreSQL race
proved that a BEFORE INSERT check alone permits reuse while a concurrent DELETE
is uncommitted. An AFTER INSERT check now rechecks retained identities after the
unique-key wait. It does not reject updates of an existing upsert winner.

The resolution journal stores exact reasons with the JSON-text codec, including
NUL characters. PostgreSQL diagnostic text columns use an escaped spelling;
the immutable report keeps the original body. Encryption seals that body while
preserving its logical and stored fingerprints.

## Local delegation application

CI reproduced duplicate output and duplicate next-step execution in all three
phone delegation scenarios. The local delegation path previously applied native
state separately from the scheduler's accepted-result recovery. Both could win.

Local delegation completion now retains the first delegation identity and result
with its accepted output. This private attachment is independently fingerprinted
and sealed by the encryption wrapper. Local resumption and scheduler recovery
use the same application receipt transaction. Replays apply the original accepted
body, including its original attachment. Ordinary worker output cannot supply
this host-only attachment through HTTP/gRPC output fields.

After this change the three phone delegation E2E scenarios and all 34 distributed
engine tests passed against the supported database matrix. The broader journal,
API and migration validation is still in progress.

## Context replacement

CI's ForEach mutation scenario stalled after an application-context replacement
reset the engine's run identity. The context endpoint now retains the stored
runtime section and conditionally updates against the observed timestamp, with
bounded retries. It checks quota after restoring the real runtime. The endpoint
continues replacing caller-controlled context sections. A regression checks both
an omitted runtime and an explicitly forged run/mode; seven context tests passed.
Fourteen distributed HTTP tests and six distributed API E2E cases also passed.
The original TypeScript ForEach scenario also passed against the rebuilt
schema-138 server.

## Open findings and verification

The cold-delegation regression exposed PostgreSQL's inability to represent NUL
inside native JSONB, even though its accepted attachment remained durable. This
has now been fixed with versioned native JSON encoding. The unchanged regression
passes on all four backend variants, including unencrypted PostgreSQL. Native
context, inputs, outputs, checkpoints and captured snapshots retain the original
strings and number representations; no accepted value is truncated or mutated.
See [the native JSON encoding review](2026-10-28-native-json-encoding.md) for the
contract, compatibility guards and verification evidence.

The final selected storage run passed 825 cases covering the extended lossless
paths, source snapshots, invocation/resolution contracts and migration guards. Final strict checks passed on all
seven affected packages. Broader engine, API, mobile, embedded and cross-runtime
verification subsequently passed: 4,329 distinct selected cases across the affected runs. This checkpoint
does not imply completion of the full review or readiness to merge.

The original review ledger still governs completion: paused failure recovery,
server mailbox/proxy integration, later factual outcomes after uncertain failure,
issued-claim principal retention, complete rolling protocol floors, poisoned
recovery backoff/disposition, native payload compaction after acknowledgement
and SDK/platform integration remain open. Nothing has been merged or released at this checkpoint.

## Verification and cleanup

The final checkpoint passes 4,329 distinct selected cases across affected
verification runs: 825 storage, 2,145 engine units, 34 distributed-engine cases,
405 HTTP units/integration cases, 149 gRPC units, 14 gRPC E2E cases, 701 mobile
units, 39 embedded units, 16 cross-runtime E2E/harness cases and the exact
TypeScript ForEach scenario. PostgreSQL matrix cases are required and include
plain/encrypted PostgreSQL and SQLite. Strict all-target checks pass on the seven
affected packages; the genuine-dispatch gRPC fixture also passes targeted strict
checks. This is selected affected-path coverage, not a claim that every workspace
or platform gate has completed.

Both published CI failures have locally verified fixes; CI must validate the
newly published exact head before any merge. Completed integration test
executables were removed in the preceding cleanup: 20 files, 1,932,926,560 bytes.
Source, libraries, application binaries and verification logs remain intact.

The mobile acknowledged catalog-run archive API already exists and its tests pass.
The remaining archive work concerns native outcome/report payload compaction,
transported history, and compatibility across retained claims and key rotation;
it must not duplicate the existing catalog admission/archive lifecycle.
