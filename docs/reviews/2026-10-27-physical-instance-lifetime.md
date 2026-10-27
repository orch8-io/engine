# Physical instance lifetime and acknowledged archival

The source-cut work still needs this contract before its broader portability
surface can be considered complete. This follows the chosen policy: explicit
app/backend acknowledgement releases local admission, while compact deduplication
and outcome evidence remains durable.

## Verified starting point

The current root mobile implementation passed all 701 library tests. A new
four-backend regression then demonstrated that an acknowledged cut stopped
fencing generic owner CAS: a destination owner could point back to the old
physical source, even without advancing its epoch. Both backends now include
acknowledged cuts in that CAS refusal. The complete 159-case source-cut,
continuity and portable-recovery matrix passes, including fresh destinations and
late acknowledgements that preserve newer ownership.

This retained-cut check does not establish permanent physical-ID retirement.
Current cut rows belong to continuity history and are removed when that history
is purged. New-continuity creation and compound import/activation paths also need
a common identity rule; protecting one CAS operation is insufficient.

## Required invariants

- A sealed or retired physical source never gains forward execution authority
  again. Its ID cannot be reassigned to another continuity, account, catalog or
  tenant, including after native/context/history GC.
- A new destination receives a genuinely fresh physical ID. Logical run and
  continuity identity survives a move; it must not be confused with that local
  physical ID.
- An unsealed captured cut can still be explicitly aborted and its original
  source restored. Completed publication cannot be undone by an ordinary retry.
- Repeated delivery can recover its prior operation receipt without restoring
  old context, resetting a live claim, or restarting a retired physical instance.
- Acknowledgement of an older accepted location remains valid historical evidence
  after a later transfer; it must not rewind current authority.
- Existing dispatched/unknown outcome evidence and live callback quarantine are
  retained until settled or explicitly reconciled. Retirement cannot claim that
  an independent offline owner has stopped.

## Storage and concurrency work

Use a compact identity-retirement record independent of task rows, continuity
children and artifact GC. Index physical ID globally. Preserve enough scoped
identity and accepted-transfer metadata for acknowledged archival without storing
workflow input, context, payload bytes or secret keys. Do not silently expire
retirement/deduplication evidence.

Cover plain create, admitted/batch create, snapshot restore, new continuity,
owner CAS, recovery import, activation and source acknowledgement. Scheduler,
native effect admission and every worker polling path must refuse retired
physical sources, including inconsistent legacy state. Existing outcome
settlement must remain recoverable.

Insertion/retirement and eligibility checks must form an atomic contract, not an
unprotected read followed by a write. Audit the existing ownership-before-instance
lock order before adding an ID lock; plain create, import and purge must not
acquire those locks in inverse order. PostgreSQL tests need controlled barriers
that prove behavior after waiting for another transaction's commit. SQLite must
keep its single-writer transaction and account-epoch fence.

A table plus checks in the newest binary is not sufficient: older binaries can
open a newer SQLite schema, and an INSERT/UPDATE can wait on a uniqueness lock
after an earlier retirement check. Preserve the identity rule across downgrade
and prove the post-wait database enforcement with controlled races. If triggers
are used, install their complete bodies separately from the schema string's
semicolon splitter; do not assume a pre-write check has a fresh snapshot after
a lock wait. Preserve security tombstones/guards when application code rolls back.


PostgreSQL migrations and SQLite reopen/backfill must cover existing source cuts.
SQLite's schema initializer splits statements on semicolons, so multi-statement
trigger bodies cannot be inserted into the schema string without addressing that
parser. The current unmerged source-cut versions are PostgreSQL 129 / SQLite 59.

## Archival and SDK work

Represent transferred ownership explicitly in compact receipts/history instead
of calling a paused source completed or a retired account's source cancelled.
A completed compensation remains locally fenced; its acknowledgement/outcome
policy must likewise be explicit. Preserve request/definition fingerprints and
operation-key deduplication through native GC and reopen. Distinguish unknown
outcomes when evidence is absent; never infer clean completion.

Archival must release hot scoped admission only after the trusted app/backend
acknowledges it, preserve foreign-account/catalog isolation, and retain enough
compact evidence for later retries. Review native GC eligibility separately so
that moving out of hot admission does not leak full source payloads forever.

## Verification gates

1. Same/new continuity and foreign-scope attempts to reuse a sealed/acknowledged
   source fail, while fresh destinations and captured abort still work.
2. Purge source native/history rows, reopen, then try ordinary create, batch,
   restore and a different capsule using the old physical ID; no execution or
   partial recovery state is published.
3. Replay the old capsule after destination GC; recover/refuse its old receipt
   without executing the effect again.
4. Race retirement against create/import/owner mutation using deterministic
   PostgreSQL barriers; verify one valid outcome, no lost evidence and no lock
   inversion.
5. Preserve valid late worker/native outcomes and quarantine capacity; refused
   new work must not turn uncertain effects into safe-to-retry work.
6. Upgrade older SQLite/PostgreSQL data, including retained cuts and inconsistent
   legacy ownership, and fail closed without deleting unresolved evidence.
7. Archive transferred/compensated scoped runs, prove hot quota is released,
   then verify compact outcome, deduplication, isolation and replay after GC and
   reopen through the public mobile bindings.

## First implementation and direct reproduction

A real retention regression (four plain/encrypted SQLite/PostgreSQL variants)
sealed a source cut, acknowledged a fresh completed destination, purged both
terminal native rows and the destination's continuity history, and recreated the
source physical ID as scheduled work. All four variants failed at that exact
recreation assertion before the fix. This demonstrates execution identity loss,
not merely absence of a retained-cut lookup.

The current working implementation adds PostgreSQL migration 130 and SQLite
schema 60 with independent compact physical retirement records. Database triggers
record sealed/retired/acknowledged sources, backfill retained cuts, prevent marker
mutation/deletion, refuse native insertions and refuse new or revived ownership
of retired physical IDs across tenant/continuity boundaries. SQLite installs the
complete trigger bodies atomically outside the schema's semicolon splitter.
PostgreSQL downgrade refuses to remove nonempty permanent retirement evidence.
The first full source-cut run passes all 82 cases, including the four original
failures. Expanded admitted/batch/snapshot/foreign-scope and post-uniqueness-wait
coverage is being checked separately.

This is the identity persistence portion of the contract. Native forward
mutation/claim paths, existing inconsistent rows, recovery import/activation,
older SQLite writers/reopen, controlled owner-write races, transfer-aware public
archive receipts and safe payload GC still require their full verification.
Do not infer that the complete lifetime/archival contract is implemented from
native insertion and owner-write guards alone.

The expanded run passes 83 source-cut and 67 portable-recovery tests with no
failures or skips. The recreation regression now also proves admitted creation,
snapshot restore, atomic mixed fresh/retired batches and foreign-tenant/new-
continuity attempts refuse without publishing native context or partial owners.
A controlled PostgreSQL test holds a native uniqueness conflict open while its
transaction inserts retirement evidence, observes the competing insertion's
actual lock wait, then commits. The resumed insertion refuses the retired ID.
This directly verifies the AFTER-trigger snapshot behavior on that native path;
it does not establish all ownership/import/activation race cases.

Boot-time work also needs a bounded migration contract. The first SQLite
implementation backfills retained cuts on every open with INSERT OR IGNORE;
repeating that whole history scan would grow startup cost on a long-running
device. Make successful backfill durable and skip it on subsequent opens while
retaining installed database triggers for older writers, and test interruption
and reopen around that completion marker before treating startup as bounded.

The complete mobile library suite also passes against this first retirement
implementation: 701 tests, no failures or skips (142.82 seconds). Strict storage
Clippy, storage formatting, whitespace checks and documentation validation pass.

## Existing-row admission and one-time SQLite upgrade

A second eight-case four-backend regression retained the old native source and
pending legacy worker task, acknowledged a completed destination, then purged
only destination history. Both native claiming and worker polling accepted the
source again before their independent retirement predicates were added. They
now refuse it while admitting fresh native work and a healthy worker queue.
Both native per-tenant modes and global/tenant/capability worker polling are
covered. Pending legacy evidence remains stored. A shared storage retirement
predicate also lets the engine refuse a retired ID before interpreting missing
continuity as ordinary legacy ownership; encryption and fault decorators forward
that predicate.

The SQLite upgrade regression first failed because a completed backfill was
repeated during reopen. Guard installation, retained-cut backfill and schema-60
completion are now one immediate transaction. Reopen checks the indexed version
marker instead of scanning transfer history. A forced insertion failure proves
that opening fails closed without publishing completion or partial evidence;
after removing that fault, retry backfills the retained cut, and a later reopen
proves duplicate backfill insertion is skipped. This test passes.

Rollback 129 now refuses while migration 130 exists. Tests remove an empty 130
first when checking an empty-install downgrade, and confirm that artifact
cleanup cannot remove permanent security evidence or its source-recording
trigger after a real transfer. This closes a rollback dependency that would
otherwise silently remove the trigger when dropping/recreating the cut table.

Forward state mutation by older writers, captured-cut native/worker admission,
and end-to-end late outcome settlement still need separate controlled checks;
these claim predicates must not be used to justify discarding pending outcomes.

The current combined backend run passes 92 source-cut, 56 compensation and 67
portable-recovery cases (215 total). The engine authority test passes against
SQLite and PostgreSQL: retired IDs are superseded without continuity rows,
foreign tenant labels cannot revive them, fresh IDs remain ordinary legacy work,
and retirement-read failure propagates instead of authorizing execution.

The HTTP late-completion test passes on all four plain/encrypted backends. It
uses a real original worker claim and retained retirement evidence without
continuity history. Completion records the task's outcome, leaves source state
and context untouched and saves no forward block output. A retry with a changed
payload retains the original outcome and still does not propagate. The full
HTTP evidence/recovery suite passes all 20 tests. This preserves observation
without granting new execution authority; receipt-less legacy effects still
need their broader reconciliation/invocation-binding contract.

## Next archival dependency: durable accepted-transfer evidence

A physical retirement marker records the source before acknowledgement and is
immutable. It cannot also be an updatable destination receipt. A separate
append-only compact acknowledgement must retain source/capsule/continuity,
source epoch/runtime and accepted destination epoch/runtime/physical ID after
cut/history collection. Bind it to the immutable sealed manifest identity and
validate tenant plus original source identity in the acknowledgement transaction.
Repeated and historically late acknowledgements may recover the same receipt;
they must not update current ownership or replace earlier accepted evidence.
No input/context, encrypted transport bytes or secret keys belong in this row.

The mobile archive/history surface must represent transferred ownership
explicitly, including after account retirement changes the native source state.
Do not label a continuing remote workflow cancelled merely because its local
source was cancelled. Preserve logical request/deduplication identity separately
from its runtime-local physical ID; foreign-account/catalog and session-epoch
checks still apply before receipt lookup or quota release. The trusted app/backend
acknowledgement releases hot admission while preserving compact outcome and
request/definition fingerprints.

Native payload collection requires a separate proof from hot admission release.
Audit transport/recovery of pending event waits, signals, approvals, timers,
family/execution-tree links and referenced artifacts before making the paused
source collectible. An accepted transfer alone cannot justify deleting local-only
pending state that was never included in the recovered destination. Unresolved
worker/native outcomes and live callback quarantine must also remain recoverable.

Further source inspection confirms a transport gap to resolve before native GC:
`orch8-engine/src/capsule.rs` currently emits empty `pending_waits`,
`pending_signals`, `artifacts` and `stream_cursors` arrays. The captured source
snapshot contains instance/sequence/checkpoint, outputs, tree/payload recovery,
effects and provenance, but no inbox or wait rows. `CapsuleExecutionState` holds
only execution nodes and externalized payloads, and `capsule/state.rs` restores
that subset rather than the pending arrays. The existing wire fields alone do
not prove wait/signal transport. Add real signal/event-wait/deadline recovery
regressions and preserve arrival ordering/deduplication for ingress after capture;
do not replace the requested portable behavior with blanket refusal of waiting
workflows. Define a validated versioned suspension snapshot and atomic recovery
before permitting local-only pending state to be deleted.

Validation for the existing-row admission and bounded-upgrade changes is now
complete at the targeted scope: all 215 combined backend tests, 20 HTTP safety
tests, the engine missing-history/read-failure test, both SQLite point-index
checks and all 701 mobile library tests pass. Strict all-target Clippy passes for
storage, engine, API, gRPC, CLI and mobile; formatting and documentation checks
pass. Native mutation/captured-admission, full suspension transport and archival
requirements above remain open.

## Database source freeze and enqueue/capture ordering

Further controlled baselines found eight captured-source claim revivals, four
permanent native-state revivals, four mixed-batch rescheduling violations and one
PostgreSQL repeatable-read worker omission. The current implementation now:

- skips captured/sealed sources in both native per-tenant modes, every worker
  poll phase and mixed rescheduling batches, preserving healthy work;
- rejects old/unfenced SQL worker-claim writes for captured or retired sources;
- records permanent retirement and quarantines nonterminal native rows together,
  clearing native claim/fire fields while retaining context and worker evidence;
- rejects forward state writes on permanently retired physical rows in the DB;
- refuses new pending/claimed worker insertion after capture or retirement,
  preserving explicit unsealed-abort recovery;
- serializes PostgreSQL enqueue on its native parent before the worker row/index.
  A no-op logical timestamp update changes the parent MVCC version, so a
  repeatable-read capture that waited for this producer cannot omit the newly
  committed worker. The controlled regression originally returned Ready with
  that worker absent; it now aborts and a fresh capture reports the pending task.

The parent write is intentional concurrency evidence, not a timestamp advance.
Worker resolution/retry takes the parent before its worker row/index to avoid
inversion against duplicate enqueue. A deterministic advisory barrier holds the
producer after its parent lock; retry waits there, then both finish and retain
only the new attempt. Review all remaining compound worker instance/node bindings
and child-evidence mutations separately; the worker-specific test does not prove
snapshot freshness for every other source relation.

SQLite schema 61 installs persistent guards and quarantines retained inconsistent
rows once, with completion in the same immediate transaction. Failure after the
quarantine update but before completion rolls the update back and refuses open;
retry clears legacy native claim/fire state without deleting context or compact
retirement evidence. Reopen skips completed quarantine. Installed guards remain
in the database for older writers. These markers still do not establish that an
independent/offline callback has stopped; its valid outcome/lease evidence remains.

Validation so far: 123 source-cut cases in the complete expanded run, plus the
separate schema-61 atomic/reopen case; 56 compensation, 67 portable-recovery and
136 existing storage-integration cases pass. All 20 HTTP safety cases pass after
quarantine/enqueue enforcement, including valid late completion and replay across
all four plain/encrypted backends. Strict all-target lint passes for all six
affected packages. Scheduler retirement and the full mobile rerun are the next
checks. Further captured-owner/direct native-write enforcement, pending-state
transport, accepted-transfer receipts and archival/GC gates remain open.

The scheduler regression also found that a retired row's claim-based predicate
correctly refused a stop transition. Retirement halt now uses the immutable
retirement proof and an unclaimed state CAS from Running to Paused; an already
terminal state still wins. Non-retired superseded rows keep their ordinary claim
fence. All 11 selected scheduler/driver cases pass, including retired inconsistent
running rows on SQLite/PostgreSQL, ordinary handoff parking and transfer deferral.

A stale PostgreSQL capture currently reports a transient serialization failure
at the storage boundary; a fresh caller attempt observes the committed worker
and refuses capture. Automatic service retry/response mapping, other child-row
snapshot races, and all compound worker instance/node bindings need explicit
review. The no-op parent write is an intentional additional PostgreSQL write per
pending/claimed enqueue; its throughput/contended-parent cost remains a workload
measurement item rather than a claimed performance improvement.

DME189 final validation update: all 701 mobile library tests pass after the
schema-61, enqueue and scheduler corrections (109.45 seconds), and both SQLite
point-index tests pass with the captured-cut predicates. Strict all-target lint
passes across the six affected packages. The newer foundation is being saved as
a draft for continued review; it is not declared complete or ready to merge.
Remaining source/transfer gates and the broader goal remain active.

## Draft CI production-feature repair

The first PR365 CI run exposed two build-coverage gaps: production mobile used
an artifact placeholder without memory/filesystem constructors (dev dependency
feature unification hid this locally), and the standalone fuzz lockfile omitted
storage's now-required SHA-256 dependency. A production-only mobile check
reproduced the missing constructors before repair.

Mobile now explicitly requests `artifacts-local`. That feature provides the
existing filesystem/memory implementation without object-store cloud clients;
the existing server `artifacts` feature retains S3. The actual production compiler
feature graph verifies storage has only `artifacts-local` and object_store has
filesystem support without aws/aws-base/cloud-base. All 13 artifact tests pass,
including memory/local round trips and server S3 construction. All four exact
locked dependency metadata checks pass; the fuzz lock repair adds only the
existing SHA-256 dependency reference, with no package version upgrades/removals.
The native production build and refreshed draft CI are being checked separately.

The repaired production mobile library now builds its native library outputs and
passes strict production-only Clippy. The feature trace and all four locked
metadata gates remain green; server/local artifact compatibility passes all 13
cases. Refreshing the draft head is the next step; its remaining behavioral
review gates are unchanged.

The combined server/mobile feature union also passes strict all-target Clippy
across all six packages after the repair. Production mobile check/build/lint,
actual no-cloud feature trace, 13 artifact compatibility cases and all four
locked graph gates are verified before the repaired draft revision is pushed.
