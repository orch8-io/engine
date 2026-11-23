# Migrating an embedded engine to a remote engine

> **Stability: beta**, shipped and tested; may change in a minor release with a changelog note.

`orch8 migrate --to <url>` moves the sequences and **in-flight** instances of
an embedded (SQLite) engine to a remote engine — Orch8 Cloud or your own
server — without restarting runs. Completed history stays where it is.

```bash
orch8 migrate \
  --source ./orch8.db \
  --to https://cloud.orch8.io/api/v1 \
  --target-api-key "$ORCH8_TARGET_API_KEY" \
  --tenant acme \
  --dry-run                 # first: see what would move and what is busy

orch8 migrate --source ./orch8.db --to https://cloud.orch8.io --tenant acme --wait-for-idle
```

Without `--to`, `orch8 migrate --database-url …` still applies Postgres
schema migrations exactly as before.

## What moves

| Moves                                                                                                | Stays on the source                                                          |
| ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| every version of the tenant's sequences                                                              | completed/failed/cancelled instances                                         |
| non-terminal instances (`scheduled`, `running`, `waiting`, `paused`) with their context and metadata | audit history, step logs                                                     |
| execution trees and block outputs, so finished steps never re-run                                    | audit logs and operational history                                          |
| the effect-receipt ledger, including unresolved `dispatched`/`unknown` receipts                      | triggers and credentials (use `orch8 backup`/`restore`)                       |
| pending signals                                                                                      |                                                                              |
| instance KV values, write timestamps and expiry, and checkpoint IDs and payloads                    |                                                                              |

Instance ids are preserved, so clients that stored them keep working against
the target.

Use `--include-terminal-history` to copy completed, failed and cancelled
instances. The target preserves the original run IDs, states, execution
history and stored payloads. These copies remain queryable and cannot be
rescheduled. Repeating the transfer acknowledges the existing history.

Use `--include-cron-schedules` to transfer recurring schedules with the same
IDs, time zones and overlap policies. The source transaction records the
last committed fire and retires the source schedule. After migrating
in-flight runs, the target accepts the snapshot at the next ownership epoch
and resumes the due occurrence. If the migration is interrupted, rerun it
with the same source, destination and migration ID. A schedule with an
active fire lease is reported for retry, and the command exits non-zero
while a schedule or run remains refused.

The CLI reads `GET /api/v1/migrations/capabilities` before writing to either
engine. The target declares its manifest version and accepted wire formats
per state domain. Instances with stored KV keys or checkpoints are refused
when the target lacks those formats. Dry runs check source eligibility
without contacting the target.

## How a run changes hands

For each instance:

1. **Idle check.** An instance that is mid-step (`running`) or holds pending
   or claimed worker tasks is _refused_ — reported, left untouched, and still
   running locally. `--wait-for-idle` waits up to `--idle-timeout-secs`
   (default 300) for it to settle instead.
2. **Fence.** The source ownership record moves to `transferring`: the
   existing continuity fence, so the source scheduler defers the instance and
   refuses worker lease updates. The idle check runs again after fencing; a
   run that raced into a step is unfenced and treated as busy.
3. **Import.** Under the source fence, the CLI reads effect receipts in pages
   until it reaches the end of the ledger. It sends the snapshot to
   `POST /api/v1/migrations/import/v1`. The import accepts up to 100 instances,
   500 sequences, and 16 MiB per request. The CLI sends up to 25 instances per
   batch. The target creates the paused instance and a fenced ownership record
   at **epoch + 1** under a deterministic migration runtime ID. It writes the
   tree, outputs, receipts, and signals before releasing the fence and restoring
   the original state (`running` resumes as `scheduled`). The target commits
   KV keys and checkpoints in the same transaction as the ownership release.
   The target rereads the execution tree, outputs, effect receipts and pending
   signals, then verifies each SHA-256 digest against the source before
   releasing ownership. The CLI checks the returned digests and epoch before
   recording the handoff as committed.
4. **Commit.** The source records the target as owner at the same epoch,
   keeps the record `transferring` (so the local copy can never advance
   again), pauses the local instance and stamps
   `metadata.orch8_migration.phase = "committed"`.

## Resuming and idempotency

Every phase is recorded in the source database and in instance metadata
(`orch8_migration.{id, phase}`), and the migration id defaults to a hash of
source path, target URL and tenant. Re-running the same command:

- skips `committed` instances (`already_migrated` in the report);
- re-exports `fenced` ones; the target answers `already_present` if an
  earlier request imported them, and a half-written import is completed
  without duplicating rows;
- retries previously refused (busy) instances.

The command exits non-zero while any instance is refused, so it is safe to
loop in a script until it succeeds.

## Safety notes

- The source may keep running during the migration. The tool opens the
  SQLite file directly for the short fence/commit writes; SQLite's WAL
  locking makes that safe on a local filesystem (never on NFS/SMB).
- The target rejects batches that mix tenants, instances whose children
  point at another instance, terminal instances, and ids that already exist
  but were not created by this migration (409).
- After cut-over, point clients (signals, approvals, queries) at the target.
  The source keeps the paused copy as a tombstone. Resuming it does nothing:
  its ownership record stays `transferring` to the target runtime, so the
  source scheduler defers it indefinitely.
- Effect receipts keep their state. An effect that was ambiguous (`unknown`)
  before the move stays blocked on the target until a verifier or operator
  resolves it — the move never turns an ambiguous effect into a retry.
- Checkpoint creation and pruning accept `expected_epoch`. Supply the target
  epoch from the import acknowledgement for a migrated instance. A stale
  epoch returns HTTP 409.


## Large live instances

The CLI groups live imports by instance count and encoded bytes. When a
single batch exceeds 8 MiB and the target advertises chunked imports, the
CLI stages it in requests of at most 4 MiB each. The target verifies the
tenant, chunk order, total size, and SHA-256 before starting import. One
staged batch can be up to 256 MiB. Retrying an interrupted migration
reuses immutable chunks already staged for the same batch.


## Referenced artifacts

For a local source artifact store, pass `--source-artifacts-dir PATH`.
The CLI discovers references in context, output, externalized payloads,
pending signals, KV state, and checkpoints. It stages missing binary objects
in the target's tenant namespace using their SHA-256 addresses. An upload
already verified on the target is reused on retry. The target installs the
original artifact IDs before releasing ownership. Set `ORCH8_ENCRYPTION_KEY`
when the source database or artifact store uses field encryption.


## Signal cutover

The source inbox continues accepting signals while ownership is fenced
and the CLI gathers the transfer. Closing the source inbox and rejecting
new writes share SQLite's writer reservation. The CLI then exports the
final pending set with its original signal IDs. A signal sent to the old
endpoint after closure receives a conflict with the target address.
Submit subsequent signals to the target.
