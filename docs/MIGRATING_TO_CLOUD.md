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

| Moves | Stays on the source |
|---|---|
| every version of the tenant's sequences | completed/failed/cancelled instances |
| non-terminal instances (`scheduled`, `running`, `waiting`, `paused`) with their context and metadata | KV state, checkpoints, audit history, step logs |
| execution trees and block outputs, so finished steps never re-run | artifact blobs (externalized payload refs must be reachable from the target) |
| the effect-receipt ledger, including unresolved `dispatched`/`unknown` receipts | triggers, cron schedules, credentials (use `orch8 backup`/`restore`) |
| pending signals | |

Instance ids are preserved, so clients that stored them keep working against
the target.

## How a run changes hands

For each instance:

1. **Idle check.** An instance that is mid-step (`running`) or holds pending
   or claimed worker tasks is *refused* — reported, left untouched, and still
   running locally. `--wait-for-idle` waits up to `--idle-timeout-secs`
   (default 300) for it to settle instead.
2. **Fence.** The source ownership record moves to `transferring`: the
   existing continuity fence, so the source scheduler defers the instance and
   refuses worker lease updates. The idle check runs again after fencing; a
   run that raced into a step is unfenced and treated as busy.
3. **Import.** The CLI posts the snapshot to the target's
   `POST /api/v1/migrations/import` (at most 100 instances and 500 sequences
   per request; the CLI sends 25 instances per batch). The target creates the
   instance paused, writes its tree, outputs, receipts, signals and an
   ownership record at **epoch + 1** under a deterministic migration runtime
   id, then releases it to its original state (`running` resumes as
   `scheduled`).
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
