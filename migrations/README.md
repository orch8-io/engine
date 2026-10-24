# Migrations

These are PostgreSQL migrations embedded with `sqlx::migrate!`. Apply them in a
deployment step with:

```bash
orch8 migrate --database-url "$ORCH8_DATABASE_URL"
```

Single-node development can instead enable `database.run_migrations`; rolling
deployments should run the CLI command once before starting the new fleet.
`sqlx` records each migration's checksum in `_sqlx_migrations` the first time it
runs. SQLite uses its bundled schema and versioned upgrade path rather than
these PostgreSQL files.

## Rule: never edit a migration once it has shipped in a release

Editing an already-released `.sql` file changes its checksum. Any deployment
that already applied the old bytes then fails to boot on its next upgrade
with `VersionMismatch` — an outage that requires manual `_sqlx_migrations`
surgery to recover from. This has happened once already
(`010_add_concurrency_and_idempotency.sql` was edited in place in the
`v0.5.0` release commit).

If a migration needs to change after it has shipped, add a new,
separately-numbered migration instead.

`scripts/check-migration-immutability.sh` enforces this in CI by diffing
every migration file against the most recent release tag; it fails the build
if any file that existed at that tag has changed.

## Rule: migrations must be safe for a rolling deploy

`orch8 migrate` runs before the new fleet starts, so for a while the _old_
binaries run against the _new_ schema. A migration must therefore:

- never change a column type the previous release decodes (sqlx is strict:
  an `i32` field fails to decode an `INT8` column), and never drop or rename
  a column the previous release still reads;
- avoid table rewrites (`ALTER COLUMN ... TYPE` between incompatible types,
  volatile defaults) on hot tables such as `task_instances`, which hold an
  ACCESS EXCLUSIVE lock for the duration of the rewrite. Do NOT use
  `CREATE INDEX CONCURRENTLY` in a migration: it waits for every open
  transaction, including other nodes blocked on sqlx's migration advisory
  lock, so simultaneous migrators deadlock. Use a plain
  `CREATE INDEX IF NOT EXISTS` and document the equivalent `CONCURRENTLY`
  statement operators can run online before upgrading (see 084).

If a change cannot meet this (e.g. a real type widening), it needs an
expand/contract sequence across releases (add a new column, dual-write,
backfill, switch reads, drop the old column), or a documented stop-the-world
upgrade.

### Note: lock-heavy migrations 103-105 on large installs

103 (backfill `worker_tasks.tenant_id` in one transaction plus plain index
builds), 104 (three plain index builds on `task_instances`) and 105 (`DROP
INDEX` under an exclusive lock) hold locks proportional to table size. They
are not rewritten (checksums); instead `orch8 migrate` / `run_migrations`
logs a startup warning when they are pending and the tables are estimated at
1M+ rows. Operators of such installs should run the `CREATE INDEX
CONCURRENTLY` statements from the 103/104 header comments online first
(the migrations' `IF NOT EXISTS` then make the indexes a no-op), and apply
the upgrade in a low-traffic window. The 103 backfill can be done in batches
beforehand: `UPDATE worker_tasks wt SET tenant_id = ti.tenant_id FROM
task_instances ti WHERE ti.id = wt.instance_id AND wt.tenant_id IS NULL AND
wt.state IN ('pending','claimed') AND wt.id IN (SELECT id FROM worker_tasks
WHERE tenant_id IS NULL AND state IN ('pending','claimed') LIMIT 10000);`
repeated until it updates 0 rows (requires the `ADD COLUMN` from 103).

### Note: dropped migration 082

`082_widen_instance_max_concurrency.sql` (`task_instances.max_concurrency`
INTEGER -> BIGINT) was merged to `main` but never tagged, and was removed
before release because it violated both points above. The column stays
`INTEGER`; the storage layer saturates limits above `i32::MAX` (effectively
unlimited) and decodes either width. A database that already applied 082
from an untagged build keeps its `BIGINT` column (harmless) but must forget
the migration record before the next `orch8 migrate`:

```sql
DELETE FROM _sqlx_migrations WHERE version = 82;
```

## Upgrading large installs to 118–121

Migrations 118–121 (mobile-server hardening) add only indexes plus one
metadata-only column (`rollback_policies.min_samples`, constant default) and
drop two single-column telemetry tenant indexes made redundant by the new
composites. They touch `telemetry_mobile_events`, `telemetry_mobile_errors`,
`runtime_capabilities`, `push_wake_outbox`, `mobile_devices`,
`mobile_approval_requests` and `mobile_commands`; on large installs run the
`CREATE INDEX CONCURRENTLY IF NOT EXISTS` statements from each file's header
online first, then `orch8 migrate` is a no-op for them.

## Upgrading large installs to 100–106 and 111

Migrations 100–106 and 111 add indexes to hot tables (`task_instances`,
`worker_tasks`, `signal_inbox`, `usage_events`, `webhook_outbox`, the
continuity tables). Each file builds them with a plain
`CREATE INDEX IF NOT EXISTS`, which blocks writes to the table while it runs.
On a large database, first run the `CREATE INDEX CONCURRENTLY IF NOT EXISTS`
statements listed in each file's header (outside a transaction, one at a
time), then run `orch8 migrate`; the migration's own statements are then
no-ops. 103 also back-fills `worker_tasks.tenant_id` for pending/claimed rows
only (the live backlog). 100 and 103 add nullable columns (metadata-only, no
rewrite); 105 only changes storage parameters (no rewrite; `fillfactor`
applies to newly written pages — use `pg_repack` to apply it to existing
ones).


## Upgrading large installs to 122

Migration 122 adds two partial composite indexes on `task_instances` for exact
child activation and legacy block lookup. It changes no stored columns or
workflow identities. On large installs, run the equivalent `CREATE INDEX
CONCURRENTLY IF NOT EXISTS` statements in the migration header outside a
transaction, one at a time, before upgrading. The normal migration then reuses
those indexes. Keep `CONCURRENTLY` out of the sqlx migration itself, as explained
above. SQLite creates the equivalent indexes through its idempotent schema setup.


## Upgrading large installs to 123

Migration 123 adds a partial composite index for historical child adoption,
qualified by parent, block and enclosing activation suffix. It excludes
cancelled and explicitly superseded children. Run the equivalent `CREATE INDEX
CONCURRENTLY IF NOT EXISTS` statement from the migration header online, outside
a transaction, before upgrading large installations. The normal migration then
reuses that index. It changes no stored workflow columns or activation values.
SQLite adds the equivalent index through idempotent schema setup.
