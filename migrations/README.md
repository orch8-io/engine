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

### Online prebuild for output indexes 125/126

`block_outputs` is partitioned by migration 074. PostgreSQL supports concurrent
index builds on its leaf tables; creating the parent afterwards attaches valid
matching leaf indexes as a metadata operation. See the
[PostgreSQL CREATE INDEX documentation](https://www.postgresql.org/docs/14/sql-createindex.html).

For large existing installations that already have migration 074, run one
prebuild process before the upgrade, with no migrator running concurrently:

```sh
psql "$ORCH8_DATABASE_URL" -X --set=ON_ERROR_STOP=1 \
  --file=scripts/sql/prebuild-current-output-indexes.psql
orch8 migrate --database-url "$ORCH8_DATABASE_URL"
```

The [prebuild script](../scripts/sql/prebuild-current-output-indexes.psql) uses
autocommit and discovers every leaf. It skips parents that already have a valid
index, and reserves `orch8_boc_*` / `orch8_bod_*` names for its matching leaf
definitions. Do not use `psql --single-transaction`. Run the migration only after
the prebuild exits successfully. A failed concurrent build can leave an invalid
index; the script reports invalid parent/prebuild indexes instead of silently
reusing them. Repair the named invalid index and retry before migrating.

`DATABASE_URL=... python3 scripts/check-output-index-prebuild.py` exercises the
recipe in a fresh schema and removes that schema afterwards. CI runs this check
with its test database and the `psql` client. It verifies reuse of all 32 leaf
indexes, parent validity, repeatability and recovery from a real failed
concurrent build.

Normal migration without a prebuild remains supported and creates the indexes
under the usual write locks. The change to migration 125 above corrects only its
unreleased prebuild comments; its SQL definition is unchanged.

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

## Delegation publication receipts (142)

Migration 142 adds an initially empty metadata table for atomic delegation
publication. Receipts survive hot/native cleanup and cannot be updated or
deleted; rollback refuses a nonempty receipt table. SQLite schema 72 creates
the equivalent table during upgrade. No historical publication or caller is
backfilled. Deploy a compatible coordinator cohort before activating new
publication traffic; mixed older writers remain a separate enforcement gate.

## Delegation relay execution fence (143)

Migration 143 (SQLite schema 73) excludes delegation relay records from native
execution. Relay identity comes from the reserved legacy metadata field or an
immutable publication whose task owner differs from the delegated parent.
Hosted publications remain runnable, including coincident instance/delegation
UUIDs. New partial indexes support point lookups without scanning hosted history.

The upgrade clears claims and schedules on historical scheduled/running relays.
A relay with native runtime or execution-tree evidence is paused; its evidence
is retained for operator reconciliation. Other scheduled relays become waiting.
Resume cannot release a relay with native history, and pending mailbox tasks for
that relay cannot acquire a new worker claim. Already issued destination claims
retain their existing completion semantics. Reconcile possible duplicate side
effects before restoring such a quarantined relay.

Database triggers reject native running state and changes to a relay's identity
or native runtime history. Normal destination outcome integration remains
permitted. PostgreSQL rollback refuses while any relay identity remains,
including publication receipts retained after native instance cleanup. Older
coordinators may encounter explicit write errors; deploy compatible coordinators
before activating this schema. This fence does not establish general mixed-version
writer compatibility for other continuity operations.

Quarantine is stored as compact immutable `delegation_proxy_quarantines` rows,
independently of hot instance and execution-tree retention. These records must
survive full database backup/restore. They have no automatic expiry or resume
bypass: reconciliation must not reuse a poisoned physical identity. Capsule and
local-migration snapshots refuse relay records. The logical configuration/native
instance archive cannot preserve relay authority and refuses to include relays;
configuration-only exports remain available.
