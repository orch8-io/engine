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

`orch8 migrate` runs before the new fleet starts, so for a while the *old*
binaries run against the *new* schema. A migration must therefore:

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
