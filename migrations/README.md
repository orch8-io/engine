# Migrations

Applied via `sqlx::migrate!("../migrations")` (see `orch8-storage/src/postgres/mod.rs`).
`sqlx` records each migration's checksum in `_sqlx_migrations` the first time it runs.

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
