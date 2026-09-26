//! `orch8 upgrade --check` — compare the schema applied to a database with
//! the migrations bundled in this binary, before rolling out a new version.
//!
//! `PostgreSQL`: reads `_sqlx_migrations` and diffs it against the embedded
//! `migrations/` set: pending migrations are listed, each scanned for
//! destructive or locking statements (`DROP`, `ALTER … TYPE`, `TRUNCATE`,
//! `RENAME`, `SET NOT NULL`, unbounded `DELETE`/`UPDATE`), and checksum
//! mismatches / versions unknown to this binary are reported. `SQLite`: the
//! schema is reconciled additively on boot, so the check compares the
//! recorded `schema_versions` with the bundled version. The database is only
//! read. Exits non-zero when anything is pending.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use clap::Args;
use owo_colors::OwoColorize;
use serde::Serialize;
use sqlx::Row;

use crate::OutputFormat;

#[derive(Debug, Args)]
pub struct UpgradeCmd {
    /// Only report what an upgrade would apply (required; apply with
    /// `orch8 migrate` or the server's `run_migrations`).
    #[arg(long)]
    pub check: bool,
    /// Target database: `postgres://…`, `sqlite:path/to/orch8.db`, or a path.
    #[arg(long, env = "ORCH8_DATABASE_URL", hide_env_values = true)]
    pub database_url: String,
}

/// Where a database lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbTarget {
    Postgres(String),
    Sqlite(std::path::PathBuf),
}

/// Parse a database URL or bare `SQLite` path.
pub fn parse_target(url: &str) -> Result<DbTarget> {
    let trimmed = url.trim();
    if trimmed.starts_with("postgres://") || trimmed.starts_with("postgresql://") {
        return Ok(DbTarget::Postgres(trimmed.to_string()));
    }
    if trimmed.contains(":memory:") || trimmed.is_empty() {
        bail!("an in-memory SQLite database cannot be backed up, restored, or checked");
    }
    let path = trimmed
        .strip_prefix("sqlite://")
        .or_else(|| trimmed.strip_prefix("sqlite:"))
        .unwrap_or(trimmed);
    let path = path.split('?').next().unwrap_or(path);
    Ok(DbTarget::Sqlite(std::path::PathBuf::from(path)))
}

/// Risk classification of a pending migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Risk {
    /// Additive (new tables / columns / indexes).
    Safe,
    /// Rewrites or locks data (backfill `UPDATE`, `SET NOT NULL`, rename).
    Caution,
    /// Removes or reshapes data (`DROP`, `ALTER … TYPE`, `TRUNCATE`, `DELETE`).
    Destructive,
}

/// Scan migration SQL for risky statements. Returns the risk level and the
/// matched statement kinds.
pub fn classify_sql(sql: &str) -> (Risk, Vec<&'static str>) {
    // Strip line comments, normalize whitespace, uppercase.
    let normalized: String = sql
        .lines()
        .map(|line| line.split_once("--").map_or(line, |(code, _)| code))
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_uppercase();
    let mut hits = Vec::new();
    let mut risk = Risk::Safe;
    let mut flag = |hit: bool, name: &'static str, level: Risk| {
        if hit {
            hits.push(name);
            risk = risk.max(level);
        }
    };
    flag(
        normalized.contains("DROP TABLE"),
        "DROP TABLE",
        Risk::Destructive,
    );
    flag(
        normalized.contains("DROP COLUMN"),
        "DROP COLUMN",
        Risk::Destructive,
    );
    flag(
        normalized.contains("DROP SCHEMA"),
        "DROP SCHEMA",
        Risk::Destructive,
    );
    flag(
        normalized.contains("DROP TYPE"),
        "DROP TYPE",
        Risk::Destructive,
    );
    flag(
        normalized.contains("TRUNCATE"),
        "TRUNCATE",
        Risk::Destructive,
    );
    flag(
        has_alter_type(&normalized),
        "ALTER COLUMN … TYPE",
        Risk::Destructive,
    );
    flag(
        normalized.contains("DELETE FROM"),
        "DELETE FROM",
        Risk::Destructive,
    );
    flag(
        normalized.contains("DROP INDEX"),
        "DROP INDEX",
        Risk::Caution,
    );
    flag(
        normalized.contains("DROP CONSTRAINT"),
        "DROP CONSTRAINT",
        Risk::Caution,
    );
    flag(normalized.contains(" RENAME "), "RENAME", Risk::Caution);
    flag(
        normalized.contains("SET NOT NULL"),
        "SET NOT NULL",
        Risk::Caution,
    );
    flag(
        normalized.contains("UPDATE ") && normalized.contains(" SET "),
        "UPDATE (backfill)",
        Risk::Caution,
    );
    (risk, hits)
}

fn has_alter_type(normalized: &str) -> bool {
    normalized.match_indices("ALTER COLUMN").any(|(idx, _)| {
        let rest = &normalized[idx..];
        let stmt_end = rest.find(';').unwrap_or(rest.len());
        let stmt = &rest[..stmt_end];
        stmt.contains(" TYPE ") || stmt.contains(" SET DATA TYPE ")
    }) || normalized.contains("ALTER TYPE")
}

/// One bundled migration that the target has not applied.
#[derive(Debug, Clone, Serialize)]
pub struct PendingMigration {
    pub version: i64,
    pub description: String,
    pub risk: Risk,
    pub statements: Vec<&'static str>,
}

/// The upgrade-check result.
#[derive(Debug, Clone, Serialize)]
pub struct UpgradeReport {
    pub backend: &'static str,
    /// Highest schema version applied in the database (None = empty DB).
    pub applied_version: Option<i64>,
    /// Highest schema version bundled with this binary.
    pub bundled_version: i64,
    pub pending: Vec<PendingMigration>,
    /// Applied versions this binary does not know (the DB is newer).
    pub unknown_applied: Vec<i64>,
    /// Applied migrations whose checksum differs from the bundled file.
    pub checksum_mismatches: Vec<i64>,
    /// Applied-but-failed (dirty) migrations.
    pub failed: Vec<i64>,
    pub notes: Vec<String>,
}

impl UpgradeReport {
    pub fn is_current(&self) -> bool {
        self.pending.is_empty() && self.failed.is_empty()
    }
}

/// Diff applied `(version, checksum, success)` rows against the bundled
/// migrator. Pure; the SQL scan is in [`classify_sql`].
pub fn diff_postgres(
    applied: &BTreeMap<i64, (Vec<u8>, bool)>,
    bundled: &sqlx::migrate::Migrator,
) -> UpgradeReport {
    let mut report = UpgradeReport {
        backend: "postgres",
        applied_version: applied.keys().max().copied(),
        bundled_version: 0,
        pending: Vec::new(),
        unknown_applied: Vec::new(),
        checksum_mismatches: Vec::new(),
        failed: Vec::new(),
        notes: Vec::new(),
    };
    let mut known = std::collections::BTreeSet::new();
    for migration in bundled.iter() {
        if migration.migration_type.is_down_migration() {
            continue;
        }
        known.insert(migration.version);
        report.bundled_version = report.bundled_version.max(migration.version);
        if let Some((checksum, success)) = applied.get(&migration.version) {
            if !success {
                report.failed.push(migration.version);
            }
            if checksum.as_slice() != migration.checksum.as_ref() {
                report.checksum_mismatches.push(migration.version);
            }
        } else {
            let (risk, statements) = classify_sql(&migration.sql);
            report.pending.push(PendingMigration {
                version: migration.version,
                description: migration.description.to_string(),
                risk,
                statements,
            });
        }
    }
    report.unknown_applied = applied
        .keys()
        .filter(|v| !known.contains(v))
        .copied()
        .collect();
    if !report.unknown_applied.is_empty() {
        report.notes.push(
            "the database has migrations this binary does not know — it was migrated by a \
             newer release; do not downgrade onto it"
                .into(),
        );
    }
    report
}

fn bundled_migrator() -> sqlx::migrate::Migrator {
    sqlx::migrate!("../migrations")
}

/// Run the check against a target (read-only).
pub async fn check(target: &DbTarget) -> Result<UpgradeReport> {
    match target {
        DbTarget::Postgres(url) => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await
                .context("connect to PostgreSQL")?;
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
                 WHERE table_name = '_sqlx_migrations')",
            )
            .fetch_one(&pool)
            .await
            .context("inspect migration table")?;
            let mut applied = BTreeMap::new();
            if exists {
                for row in sqlx::query("SELECT version, checksum, success FROM _sqlx_migrations")
                    .fetch_all(&pool)
                    .await
                    .context("read _sqlx_migrations")?
                {
                    applied.insert(
                        row.try_get::<i64, _>("version")?,
                        (
                            row.try_get::<Vec<u8>, _>("checksum")?,
                            row.try_get::<bool, _>("success")?,
                        ),
                    );
                }
            }
            pool.close().await;
            let mut report = diff_postgres(&applied, &bundled_migrator());
            if !exists {
                report.notes.push(
                    "no _sqlx_migrations table: this is an empty (unmigrated) database".into(),
                );
            }
            Ok(report)
        }
        DbTarget::Sqlite(path) => check_sqlite(path).await,
    }
}

async fn check_sqlite(path: &std::path::Path) -> Result<UpgradeReport> {
    if !path.is_file() {
        bail!("SQLite database {} does not exist", path.display());
    }
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(path)
        .read_only(true);
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .with_context(|| format!("open {} read-only", path.display()))?;
    let has_table: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_versions')",
    )
    .fetch_one(&pool)
    .await?;
    let applied: Option<i64> = if has_table {
        sqlx::query_scalar("SELECT MAX(version) FROM schema_versions")
            .fetch_one(&pool)
            .await?
    } else {
        None
    };
    pool.close().await;
    let bundled = orch8_storage::sqlite::BUNDLED_SCHEMA_VERSION;
    let mut report = UpgradeReport {
        backend: "sqlite",
        applied_version: applied,
        bundled_version: bundled,
        pending: Vec::new(),
        unknown_applied: Vec::new(),
        checksum_mismatches: Vec::new(),
        failed: Vec::new(),
        notes: vec![
            "SQLite schemas are reconciled additively on boot (new tables, columns, and \
             indexes only; nothing is dropped)"
                .into(),
        ],
    };
    match applied {
        Some(v) if v > bundled => {
            report.unknown_applied.push(v);
            report
                .notes
                .push("the database was opened by a newer release; this binary is older".into());
        }
        Some(v) if v == bundled => {}
        other => report.pending.push(PendingMigration {
            version: bundled,
            description: format!(
                "reconcile schema from v{} to v{bundled}",
                other.map_or_else(|| "none".to_string(), |v| v.to_string())
            ),
            risk: Risk::Safe,
            statements: Vec::new(),
        }),
    }
    Ok(report)
}

pub fn render(report: &UpgradeReport) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{} schema: applied {} · bundled {}",
        report.backend,
        report
            .applied_version
            .map_or_else(|| "none".to_string(), |v| v.to_string()),
        report.bundled_version
    );
    if report.pending.is_empty() {
        let _ = writeln!(out, "{}", "up to date — no pending migrations".green());
    } else {
        let _ = writeln!(out, "{} pending migration(s):", report.pending.len());
        for m in &report.pending {
            let risk = match m.risk {
                Risk::Safe => "safe".green().to_string(),
                Risk::Caution => "caution".yellow().bold().to_string(),
                Risk::Destructive => "DESTRUCTIVE".red().bold().to_string(),
            };
            let detail = if m.statements.is_empty() {
                String::new()
            } else {
                format!(" ({})", m.statements.join(", "))
            };
            let _ = writeln!(
                out,
                "  {:>4}  {:<11} {}{detail}",
                m.version, risk, m.description
            );
        }
        let destructive = report
            .pending
            .iter()
            .filter(|m| m.risk == Risk::Destructive)
            .count();
        if destructive > 0 {
            let _ = writeln!(
                out,
                "{} {destructive} destructive migration(s): take a backup first (`orch8 backup`)",
                "warning:".red().bold()
            );
        }
    }
    for v in &report.failed {
        let _ = writeln!(
            out,
            "{} migration {v} is recorded as failed (dirty)",
            "error:".red().bold()
        );
    }
    for v in &report.checksum_mismatches {
        let _ = writeln!(
            out,
            "{} migration {v} was edited after it was applied (checksum mismatch)",
            "warning:".yellow().bold()
        );
    }
    if !report.unknown_applied.is_empty() {
        let _ = writeln!(
            out,
            "{} applied version(s) unknown to this binary: {:?}",
            "warning:".yellow().bold(),
            report.unknown_applied
        );
    }
    for note in &report.notes {
        let _ = writeln!(out, "note: {note}");
    }
    out
}

pub async fn run(cmd: UpgradeCmd, format: OutputFormat) -> Result<()> {
    if !cmd.check {
        bail!(
            "only `orch8 upgrade --check` is supported; apply migrations with `orch8 migrate` \
             (PostgreSQL) — SQLite reconciles on server boot"
        );
    }
    let report = check(&parse_target(&cmd.database_url)?).await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
        OutputFormat::Table => print!("{}", render(&report)),
    }
    if !report.is_current() {
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_risky_statements() {
        assert_eq!(classify_sql("CREATE TABLE x (id INT);").0, Risk::Safe);
        assert_eq!(
            classify_sql("CREATE INDEX IF NOT EXISTS i ON t (a);").0,
            Risk::Safe
        );
        let (risk, hits) = classify_sql("ALTER TABLE t DROP COLUMN legacy;");
        assert_eq!(risk, Risk::Destructive);
        assert_eq!(hits, ["DROP COLUMN"]);
        let (risk, hits) = classify_sql("ALTER TABLE t\n  ALTER COLUMN n TYPE BIGINT;");
        assert_eq!(risk, Risk::Destructive);
        assert!(hits.contains(&"ALTER COLUMN … TYPE"));
        assert_eq!(classify_sql("drop table old_things;").0, Risk::Destructive);
        assert_eq!(classify_sql("TRUNCATE t;").0, Risk::Destructive);
        assert_eq!(
            classify_sql("UPDATE t SET a = 1 WHERE a IS NULL;").0,
            Risk::Caution
        );
        assert_eq!(
            classify_sql("ALTER TABLE t ALTER COLUMN a SET NOT NULL;").0,
            Risk::Caution
        );
        // Comments never trigger.
        assert_eq!(classify_sql("-- DROP TABLE x\nSELECT 1;").0, Risk::Safe);
        // ALTER COLUMN without a type change is not destructive.
        assert_eq!(
            classify_sql("ALTER TABLE t ALTER COLUMN a SET DEFAULT 0;").0,
            Risk::Safe
        );
    }

    #[test]
    fn diff_lists_pending_mismatched_failed_and_unknown() {
        let migrator = bundled_migrator();
        let ups: Vec<_> = migrator
            .iter()
            .filter(|m| !m.migration_type.is_down_migration())
            .collect();
        assert!(ups.len() > 10);
        let mut applied = BTreeMap::new();
        // Everything but the last two applied; one tampered, one failed.
        for m in &ups[..ups.len() - 2] {
            applied.insert(m.version, (m.checksum.to_vec(), true));
        }
        applied.insert(ups[0].version, (vec![0u8; 4], true));
        applied.insert(ups[1].version, (ups[1].checksum.to_vec(), false));
        applied.insert(9_999, (vec![], true));
        let report = diff_postgres(&applied, &migrator);
        assert_eq!(report.pending.len(), 2);
        assert_eq!(report.pending[0].version, ups[ups.len() - 2].version);
        assert_eq!(report.checksum_mismatches, [ups[0].version]);
        assert_eq!(report.failed, [ups[1].version]);
        assert_eq!(report.unknown_applied, [9_999]);
        assert!(!report.is_current());
        assert_eq!(report.bundled_version, ups.last().unwrap().version);

        let all: BTreeMap<_, _> = ups
            .iter()
            .map(|m| (m.version, (m.checksum.to_vec(), true)))
            .collect();
        assert!(diff_postgres(&all, &migrator).is_current());
        // An empty database has every migration pending.
        assert_eq!(
            diff_postgres(&BTreeMap::new(), &migrator).pending.len(),
            ups.len()
        );
    }

    #[test]
    fn parses_targets() {
        assert_eq!(
            parse_target("postgres://u:p@h/db").unwrap(),
            DbTarget::Postgres("postgres://u:p@h/db".into())
        );
        assert_eq!(
            parse_target("sqlite:orch8.db?mode=rwc").unwrap(),
            DbTarget::Sqlite("orch8.db".into())
        );
        assert_eq!(
            parse_target("sqlite:///var/lib/orch8.db").unwrap(),
            DbTarget::Sqlite("/var/lib/orch8.db".into())
        );
        assert_eq!(
            parse_target("./data/dev.db").unwrap(),
            DbTarget::Sqlite("./data/dev.db".into())
        );
        assert!(parse_target("sqlite::memory:").is_err());
    }

    #[tokio::test]
    async fn sqlite_check_reports_current_and_behind() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("orch8.db");
        {
            let _storage = orch8_storage::sqlite::SqliteStorage::file(path.to_str().unwrap())
                .await
                .unwrap();
        }
        let report = check(&DbTarget::Sqlite(path.clone())).await.unwrap();
        assert!(report.is_current(), "{report:?}");
        assert_eq!(
            report.applied_version,
            Some(orch8_storage::sqlite::BUNDLED_SCHEMA_VERSION)
        );

        // Simulate an older database.
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect(&format!("sqlite:{}", path.display()))
            .await
            .unwrap();
        sqlx::query("DELETE FROM schema_versions")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO schema_versions (version) VALUES (1)")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        let report = check(&DbTarget::Sqlite(path)).await.unwrap();
        assert_eq!(report.pending.len(), 1);
        assert_eq!(report.pending[0].risk, Risk::Safe);
        assert!(render(&report).contains("pending migration"));
    }
}
