//! Standalone reconciliation for a file-backed database.
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context as _, Result};
use clap::{Args, Subcommand};
use orch8_storage::sqlite::{copy_snapshot, maintain_file, reconcile_file_schema};

use crate::OutputFormat;

#[derive(Debug, Args)]
pub struct SqliteSchemaCmd {
    #[command(subcommand)]
    action: SchemaAction,
}

#[derive(Debug, Subcommand)]
enum SchemaAction {
    /// Reconcile a disposable snapshot and report the pending schema changes.
    Plan(SchemaFile),
    /// Apply pending changes while holding exclusive database ownership.
    Apply(SchemaFile),
    /// Checkpoint the WAL and reclaim freelist pages under explicit budgets.
    Maintain(MaintainFile),
}

#[derive(Debug, Args)]
struct SchemaFile {
    /// Path to an existing database file.
    #[arg(long)]
    database: PathBuf,
}

#[derive(Debug, Args)]
struct MaintainFile {
    #[command(flatten)]
    file: SchemaFile,
    /// Maximum elapsed time in milliseconds.
    #[arg(long)]
    max_ms: u64,
    /// Maximum freelist pages to reclaim, or zero for checkpoint only.
    #[arg(long)]
    max_pages: u32,
}

pub async fn run(command: SqliteSchemaCmd, format: OutputFormat) -> Result<()> {
    let (file, apply) = match command.action {
        SchemaAction::Plan(file) => (file, false),
        SchemaAction::Apply(file) => (file, true),
        SchemaAction::Maintain(file) => return maintain(file, format).await,
    };
    let report = if apply {
        reconcile_file_schema(&file.database).await
    } else {
        let temp = tempfile::tempdir().context("create SQLite schema plan directory")?;
        let snapshot = temp.path().join("schema-plan.db");
        copy_snapshot(&file.database, &snapshot)
            .await
            .context("snapshot SQLite database for schema plan")?;
        reconcile_file_schema(&snapshot).await
    }
    .context("reconcile SQLite schema")?;
    if matches!(format, OutputFormat::Json) {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        for change in &report.changes {
            println!("{} {} {}", change.action, change.kind, change.name);
        }
        if report.changes.is_empty() && report.previous_version == report.reconciled_version {
            println!("SQLite schema is current");
        } else if let Some(version) = report.reconciled_version {
            let verb = if apply { "Applied" } else { "Plans" };
            println!("{verb} schema version {version}");
        }
    }
    Ok(())
}

async fn maintain(options: MaintainFile, format: OutputFormat) -> Result<()> {
    let report = maintain_file(
        &options.file.database,
        Duration::from_millis(options.max_ms),
        options.max_pages,
    )
    .await
    .context("maintain SQLite database")?;
    if matches!(format, OutputFormat::Json) {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("Reclaimed {} freelist pages", report.reclaimed_pages);
        if let (Some(checkpointed), Some(wal)) = (report.checkpointed_frames, report.wal_frames) {
            println!("Checkpointed {checkpointed} of {wal} WAL frames");
        }
        if report.busy {
            println!("SQLite is busy or some WAL frames remain");
        }
        if report.deadline_reached {
            println!("Maintenance time budget reached");
        }
    }
    Ok(())
}
