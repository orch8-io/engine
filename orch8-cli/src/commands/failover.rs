//! `orch8 failover` — inspect and move the active-region fence directly in
//! the database (the API may be down during a failover). See
//! `docs/FAILOVER.md` for the full procedure.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};

use orch8_storage::StorageBackend;

#[derive(Debug, Args)]
pub struct FailoverCmd {
    /// Database URL: `postgres://…` or `sqlite:<path>`. Point it at the
    /// database whose fence you want to read or move.
    #[arg(long, env = "ORCH8_DATABASE_URL", global = true)]
    pub database_url: Option<String>,
    #[command(subcommand)]
    pub action: FailoverAction,
}

#[derive(Debug, Subcommand)]
pub enum FailoverAction {
    /// Print the active region and fence epoch.
    Status,
    /// Make `--region` the active region (epoch + 1). Run it against the
    /// promoted primary; run it against the old primary too if that database
    /// is still reachable, so old-region engines stop within one poll.
    Promote {
        #[arg(long)]
        region: String,
        /// Refuse unless the fence is currently at this epoch (recommended:
        /// the value printed by `status`). Omit only for the first install.
        #[arg(long)]
        expect_epoch: Option<u64>,
        /// Free-text reason recorded on the fence.
        #[arg(long, default_value = "")]
        reason: String,
    },
}

async fn open(url: &str) -> Result<Arc<dyn StorageBackend>> {
    if let Some(path) = url.strip_prefix("sqlite:") {
        let path = path.trim_start_matches("//");
        if !std::path::Path::new(path).is_file() {
            bail!("SQLite database {path} does not exist");
        }
        return Ok(Arc::new(
            orch8_storage::sqlite::SqliteStorage::file(path)
                .await
                .context("open SQLite database")?,
        ));
    }
    Ok(Arc::new(
        orch8_storage::postgres::PostgresStorage::new(url, 2, None)
            .await
            .context("connect to PostgreSQL")?,
    ))
}

pub async fn run(cmd: FailoverCmd) -> Result<()> {
    let url = cmd
        .database_url
        .context("--database-url (or ORCH8_DATABASE_URL) is required")?;
    let storage = open(&url).await?;
    match cmd.action {
        FailoverAction::Status => match storage.get_region_fence().await? {
            Some(fence) => println!("{}", serde_json::to_string_pretty(&fence)?),
            None => {
                println!("no region fence installed (run `orch8 failover promote --region <r>`)");
            }
        },
        FailoverAction::Promote {
            region,
            expect_epoch,
            reason,
        } => {
            let operator = std::env::var("USER").unwrap_or_else(|_| "orch8-cli".into());
            let fence = orch8_engine::failover::promote(
                storage.as_ref(),
                &region,
                expect_epoch,
                &operator,
                &reason,
            )
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
            println!("{}", serde_json::to_string_pretty(&fence)?);
        }
    }
    Ok(())
}
