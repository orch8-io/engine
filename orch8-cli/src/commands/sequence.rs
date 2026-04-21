use std::path::PathBuf;

use anyhow::Result;
use clap::Subcommand;
use reqwest::Client;
use uuid::Uuid;

use crate::{print_response, OutputFormat};

#[derive(Subcommand)]
pub enum SequenceCmd {
    /// Create a sequence from a JSON file.
    Create {
        /// Path to the JSON definition file.
        #[arg(long, short)]
        file: PathBuf,
    },
    /// Get a sequence by ID.
    Get { id: Uuid },
    /// Look up a sequence by name.
    Lookup {
        tenant_id: String,
        namespace: String,
        name: String,
        #[arg(long)]
        version: Option<i32>,
    },
    /// List all versions of a sequence.
    Versions {
        tenant_id: String,
        namespace: String,
        name: String,
    },
    /// Deprecate a sequence version.
    Deprecate { id: Uuid },
}

pub async fn run(
    client: &Client,
    base: &str,
    cmd: SequenceCmd,
    format: OutputFormat,
) -> Result<()> {
    match cmd {
        SequenceCmd::Create { file } => {
            let content = std::fs::read_to_string(&file)
                .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", file.display()))?;
            let body: serde_json::Value = serde_json::from_str(&content)
                .map_err(|e| anyhow::anyhow!("invalid JSON in {}: {e}", file.display()))?;
            let resp = client
                .post(format!("{base}/sequences"))
                .json(&body)
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        SequenceCmd::Get { id } => {
            let resp = client.get(format!("{base}/sequences/{id}")).send().await?;
            print_response(resp, format).await?;
        }
        SequenceCmd::Lookup {
            tenant_id,
            namespace,
            name,
            version,
        } => {
            let mut params = vec![
                ("tenant_id", tenant_id),
                ("namespace", namespace),
                ("name", name),
            ];
            if let Some(v) = &version {
                params.push(("version", v.to_string()));
            }
            let resp = client
                .get(format!("{base}/sequences/by-name"))
                .query(&params)
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        SequenceCmd::Versions {
            tenant_id,
            namespace,
            name,
        } => {
            let resp = client
                .get(format!("{base}/sequences/versions"))
                .query(&[
                    ("tenant_id", &tenant_id),
                    ("namespace", &namespace),
                    ("name", &name),
                ])
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        SequenceCmd::Deprecate { id } => {
            let resp = client
                .post(format!("{base}/sequences/{id}/deprecate"))
                .send()
                .await?;
            print_response(resp, format).await?;
        }
    }
    Ok(())
}
