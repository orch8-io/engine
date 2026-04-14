use anyhow::Result;
use clap::Subcommand;
use reqwest::Client;
use uuid::Uuid;

use crate::print_response;

#[derive(Subcommand)]
pub enum CheckpointCmd {
    /// List checkpoints for an instance.
    List { instance_id: Uuid },
    /// Get the latest checkpoint for an instance.
    Latest { instance_id: Uuid },
    /// Prune old checkpoints, keeping N most recent.
    Prune {
        instance_id: Uuid,
        #[arg(long, default_value = "3")]
        keep: u32,
    },
}

pub async fn run(client: &Client, base: &str, cmd: CheckpointCmd) -> Result<()> {
    match cmd {
        CheckpointCmd::List { instance_id } => {
            let resp = client
                .get(format!("{base}/instances/{instance_id}/checkpoints"))
                .send()
                .await?;
            print_response(resp).await?;
        }
        CheckpointCmd::Latest { instance_id } => {
            let resp = client
                .get(format!("{base}/instances/{instance_id}/checkpoints/latest"))
                .send()
                .await?;
            print_response(resp).await?;
        }
        CheckpointCmd::Prune { instance_id, keep } => {
            let resp = client
                .post(format!("{base}/instances/{instance_id}/checkpoints/prune"))
                .json(&serde_json::json!({ "keep": keep }))
                .send()
                .await?;
            print_response(resp).await?;
        }
    }
    Ok(())
}
