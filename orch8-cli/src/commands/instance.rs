use anyhow::Result;
use clap::Subcommand;
use reqwest::Client;
use serde_json::Value;
use tabled::{Table, Tabled};
use uuid::Uuid;

use crate::{print_response, val_str};

#[derive(Subcommand)]
pub enum InstanceCmd {
    /// Get a single instance by ID.
    Get { id: Uuid },
    /// List instances with optional filters.
    List {
        #[arg(long)]
        tenant_id: Option<String>,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(long)]
        state: Option<String>,
        #[arg(long)]
        sequence_id: Option<Uuid>,
        #[arg(long, default_value = "50")]
        limit: u32,
    },
    /// Show execution tree for an instance.
    Tree { id: Uuid },
    /// Show block outputs for an instance.
    Outputs { id: Uuid },
    /// Update instance state.
    SetState {
        id: Uuid,
        /// New state: scheduled, running, paused, cancelled, etc.
        state: String,
    },
    /// Retry a failed instance.
    Retry { id: Uuid },
    /// List failed instances (DLQ).
    Dlq {
        #[arg(long)]
        tenant_id: Option<String>,
        #[arg(long, default_value = "50")]
        limit: u32,
    },
    /// Bulk update state for matching instances.
    BulkState {
        /// New state.
        state: String,
        #[arg(long)]
        tenant_id: Option<String>,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(long)]
        states: Option<String>,
    },
}

#[derive(Tabled)]
struct InstanceRow {
    id: String,
    state: String,
    tenant: String,
    namespace: String,
    priority: String,
    next_fire: String,
    updated: String,
}

#[allow(clippy::too_many_lines)]
pub async fn run(client: &Client, base: &str, cmd: InstanceCmd) -> Result<()> {
    match cmd {
        InstanceCmd::Get { id } => {
            let resp = client.get(format!("{base}/instances/{id}")).send().await?;
            print_response(resp).await?;
        }
        InstanceCmd::List {
            tenant_id,
            namespace,
            state,
            sequence_id,
            limit,
        } => {
            let mut params = vec![("limit", limit.to_string())];
            if let Some(t) = &tenant_id {
                params.push(("tenant_id", t.clone()));
            }
            if let Some(n) = &namespace {
                params.push(("namespace", n.clone()));
            }
            if let Some(s) = &state {
                params.push(("state", s.clone()));
            }
            if let Some(s) = &sequence_id {
                params.push(("sequence_id", s.to_string()));
            }

            let resp = client
                .get(format!("{base}/instances"))
                .query(&params)
                .send()
                .await?;
            let body: Value = resp.json().await?;

            if let Some(arr) = body.as_array() {
                if arr.is_empty() {
                    println!("No instances found.");
                } else {
                    let rows: Vec<InstanceRow> = arr
                        .iter()
                        .map(|v| InstanceRow {
                            id: val_str(v, "id"),
                            state: val_str(v, "state"),
                            tenant: val_str(v, "tenant_id"),
                            namespace: val_str(v, "namespace"),
                            priority: val_str(v, "priority"),
                            next_fire: val_str(v, "next_fire_at"),
                            updated: val_str(v, "updated_at"),
                        })
                        .collect();
                    println!("{}", Table::new(rows));
                }
            } else {
                println!("{}", serde_json::to_string_pretty(&body)?);
            }
        }
        InstanceCmd::Tree { id } => {
            let resp = client
                .get(format!("{base}/instances/{id}/tree"))
                .send()
                .await?;
            print_response(resp).await?;
        }
        InstanceCmd::Outputs { id } => {
            let resp = client
                .get(format!("{base}/instances/{id}/outputs"))
                .send()
                .await?;
            print_response(resp).await?;
        }
        InstanceCmd::SetState { id, state } => {
            let resp = client
                .patch(format!("{base}/instances/{id}/state"))
                .json(&serde_json::json!({ "state": state }))
                .send()
                .await?;
            print_response(resp).await?;
        }
        InstanceCmd::Retry { id } => {
            let resp = client
                .post(format!("{base}/instances/{id}/retry"))
                .send()
                .await?;
            print_response(resp).await?;
        }
        InstanceCmd::Dlq { tenant_id, limit } => {
            let mut params = vec![("limit", limit.to_string())];
            if let Some(t) = &tenant_id {
                params.push(("tenant_id", t.clone()));
            }
            let resp = client
                .get(format!("{base}/instances/dlq"))
                .query(&params)
                .send()
                .await?;
            print_response(resp).await?;
        }
        InstanceCmd::BulkState {
            state,
            tenant_id,
            namespace,
            states,
        } => {
            let resp = client
                .patch(format!("{base}/instances/bulk/state"))
                .json(&serde_json::json!({
                    "filter": {
                        "tenant_id": tenant_id,
                        "namespace": namespace,
                        "states": states.map(|s| s.split(',').map(|v| v.trim().to_string()).collect::<Vec<_>>()),
                    },
                    "state": state,
                }))
                .send()
                .await?;
            print_response(resp).await?;
        }
    }
    Ok(())
}
