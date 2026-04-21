use anyhow::Result;
use clap::Subcommand;
use reqwest::Client;
use serde_json::Value;
use tabled::{Table, Tabled};
use uuid::Uuid;

use crate::{colorize_state, humanize_time, print_response, val_str, OutputFormat};

#[derive(Subcommand)]
pub enum InstanceCmd {
    /// Create a new instance.
    Create {
        /// Sequence ID to run.
        #[arg(long)]
        sequence_id: Uuid,
        /// Tenant identifier.
        #[arg(long)]
        tenant_id: String,
        /// Namespace.
        #[arg(long, default_value = "default")]
        namespace: String,
        /// JSON context (inline or @file path).
        #[arg(long)]
        context: Option<String>,
    },
    /// Get a single instance by ID.
    Get {
        id: Uuid,
        /// Poll every 2 seconds until the instance reaches a terminal state.
        #[arg(long, short)]
        watch: bool,
    },
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
pub async fn run(
    client: &Client,
    base: &str,
    cmd: InstanceCmd,
    format: OutputFormat,
) -> Result<()> {
    match cmd {
        InstanceCmd::Create {
            sequence_id,
            tenant_id,
            namespace,
            context,
        } => {
            let ctx_value = match context {
                Some(s) if s.starts_with('@') => {
                    let path = &s[1..];
                    let raw = std::fs::read_to_string(path)
                        .map_err(|e| anyhow::anyhow!("failed to read {path}: {e}"))?;
                    serde_json::from_str(&raw)
                        .map_err(|e| anyhow::anyhow!("invalid JSON in {path}: {e}"))?
                }
                Some(s) => serde_json::from_str(&s)
                    .map_err(|e| anyhow::anyhow!("invalid JSON context: {e}"))?,
                None => serde_json::json!({}),
            };
            let body = serde_json::json!({
                "sequence_id": sequence_id,
                "tenant_id": tenant_id,
                "namespace": namespace,
                "context": ctx_value,
            });
            let resp = client
                .post(format!("{base}/instances"))
                .json(&body)
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        InstanceCmd::Get { id, watch } => {
            if watch {
                let terminal = ["completed", "failed", "cancelled"];
                loop {
                    // Clear screen for repaint.
                    print!("\x1b[2J\x1b[H");
                    let resp = client.get(format!("{base}/instances/{id}")).send().await?;
                    let body: Value = resp.json().await?;
                    let state = val_str(&body, "state");
                    println!("{}", serde_json::to_string_pretty(&body)?);
                    if terminal.contains(&state.as_str()) {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            } else {
                let resp = client.get(format!("{base}/instances/{id}")).send().await?;
                print_response(resp, format).await?;
            }
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

            match format {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&body)?);
                }
                OutputFormat::Table => {
                    // Support both envelope {"items":[...]} and bare array [...].
                    let arr_ref = body
                        .get("items")
                        .and_then(|v| v.as_array())
                        .or_else(|| body.as_array());
                    if let Some(arr) = arr_ref {
                        if arr.is_empty() {
                            println!("No instances found.");
                        } else {
                            let rows: Vec<InstanceRow> = arr
                                .iter()
                                .map(|v| InstanceRow {
                                    id: val_str(v, "id"),
                                    state: colorize_state(&val_str(v, "state")),
                                    tenant: val_str(v, "tenant_id"),
                                    namespace: val_str(v, "namespace"),
                                    priority: val_str(v, "priority"),
                                    next_fire: humanize_time(&val_str(v, "next_fire_at")),
                                    updated: humanize_time(&val_str(v, "updated_at")),
                                })
                                .collect();
                            println!("{}", Table::new(rows));
                            if body.get("has_more") == Some(&Value::Bool(true)) {
                                println!(
                                    "(more results available — increase --limit or add filters)"
                                );
                            }
                        }
                    } else {
                        println!("{}", serde_json::to_string_pretty(&body)?);
                    }
                }
            }
        }
        InstanceCmd::Tree { id } => {
            let resp = client
                .get(format!("{base}/instances/{id}/tree"))
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        InstanceCmd::Outputs { id } => {
            let resp = client
                .get(format!("{base}/instances/{id}/outputs"))
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        InstanceCmd::SetState { id, state } => {
            let resp = client
                .patch(format!("{base}/instances/{id}/state"))
                .json(&serde_json::json!({ "state": state }))
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        InstanceCmd::Retry { id } => {
            let resp = client
                .post(format!("{base}/instances/{id}/retry"))
                .send()
                .await?;
            print_response(resp, format).await?;
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
            print_response(resp, format).await?;
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
            print_response(resp, format).await?;
        }
    }
    Ok(())
}
