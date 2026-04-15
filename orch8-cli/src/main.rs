use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use reqwest::Client;
use serde_json::Value;
use tabled::{Table, Tabled};
use uuid::Uuid;

/// orch8 — CLI for the Orch8.io durable task engine.
#[derive(Parser)]
#[command(name = "orch8", version, about)]
struct Cli {
    /// Base URL of the Orch8 API server.
    #[arg(long, env = "ORCH8_URL", default_value = "http://127.0.0.1:8080")]
    url: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Check engine health.
    Health,
    /// Instance management.
    #[command(subcommand)]
    Instance(InstanceCmd),
    /// Sequence management.
    #[command(subcommand)]
    Sequence(SequenceCmd),
    /// Cron schedule management.
    #[command(subcommand)]
    Cron(CronCmd),
    /// Send a signal to an instance.
    Signal {
        /// Instance ID.
        instance_id: Uuid,
        /// Signal type (e.g. resume, cancel, approve).
        signal_type: String,
        /// Optional JSON payload.
        #[arg(long)]
        payload: Option<String>,
    },
    /// Checkpoint management.
    #[command(subcommand)]
    Checkpoint(CheckpointCmd),
}

#[derive(Subcommand)]
enum InstanceCmd {
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

#[derive(Subcommand)]
enum SequenceCmd {
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

#[derive(Subcommand)]
enum CronCmd {
    /// List cron schedules.
    List {
        #[arg(long)]
        tenant_id: Option<String>,
    },
    /// Get a cron schedule by ID.
    Get { id: Uuid },
    /// Delete a cron schedule.
    Delete { id: Uuid },
}

#[derive(Subcommand)]
enum CheckpointCmd {
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

// === Table row types for display ===

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

#[derive(Tabled)]
struct CronRow {
    id: String,
    tenant: String,
    expression: String,
    enabled: String,
    next_fire: String,
}

fn val_str(v: &Value, key: &str) -> String {
    match v.get(key) {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Null) | None => "-".into(),
        Some(other) => other.to_string(),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = Client::new();
    let base = cli.url.trim_end_matches('/');

    match cli.command {
        Commands::Health => {
            let resp = client
                .get(format!("{base}/healthz"))
                .send()
                .await
                .context("failed to reach server")?;
            let status = resp.status();
            let body: Value = resp.json().await.unwrap_or(Value::Null);
            if status.is_success() {
                println!("OK {}", serde_json::to_string_pretty(&body)?);
            } else {
                anyhow::bail!("Health check failed: {status} {body}");
            }
        }
        Commands::Instance(cmd) => handle_instance(&client, base, cmd).await?,
        Commands::Sequence(cmd) => handle_sequence(&client, base, cmd).await?,
        Commands::Cron(cmd) => handle_cron(&client, base, cmd).await?,
        Commands::Signal {
            instance_id,
            signal_type,
            payload,
        } => {
            let payload_val: Value = payload
                .map(|p| serde_json::from_str(&p))
                .transpose()
                .context("invalid JSON payload")?
                .unwrap_or(Value::Null);

            let resp = client
                .post(format!("{base}/instances/{instance_id}/signals"))
                .json(&serde_json::json!({
                    "signal_type": signal_type,
                    "payload": payload_val,
                }))
                .send()
                .await?;
            print_response(resp).await?;
        }
        Commands::Checkpoint(cmd) => handle_checkpoint(&client, base, cmd).await?,
    }

    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn handle_instance(client: &Client, base: &str, cmd: InstanceCmd) -> Result<()> {
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

async fn handle_sequence(client: &Client, base: &str, cmd: SequenceCmd) -> Result<()> {
    match cmd {
        SequenceCmd::Get { id } => {
            let resp = client.get(format!("{base}/sequences/{id}")).send().await?;
            print_response(resp).await?;
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
            print_response(resp).await?;
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
            print_response(resp).await?;
        }
        SequenceCmd::Deprecate { id } => {
            let resp = client
                .post(format!("{base}/sequences/{id}/deprecate"))
                .send()
                .await?;
            print_response(resp).await?;
        }
    }
    Ok(())
}

async fn handle_cron(client: &Client, base: &str, cmd: CronCmd) -> Result<()> {
    match cmd {
        CronCmd::List { tenant_id } => {
            let mut params = vec![];
            if let Some(t) = &tenant_id {
                params.push(("tenant_id", t.as_str()));
            }
            let resp = client
                .get(format!("{base}/cron"))
                .query(&params)
                .send()
                .await?;
            let body: Value = resp.json().await?;

            if let Some(arr) = body.as_array() {
                if arr.is_empty() {
                    println!("No cron schedules found.");
                } else {
                    let rows: Vec<CronRow> = arr
                        .iter()
                        .map(|v| CronRow {
                            id: val_str(v, "id"),
                            tenant: val_str(v, "tenant_id"),
                            expression: val_str(v, "cron_expression"),
                            enabled: val_str(v, "enabled"),
                            next_fire: val_str(v, "next_fire_at"),
                        })
                        .collect();
                    println!("{}", Table::new(rows));
                }
            } else {
                println!("{}", serde_json::to_string_pretty(&body)?);
            }
        }
        CronCmd::Get { id } => {
            let resp = client.get(format!("{base}/cron/{id}")).send().await?;
            print_response(resp).await?;
        }
        CronCmd::Delete { id } => {
            let resp = client.delete(format!("{base}/cron/{id}")).send().await?;
            if resp.status().is_success() {
                println!("Deleted cron schedule {id}");
            } else {
                let status = resp.status();
                let body: Value = resp.json().await.unwrap_or(Value::Null);
                anyhow::bail!("{status}: {body}");
            }
        }
    }
    Ok(())
}

async fn handle_checkpoint(client: &Client, base: &str, cmd: CheckpointCmd) -> Result<()> {
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
                .get(format!(
                    "{base}/instances/{instance_id}/checkpoints/latest"
                ))
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

async fn print_response(resp: reqwest::Response) -> Result<()> {
    let status = resp.status();
    let body: Value = resp.json().await.unwrap_or(Value::Null);

    if status.is_success() {
        println!("{}", serde_json::to_string_pretty(&body)?);
    } else {
        anyhow::bail!("{status}: {}", serde_json::to_string_pretty(&body)?);
    }
    Ok(())
}
