use anyhow::Result;
use clap::{Parser, Subcommand};
use reqwest::Client;
use serde_json::Value;

mod commands;

use commands::checkpoint::CheckpointCmd;
use commands::cron::CronCmd;
use commands::instance::InstanceCmd;
use commands::sequence::SequenceCmd;

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
        instance_id: uuid::Uuid,
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

pub fn val_str(v: &Value, key: &str) -> String {
    match v.get(key) {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Null) | None => "-".into(),
        Some(other) => other.to_string(),
    }
}

pub async fn print_response(resp: reqwest::Response) -> Result<()> {
    let status = resp.status();
    let body: Value = resp.json().await.unwrap_or(Value::Null);

    if status.is_success() {
        println!("{}", serde_json::to_string_pretty(&body)?);
    } else {
        anyhow::bail!("{status}: {}", serde_json::to_string_pretty(&body)?);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = Client::new();
    let base = cli.url.trim_end_matches('/');

    match cli.command {
        Commands::Health => commands::health::run(&client, base).await?,
        Commands::Instance(cmd) => commands::instance::run(&client, base, cmd).await?,
        Commands::Sequence(cmd) => commands::sequence::run(&client, base, cmd).await?,
        Commands::Cron(cmd) => commands::cron::run(&client, base, cmd).await?,
        Commands::Signal {
            instance_id,
            signal_type,
            payload,
        } => commands::signal::run(&client, base, instance_id, signal_type, payload).await?,
        Commands::Checkpoint(cmd) => commands::checkpoint::run(&client, base, cmd).await?,
    }

    Ok(())
}
