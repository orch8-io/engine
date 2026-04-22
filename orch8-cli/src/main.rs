use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use reqwest::{header, Client};
use serde_json::Value;

mod commands;

use commands::checkpoint::CheckpointCmd;
use commands::config::ConfigCmd;
use commands::cron::CronCmd;
use commands::instance::InstanceCmd;
use commands::sequence::SequenceCmd;

/// Output format for CLI commands.
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum OutputFormat {
    /// Human-readable table (default for list commands).
    #[default]
    Table,
    /// Raw JSON output (useful for piping to jq).
    Json,
}

/// orch8 — CLI for the Orch8.io durable task engine.
#[derive(Parser)]
#[command(
    name = "orch8",
    version,
    about,
    after_help = "© Oleksii Vasylenko Tecnologia LTDA — BUSL-1.1 — https://orch8.io"
)]
struct Cli {
    /// Base URL of the Orch8 API server.
    #[arg(long, env = "ORCH8_URL", default_value = "http://127.0.0.1:8080")]
    url: String,

    /// API key sent as `x-api-key`. Required when the server runs with auth
    /// (i.e. without `--insecure`). Reads `ORCH8_API_KEY` from the environment
    /// by default so secrets don't show in shell history.
    #[arg(long, env = "ORCH8_API_KEY", hide_env_values = true)]
    api_key: Option<String>,

    /// Tenant identifier sent as `x-tenant-id`. Required when the server
    /// enforces tenant headers; optional otherwise.
    #[arg(long, env = "ORCH8_TENANT_ID")]
    tenant_id: Option<String>,

    /// Output format: table (default) or json.
    #[arg(short, long, global = true, default_value = "table")]
    output: OutputFormat,

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
    /// Configuration management.
    #[command(subcommand)]
    Config(ConfigCmd),
    /// Initialize a new Orch8 project (config, example sequence, docker-compose).
    Init {
        /// Directory to initialize in (defaults to current directory).
        #[arg(default_value = ".")]
        dir: String,
    },
    /// Generate shell completions.
    Completions {
        /// Shell to generate completions for.
        shell: clap_complete::Shell,
    },
}

pub fn val_str(v: &Value, key: &str) -> String {
    match v.get(key) {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Null) | None => "-".into(),
        Some(other) => other.to_string(),
    }
}

/// Colorize an instance state string for terminal display.
pub fn colorize_state(state: &str) -> String {
    use owo_colors::OwoColorize;
    match state {
        "running" => state.blue().bold().to_string(),
        "completed" => state.green().to_string(),
        "failed" => state.red().bold().to_string(),
        "cancelled" => state.red().to_string(),
        "paused" => state.yellow().to_string(),
        "waiting" => state.cyan().to_string(),
        "scheduled" => state.dimmed().to_string(),
        other => other.to_string(),
    }
}

/// Format an ISO-8601 timestamp as a human-readable relative time (e.g. "3m ago", "in 2h").
pub fn humanize_time(iso: &str) -> String {
    let Ok(dt) = chrono::DateTime::parse_from_rfc3339(iso) else {
        return iso.to_string();
    };
    let now = chrono::Utc::now();
    let diff = now.signed_duration_since(dt);
    let secs = diff.num_seconds();

    if secs.abs() < 60 {
        if secs >= 0 {
            "just now".to_string()
        } else {
            "in <1m".to_string()
        }
    } else if secs.abs() < 3600 {
        let m = secs.abs() / 60;
        if secs > 0 {
            format!("{m}m ago")
        } else {
            format!("in {m}m")
        }
    } else if secs.abs() < 86400 {
        let h = secs.abs() / 3600;
        let m = (secs.abs() % 3600) / 60;
        if m == 0 {
            if secs > 0 {
                format!("{h}h ago")
            } else {
                format!("in {h}h")
            }
        } else if secs > 0 {
            format!("{h}h {m}m ago")
        } else {
            format!("in {h}h {m}m")
        }
    } else {
        let d = secs.abs() / 86400;
        if secs > 0 {
            format!("{d}d ago")
        } else {
            format!("in {d}d")
        }
    }
}

pub async fn print_response(resp: reqwest::Response, _format: OutputFormat) -> Result<()> {
    let status = resp.status();
    let body: Value = resp.json().await.unwrap_or(Value::Null);

    if status.is_success() {
        println!("{}", serde_json::to_string_pretty(&body)?);
    } else {
        anyhow::bail!("{status}: {}", serde_json::to_string_pretty(&body)?);
    }
    Ok(())
}

/// Build the shared reqwest client, stamping `x-api-key` and `x-tenant-id`
/// as default headers so every subcommand authenticates without having to
/// thread the values through. Invalid header values (control chars, non-
/// ASCII) fall through with a warning instead of crashing the CLI.
fn build_client(api_key: Option<&str>, tenant_id: Option<&str>) -> Result<Client> {
    let mut headers = header::HeaderMap::new();
    if let Some(k) = api_key.filter(|s| !s.is_empty()) {
        match header::HeaderValue::from_str(k) {
            Ok(v) => {
                let mut v = v;
                v.set_sensitive(true);
                headers.insert("x-api-key", v);
            }
            Err(e) => eprintln!("warning: invalid --api-key value, header not sent: {e}"),
        }
    }
    if let Some(t) = tenant_id.filter(|s| !s.is_empty()) {
        match header::HeaderValue::from_str(t) {
            Ok(v) => {
                headers.insert("x-tenant-id", v);
            }
            Err(e) => eprintln!("warning: invalid --tenant-id value, header not sent: {e}"),
        }
    }
    Ok(Client::builder().default_headers(headers).build()?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let format = cli.output;

    if let Commands::Completions { shell } = cli.command {
        let mut cmd = <Cli as clap::CommandFactory>::command();
        clap_complete::generate(shell, &mut cmd, "orch8", &mut std::io::stdout());
        return Ok(());
    }

    let client = build_client(cli.api_key.as_deref(), cli.tenant_id.as_deref())?;
    let base = cli.url.trim_end_matches('/');

    match cli.command {
        Commands::Health => commands::health::run(&client, base).await?,
        Commands::Instance(cmd) => commands::instance::run(&client, base, cmd, format).await?,
        Commands::Sequence(cmd) => commands::sequence::run(&client, base, cmd, format).await?,
        Commands::Cron(cmd) => commands::cron::run(&client, base, cmd, format).await?,
        Commands::Signal {
            instance_id,
            signal_type,
            payload,
        } => {
            commands::signal::run(&client, base, instance_id, signal_type, payload, format).await?;
        }
        Commands::Checkpoint(cmd) => commands::checkpoint::run(&client, base, cmd, format).await?,
        Commands::Config(cmd) => commands::config::run(cmd)?,
        Commands::Init { dir } => commands::init::run(&dir)?,
        Commands::Completions { .. } => unreachable!(),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn humanize_time_just_now() {
        let now = chrono::Utc::now().to_rfc3339();
        let result = humanize_time(&now);
        assert_eq!(result, "just now");
    }

    #[test]
    fn humanize_time_minutes_ago() {
        let past = (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339();
        let result = humanize_time(&past);
        assert_eq!(result, "5m ago");
    }

    #[test]
    fn humanize_time_hours_ago() {
        let past = (chrono::Utc::now() - chrono::Duration::hours(2)).to_rfc3339();
        let result = humanize_time(&past);
        assert_eq!(result, "2h ago");
    }

    #[test]
    fn humanize_time_future() {
        let future = (chrono::Utc::now() + chrono::Duration::minutes(30)).to_rfc3339();
        let result = humanize_time(&future);
        assert!(
            result.starts_with("in ") && result.ends_with('m'),
            "got: {result}"
        );
    }

    #[test]
    fn humanize_time_days_ago() {
        let past = (chrono::Utc::now() - chrono::Duration::days(3)).to_rfc3339();
        let result = humanize_time(&past);
        assert_eq!(result, "3d ago");
    }

    #[test]
    fn humanize_time_invalid_input() {
        assert_eq!(humanize_time("not-a-date"), "not-a-date");
    }

    #[test]
    fn humanize_time_dash_passthrough() {
        assert_eq!(humanize_time("-"), "-");
    }

    #[test]
    fn colorize_state_returns_string_for_all_states() {
        // Just ensure no panics and non-empty output for all known states.
        for state in [
            "running",
            "completed",
            "failed",
            "cancelled",
            "paused",
            "waiting",
            "scheduled",
        ] {
            let result = colorize_state(state);
            assert!(
                !result.is_empty(),
                "colorize_state({state}) should not be empty"
            );
        }
        // Unknown state passes through.
        assert_eq!(colorize_state("unknown"), "unknown");
    }

    #[test]
    fn output_format_default_is_table() {
        let fmt = OutputFormat::default();
        assert!(matches!(fmt, OutputFormat::Table));
    }

    #[test]
    fn val_str_extracts_string() {
        let v = serde_json::json!({"name": "test"});
        assert_eq!(val_str(&v, "name"), "test");
    }

    #[test]
    fn val_str_returns_dash_for_missing() {
        let v = serde_json::json!({});
        assert_eq!(val_str(&v, "missing"), "-");
    }

    #[test]
    fn val_str_returns_dash_for_null() {
        let v = serde_json::json!({"x": null});
        assert_eq!(val_str(&v, "x"), "-");
    }

    #[test]
    fn val_str_formats_numbers() {
        let v = serde_json::json!({"count": 42});
        assert_eq!(val_str(&v, "count"), "42");
    }

    #[test]
    fn cli_parses_output_flag() {
        // Verify the CLI struct accepts -o json
        use clap::Parser;
        let cli = Cli::try_parse_from(["orch8", "-o", "json", "health"]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert!(matches!(cli.output, OutputFormat::Json));
    }

    #[test]
    fn cli_parses_completions_command() {
        use clap::Parser;
        let cli = Cli::try_parse_from(["orch8", "completions", "bash"]);
        assert!(cli.is_ok());
    }
}
