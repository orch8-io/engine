use std::{fmt::Write as _, time::Duration};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use reqwest::{Client, header};
use serde_json::Value;

mod commands;
mod templates;

use commands::bootstrap::BootstrapCmd;
use commands::checkpoint::CheckpointCmd;
use commands::config::ConfigCmd;
use commands::context::ContextCmd;
use commands::continuity::{ExecutionCmd, RuntimeCmd};
use commands::cron::CronCmd;
use commands::debugger::DebugCmd;
use commands::demo::DemoCmd;
use commands::deploy::DeployCmd;
use commands::dev::DevCmd;
use commands::doctor::DoctorCmd;
use commands::inspect_cmd::InspectCmd;
use commands::instance::InstanceCmd;
use commands::package_cmd::PackageCmd;
use commands::portable::PortableCmd;
use commands::release::ReleaseCmd;
use commands::sequence::SequenceCmd;
use commands::support_bundle::SupportBundleCmd;
use commands::templates::TemplatesCmd;

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
    /// Explicit named fleet context. Overrides the selected context.
    #[arg(long, global = true)]
    context: Option<String>,

    /// Fleet context file containing URL, tenant, and credential records.
    #[arg(long, global = true, env = "ORCH8_CONTEXTS_FILE")]
    contexts_file: Option<std::path::PathBuf>,

    /// Base URL of the Orch8 API server.
    #[arg(
        long,
        global = true,
        env = "ORCH8_URL",
        default_value = "http://127.0.0.1:8080/api/v1"
    )]
    url: String,

    /// API key sent as `x-api-key`. Required when the server runs with auth
    /// (i.e. without `--insecure`). Reads `ORCH8_API_KEY` from the environment
    /// by default so secrets don't show in shell history.
    #[arg(long, global = true, env = "ORCH8_API_KEY", hide_env_values = true)]
    api_key: Option<String>,

    /// Tenant identifier sent as `x-tenant-id`. Required when the server
    /// enforces tenant headers; optional otherwise.
    #[arg(long, global = true, env = "ORCH8_TENANT_ID")]
    tenant_id: Option<String>,

    /// Output format: table (default) or json.
    #[arg(short, long, global = true, default_value = "table")]
    output: OutputFormat,

    /// Skip confirmation prompts for destructive commands.
    #[arg(long, global = true)]
    yes: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Check engine health.
    Health,
    /// Securely scaffold, preflight, migrate, start, and verify a production-capable node.
    Bootstrap(BootstrapCmd),
    /// Diagnose configuration, connectivity, compatibility, workers, continuity, and an optional instance.
    Doctor(DoctorCmd),
    /// Export a strictly redacted operational support bundle.
    SupportBundle(SupportBundleCmd),
    /// Instance management.
    #[command(subcommand)]
    Instance(InstanceCmd),
    /// Portable execution handoff, capsules, effects, and provenance.
    #[command(subcommand)]
    Execution(ExecutionCmd),
    /// Runtime capability registration and discovery.
    #[command(subcommand)]
    Runtime(RuntimeCmd),
    /// Framework-neutral agent handoff protocol, wrappers, profiles, and conformance.
    #[command(subcommand)]
    Portable(PortableCmd),
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
    /// Inspect template resolution for a block (read-only).
    #[command(subcommand)]
    Inspect(InspectCmd),
    /// Bounded terminal timeline, checkpoint, effect, and fork debugger.
    #[command(subcommand)]
    Debug(DebugCmd),
    /// Verify, validate, canary, observe, and optionally promote a release.
    Deploy(DeployCmd),
    /// Safe workflow releases: diff, validate, canary, promote, rollback.
    #[command(subcommand)]
    Release(ReleaseCmd),
    /// Signed workflow packages: keygen, build, verify, inspect, install.
    #[command(subcommand)]
    Package(PackageCmd),
    /// Checkpoint management.
    #[command(subcommand)]
    Checkpoint(CheckpointCmd),
    /// Configuration management.
    #[command(subcommand)]
    Config(ConfigCmd),
    /// Named fleet URL/tenant/credential contexts.
    #[command(subcommand)]
    Context(ContextCmd),
    /// Initialize a new Orch8 project (config, example sequence, docker-compose).
    Init {
        /// Directory to initialize in (defaults to current directory).
        #[arg(default_value = ".")]
        dir: String,
        /// Built-in template to write as sequence.json (see `orch8 templates list`).
        #[arg(long, default_value = "default")]
        template: String,
    },
    /// Browse built-in sequence templates.
    #[command(subcommand)]
    Templates(TemplatesCmd),
    /// Run self-contained demonstrations backed by real engine protocols.
    #[command(subcommand)]
    Demo(DemoCmd),
    /// Local workflow studio: run sequences with hot reload, optional HTTP
    /// API server, embedded dashboard, directory watching, and virtual time.
    Dev(DevCmd),
    /// Replay/diff tooling.
    #[command(subcommand)]
    Test(commands::test_cmd::TestCmd),
    /// Run database migrations against Postgres. Use this in CI/CD pipelines
    /// or init containers instead of the server's built-in `run_migrations` flag
    /// so that rolling deployments are safe.
    Migrate {
        /// Database URL (overrides `ORCH8_DATABASE_URL`).
        #[arg(long, env = "ORCH8_DATABASE_URL")]
        database_url: String,
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

pub fn print_table(headers: &[&str], rows: &[Vec<String>]) {
    print!("{}", format_table(headers, rows));
}

pub fn format_table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let widths: Vec<usize> = headers
        .iter()
        .enumerate()
        .map(|(idx, header)| {
            rows.iter()
                .filter_map(|row| row.get(idx))
                .map(String::len)
                .max()
                .unwrap_or(0)
                .max(header.len())
        })
        .collect();

    let mut output = String::new();
    write_table_row(&mut output, headers.iter().copied(), &widths);
    write_table_separator(&mut output, &widths);
    for row in rows {
        write_table_row(&mut output, row.iter().map(String::as_str), &widths);
    }
    output
}

fn write_table_row<'a>(
    output: &mut String,
    cells: impl IntoIterator<Item = &'a str>,
    widths: &[usize],
) {
    let mut first = true;
    for (cell, width) in cells.into_iter().zip(widths.iter().copied()) {
        if first {
            first = false;
        } else {
            output.push_str("  ");
        }
        let _ = write!(output, "{cell:<width$}");
    }
    output.push('\n');
}

fn write_table_separator(output: &mut String, widths: &[usize]) {
    let cells: Vec<String> = widths.iter().map(|width| "-".repeat(*width)).collect();
    write_table_row(output, cells.iter().map(String::as_str), widths);
}

pub async fn print_response(resp: reqwest::Response, _format: OutputFormat) -> Result<()> {
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    let body: Value = serde_json::from_str(&text).unwrap_or(Value::String(text));

    if status.is_success() {
        println!("{}", serde_json::to_string_pretty(&body)?);
    } else {
        let message = body
            .pointer("/error/message")
            .and_then(Value::as_str)
            .or_else(|| body.get("error").and_then(Value::as_str))
            .unwrap_or_else(|| status.canonical_reason().unwrap_or("request failed"));
        let hint = match status {
            reqwest::StatusCode::UNAUTHORIZED => {
                " Set --api-key or ORCH8_API_KEY to the server's configured key."
            }
            reqwest::StatusCode::BAD_REQUEST if message.contains("tenant") => {
                " Set --tenant-id or ORCH8_TENANT_ID."
            }
            _ => "",
        };
        anyhow::bail!("{status}: {message}{hint}");
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
        let mut v = header::HeaderValue::from_str(k)
            .map_err(|e| anyhow::anyhow!("invalid --api-key value: {e}"))?;
        v.set_sensitive(true);
        headers.insert("x-api-key", v);
    }
    if let Some(t) = tenant_id.filter(|s| !s.is_empty()) {
        let v = header::HeaderValue::from_str(t)
            .map_err(|e| anyhow::anyhow!("invalid --tenant-id value: {e}"))?;
        headers.insert("x-tenant-id", v);
    }
    Ok(Client::builder()
        .default_headers(headers)
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .build()?)
}

/// Atomically replace `path` with `contents`: write to a temp file in the
/// same directory, fsync, then rename so a crash never leaves a torn file.
pub(crate) fn atomic_write(path: &std::path::Path, contents: &[u8]) -> Result<()> {
    use std::io::Write as _;

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    let mut file = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create temporary file beside {}", path.display()))?;
    file.write_all(contents)?;
    file.as_file().sync_all()?;
    file.persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("atomically replace {}", path.display()))?;
    Ok(())
}

static ASSUME_YES: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub(crate) fn confirm_destructive(prompt: &str) -> Result<()> {
    use std::io::{IsTerminal as _, Write as _};

    if cfg!(test) || ASSUME_YES.load(std::sync::atomic::Ordering::Relaxed) {
        return Ok(());
    }
    if !std::io::stdin().is_terminal() {
        anyhow::bail!("{prompt}; rerun with --yes in non-interactive environments");
    }
    eprint!("{prompt} [y/N] ");
    std::io::stderr().flush()?;
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    if matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
        Ok(())
    } else {
        anyhow::bail!("cancelled")
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    use std::io::IsTerminal as _;

    let mut cli = Cli::parse();
    ASSUME_YES.store(cli.yes, std::sync::atomic::Ordering::Relaxed);
    let format = cli.output;
    if std::env::var_os("NO_COLOR").is_some()
        || (!std::io::stdout().is_terminal() && !std::io::stderr().is_terminal())
    {
        owo_colors::set_override(false);
    }

    if let Commands::Completions { shell } = cli.command {
        let mut cmd = <Cli as clap::CommandFactory>::command();
        clap_complete::generate(shell, &mut cmd, "orch8", &mut std::io::stdout());
        return Ok(());
    }

    // Handle dev before building the HTTP client — it runs an embedded
    // engine and never talks to a server.
    if let Commands::Dev(cmd) = cli.command {
        return commands::dev::run(cmd).await;
    }

    // Demonstrations are self-contained and deliberately do not require a
    // running API server or credentials.
    if let Commands::Demo(cmd) = cli.command {
        return commands::demo::run(cmd, format).await;
    }

    if let Commands::Bootstrap(cmd) = cli.command {
        return commands::bootstrap::run(cmd).await;
    }

    let contexts_path = cli
        .contexts_file
        .clone()
        .unwrap_or_else(commands::context::default_path);
    if let Commands::Context(cmd) = cli.command {
        return commands::context::run(&contexts_path, cmd);
    }

    if let Some(context) = commands::context::resolve(&contexts_path, cli.context.as_deref())? {
        cli.url = context.url;
        cli.tenant_id = Some(context.tenant_id);
        cli.api_key = Some(context.api_key);
    }

    // Handle migrate before building the HTTP client — it does not need one.
    if let Commands::Migrate { database_url } = cli.command {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;
        sqlx::migrate!("../migrations").run(&pool).await?;
        println!("Migrations applied successfully");
        return Ok(());
    }

    let client = build_client(cli.api_key.as_deref(), cli.tenant_id.as_deref())?;
    let base = cli.url.trim_end_matches('/');

    match cli.command {
        Commands::Health => commands::health::run(&client, base, format).await?,
        Commands::Doctor(cmd) => commands::doctor::run(&client, base, cmd, format).await?,
        Commands::SupportBundle(cmd) => commands::support_bundle::run(&client, base, cmd).await?,
        Commands::Instance(cmd) => {
            commands::instance::run(&client, base, cmd, format, cli.tenant_id.as_deref()).await?;
        }
        Commands::Execution(cmd) => {
            commands::continuity::run_execution(&client, base, cmd, format).await?;
        }
        Commands::Runtime(cmd) => {
            commands::continuity::run_runtime(&client, base, cmd, format).await?;
        }
        Commands::Portable(cmd) => {
            commands::portable::run(&client, base, cmd, format).await?;
        }
        Commands::Sequence(cmd) => commands::sequence::run(&client, base, cmd, format).await?,
        Commands::Cron(cmd) => commands::cron::run(&client, base, cmd, format).await?,
        Commands::Signal {
            instance_id,
            signal_type,
            payload,
        } => {
            commands::signal::run(&client, base, instance_id, signal_type, payload, format).await?;
        }
        Commands::Inspect(cmd) => commands::inspect_cmd::run(&client, base, cmd, format).await?,
        Commands::Debug(cmd) => commands::debugger::run(&client, base, cmd, format).await?,
        Commands::Deploy(cmd) => commands::deploy::run(&client, base, cmd, format).await?,
        Commands::Release(cmd) => commands::release::run(&client, base, cmd, format).await?,
        Commands::Package(cmd) => commands::package_cmd::run(&client, base, cmd, format).await?,
        Commands::Checkpoint(cmd) => commands::checkpoint::run(&client, base, cmd, format).await?,
        Commands::Config(cmd) => commands::config::run(cmd)?,
        Commands::Context(..) => {
            anyhow::bail!(
                "internal error: context command should have been handled before dispatch"
            )
        }
        Commands::Init { dir, template } => commands::init::run(&dir, &template)?,
        Commands::Templates(cmd) => commands::templates::run(cmd)?,
        Commands::Test(cmd) => commands::test_cmd::run(&client, base, cmd, format).await?,
        Commands::Dev(..)
        | Commands::Bootstrap(..)
        | Commands::Demo(..)
        | Commands::Migrate { .. }
        | Commands::Completions { .. } => {
            anyhow::bail!("internal error: command should have been handled before dispatch")
        }
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
    fn format_table_aligns_columns() {
        let rows = vec![
            vec!["a".to_string(), "long".to_string()],
            vec!["wide".to_string(), "b".to_string()],
        ];
        assert_eq!(
            format_table(&["id", "name"], &rows),
            "id    name\n----  ----\na     long\nwide  b   \n"
        );
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
    fn connection_flags_are_global_after_subcommand() {
        use clap::Parser;
        let cli = Cli::try_parse_from([
            "orch8",
            "health",
            "--url",
            "http://127.0.0.1:18999/api/v1",
            "--api-key",
            "secret",
            "--tenant-id",
            "demo",
        ])
        .unwrap();
        assert_eq!(cli.url, "http://127.0.0.1:18999/api/v1");
        assert_eq!(cli.api_key.as_deref(), Some("secret"));
        assert_eq!(cli.tenant_id.as_deref(), Some("demo"));
        assert!(matches!(cli.command, Commands::Health));
    }

    #[test]
    fn instance_create_uses_the_global_tenant_flag() {
        use clap::Parser;
        let sequence_id = uuid::Uuid::new_v4().to_string();
        let cli = Cli::try_parse_from([
            "orch8",
            "instance",
            "create",
            "--sequence-id",
            &sequence_id,
            "--tenant-id",
            "demo",
        ])
        .unwrap();

        assert_eq!(cli.tenant_id.as_deref(), Some("demo"));
        assert!(matches!(
            cli.command,
            Commands::Instance(commands::instance::InstanceCmd::Create { .. })
        ));
    }

    #[test]
    fn cli_parses_completions_command() {
        use clap::Parser;
        let cli = Cli::try_parse_from(["orch8", "completions", "bash"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn cli_parses_init_without_template_flag() {
        use clap::Parser;
        let cli = Cli::try_parse_from(["orch8", "init", "my-project"]).unwrap();
        match cli.command {
            Commands::Init { dir, template } => {
                assert_eq!(dir, "my-project");
                assert_eq!(template, "default");
            }
            _ => panic!("expected init command"),
        }
    }

    #[test]
    fn cli_parses_init_with_template_flag() {
        use clap::Parser;
        let cli = Cli::try_parse_from(["orch8", "init", ".", "--template", "react-loop"]).unwrap();
        match cli.command {
            Commands::Init { dir, template } => {
                assert_eq!(dir, ".");
                assert_eq!(template, "react-loop");
            }
            _ => panic!("expected init command"),
        }
    }

    #[test]
    fn cli_parses_templates_subcommands() {
        use clap::Parser;
        let cli = Cli::try_parse_from(["orch8", "templates", "list"]).unwrap();
        assert!(matches!(
            cli.command,
            Commands::Templates(TemplatesCmd::List)
        ));

        let cli = Cli::try_parse_from(["orch8", "templates", "show", "react-loop"]).unwrap();
        match cli.command {
            Commands::Templates(TemplatesCmd::Show { name }) => assert_eq!(name, "react-loop"),
            _ => panic!("expected templates show command"),
        }
    }

    #[test]
    fn cli_parses_migrate_command() {
        use clap::Parser;
        let cli = Cli::try_parse_from([
            "orch8",
            "migrate",
            "--database-url",
            "postgres://localhost/db",
        ]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert!(matches!(cli.command, Commands::Migrate { .. }));
    }
}
