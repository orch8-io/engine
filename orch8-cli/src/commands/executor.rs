//! `orch8 executor join <token>`: turn a cloud-issued join token into an
//! `executor` node configuration (see `docs/HYBRID.md`, `docs/NODE_ROLES.md`).

use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail};
use clap::Subcommand;
use orch8_types::config::EngineConfig;
use orch8_types::join_token::{JoinToken, validate_label};
use serde::Serialize;

use crate::OutputFormat;

#[derive(Debug, Subcommand)]
pub enum ExecutorCmd {
    /// Write an `executor` orch8.toml from a join token (`o8x1.…`) and
    /// optionally start the server. The token is a secret: prefer
    /// `ORCH8_JOIN_TOKEN` or `-` (stdin) over a shell argument.
    Join {
        /// Join token, `-` to read it from stdin. Defaults to `ORCH8_JOIN_TOKEN`.
        #[arg(env = "ORCH8_JOIN_TOKEN", hide_env_values = true)]
        token: String,
        /// Config file to create or update (other sections are preserved).
        #[arg(long, default_value = "orch8.toml")]
        config_out: PathBuf,
        /// Extra placement label, `key=value` (repeatable; overrides token labels).
        #[arg(long = "label", value_name = "KEY=VALUE")]
        labels: Vec<String>,
        /// Worker-id suffix. Defaults to `$HOSTNAME` / the machine host name.
        #[arg(long)]
        hostname: Option<String>,
        /// Replace this process with `orch8-server --config <config-out>`.
        #[arg(long)]
        run: bool,
        /// Server binary for `--run`.
        #[arg(long, env = "ORCH8_SERVER_BIN", default_value = "orch8-server")]
        server_bin: String,
    },
}

#[derive(Debug, Serialize)]
struct JoinReport {
    config: String,
    role: &'static str,
    endpoint: String,
    tenant_id: String,
    worker_id: String,
    runtime_id: String,
    region: Option<String>,
    labels: std::collections::BTreeMap<String, String>,
}

pub fn run(cmd: ExecutorCmd, format: OutputFormat) -> Result<()> {
    match cmd {
        ExecutorCmd::Join {
            token,
            config_out,
            labels,
            hostname,
            run,
            server_bin,
        } => {
            let raw = if token == "-" {
                let mut buf = String::new();
                std::io::Read::read_to_string(&mut std::io::stdin(), &mut buf)
                    .context("read join token from stdin")?;
                buf
            } else {
                token
            };
            let token = JoinToken::parse(&raw).map_err(|e| anyhow::anyhow!("{e}"))?;
            let host = hostname.unwrap_or_else(default_host);
            let report = join(&token, &config_out, &labels, &host)?;
            match format {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
                OutputFormat::Table => {
                    println!("Executor configured: {}", report.config);
                    println!("  control plane: {}", report.endpoint);
                    println!("  tenant:        {}", report.tenant_id);
                    println!("  worker id:     {}", report.worker_id);
                    println!("  runtime id:    {}", report.runtime_id);
                    if let Some(region) = &report.region {
                        println!("  region:        {region}");
                    }
                    for (k, v) in &report.labels {
                        println!("  label:         {k}={v}");
                    }
                    if !run {
                        println!(
                            "Start it with: orch8-server --config {}  (the file holds a secret; mode 0600)",
                            report.config
                        );
                    }
                }
            }
            if run {
                start_server(&server_bin, &config_out)?;
            }
            Ok(())
        }
    }
}

fn default_host() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.trim().is_empty())
        .or_else(|| {
            std::process::Command::new("hostname")
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|h| h.trim().to_owned())
                .filter(|h| !h.is_empty())
        })
        .unwrap_or_default()
}

fn parse_label(raw: &str) -> Result<(String, String)> {
    let (key, value) = raw
        .split_once('=')
        .with_context(|| format!("--label `{raw}` must be KEY=VALUE"))?;
    validate_label(key, value).map_err(|e| anyhow::anyhow!("--label `{raw}`: {e}"))?;
    Ok((key.to_owned(), value.to_owned()))
}

/// Merge the token into `path` (creating it) and validate the result parses
/// as a server config with a consistent managed-control section.
fn join(token: &JoinToken, path: &Path, labels: &[String], host: &str) -> Result<JoinReport> {
    let mut document: toml::Table = if path.is_file() {
        std::fs::read_to_string(path)
            .with_context(|| format!("read {}", path.display()))?
            .parse()
            .with_context(|| format!("{} is not valid TOML", path.display()))?
    } else {
        toml::Table::new()
    };

    let mut config: EngineConfig = toml::Value::Table(document.clone())
        .try_into()
        .with_context(|| format!("{} is not a valid orch8 config", path.display()))?;
    token.apply_to_config(&mut config, host);
    for raw in labels {
        let (key, value) = parse_label(raw)?;
        config.node.labels.insert(key, value);
    }
    if let Err(errors) = config.validate() {
        let relevant: Vec<&String> = errors
            .iter()
            .filter(|e| e.contains("node.") || e.contains("managed_control"))
            .collect();
        if !relevant.is_empty() {
            bail!("resulting config is invalid: {relevant:?}");
        }
    }

    let node = &config.node;
    let mut section = toml::Table::new();
    section.insert("role".into(), "executor".into());
    section.insert(
        "managed_control_endpoint".into(),
        node.managed_control_endpoint.clone().into(),
    );
    section.insert(
        "managed_control_api_key".into(),
        node.managed_control_api_key.expose().to_owned().into(),
    );
    section.insert(
        "managed_control_tenant_id".into(),
        node.managed_control_tenant_id.clone().into(),
    );
    section.insert(
        "managed_control_worker_id".into(),
        node.managed_control_worker_id.clone().into(),
    );
    section.insert(
        "managed_control_runtime_id".into(),
        node.managed_control_runtime_id.clone().into(),
    );
    if !node.region.is_empty() {
        section.insert("region".into(), node.region.clone().into());
    }
    if !node.labels.is_empty() {
        let labels: toml::Table = node
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), toml::Value::from(v.clone())))
            .collect();
        section.insert("labels".into(), toml::Value::Table(labels));
    }
    if !node.managed_control_headers.is_empty() {
        let headers: toml::Table = node
            .managed_control_headers
            .iter()
            .map(|(k, v)| (k.clone(), toml::Value::from(v.clone())))
            .collect();
        section.insert(
            "managed_control_headers".into(),
            toml::Value::Table(headers),
        );
    }
    document.insert("node".into(), toml::Value::Table(section));
    // The REST base for the HTTP fallback, when the token names one. Other
    // `[executor]` settings in the file are preserved.
    if let Some(api_url) = &token.api_url {
        let executor = document
            .entry("executor")
            .or_insert_with(|| toml::Value::Table(toml::Table::new()));
        let Some(executor) = executor.as_table_mut() else {
            bail!("{}: `executor` must be a table", path.display());
        };
        executor.insert("api_url".into(), api_url.trim().to_owned().into());
    }
    let rendered = format!(
        "# Written by `orch8 executor join`. Contains a secret (managed_control_api_key).\n{}",
        toml::to_string(&document)?
    );
    // Round-trip check: the server must accept exactly what we wrote.
    let _: EngineConfig = toml::from_str(&rendered).context("generated config does not parse")?;
    crate::atomic_write_private(path, rendered.as_bytes())?;

    Ok(JoinReport {
        config: path.display().to_string(),
        role: "executor",
        endpoint: node.managed_control_endpoint.clone(),
        tenant_id: node.managed_control_tenant_id.clone(),
        worker_id: node.managed_control_worker_id.clone(),
        runtime_id: node.managed_control_runtime_id.clone(),
        region: (!node.region.is_empty()).then(|| node.region.clone()),
        labels: node.labels.clone(),
    })
}

/// Replace the current process with the server (unix), or run it to
/// completion elsewhere. Arguments are passed as argv, never via a shell.
fn start_server(bin: &str, config: &Path) -> Result<()> {
    let mut command = std::process::Command::new(bin);
    command.arg("--config").arg(config);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let replace_process = <std::process::Command as CommandExt>::exec;
        let error = replace_process(&mut command);
        bail!("failed to start {bin}: {error}");
    }
    #[cfg(not(unix))]
    {
        let status = command
            .status()
            .with_context(|| format!("failed to start {bin}"))?;
        if !status.success() {
            bail!("{bin} exited with {status}");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token() -> JoinToken {
        JoinToken {
            v: 1,
            endpoint: "https://control.orch8.example".into(),
            api_key: "o8k_join_secret".into(),
            tenant_id: "acme".into(),
            runtime_id: uuid::Uuid::now_v7(),
            worker_id_prefix: "acme-dc1".into(),
            labels: std::collections::BTreeMap::from([("gpu".into(), "a10".into())]),
            region: Some("eu-west-1".into()),
            api_url: None,
            headers: std::collections::BTreeMap::new(),
        }
    }

    #[test]
    fn join_writes_routing_headers_and_api_url() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("orch8.toml");
        std::fs::write(
            &path,
            "[executor]
max_concurrent_tasks = 3
",
        )
        .unwrap();
        let token = JoinToken {
            endpoint: "https://engines.orch8.example:50051".into(),
            api_url: Some("https://engines.orch8.example/api/v1".into()),
            headers: std::collections::BTreeMap::from([(
                "fly-force-instance-id".into(),
                "148e21ea7d9389".into(),
            )]),
            ..token()
        };
        join(&token, &path, &[], "h").unwrap();
        let config: EngineConfig =
            toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            config.node.managed_control_endpoint,
            "https://engines.orch8.example:50051"
        );
        assert_eq!(
            config
                .node
                .managed_control_headers
                .get("fly-force-instance-id")
                .map(String::as_str),
            Some("148e21ea7d9389")
        );
        assert_eq!(
            config.executor.api_url,
            "https://engines.orch8.example/api/v1"
        );
        assert_eq!(config.executor.max_concurrent_tasks, 3);
    }

    #[test]
    fn join_writes_private_executor_config_and_preserves_other_sections() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("orch8.toml");
        std::fs::write(
            &path,
            "[engine]\ntick_interval_ms = 250\n[node]\nrole = \"all_in_one\"\n",
        )
        .unwrap();
        let report = join(&token(), &path, &["zone=b".into()], "host 1").unwrap();
        assert_eq!(report.worker_id, "acme-dc1-host-1");
        let written = std::fs::read_to_string(&path).unwrap();
        let config: EngineConfig = toml::from_str(&written).unwrap();
        assert_eq!(config.node.role, orch8_types::config::NodeRole::Executor);
        assert_eq!(config.engine.tick_interval_ms, 250);
        assert_eq!(
            config.node.managed_control_api_key.expose(),
            "o8k_join_secret"
        );
        assert_eq!(
            config.node.labels.get("zone").map(String::as_str),
            Some("b")
        );
        assert_eq!(
            config.node.labels.get("gpu").map(String::as_str),
            Some("a10")
        );
        assert_eq!(config.node.region, "eu-west-1");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        // The report never contains the API key.
        assert!(
            !serde_json::to_string(&report)
                .unwrap()
                .contains("o8k_join_secret")
        );
    }

    #[test]
    fn bad_labels_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("orch8.toml");
        assert!(join(&token(), &path, &["novalue".into()], "h").is_err());
        assert!(join(&token(), &path, &["bad key=x".into()], "h").is_err());
        assert!(!path.exists());
    }
}
