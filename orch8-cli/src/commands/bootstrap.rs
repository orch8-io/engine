use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Args;
use reqwest::Client;
use tokio::process::{Child, Command};

#[derive(Debug, Args)]
pub struct BootstrapCmd {
    /// Project directory containing (or receiving) orch8.toml.
    #[arg(default_value = ".")]
    pub dir: PathBuf,
    /// Built-in sequence template used when scaffolding a new directory.
    #[arg(long, default_value = "default")]
    pub template: String,
    /// orch8-server executable or absolute path.
    #[arg(long, default_value = "orch8-server")]
    pub server_bin: PathBuf,
    /// Seconds allowed for migration, startup, and readiness.
    #[arg(long, default_value_t = 30)]
    pub timeout_secs: u64,
}

fn readiness_url(config: &orch8_types::config::EngineConfig) -> Result<String> {
    let address: std::net::SocketAddr = config
        .api
        .http_addr
        .parse()
        .context("api.http_addr is not a socket address")?;
    let host = if address.ip().is_unspecified() {
        if address.is_ipv4() {
            "127.0.0.1"
        } else {
            "[::1]"
        }
    } else if address.is_ipv6() {
        return Ok(format!(
            "http://[{}]:{}/health/ready",
            address.ip(),
            address.port()
        ));
    } else {
        return Ok(format!(
            "http://{}:{}/health/ready",
            address.ip(),
            address.port()
        ));
    };
    Ok(format!("http://{host}:{}/health/ready", address.port()))
}

async fn await_readiness(child: &mut Child, url: &str, timeout: Duration) -> Result<()> {
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(2))
        .build()?;
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().context("inspect server process")? {
            anyhow::bail!("orch8-server exited before readiness with {status}");
        }
        if let Ok(response) = client.get(url).send().await
            && response.status().is_success()
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("readiness timed out after {}s at {url}", timeout.as_secs());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn terminate(child: &mut Child) {
    let _ = child.kill().await;
    let _ = child.wait().await;
}

pub async fn run(command: BootstrapCmd) -> Result<()> {
    let config_path = command.dir.join("orch8.toml");
    if !config_path.exists() {
        super::init::run(
            command
                .dir
                .to_str()
                .context("project directory is not valid UTF-8")?,
            &command.template,
        )?;
    }
    let contents = std::fs::read_to_string(&config_path)
        .with_context(|| format!("read {}", config_path.display()))?;
    let config: orch8_types::config::EngineConfig =
        toml::from_str(&contents).with_context(|| format!("parse {}", config_path.display()))?;
    if let Err(errors) = config.validate() {
        anyhow::bail!("bootstrap preflight failed: {}", errors.join("; "));
    }
    if config.api.api_key.is_empty() || config.engine.encryption_key.is_empty() {
        anyhow::bail!("bootstrap requires generated API and encryption keys");
    }
    let ready_url = readiness_url(&config)?;
    let mut child = Command::new(&command.server_bin)
        .arg("--config")
        .arg(Path::new("orch8.toml"))
        .current_dir(&command.dir)
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("start {}", command.server_bin.display()))?;

    if let Err(error) = await_readiness(
        &mut child,
        &ready_url,
        Duration::from_secs(command.timeout_secs.clamp(1, 300)),
    )
    .await
    {
        terminate(&mut child).await;
        return Err(error);
    }
    println!(
        "Orch8 is ready at {ready_url} (pid {}). Migrations and readiness checks passed.",
        child.id().unwrap_or_default()
    );
    println!("Press Ctrl-C to stop.");

    tokio::select! {
        result = child.wait() => {
            let status = result.context("wait for orch8-server")?;
            if !status.success() {
                anyhow::bail!("orch8-server exited with {status}");
            }
        }
        signal = tokio::signal::ctrl_c() => {
            signal.context("install Ctrl-C handler")?;
            terminate(&mut child).await;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_uses_loopback_for_wildcard_listener() {
        let mut config = orch8_types::config::EngineConfig::default();
        config.api.http_addr = "0.0.0.0:8080".into();
        assert_eq!(
            readiness_url(&config).unwrap(),
            "http://127.0.0.1:8080/health/ready"
        );
    }
}

#[cfg(test)]
#[path = "bootstrap_coverage_tests.rs"]
mod bootstrap_coverage_tests;
