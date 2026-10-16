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
    //! Bootstrap probes readiness on a reachable address, fails closed on any
    //! preflight problem before spawning the server, and bounds the wait.
    use super::*;

    fn config_with_addr(addr: &str) -> orch8_types::config::EngineConfig {
        let mut config = orch8_types::config::EngineConfig::default();
        config.api.http_addr = addr.into();
        config
    }

    fn bootstrap_cmd(dir: &Path, server_bin: &Path, timeout_secs: u64) -> BootstrapCmd {
        BootstrapCmd {
            dir: dir.to_path_buf(),
            template: "default".into(),
            server_bin: server_bin.to_path_buf(),
            timeout_secs,
        }
    }

    fn missing_server() -> PathBuf {
        PathBuf::from("definitely-missing-orch8-server")
    }

    async fn run_error(dir: &Path) -> String {
        let error = run(bootstrap_cmd(dir, &missing_server(), 1))
            .await
            .unwrap_err();
        format!("{error:#}")
    }

    /// A loopback port nothing listens on: bind, read the port, release it.
    fn unused_loopback_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    #[test]
    fn readiness_uses_loopback_for_wildcard_listener() {
        assert_eq!(
            readiness_url(&config_with_addr("0.0.0.0:8080")).unwrap(),
            "http://127.0.0.1:8080/health/ready"
        );
    }

    #[test]
    fn readiness_url_probes_loopback_for_wildcards_and_the_exact_address_otherwise() {
        let cases = [
            ("[::]:8080", "http://[::1]:8080/health/ready"),
            ("192.168.1.50:9000", "http://192.168.1.50:9000/health/ready"),
            (
                "[2001:db8::1]:9090",
                "http://[2001:db8::1]:9090/health/ready",
            ),
        ];
        for (addr, expected) in cases {
            assert_eq!(
                readiness_url(&config_with_addr(addr)).unwrap(),
                expected,
                "case {addr}"
            );
        }
    }

    #[test]
    fn readiness_url_rejects_addresses_that_are_not_socket_addresses() {
        for addr in ["127.0.0.1", "localhost:8080"] {
            let error = readiness_url(&config_with_addr(addr)).unwrap_err();
            assert!(
                format!("{error:#}").contains("api.http_addr is not a socket address"),
                "case {addr}: {error:#}"
            );
        }
    }

    #[tokio::test]
    async fn config_without_generated_keys_fails_before_spawning() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("orch8.toml"), "").unwrap();

        let rendered = run_error(dir.path()).await;

        assert!(
            rendered.contains("bootstrap requires generated API and encryption keys"),
            "{rendered}"
        );
    }

    #[tokio::test]
    async fn unparseable_config_fails_naming_the_config_file() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("orch8.toml");
        std::fs::write(&config_path, "[[[not toml").unwrap();

        let rendered = run_error(dir.path()).await;

        assert!(
            rendered.contains(&format!("parse {}", config_path.display())),
            "{rendered}"
        );
    }

    #[tokio::test]
    async fn gateway_role_without_secrets_fails_preflight() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("orch8.toml"),
            "[node]\nrole = \"gateway\"\n",
        )
        .unwrap();

        let rendered = run_error(dir.path()).await;

        assert!(
            rendered.contains("bootstrap preflight failed"),
            "{rendered}"
        );
    }

    #[tokio::test]
    async fn malformed_encryption_key_fails_preflight_naming_the_key() {
        let dir = tempfile::tempdir().unwrap();
        let toml = format!(
            "[engine]\nencryption_key = \"abcd\"\n\n[api]\napi_key = \"{}\"\n",
            "a".repeat(64)
        );
        std::fs::write(dir.path().join("orch8.toml"), toml).unwrap();

        let rendered = run_error(dir.path()).await;

        assert!(
            rendered.contains("bootstrap preflight failed"),
            "{rendered}"
        );
        assert!(rendered.contains("encryption_key"), "{rendered}");
    }

    #[tokio::test]
    async fn valid_scaffold_passes_preflight_and_reaches_the_spawn() {
        let dir = tempfile::tempdir().unwrap();
        super::super::init::run(dir.path().to_str().unwrap(), "default").unwrap();

        let rendered = run_error(dir.path()).await;

        assert!(
            rendered.contains("start definitely-missing-orch8-server"),
            "{rendered}"
        );
    }

    #[tokio::test]
    async fn missing_project_is_scaffolded_before_the_spawn() {
        let base = tempfile::tempdir().unwrap();
        let project = base.path().join("new-project");

        let rendered = run_error(&project).await;

        assert!(project.join("orch8.toml").exists());
        assert!(project.join("sequence.json").exists());
        assert!(
            rendered.contains("start definitely-missing-orch8-server"),
            "{rendered}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn readiness_wait_stops_as_soon_as_the_server_exits() {
        let mut child = Command::new("sh")
            .args(["-c", "exit 3"])
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        // Let the child exit before the first poll.
        child.wait().await.unwrap();
        let url = format!("http://127.0.0.1:{}/health/ready", unused_loopback_port());

        let error = await_readiness(&mut child, &url, Duration::from_secs(60))
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("exited before readiness"),
            "{error:#}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn readiness_wait_times_out_when_the_server_never_becomes_ready() {
        let mut child = Command::new("sleep")
            .arg("30")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let url = format!("http://127.0.0.1:{}/health/ready", unused_loopback_port());

        let error = await_readiness(&mut child, &url, Duration::ZERO)
            .await
            .unwrap_err();
        terminate(&mut child).await;

        assert!(
            error.to_string().contains("readiness timed out after 0s"),
            "{error:#}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn readiness_wait_succeeds_once_the_endpoint_answers_2xx() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/health/ready", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await.unwrap();
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                .await
                .unwrap();
        });
        let mut child = Command::new("sleep")
            .arg("30")
            .kill_on_drop(true)
            .spawn()
            .unwrap();

        let result = await_readiness(&mut child, &url, Duration::from_secs(10)).await;
        terminate(&mut child).await;
        server.await.unwrap();

        result.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn zero_timeout_is_clamped_to_one_second_of_readiness_wait() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        super::super::init::run(dir.path().to_str().unwrap(), "default").unwrap();
        let config_path = dir.path().join("orch8.toml");
        let config = std::fs::read_to_string(&config_path).unwrap().replace(
            "0.0.0.0:8080",
            &format!("127.0.0.1:{}", unused_loopback_port()),
        );
        std::fs::write(&config_path, config).unwrap();
        let server = dir.path().join("fake-server.sh");
        std::fs::write(&server, "#!/bin/sh\nexec sleep 30\n").unwrap();
        std::fs::set_permissions(&server, std::fs::Permissions::from_mode(0o755)).unwrap();

        let error = run(bootstrap_cmd(dir.path(), &server, 0))
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("readiness timed out after 1s"),
            "{error:#}"
        );
    }
}
