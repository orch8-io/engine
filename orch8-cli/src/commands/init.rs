use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use rand::RngCore;

use crate::templates;

pub fn run(dir: &str, template: &str) -> Result<()> {
    // Resolve the template before touching the filesystem so an unknown
    // name fails cleanly without leaving a half-initialized directory.
    let template = templates::find(template)?;

    let base = Path::new(dir);
    if !base.exists() {
        fs::create_dir_all(base).context("failed to create directory")?;
    }

    write_scaffolds(base, template)?;

    println!("Initialized Orch8 project in {dir}/");
    println!();
    println!("Files created:");
    println!("  orch8.toml          Configuration (SQLite by default)");
    println!("  sequence.json       Example sequence definition");
    println!("  docker-compose.yml  Engine + Postgres stack");
    println!();
    // Check if default port is already in use.
    if let Ok(listener) = std::net::TcpListener::bind("127.0.0.1:8080") {
        drop(listener);
    } else {
        eprintln!("  warning: port 8080 is already in use.");
        eprintln!("  Consider changing api.http_addr in orch8.toml before starting the server.");
        eprintln!();
    }

    println!("Quick start with SQLite (no Docker needed):");
    println!("  orch8-server --config orch8.toml");
    println!();
    println!("Quick start with Postgres:");
    println!("  docker compose up -d");
    println!();
    println!("Then create a sequence:");
    println!("  curl -X POST http://localhost:8080/sequences \\");
    println!("    -H 'Content-Type: application/json' \\");
    println!("    -d @sequence.json");

    Ok(())
}

/// Generate a 32-byte random token rendered as hex so the scaffold ships
/// with a real API key rather than a commented-out placeholder. The old
/// template had `# api_key = ""` with `cors_origins = "*"`, producing a
/// publicly-reachable unauthenticated server on first boot — the
/// default-insecure-by-omission path the review called out.
fn generate_api_key() -> String {
    use std::fmt::Write as _;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().fold(String::with_capacity(64), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

const ORCH8_TOML_TEMPLATE: &str = r#"# Orch8.io Engine Configuration
# Docs: https://github.com/orch8-io/engine

[database]
backend = "sqlite"                         # "sqlite" or "postgres"
url = "sqlite:orch8.db?mode=rwc"                    # connection string (single colon — sqlx URL form)
# url = "postgres://orch8:orch8@localhost:5434/orch8"
run_migrations = true
max_connections = 64

[engine]
tick_interval_ms = 100
max_instances_per_tick = 256
max_concurrent_steps = 128
stale_threshold_secs = 300
# encryption_key = ""                      # 64 hex chars for AES-256-GCM

[api]
http_addr = "0.0.0.0:8080"
grpc_addr = "0.0.0.0:50051"
# CORS: default to same-origin only. Widen to specific origins before
# serving a browser SPA ("https://app.example.com,https://staging.example.com").
# `"*"` disables the origin check entirely — only safe for short-lived
# local development; never ship it to production.
cors_origins = ""
# API key: generated at scaffold time. Rotate before production use and
# store in a secrets manager. The server refuses to boot without a key
# unless `--insecure` is passed explicitly.
api_key = "{api_key}"
require_tenant_header = true
max_concurrent_requests = 0                # 0 = unlimited (in-flight cap, not RPS)

[logging]
level = "info"                             # trace, debug, info, warn, error
json = true                                # true = JSON logs, false = pretty
"#;

const DOCKER_COMPOSE_TEMPLATE: &str = r#"services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: orch8
      POSTGRES_USER: orch8
      POSTGRES_PASSWORD: orch8
    ports:
      - "5434:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U orch8"]
      interval: 5s
      timeout: 5s
      retries: 5

  engine:
    image: ghcr.io/orch8-io/engine:latest
    ports:
      - "8080:8080"
      - "50051:50051"
    environment:
      ORCH8_DATABASE_BACKEND: postgres
      ORCH8_DATABASE_URL: postgres://orch8:orch8@postgres:5432/orch8
      ORCH8_HTTP_ADDR: 0.0.0.0:8080
      ORCH8_GRPC_ADDR: 0.0.0.0:50051
    depends_on:
      postgres:
        condition: service_healthy

volumes:
  pgdata:
"#;

fn write_scaffolds(base: &Path, template: &templates::Template) -> Result<()> {
    let api_key = generate_api_key();
    #[allow(clippy::literal_string_with_formatting_args)]
    let orch8_toml = ORCH8_TOML_TEMPLATE.replace("{api_key}", &api_key);
    write_if_absent(&base.join("orch8.toml"), &orch8_toml)?;
    write_if_absent(&base.join("sequence.json"), template.json)?;
    write_if_absent(&base.join("docker-compose.yml"), DOCKER_COMPOSE_TEMPLATE)
}

fn write_if_absent(path: &Path, content: &str) -> Result<()> {
    if path.exists() {
        println!(
            "  skip {} (already exists)",
            path.file_name().unwrap_or_default().to_string_lossy()
        );
    } else {
        fs::write(path, content).with_context(|| format!("failed to write {}", path.display()))?;
        println!(
            "  create {}",
            path.file_name().unwrap_or_default().to_string_lossy()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_default_writes_all_scaffolds() {
        let dir = tempfile::tempdir().unwrap();
        run(dir.path().to_str().unwrap(), "default").unwrap();

        for file in ["orch8.toml", "sequence.json", "docker-compose.yml"] {
            assert!(dir.path().join(file).exists(), "{file} should be created");
        }
        // Default behavior unchanged: sequence.json is the default template.
        let written = fs::read_to_string(dir.path().join("sequence.json")).unwrap();
        assert_eq!(written, templates::find("default").unwrap().json);
    }

    #[test]
    fn init_with_template_writes_that_template() {
        let dir = tempfile::tempdir().unwrap();
        run(dir.path().to_str().unwrap(), "react-loop").unwrap();

        let written = fs::read_to_string(dir.path().join("sequence.json")).unwrap();
        assert_eq!(written, templates::find("react-loop").unwrap().json);
        // The rest of the scaffold is template-independent.
        assert!(dir.path().join("orch8.toml").exists());
        assert!(dir.path().join("docker-compose.yml").exists());
    }

    #[test]
    fn init_unknown_template_errors_and_creates_nothing() {
        let base = tempfile::tempdir().unwrap();
        let target = base.path().join("project");
        let err = run(target.to_str().unwrap(), "no-such-template").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown template `no-such-template`"), "{msg}");
        assert!(
            msg.contains("default") && msg.contains("react-loop"),
            "{msg}"
        );
        // Failed before touching the filesystem.
        assert!(!target.exists());
    }

    #[test]
    fn init_does_not_overwrite_existing_sequence_json() {
        let dir = tempfile::tempdir().unwrap();
        let sequence = dir.path().join("sequence.json");
        fs::write(&sequence, "{\"custom\": true}").unwrap();

        run(dir.path().to_str().unwrap(), "guardrail-validation").unwrap();

        let written = fs::read_to_string(&sequence).unwrap();
        assert_eq!(written, "{\"custom\": true}");
    }

    #[test]
    fn init_creates_missing_directory() {
        let base = tempfile::tempdir().unwrap();
        let target = base.path().join("nested").join("project");
        run(target.to_str().unwrap(), "default").unwrap();
        assert!(target.join("sequence.json").exists());
    }
}
