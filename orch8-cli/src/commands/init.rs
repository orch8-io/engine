use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use rand::RngCore;

pub fn run(dir: &str) -> Result<()> {
    let base = Path::new(dir);
    if !base.exists() {
        fs::create_dir_all(base).context("failed to create directory")?;
    }

    write_scaffolds(base)?;

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
url = "sqlite://orch8.db"                  # connection string
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

const SEQUENCE_JSON_TEMPLATE: &str = r#"{
  "tenant_id": "demo",
  "namespace": "default",
  "name": "hello-world",
  "version": 1,
  "blocks": [
    {
      "type": "Step",
      "id": "greet",
      "handler": "greet_user",
      "params": { "message": "Hello from Orch8!" }
    },
    {
      "type": "Step",
      "id": "wait",
      "handler": "noop",
      "delay": "5s"
    },
    {
      "type": "Step",
      "id": "complete",
      "handler": "noop",
      "params": { "message": "Workflow complete." }
    }
  ]
}
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

fn write_scaffolds(base: &Path) -> Result<()> {
    let api_key = generate_api_key();
    let orch8_toml = ORCH8_TOML_TEMPLATE.replace("{api_key}", &api_key);
    write_if_absent(&base.join("orch8.toml"), &orch8_toml)?;
    write_if_absent(&base.join("sequence.json"), SEQUENCE_JSON_TEMPLATE)?;
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
