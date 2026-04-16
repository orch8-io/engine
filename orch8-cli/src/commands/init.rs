use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

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

fn write_scaffolds(base: &Path) -> Result<()> {
    write_if_absent(
        &base.join("orch8.toml"),
        r#"# Orch8.io Engine Configuration
# Docs: https://github.com/orch8-io/engine

[database]
backend = "sqlite"                         # "sqlite" or "postgres"
url = "sqlite://orch8.db"                  # connection string
# url = "postgres://orch8:orch8@localhost:5432/orch8"
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
cors_origins = "*"
# api_key = ""                             # set to enable auth
require_tenant_header = false
rate_limit_rps = 0                         # 0 = unlimited

[logging]
level = "info"                             # trace, debug, info, warn, error
format = "json"                            # "json" or "pretty"
"#,
    )?;

    write_if_absent(
        &base.join("sequence.json"),
        r#"{
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
"#,
    )?;

    write_if_absent(
        &base.join("docker-compose.yml"),
        r#"services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: orch8
      POSTGRES_USER: orch8
      POSTGRES_PASSWORD: orch8
    ports:
      - "5432:5432"
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
"#,
    )
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
