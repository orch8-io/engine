# Orch8 Engine

A self-hosted, durable task orchestration engine built in Rust. Define workflows as composable JSON sequences. Orch8 guarantees every step either completes, retries, or surfaces in a dead-letter queue.

Single binary. One dependency: PostgreSQL (or SQLite for dev).

## Features

**Workflow Primitives** -- Step, Parallel, Race, TryCatch, Loop, ForEach, Router, SubSequence, CancellationScope

**Scheduling** -- Relative delays, business days, timezone-per-instance, jitter, send windows, cron triggers

**Rate Limiting** -- Per-resource sliding window with deferred scheduling (not rejection), resource pools with rotation and warmup ramps

**Reliability** -- Crash recovery via state snapshots, configurable retry with backoff, dead letter queue, idempotency keys, circuit breaker

**Concurrency** -- Per-entity key control, priority queues (Low/Normal/High/Critical), bulk create/pause/resume/cancel

**Multi-tenancy** -- Tenant-scoped queries, per-tenant rate limits, tenant isolation middleware

**Extensibility** -- External workers (any language via REST polling), gRPC sidecar plugins, WASM plugins, webhook events, workflow interceptors

**Observability** -- Prometheus metrics, structured JSON logging, audit log, Grafana dashboard template, debug mode with breakpoints

**AI Agent Support** -- Dynamic step injection, self-modify handler, human-in-the-loop with timeout/escalation, SSE streaming

## Quick Start

### With SQLite (zero dependencies)

```bash
# Download the binary (or build from source)
cargo build --release

# Initialize a project
./target/release/orch8 init my-project
cd my-project

# Start the engine
../target/release/orch8-server --config orch8.toml
```

### With Docker

```bash
orch8 init my-project
cd my-project
docker compose up -d
```

### Create a Sequence

```bash
curl -X POST http://localhost:8080/sequences \
  -H 'Content-Type: application/json' \
  -d @sequence.json
```

### Create an Instance

```bash
curl -X POST http://localhost:8080/instances \
  -H 'Content-Type: application/json' \
  -d '{
    "sequence_id": "<id-from-above>",
    "tenant_id": "demo",
    "context": { "data": { "user": "alice" } }
  }'
```

## SDKs

| Language | Package | Repo |
|----------|---------|------|
| Node.js / TypeScript | `@orch8/sdk` | [orch8-io/sdk-node](https://github.com/orch8-io/sdk-node) |
| Python | `orch8-sdk` | [orch8-io/sdk-python](https://github.com/orch8-io/sdk-python) |
| Go | `github.com/orch8-io/sdk-go` | [orch8-io/sdk-go](https://github.com/orch8-io/sdk-go) |

## Architecture

```
orch8-server          Binary entry point
    |
orch8-api             REST + gRPC routes (axum, tonic)
    |
orch8-engine          Scheduler, evaluator, handlers, signals, recovery
    |
orch8-storage         StorageBackend trait + Postgres + SQLite
    |
orch8-types           Domain types, config, errors
```

**Key design decisions:**
- Postgres is the timer wheel (zero engine memory for scheduled instances)
- `FOR UPDATE SKIP LOCKED` prevents double-claiming across nodes
- State machine enforced at type level
- Snapshot-based resume -- no history replay, no determinism constraints
- Semaphore-bounded concurrency for step execution

## Configuration

Configuration via `orch8.toml`, environment variables (`ORCH8_*`), or both (env vars override file).

| Variable | Default | Description |
|----------|---------|-------------|
| `ORCH8_DATABASE_BACKEND` | `sqlite` | `sqlite` or `postgres` |
| `ORCH8_DATABASE_URL` | `sqlite://orch8.db` | Connection string |
| `ORCH8_HTTP_ADDR` | `0.0.0.0:8080` | HTTP API listen address |
| `ORCH8_GRPC_ADDR` | `0.0.0.0:50051` | gRPC API listen address |
| `ORCH8_TICK_INTERVAL_MS` | `100` | Scheduler tick interval |
| `ORCH8_MAX_INSTANCES_PER_TICK` | `256` | Max instances claimed per tick |
| `ORCH8_MAX_CONCURRENT_STEPS` | `128` | Semaphore limit for step execution |
| `ORCH8_ENCRYPTION_KEY` | -- | 64 hex chars for AES-256-GCM encryption at rest |
| `ORCH8_LOG_LEVEL` | `info` | trace, debug, info, warn, error |
| `ORCH8_CORS_ORIGINS` | `*` | CORS allowed origins |
| `ORCH8_API_KEY` | -- | Set to enable API key auth |
| `ORCH8_RATE_LIMIT_RPS` | `0` | API rate limit (0 = unlimited) |

## Development

```bash
# Start Postgres (for tests)
docker compose up -d

# Build
cargo build --workspace

# Test
cargo test --workspace

# Lint
cargo clippy --workspace -- -D warnings

# Format
cargo fmt --check

# E2E tests
for f in tests/e2e/*.test.js; do node --test "$f"; done
```

## Documentation

- [API Reference](docs/API.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Project Status](docs/STATUS.md)
- [Release Checklist](docs/RELEASE_CHECKLIST.md)
- [Tooling & Distribution](docs/TOOLING.md)
- [Product Overview](docs/PRODUCT.md)
- [Roadmap](docs/ROADMAP.md)

## Deployment

### Helm

```bash
helm repo add orch8 https://orch8-io.github.io/helm-charts
helm install orch8 orch8/orch8-engine
```

Chart repo: [orch8-io/helm-charts](https://github.com/orch8-io/helm-charts)

## License

BUSL-1.1
