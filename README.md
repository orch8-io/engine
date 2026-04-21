# Orch8 Engine

A self-hosted, durable workflow orchestration engine built in Rust. Define workflows as composable JSON sequences. Orch8 guarantees every step either completes, retries, or surfaces in a dead-letter queue.

Single binary. One dependency: PostgreSQL (or SQLite for dev/embedded).

## Features

**Workflow Primitives** — Step, Parallel, Race, TryCatch, Loop, ForEach, Router, SubSequence, CancellationScope, AB Split

**Scheduling** — Relative delays, business-days-only with holiday awareness, timezone-per-instance, jitter, send windows, cron triggers with configurable tick interval

**Rate Limiting** — Per-resource sliding window with deferred scheduling (not rejection), resource pools with weighted rotation, daily caps, and warmup ramps

**Reliability** — Crash recovery via state snapshots, configurable retry with exponential backoff, dead letter queue, idempotency keys, circuit breakers with fallback handler routing and persistent state

**Concurrency** — Per-entity key control, priority queues (Low/Normal/High/Critical), bulk create/pause/resume/cancel, batch instance creation (up to 10k)

**Multi-tenancy** — Tenant-scoped queries, per-tenant rate limits, per-tenant circuit breakers, tenant isolation middleware, per-tenant noisy-neighbor protection

**Extensibility** — External workers (any language via REST polling), gRPC sidecar plugins, WASM plugins, webhook events, workflow interceptors, emit-event with deduplication

**Observability** — Prometheus metrics, structured JSON logging, audit log, execution tree visualization, Grafana dashboard template

**AI Agent Support** — Dynamic step injection (self_modify), LLM call handler (OpenAI/Anthropic/Bedrock), human-in-the-loop with timeout/escalation, SSE streaming, query-instance handler for cross-workflow coordination

**Security** — AES-256-GCM encryption at rest for context and credentials, OAuth2 credential refresh, API key authentication, CORS configuration

## Quick Start

### With SQLite (zero dependencies)

```bash
cargo build --release

# Initialize a project
./target/release/orch8 init my-project
cd my-project

# Start the engine (--insecure skips API key requirement)
../target/release/orch8-server --insecure
```

### With Docker

```bash
docker compose up -d   # starts Postgres + engine
```

### Create a Sequence

```bash
curl -X POST http://localhost:8080/sequences \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "tenant_id": "demo",
    "namespace": "default",
    "name": "hello-world",
    "version": 1,
    "blocks": [
      { "type": "step", "id": "greet", "handler": "noop", "params": {} }
    ]
  }'
```

### Create an Instance

```bash
curl -X POST http://localhost:8080/instances \
  -H 'Content-Type: application/json' \
  -d '{
    "sequence_id": "550e8400-e29b-41d4-a716-446655440000",
    "tenant_id": "demo",
    "namespace": "default",
    "context": { "data": { "user": "alice" } }
  }'
```

## SDKs

| Language | Package | Install |
|----------|---------|---------|
| TypeScript | `@orch8/workflow-sdk` + `@orch8/worker-sdk` | `npm install @orch8/workflow-sdk @orch8/worker-sdk` |
| Python | `orch8-sdk` | `pip install orch8-sdk` |
| Go | `github.com/orch8-io/sdk-go` | `go get github.com/orch8-io/sdk-go` |

In-repo TypeScript SDKs:

| Package | Purpose | Location |
|---------|---------|----------|
| `@orch8/workflow-sdk` | Author sequences in TypeScript, deploy via REST | [`workflow-sdk-node/`](workflow-sdk-node/) |
| `@orch8/worker-sdk` | Build external workers that execute handlers | [`worker-sdk-node/`](worker-sdk-node/) |

## Architecture

```
orch8-server          Binary entry point, config loading, signal handling
    |
orch8-api             REST (axum) + SSE streaming, OpenAPI via utoipa
    |
orch8-grpc            gRPC service (tonic) for high-throughput clients
    |
orch8-engine          Scheduler, evaluator, handlers, signals, cron,
    |                 circuit breakers, recovery, sequence cache
    |
orch8-storage         StorageBackend trait + Postgres + SQLite + encrypting wrapper
    |
orch8-types           Domain types, config, IDs, errors
    |
orch8-cli             CLI tool (init, sequence, instance, signal, health)
```

**Key design decisions:**
- Postgres is the timer wheel (zero engine memory for scheduled instances)
- `FOR UPDATE SKIP LOCKED` prevents double-claiming across nodes
- State machine enforced at type level
- Snapshot-based resume — no history replay, no determinism constraints
- Semaphore-bounded concurrency for step execution
- Circuit breaker state persisted to storage (survives restarts)
- Sequence cache with TTL eviction for hot-path lookups

## Configuration

Configuration via `orch8.toml`, environment variables (`ORCH8_*`), or both (env vars override file).

| Variable | Default | Description |
|----------|---------|-------------|
| `ORCH8_STORAGE_BACKEND` | `sqlite` | `sqlite` or `postgres` |
| `ORCH8_DATABASE_URL` | `sqlite://orch8.db` | Connection string |
| `ORCH8_HTTP_ADDR` | `0.0.0.0:8080` | HTTP API listen address |
| `ORCH8_GRPC_ADDR` | `0.0.0.0:50051` | gRPC API listen address |
| `ORCH8_TICK_INTERVAL_MS` | `100` | Scheduler tick interval (ms) |
| `ORCH8_CRON_TICK_SECS` | `10` | Cron loop check interval (seconds) |
| `ORCH8_BATCH_SIZE` | `256` | Max instances claimed per tick |
| `ORCH8_MAX_CONCURRENT_STEPS` | `128` | Semaphore limit for step execution |
| `ORCH8_MAX_INSTANCES_PER_TENANT` | `0` | Per-tenant claim limit (0 = unlimited) |
| `ORCH8_ENCRYPTION_KEY` | — | 64 hex chars for AES-256-GCM encryption at rest |
| `ORCH8_LOG_LEVEL` | `info` | trace, debug, info, warn, error |
| `ORCH8_LOG_JSON` | `false` | JSON-formatted log output |
| `ORCH8_CORS_ORIGINS` | `*` | CORS allowed origins |
| `ORCH8_API_KEY` | — | Set to require API key auth |

See [Configuration Reference](docs/CONFIGURATION.md) for the full list.

## API Surface

59 documented REST endpoints covering:

- **Sequences** — CRUD, versioning, deprecation, migration, by-name lookup
- **Instances** — create, batch create, list/filter, state transitions, context update, retry, DLQ
- **Signals** — send pause/resume/cancel/context signals to running instances
- **Workers** — task polling, completion, failure, heartbeat, stats, queue-based routing
- **Cron** — CRUD with expression validation, enable/disable, trigger history
- **Triggers** — webhook and event-driven instance creation
- **Sessions** — stateful multi-instance coordination
- **Pools** — resource pool management with weighted allocation
- **Credentials** — encrypted credential vault with OAuth2 refresh
- **Circuit Breakers** — per-tenant/handler state, manual reset
- **Plugins** — WASM and gRPC plugin registration
- **Approvals** — human-in-the-loop approval inbox
- **Cluster** — node listing, heartbeat, drain
- **Health** — liveness, readiness, Prometheus metrics

## Development

```bash
# Start Postgres
docker compose up -d

# Build
cargo build --workspace

# Unit tests
cargo test --workspace

# Lint
cargo clippy --workspace -- -D warnings

# Format
cargo fmt --check

# E2E tests (TypeScript)
cd tests/e2e && npm ci && npm test

# E2E tests (Rust)
cargo test --test '*' --workspace
```

## Test Coverage

**1,663 tests** across three layers:

| Layer | Tests | Scope |
|-------|-------|-------|
| **Rust unit + integration** | 1,251 | 33 test suites — storage backends, evaluator, scheduler, handlers, config parsing, state machine transitions, gRPC auth, full engine integration |
| **TypeScript E2E** | 412 | 94 test files hitting the live HTTP API — sequences, instances, workers, cron, triggers, webhooks, approvals, sessions, plugins, credentials, pools, cluster, SSE streaming |

**Coverage by feature area:**

| Area | Test files |
|------|-----------|
| Sequences | versioning, migration, deprecation, lookup, delete, interceptors |
| Instances | lifecycle, batch create, state transitions, context update, retry, DLQ, priority, tree, filters |
| Workers | polling, completion, failure, heartbeat, stats, dashboard, edge cases |
| Scheduling | cron CRUD, business days, timezone/DST, jitter, send windows, SLA timers |
| Concurrency | rate limiting, resource pools, circuit breakers, bulk ops |
| Signals | pause/resume/cancel, context update, terminal guards |
| Multi-tenancy | tenant isolation CRUD, namespace filtering |
| Approvals | workflow, listing |
| Credentials | CRUD, encryption at rest, OAuth2 refresh, kind filtering |
| Plugins | WASM/gRPC registration, type validation, edge cases |
| Triggers & Webhooks | event triggers, webhook delivery, replay, secret validation |
| Sessions | state updates, lookup, edge cases |
| Observability | Prometheus metrics, audit log, health endpoints, SSE streaming |
| Infrastructure | cluster nodes, checkpoints, API validation, performance/load |

## Project Structure

```
engine/
  migrations/         29 SQL migrations (Postgres schema)
  orch8-api/          REST API layer (axum + utoipa)
  orch8-cli/          CLI binary
  orch8-engine/       Core scheduler, evaluator, handlers
  orch8-grpc/         gRPC service (tonic + protobuf)
  orch8-server/       Server binary, config, startup
  orch8-storage/      Storage trait + Postgres + SQLite impls
  orch8-types/        Shared domain types and config
  tests/e2e/          94 TypeScript E2E test files (~400 test cases)
  loadgen/            Load generator with per-template metrics
  activepieces/       Activepieces sidecar integration
  dashboard/          React admin dashboard
  examples/           Example workflows (email classifier, etc.)
  worker-sdk-node/    TypeScript worker SDK
  workflow-sdk-node/  TypeScript workflow SDK
  docs/               Documentation
```

## Documentation

- [Quick Start](docs/QUICK_START.md) — zero to first completed instance in 5 minutes
- [API Reference](docs/API.md) — REST endpoints, block types, error codes
- [Architecture](docs/ARCHITECTURE.md) — execution model, schema, performance
- [Configuration](docs/CONFIGURATION.md) — all config options and env vars
- [Deployment](docs/DEPLOYMENT.md) — production deploys (Docker, Kubernetes, managed cloud)
- [External Workers](docs/WORKERS.md) — writing handlers in any language
- [Webhooks](docs/WEBHOOKS.md) — event schema and delivery semantics
- [Externalized State](docs/EXTERNALIZATION.md) — how oversized payloads are offloaded
- [Agent Patterns](docs/agent-patterns/README.md) — example sequences for AI agents
- [Changelog](CHANGELOG.md)

## Deployment

### Docker

```bash
docker run -d \
  -p 8080:8080 \
  -e ORCH8_DATABASE_URL=postgres://user:pass@host:5432/orch8 \
  ghcr.io/orch8-io/engine:latest
```

### Helm

```bash
helm repo add orch8 https://orch8-io.github.io/helm-charts
helm install orch8 orch8/orch8-engine
```

Chart repo: [orch8-io/helm-charts](https://github.com/orch8-io/helm-charts)

## License

This project is licensed under the [Business Source License 1.1 (BUSL-1.1)](LICENSE).

**You can:**
- Use Orch8 in production for your own applications
- Modify and extend the source code
- Self-host for your team or company

**You cannot:**
- Offer Orch8 as a hosted or embedded service to third parties competing with us

The license converts to Apache 2.0 four years from publication.

### Managed Cloud

Don't want to self-host? [orch8.io/pricing](https://orch8.io/pricing) — we run it for you.

### Commercial / OEM License

Want to embed Orch8 in your SaaS product or offer it as a managed service? Contact [hello@orch8.io](mailto:hello@orch8.io) for commercial licensing.
