# Orch8 Engine — Project Status

> Last updated: 2026-04-16

## Summary

Stages 0 through 4 are **complete**. Stage 5 is **complete** (CLI, versioning, debug, audit, checkpoints, SQLite, externalization, Helm chart, Python SDK, Docker image, binary distribution, `orch8 init`, CI/CD all shipped). Stage 6 is ~85% done (clustering, A/B split, dynamic injection, sub-sequences, sessions, interceptors, queue routing, circuit breaker, SLA timers, encryption at rest, tenant isolation, API rate limiting, cancellation scopes, NATS/file-watch triggers, Node.js SDK, Go SDK all shipped — full dashboard, visual builder remain).

**Codebase**: ~7,500 lines of Rust across 5 crates, ~1,000 lines of JS E2E tests, 20 SQL migrations. 3 SDKs (Node.js, Python, Go), Helm chart.

---

## What Is Done

### Stage 0 — Foundation
- Cargo workspace (5 crates: types, storage, engine, api, server)
- `StorageBackend` async trait with 25+ methods
- PostgreSQL backend via sqlx with connection pooling
- 12 SQL migrations
- TOML + env var configuration
- Docker Compose with Postgres 16
- Health endpoints (`/health/live`, `/health/ready`)

### Stage 1 — Core Scheduling
- Task instance lifecycle with validated state machine
- Tick-based scheduler (configurable interval, `FOR UPDATE SKIP LOCKED`)
- Step execution with memoization to `block_outputs`
- Relative delay scheduling (business days, jitter, timezone-aware)
- Crash recovery (reset stale Running instances on startup)
- Graceful shutdown (CancellationToken, semaphore drain)
- Sequence definitions as recursive JSON `BlockDefinition` enum
- Priority-based claiming (Low/Normal/High/Critical)

### Stage 2 — Rate Limits + API
- REST API via axum (sequences, instances, signals, health, metrics, cron, workers)
- Per-resource rate limiting (atomic check-and-increment, defer on exceed)
- Resource pools with rotation (round-robin, weighted, random)
- Warmup ramp schedules
- Send window scheduling
- gRPC API (protobuf service definitions)
- Signal inbox (pause, resume, cancel, update_context, custom)
- Structured JSON logging via `tracing`
- Node.js Worker SDK (poll, claim, heartbeat, complete/fail)
- OpenAPI/Swagger at `/swagger-ui`
- CORS configuration

### Stage 3 — Orchestration Layer
- Execution tree model (parent-child nodes in `execution_tree` table)
- Block evaluator (recursive tree walker)
- Parallel, Race, TryCatch, Loop, ForEach, Router blocks
- Template interpolation engine (`{{path}}` resolution, expressions, defaults)
- Pluggable handler registry (built-in + user-defined + external workers)
- Human-in-the-loop steps (signal-based polling with timeout + escalation)
- Bidirectional signals
- Live state queries (`GET /instances/{id}`, execution tree inspection)

### Stage 4 — Production Hardening
- Bulk create/pause/resume/cancel/reschedule
- Retry with exponential backoff + jitter + max attempts
- Dead Letter Queue (list, inspect, retry)
- Concurrency control per entity key (position-based)
- Idempotency keys (unique index, returns existing on duplicate)
- Prometheus metrics (12 counters, 3 histograms, 2 gauges)
- Webhook event emitter (reqwest HTTP client, TLS, connection pooling)
- Multi-tenancy (tenant-scoped queries, per-tenant limits)
- Tenant isolation middleware (`X-Tenant-Id` header enforcement, cross-tenant rejection)
- API rate limiting (`ConcurrencyLimitLayer`, configurable RPS)
- Cron-triggered sequences (CRUD API, tick loop)
- Embedded test mode (SQLite in-memory)
- Grafana dashboard template (`docs/grafana-dashboard.json`)

### Stage 5 — Complete
- SQLite backend (file-backed with WAL, same trait implementation)
- CLI tool (clap) — instances, sequences, signals, cron, checkpoints
- Audit log (append-only, migration 016, queryable by instance/tenant)
- Workflow versioning (version field, deprecation API, immutable binding)
- Debug mode (breakpoints via metadata, step-through via signals)
- Output externalization (configurable threshold, reference markers)
- Checkpointing (save/list/get-latest/prune API)
- Helm chart ([orch8-io/helm-charts](https://github.com/orch8-io/helm-charts) — deployment, service, configmap, secret, ingress, serviceaccount)
- Python SDK ([orch8-io/sdk-python](https://github.com/orch8-io/sdk-python) — async httpx + pydantic, 18 typed models, polling worker, 7 tests)
- Dockerfile (multi-stage build, debian:bookworm-slim runtime, HEALTHCHECK, SQLite defaults)
- Release workflow (4-target binary builds, Docker to ghcr.io, GitHub Releases with checksums)
- `orch8 init` scaffolding (orch8.toml, sequence.json, docker-compose.yml)
- CI: e2e test job, check + clippy + fmt, unit tests with Postgres
- SDK CI/publish: npm (@orch8/sdk), PyPI (orch8-sdk), Go module (v0.1.0 tagged)

### Stage 6 — Partial
- Multi-node clustering (SKIP LOCKED, DB-based node registration, heartbeat)
- A/B split (deterministic hash-based variant selection)
- Cancellation scopes (subtree-level non-cancellability, `CancellationScope` block type)
- Hot migration API (rebind running instance to new version)
- Dynamic step injection (position-based insert, evaluator merges injected blocks)
- Self-modify handler (`self_modify` — agent steps inject blocks into own instance)
- gRPC sidecar plugin system (`grpc://host:port/Service.Method` handler dispatch)
- Event-driven triggers (inbound webhook CRUD + fire API, DB-persisted definitions)
- NATS message queue trigger (`async-nats` subscription, config-driven, auto-creates instances)
- File watch trigger (`notify` crate, create/modify events, configurable path + recursive mode)
- Trigger processor loop (background sync of trigger definitions, dynamic listener management)
- Sub-sequences (SubSequence block type, parent-child instances)
- Session management (CRUD API, lifecycle, cross-instance references)
- Workflow interceptors (lifecycle hooks)
- Task queue routing (queue_name field, queue-specific polling)
- Circuit breaker (per-handler state machine, REST API)
- SLA timers / deadlines (per-step enforcement, escalation, fast-path coverage)
- Encryption at rest (`EncryptingStorage` decorator, AES-256-GCM, config + env var wiring)
- Worker dashboard (Vite + React SPA, overview + task inspector)
- Optional API key auth middleware
- Node.js SDK ([orch8-io/sdk-node](https://github.com/orch8-io/sdk-node) — full management client, 16 typed interfaces, polling worker, 12 tests)
- Go SDK ([orch8-io/sdk-go](https://github.com/orch8-io/sdk-go) — net/http, zero external deps, context on all methods, polling worker, 5 tests)
- Rust unit tests — 47 tests across `orch8-types` (trigger, context, sequence, execution, serde) and `orch8-engine` (gRPC/WASM handler edge cases)

---

## Architecture

```
orch8-server          Binary entry point
    |
orch8-api             REST routes (axum)
    |
orch8-engine          Scheduler, evaluator, handlers, signals, recovery
    |
orch8-storage         StorageBackend trait + Postgres + SQLite
    |
orch8-types           Domain types, config, errors
```

**Key design decisions:**
- Postgres is the timer wheel (zero engine memory for scheduled instances)
- `SKIP LOCKED` prevents double-claiming across nodes
- State machine enforced at type level
- Semaphore-bounded concurrency for step execution

---

## E2E Test Coverage

| Test File | Tests | Coverage |
|---|---|---|
| `lifecycle.test.js` | 7 | Single/multi-step, sleep, outputs, batch create, list+filter |
| `signals.test.js` | 3 | Cancel, pause/resume, reject on terminal |
| `dlq.test.js` | 3 | List failed, retry from DLQ, reject retry on non-failed |
| `idempotency.test.js` | 3 | Deduplicate by key, different keys, no key |
| `concurrency.test.js` | 2 | Limit by key, no limit without key |
| `cron.test.js` | 6 | CRUD, validation, list by tenant, trigger |
| `worker-dashboard.test.js` | 5 | Dashboard API endpoints |

---

## What Is Next

- **Product features:** [ROADMAP.md](ROADMAP.md)
- **SDKs, Docker, CI, distribution:** [TOOLING.md](TOOLING.md)
- **Pre-release checklist:** [RELEASE_CHECKLIST.md](RELEASE_CHECKLIST.md)
- **Known issues:** [RUST_ISSUES.md](RUST_ISSUES.md)

---

## How to Run

```bash
# Start Postgres
docker compose up -d

# Build and run
cargo run

# Run E2E tests
for f in tests/e2e/*.test.js; do node --test "$f"; done

# Rust unit tests
cargo test --workspace

# Clippy
cargo clippy --workspace -- -D warnings
```

Server runs on port 18080 by default. Postgres on port 5434 (mapped from container 5432).
