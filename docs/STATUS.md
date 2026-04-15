# Orch8.io Engine — Project Status

> Last updated: 2026-04-15

## Summary

Stages 0 through 4 are **complete**, including cron-triggered sequences. The engine is a working Rust application with a Postgres backend, REST API, step execution with retry/backoff, signals, concurrency control, idempotency, DLQ, cron scheduling, Prometheus metrics, and webhook events. 24 E2E tests cover all major features.

**Codebase**: ~6,500 lines of Rust across 5 crates, ~1,000 lines of JS E2E tests, 11 SQL migrations.

---

## What Is Done

### Stage 0 — Foundation

| Deliverable | Status |
|---|---|
| Cargo workspace (5 crates: types, storage, engine, api, server) | Done |
| `StorageBackend` async trait with 25+ methods | Done |
| PostgreSQL backend via sqlx with connection pooling | Done |
| 10 SQL migrations (sequences, instances, execution tree, outputs, rate limits, signals, indexes, concurrency/idempotency) | Done |
| TOML + env var configuration (`EngineConfig`) | Done |
| Docker Compose with Postgres 16 | Done |
| Health endpoints (`/health/live`, `/health/ready`) | Done |

### Stage 1 — Core Scheduling

| Deliverable | Status |
|---|---|
| Task instance lifecycle (create, state transitions, terminal states) | Done |
| State machine with validated transitions (`can_transition_to`) | Done |
| Tick-based scheduler (configurable interval, batch claiming via `FOR UPDATE SKIP LOCKED`) | Done |
| Step execution with memoization to `block_outputs` | Done |
| Relative delay scheduling (business days, jitter, timezone-aware) | Done |
| Crash recovery (reset stale Running instances to Scheduled) | Done |
| Graceful shutdown (CancellationToken, semaphore drain, configurable grace period) | Done |
| Sequence definitions as JSON (recursive `BlockDefinition` enum) | Done |
| Priority-based instance claiming (Low/Normal/High/Critical) | Done |

### Stage 2 — Rate Limits + API

| Deliverable | Status |
|---|---|
| REST API via axum (sequences, instances, signals, health, metrics) | Done |
| Per-resource rate limiting (atomic check-and-increment, defer on exceed) | Done |
| Rate limit persistence in Postgres | Done |
| Signal inbox (pause, resume, cancel, update_context, custom) | Done |
| Structured JSON logging via `tracing` | Done |
| JS test client (REST wrapper, polling helpers, test utilities) | Done |

**Not done from spec:** gRPC API, Node.js SDK (napi-rs), resource pools with rotation, warmup ramp schedules, send window scheduling. Decision: JS SDK deferred; JS test client used for E2E testing instead.

### Stage 3 — Orchestration Layer

| Deliverable | Status |
|---|---|
| Execution tree model (parent-child nodes in `execution_tree` table) | Done |
| Block evaluator (recursive tree walker with dispatch) | Done |
| Parallel execution (spawn all, wait for all) | Done |
| Race execution (first wins, cancel losers) | Done |
| Try-catch-finally blocks | Done |
| Loop / forEach iteration (with configurable max iterations) | Done |
| Router (N-way branching with conditions + fallback) | Done |
| Template interpolation engine (`{{path}}` resolution from context/outputs) | Done |
| Pluggable handler registry (register custom step handlers) | Done |
| Bidirectional signals (pause/resume/cancel/update_context/custom) | Done |

**Not done from spec:** Human-in-the-loop steps, live state queries endpoint, cancellation scopes.

### Stage 4 — Production Hardening

| Deliverable | Status |
|---|---|
| Bulk create (batch INSERT, 500-row chunks in transaction) | Done |
| Bulk state update by filter | Done |
| Retry with exponential backoff + jitter + max attempts | Done |
| Attempt tracking (derived from previous block_output) | Done |
| Dead Letter Queue (GET /instances/dlq, POST /instances/{id}/retry) | Done |
| Concurrency control per entity key (position-based deterministic scheduling) | Done |
| Idempotency keys (unique index + pre-insert lookup, returns existing on duplicate) | Done |
| Prometheus metrics (steps executed/failed/retried, instances claimed/completed/failed, tick duration, queue depth, rate limits exceeded) | Done |
| Webhook event emitter (instance.completed, instance.failed, configurable URLs, retry with backoff) | Done |
| Built-in step handlers (noop, log, sleep, http_request) | Done |
| Multi-tenancy (tenant-scoped queries, per-tenant filtering) | Done |
| Cron-triggered sequences (CRUD API, tick loop, auto-instance creation) | Done |
| E2E test suite (24 tests across 6 files) | Done |

**Not done from spec:** Embedded test mode (SQLite), Grafana dashboard template, 100K benchmark.

---

## Architecture

```
orch8-server          Binary entry point, wires config/storage/api/engine
    |
orch8-api             REST routes (axum), request/response types
    |
orch8-engine          Scheduler tick loop, evaluator, handlers, signals, recovery
    |
orch8-storage         StorageBackend trait + PostgresStorage implementation
    |
orch8-types           Domain types, IDs, config, errors (zero-dependency)
```

**Key design decisions:**
- Postgres is the timer wheel. Zero engine memory for scheduled instances.
- `SKIP LOCKED` prevents double-claiming across overlapping ticks.
- State machine enforced at type level (`can_transition_to`).
- Semaphore-bounded concurrency for step execution.
- Concurrency control uses position-based selection to avoid livelock.

---

## E2E Test Coverage

| Test File | Tests | What It Covers |
|---|---|---|
| `lifecycle.test.js` | 7 | Single/multi-step completion, sleep, unknown handler failure, outputs, batch create, list with filter |
| `signals.test.js` | 3 | Cancel running instance, pause/resume, reject signal on terminal |
| `dlq.test.js` | 3 | List failed in DLQ, retry from DLQ, reject retry on non-failed |
| `idempotency.test.js` | 3 | Deduplicate by key, different keys create different instances, no key = no dedup |
| `concurrency.test.js` | 2 | Limit parallel executions by key, no limit without key |
| `cron.test.js` | 6 | Create/get, reject invalid, list by tenant, update, delete, trigger creates instance |

---

## What Is Next

### Immediate

1. **Performance benchmark** — Validate 100K instance creation in <3s. Measure tick throughput and step latency under load.

### Stage 5 — Second SDK + Polish

| Priority | Deliverable | Notes |
|---|---|---|
| High | SQLite backend | Same `StorageBackend` trait, enables embedded/dev mode and CI without Postgres |
| High | CLI tool (clap) | List instances, query state, send signals, bulk ops from terminal |
| High | Docker image | Multi-stage build, <50MB, health check, env var config |
| Medium | Audit log | Append-only event journal for state transitions, queryable by instance/tenant |
| Medium | Workflow versioning | New instances on new version, running instances complete on old |
| Medium | Output externalization | Large outputs to `externalized_state`, configurable threshold |
| Low | Helm chart | Kubernetes deployment with Postgres dependency |
| Low | Debug mode | Breakpoints, step-through, state inspection |
| Deferred | Python SDK | TypeScript/JS SDK deferred per project direction; Python also deferred |

### Stage 6 — Enterprise + Scale

| Priority | Deliverable | Notes |
|---|---|---|
| High | Dashboard UI | Instance inspector, queue visualization, embeddable web component |
| High | Multi-node clustering | Multiple workers sharing Postgres (already SKIP LOCKED ready) |
| Medium | gRPC API | Full protobuf service alongside REST |
| Medium | Dynamic step injection | Add steps to running instances at runtime |
| Medium | Sub-sequences | Step invokes another sequence as sub-workflow |
| Low | Visual sequence builder | React drag-and-drop, export as code/YAML |
| Low | A/B split primitive | Route N% to variant A, M% to variant B |
| Low | Circuit breaker | Pause failing step types, recovery after cooldown |
| Later | Go SDK, encryption at rest, SOC 2 prep | Enterprise tier |

### Technical Debt

- **gRPC not implemented** — REST-only for now. gRPC was spec'd for Stage 2 but deprioritized in favor of engine capabilities.
- **Resource pools / warmup ramps / send windows** — Stage 2 campaign primitives not yet built. These are important for the outreach/campaign use case.
- **Composite block execution in scheduler** — The evaluator handles composite blocks, but the scheduler currently logs and skips them. Integration between scheduler and evaluator needed.
- **Template interpolation not wired** — Engine exists (`template.rs`) but not called during step execution.

---

## How to Run

```bash
# Start Postgres
docker compose up -d

# Build and run
cargo run

# Run E2E tests (sequentially)
for f in tests/e2e/*.test.js; do node --test "$f"; done

# Run Rust unit tests
cargo test --workspace

# Clippy
cargo clippy --workspace -- -D warnings
```

Server runs on port 18080 by default. Postgres on port 5434 (mapped from container 5432).
