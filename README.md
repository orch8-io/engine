# Orch8 Engine

A self-hosted, durable workflow orchestration engine built in Rust. Define workflows as composable JSON sequences. Orch8 guarantees every step either completes, retries, or surfaces in a dead-letter queue.

Runs on servers and mobile devices. Single binary for servers, native SDK for iOS and Android (via UniFFI). One dependency: PostgreSQL (or SQLite for dev/embedded/mobile).

[Docs](https://orch8.io/docs) · [Discord](https://discord.gg/BAbx7Dshu) · [Cloud](https://cloud.orch8.io) · [Playbook](https://orch8.io/playbook)

![Orch8 demo — orch8 init → orch8-server → engine ready in seconds](docs/demo.gif)

## Why Orch8

Existing durable workflow engines either ship a multi-service cluster (Temporal: Cassandra + Elasticsearch + JVM workers) or assume Python everywhere (Airflow: Celery + Redis + scheduler). Both are full-time operational jobs on a small team.

Orch8 keeps the execution model — state-snapshot durability, retries, replay-on-restart — but trades the ecosystem for one Rust binary and Postgres. Workers in any language via REST long-poll. Higher-level building blocks (Parallel, Race, TryCatch, CancellationScope, plus LLM/HumanReview/ToolCall) shipped as first-class instead of patterns you build on activities.

Unlike every other workflow engine, Orch8 also runs natively on mobile devices (iOS and Android) via Rust + UniFFI. Workflows execute offline-first on-device, sync status to the server when connected, and support human-in-the-loop approvals via push notifications. No other orchestration engine can do this.

Portable Continuity takes that further: a running execution can hand off between server and device — or between two servers, or across a federation boundary — mid-flight, with cryptographic ownership transfer, tamper-evident provenance, and at-most-once effect tracking that survives the move. Nothing else in this space lets a workflow physically relocate while it's running.

## Features

**Durable execution** — Snapshot-based crash recovery, retry with exponential
backoff and conditional failure policies, idempotency keys, persistent circuit
breakers, dead-letter fingerprinting, automatic incident reproduction, and
checkpoint-based fork/resume. Side-effecting steps use a universal effect
ledger so recovery can distinguish uncommitted, committed, unknown, and
compensated effects.

**Workflow language** — Step, Parallel, Race, TryCatch, Loop, ForEach, Router,
SubSequence, CancellationScope, AB Split, and Saga; per-step `when` guards;
JSON Schema input/output contracts; dynamic block injection; and bounded
concurrent execution of independent `Parallel` branches.

**Scheduling and dispatch** — Relative and cron schedules, business calendars,
timezones, jitter, send windows, per-entity concurrency keys, weighted resource
pools, sliding-window limits, daily caps, warmup ramps, four priority levels,
and cooperative preemption at durable step boundaries.

**Workers and extensions** — Lease-based REST workers with resumable heartbeat
checkpoints, queue/version/capability routing, a [negotiated bidirectional gRPC
session](docs/GRPC_WORKER_STREAM.md), gRPC sidecars, WASM plugins, MCP client and
server modes, Activepieces, signed webhooks, and deduplicated events. Worker,
runtime, artifact-transfer, telemetry, and control sessions are bounded and
resumable.

**Data, artifacts, and streams** — Durable local or S3-compatible encrypted
artifacts, automatic externalization of oversized state, instance and
tenant-namespaced semantic memory, a resumable tenant change feed, and bounded
tumbling/sliding/session windows over durable continuity frames.

**Multi-tenancy and governance** — Capability-scoped tenant principals,
provider-neutral plan entitlements, tenant rate/concurrency limits,
tenant-isolated breakers and memory, authoritative [tenant partition
routing](docs/TENANT_PARTITION_ROUTING.md), and fail-closed residency,
disclosure, delegation, and federation policy.

**Security** — Secure-by-default API-key and tenant enforcement, AES-256-GCM
encryption for context, credentials, artifacts, worker checkpoints, and
protected mobile fields; OAuth2 credential refresh; mTLS workload identity;
HMAC-signed webhooks; signed packages/capsules/provenance; nonce and federation
replay boundaries; CORS controls; and outbound URL/SSRF validation.

**AI and human workflows** — Multi-provider `llm_call` with structured-output
repair, multimodal artifacts, cost/token telemetry, effect-safe provider
failover, durable ReAct agents, governed shared knowledge, bounded cumulative
budgets, evidence-scoped evaluation gates, and lease-safe human attention.

**Release and distribution safety** — Sequence preflight, typed-dataflow
compilation, semantic diff, historical effect-free replay, guarded canaries,
automatic rollback gates, workflow contracts, signed packages, append-only
registry history, runtime-targeted channels, attestations, dependency locks,
and verified delta fallback. See [Safe Releases](docs/RELEASES.md), [Package
Registry](docs/PACKAGE_REGISTRY.md), and [Governed Distribution](docs/DISTRIBUTION_GOVERNANCE.md).

**Mobile, edge, and portable continuity** — Native iOS/Android execution via
Rust + UniFFI, offline-first sync, protected device tools, durable APNs/FCM
wake delivery, capability-aware placement, signed capsule handoff, ownership
epochs, provenance, live migration/rollback, receipt-backed compensation,
what-if simulation, sovereign-edge enforcement, and signed federation
send/receive primitives. See [Mobile SDK](docs/MOBILE_SDK.md), [Continuity
Operations](docs/CONTINUITY_OPERATIONS.md), and [Continuity Debugging](docs/CONTINUITY_DEBUGGING.md).

**Operations and observability** — Role-specific all-in-one/control/executor/
gateway/edge nodes, secure verified bootstrap, aggregate startup preflight,
auditable draining, outbound managed-control tunnels, redacted support bundles,
Prometheus metrics, OTLP/JSON telemetry, audit and provenance logs, execution
workbench, ranked diagnosis, previewable remediation, and the operator
dashboard. See [Node Roles](docs/NODE_ROLES.md), [Secure Bootstrap](docs/SECURE_BOOTSTRAP.md),
and [Support Bundle](docs/SUPPORT_BUNDLE.md).

The [documentation index](docs/README.md) maps each capability to its guide.
For the exact release-by-release inventory, including migrations and explicit
non-guarantees, see the [changelog](CHANGELOG.md#unreleased).

## Install

```bash
# Binary release (downloads from GitHub releases)
curl -fsSL https://raw.githubusercontent.com/orch8-io/engine/main/install.sh | sh

# Homebrew
brew tap orch8-io/orch8 && brew install orch8-server
```

The container image is `ghcr.io/orch8-io/engine:latest`, but a secure container
also needs storage, API-key, and encryption configuration. Use the
[Docker deployment example](docs/DEPLOYMENT.md#docker) instead of starting the
image with only a port mapping.

## Quick Start

Run one local instance without starting a server:

```bash
orch8 init my-project
orch8 dev my-project --mock 'greet_user={"greeting":"hello"}' \
  --skip-timers --once
```

Then follow the [progressive quick starts](docs/quick-starts/README.md) to add
dataflow, the durable API server, external workers, failure recovery, and safe
production releases.

To exercise portable continuity without a server or physical device, run the
real signed/encrypted cloud-to-device-to-cloud protocol against three isolated
local runtimes:

```bash
orch8 demo portable-agent
# Add --output json for machine-readable invariant evidence.
```

The demonstration rejects an untrusted capsule, verifies idempotent
redelivery, advances ownership epochs `0 -> 1 -> 2`, and returns only a digest
of the simulated device-private input.

In CI, collapse the candidate preflight, semantic diff, and historical replay
proofs into one strict exit code:

```bash
orch8 release gate <release-id> --sample 50
```

The gate rejects failed/unknown preflight checks, incompatible or
side-effect-risking diffs, replay divergences, and inconclusive replays by
default. Each risk allowance must be opted into explicitly.

## SDKs

All SDKs live in their own repositories under the [orch8-io](https://github.com/orch8-io) GitHub organization.

| Language | Package | Install | Repo |
|----------|---------|---------|------|
| TypeScript | `@orch8.io/sdk` | `npm install @orch8.io/sdk` | [sdk-node](https://github.com/orch8-io/sdk-node) |
| Expo / React Native | `@orch8.io/expo` | `npx expo install @orch8.io/expo` | [sdk-expo](https://github.com/orch8-io/sdk-expo) |
| Python | `orch8-io-sdk` | `pip install orch8-io-sdk` | [sdk-python](https://github.com/orch8-io/sdk-python) |
| Go | `github.com/orch8-io/sdk-go` | `go get github.com/orch8-io/sdk-go` | [sdk-go](https://github.com/orch8-io/sdk-go) |

The TypeScript SDK includes both workflow authoring (sequence builder, deploy via REST) and worker support (task polling, handler registration, concurrent execution).

The Expo SDK provides a REST client, React hooks, and a native engine bridge for running workflows on-device with offline-first execution and push-notification-based approvals.

### Mobile SDK

The engine compiles to native iOS and Android libraries via [UniFFI](https://mozilla.github.io/uniffi-rs/). Workflows execute locally on-device — offline-first, battery-aware. The server acts as a mailbox: stores status updates, queues commands, dispatches silent push notifications.

See [Mobile SDK](docs/MOBILE_SDK.md) for the full design and [mobile-examples](https://github.com/orch8-io/mobile-examples) for complete iOS and Android sample apps.

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
orch8-mobile          UniFFI bindings for iOS/Android, offline-first engine
    |
orch8-push            APNs/FCM push notification providers
    |
orch8-publisher       Signed sequence/package publication and CDN registry
    |
orch8-cli             CLI tool (init, doctor, release gate, sequence, instance, signal, health)
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

Configuration comes from `orch8.toml`, `ORCH8_*` environment variables, or
both; environment variables win. The server fails closed without an API key
and encryption key unless the corresponding insecure flags are explicit.

Use the [Configuration Reference](docs/CONFIGURATION.md) as the single source
for field names, defaults, environment overrides, and complete examples.

## API Surface

The generated OpenAPI document and Swagger UI are served by the running binary
at `/api-docs/openapi.json` and `/swagger-ui`. Canonical product routes use the
`/api/v1` prefix; bare paths remain compatibility aliases. The surface covers:

- **Sequences** — CRUD, versioning, deprecation, migration, by-name lookup, preflight readiness, template inspection, dataflow bindings
- **Instances** — create, batch create, list/filter, state transitions, context update, retry, DLQ, diagnosis, previewable authorized remediation, workbench (timeline/compare/fork-preview)
- **Releases** — semantic diff, historical validation, canary routing, gate evaluation, promote/pause/rollback
- **Signals** — send pause/resume/cancel/context signals to running instances
- **Events** — ingest for `wait_for_event` correlation, producer-id deduplication
- **Workers** — task polling, completion, failure, heartbeat, stats, queue-based routing
- **Cron** — CRUD with expression validation, enable/disable, trigger history
- **Triggers** — webhook (HMAC-signed) and event-driven instance creation
- **Sessions** — stateful multi-instance coordination
- **Pools** — resource pool management with weighted allocation
- **Credentials** — encrypted credential vault with OAuth2 refresh
- **Circuit Breakers** — per-tenant/handler state, manual reset
- **Plugins** — WASM and gRPC plugin registration
- **Approvals** — human-in-the-loop approval inbox
- **Debug** — raw template resolution with provenance trace
- **Mobile** — device registration, sync, approvals, commands
- **Cluster** — node listing, heartbeat, drain
- **Continuity** — execution/checkpoint lifecycle, handoffs, capsule export/import, provenance, invariants, evaluations, budgets, migrations, what-if simulation, fault lab, DLQ reproduction, provider routing, attention leases, residency/federation
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

The repository has two primary test layers. Exact counts change frequently, so
the checked-in test tree and CI results are the source of truth.

| Layer | Scope |
|-------|-------|
| **Rust unit + integration** | Storage backends (Postgres + SQLite), evaluator, scheduler, handlers, config parsing, state machine transitions, gRPC auth, API error mapping, encryption, mobile sync, expressions, circuit breakers, crash recovery, continuity/provenance/effect-receipt races, dataflow compiler soundness |
| **TypeScript E2E** | Live HTTP API coverage for sequences, instances, workers, cron, triggers, webhooks, approvals, sessions, plugins, credentials, pools, cluster, SSE streaming, mobile sync, portable continuity, and typed dataflow |

**Coverage by feature area:**

| Area | Test suites |
|------|-------------|
| Blocks | Step, Parallel, Race, TryCatch, Loop, ForEach, Router, SubSequence, CancellationScope, AB Split, Saga |
| Conditional execution | `when` step guards, `retry_if` / `non_retryable_codes` conditional retry |
| Handlers | Built-in handlers, external worker dispatch, LLM call, query-instance |
| Features | Rate limiting, resource pools, circuit breakers, encryption, credentials, multi-tenancy |
| Signals | Pause/resume/cancel, context update, terminal guards |
| Scheduling | Cron CRUD, business days, timezone/DST, jitter, send windows, SLA timers |
| Mixing | Complex sequences combining multiple block types and features |
| Resilience | Crash recovery, retry, DLQ fingerprinting + auto-reproduction, idempotency, checkpoint/restore |
| Release safety | Semantic diff, historical validation, canary gates, workflow contracts, preflight |
| Security | API key auth, CORS, encryption at rest, tenant isolation, webhook HMAC + replay protection |
| Templating | Context expressions, dynamic params, conditional logic, dataflow type-checking |
| Observability | Prometheus metrics, audit log, health endpoints, SSE streaming, workbench |
| Portable Continuity | Ownership handoff/CAS races, capsule encryption + signing, provenance chain tamper detection, effect-receipt at-most-once, compensation-run saga rollback, invariant evaluation, live migration + rollback, what-if simulation, fault-lab scenario shrinking + incident reproduction, federation/residency/disclosure minimization, human attention leases, device delegation, full-lifecycle integration scenarios, systematic tenant-isolation sweep |

## Project Structure

```
engine/
  orch8-api/          REST API layer (axum + utoipa)
  orch8-cli/          CLI binary
  orch8-engine/       Core scheduler, evaluator, handlers
  orch8-grpc/         gRPC service (tonic + protobuf)
  orch8-mobile/       UniFFI bindings for iOS/Android
  orch8-publisher/    Signed sequence/package publication and CDN registry
  orch8-push/         Push notification providers (APNs/FCM)
  orch8-server/       Server binary, config, startup
  orch8-storage/      Storage trait + Postgres + SQLite impls
  orch8-types/        Shared domain types and config
  proto/              Protobuf service definitions
  migrations/         Ordered PostgreSQL schema migrations
  tests/e2e/          TypeScript end-to-end API tests
  loadgen/            Load generator with per-template metrics
  activepieces/       Activepieces sidecar integration
  dashboard/          React admin dashboard
  examples/           Example workflows (email classifier, iOS, Android)
  scripts/            Dev scripts (dev-up, dev-down)
  docs/               Documentation
```

## Documentation

- [Documentation index](docs/README.md) — learning, operating, reference, and architecture paths
- [Progressive Quick Starts](docs/quick-starts/README.md) — from a local workflow to guarded production releases
- [Sequences](docs/SEQUENCES.md) — build, publish, trigger, and extend sequences (the workflow format)
- [API Reference](docs/API.md) — REST endpoints, block types, error codes
- [Architecture](docs/ARCHITECTURE.md) — execution model, schema, performance
- [Configuration](docs/CONFIGURATION.md) — all config options and env vars
- [Deployment](docs/DEPLOYMENT.md) — production deploys (Docker, Kubernetes, managed cloud)
- [External Workers](docs/WORKERS.md) — writing handlers in any language
- [Applications](docs/APPLICATIONS.md) — embedding patterns and use cases
- [Webhooks](docs/WEBHOOKS.md) — event schema and delivery semantics
- [Externalized State](docs/EXTERNALIZATION.md) — how oversized payloads are offloaded
- [Mobile SDK](docs/MOBILE_SDK.md) — UniFFI bindings, iOS/Android setup, offline-first execution
- [Continuity Operations](docs/CONTINUITY_OPERATIONS.md) — portable execution handoff, capsules, migrations, upgrade/recovery guidance
- [Continuity Debugging](docs/CONTINUITY_DEBUGGING.md) — checkpoint time-travel, what-if simulation, production-to-test extraction
- [Typed Dataflow](docs/TYPED_DATAFLOW.md) — the `data.*`/`outputs.*` reference compiler and generated SDK bindings
- [Safe Releases](docs/RELEASES.md) — semantic diff, historical replay, guarded canary, promotion, and rollback
- [Operator Dashboard](docs/DASHBOARD.md) — connection, navigation, current surfaces, and verification
- [Agent Patterns](docs/agent-patterns/README.md) — example sequences for AI agents
- [Changelog](CHANGELOG.md)

## Deployment

### Docker

```bash
docker run -d \
  -p 8080:8080 \
  -e ORCH8_DATABASE_URL=postgres://user:pass@host:5432/orch8 \
  -e ORCH8_API_KEY=replace-with-a-long-random-secret \
  -e ORCH8_ENCRYPTION_KEY=replace-with-64-hex-characters \
  -e ORCH8_REQUIRE_TENANT_HEADER=true \
  ghcr.io/orch8-io/engine:latest
```

Mount configuration and secrets instead of putting them directly in shell
history in production. See the [Docker deployment guide](docs/DEPLOYMENT.md#docker)
and [secure bootstrap](docs/SECURE_BOOTSTRAP.md).

### Helm

```bash
helm repo add orch8 https://orch8-io.github.io/helm-charts
helm install orch8 orch8/orch8-engine
```

Chart repo: [orch8-io/helm-charts](https://github.com/orch8-io/helm-charts)

## Community

- [Discord](https://discord.gg/BAbx7Dshu) — questions, patterns, show & tell
- [GitHub Issues](https://github.com/orch8-io/engine/issues) — bug reports and feature requests
- [Playbook](https://orch8.io/playbook) — 22 workflow patterns with full JSON definitions

## Status & Limitations

Pre-1.0. This is the public release of an engine that has been running in
production for several months, with extensive automated coverage of core paths.
Honest about what it is not yet:

- **Not battle-tested at Temporal-scale.** Largest internal load test: ~10K concurrent instances. If you're past that or have multiple engineers depending on uptime, run Temporal until 1.0.
- **No deterministic replay debugger.** Temporal's SDKs ship deterministic replay; we don't yet, though continuity checkpoints support bounded time-travel and effect-free what-if simulation from any boundary (see [Continuity Debugging](docs/CONTINUITY_DEBUGGING.md)). Time-skipping tests *are* supported: inject a `ManualClock` via `SchedulerConfig::clock` and advance virtual time manually — a workflow with a 3-day delay completes in a millisecond-scale test.
- **Workflow versioning is younger.** Sequence definitions are versioned, but the migration ergonomics for in-flight instances aren't as polished as Temporal's `GetVersion` / patch system.
- **SDK depth varies.** TypeScript SDK has both authoring + worker support; Go and Python SDKs are worker-focused for now.
- **API is stable but evolving.** Pre-1.0 means breaking changes are possible; we'll mark them in releases and keep them minimal.

If any of these are dealbreakers, file an issue — the gap-to-feature roadmap is driven by what users hit first.

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
