# Orch8 Engine

A self-hosted, durable workflow engine written in Rust. You define workflows as JSON
sequences, and Orch8 makes sure every step completes, retries, or lands in a dead-letter
queue.

It ships as one binary for servers, backed by PostgreSQL (or SQLite for single-node and
embedded use), plus a native SDK that runs workflows on iOS and Android.

[Docs](https://orch8.io/docs) · [Discord](https://discord.gg/BAbx7Dshu) · [Cloud](https://cloud.orch8.io) · [Playbook](https://orch8.io/playbook) · [All features](docs/FEATURES.md) · [Licensing](docs/LICENSING.md)

![Orch8 demo — orch8 init → orch8-server → engine ready in seconds](docs/demo.gif)

## Pick your path

Each path is about ten lines. [Install](#install) the `orch8` CLI first. All three run
locally without a separate database.

### 1. Background jobs & cron

Durable multi-step jobs with retries, delays, and schedules. No worker fleet is needed to
start.

```bash
orch8 init my-jobs && cd my-jobs           # sequence.json + orch8.toml with generated keys
orch8 dev --no-server --skip-timers --once # run the 3-step job once; delays are fast-forwarded
orch8 dev                                  # local studio + API on http://localhost:8080, hot reload
```

Add `"retry": { "max_attempts": 3, "initial_backoff": 1000, "max_backoff": 60000, "backoff_multiplier": 2.0 }`
to any step, or `"delay": { "duration": 86400000 }` to wait a day durably. Next:
[cron schedules](docs/quick-starts/topics/01-cron-schedules.md),
[inbound webhooks](docs/quick-starts/topics/02-inbound-webhook-trigger.md),
[external workers in any language](docs/WORKERS.md), and
[one-off background jobs](docs/JOBS.md).

### 2. Durable AI agents

LLM calls, tools, and human approval gates. Each step's result is persisted, so a crash
or redeploy resumes the run instead of starting it over.

```bash
mkdir agent && cat > agent/sequence.json <<'EOF'
{ "name": "support-agent", "blocks": [
  { "type": "step", "id": "draft", "handler": "llm_call",
    "params": { "provider": "anthropic", "messages": [{ "role": "user", "content": "Draft a reply to: {{context.data.ticket}}" }] },
    "retry": { "max_attempts": 3, "initial_backoff": 1000, "max_backoff": 30000, "backoff_multiplier": 2.0 } },
  { "type": "step", "id": "approve", "handler": "human_review", "params": { "instructions": "Check the draft" },
    "wait_for_input": { "prompt": "Send it?", "store_as": "decision", "choices": [{ "label": "Send", "value": "send" }, { "label": "Discard", "value": "discard" }] } } ] }
EOF
orch8 dev agent --no-server --dry-run --skip-timers --once --mock llm_call='{"text":"Your refund is on its way."}'
```

`--mock` and `--dry-run` let the run complete offline: the LLM is stubbed and the
approval gate auto-approves. For a real run, drop both flags and export the provider key
(for example `ANTHROPIC_API_KEY`). Next: [agent patterns](docs/agent-patterns/README.md)
(ReAct loop, tool pipelines, guardrails, multi-agent), `orch8 templates list`, and
[human approval](docs/quick-starts/topics/04-human-approval.md).

### 3. Offline mobile workflows

The same engine runs on-device through Rust + UniFFI. Workflows run offline-first, sync
status when the device reconnects, and wake for approvals through push notifications.

```swift
// Package.swift: .package(url: "https://github.com/orch8-io/orch8-mobile-swift", exact: "0.7.1")
import Orch8Mobile

let engine = try MobileEngine(dbPath: dbPath, config: config)   // config: see docs/MOBILE_SDK.md
try engine.registerHandler(name: "show_screen", handler: ShowScreenHandler())
_ = try engine.sync(manifestUrl: "https://api.example.com/mobile/manifest.json", tokenProvider: nil)
engine.resume()                                                   // start the local tick loop
let id = try engine.start(sequenceName: "onboarding_v2",
                          input: "{\"user_id\": \"abc123\"}", dedupKey: "onboarding:abc123")
```

Kotlin, React Native (`npm install react-native-orch8@0.7.1`), and Expo
(`npx expo install @orch8.io/expo`) are also supported. Next: [Mobile SDK](docs/MOBILE_SDK.md)
and [mobile-examples](https://github.com/orch8-io/mobile-examples).

## Install

```bash
# Binary release (downloads from GitHub releases)
curl -fsSL https://raw.githubusercontent.com/orch8-io/engine/main/install.sh | sh

# Homebrew
brew tap orch8-io/orch8 && brew install orch8

# npm or pipx shims (download and verify the same native release)
npm install --global @orch8/cli
pipx install orch8-cli

# Windows PowerShell
irm https://raw.githubusercontent.com/orch8-io/engine/main/install.ps1 | iex
```

The container image is `ghcr.io/orch8-io/engine:latest`. A secure container also needs
storage, API-key, and encryption-key configuration, so start from the
[Docker deployment example](docs/DEPLOYMENT.md#docker) rather than running the image with
only a port mapping. For one-click and PaaS setups (Render, DigitalOcean, Railway, Fly.io,
Coolify), see [Deployment](docs/DEPLOYMENT.md#one-click-and-paas-templates). For Kubernetes,
see the [Helm chart](deploy/helm/orch8/README.md). For single-node SQLite in production,
see [SQLite + Litestream](docs/SQLITE_PRODUCTION.md).

## More from the CLI

Authoring shortcuts:

```bash
orch8 templates list --catalog-url https://cloud.orch8.io/api/catalog/templates
orch8 templates pull react-loop --out sequence.json
orch8 generate "triage support tickets with human escalation"
orch8 pieces search slack
orch8 demo crash-recovery
```

The [progressive quick starts](docs/quick-starts/README.md) take you through dataflow, the
durable API server, external workers, failure recovery, and safe production releases.
Community-contributed templates are covered in [Community templates](docs/COMMUNITY_TEMPLATES.md).

To try portable continuity without a server or a physical device, run the real
signed and encrypted cloud-to-device-to-cloud protocol against three isolated local
runtimes:

```bash
orch8 demo portable-agent
# Add --output json for machine-readable invariant evidence.
```

The demo rejects an untrusted capsule, checks that a repeated delivery is handled
idempotently, advances ownership epochs `0 -> 1 -> 2`, and returns only a digest of the
simulated device-private input.

In CI, one command combines the candidate preflight, semantic diff, and historical replay
checks into a single strict exit code:

```bash
orch8 release gate <release-id> --sample 50
```

By default the gate rejects failed or unknown preflight checks, incompatible or
side-effect-risking diffs, replay divergences, and inconclusive replays. You opt into
each risk allowance explicitly. The repository's GitHub Action can also post a semantic
diff and preflight result as a PR comment. See
[GitHub Actions examples](docs/examples/github-actions/README.md).

## Why Orch8

Existing durable workflow engines either ship a multi-service cluster (Temporal:
Cassandra + Elasticsearch + JVM workers) or assume Python everywhere (Airflow: Celery +
Redis + scheduler). On a small team, either one is a full-time operations job.

Orch8 keeps the same execution model (state-snapshot durability, retries,
replay-on-restart) but replaces that stack with one Rust binary and Postgres. Workers can
be written in any language and connect over REST long-poll. Higher-level building blocks
(Parallel, Race, TryCatch, CancellationScope, plus LLM, HumanReview, and ToolCall) ship as
built-in blocks rather than patterns you assemble from activities.

Orch8 also runs natively on iOS and Android through Rust + UniFFI. Workflows run
offline-first on the device, sync status to the server when connected, and support
human-in-the-loop approvals through push notifications.

Portable Continuity goes a step further: a running execution can move mid-flight between
server and device, between two servers, or across a federation boundary. The move carries
cryptographic ownership transfer, tamper-evident provenance, and at-most-once effect
tracking.

The full capability list is in [docs/FEATURES.md](docs/FEATURES.md). The
[changelog](CHANGELOG.md#unreleased) has the release-by-release inventory, including
migrations and explicit non-guarantees.

## SDKs

All SDKs live in their own repositories under the [orch8-io](https://github.com/orch8-io) GitHub organization.

| Language | Package | Install | Repo |
|----------|---------|---------|------|
| TypeScript | `@orch8.io/sdk` | `npm install @orch8.io/sdk` | [sdk-node](https://github.com/orch8-io/sdk-node) |
| Expo / React Native | `@orch8.io/expo` | `npx expo install @orch8.io/expo` | [sdk-expo](https://github.com/orch8-io/sdk-expo) |
| Python | `orch8-io-sdk` | `pip install orch8-io-sdk` | [sdk-python](https://github.com/orch8-io/sdk-python) |
| Go | `github.com/orch8-io/sdk-go` | `go get github.com/orch8-io/sdk-go` | [sdk-go](https://github.com/orch8-io/sdk-go) |

The TypeScript, Python, and Go SDKs include code-first builders for all eleven
workflow block types. TypeScript carries handler-specific parameter types
through nested builders; Python accepts `TypedDict` mappings and Go exposes
`TypedStep`. Framework-neutral adapters wrap `ainvoke`, `invoke`, `kickoff`,
and `run` agent contracts as durable handlers.

For zero-server validation inside an existing process, the source tree also
ships napi-rs (`packages/node-native`) and PyO3 (`packages/python-native`)
bindings compiled from the same Rust sequence types as the server.

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
at `/api-docs/openapi.json` and `/swagger-ui`; stable snapshots are published at
`https://orch8.io/contracts/openapi.json` and
`https://orch8.io/contracts/sequence.schema.json`. Canonical product routes use the
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
- [Deployment](docs/DEPLOYMENT.md) — production deploys (Docker, Kubernetes/Helm, one-click PaaS templates, managed cloud)
- [SQLite in production](docs/SQLITE_PRODUCTION.md) — single-node SQLite + Litestream, limits, and restore drill
- [Features](docs/FEATURES.md) — the full capability list
- [Licensing](docs/LICENSING.md) — plain-language "can I use this?" table
- [Benchmarks](docs/BENCHMARKS.md) — reproducible cross-engine harness and methodology
- [External Workers](docs/WORKERS.md) — writing handlers in any language
- [Applications](docs/APPLICATIONS.md) — embedding patterns and use cases
- [Webhooks](docs/WEBHOOKS.md) — event schema and delivery semantics
- [Externalized State](docs/EXTERNALIZATION.md) — how oversized payloads are offloaded
- [Mobile SDK](docs/MOBILE_SDK.md) — UniFFI bindings, iOS/Android setup, offline-first execution
- [Continuity Operations](docs/CONTINUITY_OPERATIONS.md) — portable execution handoff, capsules, migrations, upgrade/recovery guidance
- [Continuity Debugging](docs/CONTINUITY_DEBUGGING.md) — checkpoint time-travel, what-if simulation, production-to-test extraction
- [Agent Continuity Product](docs/AGENT_CONTINUITY_PRODUCT.md) — framework-neutral offers, wrappers, profiles, receipts, conformance, and relay/OEM contracts
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
  -e ORCH8_STORAGE_BACKEND=postgres \
  -e ORCH8_DATABASE_URL=postgres://user:pass@host:5432/orch8 \
  -e ORCH8_RUN_MIGRATIONS=true \
  -e ORCH8_API_KEY=replace-with-a-long-random-secret \
  -e ORCH8_ENCRYPTION_KEY=replace-with-64-hex-characters \
  -e ORCH8_REQUIRE_TENANT_HEADER=true \
  ghcr.io/orch8-io/engine:latest
```

Mount configuration and secrets instead of putting them directly in shell
history in production. See the [Docker deployment guide](docs/DEPLOYMENT.md#docker)
and [secure bootstrap](docs/SECURE_BOOTSTRAP.md).

### Helm

The chart lives in this repository and isn't published to a Helm repository yet:

```bash
helm dependency build deploy/helm/orch8
helm install orch8 deploy/helm/orch8 --set externalDatabase.url='postgres://…'
```

The [chart README](deploy/helm/orch8/README.md) covers node roles, secrets, migrations, and monitoring.

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
- **SDK depth varies.** TypeScript has the broadest generated API surface;
  Python and Go now have complete workflow builders and workers but fewer
  convenience methods for the long-tail continuity endpoints.
- **API is stable but evolving.** Pre-1.0 means breaking changes are possible; we'll mark them in releases and keep them minimal.

If any of these are dealbreakers, file an issue — the gap-to-feature roadmap is driven by what users hit first.

## License

The engine is licensed under the [Business Source License 1.1 (BUSL-1.1)](LICENSE) with
an Additional Use Grant. BUSL-1.1 is source-available, not OSI open source.
[docs/LICENSING.md](docs/LICENSING.md) has a clause-by-clause "Can I use this?" table.

In short:

- **You can** use Orch8 in development and in production for your own applications,
  self-host it for your team or company, and modify and redistribute it under the same
  license.
- **You can't** offer Orch8 to third parties "on a hosted or embedded basis that is
  competitive with the Licensor's products" without a commercial license.
- **Ask us** at [hello@orch8.io](mailto:hello@orch8.io) about anything in between, such as
  embedding Orch8 in a product you ship or running it for a client. The license doesn't
  define "competitive", so we won't guess for you.
- **Each version** becomes available under Apache 2.0 four years after it was first
  published.

### Managed Cloud

Don't want to self-host? [orch8.io/pricing](https://orch8.io/pricing) — we run it for you.

### Commercial / OEM License

To embed Orch8 in a product or offer it as a managed service, contact
[hello@orch8.io](mailto:hello@orch8.io) about commercial licensing.
