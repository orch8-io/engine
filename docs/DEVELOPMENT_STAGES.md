# Orch8.io — Development Stages

> **See also:** [PRD](../README.md) | [Technical Details](TECHNICAL_DETAILS.md) | [Business Overview](BUSINESS_PRODUCT_DETAILS.md) | [Sales Pitch](SALES_PITCH.md) | [Action Plan](ACTION_PLAN.md)
>
> **Stage mapping:** Stage 0 = Week 0-2, Stage 1 = Week 2-6, Stage 2 = Week 6-10, Stage 3 = Week 10-14, Stage 4 = Week 14-18, Stage 5 = Week 18-22, Stage 6 = Week 22+

## Stage Overview

| Stage | Timeline | Focus | Deliverables | Success Criteria |
|-------|----------|-------|--------------|------------------|
| **Stage 0** | Week 0-2 | Foundation | Project scaffolding, storage trait, Postgres backend | **DONE** |
| **Stage 1** | Week 2-6 | Core Scheduling | Task instances, step execution, delays, crash recovery | **DONE** |
| **Stage 2** | Week 6-10 | Rate Limits + API | Per-resource rate limiting, gRPC/REST, Node.js SDK | **~98%** (no full Node.js SDK) |
| **Stage 3** | Week 10-14 | Orchestration Layer | Execution tree, parallel, race, signals, queries | **~90%** (no HITL, cancellation scopes) |
| **Stage 4** | Week 14-18 | Production Hardening | Bulk ops, retry, DLQ, Prometheus, webhooks | **~98%** (no SQLite test mode, Grafana) |
| **Stage 5** | Week 18-22 | Second SDK + Polish | Python SDK, SQLite, CLI, Docker, Helm chart | **~50%** (CLI, versioning, debug, externalization, checkpoints, audit log) |
| **Stage 6** | Week 22+ | Enterprise + Scale | Go SDK, clustering, dashboard | **~60%** (8 features implemented) |

---

## Stage 0: Foundation (Week 0-2)

### Objective
Establish the Rust project structure, storage abstraction, and Postgres backend.

### Key Deliverables

#### 1. Project Scaffolding
- [x] Rust workspace with multiple crates
  - `orch8-types` — shared types and IDs
  - `orch8-storage` — storage backends
  - `orch8-engine` — engine logic
  - `orch8-api` — REST server
  - `orch8-server` — binary entrypoint
- [x] CI/CD pipeline (GitHub Actions)
  - Build, test, lint (cargo check, clippy, fmt, unit tests)
  - Docker image build
- [x] Development environment setup
  - `docker-compose.yml` with Postgres 16
  - `.env` configuration template

#### 2. Storage Abstraction Trait

See [TECHNICAL_DETAILS.md — Storage Abstraction](TECHNICAL_DETAILS.md#storage-abstraction) for the full `StorageBackend` trait definition.
```

#### 3. PostgreSQL Backend Implementation
- [x] Connection pool (sqlx)
- [x] Schema migrations (sqlx migrate — 12 migrations)
- [x] Core tables created:
  - `sequences`
  - `task_instances`
  - `rate_limits`
  - `resource_pools`
  - `execution_tree`
  - `block_outputs`
  - `signal_inbox`
  - `externalized_state`
  - `cron_schedules`
  - `worker_tasks`
- [x] Key indexes created:
  - `idx_instances_fire` (hot path)
  - `idx_instances_metadata` (GIN)
  - `idx_exec_tree_instance`
  - `idx_rate_limits_key`

#### 4. Configuration System
- [x] TOML config file parsing
- [x] Environment variable overrides
- [x] CLI flag overrides
- [x] Config validation on startup

### Exit Criteria
- [x] Postgres schema created with all core tables
- [x] Storage trait implemented for Postgres
- [x] Engine starts, connects to DB, runs migrations
- [x] Basic health check endpoint responds (`/health/live`, `/health/ready`)

---

## Stage 1: Core Scheduling (Week 2-6)

### Objective
Build the durable task execution engine with crash recovery.

### Key Deliverables

#### 1. Task Instance Lifecycle
- [x] Instance creation API
- [x] Instance state machine:
  - `scheduled` → `running` → `completed` / `failed` / `waiting` / `paused` / `cancelled`
- [x] Timer wheel for scheduling:
  - Tick loop (default 100ms interval)
  - Batch SELECT with `FOR UPDATE SKIP LOCKED`
  - Semaphore-bounded concurrent processing

#### 2. Step Execution
- [x] Step handler invocation (pluggable handler registry)
- [x] Step result memoization (persist to `block_outputs`)
- [x] No replay: resume from checkpoint only
- [x] Step timeout enforcement (`tokio::time::timeout`)

#### 3. Relative Delay Scheduling
- [x] Delay calculation relative to previous step completion
- [x] Business day awareness (skip weekends)
- [x] Configurable holiday calendars
- [x] Timezone-per-instance support
- [x] Randomized jitter (±jitter_ms)

#### 4. Conditional Branching
- [x] If/else branching based on step results (Router block)
- [x] Condition evaluation from context/outputs
- [x] Next step routing based on branch outcome

#### 5. Crash Recovery
- [x] Graceful shutdown on SIGINT/SIGTERM:
  - Stop accepting new work
  - Drain in-flight steps (configurable grace period)
  - Persist state
  - Exit
- [x] Recovery on restart:
  - Reset stale `running`/`waiting` instances → `scheduled`
  - Resume from last checkpoint
  - No history replay

#### 6. Sequence Definition
- [x] Define sequences as JSON (recursive BlockDefinition enum)
- [x] Sequence versioning (basic — version field)

### Exit Criteria
- [x] Schedule 1,000 instances with delays
- [x] Kill the process mid-execution
- [x] Restart — all instances resume correctly
- [x] Business day delays work across timezones

---

## Stage 2: Rate Limits + API (Week 6-10)

### Objective
Implement per-resource rate limiting, expose APIs, and build the first SDK.

### Key Deliverables

#### 1. Per-Resource Rate Limiting
- [x] Rate limit definition:
  - Resource key (e.g., "mailbox:john@acme.com")
  - Max count per window
  - Window calculation (sliding vs fixed)
- [x] Defer scheduling (not reject):
  - If rate limit hit → calculate next available slot
  - Reschedule instance automatically
- [x] Rate limit persistence in Postgres

#### 2. Resource Pools
- [x] Pool definition with rotation strategies:
  - Round-robin
  - Weighted
  - Random
- [x] Resource assignment to instances
- [x] Auto-defer on pool exhaustion

#### 3. Warmup Ramp Schedules
- [x] New resource starts at low capacity
- [x] Gradual ramp over configured weeks
- [x] Daily cap per resource in pool

#### 4. Send Window Scheduling
- [x] Configure send hours per timezone
- [x] Defer tasks outside window
- [x] Calculate next window open time

#### 5. gRPC API
- [x] Protobuf service definitions
- [x] gRPC server implementation
- [x] Proto-generated client stubs

#### 6. REST API
- [x] HTTP endpoints (Axum): sequences, instances, cron, signals, workers, health, metrics
- [x] OpenAPI/Swagger auto-generation (utoipa + swagger-ui at `/swagger-ui`)
- [x] CORS configuration (configurable via `cors_origins` / `ORCH8_CORS_ORIGINS`)

#### 7. Node.js SDK
- [x] Worker polling SDK (`worker-sdk-node`) — poll, claim, heartbeat, complete/fail tasks
- [ ] Full client SDK (sequence creation, instance scheduling, queries)
- [ ] TypeScript type-safe client
- [ ] Signal sending

#### 8. Structured JSON Logging
- [x] Every state transition logged (tracing spans)
- [x] Fields: instance_id, step, tenant, duration, attempt
- [x] JSON format for log aggregation tools (`ORCH8_LOG_JSON`)

### Exit Criteria
- [x] Rate limits enforced across resources
- [x] gRPC + REST APIs functional
- [x] Node.js Worker SDK can poll and complete tasks
- [x] Logs structured and parseable

---

## Stage 3: Orchestration Layer (Week 10-14)

### Objective
Add workflow orchestration capabilities: parallel, race, signals, queries.

### Key Deliverables

#### 1. Execution Tree Model
- [x] Tree structure stored in `execution_tree` table
- [x] Node types: step, parallel, race, loop, router, try-catch, for-each
- [x] Parent-child relationships with self-referencing FK
- [x] Tree evaluation engine (iterative loop, up to 200 iterations per tick)

#### 2. Parallel Execution
- [x] Spawn all branches concurrently
- [x] Wait for all branches to complete
- [x] Merge outputs on completion
- [x] Independent state per branch

#### 3. Race Execution
- [x] Spawn all branches concurrently
- [x] First to complete/succeed wins
- [x] Cancel losing branches cleanly (including external worker tasks)
- [x] First-to-resolve and first-to-succeed semantics

#### 4. Cancellation Scopes
- [x] Define scopes where cancellation propagates
- [x] Non-cancellable scopes for cleanup
- [x] Structured concurrency model

#### 5. Try-Catch-Finally
- [x] Execute try block
- [x] On failure, execute catch block
- [x] Always run finally block
- [x] Error context passed to catch (`context.data._error` with failed block IDs)

#### 6. Loop / ForEach
- [x] Iterate over collections (ForEach)
- [x] Condition evaluation per iteration (Loop)
- [x] Configurable iteration cap (prevent runaway loops)
- [x] forEach with index and value

#### 7. Router (Multi-Way Branching)
- [x] N-way routing based on conditions
- [x] Fallback/default branch
- [x] Condition evaluation order

#### 8. Bidirectional Signals
- [x] Signal types: pause, resume, cancel, update_context, custom
- [x] Signal inbox management
- [x] Signal delivery to waiting blocks
- [x] Signal payload handling

#### 9. Live State Queries
- [x] `GET /instances/{id}` returns full context
- [x] No handler boilerplate needed
- [x] Filter by metadata (JSONB queries)
- [x] Dedicated execution tree inspection endpoint (`GET /instances/{id}/tree`)

#### 10. Structured Context Management
- [x] Multi-section context:
  - data (read/write)
  - config (read-only)
  - audit (append-only)
  - steps (block outputs)
  - runtime (engine-managed)
- [x] Section-level permissions

#### 11. Template Interpolation Engine
- [x] Resolve `{{path}}` references at runtime
- [x] Resolution sources: context, previous outputs, params, config
- [x] Expression support (comparisons, arithmetic, logical, parentheses)
- [x] Fallback defaults (`{{path|default_value}}` syntax)

#### 12. Pluggable Block Handler System
- [x] Built-in handlers (noop, log, sleep, http_request)
- [x] User-defined handler registration
- [x] External worker task dispatch for unregistered handlers
- [x] Recursive execution capability

#### 13. Human-in-the-Loop Steps
- [x] Steps that block until human input
- [x] Timeout with escalation
- [x] Integration with signals for response delivery

#### 14. Graceful Shutdown
- [x] SIGINT/SIGTERM handling
- [x] Configurable drain period
- [x] Persist incomplete state
- [x] Coordinated shutdown of connections

### Exit Criteria
- [x] Parallel workflow with 3 branches executes correctly
- [x] Race workflow cancels losers
- [x] Signal pauses/resumes instance
- [x] Query returns full instance state via REST
- [x] Try-catch handles errors, runs finally

---

## Stage 4: Production Hardening (Week 14-18)

### Objective
Make the engine production-ready with reliability features and observability.

### Key Deliverables

#### 1. Bulk Operations
- [x] Bulk create (batch endpoint with 500-row chunks)
- [x] Bulk pause/resume/cancel by filter
- [x] Bulk re-schedule (`PATCH /instances/bulk/reschedule` with offset_secs)

#### 2. Retry Configuration
- [x] Per-step retry policy:
  - Max attempts
  - Exponential backoff
  - Configurable backoff multiplier
- [x] Retry attempt tracking

#### 3. Dead Letter Queue
- [x] Failed instances after retry exhaustion
- [x] DLQ inspection API (`GET /instances/dlq`)
- [x] Manual retry from DLQ (`POST /instances/{id}/retry`)

#### 4. Priority Queues
- [x] 4 levels: Low/Normal/High/Critical
- [x] Priority-based ordering in tick loop (`ORDER BY priority DESC`)

#### 5. Concurrency Control
- [x] Max N instances per entity key (`concurrency_key` + `max_concurrency`)
- [x] Position-based deferral to prevent livelock

#### 6. Idempotency Keys
- [x] Prevent duplicate instance creation on retry
- [x] Idempotency key persistence (unique index on `tenant_id, idempotency_key`)

#### 7. Prometheus Metrics
- [x] Active instances count
- [x] Steps executed per second
- [x] Queue depth
- [x] Rate limit saturation
- [x] Error rate by step type
- [x] Latency percentiles (tick, step, instance duration histograms)
- [x] Grafana dashboard template (`docs/grafana-dashboard.json`)

#### 8. Webhook Event Emitter
- [x] Events: instance.completed, instance.failed
- [x] Configurable webhook URLs
- [x] Retry on webhook failure (exponential backoff, up to 3 retries)

#### 9. Multi-Tenancy
- [x] Tenant-scoped queries
- [x] Per-tenant rate limits
- [x] Noisy-neighbor protection

#### 10. Embedded Test Mode
- [x] `engine.test_mode()` with SQLite (in-memory `SqliteStorage`)
- [x] Side-effect steps return mocks (`HandlerRegistry` mock overrides)
- [x] Signals, queries work identically (full `StorageBackend` impl)
- [x] Same engine binary

#### 11. Cron-Triggered Sequences
- [x] Recurring sequence instantiation
- [x] Cron expression parsing
- [x] CRUD API for cron schedules

### Exit Criteria
- [x] 100K instances created in < 3 seconds
- [x] Prometheus metrics scraping
- [x] Webhooks firing reliably
- [x] Test mode runs in CI without external deps (SQLite in-memory)

---

## Stage 5: Second SDK + Polish (Week 18-22)

### Objective
Expand SDK support and deployment options.

### Key Deliverables

#### 1. Python SDK
- [ ] Type-safe client
- [ ] Sequence definition
- [ ] Instance management
- [ ] Async support

#### 2. SQLite Backend
- [ ] Embedded storage for dev/small deploy
- [ ] Same schema as Postgres
- [ ] Zero external dependencies

#### 3. CLI Tool
- [x] Admin operations:
  - List instances
  - Query state
  - Send signals
  - Bulk operations
- [x] Sequence management (get, lookup, versions, deprecate)
- [x] Health checks
- [x] Cron schedule management
- [x] Checkpoint management

#### 4. Docker Image
- [ ] Minimal Docker image (< 50MB)
- [ ] Health check configuration
- [ ] Environment variable configuration

#### 5. Helm Chart
- [ ] Kubernetes deployment
- [ ] Postgres dependency
- [ ] Configurable resources

#### 6. Audit Log
- [x] Append-only event journal (`audit_log` table, migration 016)
- [x] Every state transition recorded (lifecycle integration via `audit_transition`)
- [x] Queryable by instance/tenant (`GET /instances/{id}/audit`)

#### 7. Workflow Versioning
- [x] Version field + unique index on sequences
- [x] New instances bound to specific version via SequenceId FK
- [x] Running instances complete on old version (immutable binding)
- [x] Deprecation API (`POST /sequences/{id}/deprecate`)
- [x] List versions API (`GET /sequences/versions`)

#### 8. Debug Mode
- [x] Breakpoints on steps (via `metadata._debug_breakpoints` array)
- [x] Step-through execution (pauses instance, resume via signal)
- [x] State inspection at breakpoints (full instance state via REST/CLI)

#### 9. Output Externalization
- [x] `externalized_state` table created
- [x] Configurable size threshold (`ORCH8_EXTERNALIZE_THRESHOLD`)
- [x] Reference markers in block_outputs (`_externalized` + `_ref`)

#### 10. Checkpointing
- [x] Periodic state snapshots (`checkpoints` table)
- [x] Save/list/get-latest/prune API endpoints
- [x] Prune old checkpoints (keep N most recent)

#### 11. Landing Pages
- [ ] Homepage
- [ ] /for-outreach
- [ ] /for-notifications
- [ ] /pricing
- [ ] /benchmark
- [ ] /vs-temporal

### Exit Criteria
- [ ] Docker image deploys and runs
- [ ] Python SDK schedules instances
- [ ] Helm chart deploys to K8s
- [ ] Landing pages live

---

## Stage 6: Enterprise + Scale (Week 22+)

### Objective
Enterprise features, ecosystem expansion, and multi-node support.

### Key Deliverables

#### 1. Go SDK
- [ ] Idiomatic Go client
- [ ] Context support
- [ ] gRPC integration

#### 2. Dashboard UI
- [ ] Engine status overview
- [ ] Instance inspector
- [ ] Queue visualization
- [ ] Embeddable web component

#### 3. Visual Sequence Builder
- [ ] React component
- [ ] Drag-and-drop step arrangement
- [ ] Branch configuration
- [ ] Export as code/YAML

#### 4. Clustering / Multi-Node
- [x] Multiple workers sharing Postgres
- [x] SKIP LOCKED for instance claiming (already in scheduler)
- [x] Coordinated shutdown across nodes (DB-based node registration, heartbeat, drain signaling)

#### 5. A/B Split Primitive
- [x] Route N% to variant A, M% to variant B (deterministic hash-based variant selection)
- [x] Subject line testing support

#### 6. Sequence Versioning + Hot Migration
- [x] Deploy new definition without killing running instances
- [x] Hot migration API (`POST /sequences/migrate-instance`) — rebinds running instance to new version
- [x] Validation: only non-terminal instances can be migrated

#### 7. Dynamic Step Injection
- [x] Add steps to running instance at runtime (`POST /instances/{id}/inject-blocks`)
- [x] Stored as `_injected_blocks` in instance metadata (JSONB)
- [x] AI agent use case support

#### 8. Sub-Sequences / Composition
- [x] `SubSequence` block type in `BlockDefinition` enum
- [x] Parent creates child instance with `parent_instance_id` set
- [x] Parent node transitions to Waiting, checks child completion on subsequent ticks
- [x] Output propagation (child context.data merged into parent)

#### 9. Session Management
- [x] `sessions` table (migration 017) with unique key per tenant
- [x] Full CRUD API (`/sessions`, `/sessions/{id}`, `/sessions/by-key/{tenant}/{key}`)
- [x] Session state lifecycle: Active → Completed / Expired
- [x] Cross-instance session references (`session_id` FK on `task_instances`)
- [x] List instances by session (`GET /sessions/{id}/instances`)

#### 10. Workflow Interceptors
- [x] `InterceptorDef` type with lifecycle hooks (before/after step, on-signal, on-complete, on-failure)
- [x] Stored as part of sequence definition (backward-compatible JSON format)
- [x] Handler-based actions (reference handler name + params)

#### 11. Task Queue Routing
- [x] `queue_name` field on `StepDef` and `WorkerTask`
- [x] Migration 018 adds `queue_name` column to `worker_tasks`
- [x] Queue-specific polling API (`POST /workers/tasks/poll/queue`)
- [x] Dedicated worker pools claim tasks by queue name + handler

#### 12. Circuit Breaker
- [x] In-memory state machine per handler (Closed → Open → `HalfOpen`)
- [x] Configurable failure threshold and cooldown period
- [x] REST API to inspect and reset breakers (`/circuit-breakers`)
- [x] 5 unit tests for state transitions

#### 13. SLA Timers / Deadlines
- [x] Per-step deadline (`deadline` field on `StepDef`, wall-clock enforcement)
- [x] Escalation handler on breach (`on_deadline_breach` with `EscalationDef`)

#### 14. Encryption at Rest
- [x] Encrypt instance metadata/state in storage (`FieldEncryptor` with AES-256-GCM, configurable via `encryption_key`)

#### 15. SOC 2 Preparation
- [ ] Audit trail compliance
- [ ] Security documentation

### Exit Criteria
- [ ] Multi-node cluster operational
- [ ] Enterprise customer onboarded
- [ ] Dashboard UI embedded in customer app
- [ ] Go SDK adopted

---

## Development Principles

### Across All Stages

1. **Test-first** — every feature has tests before implementation
2. **Benchmark-driven** — measure performance at each stage
3. **SDK-equality** — core features must work identically across all SDKs
4. **Zero-surprise restarts** — every restart must be crash-recovery tested
5. **Documentation-parallel** — docs written alongside code, not after
6. **Dogfood everything** — use the engine's own features in development workflows

### Quality Gates

| Gate | Criteria |
|------|----------|
| Unit tests | > 80% coverage on core logic |
| Integration tests | All API paths tested with real Postgres/SQLite |
| Performance tests | Meet targets at each stage (instances/node, latency, throughput) |
| Kill/resume tests | Every stage includes restart recovery verification |
| SDK tests | Each SDK has its own test suite |

---

## Risk Mitigation by Stage

| Stage | Primary Risk | Mitigation |
|-------|-------------|------------|
| 0 | Storage abstraction too rigid | Design with Postgres first; write trait interface with SQLite compatibility in mind (SQLite backend built in Stage 5) |
| 1 | Timer wheel doesn't scale | Benchmark with 1M instances at stage end; pivot to alternative if needed |
| 2 | Rate limiting causes thundering herd | Implement jitter in rate limit recalculation |
| 3 | Execution tree evaluation too slow | Benchmark nested parallel/race/loop; optimize queries |
| 4 | Bulk operations timeout on large batches | Use batched inserts with transaction management |
| 5 | SDK quality blocks adoption | Dedicate 30% of stage time to SDK polish |
| 6 | Multi-node coordination complex | Start with Postgres SKIP LOCKED; distributed lock later if needed |
