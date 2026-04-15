# Orch8.io — Development Stages

> **See also:** [PRD](../README.md) | [Technical Details](TECHNICAL_DETAILS.md) | [Business Overview](BUSINESS_PRODUCT_DETAILS.md) | [Sales Pitch](SALES_PITCH.md) | [Action Plan](ACTION_PLAN.md)
>
> **Stage mapping:** Stage 0 = Week 0-2, Stage 1 = Week 2-6, Stage 2 = Week 6-10, Stage 3 = Week 10-14, Stage 4 = Week 14-18, Stage 5 = Week 18-22, Stage 6 = Week 22+

## Stage Overview

| Stage | Timeline | Focus | Deliverables | Success Criteria |
|-------|----------|-------|--------------|------------------|
| **Stage 0** | Week 0-2 | Foundation | Project scaffolding, storage trait, Postgres backend | Schema created, migrations working |
| **Stage 1** | Week 2-6 | Core Scheduling | Task instances, step execution, delays, crash recovery | 1K instances survive kill/resume |
| **Stage 2** | Week 6-10 | Rate Limits + API | Per-resource rate limiting, gRPC/REST, Node.js SDK | API functional, SDK schedules instances |
| **Stage 3** | Week 10-14 | Orchestration Layer | Execution tree, parallel, race, signals, queries | Multi-block workflows execute correctly |
| **Stage 4** | Week 14-18 | Production Hardening | Bulk ops, retry, DLQ, Prometheus, webhooks | 100K instances, metrics flowing |
| **Stage 5** | Week 18-22 | Second SDK + Polish | Python SDK, SQLite, CLI, Docker, Helm chart | Deployable via Docker, Python SDK works |
| **Stage 6** | Week 22+ | Enterprise + Scale | Go SDK, clustering, audit log, dashboard | Enterprise features ready |

---

## Stage 0: Foundation (Week 0-2)

### Objective
Establish the Rust project structure, storage abstraction, and Postgres backend.

### Key Deliverables

#### 1. Project Scaffolding
- [ ] Rust workspace with multiple crates
  - `orch8-core` — engine logic
  - `orch8-storage` — storage backends
  - `orch8-api` — gRPC + REST servers
  - `orch8-sdk-node` — Node.js bindings (via NAPI-RS or Neon)
  - `orch8-cli` — CLI tool
- [ ] CI/CD pipeline (GitHub Actions)
  - Build, test, lint
  - Docker image build
- [ ] Development environment setup
  - `docker-compose.yml` with Postgres
  - `.env` configuration template

#### 2. Storage Abstraction Trait

See [TECHNICAL_DETAILS.md — Storage Abstraction](TECHNICAL_DETAILS.md#storage-abstraction) for the full `StorageBackend` trait definition.
```

#### 3. PostgreSQL Backend Implementation
- [ ] Connection pool (deadpool or sqlx)
- [ ] Schema migrations (refinery or sqlx migrate)
- [ ] Core tables created:
  - `sequences`
  - `task_instances`
  - `rate_limits`
  - `resource_pools`
  - `execution_tree`
  - `block_outputs`
  - `signal_inbox`
- [ ] Key indexes created:
  - `idx_instances_fire` (hot path)
  - `idx_instances_metadata` (GIN)
  - `idx_exec_tree_instance`
  - `idx_rate_limits_key`

#### 4. Configuration System
- [ ] TOML config file parsing
- [ ] Environment variable overrides
- [ ] CLI flag overrides
- [ ] Config validation on startup

### Exit Criteria
- [ ] Postgres schema created with all core tables
- [ ] Storage trait implemented for Postgres
- [ ] Engine starts, connects to DB, runs migrations
- [ ] Basic health check endpoint responds

---

## Stage 1: Core Scheduling (Week 2-6)

### Objective
Build the durable task execution engine with crash recovery.

### Key Deliverables

#### 1. Task Instance Lifecycle
- [ ] Instance creation API
- [ ] Instance state machine:
  - `scheduled` → `running` → `completed` / `failed` / `waiting`
- [ ] Timer wheel for scheduling:
  - Efficient handling of millions of future-dated tasks
  - Tick loop (default 100ms interval)
  - Batch SELECT with `FOR UPDATE SKIP LOCKED`

#### 2. Step Execution
- [ ] Step handler invocation
- [ ] Step result memoization (persist to `block_outputs`)
- [ ] No replay: resume from checkpoint only
- [ ] Step timeout enforcement

#### 3. Relative Delay Scheduling
- [ ] Delay calculation relative to previous step completion
- [ ] Business day awareness (skip weekends)
- [ ] Configurable holiday calendars
- [ ] Timezone-per-instance support
- [ ] Randomized jitter (±N hours)

#### 4. Conditional Branching
- [ ] If/else branching based on step results
- [ ] Condition evaluation from context/outputs
- [ ] Next step routing based on branch outcome

#### 5. Crash Recovery
- [ ] Graceful shutdown on SIGINT/SIGTERM:
  - Stop accepting new work
  - Drain in-flight steps
  - Persist state
  - Exit
- [ ] Recovery on restart:
  - Read all instances with `state = 'running'`
  - Resume from last checkpoint
  - No history replay

#### 6. Sequence Definition
- [ ] Define sequences as code (Rust/TypeScript)
- [ ] Define sequences as YAML/JSON
- [ ] Sequence versioning (basic)

### Exit Criteria
- [ ] Schedule 1,000 instances with delays
- [ ] Kill the process mid-execution
- [ ] Restart — all instances resume correctly
- [ ] Business day delays work across timezones

---

## Stage 2: Rate Limits + API (Week 6-10)

### Objective
Implement per-resource rate limiting, expose APIs, and build the first SDK.

### Key Deliverables

#### 1. Per-Resource Rate Limiting
- [ ] Rate limit definition:
  - Resource key (e.g., "mailbox:john@acme.com")
  - Max count per window
  - Window calculation (sliding vs fixed)
- [ ] Defer scheduling (not reject):
  - If rate limit hit → calculate next available slot
  - Reschedule instance automatically
- [ ] Rate limit persistence in Postgres

#### 2. Resource Pools
- [ ] Pool definition with rotation strategies:
  - Round-robin
  - Weighted
  - Random
- [ ] Resource assignment to instances
- [ ] Auto-defer on pool exhaustion

#### 3. Warmup Ramp Schedules
- [ ] New resource starts at low capacity
- [ ] Gradual ramp over configured weeks
- [ ] Daily cap per resource in pool

#### 4. Send Window Scheduling
- [ ] Configure send hours per timezone
- [ ] Defer tasks outside window
- [ ] Calculate next window open time

#### 5. gRPC API
- [ ] Protobuf service definitions:
  - `CreateSequence`
  - `ScheduleInstances`
  - `QueryInstance`
  - `SendSignal`
  - `BulkUpdate`
  - `ListInstances`
- [ ] gRPC server implementation
- [ ] Proto-generated client stubs

#### 6. REST API
- [ ] HTTP endpoints mirroring gRPC surface
- [ ] OpenAPI/Swagger auto-generation
- [ ] CORS configuration

#### 7. Node.js SDK (First SDK)
- [ ] TypeScript type-safe client
- [ ] Sequence definition API
- [ ] Instance scheduling/querying
- [ ] Signal sending
- [ ] Error handling with typed errors
- [ ] Autocomplete support (JSDoc)

#### 8. Structured JSON Logging
- [ ] Every state transition logged
- [ ] Fields: instance_id, step, tenant, duration, attempt
- [ ] JSON format for log aggregation tools

### Exit Criteria
- [ ] Rate limits enforced across 10 resources simultaneously
- [ ] gRPC + REST APIs functional
- [ ] Node.js SDK can schedule and query instances
- [ ] Logs structured and parseable

---

## Stage 3: Orchestration Layer (Week 10-14)

### Objective
Add workflow orchestration capabilities: parallel, race, signals, queries.

### Key Deliverables

#### 1. Execution Tree Model
- [ ] Tree structure stored in `execution_tree` table
- [ ] Node types: step, parallel, race, loop, router, try-catch
- [ ] Parent-child relationships with self-referencing FK
- [ ] Tree evaluation engine (event-driven, not polled)

#### 2. Parallel Execution
- [ ] Spawn all branches concurrently
- [ ] Wait for all branches to complete
- [ ] Merge outputs on completion
- [ ] Independent state per branch

#### 3. Race Execution
- [ ] Spawn all branches concurrently
- [ ] First to complete/succeed wins
- [ ] Cancel losing branches cleanly
- [ ] First-to-resolve and first-to-succeed semantics

#### 4. Cancellation Scopes
- [ ] Define scopes where cancellation propagates
- [ ] Non-cancellable scopes for cleanup
- [ ] Structured concurrency model

#### 5. Try-Catch-Finally
- [ ] Execute try block
- [ ] On failure, execute catch block
- [ ] Always run finally block
- [ ] Error context passed to catch

#### 6. Loop / ForEach
- [ ] Iterate over collections
- [ ] Condition evaluation per iteration
- [ ] Configurable iteration cap (prevent runaway loops)
- [ ] forEach with index and value

#### 7. Router (Multi-Way Branching)
- [ ] N-way routing based on conditions
- [ ] Fallback/default branch
- [ ] Condition evaluation order

#### 8. Bidirectional Signals
- [ ] Signal types: pause, resume, cancel, update_context, custom
- [ ] Signal inbox management
- [ ] Signal delivery to waiting blocks
- [ ] Signal payload handling

#### 9. Live State Queries
- [ ] `GET /instances/{id}/state` returns full context
- [ ] No handler boilerplate needed
- [ ] Filter by metadata (JSONB queries)

#### 10. Structured Context Management
- [ ] Multi-section context:
  - data (read/write)
  - config (read-only)
  - audit (append-only)
  - steps (block outputs)
  - runtime (engine-managed)
- [ ] Section-level permissions

#### 11. Template Interpolation Engine
- [ ] Resolve `{{path}}` references at runtime
- [ ] Resolution sources: context, previous outputs, params, config
- [ ] Expression support
- [ ] Fallback defaults

#### 12. Pluggable Block Handler System
- [ ] Built-in handlers for flow control
- [ ] User-defined handler registration
- [ ] Recursive execution capability

#### 13. Human-in-the-Loop Steps
- [ ] Steps that block until human input
- [ ] Timeout with escalation
- [ ] Integration with signals for response delivery

#### 14. Graceful Shutdown
- [ ] SIGINT/SIGTERM handling
- [ ] Configurable drain period
- [ ] Persist incomplete state
- [ ] Coordinated shutdown of connections

### Exit Criteria
- [ ] Parallel workflow with 3 branches executes correctly
- [ ] Race workflow cancels losers
- [ ] Signal pauses/resumes instance
- [ ] Query returns full instance state via REST
- [ ] Try-catch handles errors, runs finally

---

## Stage 4: Production Hardening (Week 14-18)

### Objective
Make the engine production-ready with reliability features and observability.

### Key Deliverables

#### 1. Bulk Operations
- [ ] Bulk create (50K+ instances in one call)
- [ ] Bulk pause/resume/cancel by filter
- [ ] Bulk re-schedule (shift by ±N hours)

#### 2. Retry Configuration
- [ ] Per-step retry policy:
  - Max attempts
  - Exponential backoff
  - Jitter
- [ ] Retry attempt tracking

#### 3. Dead Letter Queue
- [ ] Failed instances after retry exhaustion
- [ ] DLQ inspection API
- [ ] Manual retry from DLQ

#### 4. Priority Queues
- [ ] At least 3 levels: high/normal/low
- [ ] Priority-based ordering in tick loop

#### 5. Concurrency Control
- [ ] Max N instances per entity key
- [ ] Prevent duplicate campaign execution

#### 6. Idempotency Keys
- [ ] Prevent duplicate step execution on retry
- [ ] Idempotency key persistence

#### 7. Prometheus Metrics
- [ ] Active instances count
- [ ] Steps executed per second
- [ ] Queue depth
- [ ] Rate limit saturation
- [ ] Error rate by step type
- [ ] Latency percentiles (p50, p95, p99)
- [ ] Grafana dashboard template

#### 8. Webhook Event Emitter
- [ ] Events: step.completed, instance.failed, rate_limit.hit
- [ ] Configurable webhook URLs
- [ ] Retry on webhook failure

#### 9. Multi-Tenancy
- [ ] Tenant-scoped queries
- [ ] Per-tenant rate limits
- [ ] Noisy-neighbor protection

#### 10. Embedded Test Mode
- [ ] `engine.test_mode()` with SQLite
- [ ] Side-effect steps return mocks
- [ ] Signals, queries work identically
- [ ] Same engine binary

#### 11. Cron-Triggered Sequences
- [ ] Recurring sequence instantiation
- [ ] Cron expression parsing

### Exit Criteria
- [ ] 100K instances created in < 3 seconds
- [ ] Prometheus metrics scraping
- [ ] Webhooks firing reliably
- [ ] Test mode runs in CI without external deps

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
- [ ] Admin operations:
  - List instances
  - Query state
  - Send signals
  - Bulk operations
- [ ] Sequence deployment
- [ ] Health checks

#### 4. Docker Image
- [ ] Minimal Docker image (< 50MB)
- [ ] Health check configuration
- [ ] Environment variable configuration

#### 5. Helm Chart
- [ ] Kubernetes deployment
- [ ] Postgres dependency
- [ ] Configurable resources

#### 6. Audit Log
- [ ] Append-only event journal
- [ ] Every state transition recorded
- [ ] Queryable by instance/tenant

#### 7. Workflow Versioning
- [ ] Explicit state migration functions
- [ ] New instances follow new version
- [ ] Running instances complete on old version

#### 8. Debug Mode
- [ ] Breakpoints on steps
- [ ] Step-through execution
- [ ] State inspection at breakpoints

#### 9. Output Externalization
- [ ] Large outputs stored in `externalized_state` table
- [ ] Configurable size threshold
- [ ] Reference markers in block_outputs

#### 10. Checkpointing
- [ ] Periodic state snapshots
- [ ] Configurable max history events
- [ ] On threshold, reset execution state

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
- [ ] Multiple workers sharing Postgres
- [ ] SKIP LOCKED for instance claiming
- [ ] Coordinated shutdown across nodes

#### 5. A/B Split Primitive
- [ ] Route N% to variant A, M% to variant B
- [ ] Subject line testing support

#### 6. Sequence Versioning + Hot Migration
- [ ] Deploy new definition without killing running instances
- [ ] Configurable migration rules

#### 7. Dynamic Step Injection
- [ ] Add steps to running instance at runtime
- [ ] AI agent use case support

#### 8. Sub-Sequences / Composition
- [ ] Step invokes another sequence as sub-workflow
- [ ] Output propagation

#### 9. Session Management
- [ ] Session-scoped data storage
- [ ] Session lifecycle signals
- [ ] Cross-instance session references

#### 10. Workflow Interceptors
- [ ] Pluggable lifecycle hooks
- [ ] Before/after activity
- [ ] Signal received, timer fired

#### 11. Task Queue Routing
- [ ] Multiple named task queues
- [ ] Route step types to dedicated worker pools

#### 12. Circuit Breaker
- [ ] Pause steps of failing type
- [ ] Recovery after cooldown

#### 13. SLA Timers / Deadlines
- [ ] Per-step deadline
- [ ] Escalation handler on breach

#### 14. Encryption at Rest
- [ ] Encrypt instance metadata/state in storage

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
