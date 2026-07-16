# Orch8 Engine — Architecture

> Core concepts, execution model, database schema, and performance characteristics.

---

## The Big Picture

Orch8 is a durable task sequencing engine. You define a **sequence** (a list of blocks), create **instances** of that sequence, and the engine executes them — surviving crashes, respecting rate limits, and scaling to hundreds of thousands of concurrent workflows.

```
Sequence Definition          Task Instance
(the blueprint)              (one execution of the blueprint)
+-------------------+        +-----------------------------+
| blocks:           |        | id: uuid                    |
|   step "send"     |  --->  | sequence_id: ...            |
|   step "wait"     |        | state: scheduled -> running |
|   step "follow_up"|        | next_fire_at: 2026-04-15T.. |
+-------------------+        | context: { user: "alice" }  |
                              +-----------------------------+
```

**Single binary** — API server, scheduler, cron loop, and worker reaper all run in one process. PostgreSQL is the only dependency.

---

## Crate Structure

```
orch8-server          Binary entry point, wires config/storage/api/engine
    |
orch8-api             REST routes (axum), request/response types
    |
orch8-grpc            gRPC service (tonic) for high-throughput clients
    |
orch8-engine          Scheduler tick loop, evaluator, handlers, signals, recovery
    |
orch8-storage         StorageBackend trait + PostgresStorage + SqliteStorage
    |
orch8-types           Domain types, IDs, config, errors (shared by every crate)
    |
orch8-mobile          UniFFI bindings for iOS/Android, offline-first engine
    |
orch8-push            APNs/FCM push notification providers
    |
orch8-publisher       Event publishing (webhooks, NATS)
    |
orch8-cli             CLI tool (init, sequence, instance, signal, health)
```

---

## Execution Model

The engine uses **state snapshots** instead of event history replay (Temporal's model).

| Aspect | Temporal | Orch8 |
|--------|----------|-------|
| Resume mechanism | O(n) history replay | O(1) snapshot read |
| Determinism constraints | Required | None (write normal code) |
| State visibility | Requires query handler boilerplate | Direct REST query |
| Code changes | Require version markers | Apply immediately to new instances |
| Payload limits | 64KB per history event | None (stored as DB rows) |
| Testing | Requires running Temporal server | Embedded SQLite, same binary |

### Two Execution Paths

**Path 1: Timer-Driven (Scheduling Kernel)** — for sequences where steps fire on schedule:

```
Every tick (default 100ms):
  1. SELECT instances WHERE next_fire_at <= now() AND state = 'scheduled'
     ORDER BY priority DESC, next_fire_at ASC
     LIMIT batch_size FOR UPDATE SKIP LOCKED

  2. For each instance:
     a. Check rate limit → defer if exceeded
     b. Call handler
     c. Persist result to block_outputs
     d. Set next_fire_at based on delay + jitter + business days + timezone
     e. Set state = scheduled / waiting / completed
```

**Path 2: Event-Driven (Orchestration Engine)** — for workflows with parallel, race, loop, and signal-driven control flow:

```
On instance start or event:
  1. Load execution_tree for instance
  2. Evaluate current block:
     - step → call handler, persist output, advance
     - parallel → spawn all branches, wait for all
     - race → spawn all branches, first wins, cancel losers
     - loop/forEach → evaluate condition, execute body, repeat or exit
     - router → evaluate conditions, route to matching branch
     - try-catch → execute try, on failure execute catch, always run finally
  3. Resolve up the tree. If next block has delay → switch to timer-driven path
```

---

## Sequences and Blocks

A **sequence** is a named, versioned definition of work containing an ordered list of **blocks**.

### Block Types

| Block | What It Does |
|-------|-------------|
| **Step** | Calls a handler function (leaf node — actual work) |
| **Parallel** | Runs all child blocks concurrently, waits for all |
| **Race** | Runs all child blocks, first to complete wins, rest cancelled |
| **Loop** | Repeats body while condition holds (max iterations enforced) |
| **ForEach** | Iterates over a collection, runs body per item |
| **Router** | N-way branch — evaluates conditions, routes to match |
| **TryCatch** | Try block, on failure catch block, always run finally |
| **SubSequence** | Invokes another sequence as a sub-workflow |
| **ABSplit** | A/B split — routes traffic to one of several variants by weight |
| **CancellationScope** | Child blocks cannot be cancelled by external cancel signals |

Blocks are recursive — a Parallel can contain TryCatch, which can contain Steps.

### Built-in Step Handlers

| Handler | Purpose |
|---------|---------|
| `noop` | Does nothing, returns `{}` |
| `log` | Logs `params.message` at info level |
| `sleep` | Sleeps for `params.duration_ms` milliseconds |
| `fail` | Immediately fails the step with a given error message |
| `http_request` | Makes an HTTP request (method, URL, headers, body) |
| `llm_call` | Invoke an LLM provider (OpenAI/Anthropic/Bedrock) with messages |
| `tool_call` | Dispatch to a named tool (for agent tool-use loops) |
| `human_review` | Human-in-the-loop approval gate with timeout/escalation |
| `self_modify` | Dynamic step injection — modifies the running sequence at runtime |
| `emit_event` | Fire an event trigger → spawn a new workflow instance (same tenant only; supports dedupe via `dedupe_key` + `dedupe_scope` = `parent` (default) or `tenant`) |
| `send_signal` | Enqueue a signal (`pause`/`resume`/`cancel`/`update_context`/custom) to another instance (same tenant only; target terminal-state check and enqueue happen atomically in one storage transaction) |
| `query_instance` | Read another instance's context + state (same tenant only; returns `{ found: false }` for missing target) |
| `set_state` | Write a value to session-scoped state |
| `get_state` | Read a value from session-scoped state |
| `delete_state` | Delete a value from session-scoped state |
| `merge_state` | Merge an object into session-scoped state |
| `transform` | Transform context data using expressions |
| `assert` | Assert a condition, fail the step if false |
| `mcp_call` | Invoke an MCP server tool (JSON-RPC over stdio/HTTP) |
| `agent` | Autonomous agent loop — LLM + tool dispatch with durable checkpointing |
| `embed` | Generate vector embeddings for text via configured provider |
| `memory_store` | Store a vector embedding + payload into semantic memory |
| `memory_search` | Search semantic memory by similarity |
| `blob_put` | Store a binary artifact in the configured artifact backend |
| `blob_get` | Retrieve a binary artifact by key |

See [`API.md` — Workflow coordination handlers](API.md#workflow-coordination-handlers) for full param/return schemas and error semantics.

Custom handlers: registered via `HandlerRegistry` (Rust functions), or dispatched to external workers.

---

## Instance State Machine

```
                  +-----------+
         +------->| Scheduled |<------+
         |        +-----+-----+      |
         |              |             |
   retry |         claim (tick)    resume
   (backoff)            |             |
         |        +-----v-----+      |
         +--------+  Running  +------+
                  +--+--+--+--+
                     |  |  |
            +--------+  |  +--------+
            |           |           |
      +-----v---+ +----v----+ +----v-----+
      | Completed| |  Failed | | Cancelled|
      +---------+ +---------+ +----------+
                       |
                  retry from DLQ
```

Additional states: **Waiting** (for signals/external events), **Paused** (manual pause via signal).

---

## The Tick Loop

The engine's heartbeat fires every N milliseconds (default 100ms, configurable).

### Each Tick

1. **CLAIM**: `SELECT ... FOR UPDATE SKIP LOCKED` — atomically sets claimed instances to Running
2. **BATCH PREFETCH**: Fetch pending signals + completed block IDs for ALL claimed instances (2 queries, not 2N)
3. **PROCESS** (bounded by semaphore): Process signals → check concurrency → lookup sequence (LRU cache) → execute ALL pending blocks → transition state

### Key Properties

- **`FOR UPDATE SKIP LOCKED`**: Multiple engine nodes can run against the same Postgres without double-claiming
- **Postgres is the timer wheel**: Zero engine memory for scheduled instances. A million waiting instances = a million rows with `next_fire_at` timestamps
- **Priority ordering**: Critical > High > Normal > Low, then earliest `next_fire_at`
- **Multi-block execution**: All steps in a sequence run in one claim cycle (not one-per-tick)
- **Lease heartbeat**: While a step is in flight, the engine periodically touches the instance's `updated_at`, so the stale-instance reaper (`stale_instance_threshold_secs`) only recovers instances whose node actually died — a slow-but-healthy long-running step is never re-dispatched out from under itself

---

## Step Execution

### Memoization

Step results are persisted to `block_outputs`. If the engine crashes and re-processes an instance, it checks for existing output before re-running. Steps are idempotent at the engine level.

### Retry with Backoff

```
backoff = initial_backoff * multiplier^attempt (capped at max_backoff)
```

After exhausting `max_attempts`, the instance transitions to Failed and enters the DLQ.

---

## External Worker System

Write handlers in **any language**. Workers poll the engine for tasks via REST API.

1. Engine encounters a step with an unregistered handler → queues task in `worker_tasks`
2. Worker polls `POST /workers/tasks/poll` with handler name
3. Worker executes, sends heartbeats for long-running work
4. Worker reports success/failure
5. Engine resumes the workflow

**Properties:** Pull-based (no message broker), at-least-once delivery, error classification (retryable vs permanent), concurrent workers via `SKIP LOCKED`.

---

## Signals

| Signal | Effect |
|--------|--------|
| `pause` | Running → Paused |
| `resume` | Paused → Scheduled |
| `cancel` | Running → Cancelled (terminal) |
| `update_context` | Merges the payload's `data` keys into the instance context (engine-owned `runtime`/`audit` bookkeeping and read-only `config` are preserved, never overwritten) |
| `custom(name)` | Logged, for user handlers |

Signals are stored in `signal_inbox` and processed at the start of each claim cycle. At the handler boundary, `Signal::action()` lifts the `(signal_type, payload)` pair into a typed `SignalAction` variant — `UpdateContext` carries a validated `ExecutionContext`; decode failures are logged + marked delivered rather than re-tried, so malformed payloads cannot poison the queue.

---

## Background GC

| Sweeper | Target table | Cadence | Bound per tick |
|---------|-------------|---------|----------------|
| `externalized_state` TTL | rows with elapsed `expires_at` | `GC_DEFAULT_INTERVAL` (5 min) | 1 000 rows |
| `emit_event_dedupe` TTL | rows older than `EMIT_DEDUPE_DEFAULT_TTL` (30 days) | same tick | 1 000 rows |

Both sweepers share one ticker and run **concurrently via `tokio::join!`** — they touch disjoint tables and contend for neither locks nor foreign keys, so tick latency is `max(sweep_a, sweep_b)` rather than their sum. Errors are logged independently per sweep and never propagated; a missed tick is picked up by the next one. Instance-scoped cleanup (deleting an instance) is handled by `ON DELETE CASCADE` on the FK — Postgres enforces this natively; SQLite additionally requires the `PRAGMA foreign_keys = ON` pool option (already wired). The GC loop only targets **TTL-expired rows**, not instance-deletion cleanup.

---

## Database Schema

### Core Tables

| Table | Purpose |
|-------|---------|
| `sequences` | Workflow definitions (tenant, name, version, JSONB blocks) |
| `task_instances` | Executions (state, next_fire_at, priority, context, metadata) |
| `execution_tree` | Runtime tree for composite blocks (parent-child nodes) |
| `block_outputs` | Append-only step results — one row per execution (attempt, loop iteration, for_each pass). Readers that want "current state" use the most recent row by `created_at`. |
| `signal_inbox` | Pending signals for instances |
| `rate_limits` | Per-resource rate limit state |
| `cron_schedules` | Recurring workflow triggers |
| `worker_tasks` | External handler task queue |
| `resource_pools` | Resource rotation (round-robin, weighted, random) |
| `externalized_state` | Large outputs stored separately; TTL-swept by the GC loop |
| `emit_event_dedupe` | `(scope_kind, scope_value, dedupe_key) → child_instance_id` — enforces at-most-once child spawn across parent or tenant scope; TTL-swept |
| `audit_log` | Append-only state transition journal |
| `sessions` | Session-scoped data tied to instances |
| `checkpoints` | Periodic state snapshots |

### Key Indexes

```sql
-- Scheduling hot path
CREATE INDEX idx_instances_fire ON task_instances (next_fire_at) WHERE state = 'scheduled';
-- Metadata queries
CREATE INDEX idx_instances_metadata ON task_instances USING GIN (metadata jsonb_path_ops);
-- Orchestration
CREATE INDEX idx_exec_tree_instance ON execution_tree (instance_id, state);
CREATE INDEX idx_block_outputs_instance ON block_outputs (instance_id, block_id);
-- Signals
CREATE INDEX idx_signal_inbox_pending ON signal_inbox (instance_id) WHERE delivered = FALSE;
-- Rate limiting
CREATE INDEX idx_rate_limits_key ON rate_limits (tenant_id, resource_key);
```

---

## Concurrency Control

**Concurrency key**: Limit parallel instances with the same key. Uses position-based selection to avoid livelock.

**Idempotency key**: Prevent duplicate instance creation. Same key returns existing instance.

**Rate limiting**: Per-resource fixed window (not sliding — a burst can admit up to 2x `max_count` across a window boundary). Overages are deferred (not failed).

---

## Observability

### Prometheus Metrics (`GET /metrics`)

`/metrics` (and the Swagger UI at `/swagger-ui`) sit behind the same API-key auth as the rest of the management surface — a Prometheus scraper must send the `x-api-key` header (and `x-tenant-id` when tenant enforcement is on).

| Metric | Type |
|--------|------|
| `orch8_instances_claimed_total` | Counter |
| `orch8_instances_completed_total` | Counter |
| `orch8_instances_failed_total` | Counter |
| `orch8_steps_executed_total` | Counter |
| `orch8_steps_failed_total` | Counter |
| `orch8_steps_retried_total` | Counter |
| `orch8_signals_delivered_total` | Counter |
| `orch8_rate_limits_exceeded_total` | Counter |
| `orch8_recovery_stale_instances_total` | Counter |
| `orch8_webhooks_sent_total` | Counter |
| `orch8_webhooks_failed_total` | Counter |
| `orch8_cron_triggered_total` | Counter |
| `orch8_cache_hits_total` | Counter |
| `orch8_cache_misses_total` | Counter |
| `orch8_tick_duration_seconds` | Histogram |
| `orch8_step_duration_seconds` | Histogram |
| `orch8_instance_processing_seconds` | Histogram |
| `orch8_queue_depth` | Gauge |
| `orch8_active_tasks` | Gauge |

### Health Checks

- `GET /health/live` — liveness (always 200)
- `GET /health/ready` — readiness (200 if the DB is reachable **and** the engine tick loop is alive; 503 if either has died, so an orchestrator pulls the pod and restarts it rather than leaving a zombie API accepting work it will never execute)

### Webhooks

Events: `instance.completed`, `instance.failed`. Configurable URLs, retry with exponential backoff (up to 3 retries).

---

## Performance

Capacity depends on workflow shape, handler latency, database topology, payload
size, and configured concurrency. Measure the current checkout and your own
workload with the Rust benches and the isolated [load generator](../loadgen/README.md);
old point-in-time throughput numbers are not a production sizing guarantee.

The main performance mechanisms are Postgres-backed scheduling, batch prefetch,
multi-block execution per claim, sequence caching, semaphore-bounded execution,
and `SKIP LOCKED` claim coordination.

---

## Configuration

Architecture-sensitive settings include storage selection, scheduler cadence,
claim batch size, concurrency bounds, authentication, encryption, and external
state policy. Their defaults and environment names change independently from
this explanation, so the [Configuration Reference](CONFIGURATION.md) is the
single source of truth.

---

## Glossary

| Term | Definition |
|------|-----------|
| **Sequence** | Workflow template — reusable definition of blocks |
| **Instance** | Single execution of a sequence — has state, context, outputs |
| **Block** | Unit of work in a sequence (step or composite) |
| **Step** | Leaf block that executes a handler function |
| **Handler** | Function that does actual work (built-in or external worker) |
| **Execution Tree** | Runtime tree of nodes for composite sequences |
| **Worker Task** | Row in `worker_tasks` for external handler execution |
| **DLQ** | Dead Letter Queue — failed instances awaiting manual retry |
| **Tick** | One scheduler cycle (default 100ms) |
| **Claim** | Atomic acquisition of an instance (`FOR UPDATE SKIP LOCKED`) |
| **Context** | Multi-section state traveling with an instance (data, config, audit, runtime) |
| **Signal** | External command sent to a running instance |

---

## See also

- [Progressive Quick Starts](quick-starts/README.md) — build from local execution to production operations
- [API Reference](API.md) — REST endpoints, block types, error codes
- [Configuration](CONFIGURATION.md) — all config options and env vars
- [External Workers](WORKERS.md) — writing handlers in any language
- [Webhooks](WEBHOOKS.md) — event schema and delivery semantics
- [Externalized State](EXTERNALIZATION.md) — how oversized payloads are offloaded
- [Deployment](DEPLOYMENT.md) — production deploys
- [Agent Patterns](agent-patterns/README.md) — reference sequences for AI agents
