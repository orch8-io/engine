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
orch8-engine          Scheduler tick loop, evaluator, handlers, signals, recovery
    |
orch8-storage         StorageBackend trait + PostgresStorage + SqliteStorage
    |
orch8-types           Domain types, IDs, config, errors (zero-dependency)
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

Blocks are recursive — a Parallel can contain TryCatch, which can contain Steps.

### Built-in Step Handlers

| Handler | Purpose |
|---------|---------|
| `noop` | Does nothing, returns `{}` |
| `log` | Logs `params.message` at info level |
| `sleep` | Sleeps for `params.duration_ms` milliseconds |
| `http_request` | Makes an HTTP request (method, URL, headers, body) |

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
| `update_context` | Replaces instance ExecutionContext |
| `custom(name)` | Logged, for user handlers |

Signals are stored in `signal_inbox` and processed at the start of each claim cycle.

---

## Database Schema

### Core Tables

| Table | Purpose |
|-------|---------|
| `sequences` | Workflow definitions (tenant, name, version, JSONB blocks) |
| `task_instances` | Executions (state, next_fire_at, priority, context, metadata) |
| `execution_tree` | Runtime tree for composite blocks (parent-child nodes) |
| `block_outputs` | Memoized step results (instance_id + block_id unique) |
| `signal_inbox` | Pending signals for instances |
| `rate_limits` | Per-resource rate limit state |
| `cron_schedules` | Recurring workflow triggers |
| `worker_tasks` | External handler task queue |
| `resource_pools` | Resource rotation (round-robin, weighted, random) |
| `externalized_state` | Large outputs stored separately |
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

**Rate limiting**: Per-resource sliding window. Overages are deferred (not failed).

---

## Observability

### Prometheus Metrics (`GET /metrics`)

| Metric | Type |
|--------|------|
| `orch8_instances_claimed_total` | Counter |
| `orch8_instances_completed_total` | Counter |
| `orch8_instances_failed_total` | Counter |
| `orch8_steps_executed_total` | Counter |
| `orch8_steps_retried_total` | Counter |
| `orch8_rate_limits_exceeded_total` | Counter |
| `orch8_tick_duration_seconds` | Histogram |
| `orch8_step_duration_seconds` | Histogram |
| `orch8_queue_depth` | Gauge |

### Health Checks

- `GET /health/live` — liveness (always 200)
- `GET /health/ready` — readiness (200 if DB reachable)

### Webhooks

Events: `instance.completed`, `instance.failed`. Configurable URLs, retry with exponential backoff (up to 3 retries).

---

## Performance

Benchmarked on Apple Silicon, single Postgres, single engine process:

| Operation | Throughput |
|-----------|-----------|
| Batch INSERT 100K instances | ~47,000/sec |
| Claim (SKIP LOCKED, batch=256) | ~50,000/sec |
| E2E noop, 1-step sequences | ~900/sec |
| E2E noop, 3-step sequences | ~600/sec |

**What makes it fast:** Postgres as scheduler (no in-memory queue), batch prefetch (2 queries not 2N), multi-block execution per claim, sequence LRU cache, jemalloc, semaphore-bounded concurrency, SKIP LOCKED (no lock contention).

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `ORCH8_DATABASE_URL` | `postgres://...` | PostgreSQL connection string |
| `ORCH8_HTTP_ADDR` | `0.0.0.0:8080` | HTTP listen address |
| `ORCH8_GRPC_ADDR` | `0.0.0.0:50051` | gRPC listen address |
| `ORCH8_LOG_LEVEL` | `info` | Log level |
| `ORCH8_LOG_JSON` | `false` | JSON log format |
| `ORCH8_TICK_INTERVAL_MS` | `100` | Scheduler tick interval |
| `ORCH8_BATCH_SIZE` | `256` | Instances claimed per tick |
| `ORCH8_MAX_CONCURRENT_STEPS` | `128` | Max concurrent step executions |
| `ORCH8_DB_MAX_CONNECTIONS` | `64` | DB connection pool size |
| `ORCH8_SHUTDOWN_GRACE_SECS` | `30` | Shutdown grace period |
| `ORCH8_STALE_THRESHOLD_SECS` | `300` | Stale instance recovery threshold |
| `ORCH8_WEBHOOK_URLS` | (empty) | Comma-separated webhook URLs |
| `ORCH8_CORS_ORIGINS` | (empty) | CORS allowed origins |
| `ORCH8_API_KEY` | (empty) | Optional API key for auth |
| `ORCH8_STORAGE_BACKEND` | `postgres` | Storage backend (`postgres` or `sqlite`) |

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
