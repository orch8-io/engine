# Orch8.io Engine -- Core Concepts

> High-level mental model of how the engine works internally.

---

## The Big Picture

Orch8 is a durable task sequencing engine. You define a **sequence** (a list of blocks), create **instances** of that sequence, and the engine executes them -- surviving crashes, respecting rate limits, and scaling to hundreds of thousands of concurrent workflows.

```
Sequence Definition          Task Instance
(the blueprint)              (one execution of the blueprint)
+-------------------+        +-----------------------------+
| blocks:           |        | id: uuid                    |
|   step "send_email"|  --->  | sequence_id: ...            |
|   step "wait_reply" |       | state: scheduled -> running |
|   step "follow_up" |       | next_fire_at: 2026-04-15T... |
+-------------------+        | context: { user: "alice" }  |
                              +-----------------------------+
```

---

## Sequences and Blocks

A **sequence** is a named, versioned definition of work. It contains an ordered list of **blocks** -- the units of work.

### Block Types

| Block | What It Does | Stage |
|-------|-------------|-------|
| **Step** | Calls a handler function (the leaf node -- actual work happens here) | 1 |
| **Parallel** | Runs all child blocks concurrently, waits for all to complete | 3 |
| **Race** | Runs all child blocks concurrently, first to complete wins, rest cancelled | 3 |
| **Loop** | Repeats a body block while a condition holds (max iterations enforced) | 3 |
| **ForEach** | Iterates over a collection, running a body block per item | 3 |
| **Router** | N-way branch -- evaluates conditions, routes to the matching branch | 3 |
| **TryCatch** | Execute try block, on failure execute catch, always run finally | 3 |

Blocks are recursive -- a Parallel block can contain Steps, which can contain TryCatch, and so on. The `BlockDefinition` enum in Rust enforces this at the type level.

### Built-in Step Handlers

| Handler | Purpose |
|---------|---------|
| `noop` | Does nothing, returns `{}`. Used for testing. |
| `log` | Logs `params.message` at info level, returns it. |
| `sleep` | Sleeps for `params.duration_ms` milliseconds. |
| `http_request` | Makes an HTTP request (method, URL, headers, body from params). |

Custom handlers are registered via `HandlerRegistry` -- the engine is embeddable, so users add their own business logic as Rust functions.

---

## Task Instances

An **instance** is one execution of a sequence. It carries its own state, context, and position.

### Instance State Machine

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
                       |
                  +----v-----+
                  | Scheduled |
                  +----------+
```

Additional states: **Waiting** (for signals/external events), **Paused** (manual pause via signal).

Transitions are validated at runtime -- `InstanceState::can_transition_to()` enforces the state machine. Illegal transitions are rejected.

---

## The Tick Loop (Claiming)

The engine's heartbeat is the **tick loop** -- a timer that fires every N milliseconds (default 100ms, configurable).

### What Happens Each Tick

```
1. CLAIM: SELECT instances WHERE next_fire_at <= now() AND state = 'scheduled'
          ORDER BY priority DESC, next_fire_at ASC
          LIMIT batch_size
          FOR UPDATE SKIP LOCKED
          → atomically sets them to 'running'

2. BATCH PREFETCH:
   - Fetch pending signals for ALL claimed instances (1 query)
   - Fetch completed block IDs for ALL claimed instances (1 query)

3. PROCESS: For each claimed instance (bounded by semaphore):
   a. Process signals (pause? cancel? context update?)
   b. Check concurrency limits
   c. Look up sequence definition (in-memory LRU cache)
   d. Execute ALL pending step blocks in sequence (multi-block per cycle)
   e. Transition to Completed, or back to Scheduled if more blocks remain
```

### Key Properties

- **`FOR UPDATE SKIP LOCKED`**: Multiple engine nodes can run the same tick loop against the same Postgres without double-claiming. Locked rows are skipped, not blocked on.
- **Postgres is the timer wheel**: Zero engine memory for scheduled instances. A million instances waiting means a million rows with `next_fire_at` timestamps -- the database index does the scheduling.
- **Priority ordering**: Critical > High > Normal > Low, then by earliest `next_fire_at`.
- **Batch size**: How many instances to claim per tick (default 128). Higher = more throughput, more memory per tick.

---

## Instance Processing Pipeline

When an instance is claimed, the processing pipeline is:

```
┌──────────────────────────────────────────────────────────┐
│  1. Signal processing (prefetched)                       │
│     → Pause/Cancel/Resume/UpdateContext                  │
│     → If pause or cancel: stop here                      │
├──────────────────────────────────────────────────────────┤
│  2. Concurrency check                                    │
│     → If concurrency_key set and limit exceeded: defer   │
├──────────────────────────────────────────────────────────┤
│  3. Sequence lookup (cached in moka LRU, 1K entries)     │
├──────────────────────────────────────────────────────────┤
│  4. Block loop (multi-block per claim cycle)             │
│     For each block in sequence:                          │
│       → Skip if already completed (prefetched set)       │
│       → Check delay → defer if not yet due               │
│       → Check rate limit → defer if exceeded             │
│       → Look up attempt number from block_outputs        │
│       → Call handler via HandlerRegistry                 │
│       → On success: save output, continue to next block  │
│       → On retryable failure: backoff + re-schedule      │
│       → On permanent failure: mark Failed                │
├──────────────────────────────────────────────────────────┤
│  5. All blocks done → transition to Completed            │
│     → Emit webhook, increment metrics                    │
└──────────────────────────────────────────────────────────┘
```

### Multi-Block Execution

The engine executes ALL ready blocks in a single claim cycle. A 3-step sequence completes in 1 claim instead of 3. If a block is deferred (delay, rate limit) or fails, processing stops at that block and the instance is re-scheduled.

---

## Step Execution

A **step** is the leaf node -- where actual work happens.

```
StepExecParams {
    instance_id,
    block_id,
    handler_name,    // "noop", "http_request", or custom
    params,          // JSON from the sequence definition
    context,         // ExecutionContext (data, config, audit, runtime)
    attempt,         // 0-based retry counter
    timeout,         // optional per-step timeout
}
```

### Memoization

Step results are persisted to `block_outputs`. If the engine crashes mid-execution and re-processes the instance, it checks for existing output before re-running the handler. This makes steps idempotent at the engine level.

### Retry with Backoff

Failed steps with a retry policy get exponential backoff:

```
backoff = initial_backoff * multiplier^attempt
capped at max_backoff
```

The instance is re-scheduled with `next_fire_at = now + backoff`. After exhausting `max_attempts`, the instance transitions to Failed and enters the DLQ.

---

## Concurrency Control

Instances can declare a `concurrency_key` (e.g., `"tenant:acme"`) and `max_concurrency` (e.g., 5). The engine uses position-based selection:

```sql
SELECT COUNT(*) FROM task_instances
WHERE concurrency_key = $1 AND state = 'running' AND id <= $2
```

If this instance's position exceeds the limit, it's deferred for 2 seconds. This is deterministic -- the same instances always proceed, avoiding livelock.

---

## Signals

External actors can send signals to running instances:

| Signal | Effect |
|--------|--------|
| `pause` | Running -> Paused (stops execution) |
| `resume` | Paused -> Scheduled (re-enters tick loop) |
| `cancel` | Running -> Cancelled (terminal) |
| `update_context` | Replaces the instance's ExecutionContext |
| `custom(name)` | Logged, no built-in behavior (for user handlers) |

Signals are stored in `signal_inbox` and processed at the start of each instance claim cycle.

---

## Storage Layer

All state is in Postgres. The `StorageBackend` trait has 30+ methods, grouped by domain:

| Domain | Hot-Path Methods |
|--------|-----------------|
| Instances | `claim_due_instances`, `update_instance_state` |
| Block outputs | `save_block_output`, `get_completed_block_ids_batch` |
| Signals | `get_pending_signals_batch`, `mark_signals_delivered` |
| Sequences | `get_sequence` (cached in engine) |
| Rate limits | `check_rate_limit` (atomic check-and-increment) |
| Cron | `claim_due_cron_schedules`, `update_cron_fire_times` |

### Key Indexes

- `idx_instances_fire`: Partial index on `(next_fire_at) WHERE state = 'scheduled'` -- the tick loop's primary lookup.
- `idx_instances_concurrency`: On `(concurrency_key, state)` for position queries.
- `idx_block_outputs_instance`: On `(instance_id, block_id)` for memoization lookups.

---

## Metrics (Prometheus)

| Metric | Type | What It Measures |
|--------|------|-----------------|
| `orch8_instances_claimed_total` | Counter | Instances picked up per tick |
| `orch8_instances_completed_total` | Counter | Successfully finished |
| `orch8_instances_failed_total` | Counter | Processing errors |
| `orch8_steps_executed_total` | Counter | Handler invocations |
| `orch8_steps_failed_total` | Counter | Handler failures |
| `orch8_steps_retried_total` | Counter | Retry backoff events |
| `orch8_rate_limits_exceeded_total` | Counter | Rate limit deferrals |
| `orch8_signals_delivered_total` | Counter | Signals processed |
| `orch8_webhooks_sent_total` | Counter | Webhook POST attempts |
| `orch8_cron_triggered_total` | Counter | Cron-created instances |
| `orch8_tick_duration_seconds` | Histogram | Time per tick cycle |
| `orch8_step_duration_seconds` | Histogram | Time per handler call |
| `orch8_instance_processing_seconds` | Histogram | Total time per instance in one claim |
| `orch8_queue_depth` | Gauge | Instances claimed in last tick |

---

## Performance Characteristics

Benchmarked on MacBook (Apple Silicon), single Postgres, single engine process:

| Operation | Throughput |
|-----------|-----------|
| Batch INSERT 100K instances | ~47,000/sec |
| Claim (SKIP LOCKED, batch=256) | ~50,000/sec |
| E2E noop, 1-step sequences | ~900/sec |
| E2E noop, 3-step sequences | ~600/sec |

### What Makes It Fast

1. **Postgres is the scheduler** -- no in-memory priority queue to manage or sync
2. **Batch prefetch** -- signals and completed blocks fetched for entire claim batch in 2 queries (not 2N)
3. **Multi-block execution** -- all steps in a sequence run in one claim cycle (not one-per-tick)
4. **Sequence cache** -- moka LRU (1K entries, 5m TTL) avoids repeated DB lookups
5. **jemalloc** -- optimized allocator for long-running JSON-heavy workloads
6. **Semaphore-bounded concurrency** -- configurable max in-flight steps (default 128)
7. **SKIP LOCKED** -- no lock contention between concurrent engine nodes

---

## Crash Recovery

On startup, the engine runs:

```sql
UPDATE task_instances
SET state = 'scheduled', next_fire_at = NOW()
WHERE state = 'running'
  AND updated_at < NOW() - interval '60 seconds'
```

Any instance that was Running when the engine crashed is reset to Scheduled and re-processed. Step memoization prevents duplicate handler invocations.

---

## Cron Scheduling

Cron schedules automatically create instances on a schedule:

```
CronSchedule {
    sequence_id,        // which sequence to instantiate
    cron_expr,          // "0 9 * * MON-FRI" (standard cron)
    timezone,           // "America/New_York"
    enabled,            // toggle on/off
    next_fire_at,       // pre-computed next trigger time
}
```

A separate tick loop checks `claim_due_cron_schedules` and creates instances. After firing, `next_fire_at` is advanced to the next occurrence.
