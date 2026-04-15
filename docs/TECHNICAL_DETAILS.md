# Orch8.io — Technical Architecture & Details

> **See also:** [PRD](../README.md) | [Sales Pitch](SALES_PITCH.md) | [Business Overview](BUSINESS_PRODUCT_DETAILS.md) | [Action Plan](ACTION_PLAN.md) | [Development Stages](DEVELOPMENT_STAGES.md)

## System Overview

Orch8.io is a **Rust-based durable task sequencing engine** with two operational layers:

1. **Scheduling Kernel** — Campaign-specific primitives (rate limits, send windows, resource rotation, bulk operations)
2. **Orchestration Engine** — General-purpose durable workflow execution (parallel, race, try-catch, signals, queries, template interpolation)

### Core Differentiator

**No competitor combines:**
- Embeddable library (not a platform you connect to)
- Campaign primitives (rate limits, rotation, warmup)
- Workflow orchestration (parallel, race, loop, try-catch)
- Rust performance (1M+ instances/node)
- Snapshot-based resume (no history replay, no determinism constraints)

---

## Architecture

### Execution Model

The engine uses **state snapshots per instance** instead of event history replay (Temporal's model).

| Aspect | Temporal | Orch8.io |
|--------|----------|----------|
| Resume mechanism | O(n) history replay | O(1) snapshot read |
| Determinism constraints | Required (no `Date.now()`, `Math.random()`) | None (write normal code) |
| State visibility | Requires query handler boilerplate | Direct REST/SQL query |
| Code changes | Require `patched()` version markers | Apply immediately to new instances |
| Payload limits | 64KB per history event | None (stored as DB rows) |
| Testing | Requires running Temporal server | Embedded SQLite, same binary |

### Two Execution Paths

#### Path 1: Timer-Driven (Scheduling Kernel)

For campaign sequences where steps fire on schedule:

```text
Every tick (default 100ms):
  1. SELECT instances WHERE next_fire_at <= now() AND state = 'scheduled'
     ORDER BY priority DESC, next_fire_at ASC
     LIMIT batch_size
     FOR UPDATE SKIP LOCKED

  2. For each instance:
     a. Check rate limit for assigned resource
     b. If allowed → set state = 'running', call handler
     c. If exceeded → calculate next_fire_at from rate limit window, defer
     d. If send window closed → calculate next window open time, defer

  3. Handler returns result:
     a. Persist step result to block_outputs
     b. Evaluate next step (branch condition, delay calculation)
     c. Set next_fire_at based on relative delay + jitter + business days + timezone
     d. Set state = 'scheduled' / 'waiting' / 'completed'
```

#### Path 2: Event-Driven (Orchestration Engine)

For workflows with parallel, race, loop, and signal-driven control flow:

```text
On instance start or event (signal, step completion, branch resolution):
  1. Load execution_tree for instance
  2. Evaluate current block:
     - 'step' → call handler, persist output, advance
     - 'parallel' → spawn all branches, wait for all to complete
     - 'race' → spawn all branches, first wins, cancel losers
     - 'loop'/'forEach' → evaluate condition, execute body, repeat or exit
     - 'router' → evaluate conditions, route to matching branch
     - 'try-catch' → execute try, on failure execute catch, always run finally
  3. On step completion:
     a. Persist block output (externalize if > threshold)
     b. Update execution_tree node state
     c. Check parent: resolve up the tree if needed
     d. If next block exists → execute immediately (no timer)
     e. If next block has delay → set next_fire_at, switch to timer-driven path
```

---

## Technology Stack

| Layer | Choice | Rationale |
|-------|--------|-----------|
| **Language** | Rust | Performance, memory safety, no GC pauses, brand signal |
| **Primary Storage** | PostgreSQL | Most buyers already run Postgres |
| **Embedded Storage** | SQLite | Dev, testing, small-scale self-hosted |
| **API Transport** | gRPC (primary) + REST (secondary) | gRPC for SDK perf, REST for curl/testing |
| **SDK Approach** | FFI via C ABI + language-specific wrappers | One core, multiple SDKs |
| **Scheduling** | Timer wheel + persistent queue | Efficient for millions of future-dated tasks |
| **Configuration** | TOML file + env vars + CLI flags | Standard Rust ecosystem conventions |
| **Deployment** | Single static binary + Docker image | Minimize ops complexity |

---

## Database Schema

### Core Tables

#### sequences
```sql
id              UUID PK
tenant_id       TEXT
namespace       TEXT            -- infra-level isolation
name            TEXT
definition      JSONB           -- steps, branches, delays
version         INTEGER
created_at      TIMESTAMPTZ
```

#### task_instances
```sql
id              UUID PK
sequence_id     UUID FK
tenant_id       TEXT
namespace       TEXT
state           ENUM (scheduled, running, waiting, paused, completed, failed, cancelled)
next_fire_at    TIMESTAMPTZ     -- indexed, scheduling hot path
priority        SMALLINT
timezone        TEXT
metadata        JSONB           -- buyer's custom data (GIN indexed)
context         JSONB           -- structured execution context
created_at      TIMESTAMPTZ
updated_at      TIMESTAMPTZ
```

#### execution_tree
```sql
id              UUID PK
instance_id     UUID FK
block_id        TEXT
parent_id       UUID FK NULL    -- self-referencing for nested blocks
block_type      TEXT            -- 'step', 'parallel', 'race', 'loop', 'router', 'try-catch'
branch_index    SMALLINT NULL
state           ENUM (pending, running, completed, failed, cancelled, skipped)
started_at      TIMESTAMPTZ
completed_at    TIMESTAMPTZ
```

#### block_outputs
```sql
id              UUID PK
instance_id     UUID FK
block_id        TEXT
output          JSONB
output_ref      TEXT NULL       -- reference if externalized
output_size     INTEGER
attempt         SMALLINT
created_at      TIMESTAMPTZ

UNIQUE (instance_id, block_id)
```

#### rate_limits
```sql
id              UUID PK
tenant_id       TEXT
resource_key    TEXT            -- "mailbox:john@acme.com"
max_count       INTEGER
window_seconds  INTEGER
current_count   INTEGER
window_start    TIMESTAMPTZ
```

#### signal_inbox
```sql
id              UUID PK
instance_id     UUID FK
signal_type     TEXT            -- 'pause', 'resume', 'cancel', 'update_context', custom
payload         JSONB
delivered       BOOLEAN DEFAULT FALSE
created_at      TIMESTAMPTZ
delivered_at    TIMESTAMPTZ NULL
```

### Key Indexes

```sql
-- Scheduling hot path
CREATE INDEX idx_instances_fire ON task_instances (next_fire_at) WHERE state = 'scheduled';
CREATE INDEX idx_instances_tenant ON task_instances (tenant_id, state);
CREATE INDEX idx_instances_namespace ON task_instances (namespace, state);

-- Metadata queries (replaces Temporal search attributes)
CREATE INDEX idx_instances_metadata ON task_instances USING GIN (metadata jsonb_path_ops);

-- Orchestration
CREATE INDEX idx_exec_tree_instance ON execution_tree (instance_id, state);
CREATE INDEX idx_exec_tree_parent ON execution_tree (parent_id) WHERE parent_id IS NOT NULL;
CREATE INDEX idx_block_outputs_instance ON block_outputs (instance_id, block_id);
CREATE INDEX idx_signal_inbox_pending ON signal_inbox (instance_id) WHERE delivered = FALSE;

-- Rate limiting
CREATE INDEX idx_rate_limits_key ON rate_limits (tenant_id, resource_key);
```

---

## Feature Inventory

### Core Engine (P0)

| # | Feature | Description |
|---|---------|-------------|
| 1 | Durable task instances | Atomic unit: one contact/entity in one sequence. Survives restarts |
| 2 | Step-based execution with memoization | Each step runs once, result persisted. Resume from checkpoint, no replay |
| 3 | Relative delay scheduling | Next step fires relative to previous completion, not wall clock |
| 4 | Business day awareness | Delays skip weekends. Configurable holiday calendars |
| 5 | Timezone-per-instance | Each instance carries its own timezone for send window calculation |
| 6 | Randomized jitter | Spread task firing across a window (e.g. ±2h) to prevent thundering herd |
| 7 | Conditional branching | Branch on external data at runtime |
| 8 | External event triggers (signals) | Mid-sequence event advances, cancels, or re-routes an instance |
| 9 | Crash recovery via state snapshots | On restart, read last checkpoint per instance. No history replay |
| 10 | Sequence definition API | Define multi-step sequences as code or YAML/JSON |

### Rate Limiting & Resource Management

| # | Feature | Description |
|---|---------|-------------|
| 11 | Per-resource rate limiting | Cap sends per mailbox/channel/API. Defer, not reject |
| 12 | Resource pools with rotation | Assign N mailboxes/channels. Weighted round-robin. Auto-defer on exhaustion |
| 13 | Warmup ramp schedules | New resource starts at low capacity, ramps over weeks |
| 14 | Send window scheduling | Fire tasks only during configured hours in recipient's timezone |
| 15 | Global rate limiting | Engine-wide throughput cap to protect downstream systems |

### Scale & Operations

| # | Feature | Description |
|---|---------|-------------|
| 16 | Bulk create | Enqueue 50K+ instances from one API call |
| 17 | Bulk pause/resume/cancel | Operate on all instances matching a filter |
| 18 | Bulk re-schedule | Shift all pending steps by ±N hours |
| 19 | Priority queues | At least 3 levels: high/normal/low |
| 20 | Multi-tenancy | Tenant-scoped queries, per-tenant rate limits |

### Reliability & Error Handling

| # | Feature | Description |
|---|---------|-------------|
| 21 | Configurable retry with backoff | Per-step retry policy: max attempts, exponential backoff, jitter |
| 22 | Dead letter queue | Failed instances after retry exhaustion. Inspectable, manually retryable |
| 23 | Timeout per step | Step must complete within N seconds or be marked failed |
| 24 | Circuit breaker | If downstream handler fails repeatedly, pause all steps of that type |
| 25 | Idempotency keys | Prevent duplicate step execution on retry |

### API & Integration

| # | Feature | Description |
|---|---------|-------------|
| 26 | gRPC API | Primary API surface for high-performance consumers |
| 27 | HTTP/REST API | Secondary API for easy integration and testing |
| 28 | Webhook event emitter | Push events to buyer's system |
| 29 | Node.js/TypeScript SDK | First SDK. Most campaign tools are Node-based |
| 30 | Python SDK | Second SDK. Fintech/healthtech buyers |
| 31 | Go SDK | Third SDK. Infra-oriented buyers |
| 32 | Rust crate (native) | Direct embedding for Rust consumers |

### Workflow Orchestration

| # | Feature | Description |
|---|---------|-------------|
| 51 | Bidirectional signals | Multiple named signal types: pause, resume, cancel, update context, custom payloads |
| 52 | Live state queries | Query running instance's state without stopping it |
| 53 | Transactional updates | Exactly-once update semantics for operations like approval decisions |
| 54 | Parallel execution | Run multiple branches concurrently within a single instance |
| 55 | Race execution | Run multiple branches, first to complete wins. Cancel losers cleanly |
| 56 | Cancellation scopes | Structured concurrency: cancellation propagates to all child operations |
| 57 | Try-catch-finally blocks | Error handling within sequences with recovery logic |
| 58 | Loop / forEach iteration | Iterate over collections within a sequence |
| 59 | Router (multi-way branching) | Route to one of N branches based on runtime conditions |
| 60 | Workflow versioning | Deploy new logic without breaking running instances |
| 61 | Debug mode | Breakpoints, step-through execution, state inspection |
| 62 | Search attributes | Index running instances by custom attributes (JSONB) |
| 63 | Template interpolation engine | Resolve `{{path}}` references at runtime |
| 64 | Pluggable block handler system | Extensible step type registry with built-in and user-defined handlers |
| 65 | Structured context management | Multi-section execution context with different permissions |
| 66 | Output externalization | Large outputs stored externally with references |
| 67 | Checkpointing | Periodic state snapshots for long-running instances |
| 68 | Session management | Session-scoped data tied to instances with lifecycle signals |
| 69 | Human-in-the-loop steps | Steps that block until human provides input |
| 70 | Restart from arbitrary step | Resume from any step, not just last checkpoint |
| 71 | Workflow interceptors | Pluggable hooks on lifecycle events |
| 72 | Activity heartbeating | Long-running steps report progress, detect stalled steps |
| 73 | Payload compression | Pluggable codec layer with built-in gzip |
| 74 | Graceful shutdown | Stop accepting work, drain in-flight steps, persist state, exit |
| 75 | Worker tuning | Configurable max concurrent steps/instances per node |
| 76 | Namespace isolation | Logical isolation by namespace (dev/staging/prod) |
| 77 | Task queue routing | Route step types to dedicated worker pools |

---

## Performance Targets

| Metric | Target | Rationale |
|--------|--------|-----------|
| Task instances per node | 1M+ concurrent | Must exceed BullMQ/Temporal on single box |
| Scheduling throughput | 10K+ steps/sec | Handles bulk campaign launches |
| Step execution latency (p99) | < 10ms (engine overhead) | Must be invisible to buyers |
| Memory per 1M instances | < 512MB | "Runs on a $50/mo VPS" selling point |
| Crash recovery time | < 5 seconds to resume all | Near-instant after restart |
| Bulk create 100K instances | < 3 seconds | CSV import use case |
| Storage per 1M instances | < 2GB PostgreSQL | Predictable DB sizing |

---

## Temporal Pain Points Eliminated

| # | Pain Point | Temporal Forces | Orch8.io Does | Lines Eliminated |
|---|-----------|-----------------|---------------|------------------|
| 1 | History bloat & continue-as-new | Replay entire history, externalize state at 50K events | Snapshot-based. O(1) resume. No size limits | ~300* |
| 2 | Determinism tax | No `Date.now()`, `Math.random()`, requires `patched()` | Write normal code. No replay = no constraints | ~85* |
| 3 | Payload size limits | 64KB limit, compression codecs, externalization | State is Postgres rows. No limits | ~350* |
| 4 | Activity ceremony | Define, re-export, proxyActivities, error classification | Plain functions. Built-in retry | ~1,000* |
| 5 | Invisible state | 15+ query handlers needed | `GET /instances/{id}/state` returns everything | ~450* |
| 6 | Search attribute ceremony | Pre-register on server, limited fields | JSONB in Postgres. `CREATE INDEX` on whatever | ~30* |
| 7 | Testing requires infrastructure | Running Temporal server needed | Embedded SQLite. `engine.test_mode()` | ~300* |

**Total: ~2,400 lines observed in one analyzed production codebase.** Actual workaround lines vary by project size and use case. The pattern is consistent: Temporal's replay model, payload limits, and activity model require infrastructure code that Orch8.io eliminates by design.

---

## SDK Architecture

### Node.js/TypeScript SDK (First SDK)

```typescript
// Define a step as a plain function — no ceremony
async function sendEmail(context: StepContext): Promise<StepResult> {
  const email = context.params.to;
  await emailService.send({ to: email, subject: context.data.subject });
  return { status: 'success', output: { sent: true } };
}

// Define sequence as code
const sequence = defineSequence({
  name: 'Cold Outreach',
  steps: [
    { id: 'send', handler: sendEmail, retry: { maxAttempts: 3, backoff: 'exponential' } },
    { id: 'wait', delay: { days: 2, businessDays: true, jitter: { hours: 2 } } },
    { id: 'check_reply', condition: 'data.replied', then: 'end', else: 'follow_up' },
    { id: 'follow_up', handler: sendFollowUp },
  ],
});

// Schedule instances
await engine.schedule(sequence, [
  { entityId: 'contact:1', metadata: { email: 'john@acme.com' } },
  { entityId: 'contact:2', metadata: { email: 'jane@corp.com' } },
]);

// Query running instance
const state = await engine.queryInstance(instanceId);
console.log(state.context);

// Send signal to running instance
await engine.signal(instanceId, { type: 'update_context', payload: { replied: true } });
```

### Test Mode

```typescript
// Same engine, zero external dependencies
const engine = createEngine({ storage: 'sqlite', testMode: true });

// Side-effect steps return mocks unless overridden
engine.mock('sendEmail', () => ({ status: 'success', output: { sent: true } }));

// Signals, queries, search attributes all work identically
```

---

## Storage Abstraction

```rust
pub trait StorageBackend: Send + Sync {
    async fn create_instance(&self, instance: TaskInstance) -> Result<Uuid>;
    async fn get_due_instances(&self, now: DateTime<Utc>, limit: usize) -> Vec<TaskInstance>;
    async fn update_instance_state(&self, id: Uuid, state: InstanceState) -> Result<()>;
    async fn save_block_output(&self, output: BlockOutput) -> Result<()>;
    async fn get_execution_tree(&self, instance_id: Uuid) -> Vec<ExecutionNode>;
    // ... more methods
}

// Implementations
pub struct PostgresStorage { ... }
pub struct SQLiteStorage { ... }
```

---

## Deployment Architecture

### Single Node (Starter/Growth)
```
┌─────────────────────────┐
│     Application Code    │
│    (Node.js/Python/Go)  │
├─────────────────────────┤
│   Orch8.io Engine       │
│   ┌───────────────────┐ │
│   │  Rust Binary      │ │
│   │  + gRPC Server    │ │
│   │  + REST API       │ │
│   └───────────────────┘ │
├─────────────────────────┤
│    PostgreSQL/SQLite    │
└─────────────────────────┘
```

### Multi-Node (Enterprise — Future)
```
┌──────────┐  ┌──────────┐  ┌──────────┐
│ Worker 1 │  │ Worker 2 │  │ Worker N │
└────┬─────┘  └────┬─────┘  └────┬─────┘
     │             │             │
     └─────────────┼─────────────┘
                   │
          ┌────────┴────────┐
          │  PostgreSQL     │
          │  (shared state) │
          └─────────────────┘
```

---

## Observability

### Prometheus Metrics
- Active instances count
- Steps executed per second
- Queue depth (scheduled vs running)
- Rate limit saturation events
- Error rate by step type
- Latency percentiles (p50, p95, p99)

### Structured JSON Logging
```json
{
  "timestamp": "2026-04-14T10:30:00Z",
  "level": "INFO",
  "instance_id": "uuid",
  "tenant_id": "acme",
  "event": "step.completed",
  "step_id": "send_email",
  "duration_ms": 45,
  "attempt": 1
}
```

### Health Check Endpoints
- `GET /health/ready` — can accept new work
- `GET /health/live` — process is alive

---

## API Surface

### gRPC Service Definition (excerpt)

```protobuf
service Orch8Engine {
  rpc CreateSequence(CreateSequenceRequest) returns (Sequence);
  rpc ScheduleInstances(ScheduleInstancesRequest) returns (ScheduleResponse);
  rpc QueryInstance(QueryInstanceRequest) returns (InstanceState);
  rpc SendSignal(SignalRequest) returns (SignalResponse);
  rpc BulkUpdate(BulkUpdateRequest) returns (BulkUpdateResponse);
  rpc ListInstances(ListInstancesRequest) returns (stream Instance);
}
```

### REST Endpoints (excerpt)

```
POST   /sequences                    — Create sequence
POST   /instances                    — Schedule instances
GET    /instances/{id}               — Query instance state
POST   /instances/{id}/signal        — Send signal
PATCH  /instances                    — Bulk pause/resume/cancel
GET    /instances?metadata.campaign_id=X — Filter by metadata
GET    /health/ready                 — Readiness probe
GET    /health/live                  — Liveness probe
GET    /metrics                      — Prometheus metrics
```
