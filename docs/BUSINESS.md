# Orch8 Engine — Business & Product Overview

## What Is Orch8?

Orch8 is a **self-hosted, durable task orchestration engine** built in Rust. It schedules, branches, and reliably executes multi-step workflows at scale — with first-class support for retries, concurrency control, rate limiting, and polyglot handler execution.

**Core value proposition:** Define complex workflows as composable JSON sequences. Orch8 guarantees every step either completes, retries, or surfaces in a dead-letter queue. No lost work. No silent failures.

---

## Key Numbers

| Metric | Value |
|--------|-------|
| Scheduler tick frequency | 10 ticks/second (configurable, default 100ms) |
| Instances claimed per tick | Up to 256 (configurable) |
| Max concurrent step executions | 128 (configurable semaphore) |
| Default DB connection pool | 64 connections |
| Crash recovery threshold | 300 seconds (stale instance detection) |
| Worker task heartbeat timeout | 60 seconds before reaper reclaims |
| Worker reaper interval | Every 30 seconds |
| Cron evaluation interval | Every 10 seconds |
| Webhook delivery timeout | 10 seconds per attempt |
| Webhook max retries | 3 attempts with exponential backoff |
| Shutdown grace period | 30 seconds for in-flight work |
| Priority levels | 4 (Low, Normal, High, Critical) |
| Instance states | 7 (Scheduled, Running, Waiting, Paused, Completed, Failed, Cancelled) |
| Block types | 7 (Step, Parallel, Race, Loop, ForEach, Router, TryCatch) |
| Built-in handlers | 4 (noop, log, sleep, http_request) |

---

## Features

### 1. Workflow DSL — 7 Block Types

Define workflows as composable JSON blocks. Blocks nest arbitrarily — a parallel block can contain try-catch blocks, which contain steps with retries.

| Block | Purpose | Use Case |
|-------|---------|----------|
| **Step** | Execute a single handler (built-in or external) | Send email, call API, process payment |
| **Parallel** | Run N branches concurrently, wait for all | Send to email + SMS + Slack simultaneously |
| **Race** | Run N branches, first to complete wins | Try primary provider, fallback wins if faster |
| **Router** | Conditional branching (if/else/switch) | Route by user segment, plan tier, A/B test |
| **TryCatch** | Error recovery with optional finally | Try SMTP, catch with SES, always log result |
| **Loop** | Repeat while condition is true | Poll external status until ready |
| **ForEach** | Iterate over a collection | Process each item in a batch |

### 2. Durable Execution

- **Snapshot-based state** — no event sourcing, no history replay. Current state is the truth.
- **Crash recovery** — on startup, stale Running instances (>5 min) are reset to Scheduled automatically.
- **Atomic transitions** — state changes use PostgreSQL `FOR UPDATE SKIP LOCKED` for zero double-processing.
- **Memoized outputs** — each step's output is persisted. Re-execution on retry returns cached result.

### 3. External Worker System (Polyglot Handlers)

Write handlers in **any language** — Node.js, Python, Go, Java. Workers poll the engine for tasks via REST API.

**How it works:**
1. Engine encounters a step with an unregistered handler name
2. Task is queued in `worker_tasks` table, instance transitions to **Waiting**
3. External worker polls `POST /workers/tasks/poll` with the handler name
4. Worker executes the task, sends heartbeats for long-running work
5. Worker reports success (`POST /workers/tasks/{id}/complete`) or failure (`POST /workers/tasks/{id}/fail`)
6. Engine resumes the workflow from where it left off

**Key properties:**
- **Pull-based** — workers poll, no push infrastructure needed
- **At-least-once delivery** — heartbeat timeout + reaper ensures no stuck tasks
- **Error classification** — workers declare failures as retryable or permanent
- **Concurrent workers** — `SKIP LOCKED` prevents double-claiming across workers

### 4. Retry & Backoff

Per-step configurable retry with exponential backoff:

- **max_attempts** — total attempts including initial (e.g., 5 = 1 initial + 4 retries)
- **initial_backoff** — delay before first retry (e.g., 1 second)
- **max_backoff** — ceiling on backoff growth (e.g., 5 minutes)
- **backoff_multiplier** — growth factor per attempt (default 2.0x)

Example progression with `initial=1s, multiplier=2.0, max=60s`:
```
Attempt 1: immediate
Attempt 2: wait 1s
Attempt 3: wait 2s
Attempt 4: wait 4s
Attempt 5: wait 8s
```

### 5. Concurrency Control

**Concurrency key** — limit how many instances with the same key run simultaneously.

Example: Prevent running 2 campaigns for the same contact at once:
```json
{
  "concurrency_key": "contact:john@acme.com",
  "max_concurrency": 1
}
```

**Idempotency key** — prevent duplicate instance creation from retry requests:
```json
{
  "idempotency_key": "signup-flow:user-12345:2024-01-15"
}
```

### 6. Rate Limiting

Per-resource sliding window rate limits. Configured per step via `rate_limit_key`.

Example: Max 30 emails per mailbox per day:
```
resource_key: "mailbox:john@acme.com"
max_count: 30
window_seconds: 86400
```

When limit is exceeded, the step is deferred (not failed) — instance rescheduled to `retry_after` timestamp.

### 7. Cron Scheduling

Create recurring workflows with standard cron expressions and timezone support.

```json
{
  "cron_expr": "0 9 * * MON-FRI *",
  "timezone": "America/New_York",
  "sequence_id": "daily-report-sequence",
  "enabled": true
}
```

CRUD API for managing schedules. Enable/disable without deleting.

### 8. Signal System

Control running instances via signals:

| Signal | Effect |
|--------|--------|
| `pause` | Pause execution (Paused state) |
| `resume` | Resume paused instance |
| `cancel` | Cancel instance (terminal) |
| `update_context` | Merge payload into execution context |
| `custom:*` | Application-specific signals |

### 9. Multi-Tenant Architecture

- **tenant_id** — top-level isolation boundary
- **namespace** — logical grouping within a tenant (e.g., "production", "staging")
- All queries scoped to tenant+namespace
- Bulk operations scoped by filter

### 10. Execution Context

Structured, multi-section context travels with each instance:

| Section | Purpose | Mutability |
|---------|---------|------------|
| **data** | Shared data between steps (handler inputs/outputs) | Read/write by handlers |
| **config** | Instance configuration (set at creation) | Read-only after creation |
| **audit** | Append-only event log | Append-only |
| **runtime** | Engine-managed metadata (current step, attempt, timestamps) | Read-only to handlers |

### 11. Observability

- **Prometheus metrics** at `GET /metrics` — counters, histograms, gauges
- **Structured logging** with configurable level (debug/info/warn/error) and JSON format option
- **Webhook events** for instance lifecycle (started, completed, failed)
- **Dead Letter Queue** — failed instances queryable via `GET /instances/dlq`

### 12. Health Checks

Kubernetes-ready:
- `GET /health/live` — liveness probe (always 200 if process running)
- `GET /health/ready` — readiness probe (200 if database reachable, 503 otherwise)

---

## Architecture

```
┌─────────────────────────────────────────────┐
│                 orch8-server                 │
│                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │ REST API │  │ Scheduler│  │ Cron Loop│  │
│  │  (Axum)  │  │  (Tick)  │  │ (10s)    │  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  │
│       │              │              │        │
│  ┌────┴──────────────┴──────────────┴────┐  │
│  │          StorageBackend (trait)        │  │
│  │          PostgreSQL implementation     │  │
│  └───────────────────┬───────────────────┘  │
│                      │                       │
│  ┌──────────────┐  ┌┴───────────────────┐   │
│  │ HandlerReg.  │  │ Worker Task Reaper │   │
│  │ (built-in)   │  │ (30s heartbeat)    │   │
│  └──────────────┘  └────────────────────┘   │
└──────────────────────┬──────────────────────┘
                       │
              ┌────────┴────────┐
              │   PostgreSQL    │
              │  (single DB)    │
              └────────┬────────┘
                       │
         ┌─────────────┼─────────────┐
         │             │             │
    ┌────┴────┐  ┌─────┴────┐  ┌────┴────┐
    │ Node.js │  │  Python  │  │   Go    │
    │ Worker  │  │  Worker  │  │ Worker  │
    └─────────┘  └──────────┘  └─────────┘
```

**Single binary** — API server, scheduler, cron loop, and worker reaper all run in one process. No separate queue infrastructure. PostgreSQL is the only dependency.

---

## Deployment

### Requirements

- PostgreSQL 14+ (single database)
- The `orch8-server` binary (compiled from Rust)

### Configuration (Environment Variables)

| Variable | Default | Description |
|----------|---------|-------------|
| `ORCH8_DATABASE_URL` | `postgres://orch8:orch8@localhost:5432/orch8` | PostgreSQL connection string |
| `ORCH8_HTTP_ADDR` | `0.0.0.0:8080` | HTTP API listen address |
| `ORCH8_GRPC_ADDR` | `0.0.0.0:50051` | gRPC listen address |
| `ORCH8_LOG_LEVEL` | `info` | Log level (debug, info, warn, error) |
| `ORCH8_LOG_JSON` | `false` | JSON log format |
| `ORCH8_TICK_INTERVAL_MS` | `100` | Scheduler tick interval in milliseconds |
| `ORCH8_BATCH_SIZE` | `256` | Instances claimed per tick |
| `ORCH8_MAX_CONCURRENT_STEPS` | `128` | Max concurrent step executions |
| `ORCH8_DB_MAX_CONNECTIONS` | `64` | Database connection pool size |
| `ORCH8_SHUTDOWN_GRACE_SECS` | `30` | Shutdown grace period |
| `ORCH8_STALE_THRESHOLD_SECS` | `300` | Stale instance recovery threshold |
| `ORCH8_WEBHOOK_URLS` | (empty) | Comma-separated webhook URLs |
| `ORCH8_WEBHOOK_TIMEOUT_SECS` | `10` | Webhook delivery timeout |
| `ORCH8_WEBHOOK_MAX_RETRIES` | `3` | Webhook delivery retries |

### Database Migrations

Migrations run automatically on startup when `run_migrations = true` (default). 12 migration files cover:
1. Sequences table
2. Task instances table
3. Block outputs table
4. Rate limits table
5. Signals table
6. Cron schedules table
7. Execution tree (nodes) table
8. Worker tasks table
9. Various indexes for performance

---

## Glossary

| Term | Definition |
|------|-----------|
| **Sequence** | A workflow template — reusable definition of blocks to execute |
| **Instance** | A single execution of a sequence — has state, context, and outputs |
| **Block** | A unit of work in a sequence (step or composite) |
| **Step** | A leaf block that executes a handler function |
| **Handler** | A function that does the actual work (built-in Rust or external worker) |
| **Composite** | A block that contains other blocks (parallel, race, router, etc.) |
| **Execution Tree** | The runtime tree of nodes for composite sequences |
| **Worker Task** | A row in `worker_tasks` for external handler execution |
| **DLQ** | Dead Letter Queue — failed instances awaiting manual retry |
| **Tick** | One scheduler cycle (default every 100ms) |
| **Claim** | Atomic acquisition of an instance for processing (`FOR UPDATE SKIP LOCKED`) |
| **Context** | Multi-section state that travels with an instance (data, config, audit, runtime) |
| **Signal** | An external command sent to a running instance (pause, resume, cancel) |
| **Concurrency Key** | A string key for limiting parallel execution of related instances |
| **Rate Limit Key** | A resource identifier for per-resource rate limiting |
| **Idempotency Key** | A deduplication key to prevent creating duplicate instances |
