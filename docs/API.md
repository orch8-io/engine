# Orch8 Engine — API Reference

Canonical base URL: `http://localhost:8080/api/v1` (listen address configurable via `ORCH8_HTTP_ADDR`)

All request/response bodies are JSON. Dates use ISO 8601 / RFC 3339 format.

Product routes are served under `/api/v1`. Bare product paths remain compatibility
aliases for pre-versioned clients. New integrations should always use `/api/v1`.
Health, metrics, Swagger UI, and the OpenAPI JSON remain at root paths.

## Authentication

When `ORCH8_API_KEY` is set, every endpoint requires an `x-api-key` header — including `/metrics`, `/swagger-ui`, and `/api-docs/openapi.json`. When `ORCH8_REQUIRE_TENANT_HEADER` is set, requests must also carry an `x-tenant-id` header, and tenant-owned resources are scoped to that tenant.

Only the health probes (`/health/live`, `/health/ready`, `/info`) and inbound public webhooks (`POST /webhooks/{slug}`, which use their own per-trigger secrets) stay public, so load-balancer checks and third-party webhook callers keep working.

---

## Health

### Liveness Probe

```
GET /health/live
```

Always returns 200 if the process is running.

**Response:** `200 OK`

---

### Readiness Probe

```
GET /health/ready
```

Returns 200 only if the database is reachable **and** the engine tick loop is running. A server whose scheduler has died reports 503 even with a healthy database, so the load balancer stops routing work to a node that would accept instances but never execute them.

**Response:** `200 OK` or `503 Service Unavailable`

---

## Sequences

### Create Sequence

```
POST /sequences
```

**Request Body:**

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "tenant_id": "acme",
  "namespace": "default",
  "name": "welcome-campaign",
  "version": 1,
  "blocks": [
    {
      "type": "step",
      "id": "send_welcome",
      "handler": "email_send",
      "params": { "template": "welcome" },
      "retry": {
        "max_attempts": 3,
        "initial_backoff": 1000,
        "max_backoff": 60000,
        "backoff_multiplier": 2.0
      },
      "timeout": 30000
    },
    {
      "type": "step",
      "id": "wait_3_days",
      "handler": "sleep",
      "params": { "duration_ms": 259200000 }
    },
    {
      "type": "router",
      "id": "check_engagement",
      "routes": [
        {
          "condition": "opened == true",
          "blocks": [
            { "type": "step", "id": "send_followup", "handler": "email_send", "params": { "template": "followup" } }
          ]
        }
      ],
      "default": [
        { "type": "step", "id": "send_reminder", "handler": "email_send", "params": { "template": "reminder" } }
      ]
    }
  ],
  "input_schema": {
    "type": "object",
    "properties": {
      "email": { "type": "string" },
      "age": { "type": "integer", "minimum": 0 }
    },
    "required": ["email"]
  },
  "created_at": "2024-01-15T10:00:00Z"
}
```

**`input_schema`** (optional): a [JSON Schema](https://json-schema.org/) object describing the shape of `context.data` for instances of this sequence. When present:

- The schema itself is checked for well-formedness at sequence-create time — a broken schema is rejected with `400 Bad Request`.
- Every instance create (single and batch) validates its `context.data` against the schema. A payload that fails validation is rejected with `422 Unprocessable Entity` (the body lists each violation with its JSON-pointer path) **before** the instance is persisted.
- The schema doubles as the contract the dashboard renders a Run form from.

Omit `input_schema` (or set it to `null`) to accept any `context.data` unchecked.

**`sla`** (optional): an alert-only SLA policy `{ "max_runtime": <ms>, "max_step_runtime": <ms> }` (both fields optional, milliseconds). When an active instance's wall-clock lifetime exceeds `max_runtime`, or its current step's runtime exceeds `max_step_runtime`, the scheduler emits an `instance.sla_breached` webhook and increments the `orch8_sla_breached_total{type=...}` metric — **once per breach** (de-duplicated). The instance is **not** paused or failed; this is an alert, not a state change. (For a hard per-step deadline that fails the step, use `deadline` on a step instead.)

**`on_failure` / `on_cancel`** (optional): block lists run **best-effort** when the instance reaches terminal `Failed` / `Cancelled`. Each top-level **step** block is dispatched once with the instance's final context (its output is recorded under the step's block id); errors are swallowed (the instance is already terminal). Use to release a lock, post a death notification, etc. — so a failed/cancelled run doesn't silently vanish. v1 runs step blocks only (non-step blocks are skipped with a warning). `on_cancel` fires on both signal-driven cancellation and cancellation that completes through evaluation.

**Response:** `201 Created`

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000"
}
```

---

### Get Sequence

```
GET /sequences/{id}
```

**Response:** `200 OK` — full `SequenceDefinition` object.

---

### Get Sequence by Name

```
GET /sequences/by-name?tenant_id=acme&namespace=default&name=welcome-campaign&version=1
```

| Parameter | Required | Description |
|-----------|----------|-------------|
| `tenant_id` | Yes | Tenant identifier |
| `namespace` | Yes | Namespace |
| `name` | Yes | Sequence name |
| `version` | No | Specific version (latest if omitted) |

**Response:** `200 OK` — full `SequenceDefinition` object.

---

## Instances

### Create Instance

```
POST /instances
```

**Request Body:**

```json
{
  "sequence_id": "550e8400-e29b-41d4-a716-446655440000",
  "tenant_id": "acme",
  "namespace": "default",
  "priority": "normal",
  "timezone": "America/New_York",
  "metadata": { "campaign": "spring-2024", "segment": "enterprise" },
  "context": {
    "data": { "email": "john@acme.com", "name": "John" },
    "config": { "sender": "noreply@orch8.io" }
  },
  "next_fire_at": "2024-01-15T14:00:00Z",
  "concurrency_key": "contact:john@acme.com",
  "max_concurrency": 1,
  "idempotency_key": "welcome:john@acme.com:2024-01-15"
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `sequence_id` | UUID | **required** | Sequence to execute |
| `tenant_id` | string | **required** | Tenant identifier |
| `namespace` | string | **required** | Namespace |
| `priority` | string | `"normal"` | `low`, `normal`, `high`, `critical` |
| `timezone` | string | `"UTC"` | IANA timezone |
| `metadata` | object | `{}` | Arbitrary JSON metadata |
| `context` | object | `{}` | Execution context (data, config sections) |
| `next_fire_at` | datetime | now | When to start execution |
| `concurrency_key` | string | null | Key for concurrency limiting |
| `max_concurrency` | integer | null | Max parallel instances with same key |
| `idempotency_key` | string | null | Deduplication key |
| `dry_run` | boolean | `false` | Simulate the run: side-effecting steps (HTTP/LLM/MCP calls, agent loops, event/signal emission, memory writes, block injection, external plugins) skip their real effect and return a stub; control flow and templating run normally |
| `dry_run_auto_approve` | boolean | `false` | With `dry_run`, auto-approve `human_review` gates with the default choice so the simulation flows past them. Ignored unless `dry_run` is true |
| `budget` | object | null | Resource budget (token/step caps) enforced by the scheduler; when exceeded the instance is paused with `metadata.paused_reason = "budget_exceeded"` |

**Response:** `201 Created`

```json
{
  "id": "a1b2c3d4-...",
  "deduplicated": false
}
```

If `idempotency_key` matches an existing instance:

```json
{
  "id": "existing-instance-id",
  "deduplicated": true
}
```

---

### Create Instances (Batch)

```
POST /instances/batch
```

**Request Body:**

```json
{
  "instances": [
    { "sequence_id": "...", "tenant_id": "acme", "namespace": "default" },
    { "sequence_id": "...", "tenant_id": "acme", "namespace": "default" }
  ]
}
```

**Response:** `201 Created`

```json
{
  "count": 2
}
```

---

### Get Instance

```
GET /instances/{id}
```

**Response:** `200 OK`

```json
{
  "id": "a1b2c3d4-...",
  "sequence_id": "550e8400-...",
  "tenant_id": "acme",
  "namespace": "default",
  "state": "running",
  "next_fire_at": "2024-01-15T14:00:00Z",
  "priority": "normal",
  "timezone": "America/New_York",
  "metadata": { "campaign": "spring-2024" },
  "context": {
    "data": { "email": "john@acme.com" },
    "config": {},
    "audit": [],
    "runtime": { "current_step": "send_welcome", "attempt": 0 }
  },
  "concurrency_key": "contact:john@acme.com",
  "max_concurrency": 1,
  "idempotency_key": "welcome:john@acme.com:2024-01-15",
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T14:00:05Z"
}
```

---

### List Instances

```
GET /instances?tenant_id=acme&namespace=default&state=running,scheduled&limit=50&offset=0
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tenant_id` | string | — | Filter by tenant |
| `namespace` | string | — | Filter by namespace |
| `sequence_id` | UUID | — | Filter by sequence |
| `state` | string | — | Comma-separated states to include |
| `offset` | integer | `0` | Pagination offset |
| `limit` | integer | `100` | Page size (max 1000) |
| `metadata.<key>` | string | — | Filter to instances whose `metadata.<key>` equals the value. Repeatable — multiple `metadata.*` params AND together. |

**Metadata search.** Any query parameter prefixed `metadata.` filters on the instance's `metadata` object by top-level key — e.g. `GET /instances?metadata.env=prod&metadata.team=core` returns instances whose metadata has both `env=prod` and `team=core`. Matching is string-equality on top-level keys. On Postgres this is a JSONB containment (`@>`) query served by the existing GIN index on `metadata` (no Elasticsearch required, unlike Temporal search attributes); on SQLite it falls back to `json_extract`.

```
GET /instances?metadata.env=prod&metadata.team=core
```

**Response:** `200 OK` — paginated envelope:

```json
{ "items": [ /* TaskInstance objects */ ], "has_more": true }
```

`has_more` is true when the page is full (another page may exist at the next `offset`).

---

### List Child Instances

```
GET /instances/{id}/children
```

Returns the instances spawned by `{id}` as sub-sequences (those whose `parent_instance_id` equals `{id}`). `parent_instance_id` is already present on every `TaskInstance` response, so a child can link back to its parent and a parent can enumerate its children. `404` if the parent does not exist (or is owned by another tenant).

**Response:** `200 OK` — array of `TaskInstance` objects.

---

### List Step Logs

```
GET /instances/{id}/logs
```

Returns the step logs for an execution, oldest first, each annotated with its `block_id`. Two sources feed this: in-process handler logs (captured by a tracing layer scoped to each step's span) and logs an external worker attached when completing/failing a task. Each entry is `{ block_id, ts, level, message }`.

**Response:** `200 OK` — array of step-log entries.

To attach worker logs, include a `logs` array in the complete/fail body:

```
POST /workers/tasks/{id}/complete   # { worker_id, output, logs: [{ ts, level, message }] }
POST /workers/tasks/{id}/fail       # { worker_id, message, retryable, logs: [...] }
```

---

### Batch Action

```
POST /instances/batch-action
```

Apply one control action to every instance matching a filter. Always tenant-scoped (a `tenant_id` in the filter is required). Capped and audited.

**Request Body:**

```json
{
  "filter": {
    "tenant_id": "acme",
    "namespace": "default",
    "states": ["failed"],
    "metadata": { "env": "prod" }
  },
  "action": "retry",
  "signal_type": null,
  "payload": {},
  "dry_run": false,
  "limit": 1000
}
```

| Field | Description |
|-------|-------------|
| `filter` | Same shape as the list filters: `tenant_id` (required), `namespace`, `sequence_id`, `states`, and `metadata` (top-level equality map). |
| `action` | One of `retry`, `pause`, `resume`, `cancel`, `signal`. `retry` re-runs `Failed` instances; `pause`/`resume`/`cancel` enqueue the matching control signal; `signal` enqueues a custom signal named by `signal_type` carrying `payload`. |
| `signal_type` | Required when `action` is `signal` (the custom name, no `custom:` prefix). |
| `dry_run` | When `true`, only count matching instances; apply nothing. |
| `limit` | Hard cap on instances acted on. Default `1000`, max `10000`. |

**Response:** `200 OK`

```json
{ "matched": 42, "applied": 40, "skipped": 2, "failed": 0, "dry_run": false }
```

`skipped` counts instances the action did not apply to (e.g. `retry` of a non-`Failed` instance, or a signal to a terminal instance); `failed` counts storage errors. Every applied action writes a `batch_action` audit-log entry.

---

### Update Instance State

```
PATCH /instances/{id}/state
```

**Request Body:**

```json
{
  "state": "paused",
  "next_fire_at": "2024-01-16T09:00:00Z"
}
```

**Valid state transitions:**

| From | To |
|------|----|
| Scheduled | Running, Paused, Cancelled |
| Running | Scheduled, Waiting, Completed, Failed, Paused, Cancelled |
| Waiting | Running, Scheduled, Cancelled, Failed |
| Paused | Scheduled, Cancelled |
| Failed | Scheduled (retry) |
| Completed | _(terminal)_ |
| Cancelled | _(terminal)_ |

**Response:** `200 OK`

Returns `400 Bad Request` if the transition is invalid.

---

### Update Instance Context

```
PATCH /instances/{id}/context
```

**Request Body:**

```json
{
  "context": {
    "data": { "opened": true, "clicked_link": "pricing" }
  }
}
```

**Response:** `200 OK`

---

### Send Signal

```
POST /instances/{id}/signals
```

**Request Body:**

```json
{
  "signal_type": "pause",
  "payload": {}
}
```

| Signal Type | Effect |
|-------------|--------|
| `pause` | Pause execution |
| `resume` | Resume from Paused |
| `cancel` | Cancel instance |
| `update_context` | Merge payload into context.data |
| `custom:*` | Application-defined signal |

**Response:** `201 Created`

```json
{
  "signal_id": "b5c6d7e8-..."
}
```

---

### Get Outputs

```
GET /instances/{id}/outputs
```

**Response:** `200 OK`

```json
[
  {
    "id": "f1e2d3c4-...",
    "instance_id": "a1b2c3d4-...",
    "block_id": "send_welcome",
    "output": { "email_id": "msg-abc123", "status": "sent" },
    "output_ref": null,
    "output_size": 52,
    "attempt": 0,
    "created_at": "2024-01-15T14:00:03Z"
  }
]
```

---

### Retry Failed Instance

```
POST /instances/{id}/retry
```

Instance must be in `failed` state. Resets to `scheduled` with `next_fire_at = now`.

**Response:** `200 OK`

```json
{
  "id": "a1b2c3d4-...",
  "state": "scheduled"
}
```

---

### Bulk Update State

```
PATCH /instances/bulk/state
```

**Request Body:**

```json
{
  "filter": {
    "tenant_id": "acme",
    "namespace": "default",
    "sequence_id": "550e8400-...",
    "states": ["scheduled", "running"]
  },
  "state": "cancelled"
}
```

**Response:** `200 OK`

```json
{
  "count": 47
}
```

---

### List Dead Letter Queue

```
GET /instances/dlq?tenant_id=acme&namespace=default&limit=50
```

Same parameters as List Instances. Returns only `failed` instances.

**Response:** `200 OK` — array of `TaskInstance` objects.

---

## Cron Schedules

### Create Cron Schedule

```
POST /cron
```

**Request Body:**

```json
{
  "tenant_id": "acme",
  "namespace": "default",
  "sequence_id": "550e8400-...",
  "cron_expr": "0 9 * * MON-FRI *",
  "timezone": "America/New_York",
  "metadata": { "type": "daily-report" },
  "enabled": true
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `tenant_id` | string | **required** | Tenant identifier |
| `namespace` | string | **required** | Namespace |
| `sequence_id` | UUID | **required** | Sequence to instantiate |
| `cron_expr` | string | **required** | 7-field cron expression |
| `timezone` | string | `"UTC"` | IANA timezone for schedule |
| `metadata` | object | `{}` | Passed to created instances |
| `enabled` | boolean | `true` | Whether schedule is active |
| `overlap_policy` | string | `"allow"` | What to do when a fire is due while a previous run is still active: `allow`, `skip`, `buffer_one`, `cancel_previous` |

**Overlap policies** — when a fire is due and a previous run from this
schedule is still active (scheduled/running/waiting/paused):

- `allow` (default) — fire regardless.
- `skip` — skip the occurrence; increments `skipped_fires`, stamps
  `last_skipped_at`, and bumps the `orch8_cron_skipped_total` metric.
- `buffer_one` — defer until the previous run finishes, then fire once
  (multiple missed occurrences collapse into a single buffered fire).
- `cancel_previous` — cancel still-active runs, then fire.

Previous runs are attributed via `metadata.cron_schedule_id`, which the
engine stamps on every cron-created instance.

**Cron expression format (7 fields):**

```
second  minute  hour  day  month  day_of_week  year
  0       9      *     *     *      MON-FRI      *
```

Standard 5-field Unix cron (`m h dom mon dow`) is also accepted and
normalized (seconds `0`, year `*`).

**DST behavior (explicit, unit-tested):**

- **Nonexistent local time** (spring forward — e.g. a 02:30 schedule in
  `America/New_York` on the day clocks jump 02:00 → 03:00): the occurrence
  fires at the **first valid instant after the gap** (03:00 local). It is
  never silently skipped.
- **Ambiguous local time** (fall back — a 01:30 schedule on the day clocks
  rewind 02:00 → 01:00): the occurrence fires **once**, at the first
  (pre-transition) occurrence. Never twice.

**Response:** `201 Created`

```json
{
  "id": "c1d2e3f4-...",
  "next_fire_at": "2024-01-16T14:00:00Z"
}
```

---

### Get Cron Schedule

```
GET /cron/{id}
```

**Response:** `200 OK` — full `CronSchedule` object.

---

### List Cron Schedules

```
GET /cron?tenant_id=acme
```

**Response:** `200 OK` — array of `CronSchedule` objects.

---

### Update Cron Schedule

```
PUT /cron/{id}
```

**Request Body (all fields optional):**

```json
{
  "cron_expr": "0 10 * * * *",
  "timezone": "Europe/London",
  "enabled": false,
  "metadata": { "type": "weekly-digest" },
  "overlap_policy": "skip"
}
```

**Response:** `200 OK` — updated `CronSchedule`.

---

### Preview Next Fires

```
GET /cron/{id}/next-fires?n=5
```

Returns the schedule's upcoming fire instants (UTC, DST-correct) without waiting for them. `n` defaults to 5, max 50.

**Response:** `200 OK`

---

### Delete Cron Schedule

```
DELETE /cron/{id}
```

**Response:** `204 No Content`

---

## External Workers

### Poll for Tasks

```
POST /workers/tasks/poll
```

**Request Body:**

```json
{
  "handler_name": "email_send",
  "worker_id": "node-worker-42",
  "limit": 10
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `handler_name` | string | **required** | Handler to claim tasks for |
| `worker_id` | string | **required** | Unique worker identifier |
| `limit` | integer | `1` | Max tasks to claim |
| `version` | string | null | Worker build/deploy version, recorded on the worker registry and checked against [version pins](#worker-version-pins) |

**Response:** `200 OK`

```json
[
  {
    "id": "d4e5f6a7-...",
    "instance_id": "a1b2c3d4-...",
    "block_id": "send_welcome",
    "handler_name": "email_send",
    "params": { "template": "welcome", "to": "john@acme.com" },
    "context": { "data": { "name": "John" }, "config": {} },
    "attempt": 0,
    "timeout_ms": 30000,
    "state": "claimed",
    "worker_id": "node-worker-42",
    "claimed_at": "2024-01-15T14:00:00Z",
    "heartbeat_at": "2024-01-15T14:00:00Z",
    "completed_at": null,
    "output": null,
    "error_message": null,
    "error_retryable": null,
    "created_at": "2024-01-15T13:59:58Z"
  }
]
```

Returns empty array `[]` if no tasks available.

**Mechanics:**
- Uses `FOR UPDATE SKIP LOCKED` — concurrent workers never get the same task.
- Sets `state = claimed`, `worker_id`, `claimed_at`, and `heartbeat_at`.

---

### Complete Task

```
POST /workers/tasks/{task_id}/complete
```

**Request Body:**

```json
{
  "worker_id": "node-worker-42",
  "output": {
    "email_id": "msg-abc123",
    "delivered": true
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `worker_id` | string | Must match the worker that claimed the task |
| `output` | object | Result JSON (saved as BlockOutput) |

**Response:** `200 OK`

**Side effects:**
1. Worker task marked `completed`
2. `BlockOutput` created with the output JSON
3. Instance transitions `Waiting -> Scheduled` (immediate re-processing)
4. If instance uses execution tree, corresponding node marked `Completed`

---

### Fail Task

```
POST /workers/tasks/{task_id}/fail
```

**Request Body:**

```json
{
  "worker_id": "node-worker-42",
  "message": "SMTP connection refused",
  "retryable": true
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `worker_id` | string | **required** | Must match claimer |
| `message` | string | **required** | Error description |
| `retryable` | boolean | `false` | Whether the error is transient |

**Response:** `200 OK`

**Retryable failure:**
- Worker task deleted (allows re-dispatch on next tick)
- Instance reset to `Scheduled`

**Permanent failure:**
- If instance has composite blocks: execution node marked `Failed`, instance re-scheduled for evaluator (try-catch can recover)
- If instance is step-only: instance marked `Failed` (enters DLQ)

---

### Heartbeat Task

```
POST /workers/tasks/{task_id}/heartbeat
```

**Request Body:**

```json
{
  "worker_id": "node-worker-42"
}
```

**Response:** `200 OK`

Send heartbeats every 15-30 seconds for long-running tasks. Tasks without a heartbeat for 60 seconds are reclaimed by the reaper and returned to the queue.

---

## Metrics

### Prometheus Metrics

```
GET /metrics
```

**Response:** `200 OK` — Prometheus text format (v0.0.4)

When `ORCH8_API_KEY` is set, the scraper must send it as `x-api-key` (and an `x-tenant-id` when `ORCH8_REQUIRE_TENANT_HEADER` is set) — the metrics endpoint is behind the same auth as the rest of the management surface.

**Counters:**

| Metric | Description |
|--------|-------------|
| `orch8_instances_claimed_total` | Instances claimed by scheduler |
| `orch8_instances_completed_total` | Instances completed successfully |
| `orch8_instances_failed_total` | Instances failed (entered DLQ) |
| `orch8_steps_executed_total` | Steps executed |
| `orch8_steps_failed_total` | Steps that failed |
| `orch8_steps_retried_total` | Retry attempts |
| `orch8_signals_delivered_total` | Signals processed |
| `orch8_rate_limits_exceeded_total` | Rate limit deferrals |
| `orch8_recovery_stale_instances_total` | Stale instances recovered at startup |
| `orch8_webhooks_sent_total` | Webhooks delivered |
| `orch8_webhooks_failed_total` | Webhook delivery failures |
| `orch8_webhooks_parked_total` | Webhooks parked in the outbox after exhausting retries |
| `orch8_cron_triggered_total` | Cron instances created |
| `orch8_cron_skipped_total` | Cron fires skipped by the `skip` overlap policy |
| `orch8_sla_breached_total` | SLA breach events (`type` label: runtime vs step runtime) |

**Histograms:**

| Metric | Description |
|--------|-------------|
| `orch8_tick_duration_seconds` | Scheduler tick latency |
| `orch8_step_duration_seconds` | Individual step execution time |
| `orch8_instance_processing_seconds` | Total instance processing time |

**Gauges:**

| Metric | Description |
|--------|-------------|
| `orch8_queue_depth` | Instances claimed in current tick |
| `orch8_active_tasks` | Currently in-flight step executions |

---

## Block Definition Reference

All blocks are defined in the `blocks` array of a sequence. Blocks can nest arbitrarily.

### Step

```json
{
  "type": "step",
  "id": "unique_block_id",
  "handler": "handler_name",
  "params": {},
  "delay": {
    "duration": 3600000,
    "business_days_only": false,
    "jitter": 300000
  },
  "retry": {
    "max_attempts": 3,
    "initial_backoff": 1000,
    "max_backoff": 60000,
    "backoff_multiplier": 2.0
  },
  "timeout": 30000,
  "rate_limit_key": "resource:identifier"
}
```

**Built-in handlers:**

| Handler | Params | Output |
|---------|--------|--------|
| `noop` | _(none)_ | `{}` |
| `log` | `message` (string), `level` ("debug"/"info"/"warn") | `{ "message": "..." }` |
| `sleep` | `duration_ms` (integer, default 100) | `{ "slept_ms": N }` |
| `fail` | `message` (string) | _(step fails with the given message)_ |
| `http_request` | `url`, `method` ("GET"/"POST"/"PUT"/"DELETE"), `body`, `timeout_ms` (default 10000) | `{ "status": 200, "body": "..." }` |
| `llm_call` | `provider`, `model`, `messages`, `temperature`, `max_tokens` | `{ "content": "...", "usage": {...} }` |
| `tool_call` | `tool_name`, `arguments` | _(tool-specific output)_ |
| `mcp_call` | `url`, `action` ("call"/"list"), `tool_name`, `arguments`, `headers`, `timeout_ms` | _(MCP tool result, or tool list for `"list"`)_ |
| `agent` | `goal` or `messages`, `system`, `tools`, `tool_dispatch`, `max_iterations` (default 6), `auto_memory`, plus `llm_call` config passthrough | `{ "final": "...", "iterations": N, "stop_reason": "completed"\|"max_iterations", "tool_calls_made": M, "messages": [...] }` |
| `embed` | `input`, plus embedding config (`model`, `api_key`/`api_key_env`, `base_url`, `timeout_ms`) | `{ "embedding"\|"embeddings", "model", "dimensions" }` |
| `memory_store` | `text`, optional `embedding`, `key`, `metadata` | `{ "key": "...", "dimensions": N }` |
| `memory_search` | `query` or `query_embedding`, optional `top_k` | `{ "results": [{ key, text, score, metadata }], "count": N }` |
| `blob_put` | `text` or `data` (base64), `content_type`, `max_size_bytes` (default 25 MiB) | _(an `ArtifactRef` to pass between steps)_ |
| `blob_get` | `ref` (ArtifactRef), `encoding` | _(the stored content)_ |
| `human_review` | `prompt`, `timeout_ms`, `escalation_handler` | `{ "approved": bool, "reviewer": "...", "comments": "..." }` |
| `self_modify` | `blocks` (array of block definitions to inject) | `{ "injected": N }` |
| `emit_event` | See [Workflow coordination handlers](#workflow-coordination-handlers) | `{ instance_id, sequence_name, deduped }` |
| `send_signal` | See [Workflow coordination handlers](#workflow-coordination-handlers) | `{ signal_id }` |
| `query_instance` | See [Workflow coordination handlers](#workflow-coordination-handlers) | `{ found, state?, context?, created_at?, updated_at? }` |
| `set_state` | `key` (string), `value` (any) | `{}` |
| `get_state` | `key` (string) | `{ "value": ... }` |
| `delete_state` | `key` (string) | `{}` |
| `merge_state` | `data` (object) | `{}` |
| `transform` | `expression` (string), `target` (string) | `{ "result": ... }` |
| `assert` | `condition` (string), `message` (string) | `{}` |

Any handler name not registered as built-in is automatically dispatched to the external worker queue.

---

### Workflow coordination handlers

Three built-in handlers let one workflow coordinate with another **within the same tenant**. Cross-tenant targets always fail with `Permanent` — the engine reads the caller's `tenant_id` from its running instance and rejects mismatches.

> **Template resolution:** step handler `params` are resolved through the template engine before dispatch. Both the in-process scheduler and the external-worker path receive already-resolved params, so `{{ context.data.target_id }}`, `{{ outputs.prev_step.id }}`, and `{{ foo | fallback }}` all work inside any handler's `params` block — including the coordination handlers below.

#### `emit_event`

Fires an event trigger → spawns a new child workflow instance.

**Params:**

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `trigger_slug` | string | yes | Slug of an enabled event trigger in the caller's tenant |
| `data` | object | no | Event payload; becomes the child's `context.data` (default `{}`) |
| `meta` | object | no | Extra metadata merged into the child's metadata. `source` and `parent_instance_id` are always set by the engine and cannot be spoofed |
| `dedupe_key` | string | no | Non-empty string. When present, dedupe is recorded atomically — subsequent calls with the same `(scope, key)` pair return the existing child and set `deduped: true`. Empty string is rejected |
| `dedupe_scope` | string | no | `"parent"` (default) scopes dedupe to `(parent_instance_id, dedupe_key)`. `"tenant"` scopes it to `(tenant_id, dedupe_key)` so different parents in the same tenant cannot both spawn a child for the same key. Only meaningful when `dedupe_key` is present |

**Returns:** `{ "instance_id": "<uuid>", "sequence_name": "<name>", "deduped": <bool> }`

**Errors:**

| Condition | Variant |
|---|---|
| Missing `trigger_slug`; `dedupe_key` empty or non-string | `Permanent` |
| Invalid `dedupe_scope` (not `"parent"` or `"tenant"`) | `Permanent` |
| Trigger not found, disabled, or in another tenant | `Permanent` |
| Caller instance not found | `Permanent` |
| Storage connection / pool / query failure | `Retryable` |
| Other storage failure | `Permanent` |

**Dedupe semantics:**
- `dedupe_scope: "parent"` (default) — key is `(parent_instance_id, dedupe_key)`. Different parent instances can reuse the same key without colliding. Use this for the common fan-out case (e.g. one parent fires-once per external event).
- `dedupe_scope: "tenant"` — key is `(tenant_id, dedupe_key)`. All parents within the tenant share the namespace, so the child workflow fires at most once per key across the whole tenant. Use this when the dedupe identity is global (e.g. a webhook delivery id you refuse to process twice across the system).

Dedupe rows are swept by the TTL sweeper (default 30 days).

**Example:**

```json
{
  "type": "step",
  "id": "fan_out_order",
  "handler": "emit_event",
  "params": {
    "trigger_slug": "on-order-created",
    "data": { "order_id": "ord_123", "total_cents": 4200 },
    "dedupe_key": "ord_123"
  }
}
```

#### `send_signal`

Enqueues a signal on an existing instance. Target must be non-terminal.

**Params:**

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `instance_id` | string (UUID) | yes | Target instance |
| `signal_type` | string \| object | yes | See variants below |
| `payload` | any | no | Arbitrary JSON delivered with the signal (default `null`) |

`signal_type` accepts all `SignalType` variants in snake_case:

- `"pause"`, `"resume"`, `"cancel"`, `"update_context"`
- custom signals use the tagged form: `{ "custom": "my_signal_name" }`

**Returns:** `{ "signal_id": "<uuid>" }`

**Errors:**

| Condition | Variant |
|---|---|
| Missing / invalid `instance_id`; missing / invalid `signal_type` | `Permanent` |
| Target not found; target in another tenant; target in terminal state | `Permanent` |
| Enqueue conflict (UUID collision; indicates a bug) | `Permanent` |
| Storage connection / pool / query failure | `Retryable` |

**Example:**

```json
{
  "type": "step",
  "id": "cancel_child",
  "handler": "send_signal",
  "params": {
    "instance_id": "0194d2e6-7f44-7e2a-9c3e-1a2b3c4d5e6f",
    "signal_type": "cancel"
  }
}
```

Custom signal:

```json
{
  "type": "step",
  "id": "notify_child",
  "handler": "send_signal",
  "params": {
    "instance_id": "0194d2e6-7f44-7e2a-9c3e-1a2b3c4d5e6f",
    "signal_type": { "custom": "approval_granted" },
    "payload": { "reviewer": "alice" }
  }
}
```

#### `query_instance`

Reads another instance's context and state.

**Params:**

| Field | Type | Required |
|-------|------|----------|
| `instance_id` | string (UUID) | yes |

**Returns (found):** `{ "found": true, "state": "<state>", "context": { ... }, "created_at": "<ts>", "updated_at": "<ts>" }`

**Returns (missing):** `{ "found": false }` — note this is NOT an error.

**Errors:**

| Condition | Variant |
|---|---|
| Missing / invalid `instance_id` | `Permanent` |
| Caller instance not found | `Permanent` |
| Target in another tenant | `Permanent` (intentional — `query_instance` errors rather than masking cross-tenant as `{ found: false }`, so a missing target and a cross-tenant target are distinguishable) |
| Storage connection / pool / query failure | `Retryable` |

**Example:**

```json
{
  "type": "step",
  "id": "read_child",
  "handler": "query_instance",
  "params": {
    "instance_id": "0194d2e6-7f44-7e2a-9c3e-1a2b3c4d5e6f"
  }
}
```

---

### Parallel

```json
{
  "type": "parallel",
  "id": "notify_all",
  "branches": [
    [{ "type": "step", "id": "email", "handler": "email_send", "params": {} }],
    [{ "type": "step", "id": "sms", "handler": "sms_send", "params": {} }],
    [{ "type": "step", "id": "push", "handler": "push_send", "params": {} }]
  ]
}
```

Branches progress independently; the block completes when all branches finish and fails if any branch fails. External-worker steps can be in flight on their queues simultaneously; in-process steps (built-in handlers) are dispatched one at a time per instance and interleave. See [SEQUENCES.md — Parallel](SEQUENCES.md#parallel).

---

### Race

```json
{
  "type": "race",
  "id": "fastest_provider",
  "branches": [
    [{ "type": "step", "id": "provider_a", "handler": "send_via_a", "params": {} }],
    [{ "type": "step", "id": "provider_b", "handler": "send_via_b", "params": {} }]
  ],
  "semantics": "first_to_succeed"
}
```

The first branch to complete **successfully** wins and the losing branches are cancelled. A failing branch does not decide the race; the block fails only if every branch fails. (The `semantics` field is accepted by the schema but not currently consulted — behavior is always first-success-wins.)

---

### Router

```json
{
  "type": "router",
  "id": "segment_users",
  "routes": [
    {
      "condition": "plan == enterprise",
      "blocks": [{ "type": "step", "id": "vip_flow", "handler": "vip_onboard", "params": {} }]
    },
    {
      "condition": "plan == pro",
      "blocks": [{ "type": "step", "id": "pro_flow", "handler": "pro_onboard", "params": {} }]
    }
  ],
  "default": [
    { "type": "step", "id": "free_flow", "handler": "free_onboard", "params": {} }
  ]
}
```

Evaluates conditions in order against `context.data`. First match wins. Falls through to `default` if none match.

**Condition syntax:** `path == value` (equality) or `path` (truthy check). Dot notation for nested paths (e.g., `user.plan == enterprise`).

---

### TryCatch

```json
{
  "type": "try_catch",
  "id": "safe_send",
  "try_block": [
    { "type": "step", "id": "primary", "handler": "smtp_send", "params": {} }
  ],
  "catch_block": [
    { "type": "step", "id": "fallback", "handler": "ses_send", "params": {} }
  ],
  "finally_block": [
    { "type": "step", "id": "log_result", "handler": "log", "params": { "message": "send attempted" } }
  ]
}
```

- **try_block:** Executes first. If all steps succeed, catch is skipped.
- **catch_block:** Executes only if try failed. If catch succeeds, the overall block succeeds.
- **finally_block:** Always executes, regardless of try/catch outcome. Optional.

---

### Loop

```json
{
  "type": "loop",
  "id": "poll_status",
  "condition": "status.pending",
  "body": [
    { "type": "step", "id": "check", "handler": "http_request", "params": { "url": "https://api.example.com/status" } }
  ],
  "max_iterations": 50
}
```

Repeats body while `condition` evaluates to truthy in `context.data`. Safety cap via `max_iterations` (default 1000). Optional `retain_iterations: N` bounds storage on long-running loops — only the N most recent body-step output rows per block are kept; older rows are compacted at each iteration boundary (unset = keep everything).

---

### ForEach

```json
{
  "type": "for_each",
  "id": "process_batch",
  "collection": "items",
  "item_var": "item",
  "body": [
    { "type": "step", "id": "process", "handler": "process_item", "params": {} }
  ],
  "max_iterations": 500
}
```

Iterates over `context.data[collection]` (must be an array). Each iteration has `item_var` available in context. Empty or missing collection completes immediately. Supports the same optional `retain_iterations: N` output-compaction bound as `Loop`.

---

### ABSplit

```json
{
  "type": "ab_split",
  "id": "experiment_onboarding",
  "variants": [
    {
      "name": "control",
      "weight": 70,
      "blocks": [
        { "type": "step", "id": "standard_flow", "handler": "standard_onboard", "params": {} }
      ]
    },
    {
      "name": "variant_a",
      "weight": 30,
      "blocks": [
        { "type": "step", "id": "new_flow", "handler": "new_onboard", "params": {} }
      ]
    }
  ]
}
```

Routes traffic to one of several variants by weight. Useful for A/B testing different workflow branches. The selected variant is recorded in the instance context for analytics.

---

### CancellationScope

```json
{
  "type": "cancellation_scope",
  "id": "critical_section",
  "blocks": [
    { "type": "step", "id": "charge", "handler": "payment_charge", "params": {} },
    { "type": "step", "id": "confirm", "handler": "send_receipt", "params": {} }
  ]
}
```

Child blocks inside a cancellation scope cannot be cancelled by external cancel signals. Provides subtree-level non-cancellability (Temporal-style structured concurrency). Use for critical sections like payment processing that must complete atomically.

---

## Mobile Sync

Mobile sync endpoints require `ORCH8_MOBILE_SYNC_ENABLED=true`. All endpoints are tenant-scoped when `X-Tenant-Id` is provided: device ownership is enforced, so syncing against, re-registering, or queueing commands for a device that belongs to another tenant is rejected.

### Sync

**`POST /mobile/sync`** — Bidirectional sync endpoint. Devices push status updates, approval requests, and step delegations; server responds with pending commands.

**Request body:**

| Field | Type | Description |
|-------|------|-------------|
| `device_id` | string | Device identifier (required) |
| `status_updates` | array | Instance status updates from device |
| `approval_requests` | array | Human-in-the-loop approval requests |
| `step_delegations` | array | Step delegation requests |
| `command_acks` | array | Command IDs the device has processed |

**Response:** `{ "commands": [...], "sync_interval_secs": 30 }`

### Register Device

**`POST /mobile/devices/register`** — Register a mobile device for push notifications.

### List Devices

**`GET /mobile/devices`** — List registered mobile devices.

### List Approvals

**`GET /mobile/approvals`** — List pending human-in-the-loop approval requests.

### Resolve Approval

**`POST /mobile/approvals/{id}/resolve`** — Resolve a pending approval request.

### List Status

**`GET /mobile/status`** — List mobile instance status reports.

### Create Command

**`POST /mobile/commands`** — Queue a command for a mobile device.

---

## Webhook Outbox

An outbound webhook that exhausts all its retries is parked in the outbox instead of being dropped, so a delivery is never silently lost. (Each parking increments the `orch8_webhooks_parked_total` metric.)

```
GET    /webhooks/outbox?limit=100        # list parked deliveries (newest first)
POST   /webhooks/outbox/{id}/redeliver   # fresh send-with-retry; removes the row on success
DELETE /webhooks/outbox/{id}             # discard a parked delivery
```

`redeliver` returns `200 { "redelivered": true }` and deletes the row when the delivery succeeds, or **502** (row kept) when it fails again. Each entry carries `url`, `event_type`, `instance_id`, the original `payload` (replayed verbatim), `attempts`, `last_error`, and `created_at`.

---

## Queue Dispatch Mode (poll / push)

By default workers poll queues. A queue can instead be configured for **push**: when a task is enqueued to it, the engine POSTs a signed task envelope to a target URL. The durable task row is still written (completion is reported the usual way).

```
POST   /queues/dispatch                       # { tenant_id, queue_name, mode: "poll"|"push", push_url?, secret? }
GET    /queues/dispatch?tenant_id=acme        # list (secrets never returned)
DELETE /queues/dispatch/{tenant_id}/{queue_name}
```

`push` requires a non-empty `push_url`. When a `secret` is set, the envelope is HMAC-signed (`X-Orch8-Timestamp` + `X-Orch8-Signature: sha256=…` over `"{ts}.{body}"`) — the same scheme as outbound webhooks. A push failure leaves the task pending; flip the queue back to `poll` to recover.

---

## Worker Version Pins

Pin a minimum worker version per `(tenant, handler)`. A worker reporting a version below the pin is given no tasks for that handler at poll time — roll a fixed build out before older workers pick up affected work.

```
POST   /workers/version-pins                        # { tenant_id, handler_name, min_version } (upsert)
GET    /workers/version-pins?tenant_id=acme         # list
DELETE /workers/version-pins/{tenant_id}/{handler_name}
```

Versions compare numerically (`1.10.0 ≥ 1.9.0`; `"2"` == `"2.0.0"`), with a lexical fallback for non-numeric schemes. A worker that reports no `version` never satisfies a pin.

---

## Worker Control Channel

Queue control commands for a specific worker. The worker polls its channel and acts on pending commands.

```
POST   /workers/commands              # { worker_id, command: "drain"|"reload"|"ping", payload? }
GET    /workers/{worker_id}/commands  # the worker's pending commands, oldest first
DELETE /workers/commands/{id}         # ack a delivered command
```

`drain` asks the worker to stop claiming new tasks and finish in-flight ones; `reload` to re-read config / re-register handlers; `ping` is a liveness probe. Commands are delivered via this dedicated channel rather than piggybacking the task-poll response, so the task-poll contract is unchanged.

---

## Queue Routing Rules

Override the queue an external-worker task lands on, per `(tenant, handler)`, evaluated at enqueue time — move a handler's traffic to a dedicated queue without redeploying the sequence.

```
POST   /routing-rules                                  # create
GET    /routing-rules?tenant_id=acme&handler_name=x    # list (filters optional)
GET    /routing-rules/{id}                             # get
DELETE /routing-rules/{id}                             # delete
```

A rule body is `{ tenant_id, handler_name, queue_override, match_queue?, priority?, enabled? }`. `match_queue` (optional) makes the rule apply only when the task's declared queue equals that value (remap X→Y); omit it to match any. The highest-`priority` enabled matching rule wins; if none match, the step's own `queue_name` is used.

---

## Additional Endpoints

### Safe workflow releases

Pin a baseline and candidate sequence version, inspect their semantic difference,
replay recorded baseline executions without side effects, and route a guarded
canary cohort.

```
POST /releases                         # create a draft
GET  /releases                         # list newest first
GET  /releases/{id}                    # current state and evidence summary
GET  /releases/{id}/diff               # semantic operational diff
GET  /releases/{id}/decisions          # immutable transition audit
POST /releases/{id}/validate           # historical effect-free replay
POST /releases/{id}/canary             # { "percent": 10 }
POST /releases/{id}/evaluate           # evaluate gates; auto-rollback on fail
POST /releases/{id}/promote            # { "force": false }
POST /releases/{id}/pause              # return traffic to baseline; resumable
POST /releases/{id}/rollback           # terminal rollback
POST /sequences/releases/diff           # compare any exact sequence IDs
```

See [Safe workflow releases](RELEASES.md) for complete CLI and curl examples,
state transitions, and denominator semantics.

### Extended route groups

The generated OpenAPI JSON at `/api-docs/openapi.json` is authoritative for
request and response schemas. The running engine also exposes these groups,
which are summarized rather than repeated field-by-field here:

- **Sessions** — Stateful multi-instance coordination
- **Pools** — Resource pool management with weighted allocation
- **Credentials** — Encrypted credential vault with OAuth2 refresh
- **Triggers** — Webhook and event-driven instance creation
- **Circuit Breakers** — Per-tenant/handler state, manual reset
- **Plugins** — WASM and gRPC plugin registration
- **Approvals** — Human-in-the-loop approval inbox
- **Cluster** — Node listing, heartbeat, drain
- **Webhooks** — Webhook subscription management
- **Telemetry** — Execution telemetry and rollback history
- **API Keys** — Per-tenant API key create/list/revoke (`/api-keys`)
- **Sequence lifecycle** — List, delete, versions, promote/deprecate/unpublish, status, and `POST /sequences/migrate-instance`
- **Execution introspection** — `GET /instances/{id}/tree` (execution tree), `/timeline`, `/audit`, `/artifacts`, and SSE streaming at `GET /instances/{id}/stream`
- **Checkpoints & recovery** — `GET|POST /instances/{id}/checkpoints`, `/checkpoints/latest`, `/checkpoints/prune`, `POST /instances/{id}/fork`, `POST /instances/{id}/resume-from/{block_id}`, `POST /instances/{id}/inject-blocks`
- **Bulk reschedule** — `PATCH /instances/bulk/reschedule` (shift `next_fire_at` by `offset_secs`)
- **Rollback policies** — `/rollback-policies` CRUD
- **Usage & info** — `GET /usage`, `GET /info` (version + environment label)
- **MCP server** — `POST /mcp` (expose the engine itself as an MCP server)
- **Safe releases** — `/releases` diff, validation, canary gates, decisions, promotion, pause, and rollback

---

## Error Responses

All error responses follow this format:

```json
{
  "error": "Human-readable error message"
}
```

| Status Code | Meaning |
|-------------|---------|
| `400` | Bad request (invalid JSON, missing required field, invalid argument) |
| `401` | Unauthorized (missing or invalid API key) |
| `403` | Forbidden (insufficient permissions) |
| `404` | Resource not found (instance, sequence, cron schedule, worker task) |
| `409` | Conflict (state transition not allowed, duplicate resource) |
| `413` | Payload too large (context exceeds `max_context_bytes`) |
| `422` | Unprocessable entity (`context.data` fails the sequence's `input_schema`) |
| `500` | Internal server error |
| `503` | Service unavailable (storage connection failure) |
