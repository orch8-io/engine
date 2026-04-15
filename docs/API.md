# Orch8 Engine — API Reference

Base URL: `http://localhost:8080` (configurable via `ORCH8_HTTP_ADDR`)

All request/response bodies are JSON. Dates use ISO 8601 / RFC 3339 format.

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

Returns 200 if the database is reachable.

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
        "initial_backoff": "1s",
        "max_backoff": "60s",
        "backoff_multiplier": 2.0
      },
      "timeout": "30s"
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
  "created_at": "2024-01-15T10:00:00Z"
}
```

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

**Response:** `200 OK` — array of `TaskInstance` objects.

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

**Cron expression format (7 fields):**

```
second  minute  hour  day  month  day_of_week  year
  0       9      *     *     *      MON-FRI      *
```

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
  "metadata": { "type": "weekly-digest" }
}
```

**Response:** `200 OK` — updated `CronSchedule`.

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
| `orch8_cron_triggered_total` | Cron instances created |

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
    "duration": "3600s",
    "business_days_only": false,
    "jitter": "300s"
  },
  "retry": {
    "max_attempts": 3,
    "initial_backoff": "1s",
    "max_backoff": "60s",
    "backoff_multiplier": 2.0
  },
  "timeout": "30s",
  "rate_limit_key": "resource:identifier"
}
```

**Built-in handlers:**

| Handler | Params | Output |
|---------|--------|--------|
| `noop` | _(none)_ | `{}` |
| `log` | `message` (string), `level` ("debug"/"info"/"warn") | `{ "message": "..." }` |
| `sleep` | `duration_ms` (integer, default 100) | `{ "slept_ms": N }` |
| `http_request` | `url`, `method` ("GET"/"POST"/"PUT"/"DELETE"), `body`, `timeout_ms` (default 10000) | `{ "status": 200, "body": "..." }` |

Any handler name not registered as built-in is automatically dispatched to the external worker queue.

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

All branches run concurrently. Completes when all finish. Fails if any branch fails.

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

| Semantics | Behavior |
|-----------|----------|
| `first_to_resolve` (default) | First branch to complete (success or failure) wins |
| `first_to_succeed` | First successful branch wins; failures ignored until all fail |

Losing branches are cancelled.

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

Repeats body while `condition` evaluates to truthy in `context.data`. Safety cap via `max_iterations` (default 1000).

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

Iterates over `context.data[collection]` (must be an array). Each iteration has `item_var` available in context. Empty or missing collection completes immediately.

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
| `400` | Bad request (invalid JSON, missing required field, invalid state transition) |
| `404` | Resource not found (instance, sequence, cron schedule, worker task) |
| `409` | Conflict (state transition not allowed from current state) |
| `500` | Internal server error |
