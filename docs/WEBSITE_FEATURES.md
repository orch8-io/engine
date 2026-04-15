# Orch8 — Website Feature Copy & Technical Details

> Copy blocks, feature descriptions, and technical specs ready for website transfer.

---

## Hero Section

### Headline
**Durable workflow orchestration. Built in Rust. Runs anywhere.**

### Subheadline
Define multi-step workflows as composable JSON. Orch8 guarantees every step either completes, retries, or surfaces for review. No lost work. No silent failures.

### Key Stats (above the fold)
- **7 composable block types** — step, parallel, race, router, try-catch, loop, forEach
- **100ms tick latency** — scheduler processes up to 256 instances per tick
- **1 binary, 1 dependency** — single Rust binary + PostgreSQL. No Redis. No Kafka. No queue infrastructure.
- **Any language** — write handlers in Node.js, Python, Go, or any language via the pull-based worker API

---

## Feature Sections

### 1. Workflow DSL — 7 Block Types

**Section title:** Build Any Workflow with 7 Composable Blocks

**Body:** Orch8's workflow DSL is a recursive JSON structure. Seven block types compose freely — nest a try-catch inside a parallel branch, loop over a collection of router decisions. The engine handles execution ordering, error propagation, and state management.

#### Step — The Building Block

The atomic unit of work. Executes a handler function — built-in (HTTP request, logging) or external (your code in any language).

```json
{
  "type": "step",
  "id": "send_welcome_email",
  "handler": "email_send",
  "params": { "template": "welcome", "to": "{{context.data.email}}" },
  "retry": { "max_attempts": 3, "initial_backoff": "1s", "max_backoff": "60s" },
  "timeout": "30s",
  "rate_limit_key": "mailbox:{{context.data.email}}"
}
```

**Capabilities per step:**
- Automatic retry with exponential backoff (configurable attempts, backoff, multiplier)
- Per-step timeout enforcement
- Per-resource rate limiting (sliding window)
- Delay before execution (with optional jitter and business-days-only mode)
- Output memoization (idempotent re-execution on retry)

---

#### Parallel — Run Branches Concurrently

All branches start simultaneously. The block completes when every branch finishes. If any branch fails, the entire parallel block fails.

```json
{
  "type": "parallel",
  "id": "notify_all_channels",
  "branches": [
    [{ "type": "step", "id": "email",  "handler": "email_send",  "params": {} }],
    [{ "type": "step", "id": "sms",    "handler": "sms_send",    "params": {} }],
    [{ "type": "step", "id": "slack",  "handler": "slack_send",  "params": {} }]
  ]
}
```

**Use cases:**
- Send notifications across multiple channels at once
- Fan-out to N API calls in parallel
- Run independent data enrichment steps simultaneously

**Behavior:**
| Scenario | Result |
|----------|--------|
| All 3 branches succeed | Parallel block completes |
| Email fails, SMS + Slack succeed | Parallel block fails |
| 1 branch has 5 steps, others have 1 | Waits for the longest branch |

---

#### Race — First to Finish Wins

All branches start simultaneously. The first branch to complete wins. Remaining branches are cancelled.

```json
{
  "type": "race",
  "id": "fastest_delivery",
  "branches": [
    [{ "type": "step", "id": "provider_a", "handler": "send_via_sendgrid", "params": {} }],
    [{ "type": "step", "id": "provider_b", "handler": "send_via_ses",      "params": {} }]
  ],
  "semantics": "first_to_succeed"
}
```

**Two race semantics:**

| Mode | Behavior |
|------|----------|
| `first_to_resolve` (default) | First branch to finish — success or failure — wins |
| `first_to_succeed` | Only successful completions count. Failures are ignored until all branches fail. |

**Use cases:**
- Competitive provider failover (try all, take the fastest)
- Timeout racing (race an API call against a sleep timer)
- Redundant execution for critical steps

**Behavior:**
| Scenario (first_to_succeed) | Result |
|------------------------------|--------|
| Provider A returns in 200ms, B in 500ms | A wins, B cancelled, block succeeds |
| Provider A fails, B succeeds in 500ms | B wins, block succeeds |
| Both fail | Race block fails |

---

#### Router — Conditional Branching

Evaluate conditions against execution context. Execute the first matching route. Skip all others.

```json
{
  "type": "router",
  "id": "segment_response",
  "routes": [
    {
      "condition": "plan == enterprise",
      "blocks": [{ "type": "step", "id": "vip", "handler": "vip_onboard", "params": {} }]
    },
    {
      "condition": "plan == pro",
      "blocks": [{ "type": "step", "id": "pro", "handler": "pro_onboard", "params": {} }]
    }
  ],
  "default": [
    { "type": "step", "id": "free", "handler": "free_onboard", "params": {} }
  ]
}
```

**Condition syntax:**
- Equality: `field == value` (checks `context.data.field`)
- Nested paths: `user.plan == enterprise`
- Truthy check: `user.is_active` (true if exists and non-null/non-false/non-empty)

**Behavior:**
| context.data.plan | Executed route | Skipped routes |
|-------------------|----------------|----------------|
| `"enterprise"` | VIP onboard | Pro, Free |
| `"pro"` | Pro onboard | VIP, Free |
| `"free"` | Free onboard (default) | VIP, Pro |
| `"starter"` | Free onboard (default) | VIP, Pro |

**Use cases:**
- User segmentation (different flows per plan, region, or behavior)
- A/B test routing
- Conditional follow-up based on prior step output
- Feature flagging within workflows

---

#### TryCatch — Error Recovery

Execute a try block. If it fails, execute a catch block to recover. An optional finally block always runs.

```json
{
  "type": "try_catch",
  "id": "resilient_send",
  "try_block": [
    { "type": "step", "id": "primary", "handler": "smtp_send", "params": {} }
  ],
  "catch_block": [
    { "type": "step", "id": "fallback", "handler": "ses_send", "params": {} }
  ],
  "finally_block": [
    { "type": "step", "id": "audit", "handler": "log", "params": { "message": "send attempted" } }
  ]
}
```

**Three phases:**

| Phase | Runs when | Skipped when |
|-------|-----------|--------------|
| **try** | Always | Never |
| **catch** | Try failed | Try succeeded |
| **finally** | Always | Never |

**Final block state:**

| Try result | Catch result | Block result |
|-----------|--------------|--------------|
| Success | _(skipped)_ | Success |
| Failure | Success | Success (recovered) |
| Failure | Failure | Failure |
| Success | _(skipped)_ | Success (finally runs either way) |

**Use cases:**
- Primary/fallback provider switching
- Cleanup operations (finally) regardless of success/failure
- Graceful degradation — try the ideal path, catch with an acceptable alternative
- Error logging and alerting in the catch block

**Nesting:** Try, catch, and finally blocks can contain any block type — including other try-catch blocks, parallel blocks, or routers.

---

#### Loop — Conditional Repetition

Repeat the body while a condition evaluates to true.

```json
{
  "type": "loop",
  "id": "poll_until_ready",
  "condition": "status.pending",
  "body": [
    { "type": "step", "id": "check_status", "handler": "http_request", "params": {
      "url": "https://api.example.com/job/123/status"
    }}
  ],
  "max_iterations": 60
}
```

**Properties:**
- **condition:** Evaluated against `context.data` at the start of each iteration
- **max_iterations:** Safety cap (default: 1000) to prevent runaway loops
- Body can contain any block type (steps, parallel, routers, etc.)

**Use cases:**
- Poll external API until a job completes
- Retry a complex multi-step operation until success
- Process items one at a time with condition-based termination

---

#### ForEach — Collection Iteration

Iterate over an array from execution context.

```json
{
  "type": "for_each",
  "id": "process_recipients",
  "collection": "recipients",
  "item_var": "recipient",
  "body": [
    { "type": "step", "id": "send", "handler": "email_send", "params": {
      "to": "{{recipient.email}}",
      "template": "campaign"
    }}
  ],
  "max_iterations": 10000
}
```

**Properties:**
- **collection:** Dot-path to array in `context.data` (e.g., `"users"`, `"data.items"`)
- **item_var:** Variable name for current item (default: `"item"`)
- **max_iterations:** Safety cap (default: 1000)
- Empty or missing collection completes immediately (no error)
- If any iteration fails, the forEach block fails

**Use cases:**
- Process each item in a batch
- Send to a list of recipients
- Fan-out work across a dynamic set of resources

---

### Composability — Blocks Inside Blocks

Every composite block's branches/routes/body can contain any other block type. This enables patterns like:

**Try-catch inside parallel:**
```json
{
  "type": "parallel",
  "branches": [
    [{
      "type": "try_catch",
      "try_block": [{ "type": "step", "handler": "risky_api" }],
      "catch_block": [{ "type": "step", "handler": "fallback_api" }]
    }],
    [{ "type": "step", "handler": "always_works" }]
  ]
}
```

**Router inside forEach:**
```json
{
  "type": "for_each",
  "collection": "contacts",
  "body": [{
    "type": "router",
    "routes": [
      { "condition": "item.channel == email", "blocks": [{ "type": "step", "handler": "email_send" }] },
      { "condition": "item.channel == sms", "blocks": [{ "type": "step", "handler": "sms_send" }] }
    ]
  }]
}
```

---

### 2. Durable Execution

**Section title:** Crash-Proof by Design

**Body:** Orch8 uses snapshot-based state persistence — not event sourcing. Every state transition is an atomic database update. If the engine crashes mid-execution, stale instances are automatically recovered on restart.

**Key properties:**
- **Atomic claims** — `FOR UPDATE SKIP LOCKED` prevents double-processing across nodes
- **Output memoization** — step outputs are persisted. Re-execution returns the cached result.
- **Crash recovery** — instances stuck in Running for >5 minutes are automatically reset to Scheduled
- **No replay** — current state is truth. No event log to replay, no saga compensation.

---

### 3. External Worker System

**Section title:** Write Handlers in Any Language

**Body:** Any step handler not registered as a built-in is automatically dispatched to the external worker queue. Workers poll for tasks via REST, execute them in any language, and report results back.

**Flow:**
1. Engine encounters unknown handler name → creates worker task
2. Worker polls: `POST /workers/tasks/poll { handler_name: "email_send" }`
3. Worker executes, sends heartbeats for long-running tasks
4. Worker reports: `POST /workers/tasks/{id}/complete { output: {...} }`
5. Engine resumes workflow from where it left off

**Properties:**
- **Pull-based** — no message broker needed. Workers poll at their own pace.
- **At-least-once** — heartbeat timeout (60s) + reaper (30s interval) ensures no stuck tasks
- **Error classification** — workers declare retryable vs permanent failures
- **Concurrent workers** — `SKIP LOCKED` prevents double-claiming

---

### 4. Concurrency & Rate Limiting

**Section title:** Built-in Concurrency Control

**Concurrency key:** Limit how many instances with the same key run simultaneously.
```json
{ "concurrency_key": "contact:john@acme.com", "max_concurrency": 1 }
```
Prevents running 2 campaigns for the same contact at once.

**Idempotency key:** Prevent duplicate instance creation.
```json
{ "idempotency_key": "signup:user-12345:2024-01-15" }
```
Same key returns the existing instance ID — no duplicate created.

**Rate limiting:** Per-resource sliding window.
```
resource_key: "mailbox:john@acme.com"
max_count: 30
window_seconds: 86400
```
Max 30 emails per mailbox per day. Overages are deferred, not failed.

---

### 5. Cron Scheduling

**Section title:** Scheduled Workflows with Timezone Support

**Body:** Create recurring workflows with standard cron expressions. Full timezone support — schedule "every weekday at 9 AM Eastern" and it fires correctly across DST transitions.

```json
{
  "cron_expr": "0 9 * * MON-FRI *",
  "timezone": "America/New_York",
  "sequence_id": "daily-report",
  "enabled": true
}
```

CRUD API for managing schedules. Enable/disable without deleting. Metadata passed through to created instances.

---

### 6. Observability

**Section title:** Prometheus Metrics + Webhooks + DLQ

- **12 counters** — instances claimed/completed/failed, steps executed/retried, webhooks sent, cron triggered
- **3 histograms** — tick duration, step duration, instance processing time
- **2 gauges** — queue depth, active tasks
- **Webhook events** — instance.started, instance.completed, instance.failed
- **Dead Letter Queue** — `GET /instances/dlq` surfaces all failed instances for manual review or retry

---

### 7. Multi-Tenant & Signals

**Section title:** Multi-Tenant with Mid-Execution Control

- **tenant_id + namespace** — every resource is scoped. Full isolation between tenants.
- **Signals** — send pause, resume, cancel, or custom signals to running instances
- **Bulk operations** — cancel or pause thousands of instances with one API call
- **Execution context** — structured, multi-section state (data, config, audit, runtime) travels with each instance

---

## Technical Specs (for website footer/specs page)

| Spec | Value |
|------|-------|
| Language | Rust (2024 edition) |
| Runtime | Tokio async |
| HTTP framework | Axum |
| Database | PostgreSQL 14+ (only dependency) |
| Connection pooling | sqlx (max 64 connections default) |
| Deployment | Single binary |
| Container ready | Yes (health checks at /health/live and /health/ready) |
| Metrics format | Prometheus text (v0.0.4) |
| API format | REST + JSON |
| Auth | API key middleware (configurable) |
| Scheduler model | Pull-based polling (configurable tick interval) |
| State persistence | Snapshot-based (no event sourcing) |
| Crash recovery | Automatic (stale instance detection at startup) |
| License | — |

---

## Comparison Points (for marketing)

| Feature | Orch8 | Temporal | Inngest | Trigger.dev |
|---------|-------|----------|---------|-------------|
| Self-hosted | Yes | Yes | Yes | Yes |
| Single binary | Yes | No (needs Cassandra/MySQL + Elasticsearch) | No (needs multiple services) | No |
| Workflow definition | JSON DSL | Code (Go/Java/TS/Python) | Code (TypeScript) | Code (TypeScript) |
| External workers | Pull-based REST | Worker framework | Event-driven | SDK-based |
| Composite blocks | 7 types | Via code | Limited | Limited |
| Try-catch/finally | Native block | Via code | Via middleware | Via code |
| Competitive concurrency (Race) | Native block | Via code | Not native | Not native |
| Rate limiting | Built-in per-resource | Custom | Built-in | Custom |
| Database dependency | PostgreSQL only | Cassandra/MySQL + ES | PostgreSQL + Redis | PostgreSQL + Redis |
| Language | Rust | Go | TypeScript | TypeScript |
