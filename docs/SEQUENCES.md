# Sequences: Build, Publish, Trigger, and Extend

Sequences are Orch8's portable workflow format. A sequence is JSON: it names the workflow, versions it, and describes the blocks Orch8 should run. You publish that JSON once, then start instances manually, from triggers, from webhooks, from cron, or from another sequence.

This document is optimized for fast adoption and for LLMs that need to generate valid sequences.

## Mental Model

Use these building blocks:

| Concept | What it is | Example |
|---|---|---|
| Sequence | Versioned JSON workflow definition | `customer-onboarding` v3 |
| Block | One unit of workflow structure | `step`, `router`, `parallel`, `try_catch` |
| Step | Calls a handler | `send_email`, `llm_call`, `wasm://normalize` |
| Handler | Built-in, worker, plugin, or integration | `http_request`, `order_charge`, `grpc://risk-score` |
| Instance | One running execution of a sequence | onboarding for `user_123` |
| Trigger | Named entry point that creates instances | `POST /triggers/signup/fire` |
| Plugin | Packaged handler code | WASM module or gRPC sidecar |

Choose the simplest execution path that works:

1. Built-in handler: for common actions such as `noop`, `log`, `sleep`, `http_request`, `llm_call`, `tool_call`, human review, state, events, and instance queries.
2. External worker: for app-specific business logic in your service language.
3. Plugin: for reusable packaged handlers that should be invoked as `wasm://...` or `grpc://...`.
4. Trigger: for anything outside Orch8 that should start a sequence.

## Create a Sequence

A minimal sequence:

```json
{
  "id": "018f85d4-9c2b-7f5f-a2a5-f4143fa7d001",
  "tenant_id": "demo",
  "namespace": "default",
  "name": "hello-world",
  "version": 1,
  "deprecated": false,
  "blocks": [
    {
      "type": "step",
      "id": "greet",
      "handler": "log",
      "params": {
        "message": "Hello {{context.data.name}}"
      }
    }
  ],
  "created_at": "2026-05-05T12:00:00Z"
}
```

Publish it:

```bash
curl -s -X POST "$ORCH8_URL/sequences" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-Id: demo" \
  -d @sequence.json
```

Start one instance:

```bash
curl -s -X POST "$ORCH8_URL/instances" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-Id: demo" \
  -d '{
    "sequence_id": "018f85d4-9c2b-7f5f-a2a5-f4143fa7d001",
    "tenant_id": "demo",
    "namespace": "default",
    "context": {
      "data": { "name": "Alice" }
    },
    "idempotency_key": "hello-world:alice:v1"
  }'
```

## JSON Contract for LLMs

When generating a sequence, emit one complete JSON object.

Required top-level fields:

| Field | Rule |
|---|---|
| `id` | UUID string. Use a new ID for each published version. |
| `tenant_id` | Tenant that owns this sequence. |
| `namespace` | Usually `default`; use namespaces for app domains or environments. |
| `name` | Stable workflow name, e.g. `invoice-collection`. |
| `version` | Integer. Increment when workflow behavior changes. |
| `deprecated` | Boolean. Use `false` for active versions. |
| `blocks` | Non-empty array of blocks. Every block ID must be globally unique inside the sequence. |
| `created_at` | RFC 3339 timestamp string. |

Optional top-level fields:

| Field | Use |
|---|---|
| `input_schema` | JSON Schema validated against `context.data` at instance create. A create whose data fails validation is rejected with HTTP 422. Also drives the dashboard's run form. |
| `sla` | Alert-only SLA policy: `{ "max_runtime": ms, "max_step_runtime": ms }`. A breach fires an `instance.sla_breached` webhook and a metric; the instance keeps running. |
| `on_failure` | Best-effort cleanup blocks run when the instance reaches terminal `Failed`. Errors are swallowed. |
| `on_cancel` | Same as `on_failure`, for terminal `Cancelled`. |

Validation limits (enforced at publish time):

- Block nesting depth: at most 64.
- `parallel`/`race` branches: at most 256 per block.
- `loop`/`for_each` `max_iterations`: at most 100,000 (default 1,000 when omitted).
- Total blocks per sequence (root + nested): at most 5,000.

Valid block types:

```text
step
parallel
race
loop
for_each
router
try_catch
sub_sequence
ab_split
cancellation_scope
```

Generation rules:

- Use only JSON. No comments, YAML, markdown, or pseudo-code.
- Use snake_case fields exactly as shown in this guide.
- Every block needs a unique `id`.
- Use integer milliseconds for durations: `1000` means one second.
- Put per-run data in instance `context.data`.
- Put tenant/app configuration in instance `context.config`.
- Put static handler settings in block `params`.
- Use `router.routes[].condition` plus `router.routes[].blocks`; do not invent `if`, `then`, or `else`.
- Use `parallel.branches` and `race.branches` as arrays of block arrays.
- If a handler is not built in, make sure an external worker or plugin exists for it.

## Blocks

### Step

Runs one handler.

```json
{
  "type": "step",
  "id": "send_welcome_email",
  "handler": "send_email",
  "params": {
    "template": "welcome",
    "to": "{{context.data.email}}"
  },
  "retry": {
    "max_attempts": 3,
    "initial_backoff": 1000,
    "max_backoff": 60000,
    "backoff_multiplier": 2.0
  },
  "timeout": 30000
}
```

Common step fields:

| Field | Use |
|---|---|
| `handler` | Built-in, external worker name, plugin handler, or integration handler. |
| `params` | Static JSON config passed to the handler. |
| `retry` | Automatic retry/backoff for failures. |
| `timeout` | Max runtime in milliseconds. A timed-out attempt is a retryable failure: `retry` applies, and a surrounding `try_catch` can catch it. |
| `delay` | Durable wait before running the step. |
| `rate_limit_key` | Throttle this step using a named rate limit. |
| `send_window` | Run only during specific local hours/days. |
| `wait_for_input` | Pause for human input. |
| `queue_name` | Route external worker work to a named queue. |
| `deadline` | SLA duration in milliseconds. |
| `on_deadline_breach` | Handler to run when the SLA is breached. |
| `fallback_handler` | Alternate handler when circuit breaker is open. |
| `cache_key` | Cache step output by template-resolved key. |
| `context_access` | Restrict which context sections/fields this step can read, e.g. `{ "data": { "fields": ["user_id"] } }`. |
| `cancellable` | Set `false` to let this step finish even if the instance is cancelled. Default `true`. |

### Parallel

Run independent branches and continue when all complete.

```json
{
  "type": "parallel",
  "id": "order_preflight",
  "branches": [
    [
      { "type": "step", "id": "validate_order", "handler": "order_validate", "params": {} }
    ],
    [
      { "type": "step", "id": "charge_card", "handler": "order_charge", "params": {} }
    ],
    [
      { "type": "step", "id": "reserve_inventory", "handler": "order_reserve", "params": {} }
    ]
  ]
}
```

Use for independent work whose results are all needed before the sequence continues.

Execution semantics: branches progress independently and the block completes when all branches complete. Steps handled by external workers can be in flight on their queues simultaneously; steps that run in-process (built-in handlers such as `http_request` or `llm_call`) are dispatched one at a time per instance, so in-process branches interleave rather than run concurrently.

### Race

Run competing branches and keep the first result.

```json
{
  "type": "race",
  "id": "fetch_from_fastest_provider",
  "semantics": "first_to_succeed",
  "branches": [
    [
      { "type": "step", "id": "fetch_primary", "handler": "primary_api", "params": {} }
    ],
    [
      { "type": "step", "id": "fetch_backup", "handler": "backup_api", "params": {} }
    ]
  ]
}
```

Use for provider failover, lowest-latency fetches, or redundant AI/tool calls.

Winner semantics: the first branch to complete successfully wins and the remaining branches are cancelled (including any in-flight external worker tasks). A branch that fails does not decide the race; the race block fails only if every branch fails.

### Router

Choose the first matching route.

```json
{
  "type": "router",
  "id": "route_by_plan",
  "routes": [
    {
      "condition": "context.data.plan == \"enterprise\"",
      "blocks": [
        { "type": "step", "id": "assign_csm", "handler": "assign_csm", "params": {} }
      ]
    },
    {
      "condition": "context.data.plan == \"trial\"",
      "blocks": [
        { "type": "step", "id": "send_trial_tips", "handler": "send_email", "params": { "template": "trial_tips" } }
      ]
    }
  ],
  "default": [
    { "type": "step", "id": "send_standard_tips", "handler": "send_email", "params": { "template": "standard_tips" } }
  ]
}
```

Use for segmentation, approvals, error paths, and business rules.

### TryCatch

Recover from failures.

```json
{
  "type": "try_catch",
  "id": "charge_with_backup",
  "try_block": [
    { "type": "step", "id": "charge_primary", "handler": "stripe_charge", "params": {} }
  ],
  "catch_block": [
    { "type": "step", "id": "charge_backup", "handler": "braintree_charge", "params": {} }
  ],
  "finally_block": [
    { "type": "step", "id": "record_payment_attempt", "handler": "log", "params": { "event": "payment_attempted" } }
  ]
}
```

Use when a failure should not always fail the whole workflow.

### Loop

Repeat while a condition is true.

```json
{
  "type": "loop",
  "id": "agent_loop",
  "condition": "context.data.done != true",
  "max_iterations": 10,
  "body": [
    { "type": "step", "id": "think", "handler": "llm_call", "params": { "provider": "openai", "model": "gpt-4o-mini" } },
    { "type": "step", "id": "act", "handler": "tool_call", "params": {} }
  ]
}
```

Always set a practical `max_iterations` for agent loops. It defaults to 1000 and may not exceed 100,000.

For long-running loops, set `retain_iterations: N` to keep only the most recent N iterations' step outputs; older outputs are deleted at each iteration boundary so storage stays bounded. Omit it to retain everything. (`for_each` supports the same field.)

### ForEach

Process each item in a collection.

```json
{
  "type": "for_each",
  "id": "process_line_items",
  "collection": "context.data.items",
  "item_var": "item",
  "max_iterations": 500,
  "body": [
    { "type": "step", "id": "process_item", "handler": "process_item", "params": { "item": "{{item}}" } }
  ]
}
```

Use for batch processing, fan-out, enrichment, and per-record validation.

### SubSequence

Call another published sequence and wait for it.

```json
{
  "type": "sub_sequence",
  "id": "run_risk_review",
  "sequence_name": "risk-review",
  "version": 2,
  "input": {
    "customer_id": "{{context.data.customer_id}}",
    "amount_cents": "{{context.data.amount_cents}}"
  }
}
```

Use for reusable workflow components. Omit `version` to call the latest non-deprecated version.

Sub-sequences may nest at most 16 levels deep; a spawn beyond that fails the step rather than creating the child. The parent's execution budget (if any) is propagated to the child.

### ABSplit

Send traffic to weighted variants.

```json
{
  "type": "ab_split",
  "id": "email_subject_test",
  "variants": [
    {
      "name": "control",
      "weight": 70,
      "blocks": [
        { "type": "step", "id": "send_control_subject", "handler": "send_email", "params": { "subject": "Welcome" } }
      ]
    },
    {
      "name": "variant_a",
      "weight": 30,
      "blocks": [
        { "type": "step", "id": "send_variant_subject", "handler": "send_email", "params": { "subject": "Your workspace is ready" } }
      ]
    }
  ]
}
```

Use for experiments where the chosen variant must remain stable for the instance.

### CancellationScope

Protect critical blocks from cancellation until they complete.

```json
{
  "type": "cancellation_scope",
  "id": "commit_payment",
  "blocks": [
    { "type": "step", "id": "capture_payment", "handler": "capture_payment", "params": {} },
    { "type": "step", "id": "write_ledger", "handler": "write_ledger", "params": {} }
  ]
}
```

Use for cleanup, ledger writes, unlocks, and other consistency-sensitive work.

## Scheduling

Durable delay:

```json
{
  "type": "step",
  "id": "wait_three_days",
  "handler": "noop",
  "params": {},
  "delay": {
    "duration": 259200000,
    "business_days_only": true,
    "jitter": 1800000,
    "holidays": ["2026-05-25"],
    "timezone": "America/New_York"
  }
}
```

Send window:

```json
{
  "type": "step",
  "id": "send_during_business_hours",
  "handler": "send_email",
  "params": {},
  "send_window": {
    "start_hour": 9,
    "end_hour": 17,
    "days": [0, 1, 2, 3, 4]
  }
}
```

Rules:

- Durations are integer milliseconds.
- `business_days_only` skips weekends and configured holidays.
- `holidays` uses `YYYY-MM-DD` and is merged with `context.config.holidays`.
- `send_window.days` uses `0=Monday` through `6=Sunday`.
- `fire_at_local` can target a local wall-clock timestamp, e.g. `2026-03-08T02:30:00`.

## Human Review

Use `human_review` plus `wait_for_input`.

```json
{
  "type": "step",
  "id": "manager_approval",
  "handler": "human_review",
  "params": {
    "instructions": "Approve or reject this discount request",
    "reviewer": "sales-manager"
  },
  "wait_for_input": {
    "prompt": "Approve discount?",
    "timeout": 3600000,
    "choices": [
      { "label": "Approve", "value": "approved" },
      { "label": "Reject", "value": "rejected" }
    ],
    "store_as": "discount_decision"
  }
}
```

Then route on the decision:

```json
{
  "type": "router",
  "id": "route_discount_decision",
  "routes": [
    {
      "condition": "context.data.discount_decision == \"approved\"",
      "blocks": [
        { "type": "step", "id": "apply_discount", "handler": "apply_discount", "params": {} }
      ]
    }
  ],
  "default": [
    { "type": "step", "id": "notify_rejected", "handler": "send_email", "params": { "template": "discount_rejected" } }
  ]
}
```

Use human review for approvals, compliance gates, manual QA, escalations, and human-in-the-loop agent workflows.

## Publish and Version

Create version 1:

```bash
curl -s -X POST "$ORCH8_URL/sequences" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-Id: acme" \
  -d @customer-onboarding.v1.json
```

Create version 2 by keeping the same `tenant_id`, `namespace`, and `name`, then changing:

- `id`: new UUID
- `version`: higher integer
- `blocks`: updated workflow
- `created_at`: new timestamp

Find latest active version:

```bash
curl -s "$ORCH8_URL/sequences/by-name?tenant_id=acme&namespace=default&name=customer-onboarding" \
  -H "X-Tenant-Id: acme"
```

Find a specific version:

```bash
curl -s "$ORCH8_URL/sequences/by-name?tenant_id=acme&namespace=default&name=customer-onboarding&version=2" \
  -H "X-Tenant-Id: acme"
```

List all versions:

```bash
curl -s "$ORCH8_URL/sequences/versions?tenant_id=acme&namespace=default&name=customer-onboarding" \
  -H "X-Tenant-Id: acme"
```

Deprecate an old version:

```bash
curl -s -X POST "$ORCH8_URL/sequences/$OLD_SEQUENCE_ID/deprecate" \
  -H "X-Tenant-Id: acme"
```

Deprecation stops latest-by-name lookup from selecting that version. Existing instances keep their sequence binding.

Migrate a non-terminal instance to a new sequence version:

```bash
curl -s -X POST "$ORCH8_URL/sequences/migrate-instance" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-Id: acme" \
  -d '{
    "instance_id": "018f85d4-9c2b-7f5f-a2a5-f4143fa7d010",
    "target_sequence_id": "018f85d4-9c2b-7f5f-a2a5-f4143fa7d020"
  }'
```

## Use Triggers

Triggers are named entry points that create instances from a sequence name. The request body becomes the new instance's `context.data`.

Create an internal event trigger:

```bash
curl -s -X POST "$ORCH8_URL/triggers" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-Id: acme" \
  -d '{
    "slug": "signup-created",
    "sequence_name": "customer-onboarding",
    "tenant_id": "acme",
    "namespace": "default",
    "trigger_type": "event"
  }'
```

Fire it from a trusted service:

```bash
curl -s -X POST "$ORCH8_URL/triggers/signup-created/fire" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-Id: acme" \
  -d '{
    "user_id": "user_123",
    "email": "alice@example.com",
    "plan": "trial"
  }'
```

Response:

```json
{
  "instance_id": "018f85d4-9c2b-7f5f-a2a5-f4143fa7d300",
  "trigger": "signup-created",
  "sequence_name": "customer-onboarding"
}
```

Create a public webhook trigger:

```bash
curl -s -X POST "$ORCH8_URL/triggers" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-Id: acme" \
  -d '{
    "slug": "stripe-payment-succeeded",
    "sequence_name": "payment-postprocess",
    "tenant_id": "acme",
    "namespace": "default",
    "trigger_type": "webhook",
    "secret": "replace-with-long-random-secret"
  }'
```

Receive third-party events at:

```http
POST /webhooks/stripe-payment-succeeded
x-trigger-secret: replace-with-long-random-secret
x-trigger-timestamp: 1777982400
x-trigger-nonce: unique-random-id
Content-Type: application/json
```

Webhook notes:

- Public webhooks require `trigger_type: "webhook"`.
- Public webhooks require a `secret`.
- Public webhooks require replay protection headers: `x-trigger-timestamp` and `x-trigger-nonce`.
- Body size is limited to 1 MB.
- Use `/triggers/{slug}/fire` for trusted internal service calls.
- Use `/webhooks/{slug}` for public inbound third-party calls.

Trigger management:

```bash
curl -s "$ORCH8_URL/triggers?tenant_id=acme" -H "X-Tenant-Id: acme"
curl -s "$ORCH8_URL/triggers/signup-created" -H "X-Tenant-Id: acme"
curl -s -X DELETE "$ORCH8_URL/triggers/signup-created" -H "X-Tenant-Id: acme"
```

## Use External Workers

If a step handler name is not built in and is not a plugin handler, Orch8 queues it for an external worker.

Sequence step:

```json
{
  "type": "step",
  "id": "charge_customer",
  "handler": "billing_charge",
  "params": {
    "currency": "USD"
  },
  "retry": {
    "max_attempts": 3,
    "initial_backoff": 1000,
    "max_backoff": 30000
  },
  "queue_name": "billing"
}
```

Use workers when:

- The logic lives in your app service.
- You need access to internal databases or private APIs.
- You want to deploy handler code independently from Orch8.
- The handler is app-specific and not worth packaging as a plugin.

The worker polls tasks, executes the handler, then reports success or failure. See [WORKERS.md](WORKERS.md) for the protocol.

## Build Plugins

Plugins turn reusable handler code into named sequence handlers.

Use a plugin when:

- The handler should be reused across many sequences or tenants.
- You want a packaged capability with a stable handler name.
- You need low-latency in-process execution from a WASM module.
- You want to run a dedicated sidecar service over gRPC/HTTP2.
- You are integrating a vendor/tool as a durable workflow primitive.

Prefer an external worker when the handler is ordinary application business logic. Prefer a plugin when it is reusable infrastructure.

### WASM Plugin

Register a WASM plugin:

```bash
curl -s -X POST "$ORCH8_URL/plugins" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-Id: acme" \
  -d '{
    "name": "normalize-email",
    "plugin_type": "wasm",
    "source": "/opt/orch8/plugins/normalize-email.wasm",
    "tenant_id": "acme",
    "description": "Normalize and validate email addresses"
  }'
```

Use it in a sequence:

```json
{
  "type": "step",
  "id": "normalize_email",
  "handler": "wasm://normalize-email",
  "params": {
    "field": "email"
  }
}
```

WASM plugin contract:

- Input is JSON containing `instance_id`, `block_id`, `params`, `context.data`, `context.config`, and `attempt`.
- Output must be JSON.
- The module must export `alloc`, `dealloc`, and `handle`.
- Keep WASM plugins deterministic, small, and fast.
- Good fits: validation, normalization, scoring, transformations, policy checks.

### gRPC Sidecar Plugin

Register a gRPC sidecar plugin:

```bash
curl -s -X POST "$ORCH8_URL/plugins" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-Id: acme" \
  -d '{
    "name": "grpc://risk-score",
    "plugin_type": "grpc",
    "source": "risk-service.internal:9090/RiskService.Score",
    "tenant_id": "acme",
    "description": "Risk scoring sidecar"
  }'
```

Use it in a sequence:

```json
{
  "type": "step",
  "id": "score_risk",
  "handler": "grpc://risk-score",
  "params": {
    "model": "merchant-risk-v4"
  },
  "timeout": 10000,
  "retry": {
    "max_attempts": 2,
    "initial_backoff": 1000,
    "max_backoff": 5000
  }
}
```

Sidecar request body:

```json
{
  "instance_id": "018f85d4-9c2b-7f5f-a2a5-f4143fa7d400",
  "block_id": "score_risk",
  "params": {
    "model": "merchant-risk-v4"
  },
  "context": {
    "data": {},
    "config": {}
  },
  "attempt": 1
}
```

Sidecar response body:

```json
{
  "risk_score": 0.82,
  "decision": "review"
}
```

Plugin management:

```bash
curl -s "$ORCH8_URL/plugins?tenant_id=acme" -H "X-Tenant-Id: acme"
curl -s "$ORCH8_URL/plugins/normalize-email" -H "X-Tenant-Id: acme"
curl -s -X PATCH "$ORCH8_URL/plugins/normalize-email" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-Id: acme" \
  -d '{ "enabled": false }'
```

## Complete Use Case: SaaS Signup Onboarding

Sequence:

```json
{
  "id": "018f85d4-9c2b-7f5f-a2a5-f4143fa7d500",
  "tenant_id": "acme",
  "namespace": "default",
  "name": "customer-onboarding",
  "version": 1,
  "deprecated": false,
  "blocks": [
    {
      "type": "step",
      "id": "normalize_email",
      "handler": "wasm://normalize-email",
      "params": { "field": "email" }
    },
    {
      "type": "parallel",
      "id": "initial_setup",
      "branches": [
        [
          { "type": "step", "id": "create_workspace", "handler": "workspace_create", "params": {} }
        ],
        [
          { "type": "step", "id": "send_welcome", "handler": "send_email", "params": { "template": "welcome", "to": "{{context.data.email}}" } }
        ],
        [
          { "type": "step", "id": "score_account", "handler": "grpc://risk-score", "params": { "model": "signup-risk-v1" } }
        ]
      ]
    },
    {
      "type": "step",
      "id": "wait_for_activation",
      "handler": "noop",
      "params": {},
      "delay": {
        "duration": 172800000,
        "business_days_only": true,
        "jitter": 1800000
      }
    },
    {
      "type": "router",
      "id": "activation_route",
      "routes": [
        {
          "condition": "context.data.activated == true",
          "blocks": [
            { "type": "step", "id": "send_success_tips", "handler": "send_email", "params": { "template": "success_tips" } }
          ]
        },
        {
          "condition": "context.data.plan == \"enterprise\"",
          "blocks": [
            { "type": "step", "id": "notify_sales", "handler": "send_slack", "params": { "channel": "#sales" } }
          ]
        }
      ],
      "default": [
        { "type": "step", "id": "send_activation_nudge", "handler": "send_email", "params": { "template": "activation_nudge" } }
      ]
    }
  ],
  "created_at": "2026-05-05T12:00:00Z"
}
```

Trigger:

```json
{
  "slug": "signup-created",
  "sequence_name": "customer-onboarding",
  "tenant_id": "acme",
  "namespace": "default",
  "trigger_type": "event"
}
```

Event payload:

```json
{
  "user_id": "user_123",
  "email": "alice@example.com",
  "plan": "trial",
  "activated": false
}
```

## More Use Cases

| Use case | Sequence shape |
|---|---|
| Order fulfillment | `parallel` validate/charge/reserve, then ship and notify |
| Invoice collection | send invoice, delay, router on paid/unpaid, escalate |
| AI agent with tools | `loop` with `llm_call`, `tool_call`, and stop condition |
| Approval workflow | `human_review`, router on decision, audit final action |
| Data enrichment | `for_each` records, call enrichment workers, merge results |
| Provider failover | `race` providers or `try_catch` primary/backup |
| Payment consistency | `cancellation_scope` around capture and ledger writes |
| Marketing test | `ab_split` subject lines or onboarding paths |
| Webhook automation | public trigger from Stripe/GitHub/Shopify payload |
| Reusable compliance check | `sub_sequence` called from multiple workflows |

## Pre-Publish Checklist

Before publishing:

- JSON parses cleanly.
- `blocks` is not empty.
- Every block `id` is unique.
- Every step has a non-empty `handler`.
- Every external handler has a worker, plugin, or integration ready.
- Every plugin handler has a registered plugin.
- Every trigger points to an existing sequence name and namespace.
- Durations are integer milliseconds.
- Retries have valid positive values.
- Routers have at least one route or a default.
- Loops and foreach blocks have non-empty bodies and bounded iterations.
- The sequence is within validation limits: nesting ≤ 64 deep, ≤ 256 branches per parallel/race, `max_iterations` ≤ 100,000, ≤ 5,000 total blocks.
- New behavior uses a new sequence ID and higher version.

## LLM Prompt

```text
Generate an Orch8 sequence JSON object.

Output only JSON.

Requirements:
- Include id, tenant_id, namespace, name, version, deprecated, blocks, created_at.
- Use only these block types: step, parallel, race, loop, for_each, router, try_catch, sub_sequence, ab_split, cancellation_scope.
- Every block id must be unique across the whole sequence.
- Durations must be integer milliseconds.
- Router shape is { type, id, routes: [{ condition, blocks }], default }.
- Parallel and race use branches as arrays of block arrays.
- Try/catch uses try_block, catch_block, and optional finally_block.
- Put runtime data references under context.data, such as {{context.data.email}}.
- Use built-in handlers when possible.
- Use external worker handler names for app-specific business logic.
- Use wasm://name only when a WASM plugin will be registered.
- Use grpc://name only when a gRPC plugin will be registered.

Workflow intent:
<describe workflow>

Tenant:
<tenant_id>

Namespace:
<namespace>
```
