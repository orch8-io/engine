# Background Jobs

> **Stability: beta**, shipped and tested; may change in a minor release with a changelog note.

Enqueue a handler invocation with one HTTP call, no sequence required. This
is the Orch8 equivalent of a BullMQ / Sidekiq / Celery job, with the
durability, retries, DLQ, and observability of a full workflow engine.

```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-Id: acme' \
  -d '{
        "handler": "send_email",
        "payload": {"to": "ada@example.com", "template": "welcome"},
        "retry": {"max_attempts": 5, "initial_backoff_ms": 1000, "max_backoff_ms": 60000},
        "idempotency_key": "welcome-ada"
      }'
```

```json
{
  "id": "01926f3a-…",
  "instance_id": "01926f3a-…",
  "handler": "send_email",
  "status": "scheduled",
  "created_at": "2026-09-26T10:00:00Z",
  "run_at": "2026-09-26T10:00:00Z",
  "attempts": 0
}
```

## How it works

A job is an ordinary workflow instance of an auto-managed, single-step
**system sequence** named `_job.<handler>` (or `_job.<handler>~<hash>` when a
queue or retry policy is set, because step policy is part of the sequence).
The sequence is created on first use per tenant/namespace and reused
afterwards. The job id **is** the instance id, so:

- **Workers**: a built-in handler (`http_request`, `llm_call`, …) runs
  in-process. Any other handler name is dispatched to
  [external workers](WORKERS.md) on the job's `queue`, through pull
  (`/workers/tasks/poll`) or push (queue dispatch config).
- **Retries and DLQ**: the step retry policy applies. Failed jobs show up in
  `GET /instances/dlq` and in DLQ groups, and can be retried with
  `POST /instances/{id}/retry`.
- **Everything else** also applies: tenant isolation, API keys, plan
  entitlements, encryption at rest, webhooks, and dashboard views. Every
  `/instances/{id}/…` endpoint (timeline, outputs, stream) accepts a job id.

The payload is stored as the instance's `context.data` and passed verbatim
to the handler as its `params`. It must be a JSON object.

## API

All paths are relative to `/api/v1`. Tenant scoping is the same as for
`/instances`: the `X-Tenant-Id` header is authoritative, and cross-tenant reads
return 404.

### `POST /jobs`

| field | type | notes |
|---|---|---|
| `handler` | string | required |
| `payload` | object | default `{}` |
| `queue` | string | worker queue; default queue when omitted |
| `priority` | `low` \| `normal` \| `high` \| `critical` | case-insensitive; default `normal` |
| `retry.max_attempts` | u32 | **total** executions including the first (`1` = no retry, max 1000) |
| `retry.initial_backoff_ms` | u64 | first backoff; doubles on each retry |
| `retry.max_backoff_ms` | u64 | backoff cap; default `max(60000, initial_backoff_ms)` |
| `delay_ms` | u64 | run no earlier than now + delay (mutually exclusive with `run_at`) |
| `run_at` | RFC 3339 | run no earlier than this instant |
| `idempotency_key` | string | tenant-scoped; a repeat returns the existing job with **200** |
| `metadata` | object | stored on the instance (`_job` is reserved) |
| `tenant_id`, `namespace` | string | optional; header wins; namespace defaults to `default` |

Responses: `201` created, `200` idempotent replay, `400` validation error,
`413` payload over the context size limit, `429`/`403` when plan
entitlements are exceeded.

### `GET /jobs/{id}`

Returns the job plus `attempts` (executions started so far), `output` (once
completed), and `error` (the last failure message, while retrying or after
failing).

| `status` | meaning |
|---|---|
| `scheduled` | waiting for its run time, a retry backoff, or a worker |
| `running` | executing in-process or claimed by or dispatched to a worker |
| `completed` | finished; `output` holds the handler result |
| `failed` | failed, and the job had no retry policy |
| `dead_lettered` | failed although a retry policy was set (attempts exhausted or non-retryable error) |
| `cancelled` | cancelled |

### `GET /jobs?handler=&status=&limit=&cursor=`

Newest first, with keyset pagination: pass the previous page's
`next_cursor` as `cursor`. `limit` defaults to 50 (max 500). A concurrent
enqueue never shifts rows between pages.

```json
{"items": [ … ], "next_cursor": "01926f3a-…", "has_more": true}
```

Without an `X-Tenant-Id` header, only admin callers may list, optionally
narrowed with `tenant_id=`.

### `DELETE /jobs/{id}`

- A job that has not started yet is cancelled immediately: **200** with
  `status: "cancelled"`.
- A running job is sent a cancel signal, which the engine applies at the next
  step boundary: **202**.
- A job that already finished returns **409**.

## Worker example (Node, plain `fetch`)

```js
const BASE = "http://localhost:8080/api/v1";
const H = { "Content-Type": "application/json", "X-Tenant-Id": "acme" };

async function loop() {
  for (;;) {
    const res = await fetch(`${BASE}/workers/tasks/poll`, {
      method: "POST", headers: H,
      body: JSON.stringify({ handler_name: "send_email", worker_id: "mailer-1", limit: 10 }),
    });
    const { tasks, poll_after_ms } = await res.json();
    await Promise.all(tasks.map(async (task) => {
      const ack = { worker_id: "mailer-1", claim_epoch: task.claim_epoch };
      try {
        const output = await sendEmail(task.params); // task.params === job payload
        await fetch(`${BASE}/workers/tasks/${task.id}/complete`, {
          method: "POST", headers: H, body: JSON.stringify({ ...ack, output }),
        });
      } catch (err) {
        await fetch(`${BASE}/workers/tasks/${task.id}/fail`, {
          method: "POST", headers: H,
          body: JSON.stringify({ ...ack, message: String(err), retryable: true }),
        });
      }
    }));
    if (tasks.length === 0) await new Promise((r) => setTimeout(r, poll_after_ms));
  }
}
loop();
```

Workers bound to a named queue (`"queue": "mail"` on the job) poll
`POST /workers/tasks/poll/queue` with an extra `queue_name`. For push delivery,
configure `POST /queues/dispatch` for that queue. See [WORKERS.md](WORKERS.md).

## CLI

```bash
orch8 job enqueue send_email --payload '{"to":"ada@example.com"}' \
  --max-attempts 5 --backoff-ms 1000 --idempotency-key welcome-ada
orch8 job get <id>
orch8 job list --handler send_email --status dead_lettered
orch8 job cancel <id> --yes
```
