# Webhooks

The engine POSTs JSON event payloads to URLs you configure when instances reach terminal states. Webhooks are fire-and-forget with retry; they do not block the scheduler.

---

## Configuring

In `orch8.toml`:

```toml
[engine.webhooks]
urls = [
  "https://hooks.internal/orch8-events",
  "https://hooks.staging.acme.com/orch8",
]
timeout_secs = 10
max_retries = 3
```

Or via environment:

```bash
ORCH8_WEBHOOK_URLS="https://hooks.internal/orch8-events,https://hooks.staging.acme.com/orch8"
```

All configured URLs receive every event. There is no per-tenant or per-event filtering today — subscribe by URL and filter in your endpoint.

---

## Events

| `event_type` | Fires when | `data` shape |
|---|---|---|
| `instance.completed` | Instance reaches terminal `completed` state | `{}` |
| `instance.failed` | Instance reaches terminal `failed` state (including DLQ entry after retry exhaustion) | `{}` |

Only instance-terminal events are emitted today. Block-level and signal events are not published.

---

## Payload shape

```json
{
  "event_type": "instance.completed",
  "instance_id": "a1b2c3d4-e5f6-4000-8000-000000000001",
  "timestamp": "2026-04-15T14:30:22.151Z",
  "data": {}
}
```

| Field | Type | Description |
|---|---|---|
| `event_type` | string | Event identifier (see table above) |
| `instance_id` | UUID \| null | Instance the event pertains to (always set for current events) |
| `timestamp` | RFC 3339 string | When the event was produced |
| `data` | object | Reserved for event-specific fields. Currently always `{}`. |

---

## Delivery semantics

- **At-least-once.** A slow or flapping endpoint can receive duplicates.
- **Non-blocking.** Each webhook URL is delivered in its own Tokio task — a slow subscriber does not slow the scheduler.
- **Retries.** `max_retries` is the number of retries _after_ the first attempt. With the default `max_retries = 3` a single event is attempted up to 4 times total.
- **Backoff.** Exponential starting at 500ms: `500ms → 1s → 2s → 4s → ...`. Doubles on each attempt.
- **Success.** Any `2xx` or `3xx` HTTP status counts as success.
- **Failure.** `4xx` and `5xx` are both retried until `max_retries` is exhausted. On exhaustion the engine logs the failure and increments the `orch8_webhooks_failed_total` counter. **The event is dropped.** There is no dead-letter storage for failed webhooks.
- **Shutdown.** In-flight retries are aborted by the shutdown cancellation token. Tune `shutdown_grace_period_secs` if you need more time to drain.

### Guidance for subscribers

1. **Idempotency.** Use `instance_id` + `event_type` as your de-duplication key.
2. **Return quickly.** Acknowledge within `timeout_secs` (default 10s) and do work asynchronously.
3. **Return 2xx fast.** Any slow response eats into your subscriber's capacity and increases retry pressure.
4. **Don't depend on webhooks alone for consistency.** Treat them as a latency optimization on top of periodic `GET /instances` polling if you need guaranteed delivery.

---

## Signing and authentication

**Today: none.** Payloads are sent unsigned over whatever transport the URL specifies. Protect endpoints by:

- Making them private (VPC, private network, IP allowlist).
- Using HTTPS + a secret path component (`https://hooks.internal/orch8-events-<random>`).
- Gating at a reverse proxy with `Authorization` you control.

HMAC signing (`X-Orch8-Signature`) is on the roadmap but not implemented.

---

## Metrics

Exposed at `/metrics` in Prometheus format:

| Metric | Type | Description |
|---|---|---|
| `orch8_webhooks_sent_total` | Counter | Successful deliveries (2xx/3xx response) |
| `orch8_webhooks_failed_total` | Counter | Events dropped after exhausting all retries |

---

## Receiver example (Node/Express)

```ts
import express from "express";
const app = express();
app.use(express.json());

const seen = new Set<string>(); // in-memory dedup; use Redis in prod

app.post("/orch8-events", (req, res) => {
  const { event_type, instance_id, timestamp } = req.body;
  const key = `${instance_id}:${event_type}`;
  if (seen.has(key)) return res.sendStatus(200); // already processed
  seen.add(key);

  // Queue real work off the request thread.
  void handleAsync(req.body);
  res.sendStatus(202);
});

app.listen(3000);
```

---

## See also

- [Configuration — `[engine.webhooks]`](CONFIGURATION.md#enginewebhooks)
- [API reference — Metrics](API.md#metrics)
