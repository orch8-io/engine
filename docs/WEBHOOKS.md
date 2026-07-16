# Webhooks

The engine POSTs JSON event payloads to URLs you configure when instances reach terminal states. Webhooks are delivered asynchronously with retry — they do not block the scheduler — and deliveries that exhaust their retries are parked in a queryable outbox.

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
secret = "<shared-hmac-secret>"  # optional — enables payload signing
```

Or via environment:

```bash
ORCH8_WEBHOOK_URLS="https://hooks.internal/orch8-events,https://hooks.staging.acme.com/orch8"
ORCH8_WEBHOOK_SECRET="<shared-hmac-secret>"
```

All configured URLs receive every event. There is no per-tenant or per-event filtering today — subscribe by URL and filter in your endpoint.

---

## Events

| `event_type` | Fires when | `data` shape |
|---|---|---|
| `instance.completed` | Instance reaches terminal `completed` state | `{}` |
| `instance.failed` | Instance reaches terminal `failed` state (including DLQ entry after retry exhaustion) | `{}`, or `{"error": "<message>"}` on some failure paths |
| `instance.sla_breached` | An instance with an `sla` policy exceeds `max_runtime` or `max_step_runtime` | `{"type", "limit_ms", "elapsed_ms", "step_id", "sequence_id", "tenant_id"}` |

Block-level and signal events are not published.

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
| `data` | object | Event-specific fields. Completion normally uses `{}`; failures may include `error`, and SLA breaches include limit/elapsed metadata. |

---

## Delivery semantics

- **At-least-once.** A slow or flapping endpoint can receive duplicates.
- **Non-blocking.** Each webhook URL is delivered in its own Tokio task — a slow subscriber does not slow the scheduler.
- **Retries.** `max_retries` is the number of retries _after_ the first attempt. With the default `max_retries = 3` a single event is attempted up to 4 times total.
- **Backoff.** Exponential starting at 500ms: `500ms → 1s → 2s → 4s → ...`. Doubles on each attempt.
- **Success.** Any `2xx` or `3xx` HTTP status counts as success.
- **Failure.** `4xx` and `5xx` are both retried until `max_retries` is exhausted. On exhaustion the engine logs the failure, increments `orch8_webhooks_failed_total`, and **parks the event in the webhook outbox** (`orch8_webhooks_parked_total`) instead of dropping it. Inspect and act on parked events via the API: `GET /webhooks/outbox` to list, `POST /webhooks/outbox/{id}/redeliver` to retry, `DELETE /webhooks/outbox/{id}` to discard.
- **Shutdown.** In-flight deliveries are tracked and awaited at shutdown (up to 10s) before the runtime exits. A retry loop interrupted by shutdown parks its event in the outbox rather than losing it.

### Guidance for subscribers

1. **Idempotency.** Use `instance_id` + `event_type` + `timestamp` as your
   de-duplication key. Retries retain the same payload timestamp, while two
   distinct SLA breach types for one instance remain distinguishable.
2. **Return quickly.** Acknowledge within `timeout_secs` (default 10s) and do work asynchronously.
3. **Return 2xx fast.** Any slow response eats into your subscriber's capacity and increases retry pressure.
4. **Don't depend on webhooks alone for consistency.** Delivery is at-least-once and exhausted events require an operator (or automation) to redeliver from the outbox. Treat webhooks as a latency optimization on top of periodic `GET /instances` polling if you need guaranteed delivery.
5. **Monitor the outbox.** Alert on `orch8_webhooks_parked_total` and drain `GET /webhooks/outbox` — parked events are not retried automatically.

---

## Signing and authentication

When a webhook secret is configured, payloads are signed with HMAC-SHA256. The engine sends two headers:

- `X-Orch8-Timestamp` — UNIX timestamp of the signing moment
- `X-Orch8-Signature` — `sha256=<hex-encoded HMAC>` over `{timestamp}.{body}`

Verify by recomputing the HMAC on the receiving side and comparing. The timestamp prevents replay attacks.

Without a secret, payloads are sent unsigned. In that case, protect endpoints by:

- Making them private (VPC, private network, IP allowlist).
- Using HTTPS + a secret path component (`https://hooks.internal/orch8-events-<random>`).
- Gating at a reverse proxy with `Authorization` you control.

---

## Metrics

Exposed at `/metrics` in Prometheus format:

| Metric | Type | Description |
|---|---|---|
| `orch8_webhooks_sent_total` | Counter | Successful deliveries (2xx/3xx response) |
| `orch8_webhooks_failed_total` | Counter | Deliveries that exhausted all retries |
| `orch8_webhooks_parked_total` | Counter | Exhausted deliveries parked in the webhook outbox |

---

## Receiver example (Node/Express)

This minimal receiver demonstrates acknowledgement and deduplication. In a
signed deployment, capture the raw request bytes and verify
`X-Orch8-Timestamp` and `X-Orch8-Signature` before JSON parsing or queueing;
reserializing `req.body` does not reproduce the signed byte sequence.

```ts
import express from "express";
const app = express();
app.use(express.json());

const seen = new Set<string>(); // in-memory dedup; use Redis in prod

app.post("/orch8-events", (req, res) => {
  const { event_type, instance_id, timestamp } = req.body;
  const key = `${instance_id}:${event_type}:${timestamp}`;
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
