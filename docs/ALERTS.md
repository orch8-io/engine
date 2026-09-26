# Built-in Alerts

> **Stability: beta**, shipped and tested; may change in a minor release with a changelog note.

Orch8 can page you without an external monitoring stack. An alert rule
pairs one **condition** with one **destination**; every engine node
evaluates rules every `alerts.eval_interval_secs` (default 30 s) and delivers
*transitions* through the durable webhook outbox (retries, parking, manual
redelivery, and the delivery inspector all apply).

- `ok → firing` sends one alert, then nothing more while the rule stays
  firing (dedup). Re-firing after a recovery waits for `cooldown_secs`
  (default 900, min 60) since the previous firing.
- `firing → resolved` sends one recovery (PagerDuty `resolve` with the same
  `dedup_key`).
- Evaluator state is updated with a version compare-and-swap, so exactly one
  node emits each transition in a multi-node cluster.

## Conditions

| `kind` | Fields | Fires when |
|---|---|---|
| `dlq_growth` | `threshold?`, `growth?`, `window_secs` (60–86400, default 300) | failed (dead-lettered) instances ≥ `threshold`, or ≥ `growth` new failures within the window |
| `circuit_breaker_opened` | `handler?` | any (or the named handler's) circuit breaker for the tenant is open |
| `budget_breach` | `min_instances` (default 1) | instances paused by budget enforcement (`paused_reason = budget_exceeded`) ≥ `min_instances` |
| `worker_pool_empty` | `handler`, `queue?`, `seen_within_secs` (default 120) | no worker for `handler` (on `queue`) polled within the window |

## Destinations

Secrets are **never** stored in a rule: every secret field is a
`credentials://id[/field]` reference resolved in the rule's tenant at send
time (literal URLs/keys are rejected with `400`).

| `type` | Fields | Delivery |
|---|---|---|
| `slack` | `url_ref` | Block Kit message to the incoming webhook behind the credential (SSRF-checked) |
| `pagerduty` | `routing_key_ref`, `severity` (`critical`/`error`/`warning`/`info`) | Events API v2 `trigger` / `resolve`, `dedup_key = orch8-alert-<rule id>` |
| `webhook` | `url`, `secret_ref` | JSON `{event, rule, tenant_id, status, value, summary, dedup_key, timestamp}` signed with `X-Orch8-Timestamp` + `X-Orch8-Signature: sha256=<hex HMAC("{ts}.{body}")>` (SSRF-checked) |

## API (tenant-scoped, Operator capability)

```bash
curl -s -X POST "$ORCH8_URL/alerts/rules" -H "X-Tenant-Id: acme" -H "Content-Type: application/json" -d '{
  "name": "DLQ spike",
  "condition": {"kind": "dlq_growth", "growth": 10, "window_secs": 600},
  "destination": {"type": "pagerduty", "routing_key_ref": "credentials://pagerduty/routing_key", "severity": "critical"},
  "cooldown_secs": 1800
}'
curl -s "$ORCH8_URL/alerts/rules" -H "X-Tenant-Id: acme"
curl -s "$ORCH8_URL/alerts/rules/$ID" -H "X-Tenant-Id: acme"          # includes evaluator state
curl -s -X PUT "$ORCH8_URL/alerts/rules/$ID" -H "X-Tenant-Id: acme" -d @rule.json
curl -s -X DELETE "$ORCH8_URL/alerts/rules/$ID" -H "X-Tenant-Id: acme"
```

CLI: `orch8 alert list|get|create --file|update <id> --file|delete <id>`.

## Server config

Operator-declared rules are evaluated alongside API-managed ones:

```toml
[alerts]
enabled = true
eval_interval_secs = 30

[[alerts.rules]]
name = "payments workers gone"
tenant_id = "acme"
condition = { kind = "worker_pool_empty", handler = "charge_card", seen_within_secs = 120 }
destination = { type = "slack", url_ref = "credentials://ops-slack/url" }
cooldown_secs = 900
```

Invalid config rules fail startup. `ORCH8_ALERTS_ENABLED` and
`ORCH8_ALERTS_EVAL_INTERVAL_SECS` override the section; `pagerduty_events_url`
exists only for proxies and tests.

Transitions increment `orch8_alerts_emitted_total`; delivery outcomes use the
webhook outbox metrics and the delivery inspector (`GET /webhooks/deliveries`);
exhausted deliveries are parked in `GET /webhooks/outbox` for redelivery
(event types `alert.firing` / `alert.resolved`; the row `url` shows the
credential reference or target, never a secret).
