# Tenant spend budgets

Cap what a tenant spends on LLM calls per day or per month, optionally per
model family. Soft thresholds raise alerts; a hard cap stops new `llm_call`
dispatches with a clear error.

Budgets use the cost telemetry the engine already records. Every `llm_call` /
`agent` turn writes a `usage_events` row (`kind = "llm_tokens"`), and spend is
estimated from list prices in the built-in pricing table. That is the same
table `GET /usage` uses; override it with `ORCH8_MODEL_PRICING`.

## Instance budgets vs tenant budgets

| | Instance `budget` (existing) | Tenant budget (this page) |
|---|---|---|
| Scope | One execution | All executions of a tenant |
| Limits | Tokens, steps, cost/wall-time/effects (continuity) | Estimated USD per day/month, optional model prefix |
| On breach | Instance **pauses** (`budget_exceeded`) | New `llm_call` dispatches **fail** (permanent `tenant_budget_exceeded`) |
| Alerts | — | 50/80/100% (configurable) threshold records |

The two layers are independent and read the same `usage_events`. Only
`llm_tokens` rows count as consumption. Response-cache hits are recorded as
`llm_cache_hit` and count toward neither layer.

## Managing budgets

Writes require the **root/admin API key**: a tenant must not be able to lift
its own cap. Tenants can read their own budgets and alerts.

```bash
# $500/month across all models; alert at 50/80/100%, block at 100%.
orch8 budget set --tenant acme --period monthly --limit-usd 500

# $20/day on GPT-5 family models only, alert-only (never blocks).
orch8 budget set --tenant acme --period daily --limit-usd 20 --model gpt-5 \
  --thresholds 75,90,100 --soft

orch8 budget list --tenant acme      # spend, percent_used, state, blocking
orch8 budget alerts --tenant acme
orch8 budget set --tenant acme --id <budget-id> --period monthly --limit-usd 800  # replace
orch8 budget delete <budget-id> --tenant acme
```

REST:

| Method | Path | Notes |
|---|---|---|
| `POST` | `/budgets` | `{tenant_id, period: daily\|monthly, limit_usd, model?, thresholds?, hard_cap?}`, admin |
| `PUT` | `/budgets/{id}` | Replace, admin |
| `DELETE` | `/budgets/{id}?tenant_id=` | Admin |
| `GET` | `/budgets?tenant_id=` | Current-period `BudgetStatus` list |
| `GET` | `/budgets/alerts?tenant_id=&limit=` | Alert records, newest first |

`GET /usage` includes a `budgets` array with the same status objects:

```json
{
  "budget": { "id": "…", "tenant_id": "acme", "model": null, "period": "monthly",
              "limit_usd": 500.0, "thresholds": [50, 80, 100], "hard_cap": true },
  "period_start": "2026-09-01T00:00:00Z", "period_end": "2026-10-01T00:00:00Z",
  "spend_usd": 412.37, "percent_used": 82.47, "state": "warning",
  "blocking": false, "unpriced_events": 0
}
```

`state` is `ok`, `warning` (at or above the lowest threshold) or `exceeded`
(at or above 100%). `blocking` is true when a hard-capped budget is exceeded.
Periods follow UTC calendar boundaries.

## Model filter

`model` is a case-insensitive **prefix** matched against the model recorded in
usage, which is the provider-reported response model (for example
`gpt-5.4-mini-2026-03-01`). Use family prefixes such as `gpt-5`,
`claude-opus` or `gemini-3`. The hard-cap check before dispatch matches the
same prefix against the requested model.

## Hard cap behavior

Before dispatching to a provider, `llm_call` loads the tenant's budget status.
It is cached per tenant for 5 seconds and refreshed after every call in the
process. If a hard-capped, exceeded budget matches the model, the step fails
with a **permanent** error:

```text
tenant budget exceeded: tenant 'acme' monthly budget for all models spent
$500.0123 of $500.0000; new llm_call dispatches are blocked until 2026-10-01T00:00:00+00:00
```

`details` carries `code: "tenant_budget_exceeded"`, `budget_id`, `spend_usd`,
`limit_usd` and `resets_at`, so the step lands in the DLQ with a
machine-readable reason. With a `providers` failover list, each provider whose
model is blocked is skipped. The call succeeds only through a provider whose
model the budget does not govern.

Consequences worth knowing:

- **One more call.** The check happens before a call and spend is recorded
  after it, so the call that crosses 100% completes. The same is true of
  instance budgets. Across nodes, enforcement can lag by up to ~5 s.
- **Fails closed on errors.** If budget status cannot be read, the step fails
  *retryably* and nothing is dispatched.
- **Cache hits are served** even when blocked, because they cost nothing.
- **Unpriced models don't count.** Usage for models missing from the pricing
  table is excluded from spend and reported as `unpriced_events`. Add prices
  through `ORCH8_MODEL_PRICING` to govern them.
- Spend is an **estimate** from list prices. It does not include provider
  discounts, cached-input pricing or batch tiers.

## Alerts: `budget.threshold_crossed`

After every successful `llm_call`, the engine recomputes the tenant's budget
statuses. For each threshold crossed in the current period it inserts exactly
one alert record. The insert is idempotent on `(budget_id, period_start,
threshold_percent)`, so concurrent workers never double-alert. The engine also
emits a `WARN` `tracing` event on target `orch8::budget` with message
`budget.threshold_crossed`.

Alert record (`GET /budgets/alerts`, table `tenant_budget_alerts`):

```json
{
  "id": "0191…", "event": "budget.threshold_crossed",
  "tenant_id": "acme", "budget_id": "0191…", "model": null,
  "period": "monthly", "period_start": "2026-09-01T00:00:00Z",
  "threshold_percent": 80, "spend_usd": 401.2, "limit_usd": 500.0,
  "blocking": false, "created_at": "2026-09-26T14:03:11Z"
}
```

`blocking` is true for the 100% alert of a hard-capped budget, when new
dispatches start failing. Alert destinations (Slack, email, PagerDuty
routing) are a separate feature. They should consume these records by polling
`GET /budgets/alerts` or reading the table, or subscribe to the `tracing`
event. The record shape is the contract.

## Storage

Postgres migration `094_tenant_budgets.sql` (down file in `migrations/down/`)
and SQLite schema v45 add `tenant_budgets` and `tenant_budget_alerts`.
