# @orch8/activepieces-worker

HTTP sidecar that lets Orch8 workflows execute [ActivePieces](https://github.com/activepieces/activepieces) community pieces out of the box — Slack, Gmail, Stripe, GitHub, HubSpot, Notion, and ~280 others.

## How it fits

```
┌──────────────┐   ap://slack.send_channel_message   ┌─────────────────────────┐
│ orch8-engine │ ──────────────────────────▶ │ orch8-activepieces-     │
│   (Rust)     │        HTTP/1 JSON          │   worker (Node)         │
└──────────────┘ ◀────────────────────────── │ loads @activepieces/    │
                       action output         │   piece-<name> and      │
                                             │   invokes action.run()  │
                                             └─────────────────────────┘
```

The Rust engine dispatches any step whose `handler` starts with `ap://` to this sidecar via HTTP. No Rust changes are required to add new pieces — just `npm install @activepieces/piece-<name>` into the sidecar's `node_modules`.

## Install

```bash
cd activepieces
npm install
# install whichever pieces you want to make available:
npm install @activepieces/piece-slack @activepieces/piece-gmail @activepieces/piece-stripe
npm run build
```

## Run

```bash
ORCH8_AP_PORT=50052 node dist/index.js
```

Environment variables:

| Var                    | Default                | Purpose                                                     |
|------------------------|------------------------|-------------------------------------------------------------|
| `ORCH8_AP_PORT`        | `50052`                | Listen port                                                 |
| `ORCH8_AP_HOST`        | `127.0.0.1`            | Bind address (use `0.0.0.0` for Docker)                     |
| `ORCH8_AP_TIMEOUT_MS`  | `60000`                | Per-action execution timeout                                |
| `ORCH8_AP_ALLOWLIST`   | unset (allow any)      | Comma-separated piece names the worker is allowed to load   |

On the Rust side:

```bash
ORCH8_ACTIVEPIECES_URL=http://127.0.0.1:50052/execute ./orch8-server
```

(Default is the same URL — the env var only needs setting if you run the sidecar elsewhere.)

## Author a step that uses a piece

```json
{
  "id": "notify_channel",
  "type": "step",
  "handler": "ap://slack.send_channel_message",
  "params": {
    "auth":  { "access_token": "xoxb-..." },
    "props": { "channel": "C12345", "text": "deployment complete" }
  }
}
```

The handler prefix `ap://` is parsed as `ap://<piece>.<action>`. `auth` and `props` are passed through to the piece as `context.auth` and `context.propsValue`.

## Polling triggers (`POST /poll`)

The engine's `activepieces_poll` trigger loop calls `POST /poll` on a schedule
to run a piece trigger's poll headlessly:

```json
{
  "piece":   "stripe",
  "trigger": "new_failed_payment",
  "auth":    { "access_token": "sk_live_..." },
  "props":   { },
  "state":   { "lastPoll": 1717171717 },
  "slug":    "stripe-failed-payments"
}
```

Response: `{ "ok": true, "items": [ ... ], "state": { ... } }` — `items` are
the new events since the cursor (the engine creates one durable instance per
item), `state` is the trigger's `context.store` contents after the run. The
engine persists `state` verbatim and sends it back on the next poll, which is
exactly the dedupe contract AP's `pollingHelper` expects (`lastPoll` epoch /
seen-id cursors live in the store). `state` is `null` on the first poll.

Only `TriggerStrategy.POLLING` triggers are supported — webhook-strategy
triggers are rejected with a permanent error (`onEnable`/`onDisable`
platform registration doesn't exist headlessly). Register a poll trigger on
the orch8 side with:

```bash
curl -X POST http://localhost:8080/api/v1/triggers \
  -H "x-api-key: $ORCH8_API_KEY" \
  -H 'x-tenant-id: t1' \
  -H 'content-type: application/json' -d '{
  "slug": "stripe-failed-payments",
  "sequence_name": "payment-recovery",
  "tenant_id": "t1",
  "trigger_type": "activepieces_poll",
  "config": {
    "piece": "stripe",
    "trigger": "new_failed_payment",
    "auth": "credentials://stripe-prod",
    "interval_secs": 60
  }
}'
```

`config.auth` / `config.props` may contain `credentials://` references —
the engine resolves them tenant-scoped on every poll. Schedule is either
`interval_secs` (default 60) or `cron` (UTC), not both. Poll failures are
recorded on the registration (`GET /triggers/{slug}` returns `poll_state`
with `last_error` and `consecutive_failures`) and retried on the next tick.

## Error semantics

| HTTP status | Error type  | Retryable? |
|-------------|-------------|------------|
| 200         | —           | —          |
| 422         | `permanent` | No         |
| 500         | `permanent` | No         |
| 502         | `retryable` | Yes        |

The orch8 engine translates 5xx responses to `StepError::Retryable` and 4xx responses to `StepError::Permanent`, so retries and DLQ routing work identically to other handlers.

Classification heuristic inside the sidecar:
- Piece throws with `response.status >= 500` → **retryable**
- Piece throws with `response.status` in 4xx → **permanent**
- Node network error codes (`ECONNRESET`, `ETIMEDOUT`, `EAI_AGAIN`, ...) → **retryable**
- `TypeError`/`ReferenceError`/`SyntaxError` → **permanent** (piece code bug)
- Unknown failures → **retryable** (fail open)

## What works and what doesn't

**Actions work.** Most community piece actions execute cleanly through the stub `ActionContext`.

**Polling triggers work.** `POST /poll` runs `TriggerStrategy.POLLING` triggers with a store seeded from the engine-persisted cursor (see above). Webhook-strategy triggers are not supported — they need platform-side `onEnable`/`onDisable` registration and an inbound URL; use orch8's native webhook triggers for those events.

**OAuth2 refresh is the caller's responsibility.** The adapter does not refresh tokens — pass a fresh `access_token` in `auth`. A forthcoming companion package will integrate with orch8's plugin/credentials registry.

**`context.store` is in-memory per invocation.** Pieces that rely on persistent KV across runs won't see earlier data. Use orch8's session blocks for cross-step state.

**`context.connections.get()` returns null.** The orch8 contract is to pass resolved auth directly in `params.auth`.

## License

MIT. See `LICENSE`.

ActivePieces community pieces loaded at runtime are MIT-licensed by Activepieces, Inc. See `NOTICE` for attribution details. This worker only supports the MIT-licensed piece set under `packages/pieces/{framework,common,core,community}/**`; proprietary Enterprise Edition pieces are out of scope.
