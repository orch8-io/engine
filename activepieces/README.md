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

**Triggers don't (yet).** Polling and webhook triggers need scaffolding for `onEnable`/`onDisable` lifecycle hooks — use orch8's native webhook / NATS / file-watch triggers for inbound events.

**OAuth2 refresh is the caller's responsibility.** The adapter does not refresh tokens — pass a fresh `access_token` in `auth`. A forthcoming companion package will integrate with orch8's plugin/credentials registry.

**`context.store` is in-memory per invocation.** Pieces that rely on persistent KV across runs won't see earlier data. Use orch8's session blocks for cross-step state.

**`context.connections.get()` returns null.** The orch8 contract is to pass resolved auth directly in `params.auth`.

## License

MIT. See `LICENSE`.

ActivePieces community pieces loaded at runtime are MIT-licensed by Activepieces, Inc. See `NOTICE` for attribution details. This worker only supports the MIT-licensed piece set under `packages/pieces/{framework,common,core,community}/**`; proprietary Enterprise Edition pieces are out of scope.
