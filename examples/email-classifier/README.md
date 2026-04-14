# Email Classifier Pipeline

A production-ready example of an orch8 workflow that automatically triages
inbound emails: classifies them with AI, enriches with web context, and posts
a formatted notification to Slack.

Only emails with `VIVA ORCH8:` in the subject line are processed. Everything
else is silently dropped.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│  Gmail/Outlook                                                              │
│       │                                                                     │
│       ▼                                                                     │
│  Resend (MX record on auto.orch8.io)                                        │
│       │                                                                     │
│       ▼ POST /  (webhook)                                                   │
│  ┌──────────┐                                                               │
│  │ webhook  │  Adapter: transforms Resend payload → POST /instances         │
│  │ adapter  │  (webhook.ts, port 3333)                                      │
│  └────┬─────┘                                                               │
│       │ POST /instances                                                     │
│       ▼                                                                     │
│  ┌──────────────────────────── orch8 engine (port 8080) ──────────────────┐ │
│  │                                                                        │ │
│  │  [gate]  ← worker: check_subject_prefix                               │ │
│  │     │       Returns: { accepted: bool, clean_subject: string }         │ │
│  │     │                                                                  │ │
│  │  [filter] router: outputs.gate.accepted == true                        │ │
│  │     ├── YES:                                                           │ │
│  │     │   [classify] ← BUILT-IN llm_call (DeepSeek, runs inline)        │ │
│  │     │       Returns: { message: { content: "{...json...}" } }          │ │
│  │     │                                                                  │ │
│  │     │   [format] ← worker: search_and_format                          │ │
│  │     │       Receives classification, does web search, formats Slack    │ │
│  │     │       Returns: { slack_message: "..." }                          │ │
│  │     │                                                                  │ │
│  │     └── NO: [skip] noop                                               │ │
│  │                                                                        │ │
│  │  [notify_gate] router: outputs.gate.accepted == true                   │ │
│  │     ├── YES:                                                           │ │
│  │     │   [notify] ← ap://slack.send_channel_message (Activepieces)      │ │
│  │     │       Sends outputs.format.slack_message to Slack channel         │ │
│  │     │                                                                  │ │
│  │     └── NO: [done] noop                                               │ │
│  │                                                                        │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Three execution models in one workflow

| Step | Handler | Execution model | Why |
|------|---------|----------------|-----|
| `gate` | `check_subject_prefix` | External worker (Node.js) | Custom logic, runs on your infra |
| `classify` | `llm_call` | Built-in (inline in engine) | No worker roundtrip, completes instantly |
| `format` | `search_and_format` | External worker (Node.js) | HTTP scraping, custom formatting |
| `notify` | `ap://slack.send_channel_message` | Activepieces sidecar | Zero-code Slack integration |
| `skip`/`done` | `noop` | Built-in (inline) | No-op for clean flow completion |

### Key design decisions

**Why two routers?**

Blocks inside a router branch execute in parallel (dispatched simultaneously to
the worker queue). The `notify` step needs `outputs.format.slack_message`, which
only exists after `format` completes. By placing `notify` in a separate
top-level router AFTER the first one, we guarantee sequential execution —
top-level blocks always run in order.

**Why `llm_call` works inside the router branch:**

Built-in handlers (`llm_call`, `noop`, `log`) execute inline during the
evaluator pass — they complete before the next step in the branch is dispatched.
So `format` can safely reference `{{outputs.classify.message.content}}`.

**Why the gate is a worker step:**

The prefix check could be a built-in, but implementing it as a worker step
demonstrates the pattern for custom business logic. In production you'd replace
this with your own filtering rules (sender allowlist, regex patterns, etc).

---

## Files

| File | Purpose |
|------|---------|
| `deploy.ts` | Registers the workflow sequence with orch8 (raw JSON, no SDK abstraction) |
| `worker.ts` | Node.js worker polling for `check_subject_prefix` and `search_and_format` tasks |
| `webhook.ts` | HTTP adapter converting Resend webhook payload → orch8 instance creation |
| `trigger.ts` | CLI script to simulate an inbound email (for testing without real email) |
| `.env.example` | All required environment variables with documentation |

---

## Setup

### Prerequisites

- orch8 engine binary (built from this repo)
- Node.js 18+
- ngrok (for local development with Resend webhooks)
- Activepieces sidecar with `@activepieces/piece-slack` installed

### 1. Environment variables

```bash
cp .env.example .env
```

| Variable | Where to get it |
|----------|-----------------|
| `DEEPSEEK_API_KEY` | https://platform.deepseek.com/api_keys (free tier: $5 credit) |
| `SLACK_ACCESS_TOKEN` | Slack app → OAuth & Permissions → Bot Token (`xoxb-...`) |
| `SLACK_CHANNEL_ID` | Right-click channel → View details → ID at bottom |
| `ORCH8_URL` | Default: `http://localhost:8080` |

### 2. Slack app setup

1. https://api.slack.com/apps → **Create New App** → From Scratch
2. **OAuth & Permissions** → Bot Token Scopes → add `chat:write`
3. **Install to Workspace** → copy `xoxb-...` token
4. Invite bot to channel: `/invite @YourBot`

### 3. Activepieces sidecar

```bash
cd activepieces
npm install @activepieces/piece-slack
npm run build
node dist/index.js
```

### 4. Resend inbound email

1. https://resend.com/domains → add your domain
2. Add the MX record Resend provides to your DNS
3. https://resend.com/webhooks → create webhook:
   - Event: `email.received`
   - URL: your ngrok URL (see below)

---

## Running (5 terminals)

```bash
# 1. Orch8 engine (SQLite for local dev)
ORCH8_STORAGE_BACKEND=sqlite ./target/aarch64-apple-darwin/release/orch8-server --insecure

# 2. Activepieces sidecar
cd activepieces && node dist/index.js

# 3. Webhook adapter (receives Resend webhooks, creates orch8 instances)
cd examples/email-classifier && npm run webhook

# 4. Ngrok tunnel (exposes webhook adapter to internet)
ngrok http 3333

# 5. Deploy sequence + start worker
cd examples/email-classifier
npm run deploy
npm run worker
```

### Update Resend webhook URL

After starting ngrok, copy the URL it shows (e.g.
`https://abc123.ngrok-free.app`) and set it as your Resend webhook endpoint.

---

## Testing without real email

```bash
# Trigger with "VIVA ORCH8:" prefix — full pipeline runs
npm run trigger

# Trigger without prefix — silently skipped
npm run trigger -- --skip
```

---

## Data flow in detail

### 1. Email arrives

Resend receives mail at `enquire@auto.orch8.io` and POSTs to your webhook URL:

```json
{
  "type": "email.received",
  "data": {
    "from": "sarah@bigcorp.com",
    "subject": "VIVA ORCH8: Enterprise pricing",
    "text": "Hi, we're evaluating...",
    "to": ["enquire@auto.orch8.io"]
  }
}
```

### 2. Webhook adapter creates instance

`webhook.ts` transforms this into:

```json
POST /instances
{
  "sequence_id": "...",
  "tenant_id": "example",
  "namespace": "default",
  "context": {
    "data": {
      "from": "sarah@bigcorp.com",
      "subject": "VIVA ORCH8: Enterprise pricing",
      "body": "Hi, we're evaluating...",
      "to": "enquire@auto.orch8.io"
    }
  }
}
```

### 3. Gate step (worker)

Input: `{ subject: "VIVA ORCH8: Enterprise pricing", prefix: "VIVA ORCH8:" }`

Output:
```json
{ "accepted": true, "clean_subject": "Enterprise pricing" }
```

### 4. Classification (built-in llm_call → DeepSeek)

The engine calls DeepSeek's OpenAI-compatible API directly. No worker involved.

Output:
```json
{
  "provider": "openai",
  "model": "deepseek-chat",
  "message": {
    "role": "assistant",
    "content": "{\"category\":\"sales\",\"priority\":\"high\",\"search_query\":\"BigCorp engineering team\",\"reasoning\":\"Enterprise pricing request suggests large deal\"}"
  },
  "finish_reason": "stop"
}
```

### 5. Search and format (worker)

Receives `outputs.classify.message.content` (the JSON string), parses it,
searches DuckDuckGo, and builds the Slack message.

Output:
```json
{
  "slack_message": ":rotating_light: *New email — sales* (high)\n\n*From:* sarah@bigcorp.com\n*Subject:* Enterprise pricing\n*AI reasoning:* Enterprise pricing request suggests large deal\n\n*Web context:*\n  • <url|BigCorp Series B>",
  "category": "sales",
  "priority": "high"
}
```

### 6. Slack notification (Activepieces)

The engine sends `outputs.format.slack_message` to the Activepieces sidecar,
which calls Slack's `chat.postMessage` API. Result appears in your channel.

---

## Production deployment

In production:

- **No ngrok** — orch8 runs behind a load balancer with a real domain
- **No `--insecure`** — set `ORCH8_API_KEY` for authenticated access
- **PostgreSQL** — use `ORCH8_DATABASE_URL` instead of SQLite
- **Encrypted credentials** — store API keys in orch8's credential store, not env vars
- **Resend webhook** points directly at `https://orch8.yourcompany.com/webhook/resend`

```bash
ORCH8_DATABASE_URL=postgres://... \
ORCH8_API_KEY=your-api-key \
./orch8-server
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `502 Bad Gateway` in ngrok | orch8 or webhook adapter not running | Start the relevant process |
| `422 Unprocessable Entity` | Resend hitting orch8 directly (wrong port) | Point ngrok at 3333 (adapter), not 8080 |
| Instance stuck in `waiting` | Worker not running | Start `npm run worker` |
| `missing_scope` from Slack | Bot token lacks `chat:write` | Add scope + reinstall app + copy new token |
| Classification returns `unknown` | Wrong output path in template | Use `{{outputs.classify.message.content}}` |
| Steps run out of order | Worker steps in same branch run in parallel | Use top-level sequencing (separate routers) |
| `piece 'slack' is not installed` | Missing npm package | `cd activepieces && npm install @activepieces/piece-slack` |
