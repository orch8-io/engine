# Orch8 Engine — Quick Start

Zero to first completed instance in 5 minutes.

---

## Option A: SQLite (zero dependencies)

The fastest path. No Docker, no Postgres — just the binary and a local file.

### 1. Install

```bash
# Build from source
cargo build --release

# Or download a release binary from https://github.com/orch8-io/engine/releases
```

### 2. Initialize a project

```bash
./target/release/orch8 init my-project
cd my-project
```

This creates:

```
my-project/
  orch8.toml          # Config (SQLite by default)
  sequence.json       # Example sequence definition
  docker-compose.yml  # Postgres stack (for Option B)
```

### 3. Start the engine

The server fails closed on two things: it refuses to boot without an encryption
key (data at rest is encrypted) and without an API key. `orch8 init` already
generated an API key into `orch8.toml`; supply an encryption key via env:

```bash
export ORCH8_ENCRYPTION_KEY=$(openssl rand -hex 32)
orch8-server --config orch8.toml
```

For a throwaway local run you can skip both instead:
`orch8-server --config orch8.toml --insecure-storage` disables encryption at
rest only; `--insecure` additionally disables authentication.

The server starts on `http://localhost:8080`. Migrations run automatically.

Export the generated API key and tenant for the curl steps below — every
request needs `x-api-key` and `x-tenant-id` headers:

```bash
export ORCH8_API_KEY=$(sed -n 's/^api_key = "\(.*\)"/\1/p' orch8.toml)
export ORCH8_URL=http://localhost:8080/api/v1
alias ocurl='curl -s -H "x-api-key: $ORCH8_API_KEY" -H "x-tenant-id: demo"'
```

### 4. Create a sequence

```bash
SEQUENCE_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
ocurl -X POST "$ORCH8_URL/sequences" \
  -H 'Content-Type: application/json' \
  -d "{
    \"id\": \"$SEQUENCE_ID\",
    \"tenant_id\": \"demo\",
    \"namespace\": \"default\",
    \"name\": \"hello-world\",
    \"version\": 1,
    \"created_at\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
    \"blocks\": [
      {
        \"type\": \"step\",
        \"id\": \"greet\",
        \"handler\": \"noop\",
        \"params\": { \"message\": \"Hello from Orch8!\" }
      },
      {
        \"type\": \"step\",
        \"id\": \"wait\",
        \"handler\": \"noop\",
        \"delay\": { \"duration\": 2000 }
      },
      {
        \"type\": \"step\",
        \"id\": \"finish\",
        \"handler\": \"noop\",
        \"params\": { \"message\": \"Done.\" }
      }
    ]
  }"
```

The sequence `id` and `created_at` are supplied by the client; `delay.duration`
is in milliseconds. (Tip: `orch8 init --template <name>` and `orch8 templates list`
scaffold ready-made sequences so you don't have to hand-write this JSON.)

### 5. Create an instance

```bash
INSTANCE_ID=$(ocurl -X POST "$ORCH8_URL/instances" \
  -H 'Content-Type: application/json' \
  -d "{
    \"sequence_id\": \"$SEQUENCE_ID\",
    \"tenant_id\": \"demo\",
    \"namespace\": \"default\",
    \"context\": {
      \"data\": { \"user\": \"alice\" }
    }
  }" | jq -r '.id')

echo "Instance: $INSTANCE_ID"
```

### 6. Watch it complete

```bash
# Poll until state is "completed"
watch -n 1 "curl -s -H 'x-api-key: $ORCH8_API_KEY' -H 'x-tenant-id: demo' \
  $ORCH8_URL/instances/$INSTANCE_ID | jq '{state, updated_at}'"
```

Or a simple loop:

```bash
while true; do
  STATE=$(ocurl "$ORCH8_URL/instances/$INSTANCE_ID" | jq -r '.state')
  echo "State: $STATE"
  [ "$STATE" = "completed" ] && break
  sleep 1
done
```

---

## Option B: Docker Compose

Runs the engine and Postgres together. Suitable for local development and staging.

### 1. Initialize a project

```bash
orch8 init my-project
cd my-project
```

### 2. Start the stack

```bash
docker compose up -d postgres
```

This starts Postgres on port 5434 (mapped from 5432). The compose file also
defines an `engine` container (`ghcr.io/orch8-io/engine`), but it needs
`ORCH8_API_KEY` and `ORCH8_ENCRYPTION_KEY` added under its `environment:`
before it will boot — for the quick start, run the engine locally against the
compose Postgres instead. Point `database.url` in `orch8.toml` at the
`postgres://orch8:orch8@localhost:5434/orch8` line (already scaffolded as a
comment), then:

```bash
export ORCH8_ENCRYPTION_KEY=$(openssl rand -hex 32)
orch8-server --config orch8.toml
```

Wait a few seconds for Postgres to be healthy, then check the engine
(health endpoints are unauthenticated):

```bash
curl -s http://localhost:8080/health/ready
```

### 3. Create a sequence and instance

Same commands as Option A, steps 4–6. The API surface is identical.

---

## Option C: TypeScript SDK

Use `@orch8.io/sdk` to author sequences in TypeScript. See the [SDK README](https://github.com/orch8-io/sdk-node) for the full surface.

### 1. Install

```bash
npm install @orch8.io/sdk
```

Requires Node 18+ (uses the global `fetch`). The server requires an API key by
default (`x-api-key` header) — see the SDK README for how to configure the
client's auth headers, or run the server with `--insecure` for a local demo.

### 2. Define a workflow and deploy it

```ts
import { workflow, Orch8Client } from "@orch8.io/sdk";

const hello = workflow("hello-world")
  .step("greet", "noop", { message: "Hello!" })
  .delay({ duration: 2_000 })
  .step("finish", "noop");

const client = new Orch8Client({
  baseUrl: "http://localhost:8080",
  tenantId: "demo",
  namespace: "default",
});

await client.createSequence(hello);
```

### 3. Start an instance

```ts
const { id } = await client.createInstance({
  sequence_name: "hello-world",
  context: { data: { user: "alice" } },
});

console.log("Instance:", id);
```

### 4. Poll for completion

```ts
async function waitForCompletion(instanceId: string, timeoutMs = 30_000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const inst = await client.getInstance(instanceId);
    if (inst.state === "completed" || inst.state === "failed") {
      return inst;
    }
    await new Promise((r) => setTimeout(r, 1_000));
  }
  throw new Error("Timed out waiting for instance to complete");
}

const result = await waitForCompletion(id);
console.log("Final state:", result.state);
```

To run handlers in Node instead of built-ins, see the [SDK documentation](https://github.com/orch8-io/sdk-node) and [External Workers](WORKERS.md).

---

## Sending Signals

You can pause and resume a running instance at any time using signals.

(`ocurl` is the authenticated-curl alias from Option A, step 3.)

### Pause

```bash
ocurl -X POST "$ORCH8_URL/instances/$INSTANCE_ID/signals" \
  -H 'Content-Type: application/json' \
  -d '{
    "signal_type": "pause",
    "payload": {}
  }'
```

### Resume

```bash
ocurl -X POST "$ORCH8_URL/instances/$INSTANCE_ID/signals" \
  -H 'Content-Type: application/json' \
  -d '{
    "signal_type": "resume",
    "payload": {}
  }'
```

### Cancel

```bash
ocurl -X POST "$ORCH8_URL/instances/$INSTANCE_ID/signals" \
  -H 'Content-Type: application/json' \
  -d '{
    "signal_type": "cancel",
    "payload": {}
  }'
```

### Update context mid-flight

Merge data into `context.data` while the instance is running (existing keys
not named in the payload are preserved; `config` cannot be changed this way):

```bash
ocurl -X POST "$ORCH8_URL/instances/$INSTANCE_ID/signals" \
  -H 'Content-Type: application/json' \
  -d '{
    "signal_type": "update_context",
    "payload": { "opened": true, "score": 92 }
  }'
```

---

## Checking the Dead Letter Queue

Instances that exhaust all retry attempts land in the DLQ (they remain as `failed` state and are surfaced through a dedicated endpoint).

```bash
# List all failed instances for a tenant
ocurl "$ORCH8_URL/instances/dlq?tenant_id=demo&namespace=default&limit=50" \
  | jq '[.[] | {id, sequence_id, updated_at, "error": .context.runtime.error}]'
```

Retry a specific failed instance:

```bash
ocurl -X POST "$ORCH8_URL/instances/$INSTANCE_ID/retry"
```

Bulk-cancel all failed instances for a sequence:

```bash
ocurl -X PATCH "$ORCH8_URL/instances/bulk/state" \
  -H 'Content-Type: application/json' \
  -d "{
    \"filter\": {
      \"tenant_id\": \"demo\",
      \"namespace\": \"default\",
      \"states\": [\"failed\"]
    },
    \"state\": \"cancelled\"
  }"
```

---

## Next Steps

- [API Reference](API.md) — full endpoint documentation
- [Architecture](ARCHITECTURE.md) — execution model, crate structure, performance
- [Configuration](CONFIGURATION.md) — all config options and environment variables
- [External Workers](WORKERS.md) — write handlers in Node, Python, Go, or anything that can POST JSON
- [Webhooks](WEBHOOKS.md) — subscribe to `instance.completed` and `instance.failed` events
- [Deployment](DEPLOYMENT.md) — Docker, Kubernetes, and managed-cloud setups
- [Embedding Use Cases](APPLICATIONS.md) — mobile, browser, desktop, IoT, and more
- [Externalized State](EXTERNALIZATION.md) — handling large payloads
- [Agent Patterns](agent-patterns/README.md) — reference sequences for AI agents
