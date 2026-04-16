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
  docker-compose.yml  # Engine + Postgres stack (for Option B)
```

### 3. Start the engine

```bash
orch8-server --config orch8.toml
```

The server starts on `http://localhost:8080`. Migrations run automatically.

### 4. Create a sequence

```bash
curl -s -X POST http://localhost:8080/sequences \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id": "demo",
    "namespace": "default",
    "name": "hello-world",
    "version": 1,
    "blocks": [
      {
        "type": "Step",
        "id": "greet",
        "handler": "noop",
        "params": { "message": "Hello from Orch8!" }
      },
      {
        "type": "Step",
        "id": "wait",
        "handler": "noop",
        "delay": "2s"
      },
      {
        "type": "Step",
        "id": "finish",
        "handler": "noop",
        "params": { "message": "Done." }
      }
    ]
  }'
```

Save the returned `id` as `SEQUENCE_ID`:

```bash
SEQUENCE_ID="550e8400-e29b-41d4-a716-446655440000"  # replace with actual id
```

### 5. Create an instance

```bash
INSTANCE_ID=$(curl -s -X POST http://localhost:8080/instances \
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
watch -n 1 "curl -s http://localhost:8080/instances/$INSTANCE_ID | jq '{state, updated_at}'"
```

Or a simple loop:

```bash
while true; do
  STATE=$(curl -s http://localhost:8080/instances/$INSTANCE_ID | jq -r '.state')
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
docker compose up -d
```

This starts Postgres on port 5432 and the engine on port 8080. Wait a few seconds for Postgres to be healthy, then check the engine:

```bash
curl -s http://localhost:8080/health/ready
```

### 3. Create a sequence and instance

Same commands as Option A, steps 4–6. The API surface is identical.

---

## Option C: Node.js SDK

Use the `@orch8/sdk` package to interact with the engine from TypeScript or JavaScript.

### 1. Install

```bash
npm install @orch8/sdk
```

### 2. Create a client

```ts
import { Orch8Client } from "@orch8/sdk";

const client = new Orch8Client({
  baseUrl: "http://localhost:8080",
  tenantId: "demo",
  namespace: "default",
});
```

### 3. Create a sequence

```ts
const seq = await client.sequences.create({
  name: "hello-world",
  version: 1,
  blocks: [
    {
      type: "Step",
      id: "greet",
      handler: "noop",
      params: { message: "Hello!" },
    },
    {
      type: "Step",
      id: "wait",
      handler: "noop",
      delay: "2s",
    },
    {
      type: "Step",
      id: "finish",
      handler: "noop",
    },
  ],
});

console.log("Sequence:", seq.id);
```

### 4. Start an instance

```ts
const instance = await client.instances.create({
  sequenceId: seq.id,
  context: {
    data: { user: "alice" },
  },
});

console.log("Instance:", instance.id);
```

### 5. Poll for completion

```ts
async function waitForCompletion(instanceId: string, timeoutMs = 30_000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const inst = await client.instances.get(instanceId);
    console.log("State:", inst.state);
    if (inst.state === "completed" || inst.state === "failed") {
      return inst;
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
  throw new Error("Timed out waiting for instance to complete");
}

const result = await waitForCompletion(instance.id);
console.log("Final state:", result.state);
```

---

## Sending Signals

You can pause and resume a running instance at any time using signals.

### Pause

```bash
curl -s -X POST http://localhost:8080/instances/$INSTANCE_ID/signals \
  -H 'Content-Type: application/json' \
  -d '{
    "signal_type": "pause",
    "payload": {}
  }'
```

### Resume

```bash
curl -s -X POST http://localhost:8080/instances/$INSTANCE_ID/signals \
  -H 'Content-Type: application/json' \
  -d '{
    "signal_type": "resume",
    "payload": {}
  }'
```

### Cancel

```bash
curl -s -X POST http://localhost:8080/instances/$INSTANCE_ID/signals \
  -H 'Content-Type: application/json' \
  -d '{
    "signal_type": "cancel",
    "payload": {}
  }'
```

### Update context mid-flight

Merge data into `context.data` while the instance is running:

```bash
curl -s -X POST http://localhost:8080/instances/$INSTANCE_ID/signals \
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
curl -s "http://localhost:8080/instances/dlq?tenant_id=demo&namespace=default&limit=50" \
  | jq '[.[] | {id, sequence_id, updated_at, "error": .context.runtime.error}]'
```

Retry a specific failed instance:

```bash
curl -s -X POST http://localhost:8080/instances/$INSTANCE_ID/retry
```

Bulk-cancel all failed instances for a sequence:

```bash
curl -s -X PATCH http://localhost:8080/instances/bulk/state \
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
