# Level 4: Execute application code in a worker

The engine should own orchestration state, scheduling, retries, and recovery.
Your services should own business logic and secrets. This level connects those
responsibilities through Orch8's pull-based worker protocol.

You will deliberately implement the protocol with the built-in Node.js `fetch`
API. This makes each request visible. For a real Node application, use
`@orch8.io/sdk` after you understand the four protocol operations.

**Starting point:** the authenticated Level 3 server is running and its CLI
environment variables are exported.

**Result:** an instance pauses at an application handler, a Node worker claims
the task, returns output, and the engine resumes the instance.

## 1. Create the worker workflow

From `quickstart-work`:

```bash
mkdir -p level-4
cd level-4
```

Create `sequence.json`:

```json
{
  "id": "019a0000-0000-7000-8000-000000000004",
  "tenant_id": "demo",
  "namespace": "default",
  "name": "customer-score",
  "version": 1,
  "input_schema": {
    "type": "object",
    "required": ["customer_id", "orders"],
    "properties": {
      "customer_id": { "type": "string", "minLength": 1 },
      "orders": { "type": "integer", "minimum": 0 }
    },
    "additionalProperties": false
  },
  "blocks": [
    {
      "type": "step",
      "id": "calculate_score",
      "handler": "calculate_customer_score",
      "params": {
        "customer_id": "{{data.customer_id}}",
        "orders": "{{data.orders}}"
      },
      "retry": {
        "max_attempts": 3,
        "initial_backoff": 500,
        "max_backoff": 2000,
        "backoff_multiplier": 2.0
      },
      "timeout": 30000,
      "output_schema": {
        "type": "object",
        "required": ["customer_id", "score", "segment"],
        "properties": {
          "customer_id": { "type": "string" },
          "score": { "type": "integer" },
          "segment": { "type": "string", "enum": ["new", "regular", "vip"] }
        },
        "additionalProperties": false
      }
    },
    {
      "type": "step",
      "id": "report",
      "handler": "log",
      "params": {
        "message": "Customer {{outputs.calculate_score.customer_id}} is {{outputs.calculate_score.segment}} with score {{outputs.calculate_score.score}}"
      }
    }
  ],
  "created_at": "2026-01-01T00:00:00Z"
}
```

`calculate_customer_score` is not a built-in handler. That is intentional.
When the scheduler reaches it, the engine creates a worker task and waits.

## 2. Preflight and publish

If this is a new terminal, restore the Level 3 client environment first:

```bash
export ORCH8_URL=http://127.0.0.1:8080/api/v1
export ORCH8_TENANT_ID=demo
export ORCH8_API_KEY='<the api_key value from level-3/orch8.toml>'
```

Then run:

```bash
orch8 sequence preflight --file sequence.json
orch8 sequence apply sequence.json
```

Preflight may warn that no live worker currently serves the external handler.
That warning is useful deployment evidence; the definition is structurally
valid, but an instance cannot finish until a worker appears.

Capture the sequence ID:

```bash
SEQUENCE_ID=$(orch8 --output json sequence lookup \
  demo default customer-score | jq -r '.id')
```

## 3. Create the smallest transparent worker

Create `worker.mjs`:

```js
const api = process.env.ORCH8_URL ?? "http://127.0.0.1:8080/api/v1";
const apiKey = process.env.ORCH8_API_KEY;
const tenant = process.env.ORCH8_TENANT_ID ?? "demo";
const workerId = `score-worker-${process.pid}`;
const handlerName = "calculate_customer_score";

if (!apiKey) {
  throw new Error("ORCH8_API_KEY is required");
}

const headers = {
  "content-type": "application/json",
  "x-api-key": apiKey,
  "x-tenant-id": tenant,
};

async function request(path, body) {
  const response = await fetch(`${api}${path}`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });

  const text = await response.text();
  const payload = text ? JSON.parse(text) : {};
  if (!response.ok) {
    throw new Error(`${response.status} ${JSON.stringify(payload)}`);
  }
  return payload;
}

function calculate(params) {
  const customerId = String(params.customer_id);
  const orders = Number(params.orders);
  const score = Math.min(100, orders * 10);
  const segment = score >= 80 ? "vip" : score >= 20 ? "regular" : "new";
  return { customer_id: customerId, score, segment };
}

console.log(`Worker ${workerId} polling ${api} for ${handlerName}`);

while (true) {
  const tasks = await request("/workers/tasks/poll", {
    handler_name: handlerName,
    worker_id: workerId,
    limit: 5,
    version: "1.0.0",
  });

  for (const task of tasks) {
    try {
      console.log(`Claimed task ${task.id}`, task.params);
      const output = calculate(task.params);
      await request(`/workers/tasks/${task.id}/complete`, {
        worker_id: workerId,
        output,
      });
      console.log(`Completed task ${task.id}`, output);
    } catch (error) {
      console.error(`Failed task ${task.id}`, error);
      await request(`/workers/tasks/${task.id}/fail`, {
        worker_id: workerId,
        message: String(error),
        retryable: true,
      });
    }
  }

  await new Promise((resolve) => setTimeout(resolve, 750));
}
```

No package installation is required because Node 18+ supplies `fetch`.

## 4. First observe the waiting boundary

Before starting the worker, create an instance:

```bash
INSTANCE_ID=$(orch8 --output json instance create \
  --sequence-id "$SEQUENCE_ID" \
  --tenant-id demo \
  --context '{"data":{"customer_id":"cust-42","orders":9}}' | jq -r '.id')
```

Wait a second, then inspect it:

```bash
orch8 instance get "$INSTANCE_ID"
orch8 instance diagnose "$INSTANCE_ID"
```

The instance is not broken. It has reached an external handler and needs a
worker that polls for `calculate_customer_score`.

## 5. Start the worker and watch the engine resume

In terminal C, with the same three client variables exported:

```bash
cd quickstart-work/level-4
node worker.mjs
```

The worker should claim and complete the queued task. In the CLI terminal:

```bash
orch8 instance get "$INSTANCE_ID" --watch
orch8 instance outputs "$INSTANCE_ID"
```

The output for `calculate_score` should contain a score of `90` and segment
`vip`. The engine then resolves those fields in the built-in `report` step.

## 6. Understand the protocol you just implemented

```text
worker -> POST /workers/tasks/poll
engine -> zero or more claimed tasks
worker -> application logic
worker -> POST /workers/tasks/{id}/complete
engine -> persist output and resume instance
```

There are four operations to retain:

| Operation | When to use it |
|---|---|
| Poll | Claim bounded work for one handler (or a named queue) |
| Heartbeat | Extend ownership of a task that may run for tens of seconds |
| Complete | Persist JSON output and resume the workflow |
| Fail | Report a retryable or permanent error |

The `worker_id` identifies one process. In a deployment, use a pod name or a
hostname/process combination. Do not configure every replica with the same ID.

## 7. Know the delivery contract

Worker execution is at-least-once around failures: a process can perform a side
effect and crash before acknowledging completion. Application handlers must use
idempotency keys when calling payment, messaging, or provisioning systems. A
good key combines the instance ID and block ID, or uses a domain operation ID
already present in the input.

Long handlers should heartbeat every 15–30 seconds. A worker that stops
heartbeating loses its claim after the configured timeout, allowing another
worker to recover the task.

## Checkpoint

You are ready for Level 5 when:

- The instance waits before the worker starts and completes after it starts.
- `instance outputs` shows the JSON produced in Node.
- You can explain why the engine persists the task and output while the worker
  owns only application logic.
- You know that side effects need idempotency even though task claiming prevents
  two healthy workers from claiming the same row simultaneously.

Leave the server running. Stop the worker with `Ctrl-C`; Level 5 will replace it
with a controllably failing version.

Next: [Observe failure and recover](05-failure-and-recovery.md).

## If something failed

| Symptom | Likely cause | Fix |
|---|---|---|
| Poll returns `401` | The worker shell lacks the Level 3 API key | Export `ORCH8_API_KEY` before running Node |
| Worker polls forever with no tasks | Handler names differ, or the instance never started | Compare `calculate_customer_score` in both files and inspect the instance |
| Instance remains waiting after completion | Completion used the wrong task or worker ID | Use `task.id` and the exact `workerId` that claimed it |
| Output schema rejects the result | Worker returned strings or a different field name | Return integer `score` plus `customer_id` and `segment` exactly as shown |
| Node says `fetch is not defined` | Node is older than 18 | Upgrade Node or use the official SDK with its supported runtime |
