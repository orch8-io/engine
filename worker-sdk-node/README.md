# @orch8/worker-sdk

Node.js SDK for building [orch8.io](https://orch8.io) external workers.

A worker polls the engine for tasks targeting handler names it knows, runs them, sends heartbeats, and reports success or failure. Use it when you want to keep handler code in your existing Node service instead of the Rust engine binary.

## Install

```bash
npm install @orch8/worker-sdk
```

Requires Node 18+ (uses the global `fetch`).

## Quick start

```ts
import { Orch8Worker } from "@orch8/worker-sdk";

const worker = new Orch8Worker({
  engineUrl: "http://localhost:8080",
  workerId: "node-worker-1",
  handlers: {
    send_email: async (task) => {
      const { to, subject, body } = task.params as {
        to: string;
        subject: string;
        body: string;
      };
      // ...call your email provider...
      return { message_id: "msg-123", delivered: true };
    },
    enrich_contact: async (task) => {
      // Return whatever JSON you want exposed as this block's output.
      return { tier: "enterprise", score: 92 };
    },
  },
});

await worker.start();

process.on("SIGTERM", () => worker.stop());
```

## How it works

1. The worker calls `POST /workers/tasks/poll` for each handler name it owns.
2. The engine returns up to `limit` tasks and locks them to this `workerId` (via `FOR UPDATE SKIP LOCKED`). Concurrent workers never get the same task.
3. For each claimed task the SDK invokes your handler function.
4. A heartbeat loop pings `POST /workers/tasks/{id}/heartbeat` every `heartbeatIntervalMs`. Tasks with no heartbeat for 60s are reclaimed by the engine's reaper.
5. On success: `POST /workers/tasks/{id}/complete` with the return value as `output`.
6. On thrown error: `POST /workers/tasks/{id}/fail` with `retryable = true` unless you throw a `NonRetryableError`.

## Config

| Field | Default | Description |
|-------|---------|-------------|
| `engineUrl` | _required_ | Base URL of the engine API |
| `workerId` | _required_ | Unique identifier per worker process |
| `handlers` | _required_ | Map of handler name → async function |
| `pollIntervalMs` | `1000` | Poll cadence per handler |
| `heartbeatIntervalMs` | `15_000` | Heartbeat cadence for in-flight tasks |
| `maxConcurrent` | `10` | Max concurrent tasks across all handlers |

## Error handling

Throw any error to mark the task as failed. By default failures are classified as **retryable** — the engine re-queues the task and respects the block's retry policy. To mark a permanent failure:

```ts
import { Orch8Worker, NonRetryableError } from "@orch8/worker-sdk";

handlers: {
  charge_card: async (task) => {
    const { token } = task.params as { token: string };
    const result = await stripe.charge(token);
    if (result.error?.type === "card_declined") {
      throw new NonRetryableError("card_declined");
    }
    return result;
  },
}
```

Permanent failures skip retries and land the instance in the DLQ immediately.

## Graceful shutdown

```ts
process.on("SIGTERM", async () => {
  await worker.stop(); // stops polling, waits for in-flight tasks to finish
  process.exit(0);
});
```

## Licensing

MIT — so you can embed the worker inside your own closed-source service. The engine itself is BUSL-1.1.

## See also

- [External Workers guide](../docs/WORKERS.md) — protocol-level docs, writing workers in other languages
- [`@orch8/workflow-sdk`](../workflow-sdk-node/README.md) — author sequences in TypeScript
