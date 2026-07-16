# Level 5: Observe failure and recover

Happy-path demos do not show why durable orchestration matters. This level
forces an external handler to fail, watches the retry policy exhaust, examines
the dead-letter queue, fixes the worker, and retries the same instance.

**Starting point:** the Level 3 server is running, the Level 4 sequence is
published, and the Level 4 worker is stopped.

**Result:** an instance moves from retries to `failed`, becomes diagnosable in
the DLQ, and completes after the worker is repaired and the instance retried.

## 1. Add a controlled failure switch

In `quickstart-work/level-4/worker.mjs`, add this line after `handlerName`:

```js
const failureMode = process.env.WORKER_FAILURE_MODE ?? "never";
```

Then add these lines at the beginning of `calculate`:

```js
if (failureMode === "always") {
  throw new Error("simulated scoring service outage");
}
```

The function should now begin like this:

```js
function calculate(params) {
  if (failureMode === "always") {
    throw new Error("simulated scoring service outage");
  }

  const customerId = String(params.customer_id);
  const orders = Number(params.orders);
  // Existing scoring code continues here.
}
```

This is a test seam, not a production retry mechanism. The worker reports the
error to Orch8; the sequence's retry policy remains the source of truth.

## 2. Start the worker in failure mode

In the worker terminal:

```bash
cd quickstart-work/level-4
WORKER_FAILURE_MODE=always node worker.mjs
```

The Level 4 worker reports every thrown error with `retryable: true`. The
sequence permits three attempts with 500 ms, then 1,000 ms backoff (capped at
2,000 ms).

## 3. Create a doomed instance

In the CLI terminal, restore `SEQUENCE_ID` if necessary:

```bash
cd quickstart-work/level-4
SEQUENCE_ID=$(orch8 --output json sequence lookup \
  demo default customer-score | jq -r '.id')
```

Create an instance:

```bash
FAILED_INSTANCE_ID=$(orch8 --output json instance create \
  --sequence-id "$SEQUENCE_ID" \
  --tenant-id demo \
  --context '{"data":{"customer_id":"cust-failure","orders":4}}' | jq -r '.id')
```

Watch it:

```bash
orch8 instance get "$FAILED_INSTANCE_ID" --watch
```

The worker log should show three claimed attempts. After the final retryable
failure, the instance reaches terminal `failed` rather than retrying forever.

## 4. Diagnose before changing anything

Inspect the instance and execution tree:

```bash
orch8 instance diagnose "$FAILED_INSTANCE_ID"
orch8 instance tree "$FAILED_INSTANCE_ID"
orch8 instance outputs "$FAILED_INSTANCE_ID"
```

Then list the DLQ:

```bash
orch8 instance dlq --tenant-id demo
orch8 instance dlq --tenant-id demo --groups
```

Individual DLQ entries answer “which runs failed?” Grouped output answers
“which root cause is producing the most failures?” Grouping is the better first
view during a broad incident because hundreds of instances can share one
fingerprint.

Do not press retry yet. Retrying while the dependency is still broken consumes
attempts and adds noise without changing the outcome.

## 5. Repair the worker

Stop the failing worker with `Ctrl-C`. Restart it without the failure variable:

```bash
node worker.mjs
```

This simulates deploying a corrected application service. The workflow
definition has not changed, so a new sequence version is unnecessary.

## 6. Retry the same failed instance

In the CLI terminal:

```bash
orch8 instance retry "$FAILED_INSTANCE_ID"
orch8 instance get "$FAILED_INSTANCE_ID" --watch
orch8 instance outputs "$FAILED_INSTANCE_ID"
```

The original instance ID should now complete. Its durable execution evidence
still shows the earlier failure path; recovery does not erase the incident.

## 7. Separate the three recovery decisions

Use this decision rule in real systems:

| Situation | Correct action |
|---|---|
| Transient dependency failure and attempts remain | Let the sequence retry automatically |
| Attempts exhausted but the dependency is now healthy | Manually retry the failed instance or a controlled batch |
| Workflow logic or handler contract changed | Publish a new immutable sequence version, validate it, then decide how to migrate or replay affected work |

A retry is not a release. A retry re-executes failed work under the instance's
existing definition. A release changes routing for newly created instances.

## 8. Understand retry safety

The worker in this tutorial performs a pure calculation, so repetition is
safe. A payment or email handler must be idempotent. Consider the crash window:

```text
worker sends payment -> provider accepts -> worker crashes -> no completion ACK
```

Orch8 will eventually offer the task again because it cannot infer the remote
side effect. The handler should send a stable idempotency key to the provider
and return the provider's original result on duplicate attempts.

Use permanent failure (`retryable: false`) for errors that time cannot fix, such
as an invalid recipient or a deleted domain object. Put known business
alternatives inside `try_catch` when the workflow has a legitimate fallback.

## 9. Practice an operator loop

The durable incident loop is:

```text
detect -> group -> diagnose -> repair dependency -> retry bounded scope -> verify
```

For a production incident:

1. Start with DLQ groups, not a blind bulk retry.
2. Inspect a representative instance and its block output/error evidence.
3. Confirm the dependency or worker fix using one canary retry.
4. Retry a bounded group only after the canary succeeds.
5. Watch error rate and queue depth while draining the backlog.

## Checkpoint

You are ready for Level 6 when:

- You observed exactly the configured bounded retry behavior.
- You found the instance in individual and grouped DLQ views.
- You diagnosed before retrying.
- The same instance completed after the worker was restarted in healthy mode.
- You can distinguish automatic attempt retry, manual instance retry, and a new
  sequence release.

Next: [Ship a guarded release](06-production-release.md).

## If something failed

| Symptom | Likely cause | Fix |
|---|---|---|
| Instance completes instead of failing | The failure switch was not added or exported | Confirm `WORKER_FAILURE_MODE=always` in the worker process |
| Instance waits forever | No worker is polling | Start the worker and compare the handler name |
| Retry immediately fails again | The failing worker is still running, or another failing replica exists | Stop all failing replicas before starting the healthy worker |
| `instance retry` is rejected | The instance is not terminal `failed` | Wait for attempts to exhaust and inspect its current state |
| More than one worker processes attempts | Multiple worker processes are running | This is supported; stop extras to make the tutorial log easier to follow |

