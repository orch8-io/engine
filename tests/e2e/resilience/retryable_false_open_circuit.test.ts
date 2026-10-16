/**
 * External-worker failures and the circuit breaker (`orch8-api/src/workers.rs::fail_task`).
 *
 * Rule (L5-05): only a *retryable* worker failure counts toward the breaker.
 * A non-retryable failure means the dependency answered and the request was
 * wrong, so it fails the instance but leaves the breaker untouched.
 *
 *   - retryable=false → instance `failed`, breaker failure_count stays 0;
 *   - retryable=true on the only attempt → instance `failed`, breaker
 *     Closed with failure_count=1 (below the default threshold of 5).
 *
 * Trip-to-open after N failures is covered in `circuit_breaker_trip.test.ts`.
 *
 * SELF_MANAGED in `run-e2e.ts` because it touches globally-scoped
 * worker_tasks rows (same rationale as workers.test.ts).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

/** One attempt, so a single retryable failure fails the instance. */
const ONE_ATTEMPT = { retry: { max_attempts: 1, initial_backoff: 5, max_backoff: 20 } };

/** Run one instance whose only step is failed by a worker; return the breaker. */
async function failOnce(retryable: boolean) {
  const tenantId = `test-${uuid().slice(0, 8)}`;
  const handler = `worker_fail_handler_${uuid().slice(0, 8)}`;
  const seq = testSequence(
    "worker-failure-vs-breaker",
    [step("s1", handler, {}, ONE_ATTEMPT)],
    { tenantId },
  );
  await client.createSequence(seq);
  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  // Unknown handler name -> dispatched to the external worker queue.
  await client.waitForState(id, "waiting", { timeoutMs: 10_000 });
  const tasks = await client.pollWorkerTasks(handler, "worker-1");
  assert.equal(tasks.length, 1, "expected one worker task to be dispatched");

  await client.failWorkerTask(tasks[0]!.id, "worker-1", "boom", retryable);

  const failed = await client.waitForState(id, "failed", { timeoutMs: 10_000 });
  assert.equal(failed.state, "failed");
  return { breaker: await client.getCircuitBreaker(handler, tenantId), handler, tenantId };
}

describe("External worker failures and the circuit breaker", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("a non-retryable failure fails the instance without counting toward the breaker", async () => {
    const { breaker } = await failOnce(false);

    assert.equal(breaker.state, "closed");
    assert.equal(breaker.failure_count, 0, "permanent failures must not be recorded");
  });

  it("a retryable failure on the last attempt records exactly one breaker failure", async () => {
    const { breaker, handler, tenantId } = await failOnce(true);

    assert.equal(breaker.state, "closed", "one failure is below the threshold of 5");
    assert.equal(breaker.handler, handler);
    assert.equal(breaker.tenant_id, tenantId);
    assert.equal(breaker.failure_count, 1);
  });
});
