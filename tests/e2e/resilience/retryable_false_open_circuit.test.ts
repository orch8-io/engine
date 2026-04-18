/**
 * Verifies the interaction between a non-retryable handler failure and the
 * circuit-breaker registry.
 *
 * Confirmed engine behaviour (see `tests/e2e/circuit-breaker.test.ts` for
 * the full write-up):
 *   - The breaker registry is in-memory and NOT called from the worker
 *     `fail` path or the scheduler. `record_failure` is unreferenced
 *     outside the module's own unit tests.
 *   - Therefore a `failWorkerTask(taskId, workerId, msg, retryable=false)`
 *     call fails the instance permanently without touching the breaker
 *     state for that handler.
 *   - `getCircuitBreaker(handler)` returns 404 for any handler that
 *     hasn't been seeded by a manual `reset` / `record`.
 *
 * This test locks in that current behaviour. A future change that wires
 * the breaker into the worker fail path will break this test (which is
 * the signal to rewrite it to assert trip-on-open semantics).
 *
 * NOTE: This test hits the worker-tasks table which is globally scoped
 * (same rationale as `workers.test.ts` / `worker_heartbeat_timeout.test.ts`
 * / `worker_task_timeout.test.ts`). It should be added to
 * SELF_MANAGED_SUITES in `run-e2e.ts`.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, ApiError, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Non-Retryable Error vs Open Circuit", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("retryable=false fails the instance without tripping the handler's breaker", async () => {
    const tenantId = `test-${uuid().slice(0, 8)}`;
    const handler = `non_retryable_handler_${uuid().slice(0, 8)}`;

    const seq = testSequence("retryable-false-vs-breaker", [
      step("s1", handler, {}),
    ], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    // Unknown handler name -> dispatched to external worker queue.
    await client.waitForState(id, "waiting", { timeoutMs: 10_000 });
    const tasks = await client.pollWorkerTasks(handler, "worker-1");
    assert.equal(tasks.length, 1, "expected one worker task to be dispatched");

    // Fail with retryable=false — instance should land in `failed` immediately.
    await client.failWorkerTask(
      tasks[0]!.id,
      "worker-1",
      "non-retryable error",
      false,
    );
    const failed = await client.waitForState(id, "failed", { timeoutMs: 10_000 });
    assert.equal(failed.state, "failed");

    // Breaker for this handler must remain UNTOUCHED — the engine doesn't
    // call record_failure on the worker fail path today. A GET should 404.
    await assert.rejects(
      () => client.getCircuitBreaker(handler),
      (err: unknown) => {
        assert.equal(
          (err as ApiError).status,
          404,
          "handler's breaker should not have been auto-created by a failed worker task",
        );
        return true;
      },
    );

    // When the engine wires record_failure into the worker fail path, this
    // test will start failing at the assertion above — that's the cue to
    // rewrite it as: open the breaker via N retryable failures, then assert
    // retryable=false does NOT consume a probe slot when the breaker is open.
  });
});
