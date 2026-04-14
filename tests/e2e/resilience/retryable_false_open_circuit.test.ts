/**
 * Verifies that a single non-retryable external-worker failure records into
 * the circuit-breaker registry without tripping it, now that the worker fail
 * path is wired into `record_failure` (see `orch8-api/src/workers.rs::fail_task`).
 *
 * Context:
 *   - The breaker registry is wired into BOTH the in-process step-dispatch
 *     path (fast + tree) and the external-worker fail path. An unknown
 *     handler name (not registered in-process) dispatches to the worker
 *     queue; `failWorkerTask(..., retryable=false)` then transitions the
 *     instance to `failed` AND invokes `record_failure` on the registry.
 *   - Default threshold = 5 failures, cooldown = 60 s
 *     (`orch8-server/src/main.rs::CircuitBreakerRegistry::new(5, 60)`).
 *   - `is_breaker_tracked` skips pure control-flow built-ins. The dynamic
 *     handler name generated here is never in that skip list, so recording
 *     proceeds.
 *
 * Activation of N-failure trip-to-open is covered in
 * `circuit_breaker_trip.test.ts`. This test exercises the single-failure
 * seam: one retryable=false failure must land in the registry as
 * Closed/failure_count=1 — proof that the worker fail path is recording at
 * all, and that retryable=false isn't somehow skipped.
 *
 * SELF_MANAGED in `run-e2e.ts` because it touches globally-scoped
 * worker_tasks rows (same rationale as workers.test.ts,
 * worker_heartbeat_timeout.test.ts, worker_task_timeout.test.ts).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Non-Retryable Worker Failure Records into CB Registry", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("retryable=false fails the instance AND increments the handler's breaker", async () => {
    const tenantId = `test-${uuid().slice(0, 8)}`;
    const handler = `non_retryable_handler_${uuid().slice(0, 8)}`;

    const seq = testSequence(
      "retryable-false-vs-breaker",
      [step("s1", handler, {})],
      { tenantId },
    );
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

    // Fail with retryable=false -> instance lands in `failed` immediately,
    // and `record_failure` fires exactly once against (tenantId, handler).
    await client.failWorkerTask(
      tasks[0]!.id,
      "worker-1",
      "non-retryable error",
      false,
    );
    const failed = await client.waitForState(id, "failed", { timeoutMs: 10_000 });
    assert.equal(failed.state, "failed");

    // The breaker must now exist as Closed with failure_count=1 — a single
    // failure is below the default threshold (5) so no trip. This also
    // guards against record_failure being bypassed on the retryable=false
    // branch (the pre-Item-A gap this test replaces).
    const breaker = await client.getCircuitBreaker(handler, tenantId);
    assert.equal(breaker.state, "closed", "single failure must not trip the breaker");
    assert.equal(breaker.handler, handler);
    assert.equal(breaker.tenant_id, tenantId);
    assert.equal(
      breaker.failure_count,
      1,
      "exactly one failure should be recorded",
    );
  });
});
