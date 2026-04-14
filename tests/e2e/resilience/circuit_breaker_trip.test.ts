/**
 * End-to-end activation test for circuit-breaker trip-on-N-failures via the
 * external-worker fail path.
 *
 * Wiring under test (see `orch8-api/src/workers.rs::fail_task` and
 * `orch8-engine/src/circuit_breaker.rs`):
 *   - `AppState::circuit_breakers` is `Some` in the server binary, so the
 *     worker HTTP handlers can record success/failure into the same registry
 *     the scheduler consults at dispatch time.
 *   - `failWorkerTask(...)` triggers `record_failure` once per call, skipping
 *     the skip-list of pure control-flow built-ins. An unknown handler name
 *     (not registered in-process) is guaranteed to dispatch to the worker
 *     queue and thus flow through this path.
 *   - Default thresholds are failure_threshold=5, cooldown_secs=60
 *     (`orch8-server/src/main.rs::CircuitBreakerRegistry::new(5, 60)`).
 *
 * This test drives five consecutive external-worker failures (each its own
 * instance + sequence, because the worker-task table is not keyed by tenant
 * and we want clean polling) and asserts the breaker flips to `open`.
 *
 * SELF_MANAGED in `run-e2e.ts` because it touches globally-scoped worker_tasks
 * rows (same rationale as worker_task_timeout / worker_heartbeat_timeout).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

async function failOneWorkerTask(
  tenantId: string,
  handler: string,
): Promise<void> {
  const seq = testSequence(
    "cb-trip",
    [step("s1", handler, {})],
    { tenantId },
  );
  await client.createSequence(seq);

  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: tenantId,
    namespace: "default",
  });

  await client.waitForState(id, "waiting", { timeoutMs: 10_000 });
  const tasks = await client.pollWorkerTasks(handler, `worker-${uuid().slice(0, 4)}`);
  assert.equal(tasks.length, 1, "expected one worker task to be dispatched");

  await client.failWorkerTask(
    tasks[0]!.id,
    tasks[0]!.worker_id ?? "worker-x",
    "driven failure for CB trip test",
    false,
  );
  await client.waitForState(id, "failed", { timeoutMs: 10_000 });
}

describe("Circuit Breaker Trip-on-N-Failures", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it(
    "opens the breaker after failure_threshold external-worker failures",
    { timeout: 60_000 },
    async () => {
      const tenantId = `test-${uuid().slice(0, 8)}`;
      // Handler name must be unique per run so we don't inherit state from
      // an earlier suite on a shared server (persistence_recovery etc. don't
      // apply here because we use startServer, but it's cheap insurance).
      const handler = `cb_trip_${uuid().slice(0, 8)}`;

      // Sanity: breaker is untouched on creation.
      await assert.rejects(
        () => client.getCircuitBreaker(handler, tenantId),
        "breaker should not exist before first failure",
      );

      // Drive 5 failures sequentially. Parallelism would race the CB rehydrate
      // and the scheduler's dispatch gate — see `execute_step_block` — and
      // that's a different feature worth its own test.
      for (let i = 0; i < 5; i++) {
        await failOneWorkerTask(tenantId, handler);
      }

      // Breaker should now be Open. `getCircuitBreaker` auto-creates nothing;
      // a 200 here proves the worker fail path recorded into the registry.
      const breaker = await client.getCircuitBreaker(handler, tenantId);
      assert.equal(breaker.state, "open", "breaker must trip Open at threshold");
      assert.ok(
        typeof breaker.failure_count === "number" && breaker.failure_count >= 5,
        `expected failure_count >= 5, got ${String(breaker.failure_count)}`,
      );
      assert.equal(breaker.handler, handler);
      assert.equal(breaker.tenant_id, tenantId);
    },
  );
});
