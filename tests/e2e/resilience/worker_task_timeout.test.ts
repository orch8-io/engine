/**
 * Verifies that a worker task whose claimant never heartbeats is
 * automatically returned to `pending` and reassigned to a different worker.
 *
 * Relationship to `worker_heartbeat_timeout.test.ts`: both tests exercise
 * the same `reap_stale_worker_tasks` machinery (see
 * `orch8-engine/src/lib.rs` — 30s tick, 60s stale threshold), but this
 * test skips the initial heartbeat entirely so the stale window is
 * anchored at `claimed_at` (which the storage layer sets as the initial
 * `heartbeat_at`, per `orch8-storage/src/postgres/workers.rs::claim`).
 *
 * Engine gap: neither the tick interval nor the stale threshold is
 * configurable per-test or via env. A `StepDef.timeout` field exists but
 * is stamped onto the WorkerTask row as `timeout_ms` and is not enforced
 * by the reaper today — reclamation is purely heartbeat-driven. Closing
 * that gap (e.g. wiring StepDef.timeout into the reaper, or exposing
 * `ORCH8_RECLAIM_THRESHOLD_MS`) would let this test run in seconds
 * rather than minutes.
 *
 * This test is SELF_MANAGED in `run-e2e.ts` because it touches
 * globally-scoped worker_tasks rows.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { WorkerTask } from "../client.ts";

const client = new Orch8Client();

const RECLAIM_TIMEOUT_MS = 120_000;
const POLL_INTERVAL_MS = 1_000;

async function waitFor<T>(
  fn: () => Promise<T | undefined | null>,
  { timeoutMs = RECLAIM_TIMEOUT_MS, intervalMs = POLL_INTERVAL_MS }: { timeoutMs?: number; intervalMs?: number } = {},
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const v = await fn();
    if (v) return v;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(`Timeout after ${timeoutMs}ms waiting for condition`);
}

describe("Worker Task Claim Timeout", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it(
    "should auto-reassign a claimed task when no heartbeat arrives",
    { timeout: 180_000 },
    async () => {
      const tenantId = `test-${uuid().slice(0, 8)}`;
      const handler = `claim_timeout_${uuid().slice(0, 8)}`;

      // Try to bias the engine toward a shorter wait via StepDef.timeout —
      // currently ignored by the reaper but harmless to set. When the engine
      // closes the gap this test will get faster automatically.
      const seq = testSequence(
        "claim-timeout",
        [step("s1", handler, {}, { timeout: 5_000 })],
        { tenantId },
      );
      await client.createSequence(seq);

      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace: "default",
      });

      await client.waitForState(id, "waiting", { timeoutMs: 10_000 });

      // worker-1 claims the task but never heartbeats or completes it.
      const claimA = await client.pollWorkerTasks(handler, "worker-1");
      assert.equal(claimA.length, 1, "expected a single claimed task");
      const originalTaskId = claimA[0]!.id;
      assert.equal(claimA[0]!.worker_id, "worker-1");

      // Wait for the reaper to reclaim. worker-2 polling should eventually
      // receive the same row.
      const reclaimed = await waitFor<WorkerTask>(async () => {
        const tasks = await client.pollWorkerTasks(handler, "worker-2");
        return tasks.length > 0 ? tasks[0] : undefined;
      });

      assert.equal(
        reclaimed.id,
        originalTaskId,
        "reclaimed task should be the same row, not a new insert",
      );
      assert.equal(reclaimed.worker_id, "worker-2");
      assert.equal(reclaimed.state, "claimed");

      // worker-2 completes — the instance must finish cleanly.
      await client.completeWorkerTask(reclaimed.id, "worker-2", { ok: true });
      const done = await client.waitForState(id, "completed", { timeoutMs: 15_000 });
      assert.equal(done.state, "completed");
    },
  );
});
