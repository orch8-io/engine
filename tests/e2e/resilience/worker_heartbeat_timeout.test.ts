/**
 * Verifies that a claimed worker task whose worker stops heartbeating is
 * reaped back to `pending` by the engine's stale-task reaper, and becomes
 * available for re-polling by a new worker.
 *
 * Engine timing (see `orch8-engine/src/lib.rs`):
 *   - The reaper runs on a 30-second tick with a 60-second stale threshold.
 *   - Effective wall-clock for reclamation: 60-90 seconds after the last
 *     heartbeat.
 *   - Neither knob is currently configurable via env or per-test. That's an
 *     engine gap worth closing for test ergonomics.
 *
 * This test therefore waits ~120s in the worst case. It's marked as
 * SELF_MANAGED in `run-e2e.ts` because it touches globally-scoped
 * worker_tasks rows.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { WorkerTask } from "../client.ts";

const client = new Orch8Client();

// Worst-case reclamation window: 60s stale threshold + 30s tick interval.
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

describe("Worker Heartbeat Timeout", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it(
    "should reset stale claimed task to pending after heartbeat gap",
    { timeout: 180_000 },
    async () => {
      const tenantId = `test-${uuid().slice(0, 8)}`;
      const handler = `hb_timeout_${uuid().slice(0, 8)}`;

      const seq = testSequence("hb-timeout", [step("s1", handler, {})], { tenantId });
      await client.createSequence(seq);

      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace: "default",
      });

      await client.waitForState(id, "waiting", { timeoutMs: 10_000 });

      // worker-A claims, sends one heartbeat, then stops.
      const claimA = await client.pollWorkerTasks(handler, "worker-A");
      assert.equal(claimA.length, 1, "expected a single claimed task");
      const originalTaskId = claimA[0]!.id;
      await client.heartbeatWorkerTask(originalTaskId, "worker-A");

      // Wait for the reaper to reclaim the stale claim. Observable signal:
      // a fresh poll from worker-B eventually succeeds for the same
      // instance/block pair.
      const reclaimed = await waitFor<WorkerTask>(async () => {
        const tasks = await client.pollWorkerTasks(handler, "worker-B");
        return tasks.length > 0 ? tasks[0] : undefined;
      });

      // The reaper resets the existing row (rather than creating a new one),
      // so the task id stays stable but worker_id flips to worker-B.
      assert.equal(
        reclaimed.id,
        originalTaskId,
        "reclaimed task should reuse the original worker_tasks row",
      );
      assert.equal(reclaimed.worker_id, "worker-B");
      assert.equal(reclaimed.state, "claimed");

      // worker-B finishes the job normally.
      await client.completeWorkerTask(reclaimed.id, "worker-B", { ok: true });
      const done = await client.waitForState(id, "completed", { timeoutMs: 15_000 });
      assert.equal(done.state, "completed");
    },
  );
});
