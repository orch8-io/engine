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
 * The reaper cadence/threshold come from `SchedulerConfig` and are now
 * env-overridable (`ORCH8_WORKER_REAPER_TICK_SECS` /
 * `ORCH8_WORKER_REAPER_STALE_SECS`). This suite boots its own server with
 * a 1s tick / 2s stale window so reclamation happens in seconds instead of
 * the production defaults (30s / 60s) that made the test run for minutes.
 *
 * This test is SELF_MANAGED in `self-managed.ts` because it touches
 * globally-scoped worker_tasks rows AND needs the low-threshold env that
 * the shared attach-mode server doesn't set.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { WorkerTask } from "../client.ts";

const client = new Orch8Client();

// With a 2s stale window + 1s reaper tick (set via env below) reclamation
// lands within ~3-5s. Keep a generous ceiling for slow/loaded CI runners.
const RECLAIM_TIMEOUT_MS = 30_000;
const POLL_INTERVAL_MS = 500;

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
    server = await startServer({
      env: {
        ORCH8_WORKER_REAPER_TICK_SECS: "1",
        ORCH8_WORKER_REAPER_STALE_SECS: "2",
      },
    });
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
