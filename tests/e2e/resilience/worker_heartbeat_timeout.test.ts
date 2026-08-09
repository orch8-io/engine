/**
 * Verifies that a claimed worker task whose worker stops heartbeating is
 * reaped back to `pending` by the engine's stale-task reaper, and becomes
 * available for re-polling by a new worker.
 *
 * Engine timing (see `orch8-engine/src/lib.rs`): the reaper cadence and
 * stale threshold come from `SchedulerConfig` and are env-overridable
 * (`ORCH8_WORKER_REAPER_TICK_SECS` / `ORCH8_WORKER_REAPER_STALE_SECS`).
 * This suite boots its own server with a 1s tick / 2s stale window so
 * reclamation lands in seconds instead of the production 30s/60s.
 *
 * SELF_MANAGED in `self-managed.ts`: it touches globally-scoped
 * worker_tasks rows AND needs the low-threshold env.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { ApiError } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { WorkerTask } from "../client.ts";

const client = new Orch8Client();

// With a 2s stale window + 1s reaper tick (set via env below) reclamation
// lands within ~3-5s. Generous ceiling for slow/loaded CI runners.
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

describe("Worker Heartbeat Timeout", () => {
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
      assert.equal(claimA[0]!.claim_epoch, 1);
      const heartbeat = await client.heartbeatWorkerTask(originalTaskId, "worker-A", {
        checkpoint: { completed_batches: 4, cursor: "resume-here" },
        checkpointSeq: 0,
      });
      assert.equal(heartbeat.checkpoint_seq, 1);

      // Wait for the reaper to reclaim the stale claim. Observable signal:
      // a fresh process reusing worker-A's stable identity eventually succeeds
      // for the same instance/block pair.
      const reclaimed = await waitFor<WorkerTask>(async () => {
        const tasks = await client.pollWorkerTasks(handler, "worker-A");
        return tasks.length > 0 ? tasks[0] : undefined;
      });

      // The reaper resets the existing row (rather than creating a new one),
      // so the task id stays stable. The worker id is deliberately reused:
      // only claim_epoch can distinguish the old process from the new one.
      assert.equal(
        reclaimed.id,
        originalTaskId,
        "reclaimed task should reuse the original worker_tasks row",
      );
      assert.equal(reclaimed.worker_id, "worker-A");
      assert.equal(reclaimed.state, "claimed");
      assert.equal(reclaimed.claim_epoch, 2);
      assert.equal(reclaimed.checkpoint_seq, 1);
      assert.deepEqual(reclaimed.resume_checkpoint, {
        completed_batches: 4,
        cursor: "resume-here",
      });

      // The old process may still be alive and may reuse the same stable
      // worker_id after a restart. Its generation-1 lease must remain fenced.
      await assert.rejects(
        () => client.heartbeatWorkerTask(
          originalTaskId,
          "worker-A",
          undefined,
          claimA[0]!.claim_epoch,
        ),
        (error: unknown) => error instanceof ApiError && error.status === 409,
      );

      const beforeCompletion = await client.listWorkerTaskAttempts(originalTaskId);
      assert.deepEqual(
        beforeCompletion.map((event) => event.event),
        ["claimed", "reclaimed", "claimed", "stale_mutation_rejected"],
      );
      assert.equal(beforeCompletion[1]!.worker_id, "worker-A");
      assert.equal(beforeCompletion[2]!.worker_id, "worker-A");

      // The replacement worker-A process finishes with generation 2.
      await client.completeWorkerTask(
        reclaimed.id,
        "worker-A",
        { ok: true },
        reclaimed.claim_epoch,
      );
      const done = await client.waitForState(id, "completed", { timeoutMs: 15_000 });
      assert.equal(done.state, "completed");

      const afterCompletion = await client.listWorkerTaskAttempts(originalTaskId);
      assert.equal(afterCompletion.at(-1)!.event, "completed");
      assert.equal(afterCompletion.at(-1)!.claim_epoch, 2);
    },
  );
});
