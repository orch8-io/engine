/**
 * Verifies what happens to a worker task whose claimant never heartbeats.
 * External handlers are side-effecting, so the lease reaper never hands the
 * same attempt to another worker: the effect receipt goes `unknown` and the
 * step's retry policy decides (new attempt, or the step fails).
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
    "retries a side-effecting task as a new attempt (never silently requeues the same effect)",
    { timeout: 180_000 },
    async () => {
      const tenantId = `test-${uuid().slice(0, 8)}`;
      const handler = `claim_timeout_${uuid().slice(0, 8)}`;

      // Every external handler is side-effecting: when worker-1's lease
      // expires the effect may already have happened, so the reaper marks
      // the receipt `unknown` and applies the retry policy instead of
      // handing the same attempt to worker-2.
      const seq = testSequence(
        "claim-timeout",
        [step("s1", handler, {}, { retry: { max_attempts: 2, initial_backoff: 5, max_backoff: 20 } })],
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
      const original = claimA[0]!;
      assert.equal(original.worker_id, "worker-1");
      assert.equal(original.attempt, 0);

      // The retry is a new attempt on a new row with its own effect id.
      const reclaimed = await waitFor<WorkerTask>(async () => {
        const tasks = await client.pollWorkerTasks(handler, "worker-2");
        return tasks.length > 0 ? tasks[0] : undefined;
      });
      assert.notEqual(reclaimed.id, original.id, "a new attempt, not the same claim");
      assert.equal(reclaimed.attempt, 1);
      assert.equal(reclaimed.worker_id, "worker-2");
      if (original.effect_id && reclaimed.effect_id) {
        assert.notEqual(reclaimed.effect_id, original.effect_id);
      }

      // The stale worker-1 can no longer report on its attempt.
      await assert.rejects(
        () => client.completeWorkerTask(original.id, "worker-1", { ok: true }, original.claim_epoch),
      );

      // worker-2 completes — the instance must finish cleanly.
      await client.completeWorkerTask(reclaimed.id, "worker-2", { ok: true }, reclaimed.claim_epoch);
      const done = await client.waitForState(id, "completed", { timeoutMs: 15_000 });
      assert.equal(done.state, "completed");
    },
  );

  it(
    "fails the step when the lease expires and no retry policy allows another attempt",
    { timeout: 180_000 },
    async () => {
      const tenantId = `test-${uuid().slice(0, 8)}`;
      const handler = `claim_timeout_noretry_${uuid().slice(0, 8)}`;
      const seq = testSequence("claim-timeout-noretry", [step("s1", handler, {})], { tenantId });
      await client.createSequence(seq);
      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace: "default",
      });
      await client.waitForState(id, "waiting", { timeoutMs: 10_000 });
      const claimA = await client.pollWorkerTasks(handler, "worker-1");
      assert.equal(claimA.length, 1);
      const failed = await client.waitForState(id, "failed", { timeoutMs: 30_000 });
      assert.equal(failed.state, "failed");
      assert.equal((await client.pollWorkerTasks(handler, "worker-2")).length, 0);
    },
  );
});
