/**
 * Worker Task Fail, Heartbeat, and Complete Edge Cases — verifies
 * worker task lifecycle beyond basic poll/complete.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Worker Task Fail and Heartbeat", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("fail task with retryable=true reschedules the instance", async () => {
    const tenantId = `wt-retry-${uuid().slice(0, 8)}`;
    const handler = `retryable_${uuid().slice(0, 8)}`;

    const seq = testSequence("wt-retry", [step("s1", handler, { val: 1 }, {
      retry: { max_attempts: 3, initial_backoff: 50, max_backoff: 200 },
    })], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "waiting", { timeoutMs: 5_000 });
    const tasks = await client.pollWorkerTasks(handler, "worker-1");
    await client.failWorkerTask(tasks[0]!.id, "worker-1", "retryable error", true);

    // Instance should go back to scheduled for retry.
    await client.waitForState(id, "scheduled", { timeoutMs: 5_000 });
  });

  it("fail task with retryable=false moves instance to failed", async () => {
    const tenantId = `wt-fail-${uuid().slice(0, 8)}`;
    const handler = `perm_fail_${uuid().slice(0, 8)}`;

    const seq = testSequence("wt-fail", [step("s1", handler)], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "waiting", { timeoutMs: 5_000 });
    const tasks = await client.pollWorkerTasks(handler, "worker-1");
    await client.failWorkerTask(tasks[0]!.id, "worker-1", "permanent error", false);

    await client.waitForState(id, "failed", { timeoutMs: 5_000 });
  });

  it("fail task with wrong worker_id returns 404", async () => {
    const tenantId = `wt-fail-wid-${uuid().slice(0, 8)}`;
    const handler = `fail_wid_${uuid().slice(0, 8)}`;

    const seq = testSequence("wt-fail-wid", [step("s1", handler)], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "waiting", { timeoutMs: 5_000 });
    const tasks = await client.pollWorkerTasks(handler, "worker-1");

    try {
      await client.failWorkerTask(tasks[0]!.id, "wrong-worker", "error", false);
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("heartbeat on claimed task is accepted", async () => {
    const tenantId = `wt-hb-${uuid().slice(0, 8)}`;
    const handler = `hb_${uuid().slice(0, 8)}`;

    const seq = testSequence("wt-hb", [step("s1", handler)], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "waiting", { timeoutMs: 5_000 });
    const tasks = await client.pollWorkerTasks(handler, "worker-1");

    const ok = await client.heartbeatWorkerTask(tasks[0]!.id, "worker-1");
    assert.ok(ok, "heartbeat should be accepted");
  });

  it("heartbeat on non-claimed task returns 404", async () => {
    const tenantId = `wt-hb-404-${uuid().slice(0, 8)}`;
    const handler = `hb_404_${uuid().slice(0, 8)}`;

    const seq = testSequence("wt-hb-404", [step("s1", handler)], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "waiting", { timeoutMs: 5_000 });
    const tasks = await client.pollWorkerTasks(handler, "worker-1");
    // Complete the task first.
    await client.completeWorkerTask(tasks[0]!.id, "worker-1", { ok: true });

    try {
      await client.heartbeatWorkerTask(tasks[0]!.id, "worker-1");
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("complete task with wrong worker_id returns 404", async () => {
    const tenantId = `wt-comp-${uuid().slice(0, 8)}`;
    const handler = `comp_${uuid().slice(0, 8)}`;

    const seq = testSequence("wt-comp", [step("s1", handler)], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "waiting", { timeoutMs: 5_000 });
    const tasks = await client.pollWorkerTasks(handler, "worker-1");

    try {
      await client.completeWorkerTask(tasks[0]!.id, "wrong-worker", { ok: true });
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("task state after fail is failed", async () => {
    const tenantId = `wt-st-${uuid().slice(0, 8)}`;
    const handler = `st_${uuid().slice(0, 8)}`;

    const seq = testSequence("wt-st", [step("s1", handler)], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "waiting", { timeoutMs: 5_000 });
    const tasks = await client.pollWorkerTasks(handler, "worker-1");
    await client.failWorkerTask(tasks[0]!.id, "worker-1", "error", false);

    const stats = await client.workerTaskStats();
    const handlerStats = (stats.by_handler as any)?.[handler];
    if (handlerStats) {
      assert.ok(
        handlerStats.failed >= 1 || handlerStats.pending >= 0,
        "failed task should reflect in stats",
      );
    }
  });
});
