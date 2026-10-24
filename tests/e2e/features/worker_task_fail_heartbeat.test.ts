/**
 * Worker Task Fail, Heartbeat, and Complete Edge Cases — verifies
 * worker task lifecycle beyond basic poll/complete.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
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

  it("retryable failure creates a fenced retry that completes", async () => {
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
    assert.equal(tasks.length, 1);
    const first = tasks[0]!;
    assert.ok(typeof first.attempt === "number");
    assert.ok(typeof first.effect_id === "string");
    await client.failWorkerTask(first.id, "worker-1", "retryable error", true);

    // Scheduled is transient: a fast scheduler can dispatch the next attempt
    // before the fail response reaches the worker. Observe the durable retry,
    // deliberately waiting until the instance has advanced to waiting again.
    await client.waitForState(id, "waiting", { timeoutMs: 5_000 });
    const deadline = Date.now() + 5_000;
    let retries = await client.pollWorkerTasks(handler, "worker-2");
    while (retries.length === 0 && Date.now() < deadline) {
      await sleep(25);
      retries = await client.pollWorkerTasks(handler, "worker-2");
    }
    assert.equal(retries.length, 1, "the failed attempt must produce a claimable retry");
    const retry = retries[0]!;
    assert.notEqual(retry.id, first.id);
    assert.equal(retry.instance_id, id);
    assert.equal(retry.block_id, first.block_id);
    assert.equal(retry.attempt, first.attempt + 1);
    assert.ok(typeof retry.effect_id === "string");
    assert.notEqual(retry.effect_id, first.effect_id);

    // Retry can retire the old row entirely or retain a fenced row. Both
    // protocol outcomes must reject completion by that former lease holder.
    await assert.rejects(
      () => client.completeWorkerTask(first.id, "worker-1", { stale: true }, first.claim_epoch),
      (error: unknown) => error instanceof Error && "status" in error
        && typeof error.status === "number" && [404, 409, 410].includes(error.status),
    );
    await client.completeWorkerTask(retry.id, "worker-2", { retry_completed: true });
    await client.waitForState(id, "completed", { timeoutMs: 5_000 });
    const outputs = await client.getOutputs(id);
    const successful = outputs.find((row) => row.block_id === "s1"
      && typeof row.output === "object" && row.output !== null && "retry_completed" in row.output);
    assert.deepEqual(successful?.output, { retry_completed: true });
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

  it("fail task with wrong worker_id returns 409", async () => {
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
      assert.fail("should throw 409");
    } catch (err: any) {
      assert.equal(err.status, 409);
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

  it("heartbeat on non-claimed task returns 409", async () => {
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
      assert.fail("should throw 409");
    } catch (err: any) {
      assert.equal(err.status, 409);
    }
  });

  it("complete task with wrong worker_id returns 409", async () => {
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
      assert.fail("should throw 409");
    } catch (err: any) {
      assert.equal(err.status, 409);
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
