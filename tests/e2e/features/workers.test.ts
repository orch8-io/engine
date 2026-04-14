import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { WorkerTask } from "../types.ts";

const client = new Orch8Client();

/**
 * Poll worker tasks from a specific named queue. The default client only
 * exposes the handler-wide poll; queue-scoped polling hits
 * `/workers/tasks/poll/queue` and is needed for the queue-routing tests.
 */
async function pollQueue(
  queueName: string,
  handlerName: string,
  workerId: string,
  limit: number = 5,
): Promise<WorkerTask[]> {
  const res = await fetch(`${client.baseUrl}/workers/tasks/poll/queue`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      queue_name: queueName,
      handler_name: handlerName,
      worker_id: workerId,
      limit,
    }),
  });
  if (!res.ok) {
    throw new Error(`pollQueue failed: ${res.status} ${await res.text()}`);
  }
  return (await res.json()) as WorkerTask[];
}

describe("External Workers", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should dispatch unknown handler to worker queue and complete via poll", async () => {
    const seq = testSequence("ext-worker", [
      step("s1", "my_custom_handler", { foo: "bar" }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Instance should transition to waiting (external handler not in registry).
    const waiting = await client.waitForState(id, "waiting");
    assert.equal(waiting.state, "waiting");

    // Poll for worker tasks.
    const tasks = await client.pollWorkerTasks("my_custom_handler", "worker-1");
    assert.equal(tasks.length, 1);
    assert.equal(tasks[0]!.handler_name, "my_custom_handler");
    assert.equal(tasks[0]!.state, "claimed");
    assert.deepEqual(tasks[0]!.params, { foo: "bar" });

    // Complete the task.
    await client.completeWorkerTask(tasks[0]!.id, "worker-1", {
      result: "done",
    });

    // Instance should complete.
    const completed = await client.waitForState(id, "completed");
    assert.equal(completed.state, "completed");

    // Verify block output was saved.
    const outputs = await client.getOutputs(id);
    assert.equal(outputs.length, 1);
    assert.equal(outputs[0]!.block_id, "s1");
    assert.deepEqual(outputs[0]!.output, { result: "done" });
  });

  it("should handle retryable failure and re-dispatch", async () => {
    // Retry policy is REQUIRED — without a retry config, the server rejects
    // re-dispatch even when the worker sets `retryable: true`. Matches the
    // "no retry policy → fail immediately" branch in orch8-api/workers.rs.
    const seq = testSequence("ext-retry", [
      step(
        "s1",
        "flaky_handler",
        { attempt: 1 },
        {
          retry: {
            max_attempts: 3,
            initial_backoff: 100,
            max_backoff: 1_000,
          },
        },
      ),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for instance to go to waiting.
    await client.waitForState(id, "waiting");

    // Poll and fail with retryable.
    const tasks1 = await client.pollWorkerTasks("flaky_handler", "worker-1");
    assert.equal(tasks1.length, 1);
    await client.failWorkerTask(
      tasks1[0]!.id,
      "worker-1",
      "transient error",
      true
    );

    // Instance should go back to scheduled, then running, then waiting again.
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });

    // Poll again and complete.
    const tasks2 = await client.pollWorkerTasks("flaky_handler", "worker-1");
    assert.equal(tasks2.length, 1);
    await client.completeWorkerTask(tasks2[0]!.id, "worker-1", {
      success: true,
    });

    const completed = await client.waitForState(id, "completed");
    assert.equal(completed.state, "completed");
  });

  it("should handle permanent failure", async () => {
    const seq = testSequence("ext-perm-fail", [
      step("s1", "bad_handler", {}),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "waiting");

    const tasks = await client.pollWorkerTasks("bad_handler", "worker-1");
    assert.equal(tasks.length, 1);
    await client.failWorkerTask(
      tasks[0]!.id,
      "worker-1",
      "permanent error",
      false
    );

    // Instance should fail permanently.
    const failed = await client.waitForState(id, "failed");
    assert.equal(failed.state, "failed");
  });

  it("should accept heartbeats for claimed tasks", async () => {
    const seq = testSequence("ext-heartbeat", [
      step("s1", "slow_handler", {}),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "waiting");

    const tasks = await client.pollWorkerTasks("slow_handler", "worker-1");
    assert.equal(tasks.length, 1);

    // Send heartbeat.
    await client.heartbeatWorkerTask(tasks[0]!.id, "worker-1");

    // Complete after heartbeat.
    await client.completeWorkerTask(tasks[0]!.id, "worker-1", { ok: true });

    const completed = await client.waitForState(id, "completed");
    assert.equal(completed.state, "completed");
  });

  it("should return empty array when no tasks available", async () => {
    const tasks = await client.pollWorkerTasks(
      "nonexistent_handler",
      "worker-1"
    );
    assert.equal(tasks.length, 0);
  });

  it("should complete multi-step sequence with mixed handlers", async () => {
    const seq = testSequence("mixed-handlers", [
      step("s1", "noop"),
      step("s2", "external_step", { data: 42 }),
      step("s3", "log", { message: "after external" }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // s1 (noop) completes in-process, then s2 (external) puts instance in waiting.
    await client.waitForState(id, "waiting");

    // Poll and complete the external step.
    const tasks = await client.pollWorkerTasks("external_step", "worker-1");
    assert.equal(tasks.length, 1);
    assert.equal(tasks[0]!.block_id, "s2");
    await client.completeWorkerTask(tasks[0]!.id, "worker-1", {
      external: true,
    });

    // After s2 completes, s3 (log) runs in-process and instance completes.
    const completed = await client.waitForState(id, "completed");
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    assert.equal(outputs.length, 3);
  });

  // Plan #91: a step's `queue_name` routes its task to that specific
  // queue. Polling via the queue-scoped endpoint must return the task.
  it("routes task to named queue when step declares queue_name", async () => {
    const queueName = `q-${uuid().slice(0, 8)}`;
    const handler = `queued_handler_${uuid().slice(0, 8)}`;

    const seq = testSequence("ext-queue-route", [
      step("s1", handler, { foo: 1 }, { queue_name: queueName }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "waiting", { timeoutMs: 5000 });

    // Queue-scoped poll must see the task.
    const viaQueue = await pollQueue(queueName, handler, "worker-q-1");
    assert.equal(
      viaQueue.length,
      1,
      "queue-scoped poll should return the routed task",
    );
    assert.equal(viaQueue[0]!.handler_name, handler);

    // Completing the task should drive the instance to completion.
    await client.completeWorkerTask(viaQueue[0]!.id, "worker-q-1", { ok: 1 });
    const completed = await client.waitForState(id, "completed");
    assert.equal(completed.state, "completed");
  });

  // Plan #101: polling from queue A must NOT see tasks routed to queue B
  // (even for the same handler name). Queues isolate work.
  it("queue-scoped poll isolates tasks by queue_name", async () => {
    const queueA = `qa-${uuid().slice(0, 8)}`;
    const queueB = `qb-${uuid().slice(0, 8)}`;
    const handler = `iso_handler_${uuid().slice(0, 8)}`;

    // Two sequences — identical handler, different queue.
    const seqA = testSequence("iso-a", [
      step("s1", handler, { which: "a" }, { queue_name: queueA }),
    ]);
    const seqB = testSequence("iso-b", [
      step("s1", handler, { which: "b" }, { queue_name: queueB }),
    ]);
    await client.createSequence(seqA);
    await client.createSequence(seqB);

    const { id: idA } = await client.createInstance({
      sequence_id: seqA.id,
      tenant_id: "test",
      namespace: "default",
    });
    const { id: idB } = await client.createInstance({
      sequence_id: seqB.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(idA, "waiting", { timeoutMs: 5000 });
    await client.waitForState(idB, "waiting", { timeoutMs: 5000 });

    // Poll only queueA — we must see A's task and NOT B's.
    const onlyA = await pollQueue(queueA, handler, "worker-iso-a");
    assert.equal(onlyA.length, 1, "queueA poll must return exactly one task");
    assert.equal(
      (onlyA[0]!.params as { which?: string }).which,
      "a",
      "queueA should return only the queueA-routed task",
    );

    // Poll queueB separately — sees its own task, not A's.
    const onlyB = await pollQueue(queueB, handler, "worker-iso-b");
    assert.equal(onlyB.length, 1, "queueB poll must return exactly one task");
    assert.equal(
      (onlyB[0]!.params as { which?: string }).which,
      "b",
      "queueB should return only the queueB-routed task",
    );

    // Complete both to let instances drain.
    await client.completeWorkerTask(onlyA[0]!.id, "worker-iso-a", {});
    await client.completeWorkerTask(onlyB[0]!.id, "worker-iso-b", {});
    await client.waitForState(idA, "completed");
    await client.waitForState(idB, "completed");
  });
});
