import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

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
});
