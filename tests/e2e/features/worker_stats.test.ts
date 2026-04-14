import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Worker Task Stats", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should return stats object with task counts", async () => {
    const stats = await client.workerTaskStats();
    assert.ok(stats !== undefined);
    // Stats should have numeric fields for task states.
    assert.ok(typeof stats === "object");
  });

  it("should reflect pending tasks after external dispatch", async () => {
    // Create an instance with an unknown handler — dispatches to external worker queue.
    const handlerName = `ext-handler-${uuid().slice(0, 8)}`;
    const seq = testSequence("stats-pending", [step("s1", handlerName)]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "waiting", { timeoutMs: 10_000 });

    const stats = await client.workerTaskStats();
    // Stats uses by_state map: { "pending": N, "claimed": N, ... }
    const byState = (stats as any).by_state ?? {};
    const pending =
      byState.pending ?? byState.Pending ?? (stats as any).pending ?? 0;
    assert.ok(pending >= 1, `expected pending >= 1, got ${pending} (stats: ${JSON.stringify(stats)})`);
  });

  it("should show claimed tasks after poll", async () => {
    const handlerName = `ext-poll-${uuid().slice(0, 8)}`;
    const seq = testSequence("stats-claimed", [step("s1", handlerName)]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "waiting", { timeoutMs: 10_000 });

    // Poll the task.
    const tasks = await client.pollWorkerTasks(handlerName, "worker-stats-1");
    assert.ok(tasks.length >= 1);

    const stats = await client.workerTaskStats();
    const byState = (stats as any).by_state ?? {};
    const claimed =
      byState.claimed ?? byState.Claimed ?? (stats as any).claimed ?? 0;
    assert.ok(claimed >= 1, `expected claimed >= 1, got ${claimed} (stats: ${JSON.stringify(stats)})`);

    // Clean up: complete the task.
    await client.completeWorkerTask(tasks[0]!.id, "worker-stats-1", {});
  });
});
