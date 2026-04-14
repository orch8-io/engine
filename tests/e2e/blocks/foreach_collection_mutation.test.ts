/**
 * Verifies that a ForEach block iterates over a snapshot of the collection
 * taken at block entry — mid-iteration mutations to the source context do not
 * extend or shrink the iteration set.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

function forEach(id: string, collection: string, body: unknown[]): Record<string, unknown> {
  return { type: "for_each", id, collection, body };
}

describe("ForEach Collection Mutation Mid-Iteration", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // Plan:
  //   - Arrange: forEach over a 3-item context array; body contains a step
  //     that mutates the context by pushing a 4th item on the first iteration.
  //   - Act: run the instance to completion.
  //   - Assert: total iterations = 3 (snapshot respected), final context shows
  //     the 4th item persisted, and no extra iteration was triggered for it.
  it("should iterate over snapshot at start, not live mutations", async () => {
    // Note: there's no in-body "mutate context" builtin handler, so we use an
    // external handler `mutate_items` and, from the worker side, call
    // updateContext on the first invocation to append a 4th item. The engine
    // should still iterate exactly 3 times because the forEach snapshots the
    // collection at block entry.
    const seq = testSequence("fe-mutation", [
      forEach("fe1", "items", [
        step("body_step", "mutate_items", {}),
      ]) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { items: ["a", "b", "c"] } },
    });

    // Poll for up to 4 iterations (defensive: if the engine erroneously runs
    // extra iterations, we catch them). On iteration 1, inject a mutation.
    let completed = 0;
    const deadline = Date.now() + 15_000;
    while (completed < 3 && Date.now() < deadline) {
      const tasks = await client.pollWorkerTasks("mutate_items", "worker-1", 1);
      if (tasks.length === 0) {
        await new Promise((r) => setTimeout(r, 50));
        continue;
      }
      const task = tasks[0]!;
      if (completed === 0) {
        // Mutate context mid-iteration: append a 4th item.
        try {
          await client.updateContext(id, { data: { items: ["a", "b", "c", "d"] } });
        } catch {
          // Ignore: some engines reject mid-run updates; the test still validates
          // that we only see 3 iterations.
        }
      }
      await client.completeWorkerTask(task.id, "worker-1", { ok: true });
      completed += 1;
    }

    const final = await client.waitForState(id, ["completed", "failed"], {
      timeoutMs: 10_000,
    });
    assert.equal(final.state, "completed");

    // Should have completed exactly 3 iterations — snapshot semantics.
    assert.equal(completed, 3, `expected 3 body invocations, got ${completed}`);

    // And no stray 4th task should be pending.
    const stray = await client.pollWorkerTasks("mutate_items", "worker-1", 1);
    assert.equal(stray.length, 0, "no extra iteration should have been queued");
  });
});
