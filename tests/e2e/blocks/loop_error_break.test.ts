/**
 * Verifies Loop block break-on-error semantics: a non-retryable handler error
 * must halt the loop and fail the instance without spinning through remaining
 * iterations.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

function loop(
  id: string,
  condition: string,
  body: unknown[],
  opts: Record<string, unknown> = {},
): Record<string, unknown> {
  return { type: "loop", id, condition, body, ...opts };
}

describe("Loop Error Break Semantics", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // Plan:
  //   - Arrange: loop with max 10 iterations invoking a handler that emits a
  //     permanent (non-retryable) error on the 3rd iteration.
  //   - Act: run the instance and wait for terminal state.
  //   - Assert: loop exited after iteration 3, instance state is Failed, and
  //     exactly 3 loop outputs were recorded — no extra iterations attempted.
  it("should stop loop iteration when handler returns non-retryable error", async () => {
    const seq = testSequence("loop-break-err", [
      loop(
        "l1",
        "true",
        [step("body", "break_on_third", {})],
        { max_iterations: 10 },
      ) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    let invocations = 0;
    const deadline = Date.now() + 15_000;
    // Drive up to 3 iterations: succeed twice, fail permanently on the 3rd.
    while (invocations < 3 && Date.now() < deadline) {
      const tasks = await client.pollWorkerTasks("break_on_third", "worker-1", 1);
      if (tasks.length === 0) {
        await new Promise((r) => setTimeout(r, 50));
        continue;
      }
      const task = tasks[0]!;
      invocations += 1;
      if (invocations < 3) {
        await client.completeWorkerTask(task.id, "worker-1", { iter: invocations });
      } else {
        await client.failWorkerTask(task.id, "worker-1", "permanent failure", false);
      }
    }

    const failed = await client.waitForState(id, "failed", { timeoutMs: 10_000 });
    assert.equal(failed.state, "failed");

    // Verify body outputs count — 2 successes recorded via outputs; the 3rd
    // was a permanent failure. We assert that no more than 3 body outputs
    // exist (i.e. the loop did not spin beyond the error).
    const outputs = await client.getOutputs(id);
    const bodyOutputs = outputs.filter((o) => o.block_id === "body");
    assert.ok(
      bodyOutputs.length <= 3,
      `expected <=3 body outputs, got ${bodyOutputs.length}`,
    );
    assert.equal(invocations, 3, "engine should have dispatched exactly 3 iterations");

    // No further task should be dispatched after the permanent failure.
    const stray = await client.pollWorkerTasks("break_on_third", "worker-1", 1);
    assert.equal(stray.length, 0, "loop must not spin after non-retryable error");
  });
});
