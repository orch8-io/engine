/**
 * ForEach with a TryCatch body.
 *
 * Verifies that per-iteration failures are absorbed by the inner catch and
 * the loop continues to completion. Iterations dispatch an external handler
 * one at a time; we alternate complete/fail/complete and expect the final
 * instance state to be "completed" with at least one catch_step output.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

function tryCatch(
  id: string,
  tryBlock: unknown[],
  catchBlock: unknown[],
  finallyBlock?: unknown[],
): Record<string, unknown> {
  const block: Record<string, unknown> = {
    type: "try_catch",
    id,
    try_block: tryBlock,
    catch_block: catchBlock,
  };
  if (finallyBlock) block.finally_block = finallyBlock;
  return block;
}

function forEach(
  id: string,
  collection: string,
  body: unknown,
  opts: Record<string, unknown> = {},
): Record<string, unknown> {
  return { type: "for_each", id, collection, body, ...opts };
}

describe("ForEach: TryCatch inner body", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("failed iterations are recovered by catch; loop completes", async () => {
    const seq = testSequence("fe-tc", [
      forEach("fe", "items", [
        tryCatch(
          "tc",
          [step("try_step", "fe_tc_handler", {})],
          [step("catch_step", "log", { message: "recovered" })],
        ) as Block,
      ]) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { items: ["ok", "fail", "ok"] } },
    });

    // Sequential iterations: handle one worker task per iteration.
    // iteration 0 → complete, 1 → fail permanently, 2 → complete.
    const actions: Array<"complete" | "fail"> = ["complete", "fail", "complete"];
    for (let i = 0; i < actions.length; i++) {
      await client.waitForState(id, "waiting", { timeoutMs: 5000 });

      // Poll until the handler task for this iteration is visible.
      let tasks = await client.pollWorkerTasks("fe_tc_handler", "worker-1");
      const deadline = Date.now() + 3000;
      while (tasks.length === 0 && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 50));
        tasks = await client.pollWorkerTasks("fe_tc_handler", "worker-1");
      }
      assert.equal(tasks.length, 1, `expected task for iteration ${i}`);

      if (actions[i] === "complete") {
        await client.completeWorkerTask(tasks[0]!.id, "worker-1", { i });
      } else {
        await client.failWorkerTask(
          tasks[0]!.id,
          "worker-1",
          `iteration ${i} failed`,
          false,
        );
      }
    }

    const completed = await client.waitForState(id, "completed", {
      timeoutMs: 15_000,
    });
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const catchCount = outputs.filter((o) => o.block_id === "catch_step").length;
    assert.ok(
      catchCount >= 1,
      `expected at least 1 catch_step output, got ${catchCount}`,
    );
  });
});
