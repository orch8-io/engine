/**
 * Nested TryCatch: inner TryCatch inside outer catch_block.
 *
 * Verifies a fallback chain: outer try fails → outer catch runs → the catch
 * itself is a TryCatch whose try fails → its catch recovers. Final instance
 * state is "completed".
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

describe("Nested TryCatch: inner try_catch in outer catch_block", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("outer catch is a try_catch; inner catch recovers", async () => {
    const inner = tryCatch(
      "tc_inner",
      [step("inner_try", "nested_tc_inner_try", {})],
      [step("inner_catch", "log", { message: "inner recovered" })],
    );
    const seq = testSequence("nested-tc", [
      tryCatch(
        "tc_outer",
        [step("outer_try", "nested_tc_outer_try", {})],
        [inner],
      ) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Step 1: outer try dispatched → fail it permanently to trigger outer catch.
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });
    const outerTasks = await client.pollWorkerTasks(
      "nested_tc_outer_try",
      "worker-1",
    );
    assert.equal(outerTasks.length, 1, "expected one outer_try task");
    await client.failWorkerTask(
      outerTasks[0]!.id,
      "worker-1",
      "outer try failed",
      false,
    );

    // Step 2: outer catch runs → inner try_catch starts → inner try dispatched.
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });
    let innerTasks = await client.pollWorkerTasks(
      "nested_tc_inner_try",
      "worker-1",
    );
    const deadline = Date.now() + 3000;
    while (innerTasks.length === 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 50));
      innerTasks = await client.pollWorkerTasks(
        "nested_tc_inner_try",
        "worker-1",
      );
    }
    assert.equal(innerTasks.length, 1, "expected one inner_try task");
    await client.failWorkerTask(
      innerTasks[0]!.id,
      "worker-1",
      "inner try failed",
      false,
    );

    // Step 3: inner_catch (builtin log) runs → instance completes.
    const completed = await client.waitForState(id, "completed", {
      timeoutMs: 10_000,
    });
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(
      blockIds.includes("inner_catch"),
      "inner_catch should have produced output",
    );
  });
});
