/**
 * Verifies that a TryCatch's finally_block runs in multiple outcome shapes:
 *   - try succeeds (catch skipped, finally runs)
 *   - try fails then catch recovers (finally runs)
 *   - try_block has multiple steps all succeeding (finally still runs)
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

describe("TryCatch finally_block always runs", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("finally runs when try succeeds", async () => {
    const seq = testSequence("tcf-try-ok", [
      tryCatch(
        "tcf",
        [step("t", "log", { message: "try" })],
        [step("c", "log", { message: "catch" })],
        [step("f", "log", { message: "finally" })],
      ) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    const completed = await client.waitForState(id, "completed", {
      timeoutMs: 10_000,
    });
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(blockIds.includes("t"), "try step should run");
    assert.ok(blockIds.includes("f"), "finally step should run");
    assert.ok(!blockIds.includes("c"), "catch step should NOT run when try succeeds");
  });

  it("finally runs when try fails and catch recovers", async () => {
    const seq = testSequence("tcf-catch", [
      tryCatch(
        "tcf",
        [step("t", "tcf_try_fails", {})],
        [step("c", "log", { message: "catch" })],
        [step("f", "log", { message: "finally" })],
      ) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "waiting", { timeoutMs: 5000 });
    const tasks = await client.pollWorkerTasks("tcf_try_fails", "worker-1");
    assert.equal(tasks.length, 1);
    await client.failWorkerTask(tasks[0]!.id, "worker-1", "try failed", false);

    const completed = await client.waitForState(id, "completed", {
      timeoutMs: 10_000,
    });
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(blockIds.includes("c"), "catch step should run after try fails");
    assert.ok(blockIds.includes("f"), "finally step should run after catch");
    assert.ok(
      !blockIds.includes("t"),
      "try step should NOT produce a success output when it failed",
    );
  });

  it("finally runs when try succeeds with multiple steps in try_block", async () => {
    const seq = testSequence("tcf-multi", [
      tryCatch(
        "tcf",
        [
          step("t1", "log", { message: "try 1" }),
          step("t2", "log", { message: "try 2" }),
        ],
        [step("c", "log", { message: "catch" })],
        [step("f", "log", { message: "finally" })],
      ) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    const completed = await client.waitForState(id, "completed", {
      timeoutMs: 10_000,
    });
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(blockIds.includes("t1"), "t1 should run");
    assert.ok(blockIds.includes("t2"), "t2 should run");
    assert.ok(blockIds.includes("f"), "finally should run");
    assert.ok(!blockIds.includes("c"), "catch should NOT run when both try steps succeed");
  });
});
