/**
 * Verifies that a TryCatch block participating as a branch in a Race block
 * competes on the same timing footing as a plain branch.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

function race(
  id: string,
  branches: unknown[],
  semantics?: string,
): Record<string, unknown> {
  const block: Record<string, unknown> = { type: "race", id, branches };
  if (semantics) block.semantics = semantics;
  return block;
}

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

describe("TryCatch inside Race", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // Plan:
  //   - Arrange: race with branch A = plain step sleeping 200ms and branch B =
  //     try-catch that fails then recovers within 50ms.
  //   - Act: run the instance.
  //   - Assert: winner is branch B, A's outputs are discarded, and the instance
  //     final state reflects B's recovery output only.
  it("should let try-catch branch compete fairly against a plain branch", async () => {
    const seq = testSequence("tc-in-race", [
      race("r1", [
        [step("a", "sleep", { duration_ms: 1000 })],
        [
          tryCatch(
            "tc_inner",
            [step("x", "fail", { message: "boom", retryable: false })],
            [step("recovered", "log", { message: "recovered" })],
          ),
        ],
      ]) as Block,
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
    assert.ok(blockIds.includes("recovered"), "tryCatch branch should win via recovery");
    assert.ok(!blockIds.includes("a"), "slow sleep branch should not have produced output");
  });
});
