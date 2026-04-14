/**
 * Verifies signal buffering semantics: a cancel signal arriving while a
 * try-catch `finally` block is executing must be held until the finally
 * completes, then applied to the next block.
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

describe("Signal during try-catch finally", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // Plan:
  //   - Arrange: sequence = [try (fails) / catch / finally (sleep 300ms),
  //     then next step]; instance started normally.
  //   - Act: send a cancel signal while the finally sleep is in flight.
  //   - Assert: the finally block runs to completion (its output recorded),
  //     the next step is cancelled, and instance ends in Cancelled state.
  it("should buffer signal until finally block completes", async () => {
    const seq = testSequence("signal-during-finally", [
      tryCatch(
        "tc1",
        [step("t", "fail", { message: "boom", retryable: false })],
        [step("c", "log", { message: "caught" })],
        [step("f", "sleep", { duration_ms: 500 })],
      ) as Block,
      step("next", "log", { message: "after-tc" }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait long enough for try to fail, catch to run, and finally (sleep) to
    // be in flight — but not long enough for finally to finish.
    await new Promise((r) => setTimeout(r, 200));

    await client.sendSignal(id, "cancel");

    const final = await client.waitForState(id, ["cancelled", "failed", "completed"], {
      timeoutMs: 10_000,
    });
    assert.equal(final.state, "cancelled", `expected cancelled, got ${final.state}`);

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    // Finally should have completed since it was already in flight.
    assert.ok(blockIds.includes("f"), "finally step f should have recorded output");
    // The step after the try_catch must NOT have run.
    assert.ok(!blockIds.includes("next"), "next step should NOT run after cancel");
  });
});
