import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

/**
 * Helper: build a loop block definition.
 * See orch8-types/src/sequence.rs::LoopDef and
 * orch8-engine/src/handlers/loop_block.rs for semantics:
 *   - condition (string expression, false => loop exits)
 *   - body (blocks executed each iteration)
 *   - max_iterations (hard safety cap, default 1000)
 */
function loop(
  id: string,
  condition: string,
  body: unknown,
  opts: Record<string, unknown> = {}
): Record<string, unknown> {
  return { type: "loop", id, condition, body, ...opts };
}

describe("Loop Block", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("loop with false condition never executes body", async () => {
    // Condition is always false => body never runs, loop completes.
    const seq = testSequence("loop-false", [
      loop("l1", "false", [step("body", "log", { message: "body" })], {
        max_iterations: 5,
      }) as Block,
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

    // Body should not have produced an output since condition was false.
    const outputs = await client.getOutputs(id);
    const bodyOutputs = outputs.filter((o) => o.block_id === "body");
    assert.equal(
      bodyOutputs.length,
      0,
      "body must not execute when condition is initially false"
    );
  });

  it("loop respects max_iterations cap when condition stays truthy", async () => {
    // Condition "true" never becomes false; max_iterations caps the loop.
    // The handler tracks iterations via outputs of blocks prefixed with
    // `${loop_id}__iter_` (see loop_block.rs). Body here is a plain
    // `log` step; its own output is written as `body` block_id. The
    // max_iterations guard uses iteration tree children; we just verify
    // the instance terminates without hanging.
    const seq = testSequence("loop-cap", [
      loop("l1", "true", [step("body", "log", { message: "tick" })], {
        max_iterations: 3,
      }) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Instance must complete (not hang) — the max_iterations cap guarantees
    // the loop eventually exits even with a truthy condition.
    const completed = await client.waitForState(id, "completed", {
      timeoutMs: 15_000,
    });
    assert.equal(completed.state, "completed");
  });

  it("loop with context-driven condition: loops while flag true", async () => {
    // Initial context has `keep_going: false` — loop body should never run.
    const seq = testSequence("loop-flag", [
      loop(
        "l1",
        "context.data.keep_going",
        [step("inner", "log", { message: "inner" })],
        { max_iterations: 10 }
      ) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { keep_going: false } },
    });

    const completed = await client.waitForState(id, "completed", {
      timeoutMs: 10_000,
    });
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const innerOutputs = outputs.filter((o) => o.block_id === "inner");
    assert.equal(
      innerOutputs.length,
      0,
      "inner body must not run when keep_going is false"
    );
  });
});
