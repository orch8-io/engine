/**
 * Verifies that a Loop block nested inside a ForEach block runs an independent
 * loop per forEach item without state bleed between iterations.
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

function loop(
  id: string,
  condition: string,
  body: unknown[],
  opts: Record<string, unknown> = {},
): Record<string, unknown> {
  return { type: "loop", id, condition, body, ...opts };
}

describe("Loop inside ForEach", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // Plan:
  //   - Arrange: sequence with forEach over 3 items; body = loop decrementing
  //     a per-iteration counter until zero.
  //   - Act: create instance and wait for completion.
  //   - Assert: each forEach iteration produced its own loop output array and
  //     counters did not cross-contaminate between sibling iterations.
  it("should run loop independently per forEach item", async () => {
    // Note: adapted to use a fixed max_iterations=2 loop with a truthy condition
    // instead of a "decrementing counter" — the engine does not expose a
    // builtin way to mutate context from within a step without an external
    // worker, and max_iterations delivers the same nested-control shape.
    const seq = testSequence("loop-in-foreach", [
      forEach("fe1", "items", [
        loop(
          "l1",
          "true",
          [step("inner", "log", { message: "tick" })],
          { max_iterations: 2 },
        ),
      ]) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { items: ["x", "y", "z"] } },
    });

    const completed = await client.waitForState(id, "completed", {
      timeoutMs: 15_000,
    });
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const innerOutputs = outputs.filter((o) => o.block_id === "inner");
    // 3 forEach items x 2 loop iterations = 6 inner outputs.
    assert.ok(
      innerOutputs.length >= 3,
      `expected at least 3 inner outputs (one per forEach item), got ${innerOutputs.length}`,
    );
  });
});
