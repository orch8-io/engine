/**
 * ForEach with a Parallel block as its body.
 *
 * Verifies that each iteration runs the inner Parallel block completely, so
 * with 3 items and 2 parallel branches we expect at least 3 outputs per
 * branch block id.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

function parallel(id: string, branches: unknown[]): Record<string, unknown> {
  return { type: "parallel", id, branches };
}

function forEach(
  id: string,
  collection: string,
  body: unknown,
  opts: Record<string, unknown> = {},
): Record<string, unknown> {
  return { type: "for_each", id, collection, body, ...opts };
}

describe("ForEach: Parallel inner body", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("each iteration runs all parallel branches", async () => {
    const seq = testSequence("fe-par", [
      forEach("fe", "items", [
        parallel("p1", [
          [step("a", "log", { message: "branch-a" })],
          [step("b", "log", { message: "branch-b" })],
        ]) as Block,
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
    // Note: use >= 3 rather than == 3 because the engine may or may not
    // deduplicate outputs by block id; we only care that each branch ran
    // at least once per iteration.
    const countA = outputs.filter((o) => o.block_id === "a").length;
    const countB = outputs.filter((o) => o.block_id === "b").length;
    assert.ok(countA >= 3, `expected >= 3 outputs for "a", got ${countA}`);
    assert.ok(countB >= 3, `expected >= 3 outputs for "b", got ${countB}`);
  });
});
