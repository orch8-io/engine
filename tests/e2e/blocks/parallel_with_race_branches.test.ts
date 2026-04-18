/**
 * Verifies that a Parallel block whose branches are themselves Race blocks
 * resolves each race independently (fast branch wins, slow branch cancelled)
 * and the overall instance completes.
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

function race(
  id: string,
  branches: unknown[],
  semantics?: string,
): Record<string, unknown> {
  const block: Record<string, unknown> = { type: "race", id, branches };
  if (semantics) block.semantics = semantics;
  return block;
}

describe("Parallel with Race branches", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("each parallel branch is a race; fast branches win both", async () => {
    const seq = testSequence("par-race", [
      parallel("p1", [
        [
          race("ra", [
            [step("a_fast", "log", { message: "A fast" })],
            [step("a_slow", "par_race_slow_a", {})],
          ]),
        ],
        [
          race("rb", [
            [step("b_fast", "log", { message: "B fast" })],
            [step("b_slow", "par_race_slow_b", {})],
          ]),
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
      timeoutMs: 15_000,
    });
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(
      blockIds.includes("a_fast"),
      "A's fast branch should have produced output",
    );
    assert.ok(
      blockIds.includes("b_fast"),
      "B's fast branch should have produced output",
    );
  });
});
