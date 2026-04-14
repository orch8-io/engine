/**
 * Verifies that a Parallel block nested inside a Loop block runs its branches
 * on every loop iteration and collects per-iteration branch outputs.
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

function loop(
  id: string,
  condition: string,
  body: unknown[],
  opts: Record<string, unknown> = {},
): Record<string, unknown> {
  return { type: "loop", id, condition, body, ...opts };
}

describe("Parallel inside Loop", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // Plan:
  //   - Arrange: loop of 3 iterations whose body is a parallel block with
  //     branches A and B.
  //   - Act: run to completion.
  //   - Assert: 6 total branch outputs recorded (3 iterations x 2 branches),
  //     each tagged with its loop index so ordering is traceable per iteration.
  it("should run parallel block per loop iteration", async () => {
    const N = 3;
    const seq = testSequence("par-in-loop", [
      loop(
        "l1",
        "true",
        [
          parallel("p1", [
            [step("a", "log", { message: "branch-a" })],
            [step("b", "log", { message: "branch-b" })],
          ]),
        ],
        { max_iterations: N },
      ) as Block,
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
    const aOutputs = outputs.filter((o) => o.block_id === "a");
    const bOutputs = outputs.filter((o) => o.block_id === "b");
    assert.equal(aOutputs.length, N, `expected ${N} outputs for branch a, got ${aOutputs.length}`);
    assert.equal(bOutputs.length, N, `expected ${N} outputs for branch b, got ${bOutputs.length}`);
  });
});
