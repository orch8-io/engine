/**
 * Verifies that Router blocks embedded inside the branches of a Parallel block
 * each evaluate their own route conditions independently against the shared
 * context and run the branch-specific selected path.
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

function router(
  id: string,
  routes: unknown[],
  defaultBlocks?: unknown,
): Record<string, unknown> {
  const block: Record<string, unknown> = { type: "router", id, routes };
  if (defaultBlocks) block.default = defaultBlocks;
  return block;
}

describe("Router inside Parallel", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("each parallel branch's router dispatches independently", async () => {
    const seq = testSequence("router-in-par", [
      parallel("p1", [
        [
          router(
            "r_a",
            [
              {
                condition: 'mode == "fast"',
                blocks: [step("a_fast", "log", { message: "A fast" })],
              },
            ],
            [step("a_default", "log", { message: "A default" })],
          ),
        ],
        [
          router(
            "r_b",
            [
              {
                condition: 'mode == "slow"',
                blocks: [step("b_slow", "log", { message: "B slow" })],
              },
            ],
            [step("b_default", "log", { message: "B default" })],
          ),
        ],
      ]) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { mode: "fast" } },
    });

    const completed = await client.waitForState(id, "completed", {
      timeoutMs: 10_000,
    });
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);

    assert.ok(
      blockIds.includes("a_fast"),
      "branch A router should pick a_fast when mode=fast",
    );
    assert.ok(
      blockIds.includes("b_default"),
      "branch B router should fall through to default when mode!=slow",
    );
    assert.ok(
      !blockIds.includes("a_default"),
      "branch A default should NOT run when its fast route matched",
    );
    assert.ok(
      !blockIds.includes("b_slow"),
      "branch B slow route should NOT run when mode!=slow",
    );
  });
});
