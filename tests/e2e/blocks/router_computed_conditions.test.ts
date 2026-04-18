/**
 * Verifies Router condition evaluation supports arithmetic and comparison
 * expressions built from templated context variables.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

function router(
  id: string,
  routes: unknown[],
  defaultBlocks?: unknown,
): Record<string, unknown> {
  const block: Record<string, unknown> = { type: "router", id, routes };
  if (defaultBlocks) block.default = defaultBlocks;
  return block;
}

describe("Router Computed Conditions", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // Plan:
  //   - Arrange: router with condition `{{count}} * 2 > {{threshold}}` and two
  //     branches: ">8" and "<=8"; context supplies count=5, threshold=8.
  //   - Act: run the instance.
  //   - Assert: the ">8" branch was matched (since 5*2=10 > 8) and its output
  //     step produced the expected marker.
  it("should evaluate arithmetic and comparison expressions in router condition", async () => {
    const seq = testSequence("router-compute", [
      router(
        "rt1",
        [
          {
            condition: "count * 2 > threshold",
            blocks: [step("hit_high", "log", { message: "high" })],
          },
        ],
        [step("hit_low", "log", { message: "low" })],
      ) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { count: 5, threshold: 8 } },
    });

    const completed = await client.waitForState(id, "completed", {
      timeoutMs: 10_000,
    });
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(blockIds.includes("hit_high"), "hit_high should fire (5*2=10 > 8)");
    assert.ok(!blockIds.includes("hit_low"), "hit_low should NOT fire");
  });
});
