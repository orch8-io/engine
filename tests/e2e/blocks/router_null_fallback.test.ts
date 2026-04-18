/**
 * Verifies Router behaviour when the condition expression resolves to null or
 * a missing key — the router should fall through to its default branch.
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

describe("Router null/missing path fallback", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // Plan:
  //   - Arrange: router block with condition `{{context.maybe}}` and cases
  //     plus a default branch; instance context omits `maybe` entirely.
  //   - Act: run the instance to completion.
  //   - Assert: default branch executed, outputs reflect the default path,
  //     and no case branch emitted any step output.
  it("should fall through to default branch when condition resolves to null", async () => {
    const seq = testSequence("router-null-fallback", [
      router(
        "rt1",
        [
          {
            condition: 'maybe == "x"',
            blocks: [step("case_step", "log", { message: "case" })],
          },
        ],
        [step("default_step", "log", { message: "default" })],
      ) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      // Note: no `maybe` key in context.data.
      context: { data: {} },
    });

    const completed = await client.waitForState(id, "completed", {
      timeoutMs: 10_000,
    });
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(blockIds.includes("default_step"), "default_step should run on missing key");
    assert.ok(!blockIds.includes("case_step"), "case_step should NOT run");
  });
});
