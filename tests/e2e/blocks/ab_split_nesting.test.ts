/**
 * A/B Split nesting — verifies that ab_split works correctly inside
 * composite blocks (loop, parallel, forEach) and that nested ab_splits
 * make independent variant decisions.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

function abSplit(
  id: string,
  variants: { name: string; weight: number; blocks: Block[] }[],
): Block {
  return { type: "a_b_split", id, variants } as unknown as Block;
}

describe("A/B Split Nesting", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("ab_split inside parallel branches", async () => {
    const tenantId = `ab-par-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "ab-in-par",
      [
        {
          type: "parallel",
          id: "par",
          branches: [
            [
              abSplit("split_a", [
                { name: "X", weight: 50, blocks: [step("xa", "noop")] },
                { name: "Y", weight: 50, blocks: [step("ya", "noop")] },
              ]),
            ],
            [
              abSplit("split_b", [
                { name: "X", weight: 50, blocks: [step("xb", "noop")] },
                { name: "Y", weight: 50, blocks: [step("yb", "noop")] },
              ]),
            ],
          ],
        } as unknown as Block,
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);

    // Both splits should have produced an output.
    assert.ok(blockIds.includes("split_a"), "split_a output missing");
    assert.ok(blockIds.includes("split_b"), "split_b output missing");
  });

  it("ab_split inside forEach body", async () => {
    const tenantId = `ab-fe-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "ab-in-foreach",
      [
        {
          type: "for_each",
          id: "fe",
          collection: "items",
          item_var: "item",
          body: [
            abSplit("split", [
              { name: "A", weight: 50, blocks: [step("a_leg", "noop")] },
              { name: "B", weight: 50, blocks: [step("b_leg", "noop")] },
            ]),
          ],
          max_iterations: 5,
        } as unknown as Block,
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      context: { data: { items: [1, 2, 3] } },
    });

    const final = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(final.state, "completed", "forEach with ab_split body should complete");
  });

  it("ab_split inside loop body", async () => {
    const tenantId = `ab-loop-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "ab-in-loop",
      [
        {
          type: "loop",
          id: "lp",
          condition: "true",
          body: [
            abSplit("split", [
              { name: "A", weight: 50, blocks: [step("leg_a", "noop")] },
              { name: "B", weight: 50, blocks: [step("leg_b", "noop")] },
            ]),
          ],
          max_iterations: 3,
        } as unknown as Block,
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const outputs = await client.getOutputs(id);
    // Loop ran 3 iterations, each with an ab_split
    assert.ok(outputs.length >= 3, `expected at least 3 outputs, got ${outputs.length}`);
  });
});
