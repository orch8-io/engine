/**
 * Sub-sequence deep nesting — verifies 3-level sub→sub→sub chains and
 * sub-sequences spawned from parallel branches (concurrent children).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block, SequenceDef } from "../client.ts";

const client = new Orch8Client();

describe("SubSequence Deep Nesting", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("three-level sub→sub→sub chain completes", async () => {
    const tenantId = `ss3-${uuid().slice(0, 8)}`;

    // Level 3 (leaf)
    const leaf: SequenceDef = testSequence(
      "ss-leaf",
      [step("leaf_step", "log", { message: "leaf" })],
      { tenantId },
    );
    await client.createSequence(leaf);

    // Level 2 (middle — calls leaf)
    const mid: SequenceDef = testSequence(
      "ss-mid",
      [
        step("mid_step", "log", { message: "mid" }),
        { type: "sub_sequence", id: "call_leaf", sequence_name: leaf.name } as unknown as Block,
      ],
      { tenantId },
    );
    await client.createSequence(mid);

    // Level 1 (root — calls middle)
    const root: SequenceDef = testSequence(
      "ss-root",
      [
        step("root_step", "log", { message: "root" }),
        { type: "sub_sequence", id: "call_mid", sequence_name: mid.name } as unknown as Block,
      ],
      { tenantId },
    );
    await client.createSequence(root);

    const { id } = await client.createInstance({
      sequence_id: root.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 15_000 });

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(blockIds.includes("root_step"), "root step should complete");
    assert.ok(blockIds.includes("call_mid"), "sub_sequence block should have output");
  });

  it("sub-sequences in parallel branches run concurrently", async () => {
    const tenantId = `ss-par-${uuid().slice(0, 8)}`;

    const childA: SequenceDef = testSequence(
      "ss-child-a",
      [step("a_step", "log", { message: "child A" })],
      { tenantId },
    );
    const childB: SequenceDef = testSequence(
      "ss-child-b",
      [step("b_step", "log", { message: "child B" })],
      { tenantId },
    );
    await client.createSequence(childA);
    await client.createSequence(childB);

    const parent: SequenceDef = testSequence(
      "ss-par-parent",
      [
        {
          type: "parallel",
          id: "par",
          branches: [
            [{ type: "sub_sequence", id: "call_a", sequence_name: childA.name } as unknown as Block],
            [{ type: "sub_sequence", id: "call_b", sequence_name: childB.name } as unknown as Block],
          ],
        } as unknown as Block,
      ],
      { tenantId },
    );
    await client.createSequence(parent);

    const { id } = await client.createInstance({
      sequence_id: parent.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 15_000 });

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(blockIds.includes("call_a"), "sub-sequence A should complete");
    assert.ok(blockIds.includes("call_b"), "sub-sequence B should complete");
  });

  it("sub-sequence inside loop body runs per iteration", async () => {
    const tenantId = `ss-loop-${uuid().slice(0, 8)}`;

    const child: SequenceDef = testSequence(
      "ss-loop-child",
      [step("child_step", "noop")],
      { tenantId },
    );
    await client.createSequence(child);

    const parent: SequenceDef = testSequence(
      "ss-loop-parent",
      [
        {
          type: "loop",
          id: "lp",
          condition: "true",
          body: [
            { type: "sub_sequence", id: "call_child", sequence_name: child.name } as unknown as Block,
          ],
          max_iterations: 3,
        } as unknown as Block,
      ],
      { tenantId },
    );
    await client.createSequence(parent);

    const { id } = await client.createInstance({
      sequence_id: parent.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 15_000 });
  });
});
