/**
 * SubSequence Block — verifies that a parent sequence can invoke a nested
 * child sequence via the `sub_sequence` block type and that the child's
 * outputs are reachable from the parent. Closes the gap around cross-sequence
 * composition and output propagation.
 *
 * Engine contract (`orch8-engine/src/evaluator.rs` SubSequence branch):
 *   - Child is resolved by `sequence_name` (+ optional `version`) within the
 *     parent's tenant/namespace, NOT by a sequence id.
 *   - Child is created with `parent_instance_id = <parent id>` and metadata
 *     `{ "_parent_block_id": <block id> }`.
 *   - On child completion, the child's BlockOutputs are serialised and saved
 *     as the parent's block_output for the sub_sequence id. Parent context
 *     is NOT merged automatically — propagation is observable through the
 *     parent's BlockOutput for the sub_sequence block.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block, SequenceDef } from "../client.ts";

const client = new Orch8Client();

interface ChildInstance {
  id: string;
  parent_instance_id?: string | null;
}

describe("SubSequence Block", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("runs nested sequence as a child with parent_instance_id set", async () => {
    const tenantId = `ss-child-${uuid().slice(0, 8)}`;

    const child: SequenceDef = testSequence(
      "ss-child",
      [step("child_step", "log", { message: "from child" })],
      { tenantId },
    );
    await client.createSequence(child);

    const parent: SequenceDef = testSequence(
      "ss-parent",
      [
        {
          type: "sub_sequence",
          id: "ss",
          sequence_name: child.name,
        } as unknown as Block,
      ],
      { tenantId },
    );
    await client.createSequence(parent);

    const { id: parentId } = await client.createInstance({
      sequence_id: parent.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    const final = await client.waitForState(parentId, "completed", {
      timeoutMs: 20_000,
    });
    assert.equal(final.state, "completed");

    // Find the child via tenant listing (no parent-id filter in the API today).
    const all = (await client.listInstances({ tenant_id: tenantId })) as ChildInstance[];
    const children = all.filter((i) => i.parent_instance_id === parentId);
    assert.equal(children.length, 1, "expected exactly one child instance");
    const childInstance = children[0]!;

    const finalChild = await client.waitForState(childInstance.id, "completed", {
      timeoutMs: 5000,
    });
    assert.equal(finalChild.state, "completed");
  });

  it("propagates child outputs to the parent's block output", async () => {
    const tenantId = `ss-prop-${uuid().slice(0, 8)}`;

    const child: SequenceDef = testSequence(
      "ss-child-out",
      [step("produce", "log", { message: "child produced" })],
      { tenantId },
    );
    await client.createSequence(child);

    const parent: SequenceDef = testSequence(
      "ss-parent-out",
      [
        {
          type: "sub_sequence",
          id: "ss",
          sequence_name: child.name,
          input: { parent_seeded: true },
        } as unknown as Block,
      ],
      { tenantId },
    );
    await client.createSequence(parent);

    const { id: parentId } = await client.createInstance({
      sequence_id: parent.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(parentId, "completed", { timeoutMs: 20_000 });

    const parentOutputs = await client.getOutputs(parentId);
    const ssOut = parentOutputs.find((o) => o.block_id === "ss");
    assert.ok(ssOut, "expected parent block output for sub_sequence 'ss'");
    // The sub_sequence block output is the serialised array of child outputs.
    const body = ssOut.output as Array<{ block_id: string }>;
    assert.ok(Array.isArray(body), "sub_sequence output must be an array of child outputs");
    assert.ok(
      body.some((o) => o.block_id === "produce"),
      "child step 'produce' output should appear in parent sub_sequence output",
    );
  });

  // Plan #60: parent stays non-terminal while the child is still running —
  // i.e. the parent waits on the child to reach completion before advancing.
  it("parent remains non-terminal until child completes", async () => {
    const tenantId = `ss-wait-${uuid().slice(0, 8)}`;

    // Child has a measurable sleep so we can observe the parent is still
    // active while the child runs.
    const child: SequenceDef = testSequence(
      "ss-wait-child",
      [step("slow", "sleep", { duration_ms: 1500 })],
      { tenantId },
    );
    await client.createSequence(child);

    const parent: SequenceDef = testSequence(
      "ss-wait-parent",
      [
        {
          type: "sub_sequence",
          id: "ss",
          sequence_name: child.name,
        } as unknown as Block,
      ],
      { tenantId },
    );
    await client.createSequence(parent);

    const { id: parentId } = await client.createInstance({
      sequence_id: parent.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    // Give the tick loop time to dispatch the sub_sequence block.
    await new Promise((r) => setTimeout(r, 400));

    const midFlight = await client.getInstance(parentId);
    assert.ok(
      !["completed", "failed", "cancelled"].includes(midFlight.state),
      `parent should not be terminal while child is sleeping, was ${midFlight.state}`,
    );

    // Eventually both complete.
    const final = await client.waitForState(parentId, "completed", {
      timeoutMs: 20_000,
    });
    assert.equal(final.state, "completed");
  });

  // Plan #61: `input` on a sub_sequence block is propagated into the child's
  // context.data so the child can read it.
  it("child sees sub_sequence input in its context.data", async () => {
    const tenantId = `ss-input-${uuid().slice(0, 8)}`;

    const child: SequenceDef = testSequence(
      "ss-input-child",
      [step("echo", "noop")],
      { tenantId },
    );
    await client.createSequence(child);

    const parent: SequenceDef = testSequence(
      "ss-input-parent",
      [
        {
          type: "sub_sequence",
          id: "ss",
          sequence_name: child.name,
          input: { forwarded: "yes", count: 7 },
        } as unknown as Block,
      ],
      { tenantId },
    );
    await client.createSequence(parent);

    const { id: parentId } = await client.createInstance({
      sequence_id: parent.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(parentId, "completed", { timeoutMs: 20_000 });

    const all = (await client.listInstances({ tenant_id: tenantId })) as ChildInstance[];
    const children = all.filter((i) => i.parent_instance_id === parentId);
    assert.equal(children.length, 1, "expected one child instance");

    const childInst = await client.getInstance(children[0]!.id);
    const data = ((childInst.context ?? {}) as Record<string, unknown>).data as
      | Record<string, unknown>
      | undefined;
    assert.ok(data, "child context.data must be present");
    assert.equal(data!.forwarded, "yes", "sub_sequence input must land in child data");
    assert.equal(data!.count, 7, "numeric input fields must propagate");
  });
});
