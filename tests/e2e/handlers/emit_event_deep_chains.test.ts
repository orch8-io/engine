/**
 * emit_event cross-instance chain: A emits to trigger-B, B emits to trigger-C.
 *
 * Each emit spawns a child instance via the trigger→sequence binding
 * (`emit_event` in `orch8-engine/src/handlers/emit_event.rs`). The chain
 * verifies that emissions propagate through multiple hops and each child
 * reaches a terminal state.
 *
 * Runner note: this suite uses the triggers table (globally scoped, not
 * tenant-keyed in list queries) and is registered in `SELF_MANAGED_SUITES`
 * so it runs on a fresh server.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("emit_event Deep Parent-Child Chains", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("propagates through A → B → C via emit_event triggers", async () => {
    const tenantId = `chain-${uuid().slice(0, 8)}`;
    const namespace = "default";

    // Leaf consumer: C just noops.
    const seqC = testSequence("chain-c", [step("c1", "noop")], {
      tenantId,
      namespace,
    });
    await client.createSequence(seqC);
    const slugC = `on-c-${uuid().slice(0, 8)}`;
    await client.createTrigger({
      slug: slugC,
      sequence_name: seqC.name,
      tenant_id: tenantId,
      namespace,
      trigger_type: "event",
    });

    // Middle: B emits to C.
    const seqB = testSequence(
      "chain-b",
      [
        step("b-emit", "emit_event", {
          trigger_slug: slugC,
          data: { from: "B" },
        }),
      ],
      { tenantId, namespace },
    );
    await client.createSequence(seqB);
    const slugB = `on-b-${uuid().slice(0, 8)}`;
    await client.createTrigger({
      slug: slugB,
      sequence_name: seqB.name,
      tenant_id: tenantId,
      namespace,
      trigger_type: "event",
    });

    // Head: A emits to B.
    const seqA = testSequence(
      "chain-a",
      [
        step("a-emit", "emit_event", {
          trigger_slug: slugB,
          data: { from: "A" },
        }),
      ],
      { tenantId, namespace },
    );
    await client.createSequence(seqA);

    const { id: aId } = await client.createInstance({
      sequence_id: seqA.id,
      tenant_id: tenantId,
      namespace,
    });

    // A completes immediately after emitting.
    await client.waitForState(aId, "completed", { timeoutMs: 20_000 });

    const aOutputs = await client.getOutputs(aId);
    const aEmit = aOutputs.find((o) => o.block_id === "a-emit");
    assert.ok(aEmit, "a-emit output missing");
    const bId = (aEmit!.output as Record<string, unknown>).instance_id as string;
    assert.ok(bId, "a-emit output missing instance_id");

    // B completes after emitting to C.
    await client.waitForState(bId, "completed", { timeoutMs: 20_000 });
    const bOutputs = await client.getOutputs(bId);
    const bEmit = bOutputs.find((o) => o.block_id === "b-emit");
    assert.ok(bEmit, "b-emit output missing");
    const cId = (bEmit!.output as Record<string, unknown>).instance_id as string;
    assert.ok(cId, "b-emit output missing instance_id");

    // C completes.
    const cFinal = await client.waitForState(cId, "completed", { timeoutMs: 20_000 });
    assert.equal(cFinal.state, "completed");

    // C's trigger-delivered context carries B's emitted data.
    assert.deepEqual(
      (cFinal.context as Record<string, unknown>).data,
      { from: "B" },
    );
  });
});
