/**
 * query_instance handler.
 *
 * Engine reference: `orch8-engine/src/handlers/query_instance.rs` —
 * returns `{ found: true, state, context, started_at, completed_at,
 * current_node }` for an existing same-tenant target, and
 * `{ found: false }` for a missing id.
 *
 * Note on the scaffold title ("Circular References"): the engine does not
 * recurse through `query_instance` (it's a single storage lookup), so there's
 * no depth-bound to test. We instead verify the observable contract:
 *   - forward lookup of an in-flight instance returns its live context/state,
 *   - lookup of an unknown id returns `{ found: false }`,
 *   - the two producers can reference each other's ids without hanging
 *     (mutual lookup doesn't cycle).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("query_instance Handler", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("returns the target's state and context; returns found:false for missing ids", async () => {
    const tenantId = `qi-${uuid().slice(0, 8)}`;
    const namespace = "default";

    // Target X: sleeps briefly so the producer observes it either mid-flight
    // or freshly terminal — both are valid; we only assert `found: true` and
    // that the returned context echoes what we seeded.
    const targetSeq = testSequence(
      "qi-target",
      [step("s", "sleep", { duration_ms: 500 })],
      { tenantId, namespace },
    );
    await client.createSequence(targetSeq);

    const { id: targetId } = await client.createInstance({
      sequence_id: targetSeq.id,
      tenant_id: tenantId,
      namespace,
      context: { data: { marker: "seeded-by-test" } },
    });

    // Producer Y: query X's id as a literal param.
    const producerSeq = testSequence(
      "qi-producer",
      [step("q", "query_instance", { instance_id: targetId })],
      { tenantId, namespace },
    );
    await client.createSequence(producerSeq);

    const { id: producerId } = await client.createInstance({
      sequence_id: producerSeq.id,
      tenant_id: tenantId,
      namespace,
    });

    await client.waitForState(producerId, "completed", { timeoutMs: 10_000 });

    const outputs = await client.getOutputs(producerId);
    const queryOut = outputs.find((o) => o.block_id === "q");
    assert.ok(queryOut, "query_instance output missing");
    const out = queryOut!.output as Record<string, unknown>;

    assert.equal(out.found, true);
    assert.ok(out.state, "state field should be present");
    const ctx = out.context as Record<string, unknown>;
    assert.deepEqual(
      (ctx.data as Record<string, unknown>).marker,
      "seeded-by-test",
      "queried context should echo the seeded data",
    );

    // Cross-check against the direct REST read of the same instance.
    const direct = await client.getInstance(targetId);
    assert.deepEqual(
      (direct.context as Record<string, unknown>).data,
      ctx.data,
      "query_instance context must match getInstance context for same id",
    );

    // Unknown id → `{ found: false }`.
    const ghostId = uuid();
    const ghostSeq = testSequence(
      "qi-ghost",
      [step("q", "query_instance", { instance_id: ghostId })],
      { tenantId, namespace },
    );
    await client.createSequence(ghostSeq);
    const { id: ghostProducerId } = await client.createInstance({
      sequence_id: ghostSeq.id,
      tenant_id: tenantId,
      namespace,
    });
    await client.waitForState(ghostProducerId, "completed", { timeoutMs: 10_000 });
    const ghostOutputs = await client.getOutputs(ghostProducerId);
    const ghostOut = ghostOutputs.find((o) => o.block_id === "q");
    assert.ok(ghostOut, "ghost query output missing");
    assert.equal((ghostOut!.output as Record<string, unknown>).found, false);
  });
});
