/**
 * GET /instances list filtering — verifies that state, tenant_id,
 * and namespace query params correctly filter results.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Instance List Filters", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("filtering by state=completed returns only completed instances", async () => {
    const tenantId = `filt-state-${uuid().slice(0, 8)}`;
    const ns = "default";

    // Create a sequence that completes instantly.
    const seq = testSequence("filt-ok", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    // Create a sequence that fails.
    const failSeq = testSequence(
      "filt-fail",
      [step("f", "fail", { message: "deliberate" })],
      { tenantId },
    );
    await client.createSequence(failSeq);

    // Launch one of each.
    const { id: okId } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: ns,
    });
    const { id: failId } = await client.createInstance({
      sequence_id: failSeq.id,
      tenant_id: tenantId,
      namespace: ns,
    });

    await client.waitForState(okId, "completed", { timeoutMs: 5_000 });
    await client.waitForState(failId, "failed", { timeoutMs: 5_000 });

    // Filter by state=completed.
    const completed = await client.listInstances({
      tenant_id: tenantId,
      state: "completed",
    });
    assert.ok(
      completed.every((i) => i.state === "completed"),
      "all returned instances should be completed",
    );
    assert.ok(
      completed.some((i) => i.id === okId),
      "completed instance should appear in results",
    );
    assert.ok(
      !completed.some((i) => i.id === failId),
      "failed instance should NOT appear in completed filter",
    );
  });

  it("filtering by tenant_id scopes results", async () => {
    const tenantA = `filt-a-${uuid().slice(0, 8)}`;
    const tenantB = `filt-b-${uuid().slice(0, 8)}`;

    const seqA = testSequence("filt-ta", [step("s", "noop")], { tenantId: tenantA });
    const seqB = testSequence("filt-tb", [step("s", "noop")], { tenantId: tenantB });
    await client.createSequence(seqA);
    await client.createSequence(seqB);

    const { id: idA } = await client.createInstance({
      sequence_id: seqA.id,
      tenant_id: tenantA,
      namespace: "default",
    });
    const { id: idB } = await client.createInstance({
      sequence_id: seqB.id,
      tenant_id: tenantB,
      namespace: "default",
    });

    await client.waitForState(idA, "completed", { timeoutMs: 5_000 });
    await client.waitForState(idB, "completed", { timeoutMs: 5_000 });

    const resultsA = await client.listInstances({ tenant_id: tenantA });
    assert.ok(
      resultsA.every((i) => (i as any).tenant_id === tenantA),
      "all results should belong to tenant A",
    );
    assert.ok(
      !resultsA.some((i) => i.id === idB),
      "tenant B instance should not appear in tenant A listing",
    );
  });
});
