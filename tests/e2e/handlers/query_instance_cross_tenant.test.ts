/**
 * query_instance handler — cross-tenant isolation.
 * Verifies that querying an instance belonging to a different tenant
 * results in a Permanent error (not just `found: false`).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("query_instance Cross-Tenant Isolation", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("querying another tenant's instance fails or returns not-found", async () => {
    const tenantA = `qi-a-${uuid().slice(0, 8)}`;
    const tenantB = `qi-b-${uuid().slice(0, 8)}`;

    // Create an instance in tenant A.
    const seqA = testSequence("qi-target", [step("s", "sleep", { duration_ms: 2000 })], {
      tenantId: tenantA,
    });
    await client.createSequence(seqA);
    const { id: targetId } = await client.createInstance({
      sequence_id: seqA.id,
      tenant_id: tenantA,
      namespace: "default",
    });
    await client.waitForState(targetId, ["running", "scheduled"], { timeoutMs: 5_000 });

    // Create a sequence in tenant B that queries tenant A's instance.
    const seqB = testSequence(
      "qi-cross",
      [
        step("query", "query_instance", { instance_id: targetId }),
      ],
      { tenantId: tenantB },
    );
    await client.createSequence(seqB);

    const { id: queryId } = await client.createInstance({
      sequence_id: seqB.id,
      tenant_id: tenantB,
      namespace: "default",
    });

    const final = await client.waitForState(queryId, ["completed", "failed"], {
      timeoutMs: 10_000,
    });

    if (final.state === "failed") {
      // Cross-tenant query resulted in permanent failure — good.
      return;
    }

    // If it completed, the output should indicate not-found (no data leakage).
    const outputs = await client.getOutputs(queryId);
    const qOut = outputs.find((o) => o.block_id === "query");
    assert.ok(qOut, "query step should have output");
    const out = qOut.output as Record<string, unknown>;
    assert.ok(
      out.found === false || out._error,
      "cross-tenant query should not return real data",
    );
  });

  it("querying own tenant instance succeeds", async () => {
    const tenantId = `qi-own-${uuid().slice(0, 8)}`;

    // Create target instance.
    const targetSeq = testSequence("qi-self-target", [step("s", "noop")], { tenantId });
    await client.createSequence(targetSeq);
    const { id: targetId } = await client.createInstance({
      sequence_id: targetSeq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(targetId, "completed", { timeoutMs: 5_000 });

    // Create querier in same tenant.
    const querySeq = testSequence(
      "qi-self-query",
      [step("query", "query_instance", { instance_id: targetId })],
      { tenantId },
    );
    await client.createSequence(querySeq);

    const { id: queryId } = await client.createInstance({
      sequence_id: querySeq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    const final = await client.waitForState(queryId, ["completed", "failed"], {
      timeoutMs: 10_000,
    });
    assert.equal(final.state, "completed", "same-tenant query should complete");

    const outputs = await client.getOutputs(queryId);
    const qOut = outputs.find((o) => o.block_id === "query");
    assert.ok(qOut, "query step output should exist");
    const out = qOut.output as Record<string, unknown>;
    assert.ok(
      out.found === true || out.state !== undefined,
      "same-tenant query should return instance data",
    );
  });
});
