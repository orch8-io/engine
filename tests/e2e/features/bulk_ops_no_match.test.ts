/**
 * Bulk Operations — No Match — verifies that bulk cancel and bulk reschedule
 * return count=0 when no instances match the filter criteria.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Bulk Ops No Match", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("bulk cancel returns count=0 when no instances match", async () => {
    const tenantId = `bulk-none-${uuid().slice(0, 8)}`;

    const res = await client.bulkUpdateState(
      {
        tenant_id: tenantId,
        states: ["scheduled"],
      },
      "cancelled",
    );
    assert.equal(
      (res as any).count,
      0,
      "bulk cancel with no matches should return 0",
    );
  });

  it("bulk reschedule returns count=0 when no instances match", async () => {
    const tenantId = `bulk-none-r-${uuid().slice(0, 8)}`;

    const res = await client.bulkReschedule(
      { tenant_id: tenantId, states: ["scheduled"] },
      3600,
    );
    assert.equal(
      (res as any).count,
      0,
      "bulk reschedule with no matches should return 0",
    );
  });

  it("bulk cancel respects combined filters (tenant + sequence + state)", async () => {
    const tenantId = `bulk-comb-${uuid().slice(0, 8)}`;
    const seq = testSequence("bulk-comb-seq", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    // Create an instance that is already completed.
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed");

    // Bulk cancel only scheduled instances of this sequence — our instance
    // is already completed so it should not match.
    const res = await client.bulkUpdateState(
      {
        tenant_id: tenantId,
        sequence_id: seq.id,
        states: ["scheduled"],
      },
      "cancelled",
    );
    assert.equal(
      (res as any).count,
      0,
      "completed instance should not match scheduled-state filter",
    );
  });
});
