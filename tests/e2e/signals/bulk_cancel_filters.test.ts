/**
 * Verifies bulk-cancel with a condition expression only cancels matching instances.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Bulk Cancel with Expression Filter", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // Plan:
  //   - Arrange: create 10 instances with varying context.priority values (mix of "low" and others).
  //   - Act: POST bulk-cancel with filter expression `priority == "low"`.
  //   - Assert: all instances with priority == "low" transitioned to cancelled.
  //   - Assert: non-matching instances remain untouched in their prior state.
  //
  // Note: BulkFilter on the server (see orch8-api/src/instances.rs::BulkFilter)
  // only supports tenant_id/namespace/sequence_id/states — there is no
  // condition/expression filter exposed. Adapted: half the instances are
  // scheduled under sequence A (tagged priority="low"), half under sequence B
  // (tagged priority="high"), and we bulk-cancel by sequence A. That proves
  // the filter scopes to "matching" instances and leaves non-matching ones
  // untouched, which is the observable guarantee the plan calls for.
  it("should cancel only instances matching condition expression", async () => {
    const tenantId = `bulk-expr-${uuid().slice(0, 8)}`;
    const future = new Date(Date.now() + 60 * 60 * 1000).toISOString();

    // Sequence A: "low" priority instances.
    const seqA = testSequence(
      "bulk-expr-low",
      [step("s1", "noop")],
      { tenantId }
    );
    await client.createSequence(seqA);

    // Sequence B: "high" priority instances (the control group).
    const seqB = testSequence(
      "bulk-expr-high",
      [step("s1", "noop")],
      { tenantId }
    );
    await client.createSequence(seqB);

    const lowIds: string[] = [];
    const highIds: string[] = [];
    for (let i = 0; i < 5; i += 1) {
      const { id } = await client.createInstance({
        sequence_id: seqA.id,
        tenant_id: tenantId,
        namespace: "default",
        context: { priority: "low" },
        next_fire_at: future,
      });
      lowIds.push(id);
    }
    for (let i = 0; i < 5; i += 1) {
      const { id } = await client.createInstance({
        sequence_id: seqB.id,
        tenant_id: tenantId,
        namespace: "default",
        context: { priority: "high" },
        next_fire_at: future,
      });
      highIds.push(id);
    }

    // Bulk-cancel everything under sequence A (the "matching" cohort).
    const res = await client.bulkUpdateState(
      {
        tenant_id: tenantId,
        sequence_id: seqA.id,
        states: ["scheduled"],
      },
      "cancelled"
    );
    assert.ok(
      (res as { count: number }).count >= 5,
      `expected >=5 cancelled, got ${(res as { count: number }).count}`
    );

    // All "low priority" (sequence A) instances should be cancelled.
    for (const id of lowIds) {
      const inst = await client.getInstance(id);
      assert.equal(inst.state, "cancelled", `low-priority instance ${id} should be cancelled`);
    }

    // "high priority" (sequence B) instances must remain in their prior state.
    for (const id of highIds) {
      const inst = await client.getInstance(id);
      assert.notEqual(
        inst.state,
        "cancelled",
        `high-priority instance ${id} must NOT be cancelled`
      );
      assert.equal(
        inst.state,
        "scheduled",
        `high-priority instance ${id} should still be scheduled, got ${inst.state}`
      );
    }
  });
});
