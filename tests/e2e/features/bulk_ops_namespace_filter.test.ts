/**
 * Bulk Operations with Namespace Filter — verifies that bulk cancel and
 * bulk reschedule respect the namespace parameter in addition to tenant
 * and sequence filters.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Bulk Ops Namespace Filter", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("bulk cancel only affects instances in the specified namespace", async () => {
    const tenantId = `bulk-ns-${uuid().slice(0, 8)}`;
    const seq = testSequence("bulk-ns-seq", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const future = new Date(Date.now() + 60 * 60 * 1000).toISOString();

    const { id: idAlpha } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "ns-alpha",
      next_fire_at: future,
    });
    const { id: idBeta } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "ns-beta",
      next_fire_at: future,
    });

    // Bulk cancel only ns-alpha.
    const res = await client.bulkUpdateState(
      {
        tenant_id: tenantId,
        sequence_id: seq.id,
        namespace: "ns-alpha",
        states: ["scheduled"],
      },
      "cancelled",
    );
    assert.ok(
      (res as any).count >= 1,
      `expected at least 1 cancelled, got ${(res as any).count}`,
    );

    const alpha = await client.getInstance(idAlpha);
    const beta = await client.getInstance(idBeta);

    assert.equal(alpha.state, "cancelled", "alpha should be cancelled");
    assert.equal(beta.state, "scheduled", "beta should remain scheduled");
  });

  it("bulk reschedule only affects instances in the specified namespace", async () => {
    const tenantId = `bulk-ns-r-${uuid().slice(0, 8)}`;
    const seq = testSequence("bulk-ns-r-seq", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const future = new Date(Date.now() + 60 * 60 * 1000).toISOString();
    const { id: idAlpha } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "ns-alpha",
      next_fire_at: future,
    });
    const { id: idBeta } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "ns-beta",
      next_fire_at: future,
    });

    const beforeAlpha = await client.getInstance(idAlpha);
    const beforeAlphaTime = new Date(beforeAlpha.next_fire_at as string).getTime();

    // Bulk reschedule only ns-alpha.
    const res = await client.bulkReschedule(
      {
        tenant_id: tenantId,
        sequence_id: seq.id,
        namespace: "ns-alpha",
      },
      1800,
    );
    assert.equal(
      (res as any).count,
      1,
      `expected exactly 1 rescheduled, got ${(res as any).count}`,
    );

    const afterAlpha = await client.getInstance(idAlpha);
    const afterBeta = await client.getInstance(idBeta);

    // Alpha must have moved forward by ~1800s.
    const alphaAfterTime = new Date(afterAlpha.next_fire_at as string).getTime();
    const alphaDiff = (alphaAfterTime - beforeAlphaTime) / 1000;
    assert.ok(
      alphaDiff >= 1700 && alphaDiff <= 1900,
      `alpha should shift ~1800s, got ${alphaDiff}`,
    );

    // Beta must be virtually unchanged (within a few seconds of original).
    const betaAfterTime = new Date(afterBeta.next_fire_at as string).getTime();
    const betaDiff = (betaAfterTime - new Date(future).getTime()) / 1000;
    assert.ok(
      Math.abs(betaDiff) < 60,
      `beta should be virtually unchanged, got diff=${betaDiff}`,
    );
  });
});
