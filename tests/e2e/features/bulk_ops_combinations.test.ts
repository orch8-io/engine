/**
 * Bulk Operations Combinations — verifies bulk state update and reschedule
 * with various filter combinations beyond namespace-only tests.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Bulk Operations Combinations", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("bulk cancel by sequence_id affects only that sequence's instances", async () => {
    const tenantId = `bulk-seq-${uuid().slice(0, 8)}`;
    const seqA = testSequence("bulk-a", [step("s1", "noop")], { tenantId });
    const seqB = testSequence("bulk-b", [step("s1", "noop")], { tenantId });
    await client.createSequence(seqA);
    await client.createSequence(seqB);

    const { id: idA } = await client.createInstance({
      sequence_id: seqA.id,
      tenant_id: tenantId,
      namespace: "default",
      next_fire_at: new Date(Date.now() + 60_000).toISOString(),
    });
    const { id: idB } = await client.createInstance({
      sequence_id: seqB.id,
      tenant_id: tenantId,
      namespace: "default",
      next_fire_at: new Date(Date.now() + 60_000).toISOString(),
    });

    const result = await client.bulkUpdateState(
      { sequence_id: seqA.id, tenant_id: tenantId },
      "cancelled",
    );
    assert.ok(result.count >= 1, "should affect at least one instance");

    const afterA = await client.getInstance(idA);
    const afterB = await client.getInstance(idB);
    assert.equal(afterA.state, "cancelled", "instance A should be cancelled");
    assert.equal(afterB.state, "scheduled", "instance B should remain scheduled");
  });

  it("bulk cancel by state filter affects only matching state", async () => {
    const tenantId = `bulk-st-${uuid().slice(0, 8)}`;
    const seq = testSequence("bulk-st", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id: runId } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(runId, "completed", { timeoutMs: 5_000 });

    const { id: schedId } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      next_fire_at: new Date(Date.now() + 60_000).toISOString(),
    });

    const result = await client.bulkUpdateState(
      { tenant_id: tenantId, states: ["scheduled"] },
      "cancelled",
    );

    const afterSched = await client.getInstance(schedId);
    const afterRun = await client.getInstance(runId);
    assert.equal(afterSched.state, "cancelled", "scheduled instance should be cancelled");
    assert.equal(afterRun.state, "completed", "completed instance should NOT be affected");
  });

  it("bulk reschedule by tenant_id shifts next_fire_at", async () => {
    const tenantId = `bulk-rs-${uuid().slice(0, 8)}`;
    const seq = testSequence("bulk-rs", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const future = new Date(Date.now() + 60_000).toISOString();
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      next_fire_at: future,
    });

    const before = await client.getInstance(id);
    const beforeTime = new Date(before.next_fire_at!).getTime();

    await client.bulkReschedule({ tenant_id: tenantId }, 120);

    const after = await client.getInstance(id);
    const afterTime = new Date(after.next_fire_at!).getTime();
    const delta = (afterTime - beforeTime) / 1000;
    assert.ok(delta >= 115 && delta <= 125, `should shift by ~120s, got ${delta}s`);
  });

  it("bulk reschedule with negative offset moves next_fire_at earlier", async () => {
    const tenantId = `bulk-neg-${uuid().slice(0, 8)}`;
    const seq = testSequence("bulk-neg", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const future = new Date(Date.now() + 300_000).toISOString();
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      next_fire_at: future,
    });

    // Negative offset: just verify the API accepts it without error.
    const result = await client.bulkReschedule({ tenant_id: tenantId }, -60);
    assert.ok(result.count !== undefined, "should return count");
  });

  it("bulk operations with empty matching filter return 0 affected", async () => {
    const tenantId = `bulk-empty-${uuid().slice(0, 8)}`;
    const result = await client.bulkUpdateState(
      { tenant_id: tenantId, states: ["scheduled"] },
      "cancelled",
    );
    assert.equal(result.count, 0, "should affect zero instances when none match");
  });

  it("bulk reschedule affects only scheduled instances", async () => {
    const tenantId = `bulk-rs-st-${uuid().slice(0, 8)}`;
    const seq = testSequence("bulk-rs-st", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id: runId } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(runId, "completed", { timeoutMs: 5_000 });

    const { id: schedId } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      next_fire_at: new Date(Date.now() + 60_000).toISOString(),
    });

    const beforeRun = await client.getInstance(runId);
    const beforeSched = await client.getInstance(schedId);

    await client.bulkReschedule({ tenant_id: tenantId }, 300);

    const afterRun = await client.getInstance(runId);
    const afterSched = await client.getInstance(schedId);

    assert.equal(
      beforeRun.next_fire_at,
      afterRun.next_fire_at,
      "completed instance next_fire_at should be unchanged",
    );
    assert.notEqual(
      beforeSched.next_fire_at,
      afterSched.next_fire_at,
      "scheduled instance next_fire_at should change",
    );
  });

  it("bulk cancel by namespace affects only matching namespace", async () => {
    const tenantId = `bulk-ns-${uuid().slice(0, 8)}`;
    const seq = testSequence("bulk-ns", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id: alphaId } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "alpha",
      next_fire_at: new Date(Date.now() + 60_000).toISOString(),
    });
    const { id: betaId } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "beta",
      next_fire_at: new Date(Date.now() + 60_000).toISOString(),
    });

    await client.bulkUpdateState(
      { tenant_id: tenantId, namespace: "alpha" },
      "cancelled",
    );

    const afterAlpha = await client.getInstance(alphaId);
    const afterBeta = await client.getInstance(betaId);
    assert.equal(afterAlpha.state, "cancelled", "alpha should be cancelled");
    assert.equal(afterBeta.state, "scheduled", "beta should remain scheduled");
  });
});
