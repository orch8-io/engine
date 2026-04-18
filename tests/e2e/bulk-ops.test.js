import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "./client.js";
import { startServer, stopServer } from "./harness.js";

const client = new Orch8Client();

describe("Bulk Operations", () => {
  let server;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("bulk cancel: filter by tenant + sequence moves matching instances to cancelled", async () => {
    const tenantId = `bulk-cancel-${uuid().slice(0, 8)}`;

    const seq = testSequence(
      "bulk-cancel-seq",
      [step("s1", "noop")],
      { tenantId }
    );
    await client.createSequence(seq);

    // Schedule 3 instances far in the future so they stay in `scheduled` state.
    const future = new Date(Date.now() + 60 * 60 * 1000).toISOString();
    const ids = [];
    for (let i = 0; i < 3; i += 1) {
      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace: "default",
        next_fire_at: future,
      });
      ids.push(id);
    }

    // Bulk cancel everything under this tenant+sequence.
    const res = await client.bulkUpdateState(
      {
        tenant_id: tenantId,
        sequence_id: seq.id,
        states: ["scheduled"],
      },
      "cancelled"
    );
    assert.ok(res.count >= 3, `expected at least 3 updated, got ${res.count}`);

    // Verify each instance now reports cancelled.
    for (const id of ids) {
      const inst = await client.getInstance(id);
      assert.equal(inst.state, "cancelled", `instance ${id} should be cancelled`);
    }
  });

  it("bulk reschedule: shifts next_fire_at by offset_secs for scheduled instances", async () => {
    const tenantId = `bulk-resch-${uuid().slice(0, 8)}`;

    const seq = testSequence(
      "bulk-resch-seq",
      [step("s1", "noop")],
      { tenantId }
    );
    await client.createSequence(seq);

    const firstFire = new Date(Date.now() + 60 * 60 * 1000);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      next_fire_at: firstFire.toISOString(),
    });

    const before = await client.getInstance(id);
    const beforeT = new Date(before.next_fire_at).getTime();

    const offsetSecs = 3600; // shift 1 hour later
    const res = await client.bulkReschedule(
      { tenant_id: tenantId, sequence_id: seq.id },
      offsetSecs
    );
    assert.ok(res.count >= 1, `expected >=1 rescheduled, got ${res.count}`);

    const after = await client.getInstance(id);
    const afterT = new Date(after.next_fire_at).getTime();

    // The SQL uses make_interval(secs => offset). Require at least ~3500s of
    // shift to allow for clock skew and rounding; but not more than ~3700s.
    const diffSecs = (afterT - beforeT) / 1000;
    assert.ok(
      diffSecs >= 3500 && diffSecs <= 3700,
      `expected next_fire_at to shift ~3600s, got ${diffSecs}`
    );
  });

  it("bulk reschedule: only affects scheduled instances (not completed)", async () => {
    const tenantId = `bulk-resch-done-${uuid().slice(0, 8)}`;

    const seq = testSequence(
      "bulk-resch-done-seq",
      [step("s1", "noop")],
      { tenantId }
    );
    await client.createSequence(seq);

    // This instance runs immediately and completes.
    const { id: completedId } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    const done = await client.waitForState(completedId, "completed");
    assert.equal(done.state, "completed");

    // This instance stays scheduled.
    const future = new Date(Date.now() + 60 * 60 * 1000).toISOString();
    const { id: scheduledId } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      next_fire_at: future,
    });

    const beforeScheduled = await client.getInstance(scheduledId);

    const res = await client.bulkReschedule(
      { tenant_id: tenantId, sequence_id: seq.id },
      1800
    );
    // Only the scheduled one should be affected.
    assert.equal(res.count, 1, `expected exactly 1 rescheduled, got ${res.count}`);

    const afterScheduled = await client.getInstance(scheduledId);
    const diff =
      (new Date(afterScheduled.next_fire_at).getTime() -
        new Date(beforeScheduled.next_fire_at).getTime()) /
      1000;
    assert.ok(diff >= 1700 && diff <= 1900, `shift should be ~1800s, got ${diff}`);
  });
});
