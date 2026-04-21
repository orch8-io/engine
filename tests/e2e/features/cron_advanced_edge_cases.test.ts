/**
 * Cron Advanced Edge Cases — verifies cron timezone, disable, update,
 * delete, and metadata behavior.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Cron Advanced Edge Cases", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("cron with timezone stores timezone", async () => {
    const tenantId = `cron-tz-${uuid().slice(0, 8)}`;
    const seq = testSequence("cron-tz", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const created = await client.createCron({
      tenant_id: tenantId,
      namespace: "default",
      sequence_id: seq.id,
      cron_expr: "0 0 * * *",
      timezone: "America/New_York",
    });

    const fetched = await client.getCron(created.id);
    assert.equal(fetched.timezone, "America/New_York");
  });

  it("disabled cron does not fire", async () => {
    const tenantId = `cron-dis-${uuid().slice(0, 8)}`;
    const seq = testSequence("cron-dis", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const created = await client.createCron({
      tenant_id: tenantId,
      namespace: "default",
      sequence_id: seq.id,
      cron_expr: "* * * * *",
      enabled: false,
    });

    // Wait a few seconds — a disabled cron should not create instances.
    await new Promise((r) => setTimeout(r, 2_500));

    const instances = await client.listInstances({ tenant_id: tenantId });
    assert.equal(instances.length, 0, "disabled cron should not create instances");

    await client.deleteCron(created.id);
  });

  it("update cron expression changes next_fire_at", async () => {
    const tenantId = `cron-up-${uuid().slice(0, 8)}`;
    const seq = testSequence("cron-up", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const created = await client.createCron({
      tenant_id: tenantId,
      namespace: "default",
      sequence_id: seq.id,
      cron_expr: "0 0 * * *",
    });

    const before = await client.getCron(created.id);
    const beforeTime = new Date(before.next_fire_at).getTime();

    // Change to every minute — next fire should be much sooner.
    await client.updateCron(created.id, { cron_expr: "* * * * *" });

    const after = await client.getCron(created.id);
    const afterTime = new Date(after.next_fire_at).getTime();
    assert.ok(afterTime < beforeTime, "next_fire_at should move earlier");

    await client.deleteCron(created.id);
  });

  it("delete cron stops firing", async () => {
    const tenantId = `cron-del-${uuid().slice(0, 8)}`;
    const seq = testSequence("cron-del", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const created = await client.createCron({
      tenant_id: tenantId,
      namespace: "default",
      sequence_id: seq.id,
      cron_expr: "* * * * *",
    });

    await client.deleteCron(created.id);

    try {
      await client.getCron(created.id);
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("list cron by tenant filter", async () => {
    const tenantA = `cron-a-${uuid().slice(0, 8)}`;
    const tenantB = `cron-b-${uuid().slice(0, 8)}`;
    const seqA = testSequence("cron-ta", [step("s1", "noop")], { tenantId: tenantA });
    const seqB = testSequence("cron-tb", [step("s1", "noop")], { tenantId: tenantB });
    await client.createSequence(seqA);
    await client.createSequence(seqB);

    await client.createCron({
      tenant_id: tenantA,
      namespace: "default",
      sequence_id: seqA.id,
      cron_expr: "0 0 * * *",
    });

    const listA = await client.listCron({ tenant_id: tenantA });
    assert.ok(listA.length >= 1, "tenant A should see their cron");
    assert.ok(listA.every((c: any) => c.tenant_id === tenantA), "all results should be tenant A");
  });

  it("invalid cron expression returns error", async () => {
    const tenantId = `cron-inv-${uuid().slice(0, 8)}`;
    const seq = testSequence("cron-inv", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    try {
      await client.createCron({
        tenant_id: tenantId,
        namespace: "default",
        sequence_id: seq.id,
        cron_expr: "not-a-cron",
      });
      assert.fail("should throw error");
    } catch (err: any) {
      assert.ok(err.status >= 400, `expected error status, got ${err.status}`);
    }
  });

  it("cron metadata round-trips", async () => {
    const tenantId = `cron-meta-${uuid().slice(0, 8)}`;
    const seq = testSequence("cron-meta", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const created = await client.createCron({
      tenant_id: tenantId,
      namespace: "default",
      sequence_id: seq.id,
      cron_expr: "0 0 * * *",
      metadata: { owner: "team-alpha", priority: "high" },
    });

    const fetched = await client.getCron(created.id);
    assert.deepEqual((fetched.metadata as any)?.owner, "team-alpha");

    await client.deleteCron(created.id);
  });
});
