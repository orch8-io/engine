import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid, ApiError } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Cron Schedules", () => {
  let server: ServerHandle | undefined;
  const cronIds: string[] = [];

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    // Clean up all crons created during this suite so they don't fire
    // during subsequent suites in attach mode.
    for (const id of cronIds) {
      try {
        await client.deleteCron(id);
      } catch {
        // Already deleted or doesn't exist — fine.
      }
    }
    await stopServer(server);
  });

  it("should create a cron schedule and get it back", async () => {
    const seq = testSequence("cron-basic", [step("s1", "noop")]);
    await client.createSequence(seq);

    const result = await client.createCron({
      tenant_id: "test",
      namespace: "default",
      sequence_id: seq.id,
      cron_expr: "0 0 9 * * MON-FRI *",
    });
    cronIds.push(result.id as string);

    assert.ok(result.id, "should return cron schedule id");
    assert.ok(result.next_fire_at, "should have next_fire_at computed");

    const fetched = await client.getCron(result.id as string);
    assert.equal(fetched.id, result.id);
    assert.equal(fetched.cron_expr, "0 0 9 * * MON-FRI *");
    assert.equal(fetched.enabled, true);
    assert.equal(fetched.timezone, "UTC");
  });

  it("should reject invalid cron expressions", async () => {
    const seq = testSequence("cron-invalid", [step("s1", "noop")]);
    await client.createSequence(seq);

    await assert.rejects(
      () =>
        client.createCron({
          tenant_id: "test",
          namespace: "default",
          sequence_id: seq.id,
          cron_expr: "not valid cron",
        }),
      (err: unknown) => {
        const e = err as ApiError;
        assert.ok(e.status === 400 || e.status === 422);
        return true;
      }
    );
  });

  it("should list cron schedules by tenant", async () => {
    const tenantId = `cron-tenant-${uuid().slice(0, 8)}`;
    const seq = testSequence("cron-list", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const result = await client.createCron({
      tenant_id: tenantId,
      namespace: "default",
      sequence_id: seq.id,
      cron_expr: "0 * * * * * *",
    });
    cronIds.push(result.id as string);

    const list = await client.listCron({ tenant_id: tenantId });
    assert.ok(list.length >= 1);
    assert.ok(list.every((c) => c.tenant_id === tenantId));
  });

  it("should update a cron schedule", async () => {
    const seq = testSequence("cron-update", [step("s1", "noop")]);
    await client.createSequence(seq);

    const { id } = await client.createCron({
      tenant_id: "test",
      namespace: "default",
      sequence_id: seq.id,
      cron_expr: "0 0 9 * * MON-FRI *",
    });
    cronIds.push(id as string);

    const updated = await client.updateCron(id as string, {
      cron_expr: "0 0 12 * * * *",
      enabled: false,
    });

    assert.equal(updated.cron_expr, "0 0 12 * * * *");
    assert.equal(updated.enabled, false);

    // Delete after asserting — a disabled row can interfere with cron
    // processing in attach mode.
    await client.deleteCron(id as string);
  });

  it("should delete a cron schedule", async () => {
    const seq = testSequence("cron-delete", [step("s1", "noop")]);
    await client.createSequence(seq);

    const { id } = await client.createCron({
      tenant_id: "test",
      namespace: "default",
      sequence_id: seq.id,
      cron_expr: "0 0 9 * * MON-FRI *",
    });

    await client.deleteCron(id as string);

    await assert.rejects(() => client.getCron(id as string), (err: unknown) => {
      assert.equal((err as ApiError).status, 404);
      return true;
    });
  });

  // Plan #106: disable cron stops firing.
  it("should not create instances while cron is disabled", async () => {
    const tenantId = `cron-disabled-${uuid().slice(0, 8)}`;
    const seq = testSequence("cron-disabled", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    // Create cron already disabled to avoid race between creation and
    // the disable update (the cron tick could claim it in between).
    const { id } = await client.createCron({
      tenant_id: tenantId,
      namespace: "default",
      sequence_id: seq.id,
      cron_expr: "* * * * * * *",
      enabled: false,
    });
    cronIds.push(id as string);

    // Wait longer than the cron loop tick so we're well past any
    // firing opportunity.
    await new Promise((r) => setTimeout(r, 3_000));

    // Delete immediately — a disabled cron row can interfere with cron
    // processing in attach mode.
    await client.deleteCron(id as string);

    const instances = await client.listInstances({ tenant_id: tenantId });
    assert.equal(
      instances.length,
      0,
      `disabled cron must not fire; got ${instances.length} instances`,
    );
  });

  it("should trigger a cron schedule and create an instance", async () => {
    const tenantId = `cron-fire-${uuid().slice(0, 8)}`;
    const seq = testSequence("cron-fire", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    // Create a cron that fires every second.
    const { id } = await client.createCron({
      tenant_id: tenantId,
      namespace: "default",
      sequence_id: seq.id,
      cron_expr: "* * * * * * *",
    });
    cronIds.push(id as string);

    // Wait for the cron loop to pick it up (runs every 1s in test via ORCH8_CRON_TICK_SECS).
    await new Promise((r) => setTimeout(r, 3_000));

    // Delete immediately so it doesn't keep firing during subsequent suites.
    await client.deleteCron(id as string);

    // Check that at least one instance was created for this tenant.
    const instances = await client.listInstances({ tenant_id: tenantId });
    assert.ok(
      instances.length >= 1,
      `Expected at least 1 instance from cron trigger, got ${instances.length}`
    );
  });
});
