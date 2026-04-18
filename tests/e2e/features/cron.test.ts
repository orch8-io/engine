import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid, ApiError } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Cron Schedules", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
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

    await client.createCron({
      tenant_id: tenantId,
      namespace: "default",
      sequence_id: seq.id,
      cron_expr: "0 * * * * * *",
    });

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

    const updated = await client.updateCron(id as string, {
      cron_expr: "0 0 12 * * * *",
      enabled: false,
    });

    assert.equal(updated.cron_expr, "0 0 12 * * * *");
    assert.equal(updated.enabled, false);
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

  it("should trigger a cron schedule and create an instance", async () => {
    const tenantId = `cron-fire-${uuid().slice(0, 8)}`;
    const seq = testSequence("cron-fire", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    // Create a cron that fires every second (cron library uses sec min hr dom mon dow yr).
    await client.createCron({
      tenant_id: tenantId,
      namespace: "default",
      sequence_id: seq.id,
      cron_expr: "* * * * * * *", // every second
    });

    // Wait for the cron loop to pick it up (runs every 10s, but next_fire_at is immediate).
    await new Promise((r) => setTimeout(r, 15_000));

    // Check that at least one instance was created for this tenant.
    const instances = await client.listInstances({ tenant_id: tenantId });
    assert.ok(
      instances.length >= 1,
      `Expected at least 1 instance from cron trigger, got ${instances.length}`
    );
  });
});
