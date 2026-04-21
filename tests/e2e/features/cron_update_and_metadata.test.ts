/**
 * Cron Update and Metadata — verifies PATCH /cron/{id} and list filtering.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Cron Update and Metadata", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("update cron schedule expression", async () => {
    const seq = testSequence("cron-up-expr", [step("s1", "noop")]);
    await client.createSequence(seq);

    const created = await client.createCron({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      cron_expr: "*/5 * * * *",
      timezone: "UTC",
    });

    await client.updateCron(created.id as string, { cron_expr: "0 0 * * *" });
    const fetched = await client.getCron(created.id as string);
    assert.equal((fetched as any).cron_expr, "0 0 * * *");
  });

  it("update cron timezone", async () => {
    const seq = testSequence("cron-up-tz", [step("s1", "noop")]);
    await client.createSequence(seq);

    const created = await client.createCron({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      cron_expr: "0 * * * *",
      timezone: "UTC",
    });

    await client.updateCron(created.id as string, { timezone: "America/New_York" });
    const fetched = await client.getCron(created.id as string);
    assert.equal((fetched as any).timezone, "America/New_York");
  });

  it("list cron is tenant-scoped", async () => {
    const seq = testSequence("cron-scope", [step("s1", "noop")]);
    await client.createSequence(seq);

    await client.createCron({
      sequence_id: seq.id,
      tenant_id: "tenant-a",
      namespace: "default",
      cron_expr: "0 * * * *",
      timezone: "UTC",
    });
    await client.createCron({
      sequence_id: seq.id,
      tenant_id: "tenant-b",
      namespace: "default",
      cron_expr: "0 * * * *",
      timezone: "UTC",
    });

    const listA = await client.listCron({ tenant_id: "tenant-a" });
    assert.ok(listA.length >= 1);
    assert.ok(listA.every((c: any) => c.tenant_id === "tenant-a"));
  });

  it("delete cron removes it from list", async () => {
    const seq = testSequence("cron-del", [step("s1", "noop")]);
    await client.createSequence(seq);

    const created = await client.createCron({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      cron_expr: "0 * * * *",
      timezone: "UTC",
    });

    await client.deleteCron(created.id as string);

    const list = await client.listCron({ tenant_id: "test" });
    assert.ok(!list.some((c: any) => c.id === created.id));
  });

  it("get deleted cron returns 404", async () => {
    const seq = testSequence("cron-get-del", [step("s1", "noop")]);
    await client.createSequence(seq);

    const created = await client.createCron({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      cron_expr: "0 * * * *",
      timezone: "UTC",
    });

    await client.deleteCron(created.id as string);

    try {
      await client.getCron(created.id as string);
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });
});
