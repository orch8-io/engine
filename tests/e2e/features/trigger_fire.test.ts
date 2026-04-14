/**
 * Trigger fire — verifies that firing a trigger creates an instance
 * and that webhook alias also works.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Trigger Fire", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("firing a trigger creates and runs an instance", async () => {
    const tenantId = `trig-fire-${uuid().slice(0, 8)}`;
    const slug = `trig-${uuid().slice(0, 8)}`;

    const seq = testSequence("trig-target", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: tenantId,
      namespace: "default",
      trigger_type: "webhook",
    });

    const result = await client.fireTrigger(slug, { custom: "data" });
    const instanceId = (result as any).instance_id ?? (result as any).id;
    assert.ok(instanceId, "fire should return instance id");

    const final = await client.waitForState(instanceId, "completed", {
      timeoutMs: 10_000,
    });
    assert.equal(final.state, "completed");
  });

  it("firing non-existent trigger returns 404", async () => {
    try {
      await client.fireTrigger(`nonexistent-${uuid().slice(0, 8)}`);
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("webhook alias fires the same trigger", async () => {
    const tenantId = `wh-fire-${uuid().slice(0, 8)}`;
    const slug = `wh-${uuid().slice(0, 8)}`;
    const secret = `secret-${uuid().slice(0, 8)}`;

    const seq = testSequence("wh-target", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: tenantId,
      namespace: "default",
      trigger_type: "webhook",
      secret,
    });

    // Webhook endpoint requires x-trigger-secret header.
    const result = await client.fireWebhook(slug, { via: "webhook" }, {
      "x-trigger-secret": secret,
    });
    const instanceId = (result as any).instance_id ?? (result as any).id;
    assert.ok(instanceId, "webhook fire should return instance id");

    const final = await client.waitForState(instanceId, "completed", {
      timeoutMs: 10_000,
    });
    assert.equal(final.state, "completed");
  });
});
