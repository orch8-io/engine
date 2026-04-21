/**
 * Trigger Fire Errors — verifies error responses when firing triggers
 * under invalid conditions.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Trigger Fire Errors", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("fire non-existent trigger returns 404", async () => {
    try {
      await client.fireTrigger(`nonexistent-${uuid().slice(0, 8)}`, {});
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("fire trigger for missing sequence returns 500/404", async () => {
    const slug = `trig-miss-${uuid().slice(0, 8)}`;
    await client.createTrigger({
      slug,
      sequence_name: `definitely-missing-${uuid().slice(0, 8)}`,
      tenant_id: "test",
      namespace: "default",
      trigger_type: "event",
    });

    try {
      await client.fireTrigger(slug, {});
      assert.fail("should throw");
    } catch (err: any) {
      assert.ok(err.status === 404 || err.status === 500, `expected 404/500, got ${err.status}`);
    }
  });

  it("fire deleted trigger returns 404", async () => {
    const seq = testSequence("trig-del-seq", [step("s1", "noop")]);
    await client.createSequence(seq);
    const slug = `trig-del-${uuid().slice(0, 8)}`;
    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: "test",
      namespace: "default",
      trigger_type: "event",
    });

    await client.deleteTrigger(slug);

    try {
      await client.fireTrigger(slug, {});
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("fire webhook trigger without secret returns 401", async () => {
    const seq = testSequence("trig-wh-err", [step("s1", "noop")]);
    await client.createSequence(seq);
    const slug = `trig-wh-err-${uuid().slice(0, 8)}`;
    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: "test",
      namespace: "default",
      trigger_type: "webhook",
    });

    try {
      await client.fireWebhook(slug, {});
      assert.fail("should throw 401");
    } catch (err: any) {
      assert.equal(err.status, 401);
    }
  });

  it("fire event trigger succeeds and creates instance", async () => {
    const seq = testSequence("trig-ok-seq", [step("s1", "noop")]);
    await client.createSequence(seq);
    const slug = `trig-ok-${uuid().slice(0, 8)}`;
    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: "test",
      namespace: "default",
      trigger_type: "event",
    });

    const res = await client.fireTrigger(slug, { hello: "world" });
    assert.ok((res as any).instance_id, "should create instance");
    assert.equal((res as any).trigger, slug);
  });
});
