/**
 * Webhook Secret Validation — verifies x-trigger-secret header enforcement.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Webhook Secret Validation", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("webhook without secret is rejected", async () => {
    const seq = testSequence("wh-no-secret", [step("s1", "noop")]);
    await client.createSequence(seq);
    const slug = `wh-open-${uuid().slice(0, 8)}`;
    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: "test",
      namespace: "default",
      trigger_type: "webhook",
    });

    try {
      await client.fireWebhook(slug, { data: 1 });
      assert.fail("should throw 401");
    } catch (err: any) {
      assert.equal(err.status, 401);
    }
  });

  it("webhook with secret rejects missing header", async () => {
    const seq = testSequence("wh-secret-missing", [step("s1", "noop")]);
    await client.createSequence(seq);
    const slug = `wh-sec-${uuid().slice(0, 8)}`;
    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: "test",
      namespace: "default",
      trigger_type: "webhook",
      secret: "shhh",
    });

    try {
      await client.fireWebhook(slug, { data: 1 });
      assert.fail("should throw 401");
    } catch (err: any) {
      assert.equal(err.status, 401);
    }
  });

  it("webhook with secret rejects wrong header", async () => {
    const seq = testSequence("wh-secret-wrong", [step("s1", "noop")]);
    await client.createSequence(seq);
    const slug = `wh-wrong-${uuid().slice(0, 8)}`;
    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: "test",
      namespace: "default",
      trigger_type: "webhook",
      secret: "shhh",
    });

    try {
      await client.fireWebhook(slug, { data: 1 }, { "x-trigger-secret": "nope" });
      assert.fail("should throw 401");
    } catch (err: any) {
      assert.equal(err.status, 401);
    }
  });

  it("webhook with secret accepts correct header", async () => {
    const seq = testSequence("wh-secret-ok", [step("s1", "noop")]);
    await client.createSequence(seq);
    const slug = `wh-ok-${uuid().slice(0, 8)}`;
    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: "test",
      namespace: "default",
      trigger_type: "webhook",
      secret: "shhh",
    });

    const res = await client.fireWebhook(slug, { data: 1 }, { "x-trigger-secret": "shhh" });
    assert.ok((res as any).instance_id, "should create instance");
  });

  it("disabled webhook trigger returns 400", async () => {
    const seq = testSequence("wh-disabled", [step("s1", "noop")]);
    await client.createSequence(seq);
    const slug = `wh-off-${uuid().slice(0, 8)}`;
    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: "test",
      namespace: "default",
      trigger_type: "webhook",
    });

    // There is no disable endpoint; we delete it to make it unavailable.
    await client.deleteTrigger(slug);

    try {
      await client.fireWebhook(slug, { data: 1 });
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.ok(err.status === 404 || err.status === 400, `expected 404/400, got ${err.status}`);
    }
  });
});
