/**
 * Webhook Replay Protection and Errors — verifies webhook security
 * headers: timestamp, nonce, and secret validation.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Webhook Replay and Errors", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("webhook with valid headers succeeds", async () => {
    const tenantId = `wh-ok-${uuid().slice(0, 8)}`;
    const slug = `wh-ok-${uuid().slice(0, 8)}`;
    const secret = `sec-${uuid().slice(0, 8)}`;

    const seq = testSequence("wh-ok", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: tenantId,
      namespace: "default",
      trigger_type: "webhook",
      secret,
    });

    const result = await client.fireWebhook(slug, { ok: true }, {
      "x-trigger-secret": secret,
    });
    assert.ok((result as any).instance_id, "should return instance_id");
  });

  it("webhook with missing timestamp returns 401", async () => {
    const tenantId = `wh-ts-${uuid().slice(0, 8)}`;
    const slug = `wh-ts-${uuid().slice(0, 8)}`;
    const secret = `sec-${uuid().slice(0, 8)}`;

    const seq = testSequence("wh-ts", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: tenantId,
      namespace: "default",
      trigger_type: "webhook",
      secret,
    });

    try {
      await client.fireWebhook(slug, { ok: true }, {
        "x-trigger-secret": secret,
        "x-trigger-timestamp": "",
        "x-trigger-nonce": "nonce",
      });
      assert.fail("should throw 401");
    } catch (err: any) {
      assert.equal(err.status, 401);
    }
  });

  it("webhook with missing nonce returns 401", async () => {
    const tenantId = `wh-nonce-${uuid().slice(0, 8)}`;
    const slug = `wh-nonce-${uuid().slice(0, 8)}`;
    const secret = `sec-${uuid().slice(0, 8)}`;

    const seq = testSequence("wh-nonce", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: tenantId,
      namespace: "default",
      trigger_type: "webhook",
      secret,
    });

    try {
      await client.fireWebhook(slug, { ok: true }, {
        "x-trigger-secret": secret,
        "x-trigger-timestamp": String(Math.floor(Date.now() / 1000)),
        "x-trigger-nonce": "",
      });
      assert.fail("should throw 401");
    } catch (err: any) {
      assert.equal(err.status, 401);
    }
  });

  it("webhook with expired timestamp returns 401", async () => {
    const tenantId = `wh-exp-${uuid().slice(0, 8)}`;
    const slug = `wh-exp-${uuid().slice(0, 8)}`;
    const secret = `sec-${uuid().slice(0, 8)}`;

    const seq = testSequence("wh-exp", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: tenantId,
      namespace: "default",
      trigger_type: "webhook",
      secret,
    });

    const expired = String(Math.floor(Date.now() / 1000) - 400); // > 300s window
    try {
      await client.fireWebhook(slug, { ok: true }, {
        "x-trigger-secret": secret,
        "x-trigger-timestamp": expired,
        "x-trigger-nonce": crypto.randomUUID(),
      });
      assert.fail("should throw 401");
    } catch (err: any) {
      assert.equal(err.status, 401);
    }
  });

  it("webhook with wrong secret returns 401", async () => {
    const tenantId = `wh-wrong-${uuid().slice(0, 8)}`;
    const slug = `wh-wrong-${uuid().slice(0, 8)}`;
    const secret = `sec-${uuid().slice(0, 8)}`;

    const seq = testSequence("wh-wrong", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: tenantId,
      namespace: "default",
      trigger_type: "webhook",
      secret,
    });

    try {
      await client.fireWebhook(slug, { ok: true }, {
        "x-trigger-secret": "wrong-secret",
      });
      assert.fail("should throw 401");
    } catch (err: any) {
      assert.equal(err.status, 401);
    }
  });

  it("secretless trigger webhook is rejected with 401", async () => {
    const tenantId = `wh-none-${uuid().slice(0, 8)}`;
    const slug = `wh-none-${uuid().slice(0, 8)}`;

    const seq = testSequence("wh-none", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: tenantId,
      namespace: "default",
      trigger_type: "webhook",
    });

    try {
      await client.fireWebhook(slug, { ok: true });
      assert.fail("should throw 401");
    } catch (err: any) {
      assert.equal(err.status, 401);
    }
  });

  it("replay with same nonce is rejected", async () => {
    const tenantId = `wh-replay-${uuid().slice(0, 8)}`;
    const slug = `wh-replay-${uuid().slice(0, 8)}`;
    const secret = `sec-${uuid().slice(0, 8)}`;

    const seq = testSequence("wh-replay", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: tenantId,
      namespace: "default",
      trigger_type: "webhook",
      secret,
    });

    const nonce = crypto.randomUUID();
    const ts = String(Math.floor(Date.now() / 1000));

    await client.fireWebhook(slug, { first: true }, {
      "x-trigger-secret": secret,
      "x-trigger-timestamp": ts,
      "x-trigger-nonce": nonce,
    });

    try {
      await client.fireWebhook(slug, { second: true }, {
        "x-trigger-secret": secret,
        "x-trigger-timestamp": ts,
        "x-trigger-nonce": nonce,
      });
      assert.fail("should throw 401 on replay");
    } catch (err: any) {
      assert.equal(err.status, 401);
    }
  });
});
