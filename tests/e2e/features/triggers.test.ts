import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid, ApiError } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

/**
 * Trigger CRUD + fire.
 *
 * Notes from `orch8-api/src/triggers.rs`:
 *   - Routes exposed: POST /triggers, GET /triggers, GET /triggers/{slug},
 *     DELETE /triggers/{slug}, POST /triggers/{slug}/fire.
 *   - No PUT endpoint is implemented — updates are not part of the current
 *     trigger API. Tests below therefore do not exercise PUT.
 *   - `POST /webhooks/{slug}` accepts only `trigger_type=webhook` with a
 *     configured `secret`.
 *   - Firing a trigger creates an instance from the trigger's
 *     {sequence_name, version, tenant_id, namespace}; the request body
 *     becomes the new instance's `context.data`.
 */
describe("Triggers", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("creates, reads, lists, and deletes a trigger", async () => {
    const tenantId = `trig-crud-${uuid().slice(0, 8)}`;
    const slug = `trig-${uuid().slice(0, 8)}`;

    // A backing sequence to bind the trigger to (not fired in this test).
    const seq = testSequence("trig-crud-seq", [step("s1", "noop")], {
      tenantId,
    });
    await client.createSequence(seq);

    // Create.
    const created = await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: tenantId,
      namespace: "default",
      trigger_type: "event",
    });
    assert.equal(created.slug, slug);
    assert.equal(created.enabled, true);

    // Get.
    const fetched = await client.getTrigger(slug);
    assert.equal(fetched.slug, slug);
    assert.equal(fetched.sequence_name, seq.name);

    // List filtered by tenant.
    const list = await client.listTriggers({ tenant_id: tenantId });
    assert.ok(
      list.some((t) => t.slug === slug),
      "listed triggers should include the new slug"
    );
    assert.ok(
      list.every((t) => t.tenant_id === tenantId),
      "list filter should enforce tenant_id"
    );

    // Delete.
    await client.deleteTrigger(slug);

    // Verify 404 after delete.
    await assert.rejects(() => client.getTrigger(slug), (err: unknown) => {
      assert.equal((err as ApiError).status, 404);
      return true;
    });
  });

  it("rejects trigger creation with missing required fields", async () => {
    await assert.rejects(
      () =>
        client.createTrigger({
          slug: "",
          sequence_name: "x",
          tenant_id: "t",
        }),
      (err: unknown) => {
        const status = (err as ApiError).status;
        assert.ok(status >= 400 && status < 500);
        return true;
      }
    );
  });

  it("firing a trigger creates an instance from its sequence", async () => {
    const tenantId = `trig-fire-${uuid().slice(0, 8)}`;
    const slug = `fire-${uuid().slice(0, 8)}`;

    const seq = testSequence("trig-fire-seq", [step("s1", "noop")], {
      tenantId,
    });
    await client.createSequence(seq);

    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: tenantId,
      namespace: "default",
      trigger_type: "event",
    });

    const res = await client.fireTrigger(slug, { hello: "world" });
    assert.ok(res.instance_id, "fire should return instance_id");

    const completed = await client.waitForState(res.instance_id as string, "completed", {
      timeoutMs: 10_000,
    });
    assert.equal(completed.state, "completed");
  });

  it("webhook path fires a trigger by slug with a secret", async () => {
    const tenantId = `trig-webhook-${uuid().slice(0, 8)}`;
    const slug = `webhook-${uuid().slice(0, 8)}`;
    const secret = "shhh-webhook-secret";

    const seq = testSequence("trig-webhook-seq", [step("s1", "noop")], {
      tenantId,
    });
    await client.createSequence(seq);

    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: tenantId,
      namespace: "default",
      trigger_type: "webhook",
      secret,
    });

    const res = await client.fireWebhook(slug, { payload: 1 }, {
      "x-trigger-secret": secret,
    });
    assert.ok(res.instance_id, "webhook should return instance_id");

    const completed = await client.waitForState(res.instance_id as string, "completed", {
      timeoutMs: 10_000,
    });
    assert.equal(completed.state, "completed");
  });

  it("webhook rejects when secret header is missing or wrong", async () => {
    const tenantId = `trig-webhook-bad-${uuid().slice(0, 8)}`;
    const slug = `webhook-bad-${uuid().slice(0, 8)}`;

    const seq = testSequence("trig-webhook-bad-seq", [step("s1", "noop")], {
      tenantId,
    });
    await client.createSequence(seq);

    await client.createTrigger({
      slug,
      sequence_name: seq.name,
      tenant_id: tenantId,
      namespace: "default",
      trigger_type: "webhook",
      secret: "correct-secret",
    });

    await assert.rejects(
      () => client.fireWebhook(slug, {}, { "x-trigger-secret": "wrong" }),
      (err: unknown) => {
        assert.equal((err as ApiError).status, 401);
        return true;
      }
    );
  });
});
