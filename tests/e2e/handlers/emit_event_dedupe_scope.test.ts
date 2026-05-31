/**
 * emit_event `dedupe_scope` — end-to-end through the real storage transaction.
 *
 * Engine reference: `orch8-engine/src/handlers/emit_event.rs` (R7).
 *   - scope `"parent"` (default): dedupe identity is (parent_instance_id, key),
 *     so two DIFFERENT producers with the same key each spawn their own child.
 *   - scope `"tenant"`: dedupe identity is (tenant_id, key), so two different
 *     producers in the same tenant with the same key collide — the second is
 *     deduped to the first child (tenant-wide at-most-once fan-out).
 *
 * The unit tests cover this against in-memory SQLite; this suite proves the
 * same semantics survive the real `create_instance_with_dedupe` transaction on
 * the server's configured backend.
 *
 * Runner note: this suite creates triggers (globally scoped), so it is
 * registered in `SELF_MANAGED_SUITES` and runs on a fresh server.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

/** Run a single-step emit_event producer to completion and return its emit output. */
async function runEmitter(
  tenantId: string,
  namespace: string,
  slug: string,
  dedupeKey: string,
  scope: "parent" | "tenant",
): Promise<Record<string, unknown>> {
  const producer = testSequence(
    "dedupe-scope-producer",
    [
      step("emit", "emit_event", {
        trigger_slug: slug,
        data: {},
        dedupe_key: dedupeKey,
        dedupe_scope: scope,
      }),
    ],
    { tenantId, namespace },
  );
  await client.createSequence(producer);
  const { id } = await client.createInstance({
    sequence_id: producer.id,
    tenant_id: tenantId,
    namespace,
  });
  await client.waitForState(id, "completed", { timeoutMs: 20_000 });
  const emit = (await client.getOutputs(id)).find((o) => o.block_id === "emit");
  assert.ok(emit, "emit output missing");
  return emit!.output as Record<string, unknown>;
}

describe("emit_event dedupe_scope", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("tenant scope dedupes across two different producers (same tenant + key)", async () => {
    const tenantId = `ded-tenant-${uuid().slice(0, 8)}`;
    const namespace = "default";

    const consumer = testSequence("ds-consumer", [step("c1", "noop")], {
      tenantId,
      namespace,
    });
    await client.createSequence(consumer);
    const slug = `on-ds-tenant-${uuid().slice(0, 8)}`;
    await client.createTrigger({
      slug,
      sequence_name: consumer.name,
      tenant_id: tenantId,
      namespace,
      trigger_type: "event",
    });

    const key = `welcome-${uuid().slice(0, 8)}`;
    // Two distinct producer instances — distinct parent_instance_id — so
    // parent scope would NOT dedupe. Tenant scope must.
    const first = await runEmitter(tenantId, namespace, slug, key, "tenant");
    const second = await runEmitter(tenantId, namespace, slug, key, "tenant");

    assert.equal(first.deduped, false, "first tenant-scope emit creates the child");
    assert.equal(second.deduped, true, "second tenant-scope emit must be deduped");
    assert.equal(
      first.instance_id,
      second.instance_id,
      "tenant-scope dedupe must return the same child id across producers",
    );

    // Exactly one child exists for this tenant (the two producers are extra).
    await new Promise((r) => setTimeout(r, 200));
    const list = await client.listInstances({ tenant_id: tenantId });
    const childId = first.instance_id as string;
    const children = list.filter((i) => i.id === childId);
    assert.equal(children.length, 1, "exactly one child instance should exist");
  });

  it("parent scope does NOT dedupe across two different producers (same key)", async () => {
    const tenantId = `ded-parent-${uuid().slice(0, 8)}`;
    const namespace = "default";

    const consumer = testSequence("ds-consumer-p", [step("c1", "noop")], {
      tenantId,
      namespace,
    });
    await client.createSequence(consumer);
    const slug = `on-ds-parent-${uuid().slice(0, 8)}`;
    await client.createTrigger({
      slug,
      sequence_name: consumer.name,
      tenant_id: tenantId,
      namespace,
      trigger_type: "event",
    });

    const key = `shared-${uuid().slice(0, 8)}`;
    // Default scope is "parent"; two distinct producers → two distinct children.
    const first = await runEmitter(tenantId, namespace, slug, key, "parent");
    const second = await runEmitter(tenantId, namespace, slug, key, "parent");

    assert.equal(first.deduped, false);
    assert.equal(second.deduped, false, "parent scope must not dedupe across producers");
    assert.notEqual(
      first.instance_id,
      second.instance_id,
      "different parents with the same key must spawn distinct children",
    );
  });
});
