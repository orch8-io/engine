/**
 * Trigger Edge Cases — create with missing sequence, get wrong tenant,
 * fire non-existent trigger, list tenant-scoped.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid, ApiError } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Trigger Edge Cases", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("create trigger with non-existent sequence_name succeeds (no validation)", async () => {
    // The trigger API currently does not validate that the referenced
    // sequence_name exists at creation time.
    const slug = `bad-seq-${uuid().slice(0, 8)}`;
    const seqName = `nonexistent-${uuid().slice(0, 8)}`;
    const result = await client.createTrigger({
      tenant_id: "test",
      namespace: "default",
      slug,
      sequence_name: seqName,
    });
    assert.equal(result.slug, slug);
    assert.equal(result.sequence_name, seqName);
    await client.deleteTrigger(slug);
  });

  it("GET /triggers/{slug} is not tenant-scoped by slug alone", async () => {
    const tenantA = `trig-a-${uuid().slice(0, 8)}`;
    const seq = testSequence("trig-tenant", [step("s1", "noop")], {
      tenantId: tenantA,
    });
    await client.createSequence(seq);

    const slug = `trig-t-${uuid().slice(0, 8)}`;
    await client.createTrigger({
      tenant_id: tenantA,
      namespace: "default",
      slug,
      sequence_name: seq.name,
    });

    // The current implementation does NOT enforce tenant isolation on
    // GET /triggers/{slug} — it looks up by slug alone.
    const got = await client.getTrigger(slug);
    assert.equal(got.slug, slug);
    assert.equal(got.tenant_id, tenantA);

    await client.deleteTrigger(slug);
  });

  it("fireTrigger on non-existent slug returns 404", async () => {
    await assert.rejects(
      () => client.fireTrigger(`nonexistent-${uuid().slice(0, 8)}`, {}),
      (err: unknown) => {
        assert.equal((err as ApiError).status, 404);
        return true;
      },
    );
  });

  it("listTriggers is tenant-scoped", async () => {
    const tenantA = `trig-list-a-${uuid().slice(0, 8)}`;
    const tenantB = `trig-list-b-${uuid().slice(0, 8)}`;

    const seqA = testSequence("trig-la", [step("s1", "noop")], { tenantId: tenantA });
    const seqB = testSequence("trig-lb", [step("s1", "noop")], { tenantId: tenantB });
    await client.createSequence(seqA);
    await client.createSequence(seqB);

    const slugA = `trig-la-${uuid().slice(0, 8)}`;
    const slugB = `trig-lb-${uuid().slice(0, 8)}`;

    await client.createTrigger({
      tenant_id: tenantA,
      namespace: "default",
      slug: slugA,
      sequence_name: seqA.name,
    });
    await client.createTrigger({
      tenant_id: tenantB,
      namespace: "default",
      slug: slugB,
      sequence_name: seqB.name,
    });

    const listA = await client.listTriggers({ tenant_id: tenantA });
    assert.ok(listA.some((t) => t.slug === slugA));
    assert.ok(!listA.some((t) => t.slug === slugB));

    await client.deleteTrigger(slugA);
    await client.deleteTrigger(slugB);
  });
});
