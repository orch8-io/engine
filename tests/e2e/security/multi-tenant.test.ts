import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Multi-Tenant Isolation", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("list /instances?tenant_id=A excludes tenant B's instances", async () => {
    const tenantA = `mt-a-${uuid().slice(0, 8)}`;
    const tenantB = `mt-b-${uuid().slice(0, 8)}`;

    const seqA = testSequence("mt-seq-a", [step("s1", "noop")], {
      tenantId: tenantA,
    });
    const seqB = testSequence("mt-seq-b", [step("s1", "noop")], {
      tenantId: tenantB,
    });
    await client.createSequence(seqA);
    await client.createSequence(seqB);

    const { id: idA } = await client.createInstance({
      sequence_id: seqA.id,
      tenant_id: tenantA,
      namespace: "default",
    });
    const { id: idB } = await client.createInstance({
      sequence_id: seqB.id,
      tenant_id: tenantB,
      namespace: "default",
    });

    // Small settle delay so instances are visible.
    await new Promise((r) => setTimeout(r, 200));

    const listA = await client.listInstances({ tenant_id: tenantA });
    const listB = await client.listInstances({ tenant_id: tenantB });

    assert.ok(
      listA.some((i) => i.id === idA),
      "tenant A list should include A's instance"
    );
    assert.ok(
      !listA.some((i) => i.id === idB),
      "tenant A list must not include B's instance"
    );
    assert.ok(
      listB.some((i) => i.id === idB),
      "tenant B list should include B's instance"
    );
    assert.ok(
      !listB.some((i) => i.id === idA),
      "tenant B list must not include A's instance"
    );
    assert.ok(
      listA.every((i) => i.tenant_id === tenantA),
      "tenant A list should be homogeneous"
    );
    assert.ok(
      listB.every((i) => i.tenant_id === tenantB),
      "tenant B list should be homogeneous"
    );
  });

  it("list /cron?tenant_id=A excludes tenant B's crons", async () => {
    const tenantA = `mt-cron-a-${uuid().slice(0, 8)}`;
    const tenantB = `mt-cron-b-${uuid().slice(0, 8)}`;

    const seqA = testSequence("mt-cron-seq-a", [step("s1", "noop")], {
      tenantId: tenantA,
    });
    const seqB = testSequence("mt-cron-seq-b", [step("s1", "noop")], {
      tenantId: tenantB,
    });
    await client.createSequence(seqA);
    await client.createSequence(seqB);

    const { id: cronIdA } = await client.createCron({
      tenant_id: tenantA,
      namespace: "default",
      sequence_id: seqA.id,
      cron_expr: "0 0 * * *",
      timezone: "UTC",
      enabled: false,
    });
    const { id: cronIdB } = await client.createCron({
      tenant_id: tenantB,
      namespace: "default",
      sequence_id: seqB.id,
      cron_expr: "0 0 * * *",
      timezone: "UTC",
      enabled: false,
    });

    const listA = await client.listCron({ tenant_id: tenantA });
    const listB = await client.listCron({ tenant_id: tenantB });

    assert.ok(listA.some((c) => c.id === cronIdA));
    assert.ok(!listA.some((c) => c.id === cronIdB));
    assert.ok(listB.some((c) => c.id === cronIdB));
    assert.ok(!listB.some((c) => c.id === cronIdA));

    // Cleanup.
    await client.deleteCron(cronIdA as string);
    await client.deleteCron(cronIdB as string);
  });

  it("cross-tenant read by instance ID without X-Tenant-Id header succeeds (documented behavior)", async () => {
    // In --insecure test mode, the tenant middleware only enforces isolation
    // when X-Tenant-Id header is set. ID-based lookups without the header
    // return the instance regardless of tenant — this is expected behavior
    // (see orch8-api/src/auth.rs::enforce_tenant_access and instances.rs::get_instance).
    const tenantA = `mt-id-a-${uuid().slice(0, 8)}`;

    const seqA = testSequence("mt-id-seq-a", [step("s1", "noop")], {
      tenantId: tenantA,
    });
    await client.createSequence(seqA);

    const { id: idA } = await client.createInstance({
      sequence_id: seqA.id,
      tenant_id: tenantA,
      namespace: "default",
    });

    // Fetching by ID without header succeeds and returns the instance.
    const got = await client.getInstance(idA);
    assert.equal(got.id, idA);
    assert.equal(got.tenant_id, tenantA);
  });

  it("instance created under tenant A is not visible in tenant B's filtered list", async () => {
    const tenantA = `mt-hidden-a-${uuid().slice(0, 8)}`;
    const tenantB = `mt-hidden-b-${uuid().slice(0, 8)}`;

    const seqA = testSequence("mt-hidden-seq", [step("s1", "noop")], {
      tenantId: tenantA,
    });
    await client.createSequence(seqA);

    const { id: idA } = await client.createInstance({
      sequence_id: seqA.id,
      tenant_id: tenantA,
      namespace: "default",
    });

    await new Promise((r) => setTimeout(r, 200));

    const listB = await client.listInstances({ tenant_id: tenantB });
    assert.ok(
      !listB.some((i) => i.id === idA),
      "tenant B filtered list must not leak tenant A's instance"
    );
  });
});
