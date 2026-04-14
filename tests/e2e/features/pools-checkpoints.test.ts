import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid, ApiError } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Resource Pools + Checkpoints", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // --- Pool CRUD ---

  it("pool CRUD: create, list, get, update resource, delete", async () => {
    const tenantId = `pool-${uuid().slice(0, 8)}`;

    // Create pool.
    const pool = await client.createPool({
      tenant_id: tenantId,
      name: "smtp-accounts",
      strategy: "round_robin",
    });
    assert.ok(pool.id, "pool should have id");
    assert.equal(pool.tenant_id, tenantId);
    assert.equal(pool.name, "smtp-accounts");

    // List should include the pool.
    const listed = await client.listPools({ tenant_id: tenantId });
    assert.ok(
      listed.some((p) => p.id === pool.id),
      "list should include newly created pool"
    );

    // Get by id.
    const got = await client.getPool(pool.id as string);
    assert.equal(got.id, pool.id);
    assert.equal(got.name, "smtp-accounts");

    // Add a resource.
    const resource = await client.addPoolResource(pool.id as string, {
      resource_key: "smtp-a",
      name: "SMTP A",
      weight: 2,
      daily_cap: 100,
    });
    assert.ok(resource.id, "resource should have id");
    assert.equal(resource.weight, 2);
    assert.equal(resource.daily_cap, 100);

    // List resources.
    const resources = await client.listPoolResources(pool.id as string);
    assert.ok(resources.some((r) => r.id === resource.id));

    // Update resource (disable + raise cap).
    const updated = await client.updatePoolResource(pool.id as string, resource.id as string, {
      enabled: false,
      daily_cap: 500,
    });
    assert.equal(updated.enabled, false);
    assert.equal(updated.daily_cap, 500);

    // Delete resource.
    await client.deletePoolResource(pool.id as string, resource.id as string);
    const after = await client.listPoolResources(pool.id as string);
    assert.ok(!after.some((r) => r.id === resource.id));

    // Delete the pool.
    await client.deletePool(pool.id as string);
    try {
      await client.getPool(pool.id as string);
      assert.fail("expected 404 after delete");
    } catch (err) {
      assert.equal((err as ApiError).status, 404);
    }
  });

  it("pool list is tenant-scoped", async () => {
    const tenantA = `pool-tenant-a-${uuid().slice(0, 8)}`;
    const tenantB = `pool-tenant-b-${uuid().slice(0, 8)}`;

    const poolA = await client.createPool({
      tenant_id: tenantA,
      name: "pool-a",
    });
    const poolB = await client.createPool({
      tenant_id: tenantB,
      name: "pool-b",
    });

    const listA = await client.listPools({ tenant_id: tenantA });
    assert.ok(listA.some((p) => p.id === poolA.id));
    assert.ok(!listA.some((p) => p.id === poolB.id));

    // Cleanup.
    await client.deletePool(poolA.id as string);
    await client.deletePool(poolB.id as string);
  });

  // --- Checkpoints ---

  it("save + list + prune checkpoints on a completed instance", async () => {
    const seq = testSequence("cp-basic", [step("s1", "noop")]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed");

    // Save 3 checkpoints.
    for (let i = 0; i < 3; i += 1) {
      await client.saveCheckpoint(id, { iter: i, note: `checkpoint ${i}` });
    }

    const cps = await client.listCheckpoints(id);
    assert.ok(cps.length >= 3, `expected >=3 checkpoints, got ${cps.length}`);

    // Prune to keep only the latest 1.
    const pruneRes = await client.pruneCheckpoints(id, 1);
    assert.ok((pruneRes.count as number) >= 2, `expected >=2 pruned, got ${pruneRes.count}`);

    const remaining = await client.listCheckpoints(id);
    assert.equal(
      remaining.length,
      1,
      "exactly 1 checkpoint should remain after prune(keep=1)"
    );
  });

  it("prune on clean instance (no checkpoints) returns 200 with count=0", async () => {
    const seq = testSequence("cp-clean", [step("s1", "noop")]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed");

    const res = await client.pruneCheckpoints(id, 5);
    assert.equal(res.count, 0, "no checkpoints to prune");
  });
});
