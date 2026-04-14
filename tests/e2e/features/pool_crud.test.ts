/**
 * Pool CRUD edge cases — create, list, add resources, delete.
 * Complements pools-checkpoints.test.ts with focused CRUD validation.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Pool CRUD", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("create, get, and delete a pool", async () => {
    const tenantId = `pool-${uuid().slice(0, 8)}`;
    const pool = await client.createPool({
      tenant_id: tenantId,
      name: `pool-${uuid().slice(0, 8)}`,
      max_size: 10,
    });
    const poolId = (pool as any).id;
    assert.ok(poolId, "pool should have an id");

    const fetched = await client.getPool(poolId);
    assert.equal((fetched as any).id, poolId);

    await client.deletePool(poolId);

    try {
      await client.getPool(poolId);
      assert.fail("should 404 after delete");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("add and list pool resources", async () => {
    const tenantId = `pool-res-${uuid().slice(0, 8)}`;
    const pool = await client.createPool({
      tenant_id: tenantId,
      name: `pool-res-${uuid().slice(0, 8)}`,
      max_size: 5,
    });
    const poolId = (pool as any).id;

    await client.addPoolResource(poolId, {
      resource_key: `res-${uuid().slice(0, 8)}`,
      name: "Worker A",
      weight: 1,
      daily_cap: 100,
    });

    const resources = await client.listPoolResources(poolId);
    assert.ok(Array.isArray(resources), "resources should be an array");
    assert.ok(resources.length >= 1, "should have at least one resource");
  });

  it("delete non-existent pool returns 404", async () => {
    try {
      await client.deletePool(uuid());
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("list pools scoped by tenant", async () => {
    const tenantId = `pool-list-${uuid().slice(0, 8)}`;
    await client.createPool({
      tenant_id: tenantId,
      name: `pool-scoped-${uuid().slice(0, 8)}`,
      max_size: 3,
    });

    const pools = await client.listPools({ tenant_id: tenantId });
    assert.ok(Array.isArray(pools), "pools listing should be an array");
    assert.ok(pools.length >= 1, "should find at least the created pool");
  });
});
