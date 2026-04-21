/**
 * Pool Edge Cases — delete pool with resources, update non-existent resource,
 * list empty pool resources, add duplicate resource key.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid, ApiError } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Pool Edge Cases", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("delete pool with resources still in it succeeds (non-cascading)", async () => {
    const tenantId = `pool-del-${uuid().slice(0, 8)}`;
    const pool = await client.createPool({
      tenant_id: tenantId,
      name: `pool-del-${uuid().slice(0, 8)}`,
      max_size: 5,
    });
    const poolId = pool.id as string;

    await client.addPoolResource(poolId, {
      resource_key: "res-a",
      name: "Resource A",
      weight: 1,
    });

    // Delete the pool without deleting the resource first.
    await client.deletePool(poolId);

    // Verify pool is gone.
    await assert.rejects(
      () => client.getPool(poolId),
      (err: unknown) => {
        assert.equal((err as ApiError).status, 404);
        return true;
      },
    );
  });

  it("updatePoolResource on non-existent resource returns 404", async () => {
    const tenantId = `pool-upd-${uuid().slice(0, 8)}`;
    const pool = await client.createPool({
      tenant_id: tenantId,
      name: `pool-upd-${uuid().slice(0, 8)}`,
      max_size: 5,
    });

    await assert.rejects(
      () =>
        client.updatePoolResource(pool.id as string, uuid(), { enabled: false }),
      (err: unknown) => {
        assert.equal((err as ApiError).status, 404);
        return true;
      },
    );

    await client.deletePool(pool.id as string);
  });

  it("listPoolResources on empty pool returns empty array", async () => {
    const tenantId = `pool-empty-${uuid().slice(0, 8)}`;
    const pool = await client.createPool({
      tenant_id: tenantId,
      name: `pool-empty-${uuid().slice(0, 8)}`,
      max_size: 5,
    });

    const resources = await client.listPoolResources(pool.id as string);
    assert.ok(Array.isArray(resources));
    assert.equal(resources.length, 0);

    await client.deletePool(pool.id as string);
  });

  it("addPoolResource allows duplicate keys (documents current behaviour)", async () => {
    const tenantId = `pool-dup-${uuid().slice(0, 8)}`;
    const pool = await client.createPool({
      tenant_id: tenantId,
      name: `pool-dup-${uuid().slice(0, 8)}`,
      max_size: 5,
    });
    const poolId = pool.id as string;

    await client.addPoolResource(poolId, {
      resource_key: "dup-key",
      name: "First",
      weight: 1,
    });

    // The current implementation does NOT enforce uniqueness of resource_key
    // within a pool — it creates a second resource with the same key.
    const second = await client.addPoolResource(poolId, {
      resource_key: "dup-key",
      name: "Second",
      weight: 2,
    });
    assert.equal(second.resource_key, "dup-key");

    const resources = await client.listPoolResources(poolId);
    const dups = resources.filter((r) => r.resource_key === "dup-key");
    assert.equal(dups.length, 2, "duplicate keys are currently allowed");

    await client.deletePool(poolId);
  });
});
