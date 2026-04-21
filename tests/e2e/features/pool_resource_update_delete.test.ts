/**
 * Pool Resource Update and Delete — verifies pool resource lifecycle
 * beyond basic creation.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Pool Resource Update and Delete", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("update pool resource changes values", async () => {
    const tenantId = `pool-up-${uuid().slice(0, 8)}`;
    const pool = await client.createPool({
      name: `up-${uuid().slice(0, 8)}`,
      tenant_id: tenantId,
      namespace: "default",
      capacity: 10,
    });

    const resource = await client.addPoolResource(pool.id, {
      resource_key: "r1",
      name: "Resource One",
      weight: 1,
      value: "initial",
    });

    await client.updatePoolResource(pool.id, resource.id, {
      name: "Updated Name",
      weight: 2,
    });

    const resources = await client.listPoolResources(pool.id);
    const updated = resources.find((r: any) => r.id === resource.id);
    assert.ok(updated, "resource should still exist");
    assert.equal(updated.name, "Updated Name");
    assert.equal(updated.weight, 2);
  });

  it("delete pool resource removes it from list", async () => {
    const tenantId = `pool-del-${uuid().slice(0, 8)}`;
    const pool = await client.createPool({
      name: `del-${uuid().slice(0, 8)}`,
      tenant_id: tenantId,
      namespace: "default",
      capacity: 5,
    });

    const resource = await client.addPoolResource(pool.id, {
      resource_key: "r1",
      name: "Res",
      weight: 1,
      value: "v1",
    });

    await client.deletePoolResource(pool.id, resource.id);

    const resources = await client.listPoolResources(pool.id);
    assert.ok(!resources.some((r: any) => r.id === resource.id), "deleted resource should be gone");
  });

  it("list resources after delete excludes removed", async () => {
    const tenantId = `pool-lst-${uuid().slice(0, 8)}`;
    const pool = await client.createPool({
      name: `lst-${uuid().slice(0, 8)}`,
      tenant_id: tenantId,
      namespace: "default",
      capacity: 5,
    });

    const r1 = await client.addPoolResource(pool.id, { resource_key: "k1", name: "K1", weight: 1, value: "v1" });
    const r2 = await client.addPoolResource(pool.id, { resource_key: "k2", name: "K2", weight: 1, value: "v2" });

    await client.deletePoolResource(pool.id, r1.id);

    const resources = await client.listPoolResources(pool.id);
    assert.equal(resources.length, 1, "should have exactly one resource left");
    assert.equal(resources[0]!.id, r2.id, "remaining resource should be r2");
  });

  it("duplicate resource keys allowed in same pool", async () => {
    const tenantId = `pool-dup-${uuid().slice(0, 8)}`;
    const pool = await client.createPool({
      name: `dup-${uuid().slice(0, 8)}`,
      tenant_id: tenantId,
      namespace: "default",
      capacity: 5,
    });

    const r1 = await client.addPoolResource(pool.id, { resource_key: "same", name: "A", weight: 1, value: "v1" });
    const r2 = await client.addPoolResource(pool.id, { resource_key: "same", name: "B", weight: 1, value: "v2" });

    const resources = await client.listPoolResources(pool.id);
    assert.equal(resources.length, 2, "both resources should exist");
    assert.ok(resources.some((r: any) => r.id === r1.id), "r1 should be present");
    assert.ok(resources.some((r: any) => r.id === r2.id), "r2 should be present");
  });

  it("get pool returns resource metadata", async () => {
    const tenantId = `pool-get-${uuid().slice(0, 8)}`;
    const pool = await client.createPool({
      name: `get-${uuid().slice(0, 8)}`,
      tenant_id: tenantId,
      namespace: "default",
      capacity: 5,
    });

    await client.addPoolResource(pool.id, { resource_key: "k1", name: "K1", weight: 1, value: "v1" });

    const fetched = await client.getPool(pool.id);
    assert.equal(fetched.id, pool.id, "pool id should match");
    assert.equal(fetched.name, pool.name, "pool name should match");
  });

  it("delete non-existent pool resource is idempotent", async () => {
    const tenantId = `pool-del-404-${uuid().slice(0, 8)}`;
    const pool = await client.createPool({
      name: `del404-${uuid().slice(0, 8)}`,
      tenant_id: tenantId,
      namespace: "default",
      capacity: 5,
    });

    // Should not throw.
    await client.deletePoolResource(pool.id, "019db000-0000-7000-0000-000000000000");
  });
});
