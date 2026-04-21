/**
 * Plugin Edge Cases — verifies plugin CRUD and scoping.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Plugin Edge Cases", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("create plugin with config round-trips", async () => {
    const name = `plug-cfg-${uuid().slice(0, 8)}`;
    const created = await client.createPlugin({
      name,
      tenant_id: "test",
      namespace: "default",
      plugin_type: "wasm",
      source: "test",
      config: { endpoint: "https://example.com", timeout: 30 },
    });
    assert.equal(created.name, name);
    assert.equal((created as any).plugin_type, "wasm");
  });

  it("update plugin config changes values", async () => {
    const name = `plug-up-${uuid().slice(0, 8)}`;
    await client.createPlugin({
      name,
      tenant_id: "test",
      namespace: "default",
      plugin_type: "grpc",
      source: "test",
      config: { version: 1 },
    });

    const updated = await client.updatePlugin(name, { config: { version: 2 } });
    assert.deepEqual((updated.config as any)?.version, 2);
  });

  it("get plugin returns config", async () => {
    const name = `plug-get-${uuid().slice(0, 8)}`;
    await client.createPlugin({
      name,
      tenant_id: "test",
      namespace: "default",
      plugin_type: "wasm",
      source: "test",
      config: { key: "value" },
    });

    const fetched = await client.getPlugin(name);
    assert.equal(fetched.name, name);
    assert.deepEqual((fetched.config as any)?.key, "value");
  });

  it("list plugins is tenant-scoped", async () => {
    const nameA = `plug-ta-${uuid().slice(0, 8)}`;
    const nameB = `plug-tb-${uuid().slice(0, 8)}`;
    await client.createPlugin({ name: nameA, tenant_id: "tenant-a", namespace: "default", plugin_type: "wasm", source: "test" });
    await client.createPlugin({ name: nameB, tenant_id: "tenant-b", namespace: "default", plugin_type: "grpc", source: "test" });

    const listA = await client.listPlugins({ tenant_id: "tenant-a" });
    assert.ok(listA.some((p: any) => p.name === nameA));
    assert.ok(!listA.some((p: any) => p.name === nameB));
  });

  it("delete plugin removes it", async () => {
    const name = `plug-del-${uuid().slice(0, 8)}`;
    await client.createPlugin({ name, tenant_id: "test", namespace: "default", plugin_type: "wasm", source: "test" });

    await client.deletePlugin(name);

    try {
      await client.getPlugin(name);
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("create plugin without name returns 422", async () => {
    try {
      await client.createPlugin({ tenant_id: "test", namespace: "default", plugin_type: "wasm", source: "test" });
      assert.fail("should throw 422");
    } catch (err: any) {
      assert.equal(err.status, 422);
    }
  });
});
