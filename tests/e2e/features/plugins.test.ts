import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Plugins CRUD", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should create a wasm plugin", async () => {
    const name = `plugin-${uuid().slice(0, 8)}`;
    const res = await client.createPlugin({
      name,
      plugin_type: "wasm",
      source: "/path/to/module.wasm",
      tenant_id: "test",
      config: { memory_pages: 16 },
    });
    assert.equal((res as any).name, name);
    assert.equal((res as any).plugin_type, "wasm");
    assert.equal((res as any).enabled, true);
  });

  it("should create a grpc plugin", async () => {
    const name = `grpc-${uuid().slice(0, 8)}`;
    const res = await client.createPlugin({
      name,
      plugin_type: "grpc",
      source: "localhost:9090/MyService.Execute",
      tenant_id: "test",
    });
    assert.equal((res as any).name, name);
    assert.equal((res as any).plugin_type, "grpc");
  });

  it("should list plugins by tenant", async () => {
    const tenantId = `t-${uuid().slice(0, 8)}`;
    const name = `plugin-${uuid().slice(0, 8)}`;
    await client.createPlugin({
      name,
      plugin_type: "wasm",
      source: "/module.wasm",
      tenant_id: tenantId,
    });

    const list = await client.listPlugins({ tenant_id: tenantId });
    assert.ok(list.length >= 1);
    assert.ok(list.some((p: any) => p.name === name));
  });

  it("should get a plugin by name", async () => {
    const name = `plugin-${uuid().slice(0, 8)}`;
    await client.createPlugin({
      name,
      plugin_type: "wasm",
      source: "/module.wasm",
      tenant_id: "test",
      description: "A test plugin",
    });

    const plugin = await client.getPlugin(name);
    assert.equal((plugin as any).name, name);
    assert.equal((plugin as any).description, "A test plugin");
  });

  it("should update plugin source", async () => {
    const name = `plugin-${uuid().slice(0, 8)}`;
    await client.createPlugin({
      name,
      plugin_type: "wasm",
      source: "/old.wasm",
      tenant_id: "test",
    });

    await client.updatePlugin(name, { source: "/new.wasm" });
    const updated = await client.getPlugin(name);
    assert.equal((updated as any).source, "/new.wasm");
  });

  it("should disable a plugin", async () => {
    const name = `plugin-${uuid().slice(0, 8)}`;
    await client.createPlugin({
      name,
      plugin_type: "grpc",
      source: "host:50051/Svc.Method",
      tenant_id: "test",
    });

    await client.updatePlugin(name, { enabled: false });
    const updated = await client.getPlugin(name);
    assert.equal((updated as any).enabled, false);
  });

  it("should delete a plugin", async () => {
    const name = `plugin-${uuid().slice(0, 8)}`;
    await client.createPlugin({
      name,
      plugin_type: "wasm",
      source: "/delete-me.wasm",
      tenant_id: "test",
    });

    await client.deletePlugin(name);

    await assert.rejects(
      () => client.getPlugin(name),
      (err: any) => err.status === 404,
    );
  });

  it("should reject empty name", async () => {
    await assert.rejects(
      () =>
        client.createPlugin({
          name: "",
          plugin_type: "wasm",
          source: "/module.wasm",
          tenant_id: "test",
        }),
      (err: any) => err.status === 400,
    );
  });

  it("should reject empty source", async () => {
    await assert.rejects(
      () =>
        client.createPlugin({
          name: `plugin-${uuid().slice(0, 8)}`,
          plugin_type: "wasm",
          source: "",
          tenant_id: "test",
        }),
      (err: any) => err.status === 400,
    );
  });

  it("should reject name exceeding 255 chars", async () => {
    await assert.rejects(
      () =>
        client.createPlugin({
          name: "x".repeat(256),
          plugin_type: "wasm",
          source: "/module.wasm",
          tenant_id: "test",
        }),
      (err: any) => err.status === 400,
    );
  });

  it("should reject source exceeding 2048 chars", async () => {
    await assert.rejects(
      () =>
        client.createPlugin({
          name: `plugin-${uuid().slice(0, 8)}`,
          plugin_type: "wasm",
          source: "x".repeat(2049),
          tenant_id: "test",
        }),
      (err: any) => err.status === 400,
    );
  });

  it("should return 404 for non-existent plugin", async () => {
    await assert.rejects(
      () => client.getPlugin("non-existent-plugin"),
      (err: any) => err.status === 404,
    );
  });
});
