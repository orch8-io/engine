/**
 * Plugin Type Validation — verifies rejected plugin creation requests.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Plugin Type Validation", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("rejects invalid plugin_type", async () => {
    try {
      await client.createPlugin({
        name: `plug-bad-${uuid().slice(0, 8)}`,
        tenant_id: "test",
        namespace: "default",
        plugin_type: "invalid",
        source: "test",
      });
      assert.fail("should throw");
    } catch (err: any) {
      assert.ok(err.status >= 400 && err.status < 500);
    }
  });

  it("rejects missing source", async () => {
    try {
      await client.createPlugin({
        name: `plug-no-src-${uuid().slice(0, 8)}`,
        tenant_id: "test",
        namespace: "default",
        plugin_type: "wasm",
      });
      assert.fail("should throw");
    } catch (err: any) {
      assert.ok(err.status >= 400 && err.status < 500);
    }
  });

  it("rejects missing name", async () => {
    try {
      await client.createPlugin({
        tenant_id: "test",
        namespace: "default",
        plugin_type: "wasm",
        source: "test",
      });
      assert.fail("should throw");
    } catch (err: any) {
      assert.ok(err.status >= 400 && err.status < 500);
    }
  });

  it("accepts wasm plugin_type", async () => {
    const name = `plug-wasm-${uuid().slice(0, 8)}`;
    const created = await client.createPlugin({
      name,
      tenant_id: "test",
      namespace: "default",
      plugin_type: "wasm",
      source: "test",
    });
    assert.equal(created.name, name);
    assert.equal((created as any).plugin_type, "wasm");
  });

  it("accepts grpc plugin_type", async () => {
    const name = `plug-grpc-${uuid().slice(0, 8)}`;
    const created = await client.createPlugin({
      name,
      tenant_id: "test",
      namespace: "default",
      plugin_type: "grpc",
      source: "test",
    });
    assert.equal(created.name, name);
    assert.equal((created as any).plugin_type, "grpc");
  });
});
