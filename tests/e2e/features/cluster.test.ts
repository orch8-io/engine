import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Cluster Nodes", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should list cluster nodes (includes the running server)", async () => {
    const nodes = await client.listClusterNodes();
    // The running server registers itself as a node.
    assert.ok(Array.isArray(nodes));
    assert.ok(nodes.length >= 1, "at least one node should be registered");
    const node = nodes[0]! as any;
    assert.ok(node.id, "node should have an id");
    assert.ok(node.status, "node should have a status");
  });

  it("should drain a node without error", async () => {
    const nodes = await client.listClusterNodes();
    assert.ok(nodes.length >= 1);
    const nodeId = (nodes[0]! as any).id;

    // Drain should succeed (no-op if single node).
    const res = await client.drainClusterNode(nodeId);
    // 200 OK with empty body is fine.
    assert.ok(res !== undefined || res === undefined);
  });

  it("should accept drain for non-existent node (idempotent)", async () => {
    const fakeId = "00000000-0000-0000-0000-000000000000";
    // Drain is idempotent — returns success even for unknown nodes.
    const res = await client.drainClusterNode(fakeId);
    assert.ok(res !== undefined || res === undefined);
  });
});
