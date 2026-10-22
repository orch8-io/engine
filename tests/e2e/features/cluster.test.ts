import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { ClusterNode } from "../types.ts";

const client = new Orch8Client();

describe("Cluster Nodes", () => {
  let server: ServerHandle | undefined;
  let runningNode: ClusterNode;

  before(async () => {
    server = await startServer();
    runningNode = await client.waitForActiveClusterNode();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should list cluster nodes (includes the running server)", async () => {
    const nodes = await client.listClusterNodes();
    // Startup registration can finish after the process liveness probe.
    assert.ok(Array.isArray(nodes));
    assert.ok(nodes.length >= 1, "at least one node should be registered");
    const node = nodes.find(node => node.id === runningNode.id);
    assert.ok(node, "the registered server must remain in the listing");
    assert.ok(node.id, "node should have an id");
    assert.ok(node.status, "node should have a status");
  });

  it("should accept drain for non-existent node (idempotent)", async () => {
    const fakeId = "00000000-0000-0000-0000-000000000000";
    // Drain is idempotent — returns success even for unknown nodes.
    await client.drainClusterNode(fakeId);
  });

  // Must run last: draining the real node triggers server shutdown via
  // the heartbeat drain-check loop, making subsequent requests fail.
  it("should drain a node without error", async () => {
    // Drain the live server, not a retained stopped row from an earlier run.
    // The client accepts an empty 200 and rejects non-success responses.
    await client.drainClusterNode(runningNode.id);
  });
});
