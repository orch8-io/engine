/**
 * GET /cluster/nodes — verifies the cluster nodes listing endpoint
 * returns valid data for a single-node setup.
 */
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

  it("lists at least one cluster node", async () => {
    const nodes = await client.listClusterNodes();
    assert.ok(Array.isArray(nodes), "response should be an array");
    assert.ok(nodes.length >= 1, "should have at least one node in single-node setup");
  });

  it("cluster node has expected fields", async () => {
    const nodes = await client.listClusterNodes();
    if (nodes.length > 0) {
      const node = nodes[0] as Record<string, unknown>;
      // At minimum, a node should have an id or node_id.
      assert.ok(
        node.id || node.node_id,
        "cluster node should have an identifier",
      );
    }
  });
});
