/**
 * GET /cluster/nodes — verifies the cluster nodes listing endpoint
 * returns valid data for a single-node setup.
 */
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

  it("lists at least one cluster node", async () => {
    const nodes = await client.listClusterNodes();
    assert.ok(Array.isArray(nodes), "response should be an array");
    assert.ok(nodes.length >= 1, "should have at least one node in single-node setup");
  });

  it("cluster node has expected fields", async () => {
    const nodes = await client.listClusterNodes();
    const node = nodes.find(node => node.id === runningNode.id);
    assert.ok(node, "the running server must have a registry entry");
    assert.ok(node.id, "cluster node should have an identifier");
    assert.ok(node.name, "cluster node should have a name");
    assert.equal(node.status, "active");
    assert.equal(node.drain, false);
    assert.ok(Number.isFinite(Date.parse(node.registered_at)));
    assert.ok(Number.isFinite(Date.parse(node.last_heartbeat_at)));
  });
});
