import { afterEach, describe, it } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { Server, RequestListener } from "node:http";
import { once } from "node:events";
import { ApiError, Orch8Client } from "../client.ts";
import type { ClusterNode } from "../types.ts";

const active: ClusterNode = {
  id: "00000000-0000-7000-8000-000000000001",
  name: "test-node", status: "active", drain: false,
  registered_at: "2026-01-01T00:00:00Z",
  last_heartbeat_at: "2026-01-01T00:00:00Z",
};

describe("Cluster startup polling", () => {
  let server: Server | undefined;
  afterEach(async () => {
    const current = server;
    server = undefined;
    if (!current) return;
    const closed = new Promise<void>((resolve, reject) => {
      current.close(error => error ? reject(error) : resolve());
    });
    current.closeAllConnections();
    await closed;
  });

  async function clientFor(listener: RequestListener): Promise<Orch8Client> {
    server = createServer(listener).listen(0, "127.0.0.1");
    await once(server, "listening");
    const address = server.address();
    assert.ok(address && typeof address !== "string");
    return new Orch8Client(`http://127.0.0.1:${address.port}`, { "x-api-key": "test-root" });
  }

  it("waits past empty, stopped and draining entries until registration is active", async () => {
    let requests = 0;
    const responses = [[], [{ ...active, status: "stopped" }], [{ ...active, drain: true }], [active]];
    const client = await clientFor((req, res) => {
      assert.equal(req.url, "/cluster/nodes");
      assert.equal(req.headers["x-api-key"], "test-root");
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify(responses[requests++]));
    });
    assert.deepEqual(await client.waitForActiveClusterNode({ intervalMs: 1 }), active);
    assert.equal(requests, 4);
  });

  it("propagates authentication errors immediately", async () => {
    let requests = 0;
    const client = await clientFor((_req, res) => {
      requests++;
      res.writeHead(401).end("invalid key");
    });
    await assert.rejects(client.waitForActiveClusterNode(), error =>
      error instanceof ApiError && error.status === 401);
    assert.equal(requests, 1);
  });

  it("times out when no node registers", async () => {
    const client = await clientFor((_req, res) => {
      res.setHeader("content-type", "application/json");
      res.end("[]");
    });
    await assert.rejects(client.waitForActiveClusterNode({ timeoutMs: 50, intervalMs: 1 }),
      /Timeout waiting for an active cluster node/);
  });

  it("bounds a stalled response body with the same startup deadline", async () => {
    const client = await clientFor((_req, res) => {
      res.writeHead(200, { "content-type": "application/json" });
      res.write("["); // Keep the body open until the client's deadline aborts it.
    });
    await assert.rejects(client.waitForActiveClusterNode({ timeoutMs: 50 }),
      /Timeout waiting for an active cluster node/);
  });
});
