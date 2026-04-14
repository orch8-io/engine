/**
 * Health and metrics endpoints — verifies /health/live, /health/ready,
 * and /metrics return proper responses.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Health Endpoints", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("GET /health/live returns 200", async () => {
    const res = await client.healthLive();
    assert.equal(res.status, 200);
  });

  it("GET /health/ready returns 200", async () => {
    const res = await client.healthReady();
    assert.equal(res.status, 200);
  });

  it("GET /metrics returns prometheus-formatted text", async () => {
    const text = await client.metrics();
    assert.ok(typeof text === "string", "metrics should be a string");
    assert.ok(text.length > 0, "metrics should not be empty");
    // Prometheus format includes comments starting with # and metric lines.
    assert.ok(
      text.includes("#") || text.includes("_"),
      "metrics should contain prometheus-style content",
    );
  });
});
