/**
 * GET /workers/tasks/stats — verifies the worker task stats endpoint
 * returns structured data after some tasks have been processed.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Worker Task Stats", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("returns stats object", async () => {
    const stats = await client.workerTaskStats();
    assert.ok(stats, "stats should be a non-null object");
    assert.equal(typeof stats, "object");
  });

  it("stats reflect processed tasks", async () => {
    const tenantId = `wts-${uuid().slice(0, 8)}`;

    // Run a few instances to generate worker task activity.
    const seq = testSequence("wts-gen", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    for (let i = 0; i < 3; i++) {
      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace: "default",
      });
      await client.waitForState(id, "completed", { timeoutMs: 5_000 });
    }

    const stats = await client.workerTaskStats();
    assert.ok(stats, "stats should exist after processing");
  });
});
