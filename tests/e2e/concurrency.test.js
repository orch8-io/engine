import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "./client.js";
import { startServer, stopServer } from "./harness.js";

const client = new Orch8Client();

describe("Concurrency Control", () => {
  let server;

  before(async () => {
    server = await startServer({ build: false });
  });

  after(async () => {
    await stopServer(server);
  });

  it("should limit concurrent executions by concurrency key", async () => {
    // Create a sequence with a slow step so instances overlap.
    const seq = testSequence("conc-limit", [
      step("s1", "sleep", { duration_ms: 2000 }),
    ]);
    await client.createSequence(seq);

    const concurrencyKey = `conc-test-${Date.now()}`;

    // Create 3 instances with max_concurrency=1.
    const ids = [];
    for (let i = 0; i < 3; i++) {
      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: "test",
        namespace: "default",
        concurrency_key: concurrencyKey,
        max_concurrency: 1,
      });
      ids.push(id);
    }

    // Wait a bit for the scheduler to process.
    await new Promise((r) => setTimeout(r, 500));

    // Check states: at most 1 should be running at a time.
    let runningCount = 0;
    for (const id of ids) {
      const inst = await client.getInstance(id);
      if (inst.state === "running") runningCount++;
    }

    assert.ok(
      runningCount <= 1,
      `Expected at most 1 running instance, got ${runningCount}`
    );

    // Wait for all to eventually complete.
    for (const id of ids) {
      await client.waitForState(id, ["completed"], { timeoutMs: 20000 });
    }
  });

  it("should not limit instances without concurrency key", async () => {
    const seq = testSequence("no-conc-limit", [
      step("s1", "sleep", { duration_ms: 1000 }),
    ]);
    await client.createSequence(seq);

    // Create 3 instances without concurrency key.
    const ids = [];
    for (let i = 0; i < 3; i++) {
      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: "test",
        namespace: "default",
      });
      ids.push(id);
    }

    // All should eventually complete.
    for (const id of ids) {
      await client.waitForState(id, ["completed"], { timeoutMs: 15000 });
    }
  });
});
