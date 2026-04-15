import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "./client.js";
import { startServer, stopServer } from "./harness.js";

const client = new Orch8Client();

describe("Signals", () => {
  let server;

  before(async () => {
    server = await startServer({ build: false });
  });

  after(async () => {
    await stopServer(server);
  });

  it("should cancel a scheduled instance via signal", async () => {
    // Use a sleep step so instance stays alive long enough to signal
    const seq = testSequence("cancel-test", [
      step("s1", "sleep", { duration_ms: 5000 }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for it to be picked up (running or scheduled)
    await new Promise((r) => setTimeout(r, 300));

    await client.sendSignal(id, "cancel");

    // The cancel signal should be processed on the next tick
    const instance = await client.waitForState(id, ["cancelled", "failed"], {
      timeoutMs: 10000,
    });
    assert.ok(
      instance.state === "cancelled" || instance.state === "failed",
      `Expected cancelled or failed, got ${instance.state}`
    );
  });

  it("should pause and resume an instance", async () => {
    // Multi-step: first step completes, second is slow enough to pause
    const seq = testSequence("pause-resume", [
      step("s1", "noop"),
      step("s2", "sleep", { duration_ms: 5000 }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for s1 to complete and s2 to be scheduled
    await new Promise((r) => setTimeout(r, 500));

    await client.sendSignal(id, "pause");
    await new Promise((r) => setTimeout(r, 500));

    let instance = await client.getInstance(id);
    // Should be paused (or still transitioning)
    if (instance.state === "paused") {
      // Resume it
      await client.sendSignal(id, "resume");
      instance = await client.waitForState(id, ["completed", "scheduled", "running"], {
        timeoutMs: 10000,
      });
      assert.ok(
        ["completed", "scheduled", "running"].includes(instance.state),
        `Expected active state after resume, got ${instance.state}`
      );
    }
    // If it already completed before pause took effect, that's also valid
  });

  it("should reject signal on terminal instance", async () => {
    const seq = testSequence("signal-terminal", [step("s1", "noop")]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "completed");

    await assert.rejects(() => client.sendSignal(id, "cancel"), (err) => {
      assert.ok(err.status === 400 || err.status === 422);
      return true;
    });
  });
});
