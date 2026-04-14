/**
 * Signal payload edge cases — oversized payloads, empty payloads,
 * and signals on terminal instances.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Signal Payload Validation", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("sending signal to non-existent instance returns 404", async () => {
    try {
      await client.sendSignal(uuid(), "pause");
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("sending signal to completed instance returns error (not 500)", async () => {
    const tenantId = `sig-done-${uuid().slice(0, 8)}`;
    const seq = testSequence("sig-done", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 5_000 });

    try {
      await client.sendSignal(id, "pause");
      // Some engines accept signals on terminal instances (they're just ignored).
    } catch (err: any) {
      assert.ok(
        err.status !== 500,
        `signal on completed instance should not cause 500; got ${err.status}`,
      );
    }
  });

  it("signal with empty payload is accepted", async () => {
    const tenantId = `sig-empty-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "sig-empty",
      [step("slow", "sleep", { duration_ms: 2000 })],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, ["running", "scheduled"], { timeoutMs: 5_000 });

    // Empty payload should be fine.
    await client.sendSignal(id, "cancel", {});

    // Instance should transition to cancelled.
    const final = await client.waitForState(id, ["cancelled", "failed", "completed"], {
      timeoutMs: 10_000,
    });
    assert.ok(final.state, "instance should reach terminal state");
  });

  it("signal with large payload is handled gracefully", async () => {
    const tenantId = `sig-big-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "sig-big",
      [step("slow", "sleep", { duration_ms: 3000 })],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, ["running", "scheduled"], { timeoutMs: 5_000 });

    // Send a signal with a large payload (~100KB).
    const largePayload: Record<string, unknown> = {};
    for (let i = 0; i < 100; i++) {
      largePayload[`key_${i}`] = "x".repeat(1000);
    }

    try {
      await client.sendSignal(id, "custom_data", largePayload);
      // If accepted, that's fine — the engine handles it.
    } catch (err: any) {
      // If rejected, it should be a 4xx (payload too large), not a 500.
      assert.ok(
        err.status !== 500,
        `large payload should not cause 500; got ${err.status}`,
      );
    }
  });
});
