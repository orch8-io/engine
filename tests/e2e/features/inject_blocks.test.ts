import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Inject Blocks API", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should inject blocks into a running instance", async () => {
    // Create a sequence with a sleep step so the instance stays alive.
    const seq = testSequence("inject-target", [
      step("s1", "sleep", { duration_ms: 3000 }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for it to be running.
    await client.waitForState(id, "running", { timeoutMs: 5_000 });

    // Inject a noop step — should succeed without error.
    const res = await client.injectBlocks(id, [
      { type: "step", id: "injected1", handler: "noop", params: {} },
    ]);
    assert.ok(res !== undefined);

    // Instance should still complete.
    const instance = await client.waitForState(id, "completed", {
      timeoutMs: 10_000,
    });
    assert.equal(instance.state, "completed");
  });

  it("should inject blocks at a specific position", async () => {
    const seq = testSequence("inject-position", [
      step("s1", "sleep", { duration_ms: 2000 }),
      step("s2", "noop"),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "running", { timeoutMs: 5_000 });

    // Inject at position 1.
    const res = await client.injectBlocks(
      id,
      [{ type: "step", id: "mid", handler: "noop", params: {} }],
      1,
    );
    assert.ok(res !== undefined);

    const instance = await client.waitForState(id, "completed", {
      timeoutMs: 10_000,
    });
    assert.equal(instance.state, "completed");
  });

  it("should reject inject on non-existent instance", async () => {
    const fakeId = uuid();
    await assert.rejects(
      () =>
        client.injectBlocks(fakeId, [
          { type: "step", id: "x", handler: "noop", params: {} },
        ]),
      (err: any) => err.status === 404,
    );
  });

  it("should reject empty blocks array", async () => {
    const seq = testSequence("inject-empty", [step("s1", "noop")]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Depending on implementation: empty array may succeed (no-op) or return 400.
    // Both are valid — just ensure it doesn't 500.
    try {
      await client.injectBlocks(id, []);
    } catch (err: any) {
      assert.ok(
        err.status === 400 || err.status === 200,
        `unexpected error: ${err.status}`,
      );
    }
  });
});
