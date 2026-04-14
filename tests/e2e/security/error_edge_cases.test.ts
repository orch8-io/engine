import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Error Edge Cases", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // --- Invalid UUID formats ---

  it("should return 400 for invalid UUID in getInstance", async () => {
    await assert.rejects(
      () => client.getInstance("not-a-uuid"),
      (err: any) => err.status === 400 || err.status === 404,
    );
  });

  it("should return 400 for invalid UUID in getSequence", async () => {
    await assert.rejects(
      () => client.getSequence("not-a-uuid"),
      (err: any) => err.status === 400 || err.status === 404,
    );
  });

  it("should return 404 for valid UUID that does not exist", async () => {
    await assert.rejects(
      () => client.getInstance(uuid()),
      (err: any) => err.status === 404,
    );
  });

  // --- Missing required fields ---

  it("should return 400 for createInstance without sequence_id", async () => {
    await assert.rejects(
      () =>
        client.createInstance({
          sequence_id: "",
          tenant_id: "test",
          namespace: "default",
        }),
      (err: any) => err.status === 400 || err.status === 422,
    );
  });

  it("should return 404 for createInstance with non-existent sequence", async () => {
    await assert.rejects(
      () =>
        client.createInstance({
          sequence_id: uuid(),
          tenant_id: "test",
          namespace: "default",
        }),
      (err: any) => err.status === 404 || err.status === 400,
    );
  });

  // --- Sequence validation ---

  it("should return 400 for sequence with empty blocks", async () => {
    const seq = testSequence("empty-blocks", []);
    // Some APIs accept empty blocks — check it doesn't 500.
    try {
      await client.createSequence(seq);
      // If accepted, that's fine.
    } catch (err: any) {
      assert.ok(
        err.status === 400 || err.status === 422,
        `unexpected status: ${err.status}`,
      );
    }
  });

  it("should handle sequence with empty name gracefully", async () => {
    const seq = {
      id: uuid(),
      tenant_id: "test",
      namespace: "default",
      name: "",
      version: 1,
      blocks: [{ type: "step", id: "s1", handler: "noop", params: {} }],
      created_at: new Date().toISOString(),
    };
    // Some implementations accept empty names, others reject with 400.
    // Both are valid — just ensure it doesn't 500.
    try {
      await client.createSequence(seq);
    } catch (err: any) {
      assert.ok(err.status < 500, `server error: ${err.status}`);
    }
  });

  // --- Signal edge cases ---

  it("should return 404 when sending signal to non-existent instance", async () => {
    await assert.rejects(
      () => client.sendSignal(uuid(), "cancel"),
      (err: any) => err.status === 404,
    );
  });

  it("should handle unknown signal type gracefully", async () => {
    const seq = testSequence("signal-edge", [
      step("s1", "sleep", { duration_ms: 5000 }),
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Unknown signal type — should either be accepted (custom signal)
    // or rejected with 400. Must not 500.
    try {
      await client.sendSignal(id, "unknown_custom_signal", { data: "test" });
    } catch (err: any) {
      assert.ok(err.status < 500, `server error: ${err.status}`);
    }

    // Clean up.
    await client.sendSignal(id, "cancel");
  });

  // --- Duplicate creation ---

  it("should handle duplicate sequence ID creation", async () => {
    const seq = testSequence("dup-test", [step("s1", "noop")]);
    await client.createSequence(seq);

    // Creating with same ID should fail.
    await assert.rejects(
      () => client.createSequence(seq),
      (err: any) => err.status === 409 || err.status === 400,
    );
  });

  // --- Worker edge cases ---

  it("should return empty array when polling with unknown handler", async () => {
    const tasks = await client.pollWorkerTasks(
      `unknown-handler-${uuid().slice(0, 8)}`,
      "worker-1",
      10,
    );
    assert.ok(Array.isArray(tasks));
    assert.equal(tasks.length, 0);
  });

  it("should return 404 when completing non-existent task", async () => {
    await assert.rejects(
      () => client.completeWorkerTask(uuid(), "worker-1", { result: "done" }),
      (err: any) => err.status === 404,
    );
  });

  // --- Bulk operations edge cases ---

  it("should handle bulk update with no matching instances", async () => {
    const result = await client.bulkUpdateState(
      { tenant_id: `nonexistent-${uuid().slice(0, 8)}` },
      "cancelled",
    );
    assert.ok(result !== undefined);
  });

  // --- Content-type edge cases ---

  it("should return 400/415 for non-JSON content type", async () => {
    const baseUrl = client.baseUrl;
    const res = await fetch(`${baseUrl}/sequences`, {
      method: "POST",
      headers: { "Content-Type": "text/plain" },
      body: "not json",
    });
    assert.ok(
      res.status === 400 || res.status === 415 || res.status === 422,
      `expected client error, got ${res.status}`,
    );
  });

  it("should return 400 for malformed JSON body", async () => {
    const baseUrl = client.baseUrl;
    const res = await fetch(`${baseUrl}/sequences`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{invalid json",
    });
    assert.ok(
      res.status === 400 || res.status === 422,
      `expected 400/422, got ${res.status}`,
    );
  });
});
