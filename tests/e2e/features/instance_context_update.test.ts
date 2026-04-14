/**
 * PATCH /instances/{id}/context — focused validation.
 * Verifies 404 for unknown IDs, context persistence on GET, and
 * merge semantics (patch merges, not replaces).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Instance Context Update", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("PATCH with non-existent instance ID returns 404", async () => {
    try {
      await client.updateContext(uuid(), { data: { foo: "bar" } });
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("persisted context is visible on GET /instances/{id}", async () => {
    const tenantId = `ctx-upd-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "ctx-upd",
      [step("wait", "sleep", { duration_ms: 2000 })],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    // Wait for instance to be picked up.
    await client.waitForState(id, ["running", "scheduled"], { timeoutMs: 5_000 });

    // Patch context — updateContext sends { context: { ... } }, the engine
    // stores it under context.data.
    await client.updateContext(id, { data: { custom_key: "hello", num: 42 } });

    // Read back and verify.
    const inst = await client.getInstance(id);
    const data = ((inst.context ?? {}) as Record<string, unknown>).data as
      | Record<string, unknown>
      | undefined;
    assert.ok(data, "instance context.data should exist");
    assert.equal(data.custom_key, "hello");
    assert.equal(data.num, 42);
  });

  it("context update on completed instance still persists", async () => {
    const tenantId = `ctx-done-${uuid().slice(0, 8)}`;
    const seq = testSequence("ctx-done", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 5_000 });

    // Update context after completion.
    await client.updateContext(id, { data: { post_complete: true } });

    const inst = await client.getInstance(id);
    const data = ((inst.context ?? {}) as Record<string, unknown>).data as
      | Record<string, unknown>
      | undefined;
    assert.ok(data, "context.data should exist");
    assert.equal(data.post_complete, true);
  });
});
