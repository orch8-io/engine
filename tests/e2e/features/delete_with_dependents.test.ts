/**
 * Delete-with-dependents — verifies that deleting sequences, plugins,
 * and credentials handles dependent resources correctly (either blocking
 * the delete or cleaning up gracefully).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Delete with Dependents", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("deleting a sequence while instances are in-flight", async () => {
    const tenantId = `del-seq-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "del-seq",
      [step("slow", "sleep", { duration_ms: 2000 })],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    // Instance is running. Try to delete the sequence.
    // Depending on engine policy, this may succeed (soft delete) or fail.
    try {
      await client.deleteSequence(seq.id);
      // If it succeeds, the running instance should still complete.
      const final = await client.waitForState(id, ["completed", "failed", "cancelled"], {
        timeoutMs: 10_000,
      });
      assert.ok(final.state, "instance should reach terminal state");
    } catch (err: any) {
      // If it rejects, it should be a clear error (409 or 400), not 500.
      assert.ok(
        err.status !== 500,
        `delete should not cause 500; got ${err.status}`,
      );
    }
  });

  it("deleting a completed sequence succeeds", async () => {
    const tenantId = `del-done-${uuid().slice(0, 8)}`;
    const seq = testSequence("del-done", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 5_000 });

    // Delete should cascade-remove terminal instances and succeed.
    await client.deleteSequence(seq.id);

    // Verify sequence is gone.
    try {
      await client.getSequence(seq.id);
      assert.fail("should have returned 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("deleting a plugin that exists succeeds cleanly", async () => {
    const name = `plugin-del-${uuid().slice(0, 8)}`;
    await client.createPlugin({
      name,
      plugin_type: "wasm",
      source: "/module.wasm",
      tenant_id: "test",
    });

    await client.deletePlugin(name);

    try {
      await client.getPlugin(name);
      assert.fail("should have returned 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("deleting a credential that exists succeeds cleanly", async () => {
    const credId = `cred-del-${uuid().slice(0, 8)}`;
    await client.createCredential({
      id: credId,
      name: "Delete Me",
      kind: "api_key",
      value: "secret123",
      tenant_id: "test",
    });

    await client.deleteCredential(credId);

    try {
      await client.getCredential(credId);
      assert.fail("should have returned 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });
});
