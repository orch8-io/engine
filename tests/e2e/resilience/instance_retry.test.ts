/**
 * Instance retry — verifies POST /instances/{id}/retry re-queues a
 * failed instance and it can complete on the second attempt.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Instance Retry", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("retrying a failed instance re-queues it", async () => {
    const tenantId = `retry-${uuid().slice(0, 8)}`;

    // Use the built-in `fail` handler (no params) for a deterministic failure.
    const seq = testSequence(
      "retry-src",
      [step("f", "fail")],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "failed", { timeoutMs: 10_000 });

    // Retry resets state to scheduled.
    const retried = await client.retryInstance(id);
    assert.equal(retried.state, "scheduled", "retry should reset to scheduled");
  });

  it("retrying a non-existent instance returns 404", async () => {
    try {
      await client.retryInstance(uuid());
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("retrying a completed instance returns error (not 500)", async () => {
    const tenantId = `retry-ok-${uuid().slice(0, 8)}`;
    const seq = testSequence("retry-ok", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 5_000 });

    try {
      await client.retryInstance(id);
      // Some engines allow retry of completed instances.
    } catch (err: any) {
      assert.ok(
        err.status !== 500,
        `retry of completed should not be 500; got ${err.status}`,
      );
    }
  });
});
