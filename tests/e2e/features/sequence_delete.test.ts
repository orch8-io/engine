import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Sequence Deletion", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should delete a sequence with no running instances", async () => {
    const seq = testSequence("delete-me", [step("s1", "noop")]);
    await client.createSequence(seq);

    // Verify it exists.
    const fetched = await client.getSequence(seq.id);
    assert.equal(fetched.id, seq.id);

    // Delete it.
    await client.deleteSequence(seq.id);

    // Should be gone.
    await assert.rejects(
      () => client.getSequence(seq.id),
      (err: any) => err.status === 404,
    );
  });

  it("should reject deleting a sequence with running instances", async () => {
    const seq = testSequence("delete-blocked", [
      step("s1", "sleep", { duration_ms: 10_000 }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for it to start running.
    await client.waitForState(id, ["running", "scheduled"], { timeoutMs: 3000 });

    // Deletion should fail — instance is still active.
    await assert.rejects(
      () => client.deleteSequence(seq.id),
      (err: any) => err.status === 409 || err.status === 400,
    );

    // Clean up: cancel the running instance.
    await client.sendSignal(id, "cancel");
  });

  it("should return 404 when deleting non-existent sequence", async () => {
    await assert.rejects(
      () => client.deleteSequence(uuid()),
      (err: any) => err.status === 404,
    );
  });

  it("should allow creating a new sequence after deletion", async () => {
    const name = `recreate-${uuid().slice(0, 8)}`;
    const seq1 = testSequence(name, [step("s1", "noop")]);
    await client.createSequence(seq1);
    await client.deleteSequence(seq1.id);

    // Create another with same name.
    const seq2 = testSequence(name, [step("s1", "noop"), step("s2", "noop")]);
    await client.createSequence(seq2);

    const fetched = await client.getSequence(seq2.id);
    assert.equal(fetched.blocks.length, 2);
  });
});
