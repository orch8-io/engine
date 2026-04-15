import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "./client.js";
import { startServer, stopServer } from "./harness.js";

const client = new Orch8Client();

describe("DLQ (Dead Letter Queue)", () => {
  let server;

  before(async () => {
    server = await startServer({ build: false });
  });

  after(async () => {
    await stopServer(server);
  });

  it("should list failed instances in DLQ", async () => {
    const tenantId = `dlq-${uuid().slice(0, 8)}`;
    const seq = testSequence("dlq-fail", [step("s1", "does_not_exist")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "failed");

    const dlq = await client.listDlq({ tenant_id: tenantId });
    assert.ok(dlq.length >= 1, "DLQ should contain the failed instance");
    assert.ok(
      dlq.some((i) => i.id === id),
      "DLQ should contain our specific instance"
    );
    assert.ok(
      dlq.every((i) => i.state === "failed"),
      "DLQ should only contain failed instances"
    );
  });

  it("should retry a failed instance", async () => {
    // Create a sequence with unknown handler so it fails naturally.
    const seqFail = testSequence("dlq-retry-fail", [step("s1", "does_not_exist")]);
    await client.createSequence(seqFail);

    const { id } = await client.createInstance({
      sequence_id: seqFail.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "failed");

    // Now create a valid sequence and retry won't change the sequence_id,
    // so let's just verify retry resets state to scheduled.
    const result = await client.retryInstance(id);
    assert.equal(result.state, "scheduled");

    // It will fail again (same unknown handler), but that's fine —
    // we verified the retry mechanism works.
    await client.waitForState(id, "failed", { timeoutMs: 10000 });
  });

  it("should reject retry on non-failed instance", async () => {
    const seq = testSequence("dlq-reject", [step("s1", "noop")]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "completed");

    await assert.rejects(() => client.retryInstance(id), (err) => {
      assert.ok(err.status === 400 || err.status === 422);
      return true;
    });
  });
});
