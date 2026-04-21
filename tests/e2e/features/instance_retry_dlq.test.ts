/**
 * Instance Retry and DLQ — verifies that failed instances can be retried
 * and that the DLQ endpoint lists them correctly.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Instance Retry and DLQ", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("retryInstance transitions failed instance back to scheduled", async () => {
    const tenantId = `retry-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "retry-seq",
      [step("s1", "fail", { message: "deliberate" })],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "failed", { timeoutMs: 5_000 });

    const retried = await client.retryInstance(id);
    assert.equal(retried.state, "scheduled", "retried instance should be scheduled");
  });

  it("retryInstance clears previous outputs", async () => {
    const tenantId = `retry-clr-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "retry-clr",
      [step("s1", "fail", { message: "boom" })],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "failed", { timeoutMs: 5_000 });

    // Pre-retry outputs should exist (error output from fail step).
    const preOutputs = await client.getOutputs(id);
    assert.ok(preOutputs.length > 0, "should have error output before retry");

    await client.retryInstance(id);
    await client.waitForState(id, "failed", { timeoutMs: 8_000 });

    // After retry and re-failure, the sentinel output should be present.
    const postOutputs = await client.getOutputs(id);
    assert.ok(postOutputs.length > 0, "should have output after retry");
  });

  it("retryInstance returns 404 for non-existent instance", async () => {
    try {
      await client.retryInstance("019db000-0000-7000-0000-000000000000");
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("listDlq returns failed instances", async () => {
    const tenantId = `dlq-${uuid().slice(0, 8)}`;
    const failSeq = testSequence(
      "dlq-fail",
      [step("s1", "fail", { message: "dlq-test" })],
      { tenantId },
    );
    const okSeq = testSequence("dlq-ok", [step("s1", "noop")], { tenantId });
    await client.createSequence(failSeq);
    await client.createSequence(okSeq);

    const { id: failId } = await client.createInstance({
      sequence_id: failSeq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    const { id: okId } = await client.createInstance({
      sequence_id: okSeq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(failId, "failed", { timeoutMs: 5_000 });
    await client.waitForState(okId, "completed", { timeoutMs: 5_000 });

    const dlq = await client.listDlq({ tenant_id: tenantId });
    assert.ok(dlq.some((i: any) => i.id === failId), "DLQ should contain failed instance");
    assert.ok(!dlq.some((i: any) => i.id === okId), "DLQ should NOT contain completed instance");
  });

  it("listDlq is tenant-scoped", async () => {
    const tenantA = `dlq-a-${uuid().slice(0, 8)}`;
    const tenantB = `dlq-b-${uuid().slice(0, 8)}`;

    const seqA = testSequence("dlq-ta", [step("s1", "fail", { message: "a" })], {
      tenantId: tenantA,
    });
    const seqB = testSequence("dlq-tb", [step("s1", "fail", { message: "b" })], {
      tenantId: tenantB,
    });
    await client.createSequence(seqA);
    await client.createSequence(seqB);

    const { id: idA } = await client.createInstance({
      sequence_id: seqA.id,
      tenant_id: tenantA,
      namespace: "default",
    });
    const { id: idB } = await client.createInstance({
      sequence_id: seqB.id,
      tenant_id: tenantB,
      namespace: "default",
    });

    await client.waitForState(idA, "failed", { timeoutMs: 5_000 });
    await client.waitForState(idB, "failed", { timeoutMs: 5_000 });

    const dlqA = await client.listDlq({ tenant_id: tenantA });
    assert.ok(dlqA.some((i: any) => i.id === idA), "tenant A DLQ should have A's instance");
    assert.ok(!dlqA.some((i: any) => i.id === idB), "tenant A DLQ should NOT have B's instance");
  });

  it("retryInstance on completed instance returns error", async () => {
    const tenantId = `retry-err-${uuid().slice(0, 8)}`;
    const seq = testSequence("retry-err", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 5_000 });

    try {
      await client.retryInstance(id);
      assert.fail("should throw error");
    } catch (err: any) {
      assert.ok(err.status >= 400, "should return 4xx error");
    }
  });

  it("retryInstance preserves instance context", async () => {
    const tenantId = `retry-ctx-${uuid().slice(0, 8)}`;
    const seq = testSequence("retry-ctx", [step("s1", "fail", { message: "ctx" })], {
      tenantId,
    });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      context: { data: { marker: "preserved" } },
    });

    await client.waitForState(id, "failed", { timeoutMs: 5_000 });

    await client.retryInstance(id);
    const retried = await client.getInstance(id);
    const data = (retried.context as any)?.data;
    assert.ok(data, "context.data should exist after retry");
    assert.equal(data.marker, "preserved", "context marker should be preserved");
  });
});
