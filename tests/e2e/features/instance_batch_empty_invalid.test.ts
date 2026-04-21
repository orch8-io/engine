/**
 * Instance Batch Empty and Invalid — verifies edge cases for
 * POST /instances/batch.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Instance Batch Empty and Invalid", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("empty batch returns count zero", async () => {
    const res = await client.createInstancesBatch([]);
    assert.equal((res as any).count, 0);
  });

  it("batch with invalid sequence_id returns 404", async () => {
    try {
      await client.createInstancesBatch([
        { sequence_id: uuid(), tenant_id: "test", namespace: "default" },
      ]);
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("batch with multiple valid instances creates them", async () => {
    const seq = testSequence("batch-multi", [step("s1", "noop")]);
    await client.createSequence(seq);

    const res = await client.createInstancesBatch([
      { sequence_id: seq.id, tenant_id: "test", namespace: "default" },
      { sequence_id: seq.id, tenant_id: "test", namespace: "default" },
    ]);

    assert.equal((res as any).count, 2);
  });

  it("batch with mixed valid and invalid returns 404", async () => {
    const seq = testSequence("batch-mix", [step("s1", "noop")]);
    await client.createSequence(seq);

    try {
      await client.createInstancesBatch([
        { sequence_id: seq.id, tenant_id: "test", namespace: "default" },
        { sequence_id: uuid(), tenant_id: "test", namespace: "default" },
      ]);
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("batch with context creates instances and preserves context", async () => {
    const seq = testSequence("batch-ctx", [step("s1", "noop")]);
    await client.createSequence(seq);

    const res = await client.createInstancesBatch([
      { sequence_id: seq.id, tenant_id: "test", namespace: "default", context: { data: { a: 1 } } },
      { sequence_id: seq.id, tenant_id: "test", namespace: "default", context: { data: { b: 2 } } },
    ]);

    assert.equal((res as any).count, 2);
  });
});
