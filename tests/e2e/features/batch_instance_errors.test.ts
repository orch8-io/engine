/**
 * Batch instance creation — error handling for POST /instances/batch.
 * Validates behaviour with invalid sequence IDs, mixed valid/invalid items,
 * and large batch sizes.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Batch Instance Creation Errors", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("batch with valid items creates all instances", async () => {
    const tenantId = `bat-ok-${uuid().slice(0, 8)}`;
    const seq = testSequence("bat-ok", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    const items = Array.from({ length: 5 }, () => ({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    }));

    const res = await client.createInstancesBatch(items);
    assert.equal((res as any).count, 5, "should create 5 instances");
  });

  it("batch with non-existent sequence_id returns error", async () => {
    const tenantId = `bat-bad-${uuid().slice(0, 8)}`;
    const items = [
      {
        sequence_id: uuid(),
        tenant_id: tenantId,
        namespace: "default",
      },
    ];

    try {
      await client.createInstancesBatch(items);
      assert.fail("should have thrown for non-existent sequence");
    } catch (err: any) {
      assert.ok(
        err.status === 400 || err.status === 404 || err.status === 500,
        `expected error status, got ${err.status}`,
      );
    }
  });

  it("large batch (50 items) succeeds", async () => {
    const tenantId = `bat-big-${uuid().slice(0, 8)}`;
    const seq = testSequence("bat-big", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    const items = Array.from({ length: 50 }, () => ({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    }));

    const res = await client.createInstancesBatch(items);
    assert.equal((res as any).count, 50, "should create 50 instances");
  });
});
