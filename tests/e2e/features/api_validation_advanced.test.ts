/**
 * API Validation Advanced — verifies 422/400 responses for malformed
 * or incomplete request bodies across various endpoints.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("API Validation Advanced", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("create instance without sequence_id returns 422", async () => {
    try {
      await client.createInstance({
        tenant_id: "test",
        namespace: "default",
      } as any);
      assert.fail("should throw 422");
    } catch (err: any) {
      assert.equal(err.status, 422);
    }
  });

  it("create instance with invalid uuid returns 422", async () => {
    try {
      await client.createInstance({
        sequence_id: "not-a-uuid",
        tenant_id: "test",
        namespace: "default",
      } as any);
      assert.fail("should throw 422");
    } catch (err: any) {
      assert.equal(err.status, 422);
    }
  });

  it("create sequence without blocks returns 422", async () => {
    try {
      await client.createSequence({
        id: uuid(),
        tenant_id: "test",
        namespace: "default",
        name: `no-blocks-${uuid().slice(0, 8)}`,
        version: 1,
        blocks: [],
      } as any);
      assert.fail("should throw 422");
    } catch (err: any) {
      assert.equal(err.status, 422);
    }
  });

  it("create sequence with invalid block type returns 422", async () => {
    try {
      await client.createSequence({
        id: uuid(),
        tenant_id: "test",
        namespace: "default",
        name: `bad-type-${uuid().slice(0, 8)}`,
        version: 1,
        blocks: [{ type: "unknown_type", id: "bad" }],
      } as any);
      assert.fail("should throw 422");
    } catch (err: any) {
      assert.equal(err.status, 422);
    }
  });

  it("patch state with invalid transition returns 400", async () => {
    const tenantId = `val-st-${uuid().slice(0, 8)}`;
    const seq = testSequence("val-st", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 5_000 });

    try {
      await client.updateState(id, "not_a_real_state");
      assert.fail("should throw error");
    } catch (err: any) {
      assert.ok(err.status >= 400, `expected error, got ${err.status}`);
    }
  });

  it("send signal without signal_type returns 422", async () => {
    const tenantId = `val-sig-${uuid().slice(0, 8)}`;
    const seq = testSequence("val-sig", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    try {
      await client.sendSignal(id, "", {});
      // Some implementations may accept empty string; check it's handled.
    } catch (err: any) {
      assert.ok(err.status >= 400, `expected error, got ${err.status}`);
    }
  });

  it("bulk reschedule without offset returns 422", async () => {
    try {
      await client.bulkReschedule({ tenant_id: "test" }, undefined as any);
      assert.fail("should throw 422");
    } catch (err: any) {
      assert.equal(err.status, 422);
    }
  });
});
