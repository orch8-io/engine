/**
 * Signal Terminal and Payload — verifies signal dispatch behavior
 * for active vs terminal instances and payload preservation.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Signal Terminal and Payload", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("send signal to non-existent instance returns 404", async () => {
    try {
      await client.sendSignal(uuid(), "resume", {});
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("send signal to completed instance returns 400", async () => {
    const seq = testSequence("sig-term", [step("s1", "noop")]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed");

    try {
      await client.sendSignal(id, "resume", {});
      assert.fail("should throw 400");
    } catch (err: any) {
      assert.ok(err.status === 400 || err.status === 409, `expected 400/409, got ${err.status}`);
    }
  });

  it("send signal with payload returns signal_id", async () => {
    const seq = testSequence("sig-payload", [
      step("s1", "wait_for_input", { options: { timeout_ms: 30000 } }),
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "waiting");

    const res = await client.sendSignal(id, { custom: "my_event" } as unknown as string, { key: "value", num: 42 });
    assert.ok((res as any).signal_id, "should return signal_id");
  });

  it("send signal without payload succeeds", async () => {
    const seq = testSequence("sig-empty", [
      step("s1", "wait_for_input", { options: { timeout_ms: 30000 } }),
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "waiting");

    const res = await client.sendSignal(id, "resume", {});
    assert.ok((res as any).signal_id, "should return signal_id");
  });

  it("send signal to scheduled instance succeeds", async () => {
    const seq = testSequence("sig-scheduled", [step("s1", "noop")]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    // Instance starts as Scheduled — we can signal before it runs.
    const res = await client.sendSignal(id, "resume", { pre: true });
    assert.ok((res as any).signal_id, "should return signal_id");
  });
});
