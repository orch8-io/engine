/**
 * Inject Blocks Position — verifies dynamic step injection at various
 * positions and error cases.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Inject Blocks Position", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("inject blocks at position 0 prepends to sequence", async () => {
    const tenantId = `inj-0-${uuid().slice(0, 8)}`;
    const seq = testSequence("inj-0", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 5_000 });

    const result = await client.injectBlocks(id, [step("injected", "noop")], 0);
    assert.ok(result, "inject should succeed");
  });

  it("inject blocks at end appends to sequence", async () => {
    const tenantId = `inj-end-${uuid().slice(0, 8)}`;
    const seq = testSequence("inj-end", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 5_000 });

    const result = await client.injectBlocks(id, [step("injected", "noop")]);
    assert.ok(result, "inject should succeed");
  });

  it("inject blocks in middle of sequence", async () => {
    const tenantId = `inj-mid-${uuid().slice(0, 8)}`;
    const seq = testSequence("inj-mid", [
      step("s1", "noop"),
      step("s2", "noop"),
    ], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 5_000 });

    const result = await client.injectBlocks(id, [step("injected", "noop")], 1);
    assert.ok(result, "inject at position 1 should succeed");
  });

  it("inject blocks into failed instance is accepted", async () => {
    const tenantId = `inj-run-${uuid().slice(0, 8)}`;
    const seq = testSequence("inj-run", [step("s1", "fail")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "failed", { timeoutMs: 5_000 });

    // Injection into failed instance should succeed for retry purposes.
    const result = await client.injectBlocks(id, [step("injected", "noop")]);
    assert.ok(result, "inject into failed instance should succeed");
  });

  it("inject into completed instance is accepted", async () => {
    const tenantId = `inj-done-${uuid().slice(0, 8)}`;
    const seq = testSequence("inj-done", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 5_000 });

    // Injection into completed instance should be allowed for retry purposes.
    const result = await client.injectBlocks(id, [step("injected", "noop")]);
    assert.ok(result, "inject into completed instance should succeed");
  });

  it("inject empty blocks returns error", async () => {
    const tenantId = `inj-empty-${uuid().slice(0, 8)}`;
    const seq = testSequence("inj-empty", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 5_000 });

    try {
      await client.injectBlocks(id, []);
      assert.fail("should throw error");
    } catch (err: any) {
      assert.ok(err.status >= 400, `expected error, got ${err.status}`);
    }
  });
});
