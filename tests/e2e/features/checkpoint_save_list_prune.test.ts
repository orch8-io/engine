/**
 * Checkpoint Save, List, and Prune — verifies checkpoint CRUD operations.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Checkpoint Save List and Prune", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("save checkpoint creates entry", async () => {
    const tenantId = `chk-save-${uuid().slice(0, 8)}`;
    const seq = testSequence("chk-save", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 5_000 });

    const checkpoint = await client.saveCheckpoint(id, {
      step_id: "s1",
      state: "completed",
      output: { result: "ok" },
    });
    assert.ok(checkpoint.id, "checkpoint should have an id");
  });

  it("list checkpoints returns saved entries", async () => {
    const tenantId = `chk-lst-${uuid().slice(0, 8)}`;
    const seq = testSequence("chk-lst", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 5_000 });
    await client.saveCheckpoint(id, { step_id: "s1", state: "ok" });
    await client.saveCheckpoint(id, { step_id: "s2", state: "ok" });

    const checkpoints = await client.listCheckpoints(id);
    assert.ok(checkpoints.length >= 2, "should have at least 2 checkpoints");
  });

  it("get latest checkpoint returns most recent", async () => {
    const tenantId = `chk-latest-${uuid().slice(0, 8)}`;
    const seq = testSequence("chk-latest", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 5_000 });
    await client.saveCheckpoint(id, { marker: "first" });
    await new Promise((r) => setTimeout(r, 100));
    await client.saveCheckpoint(id, { marker: "second" });

    const latest = await client.getLatestCheckpoint(id);
    assert.equal((latest as any).checkpoint_data.marker, "second", "latest should be second");
  });

  it("prune checkpoints keeps N latest", async () => {
    const tenantId = `chk-prune-${uuid().slice(0, 8)}`;
    const seq = testSequence("chk-prune", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 5_000 });
    for (let i = 0; i < 5; i++) {
      await client.saveCheckpoint(id, { index: i });
      await new Promise((r) => setTimeout(r, 50));
    }

    await client.pruneCheckpoints(id, 2);

    const remaining = await client.listCheckpoints(id);
    assert.equal(remaining.length, 2, "should have exactly 2 checkpoints after prune");
  });

  it("checkpoint data round-trips", async () => {
    const tenantId = `chk-rt-${uuid().slice(0, 8)}`;
    const seq = testSequence("chk-rt", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 5_000 });
    const data = { nested: { array: [1, 2, 3], text: "hello" } };
    await client.saveCheckpoint(id, data);

    const checkpoints = await client.listCheckpoints(id);
    const found = checkpoints.find((c: any) =>
      (c.checkpoint_data as any)?.nested?.text === "hello"
    );
    assert.ok(found, "checkpoint data should round-trip");
  });

  it("latest checkpoint for non-existent instance returns 404", async () => {
    try {
      await client.getLatestCheckpoint("019db000-0000-7000-0000-000000000000");
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("prune to zero clears all checkpoints", async () => {
    const tenantId = `chk-zero-${uuid().slice(0, 8)}`;
    const seq = testSequence("chk-zero", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 5_000 });
    await client.saveCheckpoint(id, { marker: "x" });
    await client.pruneCheckpoints(id, 0);

    const remaining = await client.listCheckpoints(id);
    assert.equal(remaining.length, 0, "prune to 0 should clear all");
  });
});
