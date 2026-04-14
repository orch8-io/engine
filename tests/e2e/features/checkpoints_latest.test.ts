import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Checkpoints Latest Endpoint", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should return 404 when no checkpoint exists", async () => {
    const seq = testSequence("no-checkpoint", [step("s1", "noop")]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "completed");

    await assert.rejects(
      () => client.getLatestCheckpoint(id),
      (err: any) => err.status === 404,
    );
  });

  it("should return latest checkpoint after saving one", async () => {
    const seq = testSequence("with-checkpoint", [step("s1", "noop")]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "completed");

    // Save a checkpoint.
    await client.saveCheckpoint(id, { progress: 50, stage: "halfway" });

    const latest = await client.getLatestCheckpoint(id);
    assert.ok(latest);
    const data = (latest as any).checkpoint_data ?? (latest as any).data;
    assert.ok(data);
  });

  it("should return the most recent checkpoint when multiple exist", async () => {
    const seq = testSequence("multi-ckpt", [step("s1", "noop")]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "completed");

    // Save two checkpoints.
    await client.saveCheckpoint(id, { version: 1 });
    await new Promise((r) => setTimeout(r, 50));
    await client.saveCheckpoint(id, { version: 2 });

    const latest = await client.getLatestCheckpoint(id);
    const data = (latest as any).checkpoint_data ?? (latest as any).data;
    // Latest should be version 2.
    assert.equal(data?.version, 2);
  });

  it("should return 404 for non-existent instance", async () => {
    await assert.rejects(
      () => client.getLatestCheckpoint(uuid()),
      (err: any) => err.status === 404,
    );
  });
});
