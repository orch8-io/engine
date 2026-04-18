import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "./client.ts";
import { startServer, stopServer } from "./harness.ts";
import type { ServerHandle } from "./harness.ts";

const client = new Orch8Client();

describe("Instance Lifecycle", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should complete a single noop step", async () => {
    const seq = testSequence("single-noop", [step("s1", "noop")]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    const instance = await client.waitForState(id, "completed");
    assert.equal(instance.state, "completed");
  });

  it("should complete a multi-step sequence", async () => {
    const seq = testSequence("multi-step", [
      step("s1", "log", { message: "step 1" }),
      step("s2", "noop"),
      step("s3", "log", { message: "step 3" }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    const instance = await client.waitForState(id, "completed");
    assert.equal(instance.state, "completed");

    const outputs = await client.getOutputs(id);
    assert.equal(outputs.length, 3);
    const blockIds = outputs.map((o) => o.block_id).sort();
    assert.deepEqual(blockIds, ["s1", "s2", "s3"]);
  });

  it("should complete a sleep step", async () => {
    const seq = testSequence("sleep-step", [step("s1", "sleep", { duration_ms: 50 })]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    const instance = await client.waitForState(id, "completed");
    assert.equal(instance.state, "completed");

    const outputs = await client.getOutputs(id);
    assert.equal((outputs[0]!.output as any).slept_ms, 50);
  });

  it("should fail on unknown handler", async () => {
    const seq = testSequence("unknown-handler", [step("s1", "does_not_exist")]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    const instance = await client.waitForState(id, "failed");
    assert.equal(instance.state, "failed");
  });

  it("should persist and retrieve outputs", async () => {
    const seq = testSequence("output-check", [
      step("s1", "log", { message: "hello from test" }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);
    assert.equal(outputs.length, 1);
    assert.equal(outputs[0]!.block_id, "s1");
    assert.equal((outputs[0]!.output as any).message, "hello from test");
  });

  it("should create instances in batch", async () => {
    const seq = testSequence("batch-test", [step("s1", "noop")]);
    await client.createSequence(seq);

    const instances = Array.from({ length: 5 }, () => ({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    }));

    const result = await client.createInstancesBatch(instances);
    assert.equal((result as any).count, 5);
  });

  it("should list instances with filter", async () => {
    const tenantId = `tenant-${uuid().slice(0, 8)}`;
    const seq = testSequence("list-test", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    // Small delay for the instance to be visible
    await new Promise((r) => setTimeout(r, 100));

    const list = await client.listInstances({ tenant_id: tenantId });
    assert.ok(list.length >= 1);
    assert.ok(list.every((i) => i.tenant_id === tenantId));
  });
});
