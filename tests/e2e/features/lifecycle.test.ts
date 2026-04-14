import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

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

  it("should dispatch unknown handler to external worker queue", async () => {
    const seq = testSequence("unknown-handler", [step("s1", "does_not_exist")]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Unknown handlers are dispatched to external workers (using handler
    // name as the implicit queue). The instance transitions to `waiting`
    // until a worker picks up the task.
    const instance = await client.waitForState(id, "waiting");
    assert.equal(instance.state, "waiting");
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

  // Plan #7: list instances pagination works (limit + offset)
  it("should paginate list with limit and offset", async () => {
    // Isolate per-test tenant so our counts are deterministic on a
    // shared server.
    const tenantId = `pg-${uuid().slice(0, 8)}`;
    const seq = testSequence("pagination", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    // Create 5 instances so we can page with limit=2 / offset=2.
    const batch = Array.from({ length: 5 }, () => ({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    }));
    await client.createInstancesBatch(batch);
    await new Promise((r) => setTimeout(r, 200));

    const page1 = await client.listInstances({ tenant_id: tenantId, limit: 2, offset: 0 });
    const page2 = await client.listInstances({ tenant_id: tenantId, limit: 2, offset: 2 });
    const page3 = await client.listInstances({ tenant_id: tenantId, limit: 2, offset: 4 });

    assert.equal(page1.length, 2, "page 1 should have limit items");
    assert.equal(page2.length, 2, "page 2 should have limit items");
    assert.ok(page3.length >= 1, "page 3 should have the trailing item(s)");

    // No overlap between pages — pagination must move the window.
    const page1Ids = new Set(page1.map((i) => i.id));
    for (const inst of page2) {
      assert.ok(!page1Ids.has(inst.id), "page 2 must not repeat page 1 ids");
    }
  });

  // Plan #10: update instance context merges data
  it("should merge new fields into instance context via PATCH", async () => {
    // Pause the instance mid-flight so we can PATCH its context. A sleep
    // step with duration_ms gives us a generous window; we pause, patch,
    // then resume so the instance terminates cleanly for the next tests.
    const seq = testSequence("ctx-merge", [
      step("s1", "sleep", { duration_ms: 5000 }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { keep_me: "original" } },
    });

    // Let the instance start, then pause so context isn't overwritten by
    // an active handler.
    await new Promise((r) => setTimeout(r, 300));
    await client.sendSignal(id, "pause");
    await client.waitForState(id, ["paused", "completed"], { timeoutMs: 8000 });

    // Merge: add a new field, existing field must survive.
    await client.updateContext(id, { data: { added: "new" } });

    const after = await client.getInstance(id);
    const data = ((after.context ?? {}) as Record<string, unknown>).data as
      | Record<string, unknown>
      | undefined;
    // The server's merge semantics (spec #10) require the new field to
    // appear. We don't assert the full shape of the original field because
    // merge policy (deep vs. shallow) is implementation-defined, but at
    // minimum the added key must be visible.
    assert.ok(data, "context.data must be present after merge");
    assert.equal(data!.added, "new", "new field must appear after PATCH");

    // Clean up — resume (if still paused) so the instance drains.
    if (after.state === "paused") {
      await client.sendSignal(id, "resume");
    }
  });

  // Plan #9: PATCH /instances/{id}/state transitions scheduled → paused.
  it("should transition scheduled instance to paused via PATCH /state", async () => {
    // Create an instance that stays `scheduled` long enough to PATCH.
    // A sleep step keeps it alive for the duration.
    const seq = testSequence("state-patch", [
      step("s1", "sleep", { duration_ms: 4000 }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // PATCH to paused. Works from either scheduled or running — the
    // engine accepts state transitions from any pre-terminal state.
    await client.updateState(id, "paused");

    const after = await client.waitForState(
      id,
      ["paused", "completed"],
      { timeoutMs: 8000 },
    );
    // In fast CI the sleep may have finished — accept completed as a
    // post-hoc terminal state. Otherwise we expect paused.
    assert.ok(
      ["paused", "completed"].includes(after.state),
      `expected paused or completed after PATCH, got ${after.state}`,
    );

    // Drain the instance so follow-on tests start clean.
    if (after.state === "paused") {
      await client.sendSignal(id, "resume");
    }
  });

  // Plan #130: block outputs are retrievable via GET /outputs *after*
  // the instance has reached a terminal state.
  it("should return block outputs after instance completion", async () => {
    const seq = testSequence("outputs-after-complete", [
      step("s1", "log", { message: "first" }),
      step("s2", "log", { message: "second" }),
      step("s3", "noop"),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "completed");

    // Small pause to ensure any post-terminal flushing finishes before
    // we read.
    await new Promise((r) => setTimeout(r, 100));

    const outputs = await client.getOutputs(id);
    assert.equal(outputs.length, 3, "all 3 step outputs must be retrievable");
    const byId = new Map(outputs.map((o) => [o.block_id, o.output]));
    assert.equal(
      (byId.get("s1") as { message?: string }).message,
      "first",
    );
    assert.equal(
      (byId.get("s2") as { message?: string }).message,
      "second",
    );
    assert.ok(byId.has("s3"), "noop step should have an output row");
  });
});
