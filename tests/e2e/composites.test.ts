import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "./client.ts";
import { startServer, stopServer } from "./harness.ts";
import type { ServerHandle } from "./harness.ts";
import type { Block } from "./client.ts";

const client = new Orch8Client();

/**
 * Helper: build a parallel block definition.
 */
function parallel(id: string, branches: unknown[]): Record<string, unknown> {
  return { type: "parallel", id, branches };
}

/**
 * Helper: build a race block definition.
 */
function race(id: string, branches: unknown[], semantics?: string): Record<string, unknown> {
  const block: Record<string, unknown> = { type: "race", id, branches };
  if (semantics) block.semantics = semantics;
  return block;
}

/**
 * Helper: build a router block definition.
 */
function router(
  id: string,
  routes: unknown[],
  defaultBlocks?: unknown
): Record<string, unknown> {
  const block: Record<string, unknown> = { type: "router", id, routes };
  if (defaultBlocks) block.default = defaultBlocks;
  return block;
}

/**
 * Helper: build a try-catch block definition.
 */
function tryCatch(
  id: string,
  tryBlock: unknown,
  catchBlock: unknown,
  finallyBlock?: unknown
): Record<string, unknown> {
  const block: Record<string, unknown> = { type: "try_catch", id, try_block: tryBlock, catch_block: catchBlock };
  if (finallyBlock) block.finally_block = finallyBlock;
  return block;
}

/**
 * Helper: build a forEach block definition.
 */
function forEach(
  id: string,
  collection: string,
  body: unknown,
  opts: Record<string, unknown> = {}
): Record<string, unknown> {
  return { type: "for_each", id, collection, body, ...opts };
}

describe("Composite Blocks", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // --- Parallel ---

  it("parallel: both branches complete", async () => {
    const seq = testSequence("par-basic", [
      parallel("p1", [
        [step("a1", "noop")],
        [step("b1", "noop")],
      ]) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    const completed = await client.waitForState(id, "completed");
    assert.equal(completed.state, "completed");
  });

  it("parallel: multi-step branches", async () => {
    const seq = testSequence("par-multi", [
      parallel("p1", [
        [step("a1", "noop"), step("a2", "log", { message: "branch-a" })],
        [step("b1", "log", { message: "branch-b" })],
      ]) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    const completed = await client.waitForState(id, "completed");
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    // Should have outputs for a1, a2, b1
    const blockIds = outputs.map((o) => o.block_id).sort();
    assert.ok(blockIds.includes("a1"), "missing output for a1");
    assert.ok(blockIds.includes("a2"), "missing output for a2");
    assert.ok(blockIds.includes("b1"), "missing output for b1");
  });

  it("parallel: branch failure fails the parallel block", async () => {
    // Use an unknown handler in one branch to create a worker task,
    // then fail it permanently.
    const seq = testSequence("par-fail", [
      parallel("p1", [
        [step("a1", "noop")],
        [step("b1", "will_fail_handler", {})],
      ]) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for instance to go to waiting (external handler dispatched).
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });

    // Poll and permanently fail the external task.
    const tasks = await client.pollWorkerTasks("will_fail_handler", "worker-1");
    assert.equal(tasks.length, 1);
    await client.failWorkerTask(tasks[0]!.id, "worker-1", "permanent error", false);

    // Instance should fail because one branch failed.
    const failed = await client.waitForState(id, "failed", { timeoutMs: 5000 });
    assert.equal(failed.state, "failed");
  });

  // --- Race ---

  it("race: first branch to complete wins", async () => {
    // Branch A: fast noop, Branch B: external worker (slow).
    // A completes immediately, B should be cancelled.
    const seq = testSequence("race-basic", [
      race("r1", [
        [step("fast", "noop")],
        [step("slow", "slow_race_handler", {})],
      ]) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    const completed = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(completed.state, "completed");
  });

  it("race: all branches fail => race fails", async () => {
    const seq = testSequence("race-all-fail", [
      race("r1", [
        [step("f1", "fail_race_a", {})],
        [step("f2", "fail_race_b", {})],
      ]) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for waiting state (both are external handlers).
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });

    // Fail both branches permanently.
    const tasksA = await client.pollWorkerTasks("fail_race_a", "worker-1");
    if (tasksA.length > 0) {
      await client.failWorkerTask(tasksA[0]!.id, "worker-1", "fail", false);
    }

    // After first failure, instance may re-schedule. Wait for waiting again if needed.
    await client.waitForState(id, ["waiting", "failed"], { timeoutMs: 5000 });

    const tasksB = await client.pollWorkerTasks("fail_race_b", "worker-1");
    if (tasksB.length > 0) {
      await client.failWorkerTask(tasksB[0]!.id, "worker-1", "fail", false);
    }

    const failed = await client.waitForState(id, "failed", { timeoutMs: 10_000 });
    assert.equal(failed.state, "failed");
  });

  // --- Router ---

  it("router: selects correct branch based on context", async () => {
    const seq = testSequence("router-basic", [
      router(
        "rt1",
        [
          {
            condition: 'mode == "fast"',
            blocks: [step("fast_step", "log", { message: "fast path" })],
          },
          {
            condition: 'mode == "slow"',
            blocks: [step("slow_step", "log", { message: "slow path" })],
          },
        ],
        [step("default_step", "log", { message: "default path" })]
      ) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { mode: "fast" } },
    });

    const completed = await client.waitForState(id, "completed");
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(blockIds.includes("fast_step"), "should execute fast_step");
    assert.ok(!blockIds.includes("slow_step"), "should NOT execute slow_step");
    assert.ok(!blockIds.includes("default_step"), "should NOT execute default_step");
  });

  it("router: falls through to default when no conditions match", async () => {
    const seq = testSequence("router-default", [
      router(
        "rt1",
        [
          {
            condition: "mode == nonexistent",
            blocks: [step("never_step", "noop")],
          },
        ],
        [step("default_step", "log", { message: "default" })]
      ) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { mode: "something_else" } },
    });

    const completed = await client.waitForState(id, "completed");
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(blockIds.includes("default_step"), "should execute default_step");
    assert.ok(!blockIds.includes("never_step"), "should NOT execute never_step");
  });

  // --- TryCatch ---

  it("try-catch: try succeeds, catch is skipped", async () => {
    const seq = testSequence("tc-success", [
      tryCatch(
        "tc1",
        [step("try_step", "noop")],
        [step("catch_step", "log", { message: "caught" })]
      ) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    const completed = await client.waitForState(id, "completed");
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(blockIds.includes("try_step"), "try_step should have output");
    // Catch should be skipped when try succeeds.
    assert.ok(!blockIds.includes("catch_step"), "catch_step should NOT have output");
  });

  it("try-catch: try fails, catch executes and recovers", async () => {
    // Use external handler in try block so we can fail it.
    const seq = testSequence("tc-recover", [
      tryCatch(
        "tc1",
        [step("try_step", "will_fail_tc", {})],
        [step("catch_step", "log", { message: "recovered" })]
      ) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for external handler dispatch.
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });

    // Poll and fail the try step permanently.
    const tasks = await client.pollWorkerTasks("will_fail_tc", "worker-1");
    assert.equal(tasks.length, 1);
    await client.failWorkerTask(tasks[0]!.id, "worker-1", "try failed", false);

    // After try fails, catch should run and instance should complete.
    const completed = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(blockIds.includes("catch_step"), "catch_step should have output after try failure");
  });

  it("try-catch-finally: finally always runs", async () => {
    const seq = testSequence("tc-finally", [
      tryCatch(
        "tc1",
        [step("try_step", "noop")],
        [step("catch_step", "noop")],
        [step("finally_step", "log", { message: "cleanup" })]
      ) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    const completed = await client.waitForState(id, "completed");
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(blockIds.includes("try_step"), "try_step should run");
    assert.ok(blockIds.includes("finally_step"), "finally_step should always run");
  });

  // --- ForEach ---

  it("forEach: iterates over collection from context", async () => {
    const seq = testSequence("fe-basic", [
      forEach("fe1", "items", [step("body_step", "noop")]) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { items: ["a", "b", "c"] } },
    });

    const completed = await client.waitForState(id, "completed", { timeoutMs: 15_000 });
    assert.equal(completed.state, "completed");
  });

  it("forEach: empty collection completes immediately", async () => {
    const seq = testSequence("fe-empty", [
      forEach("fe1", "items", [step("body_step", "noop")]) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { items: [] } },
    });

    const completed = await client.waitForState(id, "completed", { timeoutMs: 5000 });
    assert.equal(completed.state, "completed");
  });

  it("forEach: missing collection completes (no-op)", async () => {
    const seq = testSequence("fe-missing", [
      forEach("fe1", "nonexistent", [step("body_step", "noop")]) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    const completed = await client.waitForState(id, "completed", { timeoutMs: 5000 });
    assert.equal(completed.state, "completed");
  });

  // --- Nested composites ---

  it("nested: parallel with try-catch containing external worker", async () => {
    // Branch A: try-catch where try uses external worker that fails => catch recovers.
    // Branch B: simple noop.
    const seq = testSequence("nested-par-tc", [
      parallel("p1", [
        [
          tryCatch(
            "tc1",
            [step("try_ext", "nested_fail_handler", {})],
            [step("catch_step", "log", { message: "recovered" })]
          ),
        ],
        [step("b1", "noop")],
      ]) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for external handler dispatch.
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });

    // Fail the try step permanently so catch kicks in.
    const tasks = await client.pollWorkerTasks("nested_fail_handler", "worker-1");
    assert.equal(tasks.length, 1);
    await client.failWorkerTask(tasks[0]!.id, "worker-1", "nested fail", false);

    // Instance should complete — catch recovers branch A, branch B already done.
    const completed = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(blockIds.includes("catch_step"), "catch_step should run after try failure");
    assert.ok(blockIds.includes("b1"), "b1 should complete in parallel");
  });

  // --- Composite after steps ---

  it("step then parallel: sequential + concurrent mix", async () => {
    const seq = testSequence("step-then-par", [
      step("s1", "log", { message: "before parallel" }),
      parallel("p1", [
        [step("a1", "noop")],
        [step("b1", "noop")],
      ]) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    const completed = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(blockIds.includes("s1"), "s1 should complete");
    assert.ok(blockIds.includes("a1"), "a1 should complete");
    assert.ok(blockIds.includes("b1"), "b1 should complete");
  });
});
