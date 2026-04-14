/**
 * Loop block variations — max_iterations cap, context-driven conditions,
 * multi-step body, and nested loops. All tests use in-process handlers
 * so iteration timing is deterministic.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

describe("Loop variations", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("loop with max_iterations=1 runs body once", async () => {
    const seq = testSequence("lv-one", [
      {
        type: "loop",
        id: "l",
        condition: "true",
        max_iterations: 1,
        body: [step("tick", "log", { message: "t" })],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const ticks = (await client.getOutputs(id)).filter((o) => o.block_id === "tick");
    assert.equal(ticks.length, 1);
  });

  it("loop with multi-step body records all step outputs per iteration", async () => {
    const seq = testSequence("lv-multi", [
      {
        type: "loop",
        id: "l",
        condition: "true",
        max_iterations: 3,
        body: [
          step("a", "log", { message: "a" }),
          step("b", "log", { message: "b" }),
        ],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 15_000 });
    const outputs = await client.getOutputs(id);
    assert.equal(outputs.filter((o) => o.block_id === "a").length, 3);
    assert.equal(outputs.filter((o) => o.block_id === "b").length, 3);
  });

  it("loop condition reading context flag exits early when false", async () => {
    const seq = testSequence("lv-ctx-false", [
      {
        type: "loop",
        id: "l",
        condition: "context.data.go",
        max_iterations: 10,
        body: [step("body", "log", { message: "b" })],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { go: false } },
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const bodies = (await client.getOutputs(id)).filter((o) => o.block_id === "body");
    assert.equal(bodies.length, 0);
  });

  it("nested loops: outer N × inner M iterations", async () => {
    const N = 2,
      M = 3;
    const seq = testSequence("lv-nested", [
      {
        type: "loop",
        id: "outer",
        condition: "true",
        max_iterations: N,
        body: [
          {
            type: "loop",
            id: "inner",
            condition: "true",
            max_iterations: M,
            body: [step("leaf", "log", { message: "l" })],
          },
        ],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 20_000 });
    const leaves = (await client.getOutputs(id)).filter((o) => o.block_id === "leaf");
    assert.equal(leaves.length, N * M);
  });

  it("loop body failing fails instance (no catch)", async () => {
    const seq = testSequence("lv-fail", [
      {
        type: "loop",
        id: "l",
        condition: "true",
        max_iterations: 5,
        body: [step("bad", "fail", { message: "x" })],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    const done = await client.waitForState(id, ["completed", "failed"], {
      timeoutMs: 10_000,
    });
    assert.equal(done.state, "failed");
  });

  it("loop wrapped in try_catch survives body failure", async () => {
    const seq = testSequence("lv-tc", [
      {
        type: "try_catch",
        id: "tc",
        try_block: [
          {
            type: "loop",
            id: "l",
            condition: "true",
            max_iterations: 5,
            body: [step("bad", "fail", { message: "x" })],
          },
        ],
        catch_block: [step("rescued", "log", { message: "r" })],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    const done = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(done.state, "completed");
    const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
    assert.ok(ids.has("rescued"));
  });
});
