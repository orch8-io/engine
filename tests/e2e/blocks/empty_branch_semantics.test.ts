/**
 * Empty-branch semantics for composite blocks.
 *
 * Verifies that composite blocks behave correctly when a branch has zero
 * body steps or a collection is empty — the composite should reach a
 * terminal state deterministically instead of hanging or producing
 * spurious children.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

describe("Empty-branch semantics", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("parallel with a single empty branch completes", async () => {
    const seq = testSequence("empty-parallel-one", [
      { type: "parallel", id: "p", branches: [[]] } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    const done = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(done.state, "completed");
  });

  it("parallel with one empty and one non-empty branch completes both", async () => {
    const seq = testSequence("empty-parallel-mixed", [
      {
        type: "parallel",
        id: "p",
        branches: [[], [step("a", "log", { message: "A" })]],
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
    const outputs = await client.getOutputs(id);
    assert.ok(outputs.some((o) => o.block_id === "a"));
  });

  it("router with no matching route and no default still completes", async () => {
    const seq = testSequence("empty-router-nomatch", [
      {
        type: "router",
        id: "rt",
        routes: [
          {
            condition: 'context.data.mode == "X"',
            blocks: [step("x", "log", { message: "X" })],
          },
        ],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { mode: "Y" } },
    });
    const done = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(done.state, "completed");
    const outputs = await client.getOutputs(id);
    assert.equal(outputs.filter((o) => o.block_id === "x").length, 0);
  });

  it("try_catch with empty catch swallows try failure", async () => {
    const seq = testSequence("empty-trycatch-nocatch", [
      {
        type: "try_catch",
        id: "tc",
        try_block: [step("boom", "fail", { message: "oops" })],
        catch_block: [],
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
    assert.equal(done.state, "completed");
  });

  it("for_each over empty collection completes with no body outputs", async () => {
    const seq = testSequence("empty-foreach", [
      {
        type: "for_each",
        id: "fe",
        collection: "items",
        body: [step("body", "log", { message: "x" })],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { items: [] } },
    });
    const done = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(done.state, "completed");
    const outputs = await client.getOutputs(id);
    assert.equal(outputs.filter((o) => o.block_id === "body").length, 0);
  });

  it("loop with max_iterations=0 is rejected at creation time", async () => {
    const seq = testSequence("empty-loop-zero", [
      {
        type: "loop",
        id: "l",
        condition: "true",
        max_iterations: 0,
        body: [step("body", "log", { message: "x" })],
      } as Block,
    ]);
    await assert.rejects(
      () => client.createSequence(seq),
      (err: any) => {
        assert.equal(err.status, 400);
        assert.match(err.body, /max_iterations must be > 0/);
        return true;
      },
    );
  });
});
