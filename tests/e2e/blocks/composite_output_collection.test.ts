/**
 * Output collection across composite blocks.
 *
 * After a run completes, `getOutputs` must return every block output that
 * actually executed. These tests pin down output-count invariants for
 * each composite type so regressions in the output-write path surface
 * immediately.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

describe("Composite output collection", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("parallel collects outputs from every branch", async () => {
    const seq = testSequence("co-parallel", [
      {
        type: "parallel",
        id: "p",
        branches: [
          [step("a", "log", { message: "A" })],
          [step("b", "log", { message: "B" })],
          [step("c", "log", { message: "C" })],
        ],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const ids = (await client.getOutputs(id)).map((o) => o.block_id);
    for (const i of ["a", "b", "c"]) assert.ok(ids.includes(i), `missing ${i}`);
  });

  it("loop produces one output per iteration for body step", async () => {
    const N = 4;
    const seq = testSequence("co-loop", [
      {
        type: "loop",
        id: "l",
        condition: "true",
        max_iterations: N,
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
    assert.equal(ticks.length, N);
  });

  it("for_each produces one output per item", async () => {
    const items = ["a", "b", "c", "d"];
    const seq = testSequence("co-foreach", [
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
      context: { data: { items } },
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const bodies = (await client.getOutputs(id)).filter((o) => o.block_id === "body");
    assert.equal(bodies.length, items.length);
  });

  it("router only records output from the selected branch", async () => {
    const seq = testSequence("co-router", [
      {
        type: "router",
        id: "rt",
        routes: [
          {
            condition: "context.data.mode == \"A\"",
            blocks: [step("a", "log", { message: "A" })],
          },
          {
            condition: "context.data.mode == \"B\"",
            blocks: [step("b", "log", { message: "B" })],
          },
        ],
        default: [step("d", "log", { message: "D" })],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { mode: "B" } },
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
    assert.deepEqual(
      [...ids].filter((i) => ["a", "b", "d"].includes(i)).sort(),
      ["b"],
    );
  });

  it("try_catch records try output when try succeeds (no catch output)", async () => {
    const seq = testSequence("co-trycatch-ok", [
      {
        type: "try_catch",
        id: "tc",
        try_block: [step("ok", "log", { message: "ok" })],
        catch_block: [step("caught", "log", { message: "caught" })],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
    assert.ok(ids.has("ok"));
    assert.ok(!ids.has("caught"));
  });

  it("try_catch records catch output when try fails", async () => {
    const seq = testSequence("co-trycatch-fail", [
      {
        type: "try_catch",
        id: "tc",
        try_block: [step("bad", "fail", { message: "err" })],
        catch_block: [step("rescued", "log", { message: "saved" })],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
    assert.ok(ids.has("rescued"));
  });

  it("finally output is present on both success and failure paths", async () => {
    // Success path
    const ok = testSequence("co-finally-ok", [
      {
        type: "try_catch",
        id: "tc",
        try_block: [step("t1", "log", { message: "ok" })],
        catch_block: [],
        finally_block: [step("fin", "log", { message: "fin" })],
      } as Block,
    ]);
    await client.createSequence(ok);
    const { id: okId } = await client.createInstance({
      sequence_id: ok.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(okId, "completed", { timeoutMs: 10_000 });
    const okIds = new Set((await client.getOutputs(okId)).map((o) => o.block_id));
    assert.ok(okIds.has("fin"), "finally must run on success");

    // Failure path
    const bad = testSequence("co-finally-bad", [
      {
        type: "try_catch",
        id: "tc",
        try_block: [step("t1", "fail", { message: "boom" })],
        catch_block: [step("c1", "log", { message: "c" })],
        finally_block: [step("fin2", "log", { message: "fin" })],
      } as Block,
    ]);
    await client.createSequence(bad);
    const { id: badId } = await client.createInstance({
      sequence_id: bad.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(badId, ["completed", "failed"], { timeoutMs: 10_000 });
    const badIds = new Set((await client.getOutputs(badId)).map((o) => o.block_id));
    assert.ok(badIds.has("fin2"), "finally must run after catch handled");
  });
});
