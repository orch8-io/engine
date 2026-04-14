/**
 * Try/catch/finally semantics — exhaustive coverage of the decision
 * matrix: try-succeeds × try-fails × catch-succeeds × catch-fails ×
 * finally-present × finally-absent. These are E2E counterparts to the
 * orch8-engine try_catch.rs unit tests, asserting the same behavior at
 * the instance-state layer.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

describe("try_catch semantics", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("try succeeds → instance completes, catch skipped", async () => {
    const seq = testSequence("tc-sem-ok", [
      {
        type: "try_catch",
        id: "tc",
        try_block: [step("t", "log", { message: "ok" })],
        catch_block: [step("c", "log", { message: "c" })],
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
    assert.ok(!ids.has("c"));
  });

  it("try fails + catch succeeds → instance completes", async () => {
    const seq = testSequence("tc-sem-rescue", [
      {
        type: "try_catch",
        id: "tc",
        try_block: [step("t", "fail", { message: "err" })],
        catch_block: [step("c", "log", { message: "c" })],
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
  });

  it("try fails + catch fails → instance fails", async () => {
    const seq = testSequence("tc-sem-catch-fail", [
      {
        type: "try_catch",
        id: "tc",
        try_block: [step("t", "fail", { message: "err" })],
        catch_block: [step("c", "fail", { message: "catch err" })],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    const done = await client.waitForState(id, ["failed", "completed"], {
      timeoutMs: 10_000,
    });
    assert.equal(done.state, "failed");
  });

  it("try succeeds + finally present → both run, instance completes", async () => {
    const seq = testSequence("tc-sem-fin-ok", [
      {
        type: "try_catch",
        id: "tc",
        try_block: [step("t", "log", { message: "t" })],
        catch_block: [],
        finally_block: [step("f", "log", { message: "f" })],
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
    assert.ok(ids.has("t"));
    assert.ok(ids.has("f"));
  });

  it("multiple try steps where last fails → catch runs", async () => {
    const seq = testSequence("tc-sem-mid-fail", [
      {
        type: "try_catch",
        id: "tc",
        try_block: [
          step("t1", "log", { message: "1" }),
          step("t2", "log", { message: "2" }),
          step("t3", "fail", { message: "bad" }),
        ],
        catch_block: [step("c", "log", { message: "rescued" })],
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
    assert.ok(ids.has("t1"));
    assert.ok(ids.has("t2"));
    assert.ok(ids.has("c"));
  });

  it("catch has multiple steps, all execute on try failure", async () => {
    const seq = testSequence("tc-sem-multi-catch", [
      {
        type: "try_catch",
        id: "tc",
        try_block: [step("t", "fail", { message: "x" })],
        catch_block: [
          step("c1", "log", { message: "1" }),
          step("c2", "log", { message: "2" }),
          step("c3", "log", { message: "3" }),
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
    const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
    for (const c of ["c1", "c2", "c3"]) assert.ok(ids.has(c), `missing ${c}`);
  });

  it("nested try_catch: inner catch absorbs failure, outer completes", async () => {
    const seq = testSequence("tc-sem-nested", [
      {
        type: "try_catch",
        id: "outer",
        try_block: [
          {
            type: "try_catch",
            id: "inner",
            try_block: [step("t", "fail", { message: "x" })],
            catch_block: [step("inner_c", "log", { message: "i" })],
          },
        ],
        catch_block: [step("outer_c", "log", { message: "o" })],
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
    assert.ok(ids.has("inner_c"));
    assert.ok(!ids.has("outer_c"));
  });

  it("nested try_catch: inner catch also fails, outer catch handles", async () => {
    const seq = testSequence("tc-sem-nested-bubble", [
      {
        type: "try_catch",
        id: "outer",
        try_block: [
          {
            type: "try_catch",
            id: "inner",
            try_block: [step("t", "fail", { message: "x" })],
            catch_block: [step("ic", "fail", { message: "y" })],
          },
        ],
        catch_block: [step("oc", "log", { message: "o" })],
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
    assert.ok(ids.has("oc"));
  });
});
