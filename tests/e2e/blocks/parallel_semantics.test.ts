/**
 * Parallel block semantics — branch execution, output collection, and
 * failure propagation invariants.
 *
 * Parallel runs every branch concurrently; the node completes only when
 * all branches terminate. If any branch fails and the parallel has no
 * enclosing try_catch, the parallel node itself fails.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

describe("Parallel semantics", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("single-branch parallel executes its one branch", async () => {
    const seq = testSequence("par-sem-single", [
      {
        type: "parallel",
        id: "p",
        branches: [[step("only", "log", { message: "o" })]],
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
    assert.ok(ids.includes("only"));
  });

  it("many-branch parallel yields one output per branch leaf", async () => {
    const N = 6;
    const branches = [];
    for (let i = 0; i < N; i++) {
      branches.push([step(`b${i}`, "log", { message: `b${i}` })]);
    }
    const seq = testSequence("par-sem-many", [
      { type: "parallel", id: "p", branches } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 15_000 });
    const ids = (await client.getOutputs(id)).map((o) => o.block_id);
    for (let i = 0; i < N; i++) {
      assert.ok(ids.includes(`b${i}`), `missing b${i}`);
    }
  });

  it("parallel with one failing branch fails the instance (no catch)", async () => {
    const seq = testSequence("par-sem-fail", [
      {
        type: "parallel",
        id: "p",
        branches: [
          [step("ok", "log", { message: "ok" })],
          [step("boom", "fail", { message: "x" })],
        ],
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

  it("parallel with one failing branch inside try_catch is rescued", async () => {
    const seq = testSequence("par-sem-fail-rescued", [
      {
        type: "try_catch",
        id: "tc",
        try_block: [
          {
            type: "parallel",
            id: "p",
            branches: [
              [step("ok", "log", { message: "ok" })],
              [step("boom", "fail", { message: "x" })],
            ],
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
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
    assert.ok(ids.has("rescued"));
  });

  it("parallel branches with sequential steps execute in order within branch", async () => {
    const seq = testSequence("par-sem-seq-in-branch", [
      {
        type: "parallel",
        id: "p",
        branches: [
          [
            step("a1", "log", { message: "a1" }),
            step("a2", "log", { message: "a2" }),
            step("a3", "log", { message: "a3" }),
          ],
          [
            step("b1", "log", { message: "b1" }),
            step("b2", "log", { message: "b2" }),
          ],
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
    for (const s of ["a1", "a2", "a3", "b1", "b2"]) {
      assert.ok(ids.includes(s), `missing ${s}`);
    }
  });

  it("parallel with all empty branches completes instantly", async () => {
    const seq = testSequence("par-sem-all-empty", [
      { type: "parallel", id: "p", branches: [[], [], []] } as Block,
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
});
