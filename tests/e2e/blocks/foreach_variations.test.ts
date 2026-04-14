/**
 * ForEach variations — different collection shapes and item_var bindings.
 *
 * The ForEach handler binds each collection element to `context.data.<item_var>`
 * (default "item") and iterates the body for each one.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

describe("ForEach variations", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("iterates over a numeric collection", async () => {
    const seq = testSequence("fe-var-num", [
      {
        type: "for_each",
        id: "fe",
        collection: "nums",
        body: [step("body", "log", { message: "{{context.data.item}}" })],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { nums: [1, 2, 3, 4, 5] } },
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const bodies = (await client.getOutputs(id)).filter((o) => o.block_id === "body");
    assert.equal(bodies.length, 5);
  });

  it("iterates over a string collection", async () => {
    const seq = testSequence("fe-var-str", [
      {
        type: "for_each",
        id: "fe",
        collection: "strs",
        body: [step("body", "log", { message: "s={{context.data.item}}" })],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { strs: ["alpha", "beta", "gamma"] } },
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const bodies = (await client.getOutputs(id)).filter((o) => o.block_id === "body");
    assert.equal(bodies.length, 3);
  });

  it("iterates over an array of objects with nested field access", async () => {
    const seq = testSequence("fe-var-obj", [
      {
        type: "for_each",
        id: "fe",
        collection: "users",
        body: [
          step("body", "log", {
            message: "name={{context.data.item.name}}",
          }),
        ],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: {
        data: {
          users: [
            { name: "ada", id: 1 },
            { name: "bob", id: 2 },
          ],
        },
      },
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const bodies = (await client.getOutputs(id)).filter((o) => o.block_id === "body");
    assert.equal(bodies.length, 2);
  });

  it("custom item_var name is respected in body templates", async () => {
    const seq = testSequence("fe-var-itemvar", [
      {
        type: "for_each",
        id: "fe",
        collection: "items",
        item_var: "thing",
        body: [step("body", "log", { message: "t={{context.data.thing}}" })],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { items: ["x", "y"] } },
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const bodies = (await client.getOutputs(id)).filter((o) => o.block_id === "body");
    assert.equal(bodies.length, 2);
    const messages = bodies
      .map((o) => (o.output as { message: string }).message)
      .sort();
    assert.deepEqual(messages, ["t=x", "t=y"]);
  });

  it("single-item collection runs body exactly once", async () => {
    const seq = testSequence("fe-var-single", [
      {
        type: "for_each",
        id: "fe",
        collection: "items",
        body: [step("body", "log", { message: "once" })],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { items: ["only"] } },
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const bodies = (await client.getOutputs(id)).filter((o) => o.block_id === "body");
    assert.equal(bodies.length, 1);
  });

  it("for_each inside try_catch absorbs a failing iteration", async () => {
    // for_each body fails on every iteration; catch handles it.
    const seq = testSequence("fe-var-in-tc", [
      {
        type: "try_catch",
        id: "tc",
        try_block: [
          {
            type: "for_each",
            id: "fe",
            collection: "items",
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
      context: { data: { items: ["a"] } },
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
    assert.ok(ids.has("rescued"));
  });
});
