/**
 * Comparison operators in the expression grammar.
 * Covers ==, !=, <, <=, >, >= across numbers and strings.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

function router(
  id: string,
  routes: unknown[],
  defaultBlocks?: unknown,
): Record<string, unknown> {
  const block: Record<string, unknown> = { type: "router", id, routes };
  if (defaultBlocks) block.default = defaultBlocks;
  return block;
}

async function match(
  name: string,
  cond: string,
  data: Record<string, unknown>,
): Promise<boolean> {
  const seq = testSequence(name, [
    router(
      "rt",
      [{ condition: cond, blocks: [step("hit", "log", { message: "y" })] }],
      [step("miss", "log", { message: "n" })],
    ) as Block,
  ]);
  await client.createSequence(seq);
  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: "test",
    namespace: "default",
    context: { data },
  });
  await client.waitForState(id, "completed", { timeoutMs: 10_000 });
  return new Set((await client.getOutputs(id)).map((o) => o.block_id)).has("hit");
}

describe("Comparison operators", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("equality ==", async () => {
    assert.equal(await match("cmp-eq-t", "context.data.x == 5", { x: 5 }), true);
    assert.equal(await match("cmp-eq-f", "context.data.x == 5", { x: 6 }), false);
  });

  it("inequality !=", async () => {
    assert.equal(await match("cmp-ne-t", "context.data.x != 5", { x: 6 }), true);
    assert.equal(await match("cmp-ne-f", "context.data.x != 5", { x: 5 }), false);
  });

  it("less-than <", async () => {
    assert.equal(await match("cmp-lt-t", "context.data.x < 10", { x: 3 }), true);
    assert.equal(await match("cmp-lt-f", "context.data.x < 10", { x: 10 }), false);
  });

  it("less-or-equal <=", async () => {
    assert.equal(await match("cmp-le-t", "context.data.x <= 10", { x: 10 }), true);
    assert.equal(await match("cmp-le-f", "context.data.x <= 10", { x: 11 }), false);
  });

  it("greater-than >", async () => {
    assert.equal(await match("cmp-gt-t", "context.data.x > 10", { x: 11 }), true);
    assert.equal(await match("cmp-gt-f", "context.data.x > 10", { x: 10 }), false);
  });

  it("greater-or-equal >=", async () => {
    assert.equal(await match("cmp-ge-t", "context.data.x >= 10", { x: 10 }), true);
    assert.equal(await match("cmp-ge-f", "context.data.x >= 10", { x: 9 }), false);
  });

  it("string equality", async () => {
    assert.equal(
      await match("cmp-seq-t", "context.data.s == \"hi\"", { s: "hi" }),
      true,
    );
    assert.equal(
      await match("cmp-seq-f", "context.data.s == \"hi\"", { s: "no" }),
      false,
    );
  });

  it("string inequality", async () => {
    assert.equal(
      await match("cmp-sne-t", "context.data.s != \"hi\"", { s: "x" }),
      true,
    );
  });
});
