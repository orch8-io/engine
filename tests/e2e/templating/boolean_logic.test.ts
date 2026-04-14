/**
 * Boolean logic in expression grammar — verifies that truthy/falsy
 * coercion, short-circuit, negation, parentheses, and mixed operators
 * behave correctly inside router conditions.
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

async function runWithCtx(
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
  const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
  return ids.has("hit");
}

describe("Boolean logic in conditions", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("literal true always matches", async () => {
    assert.equal(await runWithCtx("bl-lit-true", "true", {}), true);
  });

  it("literal false never matches", async () => {
    assert.equal(await runWithCtx("bl-lit-false", "false", {}), false);
  });

  it("A && B: both true -> true", async () => {
    assert.equal(
      await runWithCtx("bl-and-tt", "context.data.a && context.data.b", {
        a: true,
        b: true,
      }),
      true,
    );
  });

  it("A && B: one false -> false", async () => {
    assert.equal(
      await runWithCtx("bl-and-tf", "context.data.a && context.data.b", {
        a: true,
        b: false,
      }),
      false,
    );
  });

  it("A || B: at least one true -> true", async () => {
    assert.equal(
      await runWithCtx("bl-or-tf", "context.data.a || context.data.b", {
        a: false,
        b: true,
      }),
      true,
    );
  });

  it("A || B: both false -> false", async () => {
    assert.equal(
      await runWithCtx("bl-or-ff", "context.data.a || context.data.b", {
        a: false,
        b: false,
      }),
      false,
    );
  });

  it("!A: flips boolean", async () => {
    assert.equal(await runWithCtx("bl-not-t", "!context.data.x", { x: false }), true);
    assert.equal(await runWithCtx("bl-not-f", "!context.data.x", { x: true }), false);
  });

  it("parenthesized combo: (A || B) && C", async () => {
    const cond = "(context.data.a || context.data.b) && context.data.c";
    assert.equal(
      await runWithCtx("bl-par-1", cond, { a: false, b: true, c: true }),
      true,
    );
    assert.equal(
      await runWithCtx("bl-par-2", cond, { a: false, b: true, c: false }),
      false,
    );
  });

  it("truthy coercion: non-empty string is truthy", async () => {
    assert.equal(await runWithCtx("bl-str-truthy", "context.data.s", { s: "hi" }), true);
    assert.equal(await runWithCtx("bl-str-empty", "context.data.s", { s: "" }), false);
  });

  it("truthy coercion: non-zero number is truthy, zero is falsy", async () => {
    assert.equal(await runWithCtx("bl-num-nonzero", "context.data.n", { n: 7 }), true);
    assert.equal(await runWithCtx("bl-num-zero", "context.data.n", { n: 0 }), false);
  });

  it("null value is falsy", async () => {
    assert.equal(await runWithCtx("bl-null", "context.data.v", { v: null }), false);
  });
});
