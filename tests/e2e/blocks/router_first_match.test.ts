/**
 * Router first-match semantics.
 *
 * The router evaluates `routes` in declaration order and selects exactly
 * the FIRST one whose `condition` is truthy. Subsequent matches, even if
 * syntactically present, are not executed. The `default` array runs only
 * when no route matches.
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

describe("Router first-match semantics", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("chooses the first of multiple matching routes", async () => {
    const seq = testSequence("rt-first-wins", [
      router("rt", [
        { condition: "true", blocks: [step("first", "log", { message: "1" })] },
        { condition: "true", blocks: [step("second", "log", { message: "2" })] },
        { condition: "true", blocks: [step("third", "log", { message: "3" })] },
      ]) as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
    assert.ok(ids.has("first"));
    assert.ok(!ids.has("second"));
    assert.ok(!ids.has("third"));
  });

  it("skips earlier false routes and picks the first true one", async () => {
    const seq = testSequence("rt-skip-false", [
      router("rt", [
        { condition: "false", blocks: [step("a", "log", { message: "a" })] },
        { condition: "false", blocks: [step("b", "log", { message: "b" })] },
        { condition: "true", blocks: [step("c", "log", { message: "c" })] },
        { condition: "true", blocks: [step("d", "log", { message: "d" })] },
      ]) as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
    assert.ok(!ids.has("a"));
    assert.ok(!ids.has("b"));
    assert.ok(ids.has("c"));
    assert.ok(!ids.has("d"));
  });

  it("falls back to default when no route matches", async () => {
    const seq = testSequence("rt-default", [
      router(
        "rt",
        [
          {
            condition: "context.data.tier == \"x\"",
            blocks: [step("x", "log", { message: "x" })],
          },
          {
            condition: "context.data.tier == \"y\"",
            blocks: [step("y", "log", { message: "y" })],
          },
        ],
        [step("fallback", "log", { message: "fallback" })],
      ) as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { tier: "z" } },
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
    assert.ok(ids.has("fallback"));
    assert.ok(!ids.has("x"));
    assert.ok(!ids.has("y"));
  });

  it("default is NOT run when any route matches", async () => {
    const seq = testSequence("rt-no-default-when-match", [
      router(
        "rt",
        [{ condition: "true", blocks: [step("hit", "log", { message: "h" })] }],
        [step("def", "log", { message: "d" })],
      ) as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
    assert.ok(ids.has("hit"));
    assert.ok(!ids.has("def"));
  });

  it("condition reading nested context path selects the right route", async () => {
    const seq = testSequence("rt-nested-path", [
      router(
        "rt",
        [
          {
            condition: "context.data.user.role == \"admin\"",
            blocks: [step("admin", "log", { message: "admin" })],
          },
          {
            condition: "context.data.user.role == \"viewer\"",
            blocks: [step("viewer", "log", { message: "viewer" })],
          },
        ],
        [step("anon", "log", { message: "anon" })],
      ) as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { user: { role: "viewer" } } },
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
    assert.ok(ids.has("viewer"));
    assert.ok(!ids.has("admin"));
    assert.ok(!ids.has("anon"));
  });
});
