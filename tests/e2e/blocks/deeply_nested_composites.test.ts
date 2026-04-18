/**
 * Verifies 4-level composite nesting: ForEach → Parallel → TryCatch → Router.
 * For each item, a Parallel runs. One branch is a TryCatch whose try_block is a
 * Router that picks a branch based on context; the other branch is a plain step.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

function parallel(id: string, branches: unknown[]): Record<string, unknown> {
  return { type: "parallel", id, branches };
}

function router(
  id: string,
  routes: unknown[],
  defaultBlocks?: unknown,
): Record<string, unknown> {
  const block: Record<string, unknown> = { type: "router", id, routes };
  if (defaultBlocks) block.default = defaultBlocks;
  return block;
}

function tryCatch(
  id: string,
  tryBlock: unknown[],
  catchBlock: unknown[],
  finallyBlock?: unknown[],
): Record<string, unknown> {
  const block: Record<string, unknown> = {
    type: "try_catch",
    id,
    try_block: tryBlock,
    catch_block: catchBlock,
  };
  if (finallyBlock) block.finally_block = finallyBlock;
  return block;
}

function forEach(
  id: string,
  collection: string,
  body: unknown[],
  opts: Record<string, unknown> = {},
): Record<string, unknown> {
  return { type: "for_each", id, collection, body, ...opts };
}

describe("Deeply nested composites: ForEach > Parallel > TryCatch > Router", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("runs the nested router inside try_block across forEach iterations", async () => {
    const innerRouter = router(
      "rt",
      [
        {
          condition: 'mode == "fast"',
          blocks: [step("fast_pick", "log", { message: "fast" })],
        },
      ],
      [step("default_pick", "log", { message: "def" })],
    );

    const innerTryCatch = tryCatch(
      "tc",
      [innerRouter],
      [step("caught", "log", { message: "caught" })],
    );

    const seq = testSequence("deep-nest", [
      forEach("fe", "items", [
        parallel("p", [
          [innerTryCatch],
          [step("branch_b", "log", { message: "B" })],
        ]),
      ]) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { items: ["x", "y"], mode: "fast" } },
    });

    const completed = await client.waitForState(id, "completed", {
      timeoutMs: 20_000,
    });
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);

    assert.ok(
      blockIds.includes("fast_pick"),
      "router's fast_pick should run (mode=fast)",
    );
    assert.ok(
      blockIds.includes("branch_b"),
      "parallel branch B plain step should run",
    );
    assert.ok(
      !blockIds.includes("default_pick"),
      "router default_pick should NOT run when fast route matched",
    );
    assert.ok(
      !blockIds.includes("caught"),
      "try_catch catch should NOT run when router try succeeds",
    );
  });
});
