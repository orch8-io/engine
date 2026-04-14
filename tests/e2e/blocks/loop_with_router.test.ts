/**
 * Verifies that a Loop block whose body is a Router evaluates the router's
 * conditions each iteration and dispatches to the matching branch.
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

function loop(
  id: string,
  condition: string,
  body: unknown[],
  opts: Record<string, unknown> = {},
): Record<string, unknown> {
  return { type: "loop", id, condition, body, ...opts };
}

describe("Loop with Router body", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("loop exits before body when condition false", async () => {
    const seq = testSequence("loop-router-skip", [
      loop(
        "lp",
        "context.data.keep_going",
        [
          router(
            "rt",
            [
              {
                condition: 'branch == "x"',
                blocks: [step("x_step", "log", { message: "x" })],
              },
            ],
            [step("y_step", "log", { message: "y" })],
          ),
        ],
        { max_iterations: 2 },
      ) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { keep_going: false, branch: "x" } },
    });

    const completed = await client.waitForState(id, "completed", {
      timeoutMs: 10_000,
    });
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(
      !blockIds.includes("x_step"),
      "x_step should NOT run when loop condition is false",
    );
    assert.ok(
      !blockIds.includes("y_step"),
      "y_step should NOT run when loop condition is false",
    );
  });

  it("loop runs router body each iteration until max_iterations", async () => {
    const seq = testSequence("loop-router-iter", [
      loop(
        "lp",
        "context.data.keep_going",
        [
          router(
            "rt",
            [
              {
                condition: 'branch == "x"',
                blocks: [step("x_step", "log", { message: "x" })],
              },
            ],
            [step("y_step", "log", { message: "y" })],
          ),
        ],
        { max_iterations: 2 },
      ) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { keep_going: true, branch: "x" } },
    });

    const completed = await client.waitForState(id, "completed", {
      timeoutMs: 15_000,
    });
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(
      blockIds.includes("x_step"),
      "x_step should run when router's branch==x condition matches",
    );
    assert.ok(
      !blockIds.includes("y_step"),
      "y_step default should NOT run when branch==x matched",
    );
  });
});
