/**
 * Deep composite chaining — verifies 3+ levels of nesting terminate and
 * produce the expected subset of outputs.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

describe("Deep composite chains", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("parallel inside parallel inside parallel", async () => {
    const seq = testSequence("dc-pp-pp", [
      {
        type: "parallel",
        id: "outer",
        branches: [
          [
            {
              type: "parallel",
              id: "mid",
              branches: [
                [
                  {
                    type: "parallel",
                    id: "inner",
                    branches: [
                      [step("leaf_a", "log", { message: "a" })],
                      [step("leaf_b", "log", { message: "b" })],
                    ],
                  },
                ],
              ],
            },
          ],
          [step("side", "log", { message: "side" })],
        ],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 15_000 });
    const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
    for (const leaf of ["leaf_a", "leaf_b", "side"]) {
      assert.ok(ids.has(leaf), `missing ${leaf}`);
    }
  });

  it("router inside router inside router picks leaf by context", async () => {
    const seq = testSequence("dc-rrr", [
      {
        type: "router",
        id: "l1",
        routes: [
          {
            condition: "context.data.l1 == \"A\"",
            blocks: [
              {
                type: "router",
                id: "l2",
                routes: [
                  {
                    condition: "context.data.l2 == \"X\"",
                    blocks: [
                      {
                        type: "router",
                        id: "l3",
                        routes: [
                          {
                            condition: "context.data.l3 == \"1\"",
                            blocks: [step("leaf", "log", { message: "leaf" })],
                          },
                        ],
                        default: [step("l3_def", "log", { message: "3d" })],
                      },
                    ],
                  },
                ],
                default: [step("l2_def", "log", { message: "2d" })],
              },
            ],
          },
        ],
        default: [step("l1_def", "log", { message: "1d" })],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { l1: "A", l2: "X", l3: "1" } },
    });
    await client.waitForState(id, "completed", { timeoutMs: 15_000 });
    const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
    assert.ok(ids.has("leaf"));
    assert.ok(!ids.has("l1_def"));
    assert.ok(!ids.has("l2_def"));
    assert.ok(!ids.has("l3_def"));
  });

  it("bounded loop inside for_each inside parallel terminates", async () => {
    const seq = testSequence("dc-loop-fe-par", [
      {
        type: "parallel",
        id: "p",
        branches: [
          [
            {
              type: "for_each",
              id: "fe",
              collection: "items",
              body: [
                {
                  type: "loop",
                  id: "l",
                  condition: "true",
                  max_iterations: 2,
                  body: [step("tick", "log", { message: "t" })],
                },
              ],
            },
          ],
          [step("side", "log", { message: "s" })],
        ],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { items: ["x", "y"] } },
    });
    await client.waitForState(id, "completed", { timeoutMs: 20_000 });
    const outputs = await client.getOutputs(id);
    // 2 items × 2 loop iterations = 4 tick outputs
    const ticks = outputs.filter((o) => o.block_id === "tick");
    assert.equal(ticks.length, 4);
    assert.ok(outputs.some((o) => o.block_id === "side"));
  });

  it("try_catch inside parallel inside try_catch handles inner failure", async () => {
    const seq = testSequence("dc-tc-par-tc", [
      {
        type: "try_catch",
        id: "outer_tc",
        try_block: [
          {
            type: "parallel",
            id: "p",
            branches: [
              [
                {
                  type: "try_catch",
                  id: "inner_tc",
                  try_block: [step("bad", "fail", { message: "x" })],
                  catch_block: [step("inner_rescue", "log", { message: "r" })],
                },
              ],
              [step("other", "log", { message: "o" })],
            ],
          },
        ],
        catch_block: [step("outer_rescue", "log", { message: "ox" })],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 15_000 });
    const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
    assert.ok(ids.has("inner_rescue"));
    assert.ok(ids.has("other"));
    assert.ok(!ids.has("outer_rescue"), "outer catch must not run when inner handles");
  });

  it("race inside parallel: one branch wins race, siblings still run", async () => {
    const seq = testSequence("dc-race-in-par", [
      {
        type: "parallel",
        id: "p",
        branches: [
          [
            {
              type: "race",
              id: "r",
              branches: [
                [step("fast", "log", { message: "f" })],
                [step("also", "log", { message: "a" })],
              ],
            },
          ],
          [step("sibling", "log", { message: "s" })],
        ],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 15_000 });
    const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
    // Sibling of race must always run
    assert.ok(ids.has("sibling"));
    // At least one race branch must produce output
    assert.ok(ids.has("fast") || ids.has("also"));
  });
});
