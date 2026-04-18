/**
 * Dynamic Step Injection — verifies that a step using the `self_modify`
 * built-in handler can splice new blocks into its own running instance and
 * those blocks execute as part of the same run with their output appearing
 * in the instance's block outputs.
 *
 * Engine contract (`orch8-engine/src/handlers/self_modify.rs` +
 * `orch8-engine/src/handlers/step_block.rs`):
 *   - A step with handler `self_modify` and `params.blocks = [...]` returns
 *     a special `{ _self_modify: true, blocks, position, injected_count }`
 *     output; the step_block executor detects `_self_modify` and calls
 *     `storage.inject_blocks(...)` so the evaluator picks them up on the
 *     next tick.
 *   - `params.position` is optional; when omitted the injected blocks are
 *     appended at the end of the current block list.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Dynamic Step Injection", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("injects new steps into a running instance via self_modify", async () => {
    const tenantId = `inj-${uuid().slice(0, 8)}`;
    const injectedBlocks = [
      {
        type: "step",
        id: "injected_1",
        handler: "log",
        params: { message: "injected one" },
      },
      {
        type: "step",
        id: "injected_2",
        handler: "log",
        params: { message: "injected two" },
      },
    ];

    const seq = testSequence(
      "self-modify-inject",
      [
        step("planner", "self_modify", { blocks: injectedBlocks }),
        // A trailing anchor step proves the instance keeps making progress
        // after injection on top of the injected work.
        step("anchor", "noop"),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    const final = await client.waitForState(id, "completed", { timeoutMs: 20_000 });
    assert.equal(final.state, "completed");

    const outputs = await client.getOutputs(id);
    const ids = outputs.map((o) => o.block_id);
    assert.ok(ids.includes("planner"), "planner step should have run");
    assert.ok(ids.includes("anchor"), "anchor step should have run");
    assert.ok(ids.includes("injected_1"), "injected_1 should have executed");
    assert.ok(ids.includes("injected_2"), "injected_2 should have executed");
  });
});
