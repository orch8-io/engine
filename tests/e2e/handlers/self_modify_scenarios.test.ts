/**
 * self_modify handler — scenarios beyond the basic append in
 * `features/dynamic_step_injection.test.ts`.
 *
 * Engine contract (`orch8-engine/src/handlers/self_modify.rs` +
 * `orch8-engine/src/handlers/param_resolve.rs::apply_self_modify`):
 *   - A `self_modify` step returns `{ _self_modify, blocks, position,
 *     injected_count }`; the executor splices `blocks` into the instance's
 *     `injected_blocks` (appending when `position` is omitted, splicing at
 *     `position` otherwise) and the evaluator runs them on the next tick.
 *   - `blocks` are validated as full `BlockDefinition`s, so composite blocks
 *     (router/loop/…) are valid injection targets — not just leaf steps.
 *   - Invalid block definitions make the step fail Permanent → the instance
 *     lands in `failed`.
 *
 * This suite is tenant-scoped (no triggers), so it runs in shared-server mode.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("self_modify Scenarios", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("chained self_modify across two steps: both injections survive (append does not clobber)", async () => {
    // The append path must merge with prior injected_blocks rather than
    // overwrite them — the agent self-extension pattern depends on it. Two
    // separate self_modify steps each inject one block; both must execute.
    const tenantId = `sm-chain-${uuid().slice(0, 8)}`;

    const seq = testSequence(
      "sm-chain",
      [
        step("plan_a", "self_modify", {
          blocks: [
            { type: "step", id: "from_a", handler: "log", params: { message: "a" } },
          ],
        }),
        step("plan_b", "self_modify", {
          blocks: [
            { type: "step", id: "from_b", handler: "log", params: { message: "b" } },
          ],
        }),
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

    const ids = (await client.getOutputs(id)).map((o) => o.block_id);
    assert.ok(ids.includes("from_a"), "first injection should execute");
    assert.ok(
      ids.includes("from_b"),
      "second injection must NOT clobber the first — both must execute",
    );
    assert.ok(ids.includes("anchor"), "anchor should still run");
  });

  it("injects a composite (router) block that executes its matching route", async () => {
    const tenantId = `sm-composite-${uuid().slice(0, 8)}`;

    const injectedRouter = {
      type: "router",
      id: "injected_router",
      routes: [
        {
          condition: "true",
          blocks: [step("route_hit", "log", { message: "routed" })],
        },
      ],
    };

    const seq = testSequence(
      "sm-composite",
      [
        step("planner", "self_modify", { blocks: [injectedRouter] }),
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

    const ids = (await client.getOutputs(id)).map((o) => o.block_id);
    assert.ok(
      ids.includes("route_hit"),
      "the injected router's matching route step should have executed",
    );
  });

  it("accepts an explicit position param and runs the injected block", async () => {
    const tenantId = `sm-pos-${uuid().slice(0, 8)}`;

    const seq = testSequence(
      "sm-position",
      [
        step("planner", "self_modify", {
          position: 0,
          blocks: [
            { type: "step", id: "positioned", handler: "log", params: { message: "p" } },
          ],
        }),
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
    assert.ok(ids.includes("positioned"), "position:0 injected block should execute");

    // The planner step records the position it injected at.
    const planner = outputs.find((o) => o.block_id === "planner");
    assert.ok(planner, "planner output missing");
    const out = planner!.output as Record<string, unknown>;
    assert.equal(out.position, 0, "planner output should echo position 0");
    assert.equal(out.injected_count, 1);
  });

  it("fails the instance permanently when injected blocks are invalid", async () => {
    const tenantId = `sm-bad-${uuid().slice(0, 8)}`;

    // `blocks` is an array (so it passes the array check) but the element is
    // not a valid BlockDefinition — the handler returns Permanent.
    const seq = testSequence(
      "sm-invalid",
      [
        step("planner", "self_modify", { blocks: [{ not: "a valid block" }] }),
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

    const final = await client.waitForState(id, ["failed", "completed"], {
      timeoutMs: 15_000,
    });
    assert.equal(
      final.state,
      "failed",
      `invalid block defs must fail the instance permanently, got ${final.state}`,
    );
  });
});
