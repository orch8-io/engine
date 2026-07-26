/**
 * Cancel-vs-finally protection: a `cancel` signal delivered while a
 * try_catch `finally` branch is executing must be DEFERRED — the finally
 * branch runs to completion before the instance transitions to `cancelled`.
 *
 * Engine reference: `orch8-engine/src/signals.rs`:
 *   - `SignalAction::Cancel` (~L142-159): scoped cancellation defers the
 *     full instance cancel while non-cancellable nodes are still active.
 *   - `cancel_scoped` (~L332-335): nodes inside a try_catch finally branch
 *     (branch_index == 2) are treated as non-cancellable, mirroring
 *     Java/Python try-finally semantics.
 * Unit-pinned in `orch8-engine/src/signals_coverage_tests.rs`.
 *
 * Second case covers the sibling mechanism: `cancellation_scope` children
 * are likewise shielded (`orch8-types/src/sequence.rs::CancellationScopeDef`).
 *
 * Timing note: the server runs with a 100ms tick; the finally/scope sleep
 * steps (2000ms) give a wide window around the signal so assertions stay
 * tolerant of tick granularity.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

/** One node of the execution tree, loosely typed (matches getInstanceTree). */
interface TreeNode {
  block_id: string;
  state: string;
  [k: string]: unknown;
}

async function getTree(instanceId: string): Promise<TreeNode[]> {
  return (await client.getInstanceTree(instanceId)) as unknown as TreeNode[];
}

/**
 * Poll the execution tree until the node for `blockId` reaches one of the
 * target states. Returns the node; throws on timeout.
 */
async function waitForNodeState(
  instanceId: string,
  blockId: string,
  targetStates: string[],
  { timeoutMs = 10_000, intervalMs = 50 }: { timeoutMs?: number; intervalMs?: number } = {},
): Promise<TreeNode> {
  const deadline = Date.now() + timeoutMs;
  let lastTree: TreeNode[] = [];
  while (Date.now() < deadline) {
    lastTree = await getTree(instanceId);
    const node = lastTree.find((n) => n.block_id === blockId);
    if (node && targetStates.includes(node.state)) return node;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  const node = lastTree.find((n) => n.block_id === blockId);
  throw new Error(
    `Timeout waiting for node ${blockId} to reach [${targetStates}]. ` +
      `Current: ${node ? node.state : "absent"} (tree: ${JSON.stringify(lastTree.map((n) => [n.block_id, n.state]))})`,
  );
}

describe("Cancel signal deferral during finally / cancellation_scope", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("defers cancel while try_catch finally branch is executing", async () => {
    const tenantId = `finprot-${uuid().slice(0, 8)}`;
    const namespace = "default";

    // try fails immediately → catch runs (noop) → finally runs a slow sleep.
    // The cancel signal lands mid-finally and must be deferred until the
    // finally sleep completes.
    const seq = testSequence(
      "cancel-finally",
      [
        {
          type: "try_catch",
          id: "tc",
          try_block: [step("t", "fail", { message: "boom" })],
          catch_block: [step("c", "noop")],
          finally_block: [step("fin", "sleep", { duration_ms: 2000 })],
        } as Block,
      ],
      { tenantId, namespace },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace,
    });

    // Wait until the finally sleep is actually executing.
    await waitForNodeState(id, "fin", ["running"]);

    await client.sendSignal(id, "cancel");

    // The cancel must NOT take effect while finally is running. Check a few
    // ticks past the 100ms scheduler tick but well inside the 2s sleep.
    await new Promise((r) => setTimeout(r, 500));
    const during = await client.getInstance(id);
    assert.notEqual(
      during.state,
      "cancelled",
      `instance must not be cancelled while finally is running, got ${during.state}`,
    );
    const finDuring = (await getTree(id)).find((n) => n.block_id === "fin");
    assert.ok(finDuring, "finally node must exist in the tree");
    assert.notEqual(
      finDuring!.state,
      "cancelled",
      `finally node must not be cancelled, got ${finDuring!.state}`,
    );

    // The finally branch runs to completion...
    const finDone = await waitForNodeState(id, "fin", ["completed"], {
      timeoutMs: 10_000,
    });
    assert.equal(finDone.state, "completed");

    // ...and only then does the deferred cancel land.
    const final = await client.waitForState(id, "cancelled", { timeoutMs: 15_000 });
    assert.equal(final.state, "cancelled");
  });

  it("defers cancel while a cancellation_scope child is executing", async () => {
    const tenantId = `csprot-${uuid().slice(0, 8)}`;
    const namespace = "default";

    const seq = testSequence(
      "cancel-scope",
      [
        {
          type: "cancellation_scope",
          id: "cs",
          blocks: [step("inner", "sleep", { duration_ms: 2000 })],
        } as Block,
      ],
      { tenantId, namespace },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace,
    });

    // Wait until the scoped sleep is executing.
    await waitForNodeState(id, "inner", ["running"]);

    await client.sendSignal(id, "cancel");

    // The scope child is shielded: neither it nor the instance may be
    // cancelled while it is still running.
    await new Promise((r) => setTimeout(r, 500));
    const during = await client.getInstance(id);
    assert.notEqual(
      during.state,
      "cancelled",
      `instance must not be cancelled while scope child is running, got ${during.state}`,
    );
    const innerDuring = (await getTree(id)).find((n) => n.block_id === "inner");
    assert.ok(innerDuring, "scope child node must exist in the tree");
    assert.notEqual(
      innerDuring!.state,
      "cancelled",
      `scope child must not be cancelled, got ${innerDuring!.state}`,
    );

    // The scoped child completes and produces its output.
    const innerDone = await waitForNodeState(id, "inner", ["completed"], {
      timeoutMs: 10_000,
    });
    assert.equal(innerDone.state, "completed");

    // Once the scope drains, the deferred cancel is applied (or, if the
    // engine absorbed it post-scope, the instance simply completes — either
    // terminal state is acceptable; the shielding is the behavior under test).
    const final = await client.waitForState(id, ["cancelled", "completed"], {
      timeoutMs: 15_000,
    });
    assert.ok(
      ["cancelled", "completed"].includes(final.state),
      `expected terminal state after scope drains, got ${final.state}`,
    );

    const outputs = await client.getOutputs(id);
    assert.ok(
      outputs.some((o) => o.block_id === "inner"),
      "scope child output must exist despite the cancel signal",
    );
  });
});
