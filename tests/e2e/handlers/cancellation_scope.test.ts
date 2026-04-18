/**
 * cancellation_scope block: children inside the scope continue even when the
 * instance receives an external `cancel` signal.
 *
 * Engine reference: `orch8-engine/src/handlers/cancellation_scope.rs`:
 *   "External cancel signals do NOT propagate into children — the scope
 *    completes normally even if the parent instance receives a cancel
 *    request."
 *
 * Block serialization: `{ type: "cancellation_scope", id, blocks: [...] }`
 * (see `orch8-types/src/sequence.rs::CancellationScopeDef`).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

function cancellationScope(id: string, blocks: unknown[]): Record<string, unknown> {
  return { type: "cancellation_scope", id, blocks };
}

describe("cancellation_scope Handler", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("protects child blocks from external cancel signals", async () => {
    const tenantId = `cs-${uuid().slice(0, 8)}`;
    const namespace = "default";

    // A short sleep (so the test stays fast) wrapped in a cancellation_scope.
    // A cancel signal fires while the child is still running. Per the engine
    // contract, the child should still produce its output.
    const seq = testSequence(
      "cs-shield",
      [
        cancellationScope("cs1", [
          step("inner", "sleep", { duration_ms: 800 }),
        ]) as Block,
      ],
      { tenantId, namespace },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace,
    });

    // Let the scope's inner sleep start.
    await new Promise((r) => setTimeout(r, 200));
    await client.sendSignal(id, "cancel");

    // Scope children must complete regardless of the cancel signal. Once the
    // scope finishes, the instance reaches a terminal state (completed if
    // cancel was absorbed post-scope, or cancelled if the engine applies the
    // deferred cancel at the end — either way the scope's child output exists).
    const final = await client.waitForState(id, ["completed", "cancelled", "failed"], {
      timeoutMs: 10_000,
    });
    assert.notEqual(final.state, "failed", `scope should not fail: ${final.state}`);

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(
      blockIds.includes("inner"),
      `inner step should have produced output despite cancel; got blocks: ${blockIds.join(",")}`,
    );
  });
});
