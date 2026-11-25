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

  for (const scenario of ["scope", "following", "race", "step"] as const) {
    it(`drains a protected wait before cancelling in the ${scenario} workflow`, async () => {
      const tenantId = `cs-${uuid().slice(0, 8)}`;
      const namespace = "default";
      const inner = step("inner", "human_review", {}, {
        cancellable: scenario !== "step",
        wait_for_input: {
          prompt: "Release the protected step?",
          choices: [{ label: "Continue", value: "continue" }],
        },
      });
      const scoped = cancellationScope("cs1", [inner]) as Block;
      const blocks: Block[] = (() => {
        switch (scenario) {
          case "step":
            return [inner];
          case "scope":
            return [scoped];
          case "following":
            return [
              scoped,
              step("following", "noop"),
              cancellationScope("unentered", [step("unentered_step", "noop")]) as Block,
              {
                type: "try_catch",
                id: "unentered_try",
                try_block: [step("unentered_try_step", "noop")],
                catch_block: [],
                finally_block: [step("unentered_cleanup", "noop")],
              } as Block,
            ];
          case "race":
            return [{
              type: "race",
              id: "competition",
              branches: [[scoped], [step("competitor", "cancellation_test_worker")]],
            } as Block];
        }
      })();
      const seq = testSequence("cs-shield", blocks, { tenantId, namespace });
      await client.createSequence(seq);

      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace,
      });

      // The only wait in the workflow is inside the scope.
      await client.waitForState(id, "waiting", { timeoutMs: 10_000 });
      await client.sendSignal(id, "cancel");
      await client.sendSignal(id, "cancel");

      // Signals are processed in order. The marker acknowledges that the
      // scheduler handled cancel before we release the protected child.
      await client.sendSignal(id, "update_context", {
        data: { cancel_processed: true },
        config: {},
        audit: [],
        runtime: {},
      });
      const deadline = Date.now() + 10_000;
      const cancelProcessed = async () => {
        const { context } = await client.getInstance(id);
        const data = context?.data as { cancel_processed?: boolean } | undefined;
        return data?.cancel_processed === true;
      };
      while (!(await cancelProcessed())) {
        assert.ok(Date.now() < deadline, "cancel was not processed while the scope was waiting");
        await new Promise((resolve) => setTimeout(resolve, 25));
      }
      const tree = await client.getInstanceTree(id);
      assert.equal(tree.find((node) => node.block_id === "inner")?.state, "waiting");
      if (scenario === "following" && server && !server.attached) {
        const port = server.port;
        await stopServer(server);
        server = await startServer({ port, skipCleanup: true });
        const restored = await client.getInstanceTree(id);
        assert.equal(restored.find((node) => node.block_id === "inner")?.state, "waiting");
      }
      await client.sendCustomSignal(id, "human_input:inner", { value: "continue" });

      await client.waitForState(id, "cancelled", { timeoutMs: 10_000 });
      if (scenario === "following") {
        const finalTree = await client.getInstanceTree(id);
        assert.equal(finalTree.find((node) => node.block_id === "following")?.state, "cancelled");
        for (const block of ["unentered", "unentered_try", "unentered_cleanup"]) {
          assert.equal(finalTree.find((node) => node.block_id === block)?.state, "cancelled");
        }
      }

      const outputs = await client.getOutputs(id);
      const blockIds = outputs.map((o) => o.block_id);
      assert.ok(
        blockIds.includes("inner"),
        `inner step should have produced output despite cancel; got blocks: ${blockIds.join(",")}`,
      );
    });
  }
});
