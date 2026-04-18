/**
 * Race where every branch fails.
 *
 * Verifies that when all branches of a race block fail permanently, the race
 * block itself fails and the instance terminates in state "failed".
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

function race(
  id: string,
  branches: unknown[],
  semantics?: string,
): Record<string, unknown> {
  const block: Record<string, unknown> = { type: "race", id, branches };
  if (semantics) block.semantics = semantics;
  return block;
}

describe("Race: all branches fail", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("race fails when every branch fails permanently", async () => {
    const seq = testSequence("race-all-fail", [
      race(
        "r1",
        [
          [step("f1", "race_af_a", {})],
          [step("f2", "race_af_b", {})],
        ],
        "first_to_succeed",
      ) as Block,
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, ["waiting", "running"], { timeoutMs: 5000 });

    // Fail branch A permanently.
    const tasksA = await client.pollWorkerTasks("race_af_a", "worker-1");
    if (tasksA.length > 0) {
      await client.failWorkerTask(tasksA[0]!.id, "worker-1", "a failed", false);
    }

    // Branch B may still be pending. The instance may briefly transition back
    // to waiting while B runs (or already be failed if branches are sequential).
    await client.waitForState(id, ["waiting", "running", "failed"], {
      timeoutMs: 5000,
    });

    // Poll up to a short deadline to grab B's task if it exists.
    let tasksB = await client.pollWorkerTasks("race_af_b", "worker-1");
    const deadline = Date.now() + 3000;
    while (tasksB.length === 0 && Date.now() < deadline) {
      const inst = await client.getInstance(id);
      if (inst.state === "failed") break;
      await new Promise((r) => setTimeout(r, 50));
      tasksB = await client.pollWorkerTasks("race_af_b", "worker-1");
    }
    if (tasksB.length > 0) {
      await client.failWorkerTask(tasksB[0]!.id, "worker-1", "b failed", false);
    }

    const failed = await client.waitForState(id, "failed", { timeoutMs: 10_000 });
    assert.equal(failed.state, "failed");
  });
});
