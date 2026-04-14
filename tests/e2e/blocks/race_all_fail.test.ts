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

    // Poll each branch in a loop until its worker task appears, then fail it.
    // `waitForState(["waiting", "running"])` can return before the engine has
    // created the per-branch tasks — polling unconditionally at that instant
    // races the tick loop. Loop with a deadline, bailing early if the
    // instance is already terminal.
    const grabAndFail = async (handler: string, reason: string) => {
      const deadline = Date.now() + 5000;
      while (Date.now() < deadline) {
        const tasks = await client.pollWorkerTasks(handler, "worker-1");
        if (tasks.length > 0) {
          await client.failWorkerTask(tasks[0]!.id, "worker-1", reason, false);
          return true;
        }
        const inst = await client.getInstance(id);
        if (inst.state === "failed" || inst.state === "completed") return false;
        await new Promise((r) => setTimeout(r, 50));
      }
      return false;
    };

    await grabAndFail("race_af_a", "a failed");
    await grabAndFail("race_af_b", "b failed");

    const failed = await client.waitForState(id, "failed", { timeoutMs: 10_000 });
    assert.equal(failed.state, "failed");
  });
});
