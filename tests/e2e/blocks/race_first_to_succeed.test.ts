/**
 * Race with semantics="first_to_succeed".
 *
 * Verifies that a fast failure in one branch does NOT decide the race —
 * the race continues until another branch succeeds (or all branches fail).
 * Contrast with default "first_to_resolve" semantics, where the fast failure
 * would win and the race block would fail.
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

describe("Race: first_to_succeed semantics", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("fast failure does not win; slower success wins", async () => {
    const seq = testSequence("race-fts", [
      race(
        "r1",
        [
          [step("fail_fast", "race_fts_a", {})],
          [step("succeed_slow", "race_fts_b", {})],
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

    // Both external handlers dispatched → instance goes to waiting.
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });

    // Fail branch A permanently — with first_to_succeed, this must NOT end the race.
    const tasksA = await client.pollWorkerTasks("race_fts_a", "worker-1");
    assert.equal(tasksA.length, 1, "expected one task for race_fts_a");
    await client.failWorkerTask(tasksA[0]!.id, "worker-1", "fast fail", false);

    // Branch B may have been dispatched already or still pending — poll until we get it.
    // The race should still be alive while B runs.
    let tasksB = await client.pollWorkerTasks("race_fts_b", "worker-1");
    const deadline = Date.now() + 5000;
    while (tasksB.length === 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 50));
      tasksB = await client.pollWorkerTasks("race_fts_b", "worker-1");
    }
    assert.equal(tasksB.length, 1, "expected one task for race_fts_b");
    await client.completeWorkerTask(tasksB[0]!.id, "worker-1", { winner: "b" });

    const completed = await client.waitForState(id, "completed", {
      timeoutMs: 10_000,
    });
    assert.equal(completed.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(
      blockIds.includes("succeed_slow"),
      "succeed_slow should have produced output as winner",
    );
  });
});
