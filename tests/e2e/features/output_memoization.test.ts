/**
 * Output Memoization — locks in the no-duplicate-output invariant across a
 * mid-instance failure + `retryInstance` cycle.
 *
 * Background from `orch8-engine/src/handlers/step.rs::execute_step`:
 *   - The memoization check only fires when `exec.attempt > 0` AND a matching
 *     BlockOutput already exists. On success the node is marked Completed and
 *     is never re-executed within the same tree traversal.
 *   - There is no public endpoint that forces a Completed node back to
 *     Running (would need a `/blocks/{id}/replay` or node-level state patch).
 *
 * Observable invariant: after
 *   s1 (succeeds) → s2 (fails permanently) → instance=Failed →
 *   `retryInstance` → instance scheduled → s2 fails again → instance=Failed
 * the BlockOutput rows for s1 are unchanged (count and created_at). On the
 * replay traversal the scheduler sees s1 already Completed and does not
 * re-run it. If memoization regressed to a duplicate-on-retry model, s1
 * would emit a second BlockOutput row — this test would catch it.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Output Memoization", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("does not duplicate a Completed block's output after retryInstance", async () => {
    const tenantId = `memo-${uuid().slice(0, 8)}`;
    // s1 succeeds, s2 fails permanently → instance lands in `failed` AFTER
    // s1 has already written its single BlockOutput row.
    const seq = testSequence(
      "memo-seq",
      [
        step("s1", "log", { message: "first-run-marker" }),
        step("s2", "fail", { message: "forced", retryable: false }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "failed");

    // Snapshot s1's outputs before retry.
    const beforeAll = await client.getOutputs(id);
    const beforeS1 = beforeAll.filter((o) => o.block_id === "s1");
    assert.equal(
      beforeS1.length,
      1,
      `s1 should have exactly one BlockOutput after first run, got ${beforeS1.length}`,
    );
    const beforeCreatedAt = (beforeS1[0] as { created_at?: string }).created_at;
    assert.ok(
      typeof beforeCreatedAt === "string" && beforeCreatedAt.length > 0,
      "s1 BlockOutput should carry a created_at timestamp",
    );

    // Trigger retry. Engine transitions Failed → Scheduled; the execution
    // tree is preserved, so s1 stays Completed and is not re-executed.
    const retryResult = await client.retryInstance(id);
    assert.equal(retryResult.state, "scheduled");

    // The instance will fail again at s2 (same permanent error). Wait for
    // it so we know the traversal has completed a second time.
    await client.waitForState(id, "failed", { timeoutMs: 15_000 });

    const afterAll = await client.getOutputs(id);
    const afterS1 = afterAll.filter((o) => o.block_id === "s1");

    // Core invariant: no duplicate BlockOutput row for the Completed node.
    assert.equal(
      afterS1.length,
      1,
      `s1 should still have exactly one BlockOutput after retry (memoization), got ${afterS1.length}`,
    );

    // Stronger guard: the surviving row is the SAME row — unchanged
    // created_at means the handler was not re-invoked and no new row was
    // written. If memoization broke we'd see either count=2 or a refreshed
    // timestamp.
    const afterCreatedAt = (afterS1[0] as { created_at?: string }).created_at;
    assert.equal(
      afterCreatedAt,
      beforeCreatedAt,
      "s1 BlockOutput.created_at should not change after retry — handler must not re-run",
    );

    // And the captured output payload is identical.
    assert.deepEqual(
      afterS1[0]!.output,
      beforeS1[0]!.output,
      "s1 output payload should be byte-equal across retry",
    );
  });
});
