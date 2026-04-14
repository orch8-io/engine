/**
 * Output Memoization — verifies retry semantics for block outputs.
 *
 * `retryInstance` deletes the execution tree and only sentinel block
 * outputs (in-progress markers from permanently failed steps). Real
 * outputs from successfully completed steps are preserved so they are
 * skipped on retry — preventing double execution of side-effectful
 * handlers (email, HTTP POST, etc.).
 *
 * Observable invariant after retry:
 *   s1 (succeeds) → s2 (fails permanently) → instance=Failed →
 *   `retryInstance` (clears tree + sentinels) → s1 skipped (output kept)
 *   → s2 re-runs and fails again → instance=Failed
 * Each step still has exactly one BlockOutput row after retry.
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

    // Trigger retry. retryInstance clears the execution tree and sentinel
    // outputs, then re-schedules. Completed steps (s1) are skipped; only
    // failed steps (s2) re-execute.
    const retryResult = await client.retryInstance(id);
    assert.equal(retryResult.state, "scheduled");

    // The instance will fail again at s2 (same permanent error). Wait for
    // it so we know the traversal has completed a second time.
    await client.waitForState(id, "failed", { timeoutMs: 15_000 });

    const afterAll = await client.getOutputs(id);
    const afterS1 = afterAll.filter((o) => o.block_id === "s1");

    // Core invariant: within a single execution pass, each step produces
    // exactly one BlockOutput row — no duplicates.
    assert.equal(
      afterS1.length,
      1,
      `s1 should have exactly one BlockOutput after retry (no duplicates within a pass), got ${afterS1.length}`,
    );

    // The output payload is semantically identical (same handler, same params).
    assert.deepEqual(
      afterS1[0]!.output,
      beforeS1[0]!.output,
      "s1 output payload should be identical across retry (same handler + params)",
    );
  });
});
