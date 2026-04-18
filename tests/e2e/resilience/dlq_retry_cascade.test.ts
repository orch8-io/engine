/**
 * Verifies the DLQ retry cascade: a permanently failing instance lands in
 * the DLQ, `retryInstance` re-runs it, it fails again, and remains visible
 * in the DLQ listing for operator action.
 *
 * Implementation note: the existing public API does not expose a
 * per-instance `total_attempts` counter, so the strict "retry from DLQ
 * never exceeds the original max_attempts" invariant can't be asserted
 * directly today. What we CAN assert is:
 *   - the instance keeps landing in `failed` and appears in the DLQ list
 *   - `retryInstance` moves it out of `failed` (back to `scheduled`) each
 *     time and allows the cascade to repeat
 *   - retrying a currently-failed instance is accepted by the API
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("DLQ Retry Respects max_attempts", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("re-enqueues a permanently failing instance through repeated DLQ retries", async () => {
    const tenantId = `test-${uuid().slice(0, 8)}`;

    // Use the built-in `fail` handler with retryable=false so the instance
    // fails permanently and ends up in the DLQ (per dlq.test.ts + the
    // tool_call_missing_params pattern).
    const seq = testSequence(
      "dlq-cascade",
      [step("s1", "fail", { message: "always fail", retryable: false })],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    // Initial failure -> DLQ.
    await client.waitForState(id, "failed", { timeoutMs: 10_000 });
    const dlq1 = await client.listDlq({ tenant_id: tenantId });
    assert.ok(
      dlq1.some((i) => i.id === id),
      "instance should appear in DLQ after first permanent failure",
    );

    // Retry #1 — instance goes back to scheduled and fails again.
    const retry1 = await client.retryInstance(id);
    assert.equal(retry1.state, "scheduled");
    await client.waitForState(id, "failed", { timeoutMs: 10_000 });
    const dlq2 = await client.listDlq({ tenant_id: tenantId });
    assert.ok(
      dlq2.some((i) => i.id === id),
      "instance should re-appear in DLQ after retry #1 fails",
    );

    // Retry #2 — same cascade. Locks in that the DLQ retry path doesn't
    // silently drop the instance after N attempts.
    const retry2 = await client.retryInstance(id);
    assert.equal(retry2.state, "scheduled");
    await client.waitForState(id, "failed", { timeoutMs: 10_000 });
    const dlq3 = await client.listDlq({ tenant_id: tenantId });
    assert.ok(
      dlq3.some((i) => i.id === id),
      "instance should re-appear in DLQ after retry #2 fails",
    );

    // Engine gap: there is no public `total_attempts` field on Instance to
    // verify the max_attempts budget is globally respected across DLQ
    // retries. When the engine exposes per-instance attempt counters, add
    // an assertion here that total_attempts never exceeds the original
    // retry policy's max_attempts budget.
  });
});
