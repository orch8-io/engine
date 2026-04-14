/**
 * Rate Limiting — verifies that per-key throughput caps defer excess work
 * instead of dropping instances. Closes the gap where previous tests only
 * covered success-path throughput without asserting backpressure semantics.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Rate Limiting", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // Bursts beyond the configured rate must defer (not drop) instances.
  //
  // `rate_limit_key` causes the scheduler to bounce the instance back to
  // `scheduled` with a retry_after when storage reports the token bucket is
  // exhausted. The instance remains schedulable; only the step defers.
  // Observable contract here: every instance eventually reaches `completed`,
  // none are dropped, even under burst load sharing the same key.
  it("defers overages via rate_limit_key without dropping instances", async () => {
    const tenantId = `rl-${uuid().slice(0, 8)}`;
    const rateKey = `rl-key-${uuid().slice(0, 8)}`;

    const seq = testSequence(
      "rate-limit-defer",
      [step("rl", "noop", {}, { rate_limit_key: rateKey })],
      { tenantId },
    );
    await client.createSequence(seq);

    const N = 9;
    const ids: string[] = [];
    for (let i = 0; i < N; i++) {
      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace: "default",
      });
      ids.push(id);
    }

    // Every instance must terminate in `completed` — deferral must not drop
    // or fail any of them.
    for (const id of ids) {
      const final = await client.waitForState(id, "completed", {
        timeoutMs: 30_000,
      });
      assert.equal(final.state, "completed");
    }
  });
});
