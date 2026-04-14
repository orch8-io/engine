/**
 * Concurrency + rate limit interaction — verifies that concurrency limits
 * and rate limits work together correctly when both are configured on
 * the same instances/steps.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Concurrency + Rate Limit Interaction", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("concurrency limit defers excess instances while rate-limited steps still complete", async () => {
    const tenantId = `cr-${uuid().slice(0, 8)}`;
    const concurrencyKey = `key-${uuid().slice(0, 8)}`;
    const rateLimitKey = `rl-${uuid().slice(0, 8)}`;

    const seq = testSequence(
      "cr-interact",
      [
        step("limited", "sleep", { duration_ms: 200 }, {
          rate_limit_key: rateLimitKey,
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    // Create 3 instances with concurrency limit of 1.
    const ids: string[] = [];
    for (let i = 0; i < 3; i++) {
      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace: "default",
        concurrency_key: concurrencyKey,
        max_concurrency: 1,
      });
      ids.push(id);
    }

    // All should eventually complete — concurrency limit queues them
    // sequentially, not fails them.
    for (const id of ids) {
      await client.waitForState(id, "completed", { timeoutMs: 15_000 });
    }
  });

  it("rate-limited step completes after throttle period", async () => {
    const tenantId = `rl-comp-${uuid().slice(0, 8)}`;
    const rateLimitKey = `rl-${uuid().slice(0, 8)}`;

    const seq = testSequence(
      "rl-complete",
      [
        step("throttled", "noop", {}, { rate_limit_key: rateLimitKey }),
        step("after", "noop"),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    // Create two instances that share a rate-limit key.
    const { id: id1 } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    const { id: id2 } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    // Both should complete — rate limit delays but doesn't fail.
    await client.waitForState(id1, "completed", { timeoutMs: 15_000 });
    await client.waitForState(id2, "completed", { timeoutMs: 15_000 });

    // Verify both executed both steps.
    const out1 = await client.getOutputs(id1);
    const out2 = await client.getOutputs(id2);
    assert.ok(out1.some((o) => o.block_id === "after"), "instance 1 should complete after step");
    assert.ok(out2.some((o) => o.block_id === "after"), "instance 2 should complete after step");
  });
});
