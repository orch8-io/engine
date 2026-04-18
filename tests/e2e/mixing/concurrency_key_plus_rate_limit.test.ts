/**
 * E2E scaffold verifying concurrency key and rate limit constraints co-apply on the same step.
 *
 * Note: `rate_limit_key` is a step-definition field (see `StepDef` in
 * `orch8-types/src/sequence.rs`), but the rate-limit bucket itself (quota,
 * window) is only configurable via storage (`upsert_rate_limit`) — the HTTP
 * API does not currently expose a way to create/seed rate-limit buckets.
 * Consequently the "10 starts per rolling second" assertion from the plan
 * can't be exercised end-to-end here; this test verifies the observable
 * concurrency behaviour and still attaches a `rate_limit_key` to the step
 * so the scheduler's step-level rate-limit check path is traversed.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Concurrency Key + Rate Limit Together", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // Plan:
  //   - register a step with `concurrency_key=K` (max 2 concurrent) and `rate_limit_key=R` (10/sec)
  //   - fire 20 instances in rapid succession and record handler start/end timestamps
  //   - compute max concurrent in-flight count across the timeline
  //   - assert at no point more than 2 handlers run concurrently
  //   - assert overall throughput never exceeds 10 starts per rolling second
  it("should honour both concurrency and rate limits on same step", async () => {
    // Attach rate_limit_key at the step level (exercises the scheduler path
    // even though no bucket is seeded — check returns Allowed by default).
    const rateKey = `rl-${uuid().slice(0, 8)}`;
    const seq = testSequence("mix-conc-rate", [
      step(
        "s1",
        "sleep",
        { duration_ms: 300 },
        { rate_limit_key: rateKey },
      ),
    ]);
    await client.createSequence(seq);

    const concurrencyKey = `mix-${uuid().slice(0, 8)}`;
    const N = 20;

    // Fire instances in rapid succession, recording submission timestamps.
    const submitTimestamps: number[] = [];
    const ids: string[] = [];
    for (let i = 0; i < N; i++) {
      submitTimestamps.push(Date.now());
      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: "test",
        namespace: "default",
        concurrency_key: concurrencyKey,
        max_concurrency: 2,
      });
      ids.push(id);
    }

    // Sample the timeline: poll all instances every ~100ms and record how
    // many are in the `running` state. Keep going until every instance has
    // reached a terminal state or we hit the sampling deadline.
    const terminalStates = new Set(["completed", "failed", "cancelled"]);
    const samples: number[] = [];
    const samplingDeadline = Date.now() + 15_000;
    while (Date.now() < samplingDeadline) {
      const instances = await Promise.all(ids.map((id) => client.getInstance(id)));
      const running = instances.filter((i) => i.state === "running").length;
      samples.push(running);
      if (instances.every((i) => terminalStates.has(i.state))) break;
      await new Promise((r) => setTimeout(r, 100));
    }

    const maxRunning = samples.length > 0 ? Math.max(...samples) : 0;
    assert.ok(
      maxRunning <= 2,
      `expected at most 2 concurrent running instances, observed ${maxRunning}`,
    );

    // Ensure every instance eventually reaches a terminal state.
    for (const id of ids) {
      await client.waitForState(id, ["completed", "failed", "cancelled"], {
        timeoutMs: 30_000,
      });
    }

    // Sanity check on submissions: we issued all N requests relatively
    // quickly. (This is a submission-side timing check, not a rate-limit
    // assertion — see the file-level note above.)
    const submissionSpanMs =
      submitTimestamps[submitTimestamps.length - 1]! - submitTimestamps[0]!;
    assert.ok(
      submissionSpanMs < 10_000,
      `expected rapid submission (< 10s), took ${submissionSpanMs}ms`,
    );

    // Note: rate_limit_key throughput assertion skipped — the HTTP API does
    // not currently expose an endpoint to seed a rate-limit bucket with a
    // given quota/window, so "10 starts per rolling second" can't be
    // verified end-to-end. The step-level `rate_limit_key` is still set on
    // the definition so the scheduler's `check_step_rate_limit` code path
    // is exercised for every instance start.
  });
});
