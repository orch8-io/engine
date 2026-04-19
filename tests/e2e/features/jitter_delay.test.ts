/**
 * Jitter Delay — verifies that a step with `delay.jitter` introduces
 * randomised spread around the base delay. Multiple instances of the
 * same sequence should receive different `next_fire_at` values while
 * in the Scheduled state waiting for the delay to fire.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Jitter Delay", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("applies jitter so next_fire_at varies across instances", async () => {
    const tenantId = `jitter-${uuid().slice(0, 8)}`;

    // Large delay (1 hour) so the instance stays in Scheduled state
    // long enough for us to observe the fire time. 30s jitter window.
    const seq = testSequence(
      "jitter-spread",
      [
        step("s1", "noop", {}, {
          delay: { duration: 3_600_000, jitter: 30_000 },
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const N = 6;
    const ids: string[] = [];
    for (let i = 0; i < N; i++) {
      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace: "default",
      });
      ids.push(id);
    }

    // Wait for the scheduler to apply delays and transition to Scheduled.
    const deadline = Date.now() + 15_000;
    const fireTimes: number[] = [];

    for (const id of ids) {
      let fireAt: string | null = null;
      while (Date.now() < deadline) {
        const inst = await client.getInstance(id);
        // The delay sets next_fire_at and moves state to Scheduled.
        // Check that next_fire_at is in the future (delay was applied).
        if (inst.next_fire_at) {
          const ft = new Date(inst.next_fire_at).getTime();
          // Only accept fire times that are at least 30 minutes from now
          // (proving the 1-hour delay was applied, not just a scheduling
          // timestamp).
          if (ft > Date.now() + 30 * 60 * 1000) {
            fireAt = inst.next_fire_at;
            break;
          }
        }
        await new Promise((r) => setTimeout(r, 100));
      }
      assert.ok(fireAt, `instance ${id} must have a future next_fire_at`);
      fireTimes.push(new Date(fireAt!).getTime());
    }

    // All fire times should be approximately 1 hour ± 30s from now.
    const now = Date.now();
    for (const ft of fireTimes) {
      const diffS = (ft - now) / 1000;
      assert.ok(
        diffS > 3500 && diffS < 3700,
        `fire time should be ~1 hour from now, got ${diffS.toFixed(0)}s`,
      );
    }

    // At least 2 distinct values among N instances proves jitter is working.
    // Round to seconds to avoid sub-ms comparison noise.
    const uniqueSeconds = new Set(fireTimes.map((t) => Math.round(t / 1000)));
    assert.ok(
      uniqueSeconds.size >= 2,
      `jitter should produce variation across ${N} instances, got ${uniqueSeconds.size} unique fire times (seconds)`,
    );
  });

  it("next_fire_at stays within jitter bounds", async () => {
    const tenantId = `jitter-bounds-${uuid().slice(0, 8)}`;

    // 2-hour delay + exactly 10s jitter
    const seq = testSequence(
      "jitter-bounds",
      [
        step("s1", "noop", {}, {
          delay: { duration: 7_200_000, jitter: 10_000 },
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const beforeCreate = Date.now();
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    // Wait for next_fire_at to be set with a future delay.
    const deadline = Date.now() + 10_000;
    let fireAtMs = 0;
    while (Date.now() < deadline) {
      const inst = await client.getInstance(id);
      if (inst.next_fire_at) {
        const ft = new Date(inst.next_fire_at).getTime();
        if (ft > Date.now() + 60 * 60 * 1000) {
          fireAtMs = ft;
          break;
        }
      }
      await new Promise((r) => setTimeout(r, 100));
    }
    assert.ok(fireAtMs > 0, "instance must have a future next_fire_at");

    const afterPoll = Date.now();
    // Expected: somewhere between beforeCreate+2h-10s and afterPoll+2h+10s
    // with generous tolerance for scheduling latency.
    const baseLow = beforeCreate + 7_200_000 - 10_000 - 5_000;
    const baseHigh = afterPoll + 7_200_000 + 10_000 + 5_000;
    assert.ok(
      fireAtMs >= baseLow && fireAtMs <= baseHigh,
      `fire time must be within 2h ± 10s window (got offset ${((fireAtMs - beforeCreate) / 1000).toFixed(0)}s from creation)`,
    );
  });

  it("zero jitter produces consistent timing", async () => {
    const tenantId = `jitter-zero-${uuid().slice(0, 8)}`;

    // 30-minute delay + 0ms jitter
    const seq = testSequence(
      "jitter-zero",
      [
        step("s1", "noop", {}, {
          delay: { duration: 1_800_000, jitter: 0 },
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const ids: string[] = [];
    for (let i = 0; i < 4; i++) {
      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace: "default",
      });
      ids.push(id);
    }

    const deadline = Date.now() + 10_000;
    const fireTimes: number[] = [];
    for (const id of ids) {
      while (Date.now() < deadline) {
        const inst = await client.getInstance(id);
        if (inst.next_fire_at) {
          const ft = new Date(inst.next_fire_at).getTime();
          if (ft > Date.now() + 10 * 60 * 1000) {
            fireTimes.push(ft);
            break;
          }
        }
        await new Promise((r) => setTimeout(r, 100));
      }
    }
    assert.equal(fireTimes.length, 4, "all instances should have delayed fire times");

    // With zero jitter, fire times should differ only by creation-time gap
    // (instances created sequentially). Spread should be < 5s.
    const min = Math.min(...fireTimes);
    const max = Math.max(...fireTimes);
    assert.ok(
      max - min < 5_000,
      `zero jitter should yield nearly identical fire times, spread was ${max - min}ms`,
    );
  });
});
