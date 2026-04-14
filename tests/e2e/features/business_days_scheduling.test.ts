/**
 * Business-Days-Only Scheduling — verifies that a step with a DelaySpec
 * whose `business_days_only=true` rolls `next_fire_at` forward past
 * weekends / configured holidays. Closes the gap around calendar-aware
 * delay semantics.
 *
 * Engine contract (`orch8-engine/src/scheduling/delay.rs::calculate_next_fire_at`):
 *   - `target = now + duration`, then while target.weekday is Sat/Sun OR
 *     target date is in `delay.holidays ∪ context.config.holidays`, advance
 *     one or two days (Saturday jumps directly to Monday).
 *
 * Strategy: wall-clock independence is hard without a fake clock, but the
 * holiday list is enough to force deterministic rollover. We mark today
 * (and the next several days) as holidays so the engine MUST push
 * next_fire_at at least one day ahead of `now`, regardless of weekday.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

function toIsoDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

describe("Business-Days-Only Scheduling", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("rolls next_fire_at past configured holidays when business_days_only is true", async () => {
    const tenantId = `bdays-${uuid().slice(0, 8)}`;

    // Mark today + the next 5 calendar days as holidays so the scheduler
    // must push next_fire_at at least one day forward regardless of
    // which weekday we run on.
    const now = new Date();
    const holidays: string[] = [];
    for (let i = 0; i <= 5; i++) {
      const d = new Date(now);
      d.setUTCDate(d.getUTCDate() + i);
      holidays.push(toIsoDate(d));
    }

    const seq = testSequence(
      "bdays-defer",
      [
        step("delayed", "noop", {}, {
          delay: {
            duration: 100, // ms — base target is ~now
            business_days_only: true,
            holidays,
          },
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    // Wait until the scheduler has applied the delay and re-scheduled.
    // State transitions to Scheduled with a future next_fire_at once the
    // check_step_delay branch runs.
    const deadline = Date.now() + 10_000;
    let instance = await client.getInstance(id);
    while (Date.now() < deadline) {
      if (instance.next_fire_at) {
        const fireAt = new Date(instance.next_fire_at);
        if (fireAt.getTime() > now.getTime() + 60 * 60 * 1000) break;
      }
      await new Promise((r) => setTimeout(r, 100));
      instance = await client.getInstance(id);
    }

    assert.ok(instance.next_fire_at, "instance must have a next_fire_at after delay applied");
    const fireAt = new Date(instance.next_fire_at);
    // At least one full day ahead of now — weekend/holiday skip landed.
    const oneDayMs = 24 * 60 * 60 * 1000;
    assert.ok(
      fireAt.getTime() - now.getTime() > oneDayMs,
      `next_fire_at must roll forward past today's holiday (fireAt=${instance.next_fire_at}, now=${now.toISOString()})`,
    );
  });
});
