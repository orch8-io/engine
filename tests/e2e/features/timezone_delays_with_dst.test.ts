/**
 * Timezone-Aware Delays Across DST.
 *
 * This file has two cases:
 *   1. A real test that locks in the instance `timezone` field round-trip
 *      via the REST API. `TaskInstance` already carries `timezone: String`
 *      (see `orch8-types/src/instance.rs:102`) and
 *      `orch8-api/src/instances.rs:60,198,265` reads/writes it on
 *      CreateInstanceRequest and GET paths, so this invariant is observable
 *      today.
 *   2. A ready-to-flip `it.skip` for the actual DST behaviour, blocked on
 *      engine primitives (no wall-clock target in `DelaySpec`, no fake
 *      clock).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Timezone-Aware Delays Across DST", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // REAL: instance timezone round-trips via the API. Confirms the field is
  // wired end-to-end (request parsing → storage → response), which is the
  // prerequisite for any future wall-clock or DST-aware logic.
  it("round-trips instance timezone through create + get", async () => {
    const tenantId = `tz-${uuid().slice(0, 8)}`;
    const seq = testSequence("tz-roundtrip", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const TARGET_TZ = "America/New_York";

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      timezone: TARGET_TZ,
    });

    const fetched = await client.getInstance(id);
    assert.equal(
      (fetched as { timezone?: string }).timezone,
      TARGET_TZ,
      "instance timezone must round-trip exactly as provided",
    );
  });

  it("defaults instance timezone to UTC when omitted", async () => {
    const tenantId = `tz-default-${uuid().slice(0, 8)}`;
    const seq = testSequence("tz-default", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    const fetched = await client.getInstance(id);
    assert.equal(
      (fetched as { timezone?: string }).timezone,
      "UTC",
      "omitted timezone should default to UTC (per orch8-api/src/instances.rs::default_timezone)",
    );
  });

  // BLOCKED BY: `DelaySpec` has no wall-clock target field and the engine
  //   has no fake-clock hook.
  //   - `DelaySpec` (in `orch8-types/src/sequence.rs`) exposes only
  //     `duration` (ms relative to now). `calculate_next_fire_at` computes
  //     `now + duration` in UTC and uses the instance timezone only for
  //     skip-weekend / skip-holiday checks — no "fire at local 02:30 on
  //     YYYY-MM-DD" primitive.
  //   - The engine has no fake-clock env var, so a real-time DST assertion
  //     would be wall-clock bound.
  // UNBLOCK:
  //   - Add `fire_at_local: Option<NaiveDateTime>` to `DelaySpec` and honour
  //     the instance timezone in `calculate_next_fire_at` inside
  //     `orch8-engine/src/scheduler.rs`.
  //   - Expose an `ORCH8_FAKE_NOW` env var read by the engine clock source
  //     so tests can pin "now" just before a DST transition.
  // Flip to `it` once both land.
  it.skip(
    "honours wall-clock time across DST boundary",
    async () => {
      // Scenario: pin fake_now to just before US spring-forward
      // (2026-03-07 12:00 America/New_York → 17:00 UTC). Schedule a step
      // whose delay fires at local 02:30 on 2026-03-08 in
      // America/New_York — the hour 02:00–03:00 is skipped for DST.
      //
      // Either DST-aware policy is acceptable as long as it is documented:
      // (a) roll forward to 03:30 EDT → 07:30 UTC, or (b) fire at pre-DST
      // 02:30 EST → 07:30 UTC as well (yes, same UTC for different local
      // meanings — pick one and lock it in when unblocked). A NAIVE
      // +14h30m-from-now computation ignoring the DST gap would land at
      // a different UTC; this test exists to catch that regression.
      const tenantId = `tz-dst-${uuid().slice(0, 8)}`;
      const seq = testSequence(
        "tz-dst-wallclock",
        [
          // Placeholder shape. Real field name matches whatever the engine
          // introduces — substitute `delay` key when unblocked.
          step(
            "s1",
            "noop",
            {},
            {
              delay: {
                fire_at_local: "2026-03-08T02:30:00",
                timezone: "America/New_York",
              },
            },
          ),
        ],
        { tenantId },
      );
      await client.createSequence(seq);

      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace: "default",
        timezone: "America/New_York",
      });

      const inst = await client.getInstance(id);
      const nextFireAt = inst.next_fire_at;
      assert.ok(
        typeof nextFireAt === "string",
        "instance should have next_fire_at set from the wall-clock delay",
      );
      const utc = new Date(nextFireAt!).toISOString();
      // Both branches represent sane DST-aware resolutions; the engine
      // choosing one is acceptable, ignoring DST is not.
      const okPolicies = new Set([
        "2026-03-08T07:30:00.000Z", // rolled to 03:30 EDT
        "2026-03-08T06:30:00.000Z", // fires at pre-DST 02:30 EST
      ]);
      assert.ok(
        okPolicies.has(utc),
        `DST-aware resolution expected; got ${utc}.`,
      );
    },
  );
});
