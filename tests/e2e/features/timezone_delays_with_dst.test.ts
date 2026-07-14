/**
 * Timezone-Aware Delays Across DST.
 *
 * Tests:
 *   1. Instance `timezone` field round-trips via the REST API.
 *   2. `fire_at_local` on delay spec honours DST transitions.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

/**
 * US spring-forward is the second Sunday in March. Computed relative to
 * "now" (rather than hardcoded) so this test never goes stale — a fixed
 * past date would silently stop exercising the DST transition once the
 * calendar moved past it.
 */
function nextUsSpringForwardDate(): string {
  const now = new Date();
  for (const year of [now.getUTCFullYear(), now.getUTCFullYear() + 1]) {
    const marchFirst = new Date(Date.UTC(year, 2, 1));
    const firstSunday = 1 + ((7 - marchFirst.getUTCDay()) % 7);
    const secondSunday = firstSunday + 7;
    const candidate = new Date(Date.UTC(year, 2, secondSunday));
    if (candidate.getTime() > now.getTime()) {
      const mm = String(candidate.getUTCMonth() + 1).padStart(2, "0");
      const dd = String(candidate.getUTCDate()).padStart(2, "0");
      return `${candidate.getUTCFullYear()}-${mm}-${dd}`;
    }
  }
  throw new Error("unreachable: two-year search must find a future date");
}

describe("Timezone-Aware Delays Across DST", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

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

  it("honours wall-clock time across DST boundary", async () => {
    // US spring-forward in America/New_York: 2:00 AM jumps to 3:00 AM.
    // Local time 02:30 does not exist on that date.
    // The engine should roll forward to 03:30 EDT = 07:30 UTC (or fire at
    // the pre-DST 02:30 EST = 06:30 UTC — both are sane DST-aware policies).
    const dstDate = nextUsSpringForwardDate();
    const tenantId = `tz-dst-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "tz-dst-wallclock",
      [
        step(
          "s1",
          "noop",
          {},
          {
            delay: {
              duration: 0,
              fire_at_local: `${dstDate}T02:30:00`,
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

    // Wait briefly for the scheduler to process the delay.
    await new Promise((r) => setTimeout(r, 500));

    const inst = await client.getInstance(id);
    const nextFireAt = inst.next_fire_at;
    assert.ok(
      typeof nextFireAt === "string",
      "instance should have next_fire_at set from the wall-clock delay",
    );
    const utc = new Date(nextFireAt!).toISOString();
    // Both policies represent sane DST-aware resolutions:
    // (a) rolled to 03:30 EDT → 07:30 UTC
    // (b) fires at pre-DST 02:30 EST → 06:30 UTC
    const okPolicies = new Set([
      `${dstDate}T07:30:00.000Z`, // rolled to 03:30 EDT
      `${dstDate}T06:30:00.000Z`, // fires at pre-DST 02:30 EST
    ]);
    assert.ok(
      okPolicies.has(utc),
      `DST-aware resolution expected; got ${utc}.`,
    );
  });
});
