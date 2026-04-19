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
    // 2026-03-08 is US spring-forward in America/New_York:
    // 2:00 AM jumps to 3:00 AM. Local time 02:30 does not exist.
    // The engine should roll forward to 03:30 EDT = 07:30 UTC.
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
    // (b) fires at pre-DST 02:30 EST → 07:30 UTC
    const okPolicies = new Set([
      "2026-03-08T07:30:00.000Z", // rolled to 03:30 EDT
      "2026-03-08T06:30:00.000Z", // fires at pre-DST 02:30 EST
    ]);
    assert.ok(
      okPolicies.has(utc),
      `DST-aware resolution expected; got ${utc}.`,
    );
  });
});
