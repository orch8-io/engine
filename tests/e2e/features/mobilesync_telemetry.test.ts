/**
 * Mobile telemetry ingestion — batch-level device metadata + bounds.
 *
 * perf(mobile): bound idle sync memory (66002f5) changed the wire model:
 * new clients send ONE batch-level `device` instead of repeating it per
 * event; the server stays backward compatible with per-event devices
 * (orch8-api/src/telemetry.rs `IngestTelemetryWireRequest` /
 * `normalize_ingest_request`). Batch cap is 500 events (413 above that).
 *
 * Rows are verified directly in `telemetry_mobile_events` /
 * `telemetry_mobile_errors` via TEST_DB_URL.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import { only, post, psqlRows, sqlStr, uid } from "./pushwake_helpers.ts";

function device(deviceId: string, overrides: Record<string, unknown> = {}) {
  return {
    device_id: deviceId,
    os_name: "iOS",
    os_version: "18.4",
    app_version: "2.1.0",
    sdk_version: "0.9.0",
    ...overrides,
  };
}

function event(i: number, overrides: Record<string, unknown> = {}) {
  return {
    event_type: "sync_heartbeat",
    payload: JSON.stringify({ seq: i }),
    timestamp: new Date(Date.now() - i * 1000).toISOString(),
    ...overrides,
  };
}

interface TelemetryRow {
  event_type: string;
  payload: Record<string, unknown>;
  device_id: string;
  os_name: string;
  os_version: string;
  app_version: string;
  sdk_version: string;
  tenant_id: string;
  created_at: string;
}

async function eventRows(deviceId: string): Promise<TelemetryRow[]> {
  return psqlRows<TelemetryRow>(
    `SELECT * FROM telemetry_mobile_events WHERE device_id = ${sqlStr(deviceId)} ORDER BY id`,
  );
}

describe("mobile telemetry — batch ingestion and device metadata", () => {
  let server: ServerHandle | undefined;

  // KNOWN BUG (pinned, not fixed — the pin below must be flipped when the
  // storage layer is fixed):
  //   `ingest_telemetry_events_batch`
  //   (orch8-storage/src/postgres/telemetry.rs) binds `payload` as a plain
  //   String → TEXT parameter into a JSONB column. Postgres has no implicit
  //   text→jsonb assignment cast, so EVERY non-empty batch insert fails with:
  //     column "payload" is of type jsonb but expression is of type text
  //   and the API returns 500. Validation-only paths (400/413/empty batch)
  //   and the separate error-report endpoint are unaffected.
  //
  // Convention (shared with mobilesync_commands_sync.test.ts):
  //   - the test asserting the CURRENT (buggy) behavior is named
  //     `KNOWN BUG: ...` and carries an inline `// KNOWN BUG:` comment with
  //     the exact storage location;
  //   - tests asserting CORRECT behavior stay skipped via the
  //     `itBlockedBy*Bug = it.skip` alias and reference the same note.
  const itBlockedByJsonbBug = it.skip;

  before(async () => {
    // Telemetry routes are always mounted; no mobile-sync flag needed.
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // KNOWN BUG (see header note): `ingest_telemetry_events_batch`
  // (orch8-storage/src/postgres/telemetry.rs) binds payload as TEXT into a
  // JSONB column. Pins the CURRENT (buggy) behavior.
  it("KNOWN BUG: any non-empty telemetry batch fails with 500 (text→jsonb bind)", async () => {
    const res = await post(
      "/telemetry/mobile",
      { events: [event(0)], device: device(uid("dev")) },
      uid("t"),
    );
    assert.equal(
      res.status,
      500,
      "storage binds payload as TEXT into a JSONB column — insert always fails on Postgres",
    );
    assert.match(res.text, /internal server error/);
  });

  // Asserts CORRECT behavior — skipped until the KNOWN BUG (header note) is fixed.
  itBlockedByJsonbBug("batch-level device is applied to every event", async () => {
    const tenant = uid("t");
    const deviceId = uid("dev");
    const res = await post(
      "/telemetry/mobile",
      {
        events: [event(0), event(1), event(2)],
        device: device(deviceId),
      },
      tenant,
    );
    assert.equal(res.status, 202, res.text);
    assert.equal(res.body.accepted, 3);

    const rows = await eventRows(deviceId);
    assert.equal(rows.length, 3);
    for (const row of rows) {
      assert.equal(row.device_id, deviceId);
      assert.equal(row.os_name, "iOS");
      assert.equal(row.os_version, "18.4");
      assert.equal(row.app_version, "2.1.0");
      assert.equal(row.sdk_version, "0.9.0");
      assert.equal(row.tenant_id, tenant);
      assert.equal(row.event_type, "sync_heartbeat");
    }
    assert.deepEqual(
      rows.map((r) => r.payload.seq).sort(),
      [0, 1, 2],
      "payloads stored as JSONB round-trip",
    );
  });

  // Asserts CORRECT behavior — skipped until the KNOWN BUG (header note) is fixed.
  itBlockedByJsonbBug("per-event device overrides the batch-level device", async () => {
    const tenant = uid("t");
    const batchDevice = uid("dev-batch");
    const eventDevice = uid("dev-event");

    const res = await post(
      "/telemetry/mobile",
      {
        events: [event(0), event(1, { device: device(eventDevice, { os_name: "Android" }) })],
        device: device(batchDevice),
      },
      tenant,
    );
    assert.equal(res.status, 202, res.text);
    assert.equal(res.body.accepted, 2);

    const batchRows = await eventRows(batchDevice);
    assert.equal(batchRows.length, 1);
    const eventRows2 = await eventRows(eventDevice);
    assert.equal(eventRows2.length, 1);
    assert.equal(only(eventRows2, "event-device rows").os_name, "Android");
  });

  // Asserts CORRECT behavior — skipped until the KNOWN BUG (header note) is fixed.
  itBlockedByJsonbBug("legacy per-event-only payloads (no batch device) are still accepted", async () => {
    const tenant = uid("t");
    const deviceId = uid("dev-legacy");
    const res = await post(
      "/telemetry/mobile",
      { events: [event(0, { device: device(deviceId) }), event(1, { device: device(deviceId) })] },
      tenant,
    );
    assert.equal(res.status, 202, res.text);
    assert.equal(res.body.accepted, 2);

    const rows = await eventRows(deviceId);
    assert.equal(rows.length, 2);
  });

  it("events with no device at either level are rejected with 400", async () => {
    const res = await post("/telemetry/mobile", { events: [event(0)] }, uid("t"));
    assert.equal(res.status, 400, res.text);
    assert.match(res.text, /device is required/);
  });

  it("a single under-specified event fails the whole batch", async () => {
    const tenant = uid("t");
    const deviceId = uid("dev");
    const res = await post(
      "/telemetry/mobile",
      {
        events: [
          event(0, { device: device(deviceId) }),
          event(1, { device: device(deviceId) }),
          event(2), // no per-event device, and no batch-level device below
        ],
      },
      tenant,
    );
    assert.equal(res.status, 400, res.text);

    const rows = await eventRows(deviceId);
    assert.equal(rows.length, 0, "validation happens before any insert");
  });

  // Asserts CORRECT behavior — skipped until the KNOWN BUG (header note) is fixed.
  itBlockedByJsonbBug("exactly 500 events are accepted; 501 are rejected with 413", async () => {
    const tenant = uid("t");
    const deviceId = uid("dev");

    const atCap = await post(
      "/telemetry/mobile",
      {
        events: Array.from({ length: 500 }, (_, i) => event(i)),
        device: device(deviceId),
      },
      tenant,
    );
    assert.equal(atCap.status, 202, atCap.text);
    assert.equal(atCap.body.accepted, 500);

    const rows = await eventRows(deviceId);
    assert.equal(rows.length, 500, "full 500-event batch persisted");
  });

  it("batches over 500 events are rejected with 413 before any insert", async () => {
    const tenant = uid("t");
    const overCap = await post(
      "/telemetry/mobile",
      {
        events: Array.from({ length: 501 }, (_, i) => event(i)),
        device: device(uid("dev-over")),
      },
      tenant,
    );
    assert.equal(overCap.status, 413, overCap.text);
    assert.match(overCap.text, /exceeds maximum of 500/);
  });

  it("empty event batches are accepted as a no-op", async () => {
    const res = await post(
      "/telemetry/mobile",
      { events: [], device: device(uid("dev")) },
      uid("t"),
    );
    assert.equal(res.status, 202, res.text);
    assert.equal(res.body.accepted, 0);
  });

  // Asserts CORRECT behavior — skipped until the KNOWN BUG (header note) is fixed.
  itBlockedByJsonbBug("unparseable event timestamps fall back to ingest time", async () => {
    const tenant = uid("t");
    const deviceId = uid("dev");
    const res = await post(
      "/telemetry/mobile",
      {
        events: [event(0, { timestamp: "not-a-timestamp" })],
        device: device(deviceId),
      },
      tenant,
    );
    assert.equal(res.status, 202, res.text);

    const row = only(await eventRows(deviceId));
    const skewMs = Math.abs(Date.now() - new Date(row.created_at).getTime());
    assert.ok(skewMs < 60_000, `created_at falls back to now (skew ${skewMs}ms)`);
  });

  // Asserts CORRECT behavior — skipped until the KNOWN BUG (header note) is fixed.
  itBlockedByJsonbBug("the X-Tenant-Id header wins over the body tenant_id", async () => {
    const headerTenant = uid("t-header");
    const bodyTenant = uid("t-body");
    const deviceId = uid("dev");

    const res = await post(
      "/telemetry/mobile",
      {
        events: [event(0)],
        device: device(deviceId),
        tenant_id: bodyTenant,
      },
      headerTenant,
    );
    assert.equal(res.status, 202, res.text);

    const row = only(await eventRows(deviceId));
    assert.equal(row.tenant_id, headerTenant, "header scopes the tenant, body is ignored");
  });

  // Asserts CORRECT behavior — skipped until the KNOWN BUG (header note) is fixed.
  itBlockedByJsonbBug("without any tenant context events land in the 'default' tenant", async () => {
    const deviceId = uid("dev");
    const res = await post("/telemetry/mobile", {
      events: [event(0)],
      device: device(deviceId),
    });
    assert.equal(res.status, 202, res.text);

    const row = only(await eventRows(deviceId));
    assert.equal(row.tenant_id, "default");
  });

  // Asserts CORRECT behavior — skipped until the KNOWN BUG (header note) is fixed.
  itBlockedByJsonbBug("body tenant_id is honoured when no header is present", async () => {
    const bodyTenant = uid("t-body");
    const deviceId = uid("dev");
    const res = await post("/telemetry/mobile", {
      events: [event(0)],
      device: device(deviceId),
      tenant_id: bodyTenant,
    });
    assert.equal(res.status, 202, res.text);

    const row = only(await eventRows(deviceId));
    assert.equal(row.tenant_id, bodyTenant);
  });

  it("structured error reports are persisted with full device context", async () => {
    const tenant = uid("t");
    const deviceId = uid("dev");
    const instanceId = uid("inst");

    const res = await post(
      "/telemetry/mobile/errors",
      {
        error_type: "engine_crash",
        message: "sequence exploded",
        stack_trace: "frame1\nframe2",
        device: device(deviceId),
        instance_id: instanceId,
      },
      tenant,
    );
    assert.equal(res.status, 202, res.text);

    const rows = await psqlRows<Record<string, unknown>>(
      `SELECT * FROM telemetry_mobile_errors WHERE device_id = ${sqlStr(deviceId)}`,
    );
    const row = only(rows, "error rows");
    assert.equal(row.error_type, "engine_crash");
    assert.equal(row.message, "sequence exploded");
    assert.equal(row.stack_trace, "frame1\nframe2");
    assert.equal(row.instance_id, instanceId);
    assert.equal(row.tenant_id, tenant);
    assert.equal(row.sdk_version, "0.9.0");
  });
});
