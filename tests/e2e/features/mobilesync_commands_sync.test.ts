/**
 * Mobile sync protocol — command mailbox, approvals, delegations, bounds.
 *
 * Covers `POST /mobile/sync` (orch8-api/src/mobile_sync.rs handle_sync):
 *   - pending command delivery (oldest first, hard limit 50 per sync),
 *   - adaptive `sync_interval_secs` (5 with pending work, 30 when idle),
 *   - per-array item cap of 500 (memory bound from perf(mobile) work),
 *   - approval request ingest + dedupe + resolution flow,
 *   - server-side credential resolution for delegated steps,
 *   - status update upserts,
 *   - `last_sync_at` bookkeeping.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import {
  commandRowsForDevice,
  createCommand,
  deviceSpec,
  get,
  mustRegisterDevice,
  only,
  post,
  psqlRows,
  sqlStr,
  syncDevice,
  uid,
} from "./pushwake_helpers.ts";

const client = new Orch8Client();

// KNOWN BUGS (pinned, not fixed — the pins below must be flipped when the
// storage layer is fixed):
//  1. `upsert_mobile_instance_status` (orch8-storage/src/postgres/mobile_sync.rs)
//     binds `updated_at` as TEXT into a TIMESTAMPTZ column — every sync
//     carrying status_updates fails 500:
//       column "updated_at" is of type timestamp with time zone but
//       expression is of type text
//  2. `list_mobile_approvals` (orch8-storage/src/postgres/mobile_sync.rs)
//     decodes `timeout_secs` (INT4) into Option<i64> (INT8) — GET
//     /mobile/approvals fails 500 as soon as any listed row has a timeout
//     set. Insert works (bigint→integer is an assignment cast); only the
//     read path breaks.
//
// Convention (shared with mobilesync_telemetry.test.ts):
//   - tests asserting the CURRENT (buggy) behavior are named
//     `KNOWN BUG: ...` and carry an inline `// KNOWN BUG:` comment with the
//     exact storage location;
//   - tests asserting CORRECT behavior stay skipped via an
//     `itBlockedBy*Bug = it.skip` alias and reference the same note.
const itBlockedByTimestamptzBug = it.skip;
const itBlockedByApprovalsTimeoutBug = it.skip;

describe("mobile sync — command mailbox protocol", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({ env: { ORCH8_MOBILE_SYNC_ENABLED: "true" } });
  });

  after(async () => {
    await stopServer(server);
  });

  it("empty sync returns no commands and the 30s idle interval", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());

    const res = await syncDevice(tenant, { device_id: device.device_id });
    assert.equal(res.status, 200, res.text);
    assert.deepEqual(res.body.commands, []);
    assert.equal(res.body.sync_interval_secs, 30);
  });

  it("sync with pending commands returns them oldest-first with the 5s active interval", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    for (let i = 0; i < 3; i++) {
      await createCommand(tenant, device.device_id, "tick", { i });
    }

    const res = await syncDevice(tenant, { device_id: device.device_id });
    assert.equal(res.status, 200, res.text);
    assert.equal(res.body.sync_interval_secs, 5);
    assert.equal(res.body.commands.length, 3);
    const order = res.body.commands.map((c: { payload: { i: number } }) => c.payload.i);
    assert.deepEqual(order, [0, 1, 2], "commands delivered oldest-first");
    for (const command of res.body.commands) {
      assert.ok(command.id, "command id present");
      assert.equal(command.type, "tick");
    }
  });

  it("sync delivers at most 50 commands per call and the remainder stays queued", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    const total = 55;
    for (let i = 0; i < total; i++) {
      const res = await createCommand(tenant, device.device_id, "bulk", { i });
      assert.equal(res.status, 201, res.text);
    }

    const first = await syncDevice(tenant, { device_id: device.device_id });
    assert.equal(first.body.commands.length, 50, "first page capped at 50");
    assert.equal(first.body.sync_interval_secs, 5, "still work to do");

    const ackIds = first.body.commands.map((c: { id: string }) => c.id);
    const second = await syncDevice(tenant, {
      device_id: device.device_id,
      command_acks: ackIds,
    });
    assert.equal(second.body.commands.length, 5, "remaining commands delivered next sync");

    const third = await syncDevice(tenant, {
      device_id: device.device_id,
      command_acks: second.body.commands.map((c: { id: string }) => c.id),
    });
    assert.equal(third.body.commands.length, 0);
    assert.equal(third.body.sync_interval_secs, 30, "queue drained — back to idle cadence");

    const commands = await commandRowsForDevice(device.device_id);
    assert.equal(commands.length, total);
    assert.ok(commands.every((c) => c.acked_at !== null), "all acked in the end");
  });

  it("sync updates last_sync_at on the device row", async () => {
    const tenant = uid("t");
    const spec = await mustRegisterDevice(tenant, deviceSpec());

    const before = await get(`/mobile/devices?tenant_id=${tenant}`, tenant);
    const pre = before.body.items.find(
      (d: { device_id: string }) => d.device_id === spec.device_id,
    );
    assert.equal(pre.last_sync_at, null);

    await syncDevice(tenant, { device_id: spec.device_id });

    const after = await get(`/mobile/devices?tenant_id=${tenant}`, tenant);
    const postRow = after.body.items.find(
      (d: { device_id: string }) => d.device_id === spec.device_id,
    );
    assert.ok(postRow.last_sync_at, "last_sync_at recorded");
    assert.ok(
      Date.now() - new Date(postRow.last_sync_at).getTime() < 60_000,
      "last_sync_at is wall-clock recent",
    );
  });

  it("each sync array rejects 501 items and accepts exactly 500", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    const stamp = new Date().toISOString();

    const statusUpdate = (i: number) => ({
      instance_id: `inst-${i}`,
      state: "running",
      timestamp: stamp,
    });

    const tooMany = await syncDevice(tenant, {
      device_id: device.device_id,
      status_updates: Array.from({ length: 501 }, (_, i) => statusUpdate(i)),
    });
    assert.equal(tooMany.status, 400, tooMany.text);
    assert.match(tooMany.text, /at most 500/);

    // Acceptance at the cap uses command_acks: status_updates currently hit
    // the updated_at text→timestamptz bind bug (see KNOWN BUG notes above).
    const atCap = await syncDevice(tenant, {
      device_id: device.device_id,
      command_acks: Array.from({ length: 500 }, (_, i) => `ack-${i}`),
    });
    assert.equal(atCap.status, 200, atCap.text);

    const tooManyAcks = await syncDevice(tenant, {
      device_id: device.device_id,
      command_acks: Array.from({ length: 501 }, () => uid("c")),
    });
    assert.equal(tooManyAcks.status, 400);

    const tooManyApprovals = await syncDevice(tenant, {
      device_id: device.device_id,
      approval_requests: Array.from({ length: 501 }, (_, i) => ({
        instance_id: `inst-a-${i}`,
        block_id: `b-${i}`,
      })),
    });
    assert.equal(tooManyApprovals.status, 400);

    const tooManyDelegations = await syncDevice(tenant, {
      device_id: device.device_id,
      step_delegations: Array.from({ length: 501 }, (_, i) => ({
        request_id: `r-${i}`,
        instance_id: `inst-d-${i}`,
        block_id: `b-${i}`,
        handler: "noop",
        params: {},
      })),
    });
    assert.equal(tooManyDelegations.status, 400);
  });

  // Asserts CORRECT behavior — skipped until KNOWN BUG #1 (header note) is fixed.
  itBlockedByTimestamptzBug("status updates upsert per (device, instance) and surface in /mobile/status", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    const instanceId = uid("inst");

    const report = (state: string, step?: string) =>
      syncDevice(tenant, {
        device_id: device.device_id,
        status_updates: [
          {
            instance_id: instanceId,
            sequence_name: "deploy",
            state,
            current_step: step,
            handler: "noop",
            timestamp: new Date().toISOString(),
            context_summary: { phase: state },
          },
        ],
      });

    assert.equal((await report("running", "s1")).status, 200);
    assert.equal((await report("completed", "s2")).status, 200);

    const res = await get(
      `/mobile/status?tenant_id=${tenant}&device_id=${device.device_id}`,
      tenant,
    );
    assert.equal(res.status, 200, res.text);
    const rows = res.body.items.filter(
      (s: { instance_id: string }) => s.instance_id === instanceId,
    );
    const row = only(rows, "status rows") as { state: string; current_step: string };
    assert.equal(row.state, "completed");
    assert.equal(row.current_step, "s2");
  });

  it("duplicate approval requests (same device/instance/block) are deduped", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    const instanceId = uid("inst");

    for (let attempt = 0; attempt < 3; attempt++) {
      const res = await syncDevice(tenant, {
        device_id: device.device_id,
        approval_requests: [
          { instance_id: instanceId, block_id: "gate", prompt: "Proceed?" },
        ],
      });
      assert.equal(res.status, 200, `attempt ${attempt}: ${res.text}`);
    }

    const list = await get(`/mobile/approvals?tenant_id=${tenant}`, tenant);
    const mine = list.body.items.filter(
      (a: { instance_id: string }) => a.instance_id === instanceId,
    );
    assert.equal((only(mine, "approvals") as { state: string }).state, "pending", "dedupes retries");
  });

  it("full approval round-trip: request → list → resolve → command+wake → sync → ack", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    const instanceId = uid("inst");

    await syncDevice(tenant, {
      device_id: device.device_id,
      approval_requests: [
        {
          instance_id: instanceId,
          block_id: "human-gate",
          prompt: "Approve deploy?",
          choices: ["ship", "hold"],
        },
      ],
    });

    const list = await get(`/mobile/approvals?state=pending&tenant_id=${tenant}`, tenant);
    const approval = list.body.items.find(
      (a: { instance_id: string }) => a.instance_id === instanceId,
    );
    assert.ok(approval, "approval listed as pending");
    assert.equal(approval.prompt, "Approve deploy?");

    const resolve = await post(
      `/mobile/approvals/${approval.id}/resolve`,
      { output: { choice: "ship" } },
      tenant,
    );
    assert.equal(resolve.status, 200, resolve.text);

    const afterResolve = await get(
      `/mobile/approvals?state=pending&tenant_id=${tenant}`,
      tenant,
    );
    const stillPending = afterResolve.body.items.filter(
      (a: { id: string }) => a.id === approval.id,
    );
    assert.equal(stillPending.length, 0, "no longer pending after resolution");

    const sync = await syncDevice(tenant, { device_id: device.device_id });
    assert.equal(sync.body.commands.length, 1);
    assert.equal(sync.body.commands[0].type, "complete_step");
    assert.equal(sync.body.commands[0].payload.output.choice, "ship");
    assert.equal(sync.body.commands[0].payload.instance_id, instanceId);

    const ack = await syncDevice(tenant, {
      device_id: device.device_id,
      command_acks: [sync.body.commands[0].id],
    });
    assert.equal(ack.status, 200, ack.text);
    assert.equal(ack.body.commands.length, 0);

    const secondResolve = await post(
      `/mobile/approvals/${approval.id}/resolve`,
      { output: { choice: "hold" } },
      tenant,
    );
    assert.equal(secondResolve.status, 404, "already-resolved approvals are gone");
  });

  it("resolving a nonexistent approval is 404 and writes nothing", async () => {
    const tenant = uid("t");
    const res = await post(
      `/mobile/approvals/${uid("nope")}/resolve`,
      { output: {} },
      tenant,
    );
    assert.equal(res.status, 404, res.text);
  });

  it("step delegation resolves credentials server-side and returns resolved_params", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    const credId = uid("cred");
    const cred = await client.createCredential({
      id: credId,
      name: "delegation cred",
      kind: "api_key",
      value: "super-secret-value",
      tenant_id: tenant,
    });
    assert.equal(cred.id, credId);

    const res = await syncDevice(tenant, {
      device_id: device.device_id,
      step_delegations: [
        {
          request_id: uid("req"),
          instance_id: uid("inst"),
          block_id: "call-api",
          handler: "http_request",
          params: {
            url: "https://example.test/hook",
            auth: `credentials://${credId}`,
          },
        },
      ],
    });
    assert.equal(res.status, 200, res.text);

    const command = only(await commandRowsForDevice(device.device_id), "commands");
    assert.equal(command.command_type, "step_result");
    const payload = JSON.parse(command.payload);
    assert.equal(payload.success, true);
    assert.equal(payload.handler, "http_request");
    assert.equal(
      payload.resolved_params.auth,
      "super-secret-value",
      "credential resolved in transit (never stored on device)",
    );
  });

  it("failed credential resolution yields a success:false step_result, not a sync failure", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());

    const res = await syncDevice(tenant, {
      device_id: device.device_id,
      step_delegations: [
        {
          request_id: uid("req"),
          instance_id: uid("inst"),
          block_id: "call-api",
          handler: "http_request",
          params: { auth: `credentials://${uid("missing")}` },
        },
      ],
    });
    assert.equal(res.status, 200, res.text);

    const command = only(await commandRowsForDevice(device.device_id), "commands");
    const payload = JSON.parse(command.payload);
    assert.equal(payload.success, false);
    assert.match(payload.error, /credential resolution failed/);
  });

  it("multiple delegations in one sync each get their own step_result command", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    const reqIds = [uid("r"), uid("r"), uid("r")];

    const res = await syncDevice(tenant, {
      device_id: device.device_id,
      step_delegations: reqIds.map((request_id, i) => ({
        request_id,
        instance_id: uid("inst"),
        block_id: `b${i}`,
        handler: "noop",
        params: { i },
      })),
    });
    assert.equal(res.status, 200, res.text);

    const commands = await commandRowsForDevice(device.device_id);
    assert.equal(commands.length, 3);
    const returnedReqIds = commands
      .map((c) => JSON.parse(c.payload).request_id)
      .sort();
    assert.deepEqual(returnedReqIds, [...reqIds].sort());
  });

  it("sync for an unknown device still succeeds (fall-open without tenant) or 404 (scoped)", async () => {
    const tenant = uid("t");
    const ghost = uid("ghost");

    const scoped = await syncDevice(tenant, { device_id: ghost });
    assert.equal(scoped.status, 404, "scoped sync on unknown device must 404");

    const unscoped = await syncDevice(undefined, { device_id: ghost });
    assert.equal(
      unscoped.status,
      200,
      "insecure fall-open: unscoped sync is allowed and empty",
    );
    assert.deepEqual(unscoped.body.commands, []);
  });

  // KNOWN BUG #1 (see header note): `upsert_mobile_instance_status`
  // (orch8-storage/src/postgres/mobile_sync.rs) binds updated_at as TEXT
  // into a TIMESTAMPTZ column. Pins the CURRENT (buggy) behavior.
  it("KNOWN BUG: sync status_updates fail 500 (updated_at text→timestamptz bind)", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());

    const res = await syncDevice(tenant, {
      device_id: device.device_id,
      status_updates: [
        {
          instance_id: uid("inst"),
          state: "running",
          timestamp: new Date().toISOString(),
        },
      ],
    });
    assert.equal(
      res.status,
      500,
      "upsert_mobile_instance_status binds updated_at as TEXT into TIMESTAMPTZ",
    );
    assert.match(res.text, /internal server error/);

    const rows = await psqlRows(
      `SELECT * FROM mobile_instance_status WHERE device_id = ${sqlStr(device.device_id)}`,
    );
    assert.equal(rows.length, 0, "no status row persisted");
  });

  // KNOWN BUG #2 (see header note): `list_mobile_approvals`
  // (orch8-storage/src/postgres/mobile_sync.rs) decodes timeout_secs (INT4)
  // into Option<i64>. Pins the CURRENT (buggy) behavior.
  it("KNOWN BUG: approvals with timeout_seconds break GET /mobile/approvals (INT4→INT8 decode)", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    const instanceId = uid("inst");

    // Insert path works (bigint→integer assignment cast)…
    const sync = await syncDevice(tenant, {
      device_id: device.device_id,
      approval_requests: [
        { instance_id: instanceId, block_id: "timed-gate", timeout_seconds: 3600 },
      ],
    });
    assert.equal(sync.status, 200, sync.text);

    // …but the read path decodes timeout_secs INT4 into Option<i64> and 500s.
    const list = await get(`/mobile/approvals?tenant_id=${tenant}`, tenant);
    assert.equal(
      list.status,
      500,
      "list_mobile_approvals must not fail decoding timeout_secs",
    );

    const rows = await psqlRows(
      `SELECT id, timeout_secs FROM mobile_approval_requests WHERE instance_id = ${sqlStr(instanceId)}`,
    );
    assert.equal(rows.length, 1, "row exists — only the decode breaks");
  });

  // Asserts CORRECT behavior — skipped until KNOWN BUG #2 (header note) is fixed.
  itBlockedByApprovalsTimeoutBug("approvals with timeout_seconds list with the timeout intact", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    const instanceId = uid("inst");

    await syncDevice(tenant, {
      device_id: device.device_id,
      approval_requests: [
        { instance_id: instanceId, block_id: "timed-gate", timeout_seconds: 3600 },
      ],
    });

    const list = await get(`/mobile/approvals?tenant_id=${tenant}`, tenant);
    assert.equal(list.status, 200, list.text);
    const mine = list.body.items.filter(
      (a: { instance_id: string }) => a.instance_id === instanceId,
    );
    const approval = only(mine, "timed approvals") as {
      timeout_secs: number | null;
    };
    assert.equal(approval.timeout_secs, 3600, "timeout survives the read path");
  });

  // Asserts CORRECT behavior — skipped until KNOWN BUG #1 (header note) is fixed.
  itBlockedByTimestamptzBug("sync without device ownership check writes status rows under the raw device id", async () => {
    // Insecure-mode documentation test: an unscoped sync CAN write status for
    // a device id that was never registered (no FK on mobile_instance_status).
    const ghost = uid("ghost");
    const res = await syncDevice(undefined, {
      device_id: ghost,
      status_updates: [
        { instance_id: uid("inst"), state: "running", timestamp: new Date().toISOString() },
      ],
    });
    assert.equal(res.status, 200, res.text);

    const rows = await psqlRows(
      `SELECT * FROM mobile_instance_status WHERE device_id = ${sqlStr(ghost)}`,
    );
    assert.equal(rows.length, 1);
  });
});
