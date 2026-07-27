/**
 * Push wake outbox — enqueue persistence through the real HTTP API.
 *
 * Every `POST /mobile/commands` and every approval resolution must insert the
 * mobile command AND its `push_wake_outbox` row in one transaction
 * (`create_mobile_command_with_wake`, orch8-storage postgres/mobile_sync.rs).
 * These suites assert the durable evidence directly in Postgres.
 *
 * Engine references:
 *   - migrations/076_push_wake_outbox.sql      (base table)
 *   - migrations/079_push_wake_governance.sql  (execution/topic/collapse cols)
 *   - orch8-api/src/mobile_sync.rs             (route surface)
 *
 * NOTE: the server binary wires `NoopPushProvider` unconditionally
 * (orch8-server/src/main.rs `build_app_state`), so no row is ever claimed or
 * delivered in this environment. Delivery-side lifecycle is covered in
 * pushwake_outbox_lifecycle.test.ts only to the extent the binary allows.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
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
  psqlQuery,
  registerDevice,
  sqlStr,
  syncDevice,
  uid,
  wakeRowsForCommand,
  wakeRowsForDevice,
} from "./pushwake_helpers.ts";

describe("push wake outbox — enqueue persistence", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({ env: { ORCH8_MOBILE_SYNC_ENABLED: "true" } });
  });

  after(async () => {
    await stopServer(server);
  });

  it("command creation persists a pending wake row with zeroed lifecycle fields", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());

    const res = await createCommand(tenant, device.device_id, "refresh_state", {
      reason: "initial",
    });
    assert.equal(res.status, 201, res.text);

    const command = only(await commandRowsForDevice(device.device_id), "commands");
    assert.equal(command.command_type, "refresh_state");
    assert.equal(command.acked_at, null);

    const wake = only(await wakeRowsForDevice(device.device_id), "wakes");
    assert.equal(wake.command_id, command.id, "wake correlates to command id");
    assert.equal(wake.tenant_id, tenant);
    assert.equal(wake.device_id, device.device_id);
    assert.equal(wake.status, "pending");
    assert.equal(wake.attempts, 0);
    assert.equal(wake.next_attempt_at, null, "fresh wake is immediately due");
    assert.equal(wake.lease_until, null);
    assert.equal(wake.last_error, null);
    assert.equal(wake.terminal_reason, null);
    assert.equal(wake.delivered_at, null);
    assert.equal(wake.command_acked_at, null);
    assert.ok(wake.created_at, "created_at populated");
  });

  it("wake rows carry no collapse metadata when enqueued through the plain command API", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    await createCommand(tenant, device.device_id, "ping", {});

    const wake = only(await wakeRowsForDevice(device.device_id), "wakes");
    assert.equal(wake.execution_id, null);
    assert.equal(wake.topic, null);
    assert.equal(wake.collapse_key, null);
    assert.equal(wake.superseded_by, null);
  });

  it("each command gets its own wake row; ids and correlation stay 1:1", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());

    for (let i = 0; i < 5; i++) {
      const res = await createCommand(tenant, device.device_id, "step_hint", { i });
      assert.equal(res.status, 201, res.text);
    }

    const commands = await commandRowsForDevice(device.device_id);
    const wakes = await wakeRowsForDevice(device.device_id);
    assert.equal(commands.length, 5);
    assert.equal(wakes.length, 5);
    assert.deepEqual(
      wakes.map((w) => w.command_id).sort(),
      commands.map((c) => c.id).sort(),
      "every command id appears exactly once in the outbox",
    );
    assert.equal(
      new Set(wakes.map((w) => w.id)).size,
      5,
      "wake ids are unique",
    );
    assert.ok(
      wakes.every((w) => w.status === "pending" && w.attempts === 0),
      "all fresh wakes are pending with zero attempts",
    );
  });

  it("approval resolution enqueues a complete_step command AND its wake", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());

    const syncRes = await syncDevice(tenant, {
      device_id: device.device_id,
      approval_requests: [
        {
          instance_id: uid("inst"),
          block_id: "approve-deploy",
          prompt: "Ship it?",
          choices: ["yes", "no"],
        },
      ],
    });
    assert.equal(syncRes.status, 200, syncRes.text);

    const approvals = await get(`/mobile/approvals?tenant_id=${tenant}`, tenant);
    assert.equal(approvals.status, 200);
    assert.equal(approvals.body.items.length, 1);
    const approvalId = approvals.body.items[0].id;

    const resolveRes = await post(
      `/mobile/approvals/${approvalId}/resolve`,
      { output: { choice: "yes" } },
      tenant,
    );
    assert.equal(resolveRes.status, 200, resolveRes.text);

    const command = only(await commandRowsForDevice(device.device_id), "commands");
    assert.equal(command.command_type, "complete_step");
    const payload = JSON.parse(command.payload);
    assert.equal(payload.output.choice, "yes");

    const wake = only(await wakeRowsForCommand(command.id), "wakes");
    assert.equal(wake.tenant_id, tenant);
    assert.equal(wake.status, "pending");
  });

  it("step delegations produce step_result commands WITHOUT a wake row", async () => {
    // Documents an asymmetry in `handle_sync`: delegation results go through
    // `create_mobile_command` (no wake), while /mobile/commands and approval
    // resolutions go through `create_mobile_command_with_wake`. A device that
    // delegated a step is actively syncing, so no wake is arguably needed —
    // but the outbox then cannot evidence this delivery path.
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());

    const res = await syncDevice(tenant, {
      device_id: device.device_id,
      step_delegations: [
        {
          request_id: uid("req"),
          instance_id: uid("inst"),
          block_id: "b1",
          handler: "noop",
          params: { plain: "value" },
        },
      ],
    });
    assert.equal(res.status, 200, res.text);

    const command = only(await commandRowsForDevice(device.device_id), "commands");
    assert.equal(command.command_type, "step_result");

    const wakes = await wakeRowsForDevice(device.device_id);
    assert.equal(
      wakes.length,
      0,
      "delegation step_result commands currently bypass the wake outbox",
    );
  });

  it("wakes are enqueued even for devices registered without a push token", async () => {
    const tenant = uid("t");
    const spec = deviceSpec();
    delete spec.push_token;
    const device = await mustRegisterDevice(tenant, spec);

    const res = await createCommand(tenant, device.device_id, "ping", {});
    assert.equal(res.status, 201, res.text);

    const wake = only(await wakeRowsForDevice(device.device_id), "wakes");
    assert.equal(wake.status, "pending");
  });

  it("rejected command creation (unknown device) persists neither command nor wake", async () => {
    const tenant = uid("t");
    const ghost = uid("ghost");

    const res = await createCommand(tenant, ghost, "ping", {});
    assert.equal(res.status, 404, res.text);

    const commands = await commandRowsForDevice(ghost);
    const wakes = await wakeRowsForDevice(ghost);
    assert.equal(commands.length, 0);
    assert.equal(wakes.length, 0);
  });

  it("rejected cross-tenant command creation persists nothing for the target device", async () => {
    const tenantA = uid("ta");
    const tenantB = uid("tb");
    const device = await mustRegisterDevice(tenantA, deviceSpec());

    const res = await createCommand(tenantB, device.device_id, "inject", {
      evil: true,
    });
    assert.equal(res.status, 404, "cross-tenant command injection must be refused");

    const commands = await commandRowsForDevice(device.device_id);
    const wakes = await wakeRowsForDevice(device.device_id);
    assert.equal(commands.length, 0);
    assert.equal(wakes.length, 0);
  });

  it("command payload is stored as raw JSON text and round-trips through sync", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    const marker = uid("marker");

    await createCommand(tenant, device.device_id, "custom", {
      nested: { marker, list: [1, 2, 3] },
    });

    const command = only(await commandRowsForDevice(device.device_id), "commands");
    const stored = JSON.parse(command.payload);
    assert.equal(stored.nested.marker, marker);
    assert.deepEqual(stored.nested.list, [1, 2, 3]);

    const syncRes = await syncDevice(tenant, { device_id: device.device_id });
    assert.equal(syncRes.status, 200);
    assert.equal(syncRes.body.commands.length, 1);
    assert.equal(syncRes.body.commands[0].payload.nested.marker, marker);
  });

  it("wake rows survive device re-registration (token rotation does not rewrite the outbox)", async () => {
    const tenant = uid("t");
    const spec = deviceSpec();
    const device = await mustRegisterDevice(tenant, spec);
    await createCommand(tenant, device.device_id, "ping", {});
    const before = only(await wakeRowsForDevice(device.device_id), "wakes");

    const reRegister = await registerDevice(tenant, {
      ...spec,
      push_token: `tok-${crypto.randomUUID()}`,
      app_version: "2.0.0",
    });
    assert.equal(reRegister.status, 201, reRegister.text);

    const after = only(await wakeRowsForDevice(device.device_id), "wakes");
    assert.equal(after.id, before.id, "re-registration must not duplicate wakes");
    assert.equal(after.status, "pending");
    assert.equal(after.command_acked_at, before.command_acked_at);
  });

  it("outbox unique constraint rejects a duplicate (tenant, device, command) tuple", async () => {
    // Storage-level idempotency boundary: `enqueue_wake` uses
    // ON CONFLICT DO NOTHING, which only works because this UNIQUE exists.
    // Exercise the constraint directly so a migration regression (dropped
    // constraint) fails loudly here instead of silently double-delivering.
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    await createCommand(tenant, device.device_id, "ping", {});
    const wake = only(await wakeRowsForDevice(device.device_id), "wakes");

    const dupOut = psqlQuery(
      `INSERT INTO push_wake_outbox (id, tenant_id, device_id, command_id, created_at)
       VALUES (gen_random_uuid(), ${sqlStr(wake.tenant_id)}, ${sqlStr(wake.device_id)}, ${sqlStr(wake.command_id)}, now())
       ON CONFLICT (tenant_id, device_id, command_id) DO NOTHING
       RETURNING id`,
    );
    // No RETURNING row output: psql prints only the command tag, and the
    // `0 0` in it confirms zero rows were inserted by the duplicate.
    assert.equal(dupOut, "INSERT 0 0", "duplicate tuple must be absorbed by ON CONFLICT");

    const wakes = await wakeRowsForDevice(device.device_id);
    assert.equal(wakes.length, 1, "still exactly one row after duplicate enqueue");
    assert.equal(only(wakes).id, wake.id, "original row identity preserved");
  });
});
