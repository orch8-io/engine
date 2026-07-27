/**
 * Push wake + mobile routes — tenant isolation.
 *
 * The mobile module "falls open" only when no tenant context exists
 * (insecure mode without X-Tenant-Id). With a tenant header, every route
 * must scope strictly: registration conflicts, cross-tenant command
 * injection, cross-tenant sync pulls, and cross-tenant approval resolution
 * are all refused, and the outbox rows always bind the DEVICE's tenant —
 * never the caller's.
 *
 * Engine reference: ownership checks in orch8-api/src/mobile_sync.rs
 * (`register_device`, `handle_sync`, `create_command`, `resolve_approval`).
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
  only,
  mustRegisterDevice,
  post,
  psqlExec,
  registerDevice,
  sqlStr,
  syncDevice,
  uid,
  wakeRowsForDevice,
} from "./pushwake_helpers.ts";

describe("push wake — tenant isolation", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({ env: { ORCH8_MOBILE_SYNC_ENABLED: "true" } });
  });

  after(async () => {
    await stopServer(server);
  });

  it("re-registering another tenant's device_id is a 409 conflict and changes nothing", async () => {
    const tenantA = uid("ta");
    const tenantB = uid("tb");
    const spec = deviceSpec();
    await mustRegisterDevice(tenantA, spec);

    const hijack = await registerDevice(tenantB, {
      ...spec,
      push_token: `tok-${crypto.randomUUID()}`,
      platform: "android",
    });
    assert.equal(hijack.status, 409, hijack.text);

    const listA = await get(`/mobile/devices?tenant_id=${tenantA}`, tenantA);
    const [device] = listA.body.items;
    assert.equal(device.push_token, spec.push_token, "token not overwritten");
    assert.equal(device.platform, "ios", "platform not overwritten");
    assert.equal(device.tenant_id, tenantA);
  });

  it("same-tenant re-registration is an idempotent upsert", async () => {
    const tenant = uid("t");
    const spec = deviceSpec();
    await mustRegisterDevice(tenant, spec);

    const rotated = `tok-${crypto.randomUUID()}`;
    const res = await registerDevice(tenant, { ...spec, push_token: rotated });
    assert.equal(res.status, 201, res.text);

    const list = await get(`/mobile/devices?tenant_id=${tenant}`, tenant);
    const mine = list.body.items.filter(
      (d: { device_id: string }) => d.device_id === spec.device_id,
    );
    assert.equal(mine.length, 1, "upsert must not duplicate the device row");
    assert.equal(mine[0].push_token, rotated);
  });

  it("cross-tenant command creation is 404 and writes no wake for the attacker tenant", async () => {
    const tenantA = uid("ta");
    const tenantB = uid("tb");
    const device = await mustRegisterDevice(tenantA, deviceSpec());

    const res = await createCommand(tenantB, device.device_id, "complete_step", {
      output: { forged: true },
    });
    assert.equal(res.status, 404, "must look like the device does not exist");

    const wakes = await wakeRowsForDevice(device.device_id);
    assert.equal(wakes.length, 0);
    const commands = await commandRowsForDevice(device.device_id);
    assert.equal(commands.length, 0);
  });

  it("cross-tenant sync cannot pull or ack the victim's command queue", async () => {
    const tenantA = uid("ta");
    const tenantB = uid("tb");
    const device = await mustRegisterDevice(tenantA, deviceSpec());
    await createCommand(tenantA, device.device_id, "step_result", {
      resolved_params: { secret: "credential-material" },
    });

    const pull = await syncDevice(tenantB, { device_id: device.device_id });
    assert.equal(pull.status, 404, pull.text);

    const command = only(await commandRowsForDevice(device.device_id), "commands");
    const ack = await syncDevice(tenantB, {
      device_id: device.device_id,
      command_acks: [command.id],
    });
    assert.equal(ack.status, 404, "ack attempt must also be refused before any mutation");

    const wake = only(await wakeRowsForDevice(device.device_id), "wakes");
    assert.equal(wake.command_acked_at, null, "no ack evidence written cross-tenant");
    const stillPending = only(await commandRowsForDevice(device.device_id), "commands");
    assert.equal(stillPending.acked_at, null, "command itself still pending");
  });

  it("cross-tenant approval resolution is refused with 404", async () => {
    const tenantA = uid("ta");
    const tenantB = uid("tb");
    const device = await mustRegisterDevice(tenantA, deviceSpec());
    await syncDevice(tenantA, {
      device_id: device.device_id,
      approval_requests: [{ instance_id: uid("inst"), block_id: "b1" }],
    });
    const approvals = await get(`/mobile/approvals?tenant_id=${tenantA}`, tenantA);
    const approvalId = approvals.body.items[0].id;

    const res = await post(
      `/mobile/approvals/${approvalId}/resolve`,
      { output: { choice: "forged" } },
      tenantB,
    );
    assert.equal(res.status, 404, res.text);

    const commands = await commandRowsForDevice(device.device_id);
    assert.equal(commands.length, 0, "forged resolution must not enqueue a command");
    const wakes = await wakeRowsForDevice(device.device_id);
    assert.equal(wakes.length, 0);
  });

  it("wake rows always bind the device's tenant even when the caller omits the header", async () => {
    // In insecure mode without X-Tenant-Id the module falls open — but
    // `create_command` still persists the wake under the DEVICE's tenant,
    // not under an empty tenant. The claim join depends on this.
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());

    // No X-Tenant-Id header at all (insecure-mode fall-open).
    const res = await post(
      "/mobile/commands",
      { device_id: device.device_id, command_type: "ping", payload: {} },
      undefined,
    );
    assert.equal(res.status, 201, res.text);

    const wake = only(await wakeRowsForDevice(device.device_id), "wakes");
    assert.equal(wake.tenant_id, tenant, "wake binds the device's tenant, not the caller's");
  });

  it("device listing honours the tenant header over the query parameter", async () => {
    const tenantA = uid("ta");
    const tenantB = uid("tb");
    const specA = await mustRegisterDevice(tenantA, deviceSpec());
    await mustRegisterDevice(tenantB, deviceSpec());

    // Header for A, query param for B: header must win (scoped listing).
    const res = await get(`/mobile/devices?tenant_id=${tenantB}`, tenantA);
    assert.equal(res.status, 200, res.text);
    const ids = res.body.items.map((d: { device_id: string }) => d.device_id);
    assert.ok(ids.includes(specA.device_id), "own device visible");
    for (const item of res.body.items) {
      assert.equal(item.tenant_id, tenantA, "header scopes the listing");
    }
  });

  it("approvals listing is scoped to the requesting tenant", async () => {
    const tenantA = uid("ta");
    const tenantB = uid("tb");
    const deviceA = await mustRegisterDevice(tenantA, deviceSpec());
    const deviceB = await mustRegisterDevice(tenantB, deviceSpec());

    await syncDevice(tenantA, {
      device_id: deviceA.device_id,
      approval_requests: [{ instance_id: uid("inst"), block_id: "ba" }],
    });
    await syncDevice(tenantB, {
      device_id: deviceB.device_id,
      approval_requests: [{ instance_id: uid("inst"), block_id: "bb" }],
    });

    const listA = await get(`/mobile/approvals?tenant_id=${tenantA}`, tenantA);
    assert.equal(listA.body.items.length, 1);
    assert.equal(listA.body.items[0].block_id, "ba");
    assert.equal(listA.body.items[0].tenant_id, tenantA);

    const listB = await get(`/mobile/approvals?tenant_id=${tenantB}`, tenantB);
    assert.equal(listB.body.items.length, 1);
    assert.equal(listB.body.items[0].block_id, "bb");
  });

  it("status listing cannot be enumerated cross-tenant via device_id probe", async () => {
    const tenantA = uid("ta");
    const tenantB = uid("tb");
    const device = await mustRegisterDevice(tenantA, deviceSpec());
    // Known server bug: /mobile/sync binds updated_at as TEXT into the
    // TIMESTAMPTZ column and 500s, so the sync-based setup cannot plant
    // tenant A's status row. Insert it directly via SQL instead — without
    // a real row, the leaked.length === 0 assertion below is vacuous.
    psqlExec(
      `INSERT INTO mobile_instance_status (device_id, instance_id, state, updated_at)
       VALUES (${sqlStr(device.device_id)}, ${sqlStr(uid("inst"))}, 'running', now())`,
    );

    const res = await get(
      `/mobile/status?tenant_id=${tenantB}&device_id=${device.device_id}`,
      tenantB,
    );
    assert.equal(res.status, 200, res.text);
    const leaked = res.body.items.filter(
      (s: { device_id: string }) => s.device_id === device.device_id,
    );
    assert.equal(leaked.length, 0, "tenant B must not see tenant A device status");
  });
});
