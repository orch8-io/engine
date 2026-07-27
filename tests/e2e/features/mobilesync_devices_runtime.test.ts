/**
 * Mobile devices + runtime capability registration.
 *
 * Covers:
 *   - POST /mobile/devices/register (upsert semantics, conflict rules),
 *   - GET /mobile/devices (scoping, limit clamp),
 *   - POST /mobile/devices/{id}/runtime — device capability mesh join with
 *     validation (kind, expiry window, battery, trust, fact bounds) from
 *     orch8-api/src/continuity.rs validate_runtime_registration.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import {
  deviceSpec,
  get,
  mustRegisterDevice,
  post,
  registerDevice,
  uid,
  type DeviceSpec,
} from "./pushwake_helpers.ts";

const client = new Orch8Client();

function mobileCapabilities(
  runtimeId: string,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  const now = Date.now();
  return {
    runtime_id: runtimeId,
    kind: "mobile",
    trust: "registered",
    handlers: ["noop"],
    offline_capable: true,
    connectivity: "wifi",
    battery_percent: 80,
    observed_at: new Date(now).toISOString(),
    expires_at: new Date(now + 60_000).toISOString(),
    ...overrides,
  };
}

describe("mobile devices — registration and runtime capabilities", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({ env: { ORCH8_MOBILE_SYNC_ENABLED: "true" } });
  });

  after(async () => {
    await stopServer(server);
  });

  it("registers a device and lists it with all stored fields", async () => {
    const tenant = uid("t");
    const spec: DeviceSpec = {
      device_id: uid("dev"),
      push_token: `tok-${crypto.randomUUID()}`,
      platform: "android",
      app_version: "3.2.1",
    };
    const res = await registerDevice(tenant, spec);
    assert.equal(res.status, 201, res.text);

    const list = await get(`/mobile/devices?tenant_id=${tenant}`, tenant);
    assert.equal(list.status, 200, list.text);
    const device = list.body.items.find(
      (d: { device_id: string }) => d.device_id === spec.device_id,
    );
    assert.ok(device, "device listed");
    assert.equal(device.tenant_id, tenant);
    assert.equal(device.push_token, spec.push_token);
    assert.equal(device.platform, "android");
    assert.equal(device.app_version, "3.2.1");
    assert.equal(device.active, true);
    assert.equal(device.last_sync_at, null);
    assert.ok(device.registered_at, "registered_at populated by the DB default");
  });

  it("registers a device without a push token (wifi-only / simulator)", async () => {
    const tenant = uid("t");
    const spec = deviceSpec();
    delete spec.push_token;
    const res = await registerDevice(tenant, spec);
    assert.equal(res.status, 201, res.text);

    const list = await get(`/mobile/devices?tenant_id=${tenant}`, tenant);
    const device = list.body.items.find(
      (d: { device_id: string }) => d.device_id === spec.device_id,
    );
    assert.equal(device.push_token, null);
    assert.equal(device.active, true);
  });

  it("device listing clamps the limit parameter to the server maximum", async () => {
    const tenant = uid("t");
    for (let i = 0; i < 3; i++) {
      await mustRegisterDevice(tenant, deviceSpec());
    }

    const absurd = await get(`/mobile/devices?tenant_id=${tenant}&limit=99999`, tenant);
    assert.equal(absurd.status, 200, absurd.text);
    assert.ok(absurd.body.items.length >= 3, "clamped limit still returns all devices");

    const exact = await get(`/mobile/devices?tenant_id=${tenant}&limit=1`, tenant);
    assert.equal(exact.status, 200);
    assert.equal(exact.body.items.length, 1, "limit is honoured");
    assert.equal(exact.body.total, 1);
  });

  it("devices are not listed cross-tenant", async () => {
    const tenantA = uid("ta");
    const tenantB = uid("tb");
    const specA = await mustRegisterDevice(tenantA, deviceSpec());
    await mustRegisterDevice(tenantB, deviceSpec());

    const listB = await get(`/mobile/devices?tenant_id=${tenantB}`, tenantB);
    const leaked = listB.body.items.filter(
      (d: { device_id: string }) => d.device_id === specA.device_id,
    );
    assert.equal(leaked.length, 0);
  });

  it("runtime registration joins the capability mesh and appends the device fact", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    const runtimeId = crypto.randomUUID();

    const res = await post(
      `/mobile/devices/${device.device_id}/runtime`,
      { capabilities: mobileCapabilities(runtimeId) },
      tenant,
    );
    assert.equal(res.status, 201, res.text);
    assert.equal(res.body.runtime_id, runtimeId);
    assert.ok(
      res.body.hardware.includes(`device:${device.device_id}`),
      "server appends the device:<id> hardware fact",
    );

    const runtimes = await client.listRuntimes(tenant);
    const mine = runtimes.find(
      (r: { capabilities?: { runtime_id?: string }; runtime_id?: string }) =>
        (r.capabilities?.runtime_id ?? r.runtime_id) === runtimeId,
    );
    assert.ok(mine, "runtime discoverable via /runtimes after mesh join");
  });

  it("runtime registration rejects a non-mobile kind", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());

    const res = await post(
      `/mobile/devices/${device.device_id}/runtime`,
      { capabilities: mobileCapabilities(crypto.randomUUID(), { kind: "server" }) },
      tenant,
    );
    assert.equal(res.status, 400, res.text);
    assert.match(res.text, /kind `mobile`/);
  });

  it("runtime registration validates the observation/expiry window", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    const now = Date.now();

    const expired = await post(
      `/mobile/devices/${device.device_id}/runtime`,
      {
        capabilities: mobileCapabilities(crypto.randomUUID(), {
          observed_at: new Date(now - 120_000).toISOString(),
          expires_at: new Date(now - 60_000).toISOString(),
        }),
      },
      tenant,
    );
    assert.equal(expired.status, 400, expired.text);
    assert.match(expired.text, /expiry must be in the future/);

    const tooLong = await post(
      `/mobile/devices/${device.device_id}/runtime`,
      {
        capabilities: mobileCapabilities(crypto.randomUUID(), {
          expires_at: new Date(now + 10 * 60_000).toISOString(),
        }),
      },
      tenant,
    );
    assert.equal(tooLong.status, 400, tooLong.text);
    assert.match(tooLong.text, /no longer than five minutes/);

    const futureObserved = await post(
      `/mobile/devices/${device.device_id}/runtime`,
      {
        capabilities: mobileCapabilities(crypto.randomUUID(), {
          observed_at: new Date(now + 60_000).toISOString(),
          expires_at: new Date(now + 120_000).toISOString(),
        }),
      },
      tenant,
    );
    assert.equal(futureObserved.status, 400, futureObserved.text);
    assert.match(futureObserved.text, /too far in the future/);
  });

  it("runtime registration rejects battery > 100 and elevated trust", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());

    const battery = await post(
      `/mobile/devices/${device.device_id}/runtime`,
      { capabilities: mobileCapabilities(crypto.randomUUID(), { battery_percent: 101 }) },
      tenant,
    );
    assert.equal(battery.status, 400, battery.text);
    assert.match(battery.text, /battery_percent/);

    const trust = await post(
      `/mobile/devices/${device.device_id}/runtime`,
      { capabilities: mobileCapabilities(crypto.randomUUID(), { trust: "signed" }) },
      tenant,
    );
    assert.equal(trust.status, 400, trust.text);
    assert.match(trust.text, /attestation/);
  });

  it("runtime registration on an unknown device is 404; cross-tenant is also 404", async () => {
    const tenantA = uid("ta");
    const tenantB = uid("tb");
    const device = await mustRegisterDevice(tenantA, deviceSpec());

    const unknown = await post(
      `/mobile/devices/${uid("ghost")}/runtime`,
      { capabilities: mobileCapabilities(crypto.randomUUID()) },
      tenantA,
    );
    assert.equal(unknown.status, 404, unknown.text);

    const crossTenant = await post(
      `/mobile/devices/${device.device_id}/runtime`,
      { capabilities: mobileCapabilities(crypto.randomUUID()) },
      tenantB,
    );
    assert.equal(crossTenant.status, 404, crossTenant.text);
  });

  it("repeated runtime registration upserts capabilities for the device", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    const runtimeId = crypto.randomUUID();

    const first = await post(
      `/mobile/devices/${device.device_id}/runtime`,
      { capabilities: mobileCapabilities(runtimeId, { handlers: ["noop"] }) },
      tenant,
    );
    assert.equal(first.status, 201, first.text);

    const second = await post(
      `/mobile/devices/${device.device_id}/runtime`,
      { capabilities: mobileCapabilities(runtimeId, { handlers: ["noop", "http_request"] }) },
      tenant,
    );
    assert.equal(second.status, 201, second.text);

    const runtimes = await client.listRuntimes(tenant);
    const mine = runtimes.filter(
      (r: { capabilities?: { runtime_id?: string }; runtime_id?: string }) =>
        (r.capabilities?.runtime_id ?? r.runtime_id) === runtimeId,
    );
    assert.equal(mine.length, 1, "re-registration upserts instead of duplicating");
    const handlers = mine[0].capabilities?.handlers ?? mine[0].handlers;
    assert.deepEqual(handlers, ["noop", "http_request"]);
  });
});
