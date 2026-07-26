/**
 * Capability-scoped principals — the `device` capability.
 *
 * Device keys authenticate mobile clients: they may only touch the
 * `/mobile/**` family (sync, device registration, mobile approvals). The
 * suite runs with ORCH8_MOBILE_SYNC_ENABLED=1 so the family is actually
 * mounted — a device key can then be exercised on real mobile routes while
 * every other family must answer 403.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import {
  Orch8Client,
  testSequence,
  step,
  uuid,
} from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const ROOT_KEY = `root-principals-dv-${uuid().slice(0, 8)}`;

describe("Principals — device capability", () => {
  let server: ServerHandle | undefined;
  let root: Orch8Client;
  let tenantId: string;
  let device: Orch8Client;
  let deviceSecret: string;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_API_KEY: ROOT_KEY,
        ORCH8_ALLOW_NO_TENANT_ISOLATION: "1",
        ORCH8_MOBILE_SYNC_ENABLED: "1",
      },
    });
    root = new Orch8Client(`http://localhost:${server.port}`, {
      "X-API-Key": ROOT_KEY,
    });
    tenantId = `dv-${uuid().slice(0, 8)}`;
    const key = await root.createApiKey({
      tenant_id: tenantId,
      name: "device",
      capabilities: ["device"],
    });
    deviceSecret = key.secret;
    device = new Orch8Client(`http://localhost:${server.port}`, {
      "X-API-Key": deviceSecret,
    });
  });

  after(async () => {
    await stopServer(server);
  });

  function mobileFetch(
    path: string,
    init: RequestInit = {},
  ): Promise<Response> {
    return fetch(`http://localhost:${server!.port}${path}`, {
      ...init,
      headers: {
        "Content-Type": "application/json",
        "X-API-Key": deviceSecret,
        ...(init.headers ?? {}),
      },
    });
  }

  it("registers a device and lists it back through the mobile family", async () => {
    const deviceId = `dev-${uuid().slice(0, 8)}`;
    const register = await mobileFetch("/mobile/devices/register", {
      method: "POST",
      body: JSON.stringify({
        device_id: deviceId,
        platform: "ios",
        app_version: "1.0.0",
      }),
    });
    assert.ok(
      register.status === 200 || register.status === 201,
      `device registration must succeed for a device key, got ${register.status}`,
    );

    const list = await mobileFetch("/mobile/devices");
    assert.equal(list.status, 200);
    const devices = (await list.json()) as unknown;
    assert.ok(
      JSON.stringify(devices).includes(deviceId),
      "registered device visible to the same device key",
    );
  });

  it("reads mobile status and mobile approvals", async () => {
    const status = await mobileFetch("/mobile/status");
    assert.equal(status.status, 200, "GET /mobile/status allowed");

    const approvals = await mobileFetch("/mobile/approvals");
    assert.equal(approvals.status, 200, "GET /mobile/approvals allowed");
  });

  it("reaches the sync endpoint (validation error is fine — 403 is not)", async () => {
    const sync = await mobileFetch("/mobile/sync", {
      method: "POST",
      body: JSON.stringify({}),
    });
    assert.notEqual(
      sync.status,
      403,
      "device key must pass the capability gate on /mobile/sync",
    );
    assert.notEqual(sync.status, 401);
  });

  it("denies sequence reads and writes with 403", async () => {
    await assert.rejects(device.listSequences({ tenant_id: tenantId }), {
      status: 403,
    } as object);
    await assert.rejects(
      device.createSequence(
        testSequence("dv-deny", [step("s", "noop")], { tenantId }),
      ),
      { status: 403 } as object,
    );
  });

  it("denies instance creation and reads with 403", async () => {
    await assert.rejects(
      device.createInstance({
        sequence_id: uuid(),
        tenant_id: tenantId,
        namespace: "default",
      }),
      { status: 403 } as object,
    );
    await assert.rejects(device.listInstances({ tenant_id: tenantId }), {
      status: 403,
    } as object);
  });

  it("denies the worker family with 403", async () => {
    await assert.rejects(device.pollWorkerTasks("h", "w-1"), {
      status: 403,
    } as object);
  });

  it("denies the core approvals route with 403 (mobile approvals are separate)", async () => {
    await assert.rejects(device.listApprovals({ tenant_id: tenantId }), {
      status: 403,
    } as object);
  });

  it("denies key management with 403", async () => {
    await assert.rejects(device.listApiKeys(tenantId), {
      status: 403,
    } as object);
    await assert.rejects(device.revokeApiKey("ak_whatever"), {
      status: 403,
    } as object);
  });

  it("device key of tenant A cannot see tenant B's devices", async () => {
    const tenantB = `dv-b-${uuid().slice(0, 8)}`;
    const deviceIdB = `dev-b-${uuid().slice(0, 8)}`;
    // Root plants a device for tenant B by impersonating the tenant header.
    const plantB = await fetch(
      `http://localhost:${server!.port}/mobile/devices/register`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-API-Key": ROOT_KEY,
          "X-Tenant-Id": tenantB,
        },
        body: JSON.stringify({ device_id: deviceIdB, platform: "android" }),
      },
    );
    assert.ok(plantB.status === 200 || plantB.status === 201);

    const list = await mobileFetch("/mobile/devices");
    assert.equal(list.status, 200);
    assert.ok(
      !JSON.stringify(await list.json()).includes(deviceIdB),
      "tenant B device must not leak into tenant A's device listing",
    );
  });
});
