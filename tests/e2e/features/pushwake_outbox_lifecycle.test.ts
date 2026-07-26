/**
 * Push wake outbox — lifecycle, ack correlation, and drain guarantees.
 *
 * The shipped binary hard-wires `NoopPushProvider`
 * (orch8-server/src/main.rs `build_app_state`: `push_provider` is always
 * `Arc::new(orch8_push::NoopPushProvider)`), and `PushOutboxWorker::drain_once`
 * returns early without claiming when `provider.is_configured()` is false.
 * docs/PUSH_DELIVERY.md states this as a guarantee: "When no real provider is
 * configured, the worker leaves rows pending; a no-op provider never
 * manufactures successful delivery evidence."
 *
 * These tests pin that guarantee against the real 1-second worker loop, and
 * cover the ack-correlation path (`record_command_acks`) that IS reachable:
 * `/mobile/sync` writes `command_acked_at` onto matching outbox rows.
 *
 * Also covers durable invariants that survive simulated worker-crash states
 * (expired leases, terminal rows) injected directly into Postgres.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import {
  commandRowsForDevice,
  createCommand,
  deviceSpec,
  mustRegisterDevice,
  psqlExec,
  psqlRows,
  sleep,
  sqlStr,
  syncDevice,
  uid,
  only,
  wakeRowsForDevice,
  type WakeRow,
} from "./pushwake_helpers.ts";

/** Wait longer than two worker ticks (interval = 1s). */
async function waitForDrainTicks(): Promise<void> {
  await sleep(2_500);
}

describe("push wake outbox — lifecycle and drain guarantees", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({ env: { ORCH8_MOBILE_SYNC_ENABLED: "true" } });
  });

  after(async () => {
    await stopServer(server);
  });

  it("noop provider never claims due wakes: rows stay pending with zero attempts", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    await createCommand(tenant, device.device_id, "ping", {});
    await createCommand(tenant, device.device_id, "ping", {});

    await waitForDrainTicks();

    const wakes = await wakeRowsForDevice(device.device_id);
    assert.equal(wakes.length, 2);
    for (const wake of wakes) {
      assert.equal(wake.status, "pending", "unconfigured provider must not claim");
      assert.equal(wake.attempts, 0, "no attempt may be recorded");
      assert.equal(wake.lease_until, null, "no lease may be written");
      assert.equal(wake.delivered_at, null, "no delivery evidence may be manufactured");
      assert.equal(wake.last_error, null);
      assert.equal(wake.terminal_reason, null);
    }
  });

  it("overdue retries (next_attempt_at in the past) are still not claimed without a provider", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    await createCommand(tenant, device.device_id, "ping", {});

    // Simulate a wake that a previous worker generation already retried once:
    // due again (next_attempt_at well in the past), attempts > 0.
    psqlExec(
      `UPDATE push_wake_outbox
       SET attempts = 3, next_attempt_at = now() - interval '10 minutes', last_error = 'simulated transient failure'
       WHERE device_id = ${sqlStr(device.device_id)}`,
    );

    await waitForDrainTicks();

    const wake = only(await wakeRowsForDevice(device.device_id), "wakes");
    assert.equal(wake.status, "pending");
    assert.equal(wake.attempts, 3, "worker must not bump attempts while unconfigured");
    assert.equal(wake.lease_until, null);
    assert.equal(wake.last_error, "simulated transient failure");
  });

  it("expired in-flight leases (worker crash) are not reclaimed without a provider", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    await createCommand(tenant, device.device_id, "ping", {});

    // Simulate a worker that claimed the row and died mid-delivery: lease
    // long expired. The documented recovery path would re-claim it after
    // lease expiry — but only a configured provider may do so.
    psqlExec(
      `UPDATE push_wake_outbox
       SET status = 'in_flight', lease_until = now() - interval '1 hour'
       WHERE device_id = ${sqlStr(device.device_id)}`,
    );

    await waitForDrainTicks();

    const wake = only(await wakeRowsForDevice(device.device_id), "wakes");
    assert.equal(wake.status, "in_flight", "noop drain must leave crashed leases untouched");
    assert.equal(wake.attempts, 0);
    assert.ok(wake.lease_until !== null, "lease evidence preserved for a future worker");
  });

  it("sync ack correlates command acknowledgement onto the wake row", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    await createCommand(tenant, device.device_id, "refresh", { n: 1 });

    const sync1 = await syncDevice(tenant, { device_id: device.device_id });
    assert.equal(sync1.status, 200, sync1.text);
    assert.equal(sync1.body.commands.length, 1);
    const commandId = sync1.body.commands[0].id;

    const sync2 = await syncDevice(tenant, {
      device_id: device.device_id,
      command_acks: [commandId],
    });
    assert.equal(sync2.status, 200, sync2.text);
    assert.equal(sync2.body.commands.length, 0, "acked command must not be redelivered");

    const wake = only(await wakeRowsForDevice(device.device_id), "wakes");
    assert.equal(wake.command_id, commandId);
    assert.ok(
      wake.command_acked_at !== null,
      "command_acked_at distinguishes device ack from provider acceptance",
    );
    assert.equal(wake.delivered_at, null, "provider acceptance still absent (noop provider)");

    const command = only(await commandRowsForDevice(device.device_id), "commands");
    assert.ok(command.acked_at !== null, "command row itself marked acked");
  });

  it("acking one of several commands only touches that command's wake row", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    await createCommand(tenant, device.device_id, "a", {});
    await createCommand(tenant, device.device_id, "b", {});
    await createCommand(tenant, device.device_id, "c", {});

    const sync1 = await syncDevice(tenant, { device_id: device.device_id });
    assert.equal(sync1.body.commands.length, 3);
    const acked = sync1.body.commands[1].id;

    await syncDevice(tenant, { device_id: device.device_id, command_acks: [acked] });

    const wakes = await wakeRowsForDevice(device.device_id);
    const byId = new Map(wakes.map((w) => [w.command_id, w]));
    assert.ok(byId.get(acked)?.command_acked_at !== null, "acked command correlated");
    for (const [id, wake] of byId) {
      if (id !== acked) {
        assert.equal(wake.command_acked_at, null, `command ${id} must remain un-acked`);
      }
    }

    const sync2 = await syncDevice(tenant, { device_id: device.device_id });
    const deliveredIds = sync2.body.commands.map((c: { id: string }) => c.id);
    assert.equal(deliveredIds.length, 2);
    assert.ok(!deliveredIds.includes(acked), "acked command removed from pending queue");
  });

  it("acking unknown command ids is a silent no-op on the outbox", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    await createCommand(tenant, device.device_id, "ping", {});

    const res = await syncDevice(tenant, {
      device_id: device.device_id,
      command_acks: [uid("nope"), uid("nope")],
    });
    assert.equal(res.status, 200, res.text);

    const wake = only(await wakeRowsForDevice(device.device_id), "wakes");
    assert.equal(wake.command_acked_at, null);
  });

  it("re-acking an already-acked command stays 200 and keeps the wake correlated", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    await createCommand(tenant, device.device_id, "ping", {});

    const sync1 = await syncDevice(tenant, { device_id: device.device_id });
    const commandId = sync1.body.commands[0].id;
    await syncDevice(tenant, { device_id: device.device_id, command_acks: [commandId] });

    const again = await syncDevice(tenant, {
      device_id: device.device_id,
      command_acks: [commandId],
    });
    assert.equal(again.status, 200, again.text);

    const wake = only(await wakeRowsForDevice(device.device_id), "wakes");
    assert.ok(wake.command_acked_at !== null);
    const command = only(await commandRowsForDevice(device.device_id), "commands");
    assert.ok(command.acked_at !== null);
  });

  it("ack correlation is status-agnostic: terminal wake rows still receive command_acked_at", async () => {
    // record_command_acks has no status filter — a wake already parked as
    // terminal (e.g. superseded by a collapse) still collects the device ack.
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    await createCommand(tenant, device.device_id, "ping", {});
    const command = only(await commandRowsForDevice(device.device_id), "commands");

    psqlExec(
      `UPDATE push_wake_outbox
       SET status = 'terminal', terminal_reason = 'superseded', superseded_by = 'newer-command'
       WHERE device_id = ${sqlStr(device.device_id)}`,
    );

    const res = await syncDevice(tenant, {
      device_id: device.device_id,
      command_acks: [command.id],
    });
    assert.equal(res.status, 200, res.text);

    const wake = only(await wakeRowsForDevice(device.device_id), "wakes");
    assert.equal(wake.status, "terminal", "ack must not resurrect a terminal wake");
    assert.equal(wake.terminal_reason, "superseded");
    assert.ok(wake.command_acked_at !== null, "ack still correlated for evidence");
  });

  it("terminal wake rows do not block delivery of the command mailbox itself", async () => {
    // The outbox is a hint channel; the durable command queue is authoritative.
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    await createCommand(tenant, device.device_id, "important", {});

    psqlExec(
      `UPDATE push_wake_outbox
       SET status = 'terminal', terminal_reason = 'invalid_token'
       WHERE device_id = ${sqlStr(device.device_id)}`,
    );

    const res = await syncDevice(tenant, { device_id: device.device_id });
    assert.equal(res.status, 200, res.text);
    assert.equal(res.body.commands.length, 1, "command still delivered despite terminal wake");
    assert.equal(res.body.commands[0].type, "important");
  });

  it("a device deactivated after invalid-token quarantine keeps its pending commands", async () => {
    // On terminal invalid_token the storage layer deactivates the device and
    // clears its token (record_wake_outcome in postgres/push_outbox.rs).
    // Simulate that end state and assert the command queue remains drainable
    // via sync (push is only a hint).
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    await createCommand(tenant, device.device_id, "still-needed", {});

    psqlExec(
      `UPDATE mobile_devices SET active = FALSE, push_token = NULL WHERE device_id = ${sqlStr(device.device_id)}`,
    );
    psqlExec(
      `UPDATE push_wake_outbox
       SET status = 'terminal', terminal_reason = 'invalid_token', last_error = 'device token rejected by provider'
       WHERE device_id = ${sqlStr(device.device_id)}`,
    );

    const res = await syncDevice(tenant, { device_id: device.device_id });
    assert.equal(res.status, 200, res.text);
    assert.equal(res.body.commands.length, 1, "quarantine must not destroy the command queue");
  });

  it("outbox rows for many tenants/devices stay isolated by (tenant_id, device_id)", async () => {
    const tenantA = uid("ta");
    const tenantB = uid("tb");
    const deviceA = await mustRegisterDevice(tenantA, deviceSpec());
    const deviceB = await mustRegisterDevice(tenantB, deviceSpec());

    await createCommand(tenantA, deviceA.device_id, "ping", { owner: "a" });
    await createCommand(tenantB, deviceB.device_id, "ping", { owner: "b" });

    const wakesA = await wakeRowsForDevice(deviceA.device_id);
    const wakesB = await wakeRowsForDevice(deviceB.device_id);
    assert.equal(wakesA.length, 1);
    assert.equal(wakesB.length, 1);
    const wakeA = only(wakesA, "tenant A wakes");
    const wakeB = only(wakesB, "tenant B wakes");
    assert.equal(wakeA.tenant_id, tenantA);
    assert.equal(wakeB.tenant_id, tenantB);
    assert.notEqual(wakeA.command_id, wakeB.command_id);

    // Cross-check at the SQL level: no wake row may join a device row whose
    // tenant disagrees (the claim query relies on this join).
    const mismatched = await psqlRows<WakeRow>(
      `SELECT o.* FROM push_wake_outbox o
       JOIN mobile_devices d ON d.device_id = o.device_id
       WHERE o.device_id IN (${sqlStr(deviceA.device_id)}, ${sqlStr(deviceB.device_id)})
         AND d.tenant_id <> o.tenant_id`,
    );
    assert.equal(mismatched.length, 0, "wake tenant must always match device tenant");
  });

  it("wake created_at is monotonic with enqueue order for one device", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    for (let i = 0; i < 3; i++) {
      await createCommand(tenant, device.device_id, "seq", { i });
      await sleep(20); // ensure distinct timestamps at ms resolution
    }

    const wakes = await wakeRowsForDevice(device.device_id);
    assert.equal(wakes.length, 3);
    const created = wakes.map((w) => new Date(w.created_at).getTime());
    const [c0, c1, c2] = created as [number, number, number];
    assert.ok(
      c0 <= c1 && c1 <= c2,
      `created_at must follow enqueue order: ${created}`,
    );
    // created_at is the API-side enqueue timestamp, distinct from the
    // server-default now() used by raw inserts — sanity check it's recent.
    assert.ok(Date.now() - c2 < 60_000, "created_at must be wall-clock recent");
  });
});
