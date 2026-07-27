/**
 * Push wake / mobile command payload encryption at rest.
 *
 * When `ORCH8_ENCRYPTION_KEY` is configured, `EncryptingStorage`
 * (orch8-storage/src/encrypting.rs) encrypts mobile command payloads on the
 * way in (`create_mobile_command` / `create_mobile_command_with_wake`,
 * :2152-2175) and decrypts them on the way out (`fetch_pending_commands`,
 * :2177-2185). Payloads can carry resolved credentials (a step delegation's
 * params), so the plaintext must never touch Postgres.
 *
 * These tests assert, against the real server + Postgres:
 *   - `mobile_commands.payload` is ciphertext (`enc:v1:` prefix, marker absent)
 *     for BOTH creation paths (plain /mobile/commands and sync step delegations)
 *   - `/mobile/sync` returns the decrypted payload (marker present)
 *   - the wake-claim/ack passthrough still works under the encrypting wrapper
 *     (outbox rows are not secret-bearing and pass through untouched)
 *
 * SELF_MANAGED: starts its own server with ORCH8_ENCRYPTION_KEY +
 * ORCH8_MOBILE_SYNC_ENABLED=true at boot.
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
  only,
  syncDevice,
  uid,
  wakeRowsForDevice,
} from "./pushwake_helpers.ts";

// 64 hex chars -> 32 bytes -> AES-256 key (same pattern as
// security/credential_encryption_at_rest.test.ts).
const TEST_KEY = "b".repeat(64);

describe("push wake — mobile command payload encryption at rest", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_ENCRYPTION_KEY: TEST_KEY,
        ORCH8_MOBILE_SYNC_ENABLED: "true",
      },
    });
  });

  after(async () => {
    await stopServer(server);
  });

  it("command payload is ciphertext at rest but decrypted in the sync response", async () => {
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    const marker = uid("marker");

    const res = await createCommand(tenant, device.device_id, "refresh_state", {
      nested: { marker },
    });
    assert.equal(res.status, 201, res.text);

    // Raw DB: payload must NOT contain the plaintext marker.
    const command = only(await commandRowsForDevice(device.device_id), "commands");
    assert.ok(command.payload.length > 0, "stored payload must exist");
    assert.ok(
      !command.payload.includes(marker),
      `plaintext marker must not appear in raw DB; got: ${command.payload.slice(0, 80)}...`,
    );
    assert.ok(
      command.payload.startsWith("enc:v1:"),
      `payload should carry 'enc:v1:' prefix; got: ${command.payload.slice(0, 40)}...`,
    );

    // API: /mobile/sync decrypts on the way out — marker present again.
    const syncRes = await syncDevice(tenant, { device_id: device.device_id });
    assert.equal(syncRes.status, 200, syncRes.text);
    assert.equal(syncRes.body.commands.length, 1);
    assert.equal(
      syncRes.body.commands[0].payload.nested.marker,
      marker,
      "sync must return the decrypted payload",
    );
  });

  it("step delegation params (create_mobile_command path) are also encrypted at rest", async () => {
    // Delegation results go through `create_mobile_command` (no wake row) —
    // the second encrypt site in the wrapper. Their params can carry resolved
    // credentials, so they must be ciphertext too.
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    const secret = `sk_live_${uid("secret")}`;

    const res = await syncDevice(tenant, {
      device_id: device.device_id,
      step_delegations: [
        {
          request_id: uid("req"),
          instance_id: uid("inst"),
          block_id: "b1",
          handler: "noop",
          params: { credential: secret },
        },
      ],
    });
    assert.equal(res.status, 200, res.text);

    const command = only(await commandRowsForDevice(device.device_id), "commands");
    assert.equal(command.command_type, "step_result");
    assert.ok(
      !command.payload.includes(secret),
      `delegation secret must not appear in raw DB; got: ${command.payload.slice(0, 80)}...`,
    );
    assert.ok(
      command.payload.startsWith("enc:v1:"),
      "step_result payload must be encrypted at rest",
    );

    // And it round-trips decrypted through sync.
    const syncRes = await syncDevice(tenant, { device_id: device.device_id });
    assert.equal(syncRes.status, 200, syncRes.text);
    assert.equal(syncRes.body.commands.length, 1);
    assert.equal(syncRes.body.commands[0].type, "step_result");
    assert.ok(
      JSON.stringify(syncRes.body.commands[0].payload).includes(secret),
      "delegation payload must decrypt back through sync",
    );
  });

  it("wake claim/ack passthrough works under the encrypting wrapper", async () => {
    // The PushOutboxStore impl on EncryptingStorage is a pure passthrough
    // (no secret-bearing columns). Mirror the lifecycle: enqueue creates a
    // pending wake, sync delivers the command, ack correlates onto the wake.
    const tenant = uid("t");
    const device = await mustRegisterDevice(tenant, deviceSpec());
    const marker = uid("marker");

    await createCommand(tenant, device.device_id, "refresh", { marker });

    const wake = only(await wakeRowsForDevice(device.device_id), "wakes");
    assert.equal(wake.status, "pending", "noop provider must leave the wake pending");
    assert.equal(wake.attempts, 0);
    assert.equal(wake.command_acked_at, null);

    const sync1 = await syncDevice(tenant, { device_id: device.device_id });
    assert.equal(sync1.status, 200, sync1.text);
    assert.equal(sync1.body.commands.length, 1);
    const commandId = sync1.body.commands[0].id;
    assert.equal(sync1.body.commands[0].payload.marker, marker, "payload decrypted on delivery");

    const sync2 = await syncDevice(tenant, {
      device_id: device.device_id,
      command_acks: [commandId],
    });
    assert.equal(sync2.status, 200, sync2.text);
    assert.equal(sync2.body.commands.length, 0, "acked command must not be redelivered");

    const ackedWake = only(await wakeRowsForDevice(device.device_id), "wakes");
    assert.equal(ackedWake.command_id, commandId);
    assert.ok(
      ackedWake.command_acked_at !== null,
      "ack must correlate onto the wake row under the encrypting wrapper",
    );
    assert.equal(ackedWake.delivered_at, null, "provider acceptance still absent (noop provider)");

    const command = only(await commandRowsForDevice(device.device_id), "commands");
    assert.ok(command.acked_at !== null, "command row itself marked acked");
    assert.ok(
      command.payload.startsWith("enc:v1:"),
      "payload stays ciphertext after the ack round-trip",
    );
  });
});
