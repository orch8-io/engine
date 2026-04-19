/**
 * Verifies encrypted context remains readable after an encryption key rotation (K1 -> K2).
 *
 * SELF_MANAGED: this suite restarts the server between key configurations.
 *
 * Flow:
 *   - Start server with K1, create instance with sensitive context
 *   - Stop server, restart with K2 as primary and K1 as old decryption key
 *   - Read back the instance: context should be readable (decrypted via K1 fallback)
 *   - Create a new instance: should work (encrypted via K2)
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

// 64 hex chars -> 32 bytes -> AES-256 keys.
const K1 = "aa".repeat(32);
const K2 = "bb".repeat(32);

describe("Encryption Key Rotation Mid-Workflow", () => {
  let server: ServerHandle | undefined;

  after(async () => {
    await stopServer(server);
  });

  it("should decrypt legacy rows after key rotation", async () => {
    const tenantId = `rot-${uuid().slice(0, 8)}`;
    const secret = `ROTATED-SECRET-${uuid()}`;

    // -- Phase 1: start with K1, create data --
    server = await startServer({
      env: { ORCH8_ENCRYPTION_KEY: K1 },
    });

    const client1 = new Orch8Client(`http://localhost:${server.port}`);

    const seq = testSequence("key-rotation", [step("s1", "noop")], { tenantId });
    await client1.createSequence(seq);

    const { id: instanceId } = await client1.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      context: { data: { secret, notes: "encrypted-with-k1" } },
    });

    // Verify the instance is readable under K1.
    const inst1 = await client1.getInstance(instanceId);
    const data1 = (inst1.context as { data?: Record<string, unknown> } | undefined)?.data;
    assert.ok(data1, "context.data should exist under K1");
    assert.equal(data1.secret, secret, "should read secret under K1");

    // -- Phase 2: restart with K2 (primary) + K1 (old) --
    await stopServer(server);

    server = await startServer({
      env: {
        ORCH8_ENCRYPTION_KEY: K2,
        ORCH8_OLD_ENCRYPTION_KEY: K1,
      },
      skipCleanup: true,
    });

    const client2 = new Orch8Client(`http://localhost:${server.port}`);

    // Read the legacy row — should decrypt via K1 fallback.
    const inst2 = await client2.getInstance(instanceId);
    const data2 = (inst2.context as { data?: Record<string, unknown> } | undefined)?.data;
    assert.ok(data2, "context.data should be readable after key rotation");
    assert.equal(
      data2.secret,
      secret,
      "legacy row should decrypt via old key fallback",
    );
    assert.equal(data2.notes, "encrypted-with-k1");

    // Create a new instance under K2 — should encrypt and decrypt fine.
    const newSecret = `NEW-SECRET-${uuid()}`;
    const { id: newInstanceId } = await client2.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      context: { data: { secret: newSecret, notes: "encrypted-with-k2" } },
    });

    const inst3 = await client2.getInstance(newInstanceId);
    const data3 = (inst3.context as { data?: Record<string, unknown> } | undefined)?.data;
    assert.ok(data3, "new instance context.data should exist");
    assert.equal(
      data3.secret,
      newSecret,
      "new instance should be readable (encrypted with K2)",
    );
    assert.equal(data3.notes, "encrypted-with-k2");
  });
});
