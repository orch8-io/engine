/**
 * Verifies encrypted context remains readable after an encryption key rotation (K1 -> K2).
 *
 * SELF_MANAGED: this suite restarts the server between key configurations.
 *
 * Skipped: orch8 encryption-at-rest is gated by the `ORCH8_ENCRYPTION_KEY` env var read in
 * orch8-server/src/main.rs at startup (see the FieldEncryptor wiring around the storage layer).
 * The current test harness (`tests/e2e/harness.ts::startServer`) does NOT expose a way to
 * inject environment variables — `StartServerOptions` only accepts `port`. Rotating keys
 * additionally requires multi-key support (old decryption key + new encryption key) which is
 * not yet present: `EncryptingStorage` holds a single `FieldEncryptor`. Until both
 * (a) harness env-override and (b) dual-key rotation land, an end-to-end rotation test would
 * assert vaporware.
 *
 * Expected future behavior:
 *   - startServer({ env: { ORCH8_ENCRYPTION_KEY: K1 } }) → create instance with sensitive context
 *   - stopServer → startServer({ env: { ORCH8_OLD_ENCRYPTION_KEY: K1, ORCH8_ENCRYPTION_KEY: K2 } })
 *   - read back: plaintext round-trips; new writes produce ciphertext under K2
 *   - verify raw DB column starts with `enc:v1:` prefix (see orch8-types/src/encryption.rs)
 */
import { describe, it, before, after } from "node:test";
import { Orch8Client } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();
void client;

describe("Encryption Key Rotation Mid-Workflow", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // Plan:
  //   - arrange: start server with key K1, create instance with sensitive context
  //   - act: stopServer, restart with K1 as old decryption key + K2 as new encryption key
  //   - assert: reads of legacy rows still return plaintext (decrypt via K1)
  //   - assert: new writes are encrypted under K2 (verify ciphertext marker)
  // Skipped: harness cannot inject ORCH8_ENCRYPTION_KEY (StartServerOptions accepts only
  // `port`); EncryptingStorage holds a single FieldEncryptor with no old/new rotation slot.
  // See file-level docstring for the expected future behavior.
  it.skip("should decrypt legacy rows after key rotation", () => {
    // Implementation blocked — see skip reason above.
  });
});
