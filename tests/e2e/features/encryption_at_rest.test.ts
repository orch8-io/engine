/**
 * Encryption at Rest — verifies that sensitive context is stored as ciphertext
 * in Postgres but decrypted transparently by the API when
 * `ORCH8_ENCRYPTION_KEY` is configured.
 *
 * SELF_MANAGED: this suite starts its own server with the encryption key in
 * env. The shared attach-mode server in `run-e2e.ts` runs without a key, so
 * the suite is registered in `SELF_MANAGED_SUITES` to spawn a dedicated
 * process with the key set.
 *
 * Engine reference:
 *   - `orch8-server/src/main.rs` reads `ORCH8_ENCRYPTION_KEY` at startup and
 *     wraps storage in `EncryptingStorage` when the key is non-empty. No
 *     feature flag; presence of a 64-hex-char key is the only gate.
 *   - `orch8-storage/src/encrypting.rs` encrypts only `instance.context.data`
 *     on write and decrypts on read. The ciphertext has the JSON-string
 *     prefix `enc:v1:` (see `orch8-types/src/encryption.rs`).
 *   - Target column: `task_instances.context` (jsonb) — the `.data` field is
 *     replaced in place with the `enc:v1:...` string.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer, TEST_DB_URL } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

// 64 hex chars → 32 bytes → AES-256 key.
const TEST_KEY = "a".repeat(64);

/** Run a SELECT and return raw psql output (unaligned, no header). */
function psqlQuery(sql: string): string {
  const dbUrl = new URL(TEST_DB_URL);
  return execFileSync(
    "psql",
    [
      "-h", dbUrl.hostname,
      "-p", dbUrl.port,
      "-U", dbUrl.username,
      "-d", dbUrl.pathname.slice(1),
      "-v", "ON_ERROR_STOP=1",
      "-A",
      "-t",
      "-c", sql,
    ],
    {
      env: { ...process.env, PGPASSWORD: dbUrl.password },
      encoding: "utf-8",
    },
  ).trim();
}

describe("Encryption at Rest", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({
      env: { ORCH8_ENCRYPTION_KEY: TEST_KEY },
    });
  });

  after(async () => {
    await stopServer(server);
  });

  it("persists context.data as ciphertext but returns plaintext on read", async () => {
    const tenantId = `enc-${uuid().slice(0, 8)}`;
    const seq = testSequence("enc-at-rest", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    // Unique sentinel — absent from the raw row, present in the API response.
    const secret = `SECRET-${uuid()}`;

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      context: { data: { ssn: secret, notes: "private" } },
    });

    // Raw jsonb — bypasses EncryptingStorage, so ciphertext is visible.
    const raw = psqlQuery(
      `SELECT context::text FROM task_instances WHERE id = '${id}'`,
    );
    assert.ok(raw.length > 0, "raw DB row should exist");
    assert.ok(
      !raw.includes(secret),
      `plaintext secret must not appear in raw DB row; got: ${raw.slice(0, 200)}...`,
    );
    assert.ok(
      raw.includes("enc:v1:"),
      `encrypted context should carry the 'enc:v1:' prefix; got: ${raw.slice(0, 200)}...`,
    );

    // API path: decrypting storage transparently returns plaintext.
    const instance = await client.getInstance(id);
    const data = (instance.context as { data?: Record<string, unknown> } | undefined)
      ?.data;
    assert.ok(data, "context.data should round-trip through the API");
    assert.equal(data.ssn, secret, "API response should return decrypted ssn");
    assert.equal(
      data.notes,
      "private",
      "API response should return decrypted notes",
    );
  });
});
