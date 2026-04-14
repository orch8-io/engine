/**
 * Credential Encryption at Rest — verifies that credential `value` and
 * `refresh_token` fields are stored as ciphertext in Postgres when
 * `ORCH8_ENCRYPTION_KEY` is configured, but returned decrypted via the API.
 *
 * SELF_MANAGED: starts its own server with an encryption key.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { Orch8Client, uuid, testSequence, step } from "../client.ts";
import { startServer, stopServer, TEST_DB_URL } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

// 64 hex chars -> 32 bytes -> AES-256 key.
const TEST_KEY = "b".repeat(64);

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

describe("Credential Encryption at Rest", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({
      env: { ORCH8_ENCRYPTION_KEY: TEST_KEY },
    });
  });

  after(async () => {
    await stopServer(server);
  });

  it("stores credential value as ciphertext but API returns plaintext", async () => {
    const credId = `enc-cred-${uuid().slice(0, 8)}`;
    const secretValue = `sk_live_${uuid()}`;

    await client.createCredential({
      id: credId,
      name: "Encrypted Test Key",
      kind: "api_key",
      value: secretValue,
      tenant_id: "enc-test",
    });

    // Raw DB: value column must NOT contain the plaintext secret.
    const rawValue = psqlQuery(
      `SELECT value FROM credentials WHERE id = '${credId}'`,
    );
    assert.ok(rawValue.length > 0, "raw DB row should exist");
    assert.ok(
      !rawValue.includes(secretValue),
      `plaintext secret must not appear in raw DB; got: ${rawValue.slice(0, 80)}...`,
    );
    assert.ok(
      rawValue.startsWith("enc:v1:"),
      `credential value should carry 'enc:v1:' prefix; got: ${rawValue.slice(0, 40)}...`,
    );

    // API: credential value is completely stripped from responses.
    const fetched = await client.getCredential(credId);
    assert.equal((fetched as any).value, undefined, "value must not appear in response");
    assert.equal((fetched as any).refresh_token, undefined, "refresh_token must not appear in response");
  });

  it("stores refresh_token as ciphertext in DB", async () => {
    const credId = `enc-oauth-${uuid().slice(0, 8)}`;
    const refreshSecret = `rt_${uuid()}`;

    await client.createCredential({
      id: credId,
      name: "Encrypted OAuth",
      kind: "oauth2",
      value: `{"access_token":"at_${uuid().slice(0, 8)}"}`,
      tenant_id: "enc-test",
      refresh_url: "https://example.com/token",
      refresh_token: refreshSecret,
    });

    // Raw DB: refresh_token column must be encrypted.
    const rawRt = psqlQuery(
      `SELECT refresh_token FROM credentials WHERE id = '${credId}'`,
    );
    assert.ok(rawRt.length > 0, "refresh_token row should exist");
    assert.ok(
      !rawRt.includes(refreshSecret),
      `plaintext refresh_token must not appear in DB; got: ${rawRt.slice(0, 80)}...`,
    );
    assert.ok(
      rawRt.startsWith("enc:v1:"),
      `refresh_token should carry 'enc:v1:' prefix; got: ${rawRt.slice(0, 40)}...`,
    );
  });

  it("credential resolver still works with encrypted credentials", async () => {
    const credId = `enc-resolve-${uuid().slice(0, 8)}`;
    const tenantId = `enc-t-${uuid().slice(0, 8)}`;
    const apiKey = `sk_test_${uuid().slice(0, 12)}`;

    // Create an encrypted credential.
    await client.createCredential({
      id: credId,
      name: "Resolver Test",
      kind: "api_key",
      value: `{"token":"${apiKey}"}`,
      tenant_id: tenantId,
    });

    // Create a sequence that references this credential.
    const seq = testSequence("enc-resolve", [step("s1", "noop", { auth: `credentials://${credId}` })], { tenantId });

    await client.createSequence(seq);

    // Create an instance — the engine will resolve the credential reference.
    const { id: instanceId } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      context: { data: {} },
    });

    // Wait for the instance to complete (noop handler should finish quickly).
    const deadline = Date.now() + 10_000;
    let instance: any;
    while (Date.now() < deadline) {
      instance = await client.getInstance(instanceId);
      if (instance.state === "completed" || instance.state === "failed") break;
      await new Promise((r) => setTimeout(r, 200));
    }

    // The instance should complete (not fail with a decryption error).
    assert.equal(
      instance!.state,
      "completed",
      `instance should complete; got state=${instance!.state}`,
    );
  });

  it("update credential re-encrypts value in DB", async () => {
    const credId = `enc-upd-${uuid().slice(0, 8)}`;
    const originalSecret = `orig_${uuid().slice(0, 12)}`;
    const newSecret = `new_${uuid().slice(0, 12)}`;

    await client.createCredential({
      id: credId,
      name: "Update Test",
      kind: "api_key",
      value: originalSecret,
      tenant_id: "enc-test",
    });

    // Update the credential value.
    await client.updateCredential(credId, { value: newSecret });

    // Raw DB must not contain either secret in plaintext.
    const rawValue = psqlQuery(
      `SELECT value FROM credentials WHERE id = '${credId}'`,
    );
    assert.ok(!rawValue.includes(originalSecret), "original secret must not be in DB");
    assert.ok(!rawValue.includes(newSecret), "new secret must not be in DB plaintext");
    assert.ok(rawValue.startsWith("enc:v1:"), "updated value must be encrypted");
  });
});
