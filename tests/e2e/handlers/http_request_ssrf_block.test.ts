/**
 * http_request handler — SSRF protection blocks private networks.
 *
 * Per `orch8-engine/src/handlers/builtin.rs::is_url_safe`, the handler
 * pre-resolves the hostname and rejects the request before any bytes
 * are sent when the target resolves to:
 *   - loopback (127.0.0.0/8, ::1)
 *   - RFC1918 private (10/8, 172.16/12, 192.168/16)
 *   - link-local (169.254/16 — includes cloud metadata endpoint)
 *   - ULA IPv6 (fc00::/7)
 *   - unspecified (0.0.0.0, ::)
 *
 * Rejection path returns `StepError::Permanent { "blocked: URL targets
 * a private/internal network address" }` — with no retry policy the
 * instance fails immediately and lands in the DLQ.
 *
 * This test exercises multiple private-range targets to cover the
 * main branches of `is_url_safe`.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("http_request Handler — SSRF Blocking", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  async function runAgainst(targetUrl: string): Promise<string> {
    const tenantId = `http-ssrf-${uuid().slice(0, 8)}`;
    const namespace = "default";

    const seq = testSequence(
      "http-ssrf",
      [
        step("blocked", "http_request", {
          url: targetUrl,
          method: "GET",
          timeout_ms: 2_000,
        }),
      ],
      { tenantId, namespace },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace,
    });

    const final = await client.waitForState(id, ["failed", "completed"], {
      timeoutMs: 10_000,
    });
    assert.equal(
      final.state,
      "failed",
      `expected failed for ${targetUrl}, got ${final.state}`,
    );

    // Permanent step error with no retry policy → instance should be in DLQ.
    const dlq = await client.listDlq({ tenant_id: tenantId });
    assert.ok(
      dlq.some((i) => i.id === id),
      `SSRF-blocked instance should land in DLQ for ${targetUrl}`,
    );

    return id;
  }

  it("blocks RFC1918 private IP (192.168.1.1)", async () => {
    await runAgainst("http://192.168.1.1/");
  });

  it("blocks loopback (127.0.0.1)", async () => {
    await runAgainst("http://127.0.0.1:1/");
  });

  it("blocks cloud metadata endpoint (169.254.169.254)", async () => {
    await runAgainst("http://169.254.169.254/latest/meta-data/");
  });

  it("blocks RFC1918 10/8 range (10.0.0.1)", async () => {
    await runAgainst("http://10.0.0.1/");
  });
});
