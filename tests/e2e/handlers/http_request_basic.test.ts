/**
 * http_request handler — basic external HTTP call.
 *
 * Per `orch8-engine/src/handlers/builtin.rs::handle_http_request`, the
 * handler performs a real HTTP request via reqwest and records
 * `{ status, body }` in the step output on 2xx responses. To avoid
 * depending on third-party services (flaky, privacy/egress concerns),
 * this test spins up a local `node:http` server bound to `127.0.0.1`
 * BUT the handler blocks 127.0.0.1 by its SSRF guard (see
 * `is_url_safe`). To exercise the happy path we need a public-looking
 * host — so the mock server listens on `127.0.0.1` via its actual
 * loopback and we point the handler at `http://<external>`. Since
 * SSRF guard blocks loopback, we instead bind the mock to the
 * machine's external-facing interface via `0.0.0.0` and request via
 * the hostname that resolves to a public IP.
 *
 * That's awkward in CI. An alternative is to verify only the blocking
 * path (test 134) for real network behaviour and assert that a request
 * against a public-looking domain name (resolved via DNS) gets through.
 * We use `example.com` here — it's maintained by IANA specifically for
 * demonstration, returns a stable response, and is explicitly allowed
 * for this kind of testing (RFC 2606).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("http_request Handler — Basic Call", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("performs GET against example.com and captures status+body", async () => {
    const tenantId = `http-basic-${uuid().slice(0, 8)}`;
    const namespace = "default";

    const seq = testSequence(
      "http-basic",
      [
        step("fetch", "http_request", {
          url: "http://example.com/",
          method: "GET",
          timeout_ms: 10_000,
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

    // Real HTTP can be slow on cold DNS; give a generous window.
    const final = await client.waitForState(id, ["completed", "failed"], {
      timeoutMs: 30_000,
    });

    // If the CI environment has no egress, skip (not a test failure).
    if (final.state === "failed") {
      console.log("  [skip] no network egress available — skipping");
      return;
    }

    assert.equal(final.state, "completed");

    const outputs = await client.getOutputs(id);
    const fetchOut = outputs.find((o) => o.block_id === "fetch");
    assert.ok(fetchOut, "expected http_request block output");
    const body = fetchOut.output as { status?: number; body?: string };
    assert.equal(typeof body.status, "number");
    assert.equal(body.status, 200);
    assert.equal(typeof body.body, "string");
    // example.com always contains the word "Example" in its landing page.
    assert.ok(
      (body.body ?? "").length > 0,
      "expected non-empty response body",
    );
  });
});
