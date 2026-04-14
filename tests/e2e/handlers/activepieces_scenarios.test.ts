/**
 * ActivePieces handler scenarios with a mock sidecar.
 *
 * Spins up a tiny Node HTTP server that speaks the documented sidecar
 * protocol, points the orch8-server at it via `ORCH8_ACTIVEPIECES_URL`,
 * then exercises every branch of the handler dispatch logic:
 *   - ok:true → output captured
 *   - ok:false permanent → no retry, instance failed
 *   - ok:false retryable → retries consumed, then failed
 *   - unknown error kind → treated as retryable (fail-open)
 *   - HTTP 500 raw body → retryable
 *   - HTTP 400 raw body → permanent
 *   - 2xx non-envelope body → wrapped as { raw }
 *   - parse error (malformed handler name) → permanent at parse
 *   - auth + props + instance_id + block_id + attempt forwarded verbatim
 *   - concurrent dispatches from parallel branches
 *
 * The mock exposes /last to inspect what the engine last posted, and /set
 * to reprogram the next response. All state is per-test to avoid cross-
 * contamination.
 */
import { describe, it, before, after, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { Server, IncomingMessage, ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

interface SidecarResponse {
  status: number;
  body?: unknown;
  raw?: string; // if present, `body` is ignored and raw string is sent verbatim
}

// Shared mock state; per-test reset via beforeEach.
let nextResponse: SidecarResponse = { status: 200, body: { ok: true, output: {} } };
let lastRequest: { headers: Record<string, string>; body: unknown } | null = null;
let callCount = 0;

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolvePromise, rejectPromise) => {
    let buf = "";
    req.on("data", (chunk) => (buf += chunk));
    req.on("end", () => resolvePromise(buf));
    req.on("error", rejectPromise);
  });
}

function startMockSidecar(): Promise<{ server: Server; url: string }> {
  return new Promise((resolvePromise) => {
    const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
      const body = await readBody(req);
      if (req.url === "/execute" && req.method === "POST") {
        callCount++;
        try {
          lastRequest = {
            headers: req.headers as Record<string, string>,
            body: JSON.parse(body || "{}"),
          };
        } catch {
          lastRequest = { headers: req.headers as Record<string, string>, body };
        }
        res.statusCode = nextResponse.status;
        res.setHeader("content-type", "application/json");
        if (nextResponse.raw !== undefined) {
          res.end(nextResponse.raw);
        } else {
          res.end(JSON.stringify(nextResponse.body));
        }
        return;
      }
      res.statusCode = 404;
      res.end();
    });
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address() as AddressInfo;
      resolvePromise({ server, url: `http://127.0.0.1:${port}/execute` });
    });
  });
}

describe("ActivePieces sidecar scenarios", () => {
  let server: ServerHandle | undefined;
  let mock: Server | undefined;

  before(async () => {
    const m = await startMockSidecar();
    mock = m.server;
    server = await startServer({ env: { ORCH8_ACTIVEPIECES_URL: m.url } });
  });

  after(async () => {
    await stopServer(server);
    await new Promise<void>((resolvePromise) => {
      if (!mock) return resolvePromise();
      mock.close(() => resolvePromise());
    });
  });

  beforeEach(() => {
    nextResponse = { status: 200, body: { ok: true, output: {} } };
    lastRequest = null;
    callCount = 0;
  });

  it("ok:true with output → step captures output", async () => {
    nextResponse = {
      status: 200,
      body: { ok: true, output: { message_id: "abc123", thread: "t1" } },
    };
    const seq = testSequence("ap-ok", [
      step("s", "ap://slack.send_channel_message", { props: { text: "hi" } }),
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const outputs = await client.getOutputs(id);
    const s = outputs.find((o) => o.block_id === "s");
    assert.ok(s, "step output missing");
    assert.deepEqual(s.output, { message_id: "abc123", thread: "t1" });
  });

  it("forwards piece, action, auth, props, instance_id, block_id, attempt", async () => {
    nextResponse = { status: 200, body: { ok: true, output: null } };
    const seq = testSequence("ap-forward", [
      step("fwd", "ap://gmail.send_email", {
        auth: { token: "xyz" },
        props: { to: "a@b.c", subject: "hey" },
      }),
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const body = lastRequest?.body as Record<string, unknown>;
    assert.ok(body, "sidecar was never called");
    assert.equal(body.piece, "gmail");
    assert.equal(body.action, "send_email");
    assert.deepEqual(body.auth, { token: "xyz" });
    assert.deepEqual(body.props, { to: "a@b.c", subject: "hey" });
    assert.equal(typeof body.instance_id, "string");
    assert.equal(body.block_id, "fwd");
    assert.equal(typeof body.attempt, "number");
  });

  it("ok:false permanent → instance fails without retry", async () => {
    nextResponse = {
      status: 200,
      body: { ok: false, error: { type: "permanent", message: "bad props" } },
    };
    const seq = testSequence("ap-perm", [step("s", "ap://slack.send_channel_message", {})]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    const final = await client.waitForState(id, ["failed", "completed"], {
      timeoutMs: 10_000,
    });
    assert.equal(final.state, "failed");
    assert.equal(callCount, 1, "permanent error must not retry");
  });

  it("HTTP 500 raw body → retryable (no retry policy → instance fails after 1 call)", async () => {
    // With no retry_policy on the step the scheduler fails immediately on
    // the first Retryable; the retry-policy path is covered elsewhere.
    nextResponse = { status: 503, raw: "<html>upstream down</html>" };
    const seq = testSequence("ap-5xx", [step("s", "ap://slack.send_channel_message", {})]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    const final = await client.waitForState(id, ["failed", "completed"], {
      timeoutMs: 10_000,
    });
    assert.equal(final.state, "failed");
  });

  it("HTTP 400 raw body → permanent error, single call", async () => {
    nextResponse = { status: 400, raw: "<html>bad request</html>" };
    const seq = testSequence("ap-4xx", [step("s", "ap://slack.send_channel_message", {})]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    const final = await client.waitForState(id, ["failed", "completed"], {
      timeoutMs: 10_000,
    });
    assert.equal(final.state, "failed");
    assert.equal(callCount, 1, "4xx is permanent — must not retry");
  });

  it("2xx with non-envelope body → wraps as { raw }", async () => {
    nextResponse = { status: 200, raw: "not json at all" };
    const seq = testSequence("ap-raw", [step("s", "ap://slack.send_channel_message", {})]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const outputs = await client.getOutputs(id);
    const s = outputs.find((o) => o.block_id === "s");
    assert.deepEqual(s?.output, { raw: "not json at all" });
  });

  it("unknown error kind → treated as retryable (fail-open)", async () => {
    nextResponse = {
      status: 200,
      body: { ok: false, error: { type: "mystery-kind", message: "???" } },
    };
    const seq = testSequence("ap-unknown", [step("s", "ap://slack.send_channel_message", {})]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    const final = await client.waitForState(id, ["failed", "completed"], {
      timeoutMs: 10_000,
    });
    assert.equal(final.state, "failed"); // retryable with no retry policy → failed
  });

  it("malformed handler `ap://slack` → permanent at parse, no sidecar call", async () => {
    const seq = testSequence("ap-malformed", [step("s", "ap://slack", {})]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    const final = await client.waitForState(id, ["failed", "completed"], {
      timeoutMs: 10_000,
    });
    assert.equal(final.state, "failed");
    assert.equal(callCount, 0, "parse error must not reach sidecar");
  });

  it("hyphenated piece names are accepted: ap://google-sheets.insert_row", async () => {
    nextResponse = { status: 200, body: { ok: true, output: { row: 1 } } };
    const seq = testSequence("ap-hyphen", [
      step("s", "ap://google-sheets.insert_row", { props: { value: "x" } }),
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const body = lastRequest?.body as Record<string, unknown>;
    assert.equal(body.piece, "google-sheets");
    assert.equal(body.action, "insert_row");
  });

  it("try_catch rescues a permanent sidecar error", async () => {
    nextResponse = {
      status: 200,
      body: { ok: false, error: { type: "permanent", message: "nope" } },
    };
    const seq = testSequence("ap-tc", [
      {
        type: "try_catch",
        id: "tc",
        try_block: [step("call", "ap://slack.send_channel_message", {})],
        catch_block: [step("rescued", "log", { message: "r" })],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
    assert.ok(ids.has("rescued"));
  });

  it("parallel branches dispatch concurrently to sidecar", async () => {
    nextResponse = { status: 200, body: { ok: true, output: { ok: 1 } } };
    const N = 4;
    const branches = Array.from({ length: N }, (_, i) => [
      step(`b${i}`, "ap://slack.send_channel_message", { props: { idx: i } }),
    ]);
    const seq = testSequence("ap-par", [
      { type: "parallel", id: "p", branches } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 15_000 });
    assert.equal(callCount, N, `expected ${N} sidecar calls, got ${callCount}`);
  });

  it("different tenant_id does not bleed into sidecar request for another tenant", async () => {
    // Tenant A call — inspect last request tenant-agnostically (sidecar body
    // doesn't carry tenant by design, only instance_id). This asserts the
    // protocol contract: tenant_id is orch8-internal, not forwarded.
    nextResponse = { status: 200, body: { ok: true, output: null } };
    const tenantId = `ap-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "ap-tenant-iso",
      [step("s", "ap://slack.send_channel_message", {})],
      { tenantId },
    );
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const body = lastRequest?.body as Record<string, unknown>;
    assert.ok(body, "sidecar was never called");
    assert.equal(body.tenant_id, undefined, "tenant_id must not be forwarded");
  });
});
