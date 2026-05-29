/**
 * mcp_call handler — native Model Context Protocol client over the
 * Streamable HTTP transport.
 *
 * These tests run a local mock MCP server (a JSON-RPC-over-HTTP endpoint)
 * and point the handler at it via `url`. The harness sets
 * `ORCH8_ALLOW_INTERNAL_URLS=1`, so the handler's SSRF guard permits the
 * loopback mock.
 *
 * Covered:
 *   - action="call" happy path → result content surfaced, is_error=false.
 *   - the full handshake is performed (initialize → notifications/initialized
 *     → tools/call) and arguments are forwarded.
 *   - action="list" → server tool catalog returned.
 *   - a tool returning isError:true completes the step with is_error=true
 *     (a domain error is an observation, not a step failure).
 *   - SSE-framed (text/event-stream) responses are parsed.
 *   - a JSON-RPC protocol error fails the step permanently (→ DLQ).
 *   - missing `url` fails permanently before any network traffic.
 */
import { describe, it, before, after, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

interface RpcRequest {
  jsonrpc: string;
  id?: number;
  method: string;
  params?: Record<string, unknown>;
}

const client = new Orch8Client();

async function readBody(req: IncomingMessage): Promise<RpcRequest | null> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const raw = Buffer.concat(chunks).toString("utf-8");
  if (!raw) return null;
  try {
    return JSON.parse(raw) as RpcRequest;
  } catch {
    return null;
  }
}

interface McpMockOptions {
  /** Override the tools/call result. */
  callResult?: (args: Record<string, unknown>) => unknown;
  /** Emit the tools/call reply as SSE (text/event-stream) instead of JSON. */
  sse?: boolean;
  /** Return a JSON-RPC error for tools/call. */
  callError?: { code: number; message: string };
}

interface McpMockHandle {
  baseUrl: string;
  received: RpcRequest[];
  close(): Promise<void>;
}

/** A minimal Streamable-HTTP MCP server sufficient to exercise the handler. */
function startMcpMock(opts: McpMockOptions = {}): Promise<McpMockHandle> {
  return new Promise((resolvePromise) => {
    const received: RpcRequest[] = [];

    const server = createServer((req, res) => {
      void (async () => {
        const body = await readBody(req);
        if (body) received.push(body);

        // Notifications carry no id and expect no body.
        if (!body || body.id === undefined) {
          res.statusCode = 202;
          res.end();
          return;
        }

        const reply = (result: unknown) =>
          JSON.stringify({ jsonrpc: "2.0", id: body.id, result });

        if (body.method === "initialize") {
          res.setHeader("Content-Type", "application/json");
          res.setHeader("Mcp-Session-Id", "sess-123");
          res.end(
            reply({
              protocolVersion: "2025-06-18",
              capabilities: { tools: {} },
              serverInfo: { name: "mock-mcp", version: "0.0.1" },
            }),
          );
          return;
        }

        if (body.method === "tools/list") {
          res.setHeader("Content-Type", "application/json");
          res.end(
            reply({
              tools: [
                {
                  name: "echo",
                  description: "Echo the input",
                  inputSchema: { type: "object" },
                },
              ],
            }),
          );
          return;
        }

        if (body.method === "tools/call") {
          if (opts.callError) {
            res.setHeader("Content-Type", "application/json");
            res.end(
              JSON.stringify({ jsonrpc: "2.0", id: body.id, error: opts.callError }),
            );
            return;
          }
          const args = (body.params?.arguments ?? {}) as Record<string, unknown>;
          const result = opts.callResult
            ? opts.callResult(args)
            : { content: [{ type: "text", text: `echo:${JSON.stringify(args)}` }], isError: false };

          if (opts.sse) {
            res.setHeader("Content-Type", "text/event-stream");
            res.end(`event: message\ndata: ${reply(result)}\n\n`);
          } else {
            res.setHeader("Content-Type", "application/json");
            res.end(reply(result));
          }
          return;
        }

        res.statusCode = 400;
        res.end(JSON.stringify({ jsonrpc: "2.0", id: body.id, error: { code: -32601, message: "Method not found" } }));
      })();
    });

    server.listen(0, "127.0.0.1", () => {
      const port = (server.address() as AddressInfo).port;
      resolvePromise({
        baseUrl: `http://127.0.0.1:${port}`,
        received,
        close: () => new Promise<void>((r) => server.close(() => r())),
      });
    });
  });
}

describe("mcp_call Handler (mock MCP server)", () => {
  let server: ServerHandle | undefined;
  let mock: McpMockHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  beforeEach(async () => {
    if (mock) await mock.close();
    mock = undefined;
  });

  after(async () => {
    if (mock) await mock.close();
  });

  it("action=call performs the handshake, forwards arguments, returns content", async () => {
    mock = await startMcpMock();
    const tenantId = `mcp-ok-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "mcp-call-ok",
      [
        step("s1", "mcp_call", {
          url: mock.baseUrl,
          tool_name: "echo",
          arguments: { q: "rust" },
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 15_000 });

    // Handshake order: initialize → notifications/initialized → tools/call.
    const methods = mock.received.map((r) => r.method);
    assert.deepEqual(methods, ["initialize", "notifications/initialized", "tools/call"]);
    const callReq = mock.received[2]!;
    assert.equal(callReq.params?.name, "echo");
    assert.deepEqual(callReq.params?.arguments, { q: "rust" });

    const outputs = await client.getOutputs(id);
    const s1 = outputs.find((o) => o.block_id === "s1");
    assert.ok(s1, "s1 output should exist");
    const out = s1!.output as { tool_name: string; result: Array<{ text: string }>; is_error: boolean };
    assert.equal(out.tool_name, "echo");
    assert.equal(out.is_error, false);
    assert.match(out.result[0]!.text, /echo:/);
  });

  it("action=list returns the server tool catalog", async () => {
    mock = await startMcpMock();
    const tenantId = `mcp-list-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "mcp-call-list",
      [step("s1", "mcp_call", { url: mock.baseUrl, action: "list" })],
      { tenantId },
    );
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 15_000 });

    const outputs = await client.getOutputs(id);
    const out = outputs.find((o) => o.block_id === "s1")!.output as {
      tools: Array<{ name: string }>;
    };
    assert.equal(out.tools[0]!.name, "echo");
  });

  it("a tool returning isError:true completes with is_error=true (not a step failure)", async () => {
    mock = await startMcpMock({
      callResult: () => ({ content: [{ type: "text", text: "tool blew up" }], isError: true }),
    });
    const tenantId = `mcp-terr-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "mcp-tool-error",
      [step("s1", "mcp_call", { url: mock.baseUrl, tool_name: "echo", arguments: {} })],
      { tenantId },
    );
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    const final = await client.waitForState(id, ["completed", "failed"], { timeoutMs: 15_000 });
    assert.equal(final.state, "completed", "tool-domain errors must not fail the step");

    const outputs = await client.getOutputs(id);
    const out = outputs.find((o) => o.block_id === "s1")!.output as { is_error: boolean };
    assert.equal(out.is_error, true);
  });

  it("parses SSE-framed (text/event-stream) responses", async () => {
    mock = await startMcpMock({ sse: true });
    const tenantId = `mcp-sse-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "mcp-sse",
      [step("s1", "mcp_call", { url: mock.baseUrl, tool_name: "echo", arguments: { a: 1 } })],
      { tenantId },
    );
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 15_000 });
    const outputs = await client.getOutputs(id);
    const out = outputs.find((o) => o.block_id === "s1")!.output as { is_error: boolean };
    assert.equal(out.is_error, false);
  });

  it("a JSON-RPC protocol error fails the step permanently (→ DLQ)", async () => {
    mock = await startMcpMock({ callError: { code: -32602, message: "Invalid params" } });
    const tenantId = `mcp-rpcerr-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "mcp-rpc-error",
      [step("s1", "mcp_call", { url: mock.baseUrl, tool_name: "echo", arguments: {} })],
      { tenantId },
    );
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    const final = await client.waitForState(id, ["completed", "failed"], { timeoutMs: 15_000 });
    assert.equal(final.state, "failed");
    const dlq = await client.listDlq({ tenant_id: tenantId });
    assert.ok(dlq.some((i) => i.id === id), "permanent failure should land in DLQ");
  });

  it("fails permanently when url is missing", async () => {
    const tenantId = `mcp-nourl-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "mcp-no-url",
      [step("s1", "mcp_call", { tool_name: "echo" })],
      { tenantId },
    );
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    const final = await client.waitForState(id, ["completed", "failed"], { timeoutMs: 10_000 });
    assert.equal(final.state, "failed");
  });

  it("resolves a named server from context.config.mcp_servers", async () => {
    mock = await startMcpMock();
    const tenantId = `mcp-named-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "mcp-named-server",
      [step("s1", "mcp_call", { server: "testmock", tool_name: "echo", arguments: { q: "hi" } })],
      { tenantId },
    );
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      // The engine-local registry: resolve `server: "testmock"` → this url.
      context: { config: { mcp_servers: { testmock: { url: mock.baseUrl } } } },
    });
    await client.waitForState(id, "completed", { timeoutMs: 15_000 });

    const outputs = await client.getOutputs(id);
    const out = outputs.find((o) => o.block_id === "s1")!.output as {
      tool_name: string;
      is_error: boolean;
    };
    assert.equal(out.tool_name, "echo");
    assert.equal(out.is_error, false);
    // The handshake reached the resolved server.
    assert.deepEqual(
      mock.received.map((r) => r.method),
      ["initialize", "notifications/initialized", "tools/call"],
    );
  });
});
