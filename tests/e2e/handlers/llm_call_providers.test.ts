/**
 * llm_call provider behaviour against a local HTTP mock.
 *
 * These tests verify the provider-specific semantics of the `llm_call`
 * handler (`orch8-engine/src/handlers/llm.rs`) without hitting real
 * provider endpoints:
 *
 *   - OpenAI path: POST `/chat/completions`, Bearer token, body shaped
 *     with `model` + `messages`.
 *   - Anthropic path: POST `/messages`, `x-api-key` header, `system` as a
 *     top-level field.
 *   - Failover: cascade on 5xx / stop on first success / exhaust → fail.
 *   - `api_key_env`: key resolved from the named env var.
 *   - 429: classified as Retryable (observable here as the step completing
 *     after the mock flips from 429 to 200 on a retry).
 *   - `system` prompt: forwarded to the provider body verbatim.
 *
 * The mock records every incoming request into `received[]` so tests can
 * assert on path, headers, and body shape. Each test reaches the mock via
 * `base_url` overrides supplied in the step params.
 */
import { describe, it, before, after, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

interface RecordedRequest {
  method: string;
  path: string;
  headers: Record<string, string | string[] | undefined>;
  body: unknown;
}

type MockHandler = (req: RecordedRequest, res: ServerResponse) => void;

const client = new Orch8Client();

async function readBody(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const raw = Buffer.concat(chunks).toString("utf-8");
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

interface MockHandle {
  baseUrl: string;
  received: RecordedRequest[];
  setHandler(h: MockHandler): void;
  close(): Promise<void>;
}

function startMock(): Promise<MockHandle> {
  return new Promise((resolvePromise) => {
    const received: RecordedRequest[] = [];
    let handler: MockHandler = (_req, res) => {
      res.statusCode = 500;
      res.end("{}");
    };

    const server = createServer((req, res) => {
      void (async () => {
        const body = await readBody(req);
        const recorded: RecordedRequest = {
          method: req.method ?? "",
          path: req.url ?? "",
          headers: req.headers,
          body,
        };
        received.push(recorded);
        handler(recorded, res);
      })();
    });

    server.listen(0, "127.0.0.1", () => {
      const port = (server.address() as AddressInfo).port;
      resolvePromise({
        baseUrl: `http://127.0.0.1:${port}`,
        received,
        setHandler(h) {
          handler = h;
        },
        close: () =>
          new Promise<void>((r) => {
            server.close(() => r());
          }),
      });
    });
  });
}

function openAiSuccessBody(content: string): string {
  return JSON.stringify({
    id: "chatcmpl-mock",
    object: "chat.completion",
    model: "gpt-4o-mini",
    choices: [
      {
        index: 0,
        message: { role: "assistant", content },
        finish_reason: "stop",
      },
    ],
    usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
  });
}

function anthropicSuccessBody(text: string): string {
  return JSON.stringify({
    id: "msg_mock",
    type: "message",
    model: "claude-3-5-sonnet-latest",
    content: [{ type: "text", text }],
    stop_reason: "end_turn",
    usage: { input_tokens: 1, output_tokens: 1 },
  });
}

describe("llm_call provider semantics (mock HTTP)", () => {
  let server: ServerHandle | undefined;
  let mock: MockHandle;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  beforeEach(async () => {
    if (mock) await mock.close();
    mock = await startMock();
  });

  after(async () => {
    if (mock) await mock.close();
  });

  it("OpenAI provider hits /chat/completions with Bearer auth and returns content", async () => {
    mock.setHandler((_req, res) => {
      res.setHeader("Content-Type", "application/json");
      res.end(openAiSuccessBody("hello world"));
    });

    const tenantId = `llm-oa-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "llm-openai",
      [
        step("s1", "llm_call", {
          provider: "openai",
          base_url: mock.baseUrl,
          api_key: "test-key-openai",
          model: "gpt-4o-mini",
          messages: [{ role: "user", content: "hi" }],
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

    assert.equal(mock.received.length, 1, "mock should see exactly one call");
    const call = mock.received[0]!;
    assert.equal(call.method, "POST");
    assert.equal(call.path, "/chat/completions");
    assert.equal(
      call.headers["authorization"],
      "Bearer test-key-openai",
      "Authorization header should be Bearer <key>",
    );
    const body = call.body as { model: string; messages: Array<{ role: string }> };
    assert.equal(body.model, "gpt-4o-mini");
    assert.equal(body.messages[0]!.role, "user");

    const outputs = await client.getOutputs(id);
    const s1 = outputs.find((o) => o.block_id === "s1");
    assert.ok(s1, "s1 output should exist");
    const out = s1!.output as { message?: { content?: string }; provider?: string };
    assert.equal(out.provider, "openai");
    assert.equal(out.message?.content, "hello world");
  });

  it("Anthropic provider hits /messages with x-api-key header and top-level system", async () => {
    mock.setHandler((_req, res) => {
      res.setHeader("Content-Type", "application/json");
      res.end(anthropicSuccessBody("claude reply"));
    });

    const tenantId = `llm-an-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "llm-anthropic",
      [
        step("s1", "llm_call", {
          provider: "anthropic",
          base_url: mock.baseUrl,
          api_key: "test-key-anthropic",
          model: "claude-3-5-sonnet-latest",
          system: "you are terse",
          messages: [{ role: "user", content: "hello" }],
          max_tokens: 64,
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

    assert.equal(mock.received.length, 1);
    const call = mock.received[0]!;
    assert.equal(call.path, "/messages", "Anthropic uses /messages");
    assert.equal(
      call.headers["x-api-key"],
      "test-key-anthropic",
      "Anthropic uses x-api-key, not Authorization",
    );
    assert.equal(
      call.headers["anthropic-version"],
      "2023-06-01",
      "Anthropic request must carry an anthropic-version header",
    );

    const body = call.body as { system?: string; messages?: unknown[]; max_tokens?: number };
    assert.equal(body.system, "you are terse", "system prompt forwarded as top-level field");
    assert.ok(Array.isArray(body.messages));
    assert.equal(body.max_tokens, 64);
  });

  it("failover cascades past a 5xx response to the next provider", async () => {
    let calls = 0;
    mock.setHandler((req, res) => {
      calls += 1;
      if (req.path === "/chat/completions") {
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ error: { message: "primary down" } }));
      } else if (req.path === "/messages") {
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.end(anthropicSuccessBody("secondary ok"));
      } else {
        res.statusCode = 404;
        res.end("{}");
      }
    });

    const tenantId = `llm-fo-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "llm-failover-cascade",
      [
        step("s1", "llm_call", {
          providers: [
            {
              provider: "openai",
              base_url: mock.baseUrl,
              api_key: "primary-key",
              model: "gpt-4o-mini",
            },
            {
              provider: "anthropic",
              base_url: mock.baseUrl,
              api_key: "secondary-key",
              model: "claude-3-5-sonnet-latest",
            },
          ],
          messages: [{ role: "user", content: "hi" }],
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
    await client.waitForState(id, "completed", { timeoutMs: 20_000 });

    assert.equal(calls, 2, "both providers should have been called");
    const paths = mock.received.map((r) => r.path);
    assert.deepEqual(
      paths,
      ["/chat/completions", "/messages"],
      "openai tried first, then anthropic",
    );

    const outputs = await client.getOutputs(id);
    const s1 = outputs.find((o) => o.block_id === "s1")!;
    const out = s1.output as { tried?: string[]; provider?: string };
    assert.deepEqual(out.tried, ["openai", "anthropic"], "tried list records the cascade");
    assert.equal(out.provider, "anthropic");
  });

  it("failover stops on first successful provider and does not call the rest", async () => {
    let secondProviderCalls = 0;
    mock.setHandler((req, res) => {
      if (req.path === "/chat/completions") {
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.end(openAiSuccessBody("primary ok"));
      } else {
        secondProviderCalls += 1;
        res.statusCode = 500;
        res.end("{}");
      }
    });

    const tenantId = `llm-first-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "llm-first-wins",
      [
        step("s1", "llm_call", {
          providers: [
            {
              provider: "openai",
              base_url: mock.baseUrl,
              api_key: "k1",
              model: "gpt-4o-mini",
            },
            {
              provider: "anthropic",
              base_url: mock.baseUrl,
              api_key: "k2",
              model: "claude-3-5-sonnet-latest",
            },
          ],
          messages: [{ role: "user", content: "hi" }],
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

    assert.equal(
      secondProviderCalls,
      0,
      "second provider must not be called when first succeeds",
    );

    const outputs = await client.getOutputs(id);
    const out = outputs.find((o) => o.block_id === "s1")!.output as {
      tried?: string[];
      provider?: string;
    };
    assert.deepEqual(out.tried, ["openai"], "only the first provider appears in tried list");
    assert.equal(out.provider, "openai");
  });

  it("api_key_env reports a permanent error when the named env var is unset", async () => {
    // The handler's `resolve_api_key` path (`orch8-engine/src/handlers/llm.rs`)
    // surfaces a `Permanent` error when `api_key_env` names an env var that
    // doesn't exist on the server process. Without a guaranteed env var to
    // read on the attach-mode server, we verify the negative path: the
    // instance lands in `failed` without the mock ever being hit.
    mock.setHandler((_req, res) => {
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(openAiSuccessBody("should not be called"));
    });

    const tenantId = `llm-env-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "llm-env-missing",
      [
        step("s1", "llm_call", {
          provider: "openai",
          base_url: mock.baseUrl,
          api_key_env: `ORCH8_TEST_LLM_KEY_${uuid().slice(0, 8).toUpperCase()}`,
          model: "gpt-4o-mini",
          messages: [{ role: "user", content: "hi" }],
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

    await client.waitForState(id, "failed", { timeoutMs: 15_000 });
    assert.equal(
      mock.received.length,
      0,
      "handler must short-circuit before any HTTP call when the env var is missing",
    );
  });

  it("429 response is classified as Retryable and the step eventually succeeds after the mock flips to 200", async () => {
    let call = 0;
    mock.setHandler((_req, res) => {
      call += 1;
      if (call === 1) {
        res.statusCode = 429;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ error: { message: "rate limited" } }));
      } else {
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.end(openAiSuccessBody("ok after retry"));
      }
    });

    const tenantId = `llm-429-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "llm-429-retry",
      [
        step(
          "s1",
          "llm_call",
          {
            provider: "openai",
            base_url: mock.baseUrl,
            api_key: "k",
            model: "gpt-4o-mini",
            messages: [{ role: "user", content: "hi" }],
          },
          // Give the step enough attempts to retry at least once after
          // the initial 429. Durations on RetryPolicy are milliseconds
          // (`orch8-types::serde_duration`).
          {
            retry: {
              max_attempts: 3,
              initial_backoff: 50,
              max_backoff: 500,
            },
          },
        ),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 30_000 });
    assert.ok(call >= 2, "handler should retry after the first 429");

    const outputs = await client.getOutputs(id);
    const out = outputs.find((o) => o.block_id === "s1")!.output as {
      message?: { content?: string };
    };
    assert.equal(out.message?.content, "ok after retry");
  });

  it("system prompt is forwarded to the OpenAI body as a system-role message", async () => {
    mock.setHandler((_req, res) => {
      res.setHeader("Content-Type", "application/json");
      res.end(openAiSuccessBody("ok"));
    });

    const tenantId = `llm-sys-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "llm-system-prompt",
      [
        step("s1", "llm_call", {
          provider: "openai",
          base_url: mock.baseUrl,
          api_key: "k",
          model: "gpt-4o-mini",
          // OpenAI-compat path: system messages travel inside the
          // `messages` array as {role: "system"}. The test plan item
          // asks the handler to pass the system content through to the
          // provider — verified below.
          messages: [
            { role: "system", content: "behave like a cat" },
            { role: "user", content: "hi" },
          ],
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

    assert.equal(mock.received.length, 1);
    const body = mock.received[0]!.body as {
      messages: Array<{ role: string; content: string }>;
    };
    const sys = body.messages.find((m) => m.role === "system");
    assert.ok(sys, "system-role message should be present in the forwarded body");
    assert.equal(sys!.content, "behave like a cat");
  });
});
