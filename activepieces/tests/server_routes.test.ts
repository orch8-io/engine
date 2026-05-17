// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Orch8, Inc.
//
// HTTP route tests for the ActivePieces sidecar server: health endpoints,
// routing, request validation, output serialization, and error classification
// through the full HTTP stack.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { AddressInfo } from "node:net";
import { createServer } from "../src/server.ts";
import { createDefaultLoader, Piece } from "../src/registry.ts";
import { PieceExecutionError } from "../src/errors.ts";

// ---------------------------------------------------------------------------
// Helpers (same pattern as roundtrip.test.ts)
// ---------------------------------------------------------------------------

function fakePiece(opts: {
  run: (ctx: unknown) => Promise<unknown>;
  actionName?: string;
  displayName?: string;
}): Piece {
  return {
    displayName: opts.displayName ?? "fake",
    actions: () => ({
      [opts.actionName ?? "do_thing"]: {
        name: opts.actionName ?? "do_thing",
        displayName: opts.displayName,
        run: opts.run,
      },
    }),
  };
}

function multiActionPiece(actions: Record<string, { name: string; displayName?: string; run: (ctx: unknown) => Promise<unknown> }>): Piece {
  return {
    displayName: "multi",
    actions: () => actions,
  };
}

async function withServer<T>(
  registerPiece: (loader: ReturnType<typeof createDefaultLoader>) => void,
  fn: (baseUrl: string) => Promise<T>,
): Promise<T> {
  const loader = createDefaultLoader();
  registerPiece(loader);
  const server = createServer({
    port: 0,
    host: "127.0.0.1",
    loader,
    requestTimeoutMs: 5_000,
    log: () => {}, // silence logs during tests
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const addr = server.address() as AddressInfo;
  const baseUrl = `http://127.0.0.1:${addr.port}`;
  try {
    return await fn(baseUrl);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

async function post(url: string, body: unknown): Promise<{ status: number; body: any }> {
  const res = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : null };
}

async function get(url: string): Promise<{ status: number; body: any }> {
  const res = await fetch(url);
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : null };
}

// ---------------------------------------------------------------------------
// Health / routing
// ---------------------------------------------------------------------------

test("GET / returns health", async () => {
  await withServer(
    () => {},
    async (baseUrl) => {
      const { status, body } = await get(baseUrl);
      assert.equal(status, 200);
      assert.equal(body.ok, true);
      assert.equal(body.service, "orch8-activepieces-worker");
    },
  );
});

test("GET /health returns health", async () => {
  await withServer(
    () => {},
    async (baseUrl) => {
      const { status, body } = await get(`${baseUrl}/health`);
      assert.equal(status, 200);
      assert.equal(body.ok, true);
    },
  );
});

test("POST to unknown path returns 404", async () => {
  await withServer(
    () => {},
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/unknown`, {});
      assert.equal(status, 404);
      assert.equal(body.ok, false);
      assert.equal(body.error.type, "permanent");
    },
  );
});

test("PUT /execute returns 404", async () => {
  await withServer(
    () => {},
    async (baseUrl) => {
      const res = await fetch(`${baseUrl}/execute`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ piece: "slack", action: "send" }),
      });
      assert.equal(res.status, 404);
      const body = await res.json();
      assert.equal(body.ok, false);
    },
  );
});

test("GET /execute returns 404", async () => {
  await withServer(
    () => {},
    async (baseUrl) => {
      const { status, body } = await get(`${baseUrl}/execute`);
      assert.equal(status, 404);
      assert.equal(body.ok, false);
    },
  );
});

// ---------------------------------------------------------------------------
// Request validation — POST /execute
// ---------------------------------------------------------------------------

test("empty body on POST /execute returns 422", async () => {
  await withServer(
    () => {},
    async (baseUrl) => {
      // Send empty string body (readJson parses "" as {})
      const res = await fetch(`${baseUrl}/execute`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "",
      });
      assert.equal(res.status, 422);
      const body = await res.json();
      assert.equal(body.ok, false);
      assert.match(body.error.message, /piece.*action/);
    },
  );
});

test("body with only piece field (no action) returns 422", async () => {
  await withServer(
    () => {},
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, { piece: "slack" });
      assert.equal(status, 422);
      assert.equal(body.ok, false);
      assert.equal(body.error.type, "permanent");
    },
  );
});

test("body with only action field (no piece) returns 422", async () => {
  await withServer(
    () => {},
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, { action: "send" });
      assert.equal(status, 422);
      assert.equal(body.ok, false);
      assert.equal(body.error.type, "permanent");
    },
  );
});

test("numeric piece field returns 422", async () => {
  await withServer(
    () => {},
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, { piece: 123, action: "send" });
      assert.equal(status, 422);
      assert.equal(body.error.type, "permanent");
    },
  );
});

test("numeric action field returns 422", async () => {
  await withServer(
    () => {},
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, { piece: "slack", action: 456 });
      assert.equal(status, 422);
      assert.equal(body.error.type, "permanent");
    },
  );
});

// ---------------------------------------------------------------------------
// Output serialization — various return types
// ---------------------------------------------------------------------------

test("piece returning undefined output results in null in response", async () => {
  await withServer(
    (loader) =>
      loader.cache("undef", fakePiece({ run: async () => undefined })),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "undef",
        action: "do_thing",
      });
      assert.equal(status, 200);
      assert.equal(body.ok, true);
      assert.equal(body.output, null);
    },
  );
});

test("piece returning empty object results in empty object in response", async () => {
  await withServer(
    (loader) =>
      loader.cache("empty-obj", fakePiece({ run: async () => ({}) })),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "empty-obj",
        action: "do_thing",
      });
      assert.equal(status, 200);
      assert.deepEqual(body.output, {});
    },
  );
});

test("piece returning array results in array in response", async () => {
  await withServer(
    (loader) =>
      loader.cache("arr", fakePiece({ run: async () => [1, 2, 3] })),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "arr",
        action: "do_thing",
      });
      assert.equal(status, 200);
      assert.deepEqual(body.output, [1, 2, 3]);
    },
  );
});

test("piece returning string results in string in response", async () => {
  await withServer(
    (loader) =>
      loader.cache("str", fakePiece({ run: async () => "hello world" })),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "str",
        action: "do_thing",
      });
      assert.equal(status, 200);
      assert.equal(body.output, "hello world");
    },
  );
});

test("piece returning number results in number in response", async () => {
  await withServer(
    (loader) =>
      loader.cache("num", fakePiece({ run: async () => 42 })),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "num",
        action: "do_thing",
      });
      assert.equal(status, 200);
      assert.equal(body.output, 42);
    },
  );
});

test("piece returning boolean results in boolean in response", async () => {
  await withServer(
    (loader) =>
      loader.cache("bool", fakePiece({ run: async () => true })),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "bool",
        action: "do_thing",
      });
      assert.equal(status, 200);
      assert.equal(body.output, true);
    },
  );
});

test("piece returning nested objects are preserved in response", async () => {
  const nested = {
    users: [
      { id: 1, profile: { name: "Alice", tags: ["admin", "owner"] } },
      { id: 2, profile: { name: "Bob", tags: [] } },
    ],
    meta: { total: 2, page: 1 },
  };
  await withServer(
    (loader) =>
      loader.cache("nested", fakePiece({ run: async () => nested })),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "nested",
        action: "do_thing",
      });
      assert.equal(status, 200);
      assert.deepEqual(body.output, nested);
    },
  );
});

// ---------------------------------------------------------------------------
// Default field values
// ---------------------------------------------------------------------------

test("default props when none provided results in empty object in context", async () => {
  await withServer(
    (loader) =>
      loader.cache(
        "echo-props",
        fakePiece({ run: async (ctx: any) => ({ props: ctx.propsValue }) }),
      ),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "echo-props",
        action: "do_thing",
        // no props field
      });
      assert.equal(status, 200);
      assert.deepEqual(body.output.props, {});
    },
  );
});

test("default instance_id when none provided results in empty string", async () => {
  await withServer(
    (loader) =>
      loader.cache(
        "echo-run",
        fakePiece({ run: async (ctx: any) => ({ runId: ctx.run.id }) }),
      ),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "echo-run",
        action: "do_thing",
        // no instance_id
      });
      assert.equal(status, 200);
      assert.equal(body.output.runId, "");
    },
  );
});

test("default block_id when none provided results in empty string", async () => {
  await withServer(
    (loader) =>
      loader.cache(
        "echo-step",
        fakePiece({ run: async (ctx: any) => ({ step: ctx.step.name }) }),
      ),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "echo-step",
        action: "do_thing",
        // no block_id
      });
      assert.equal(status, 200);
      assert.equal(body.output.step, "");
    },
  );
});

// ---------------------------------------------------------------------------
// Error classification through the HTTP stack
// ---------------------------------------------------------------------------

test("network-like error from piece results in 502 retryable", async () => {
  await withServer(
    (loader) =>
      loader.cache(
        "net-err",
        fakePiece({
          run: async () => {
            const err: any = new Error("socket hang up");
            err.code = "ECONNRESET";
            throw err;
          },
        }),
      ),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "net-err",
        action: "do_thing",
      });
      assert.equal(status, 502);
      assert.equal(body.ok, false);
      assert.equal(body.error.type, "retryable");
    },
  );
});

test("ECONNREFUSED-like error from piece results in 502 retryable", async () => {
  await withServer(
    (loader) =>
      loader.cache(
        "conn-refused",
        fakePiece({
          run: async () => {
            const err: any = new Error("connect ECONNREFUSED");
            err.code = "ECONNREFUSED";
            throw err;
          },
        }),
      ),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "conn-refused",
        action: "do_thing",
      });
      assert.equal(status, 502);
      assert.equal(body.error.type, "retryable");
    },
  );
});

test("TypeError from piece results in 500 permanent", async () => {
  await withServer(
    (loader) =>
      loader.cache(
        "type-err",
        fakePiece({
          run: async () => {
            throw new TypeError("Cannot read properties of null");
          },
        }),
      ),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "type-err",
        action: "do_thing",
      });
      assert.equal(status, 500);
      assert.equal(body.ok, false);
      assert.equal(body.error.type, "permanent");
    },
  );
});

test("action with displayName different from key can be found by name", async () => {
  await withServer(
    (loader) =>
      loader.cache(
        "named",
        multiActionPiece({
          send_msg: {
            name: "send_channel_message",
            displayName: "Send Channel Message",
            run: async () => ({ sent: true }),
          },
        }),
      ),
    async (baseUrl) => {
      // Look up by .name field, not by record key
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "named",
        action: "send_channel_message",
      });
      assert.equal(status, 200);
      assert.equal(body.ok, true);
      assert.deepEqual(body.output, { sent: true });
    },
  );
});

// ---------------------------------------------------------------------------
// Multiple sequential requests (connection reuse)
// ---------------------------------------------------------------------------

test("multiple sequential requests work (connection reuse)", async () => {
  await withServer(
    (loader) => {
      let counter = 0;
      loader.cache(
        "counter",
        fakePiece({
          run: async () => ({ count: ++counter }),
        }),
      );
    },
    async (baseUrl) => {
      const r1 = await post(`${baseUrl}/execute`, {
        piece: "counter",
        action: "do_thing",
      });
      assert.equal(r1.status, 200);
      assert.equal(r1.body.output.count, 1);

      const r2 = await post(`${baseUrl}/execute`, {
        piece: "counter",
        action: "do_thing",
      });
      assert.equal(r2.status, 200);
      assert.equal(r2.body.output.count, 2);

      const r3 = await post(`${baseUrl}/execute`, {
        piece: "counter",
        action: "do_thing",
      });
      assert.equal(r3.status, 200);
      assert.equal(r3.body.output.count, 3);
    },
  );
});

// ---------------------------------------------------------------------------
// Mixed success and error requests on same server
// ---------------------------------------------------------------------------

test("server handles mixed success and error requests", async () => {
  await withServer(
    (loader) => {
      loader.cache("good", fakePiece({ run: async () => "ok" }));
      loader.cache(
        "bad",
        fakePiece({
          run: async () => {
            throw new Error("boom");
          },
        }),
      );
    },
    async (baseUrl) => {
      // Success
      const r1 = await post(`${baseUrl}/execute`, {
        piece: "good",
        action: "do_thing",
      });
      assert.equal(r1.status, 200);
      assert.equal(r1.body.output, "ok");

      // Error
      const r2 = await post(`${baseUrl}/execute`, {
        piece: "bad",
        action: "do_thing",
      });
      assert.equal(r2.status, 502);
      assert.equal(r2.body.ok, false);

      // Success again — server still healthy
      const r3 = await post(`${baseUrl}/execute`, {
        piece: "good",
        action: "do_thing",
      });
      assert.equal(r3.status, 200);
      assert.equal(r3.body.output, "ok");
    },
  );
});

test("null piece and action fields returns 422", async () => {
  await withServer(
    () => {},
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: null,
        action: null,
      });
      assert.equal(status, 422);
      assert.equal(body.error.type, "permanent");
    },
  );
});

test("piece returning null output results in null in response", async () => {
  await withServer(
    (loader) =>
      loader.cache("null-out", fakePiece({ run: async () => null })),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "null-out",
        action: "do_thing",
      });
      assert.equal(status, 200);
      assert.equal(body.ok, true);
      assert.equal(body.output, null);
    },
  );
});
