// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Orch8, Inc.
//
// Round-trip tests for the ActivePieces sidecar using a fake in-memory piece.
// No real `@activepieces/piece-*` packages are required to run these tests.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { AddressInfo } from "node:net";

import { createServer } from "../src/server.ts";
import { createDefaultLoader, Piece } from "../src/registry.ts";
import { PieceExecutionError } from "../src/errors.ts";

function fakePiece(opts: {
  run: (ctx: unknown) => Promise<unknown>;
  actionName?: string;
}): Piece {
  return {
    displayName: "fake",
    actions: () => ({
      [opts.actionName ?? "do_thing"]: {
        name: opts.actionName ?? "do_thing",
        run: opts.run,
      },
    }),
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

test("health endpoint responds 200", async () => {
  await withServer(
    () => {},
    async (baseUrl) => {
      const res = await fetch(`${baseUrl}/health`);
      assert.equal(res.status, 200);
      const body = await res.json();
      assert.equal(body.ok, true);
    },
  );
});

test("round-trip: action receives auth + props and returns output", async () => {
  await withServer(
    (loader) =>
      loader.cache(
        "echo",
        fakePiece({
          run: async (ctx: any) => ({
            echoedAuth: ctx.auth,
            echoedProps: ctx.propsValue,
            instanceSeen: ctx.run.id,
            blockSeen: ctx.step.name,
          }),
        }),
      ),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "echo",
        action: "do_thing",
        auth: { token: "abc" },
        props: { channel: "C1", text: "hi" },
        instance_id: "inst-1",
        block_id: "step-1",
      });
      assert.equal(status, 200);
      assert.equal(body.ok, true);
      assert.deepEqual(body.output, {
        echoedAuth: { token: "abc" },
        echoedProps: { channel: "C1", text: "hi" },
        instanceSeen: "inst-1",
        blockSeen: "step-1",
      });
    },
  );
});

test("piece throws HTTP 500 → classified retryable → sidecar responds 502", async () => {
  await withServer(
    (loader) =>
      loader.cache(
        "flaky",
        fakePiece({
          run: async () => {
            const err: any = new Error("upstream failed");
            err.response = { status: 503, data: { reason: "overloaded" } };
            throw err;
          },
        }),
      ),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "flaky",
        action: "do_thing",
      });
      assert.equal(status, 502);
      assert.equal(body.ok, false);
      assert.equal(body.error.type, "retryable");
      assert.match(body.error.message, /upstream failed/);
    },
  );
});

test("piece throws HTTP 400 → classified permanent → sidecar responds 500", async () => {
  await withServer(
    (loader) =>
      loader.cache(
        "strict",
        fakePiece({
          run: async () => {
            const err: any = new Error("bad request");
            err.response = { status: 400, data: { field: "channel" } };
            throw err;
          },
        }),
      ),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "strict",
        action: "do_thing",
      });
      assert.equal(status, 500);
      assert.equal(body.error.type, "permanent");
    },
  );
});

test("unknown piece returns 422 permanent", async () => {
  await withServer(
    () => {},
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "not-installed",
        action: "anything",
      });
      assert.equal(status, 422);
      assert.equal(body.error.type, "permanent");
      assert.match(body.error.message, /not installed/);
    },
  );
});

test("invalid piece name rejected without import attempt", async () => {
  await withServer(
    () => {},
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "../etc/passwd",
        action: "read",
      });
      assert.equal(status, 422);
      assert.equal(body.error.type, "permanent");
      assert.match(body.error.message, /invalid piece name/);
    },
  );
});

test("unknown action on valid piece returns 422 permanent", async () => {
  await withServer(
    (loader) =>
      loader.cache(
        "one-action",
        fakePiece({ run: async () => ({}), actionName: "only_this" }),
      ),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "one-action",
        action: "does_not_exist",
      });
      assert.equal(status, 422);
      assert.match(body.error.message, /not found/);
    },
  );
});

test("missing piece/action fields → 422", async () => {
  await withServer(
    () => {},
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, { piece: 1, action: null });
      assert.equal(status, 422);
      assert.equal(body.error.type, "permanent");
    },
  );
});

test("piece timeout → retryable", async () => {
  await withServer(
    (loader) =>
      loader.cache(
        "slow",
        fakePiece({
          run: () => new Promise(() => {}), // never resolves
        }),
      ),
    async (baseUrl) => {
      // Narrow timeout by creating a dedicated server, since the helper's is 5s.
      const loader = createDefaultLoader();
      loader.cache(
        "slow",
        fakePiece({ run: () => new Promise(() => {}) }),
      );
      const server = createServer({
        port: 0,
        host: "127.0.0.1",
        loader,
        requestTimeoutMs: 200,
        log: () => {},
      });
      await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
      const addr = server.address() as AddressInfo;
      try {
        const { status, body } = await post(`http://127.0.0.1:${addr.port}/execute`, {
          piece: "slow",
          action: "do_thing",
        });
        assert.equal(status, 502);
        assert.equal(body.error.type, "retryable");
        assert.match(body.error.message, /timed out/);
      } finally {
        await new Promise<void>((resolve) => server.close(() => resolve()));
      }
      // Suppress unused-var warning for the `baseUrl` parameter in outer helper.
      void baseUrl;
    },
  );
});

test("PieceExecutionError from piece run propagates verbatim", async () => {
  await withServer(
    (loader) =>
      loader.cache(
        "explicit",
        fakePiece({
          run: async () => {
            throw new PieceExecutionError("permanent", "cannot proceed", { code: "X" });
          },
        }),
      ),
    async (baseUrl) => {
      const { status, body } = await post(`${baseUrl}/execute`, {
        piece: "explicit",
        action: "do_thing",
      });
      assert.equal(status, 500);
      assert.equal(body.error.type, "permanent");
      assert.equal(body.error.message, "cannot proceed");
      assert.deepEqual(body.error.details, { code: "X" });
    },
  );
});

test("allowlist blocks non-allowed pieces", async () => {
  const loader = createDefaultLoader({ allowlist: ["slack"] });
  loader.cache("slack", fakePiece({ run: async () => "ok" }));
  const server = createServer({ port: 0, host: "127.0.0.1", loader, log: () => {} });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const addr = server.address() as AddressInfo;
  try {
    const { status } = await post(`http://127.0.0.1:${addr.port}/execute`, {
      piece: "gmail",
      action: "send",
    });
    assert.equal(status, 422);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
