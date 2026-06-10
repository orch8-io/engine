// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Orch8, Inc.
//
// Tests for the polling-trigger op (POST /poll): stub pieces with polling
// triggers, cursor/state round-trips through the seeded store, item shapes,
// error classification, and webhook-strategy rejection.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { AddressInfo } from "node:net";
import { createServer } from "../src/server.ts";
import { createDefaultLoader, Piece, PieceTrigger } from "../src/registry.ts";
import { buildTriggerContext } from "../src/context.ts";
import { PieceExecutionError } from "../src/errors.ts";

// ---------------------------------------------------------------------------
// Helpers (same pattern as server_routes.test.ts)
// ---------------------------------------------------------------------------

function pieceWithTriggers(triggers: Record<string, PieceTrigger>): Piece {
  return {
    displayName: "fake-with-triggers",
    actions: () => ({}),
    triggers: () => triggers,
  };
}

/**
 * Stub polling trigger mimicking the AP `pollingHelper` contract: keeps a
 * numeric cursor in `ctx.store` under "cursor" and returns the items that
 * appeared since. `source` is the fake upstream event log.
 */
function pollingTrigger(name: string, source: Array<Record<string, unknown>>): PieceTrigger {
  return {
    name,
    type: "POLLING",
    run: async (rawCtx: unknown) => {
      const ctx = rawCtx as {
        store: {
          get: (k: string) => Promise<unknown>;
          put: (k: string, v: unknown) => Promise<unknown>;
        };
        auth: unknown;
        propsValue: Record<string, unknown>;
      };
      const cursor = ((await ctx.store.get("cursor")) as number | null) ?? 0;
      const fresh = source.slice(cursor);
      await ctx.store.put("cursor", source.length);
      return fresh;
    },
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
    log: () => {},
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

// ---------------------------------------------------------------------------
// buildTriggerContext
// ---------------------------------------------------------------------------

test("buildTriggerContext seeds the store from state and dumps mutations", async () => {
  const { ctx, dumpStore } = buildTriggerContext({
    auth: { token: "t" },
    propsValue: { a: 1 },
    state: { cursor: 41, seen: ["x"] },
    slug: "my-trigger",
  });
  const store = (ctx as any).store;
  assert.equal(await store.get("cursor"), 41);
  assert.deepEqual(await store.get("seen"), ["x"]);
  assert.equal(await store.get("missing"), null);

  await store.put("cursor", 42);
  await store.delete("seen");
  assert.deepEqual(dumpStore(), { cursor: 42 });
});

test("buildTriggerContext with null state starts empty", async () => {
  const { ctx, dumpStore } = buildTriggerContext({
    auth: null,
    propsValue: {},
    state: null,
    slug: "s",
  });
  assert.equal(await (ctx as any).store.get("anything"), null);
  assert.deepEqual(dumpStore(), {});
  // Webhook members exist but are empty.
  assert.deepEqual((ctx as any).payload, {});
  assert.equal((ctx as any).webhookUrl, "");
});

// ---------------------------------------------------------------------------
// POST /poll happy paths
// ---------------------------------------------------------------------------

test("POST /poll returns items and new state from a stub polling trigger", async () => {
  const source = [{ id: "evt_1" }, { id: "evt_2" }];
  await withServer(
    (loader) =>
      loader.cache("faketrigger", pieceWithTriggers({ new_event: pollingTrigger("new_event", source) })),
    async (base) => {
      const { status, body } = await post(`${base}/poll`, {
        piece: "faketrigger",
        trigger: "new_event",
        auth: { key: "k" },
        props: {},
        state: null,
        slug: "reg-1",
      });
      assert.equal(status, 200);
      assert.equal(body.ok, true);
      assert.deepEqual(body.items, [{ id: "evt_1" }, { id: "evt_2" }]);
      assert.deepEqual(body.state, { cursor: 2 });
    },
  );
});

test("POST /poll round-trips the cursor: second poll only sees new items", async () => {
  const source = [{ id: "evt_1" }, { id: "evt_2" }];
  await withServer(
    (loader) =>
      loader.cache("faketrigger", pieceWithTriggers({ new_event: pollingTrigger("new_event", source) })),
    async (base) => {
      const first = await post(`${base}/poll`, {
        piece: "faketrigger",
        trigger: "new_event",
        state: null,
      });
      assert.equal(first.body.items.length, 2);

      // Upstream produces one more event; replay the returned state.
      source.push({ id: "evt_3" });
      const second = await post(`${base}/poll`, {
        piece: "faketrigger",
        trigger: "new_event",
        state: first.body.state,
      });
      assert.equal(second.status, 200);
      assert.deepEqual(second.body.items, [{ id: "evt_3" }]);
      assert.deepEqual(second.body.state, { cursor: 3 });

      // Replaying the same state again yields no items (dedupe).
      const third = await post(`${base}/poll`, {
        piece: "faketrigger",
        trigger: "new_event",
        state: second.body.state,
      });
      assert.deepEqual(third.body.items, []);
    },
  );
});

test("POST /poll passes auth and props through to the trigger context", async () => {
  let seen: { auth: unknown; props: unknown } | null = null;
  const trig: PieceTrigger = {
    name: "spy",
    type: "POLLING",
    run: async (rawCtx: unknown) => {
      const ctx = rawCtx as { auth: unknown; propsValue: unknown };
      seen = { auth: ctx.auth, props: ctx.propsValue };
      return [];
    },
  };
  await withServer(
    (loader) => loader.cache("spy-piece", pieceWithTriggers({ spy: trig })),
    async (base) => {
      const { status } = await post(`${base}/poll`, {
        piece: "spy-piece",
        trigger: "spy",
        auth: { access_token: "xoxb" },
        props: { channel: "C1" },
      });
      assert.equal(status, 200);
      assert.deepEqual(seen, { auth: { access_token: "xoxb" }, props: { channel: "C1" } });
    },
  );
});

test("POST /poll wraps a single non-array result into one item", async () => {
  const trig: PieceTrigger = {
    name: "single",
    type: "POLLING",
    run: async () => ({ id: "only" }),
  };
  await withServer(
    (loader) => loader.cache("single-piece", pieceWithTriggers({ single: trig })),
    async (base) => {
      const { body } = await post(`${base}/poll`, { piece: "single-piece", trigger: "single" });
      assert.deepEqual(body.items, [{ id: "only" }]);
    },
  );
});

test("POST /poll finds array-shaped triggers by name", async () => {
  const piece: Piece = {
    actions: () => ({}),
    triggers: () => [pollingTrigger("arr_trigger", [{ n: 1 }])],
  };
  await withServer(
    (loader) => loader.cache("arr-piece", piece),
    async (base) => {
      const { status, body } = await post(`${base}/poll`, {
        piece: "arr-piece",
        trigger: "arr_trigger",
      });
      assert.equal(status, 200);
      assert.deepEqual(body.items, [{ n: 1 }]);
    },
  );
});

// ---------------------------------------------------------------------------
// POST /poll failures
// ---------------------------------------------------------------------------

test("POST /poll 422 on missing piece/trigger fields", async () => {
  await withServer(
    () => {},
    async (base) => {
      const r1 = await post(`${base}/poll`, { piece: "x" });
      assert.equal(r1.status, 422);
      assert.equal(r1.body.error.type, "permanent");

      const r2 = await post(`${base}/poll`, { trigger: "t" });
      assert.equal(r2.status, 422);

      const r3 = await post(`${base}/poll`, { piece: "x", trigger: "t", state: [1, 2] });
      assert.equal(r3.status, 422);
      assert.match(r3.body.error.message, /state/);
    },
  );
});

test("POST /poll 422 on unknown piece or trigger", async () => {
  await withServer(
    (loader) => loader.cache("known", pieceWithTriggers({})),
    async (base) => {
      const missingPiece = await post(`${base}/poll`, { piece: "ghost", trigger: "t" });
      assert.equal(missingPiece.status, 422);
      assert.match(missingPiece.body.error.message, /not installed|invalid piece/);

      const missingTrigger = await post(`${base}/poll`, { piece: "known", trigger: "ghost" });
      assert.equal(missingTrigger.status, 422);
      assert.match(missingTrigger.body.error.message, /trigger 'ghost' not found/);
    },
  );
});

test("POST /poll rejects webhook-strategy triggers with a permanent error", async () => {
  const trig: PieceTrigger = {
    name: "hooked",
    type: "WEBHOOK",
    run: async () => [],
  };
  await withServer(
    (loader) => loader.cache("hook-piece", pieceWithTriggers({ hooked: trig })),
    async (base) => {
      const { status, body } = await post(`${base}/poll`, { piece: "hook-piece", trigger: "hooked" });
      assert.equal(status, 422);
      assert.equal(body.error.type, "permanent");
      assert.match(body.error.message, /webhook strategy/);
    },
  );
});

test("POST /poll classifies trigger run errors like /execute does", async () => {
  const retryable: PieceTrigger = {
    name: "flaky",
    type: "POLLING",
    run: async () => {
      throw new PieceExecutionError("retryable", "upstream 503");
    },
  };
  const permanent: PieceTrigger = {
    name: "broken",
    type: "POLLING",
    run: async () => {
      throw new TypeError("piece bug");
    },
  };
  await withServer(
    (loader) => loader.cache("errs", pieceWithTriggers({ flaky: retryable, broken: permanent })),
    async (base) => {
      const r = await post(`${base}/poll`, { piece: "errs", trigger: "flaky" });
      assert.equal(r.status, 502);
      assert.equal(r.body.error.type, "retryable");
      assert.match(r.body.error.message, /upstream 503/);

      const p = await post(`${base}/poll`, { piece: "errs", trigger: "broken" });
      assert.equal(p.status, 500);
      assert.equal(p.body.error.type, "permanent");
    },
  );
});

test("POST /poll trigger state survives a run that throws (engine keeps old cursor)", async () => {
  // The sidecar returns no state on failure — the engine keeps its persisted
  // cursor. This test just pins the failure envelope shape: no items/state.
  const trig: PieceTrigger = {
    name: "boom",
    type: "POLLING",
    run: async () => {
      throw new PieceExecutionError("retryable", "kaput");
    },
  };
  await withServer(
    (loader) => loader.cache("boom-piece", pieceWithTriggers({ boom: trig })),
    async (base) => {
      const { status, body } = await post(`${base}/poll`, {
        piece: "boom-piece",
        trigger: "boom",
        state: { cursor: 7 },
      });
      assert.equal(status, 502);
      assert.equal(body.ok, false);
      assert.equal(body.items, undefined);
      assert.equal(body.state, undefined);
    },
  );
});
