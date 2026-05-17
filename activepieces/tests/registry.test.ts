// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Orch8, Inc.
//
// Unit tests for the piece registry: loader creation, name validation,
// caching, allowlist enforcement, and action lookup.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { createDefaultLoader, findAction } from "../src/registry.ts";
import { PieceExecutionError } from "../src/errors.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function recordPiece(actions: Record<string, { name: string; run: (ctx: unknown) => Promise<unknown> }>) {
  return {
    displayName: "test-piece",
    actions: () => actions,
  };
}

function arrayPiece(actions: Array<{ name: string; run: (ctx: unknown) => Promise<unknown> }>) {
  return {
    displayName: "array-piece",
    actions: () => actions as any,
  };
}

// ---------------------------------------------------------------------------
// createDefaultLoader — basic shape
// ---------------------------------------------------------------------------

test("createDefaultLoader returns loader with load, cache, clear methods", () => {
  const loader = createDefaultLoader();
  assert.equal(typeof loader.load, "function");
  assert.equal(typeof loader.cache, "function");
  assert.equal(typeof loader.clear, "function");
});

test("createDefaultLoader with no options works (no allowlist)", () => {
  const loader = createDefaultLoader();
  // Should not throw during creation
  assert.ok(loader);
});

// ---------------------------------------------------------------------------
// cache / load / clear
// ---------------------------------------------------------------------------

test("cache() stores piece, load() retrieves from cache", async () => {
  const loader = createDefaultLoader();
  const piece = recordPiece({ ping: { name: "ping", run: async () => "pong" } });
  loader.cache("test-piece", piece);

  const loaded = await loader.load("test-piece");
  assert.equal(loaded, piece);
});

test("clear() empties cache, subsequent load triggers import (and fails for missing pkg)", async () => {
  const loader = createDefaultLoader();
  const piece = recordPiece({ a: { name: "a", run: async () => 1 } });
  loader.cache("cached-thing", piece);

  // Confirm it's cached
  const loaded = await loader.load("cached-thing");
  assert.equal(loaded, piece);

  // Clear and try again — should fail because @activepieces/piece-cached-thing is not installed
  loader.clear();
  await assert.rejects(
    () => loader.load("cached-thing"),
    (err: any) => {
      assert.ok(err instanceof PieceExecutionError);
      assert.equal(err.type, "permanent");
      assert.match(err.message, /not installed/);
      return true;
    },
  );
});

test("multiple cached pieces are independent", async () => {
  const loader = createDefaultLoader();
  const pieceA = recordPiece({ x: { name: "x", run: async () => "a" } });
  const pieceB = recordPiece({ y: { name: "y", run: async () => "b" } });

  loader.cache("alpha", pieceA);
  loader.cache("beta", pieceB);

  assert.equal(await loader.load("alpha"), pieceA);
  assert.equal(await loader.load("beta"), pieceB);
  assert.notEqual(pieceA, pieceB);
});

// ---------------------------------------------------------------------------
// load() — name validation
// ---------------------------------------------------------------------------

test("load() rejects piece names with path traversal (../)", async () => {
  const loader = createDefaultLoader();
  await assert.rejects(
    () => loader.load("../etc/passwd"),
    (err: any) => {
      assert.ok(err instanceof PieceExecutionError);
      assert.equal(err.type, "permanent");
      assert.match(err.message, /invalid piece name/);
      return true;
    },
  );
});

test("load() rejects piece names with uppercase letters", async () => {
  const loader = createDefaultLoader();
  await assert.rejects(
    () => loader.load("Slack"),
    (err: any) => {
      assert.ok(err instanceof PieceExecutionError);
      assert.match(err.message, /invalid piece name/);
      return true;
    },
  );
});

test("load() rejects empty string", async () => {
  const loader = createDefaultLoader();
  await assert.rejects(
    () => loader.load(""),
    (err: any) => {
      assert.ok(err instanceof PieceExecutionError);
      assert.match(err.message, /invalid piece name/);
      return true;
    },
  );
});

test("load() rejects names starting with hyphen", async () => {
  const loader = createDefaultLoader();
  await assert.rejects(
    () => loader.load("-slack"),
    (err: any) => {
      assert.ok(err instanceof PieceExecutionError);
      assert.match(err.message, /invalid piece name/);
      return true;
    },
  );
});

test("load() rejects names with spaces", async () => {
  const loader = createDefaultLoader();
  await assert.rejects(
    () => loader.load("google sheets"),
    (err: any) => {
      assert.ok(err instanceof PieceExecutionError);
      assert.match(err.message, /invalid piece name/);
      return true;
    },
  );
});

test("load() rejects names with @ character", async () => {
  const loader = createDefaultLoader();
  await assert.rejects(
    () => loader.load("@scope/pkg"),
    (err: any) => {
      assert.ok(err instanceof PieceExecutionError);
      assert.match(err.message, /invalid piece name/);
      return true;
    },
  );
});

test("load() rejects names with / character", async () => {
  const loader = createDefaultLoader();
  await assert.rejects(
    () => loader.load("foo/bar"),
    (err: any) => {
      assert.ok(err instanceof PieceExecutionError);
      assert.match(err.message, /invalid piece name/);
      return true;
    },
  );
});

test("load() rejects names with \\ character", async () => {
  const loader = createDefaultLoader();
  await assert.rejects(
    () => loader.load("foo\\bar"),
    (err: any) => {
      assert.ok(err instanceof PieceExecutionError);
      assert.match(err.message, /invalid piece name/);
      return true;
    },
  );
});

test("load() accepts valid lowercase-hyphen name 'slack' (from cache)", async () => {
  const loader = createDefaultLoader();
  const piece = recordPiece({ send: { name: "send", run: async () => "ok" } });
  loader.cache("slack", piece);

  const loaded = await loader.load("slack");
  assert.equal(loaded, piece);
});

test("load() accepts valid name 'google-sheets' (from cache)", async () => {
  const loader = createDefaultLoader();
  const piece = recordPiece({ read: { name: "read", run: async () => [] } });
  loader.cache("google-sheets", piece);

  const loaded = await loader.load("google-sheets");
  assert.equal(loaded, piece);
});

test("load() accepts valid name 'http' (from cache)", async () => {
  const loader = createDefaultLoader();
  const piece = recordPiece({ get: { name: "get", run: async () => ({}) } });
  loader.cache("http", piece);

  const loaded = await loader.load("http");
  assert.equal(loaded, piece);
});

// ---------------------------------------------------------------------------
// load() — allowlist enforcement
// ---------------------------------------------------------------------------

test("load() with allowlist blocks non-allowed pieces", async () => {
  const loader = createDefaultLoader({ allowlist: ["slack", "http"] });
  loader.cache("gmail", recordPiece({ send: { name: "send", run: async () => "ok" } }));

  await assert.rejects(
    () => loader.load("gmail"),
    (err: any) => {
      assert.ok(err instanceof PieceExecutionError);
      assert.equal(err.type, "permanent");
      assert.match(err.message, /not in the configured allowlist/);
      return true;
    },
  );
});

test("load() with allowlist allows listed pieces (from cache)", async () => {
  const loader = createDefaultLoader({ allowlist: ["slack"] });
  const piece = recordPiece({ post: { name: "post", run: async () => "sent" } });
  loader.cache("slack", piece);

  const loaded = await loader.load("slack");
  assert.equal(loaded, piece);
});

test("load() with empty allowlist array blocks all pieces", async () => {
  const loader = createDefaultLoader({ allowlist: [] });
  loader.cache("slack", recordPiece({ x: { name: "x", run: async () => 1 } }));

  await assert.rejects(
    () => loader.load("slack"),
    (err: any) => {
      assert.ok(err instanceof PieceExecutionError);
      assert.match(err.message, /not in the configured allowlist/);
      return true;
    },
  );
});

test("allowlist with single item works", async () => {
  const loader = createDefaultLoader({ allowlist: ["http"] });
  const piece = recordPiece({ fetch: { name: "fetch", run: async () => ({}) } });
  loader.cache("http", piece);

  const loaded = await loader.load("http");
  assert.equal(loaded, piece);

  // Any other piece is blocked
  loader.cache("slack", recordPiece({ x: { name: "x", run: async () => 1 } }));
  await assert.rejects(
    () => loader.load("slack"),
    (err: any) => {
      assert.match(err.message, /not in the configured allowlist/);
      return true;
    },
  );
});

// ---------------------------------------------------------------------------
// load() — non-installed piece
// ---------------------------------------------------------------------------

test("load() for non-installed piece throws PieceExecutionError permanent", async () => {
  const loader = createDefaultLoader();
  await assert.rejects(
    () => loader.load("definitely-not-a-real-piece"),
    (err: any) => {
      assert.ok(err instanceof PieceExecutionError);
      assert.equal(err.type, "permanent");
      assert.match(err.message, /not installed/);
      return true;
    },
  );
});

// ---------------------------------------------------------------------------
// findAction — record-based
// ---------------------------------------------------------------------------

test("findAction() finds action from record-based actions by key", () => {
  const piece = recordPiece({
    send_message: { name: "send_message", run: async () => "sent" },
    delete_message: { name: "delete_message", run: async () => "deleted" },
  });

  const action = findAction(piece, "send_message");
  assert.equal(action.name, "send_message");
});

test("findAction() finds action by name field in record values (key differs from name)", () => {
  // Key is "send" but action.name is "send_channel_message"
  const piece = recordPiece({
    send: { name: "send_channel_message", run: async () => "ok" },
  });

  const action = findAction(piece, "send_channel_message");
  assert.equal(action.name, "send_channel_message");
});

test("findAction() with empty record throws PieceExecutionError", () => {
  const piece = recordPiece({});

  assert.throws(
    () => findAction(piece, "anything"),
    (err: any) => {
      assert.ok(err instanceof PieceExecutionError);
      assert.match(err.message, /not found/);
      return true;
    },
  );
});

// ---------------------------------------------------------------------------
// findAction — array-based
// ---------------------------------------------------------------------------

test("findAction() finds action from array-based actions", () => {
  const piece = arrayPiece([
    { name: "read_rows", run: async () => [] },
    { name: "write_rows", run: async () => ({ count: 5 }) },
  ]);

  const action = findAction(piece, "write_rows");
  assert.equal(action.name, "write_rows");
});

test("findAction() with empty array throws PieceExecutionError", () => {
  const piece = arrayPiece([]);

  assert.throws(
    () => findAction(piece, "something"),
    (err: any) => {
      assert.ok(err instanceof PieceExecutionError);
      assert.match(err.message, /not found/);
      return true;
    },
  );
});

test("findAction() with null entry in array throws TypeError (no null guard)", () => {
  // The current implementation does raw.find((a) => a.name === actionName)
  // which crashes on null entries — this documents the behavior.
  const piece = {
    displayName: "has-null",
    actions: () => [null, { name: "valid_action", run: async () => "ok" }] as any,
  };

  assert.throws(
    () => findAction(piece, "valid_action"),
    (err: any) => {
      assert.ok(err instanceof TypeError);
      return true;
    },
  );
});

// ---------------------------------------------------------------------------
// findAction — missing action
// ---------------------------------------------------------------------------

test("findAction() throws PieceExecutionError for missing action", () => {
  const piece = recordPiece({
    only_this: { name: "only_this", run: async () => 42 },
  });

  assert.throws(
    () => findAction(piece, "does_not_exist"),
    (err: any) => {
      assert.ok(err instanceof PieceExecutionError);
      assert.equal(err.type, "permanent");
      assert.match(err.message, /not found/);
      return true;
    },
  );
});

// ---------------------------------------------------------------------------
// findAction — actions as property (not function)
// ---------------------------------------------------------------------------

test("findAction() handles piece with actions as plain object (not function)", () => {
  // Some older pieces expose actions as a direct object, not a function
  const piece = {
    displayName: "legacy",
    actions: {
      do_thing: { name: "do_thing", run: async () => "done" },
    },
  } as any;

  const action = findAction(piece, "do_thing");
  assert.equal(action.name, "do_thing");
});

// ---------------------------------------------------------------------------
// isPiece — structural recognition
// ---------------------------------------------------------------------------

test("piece with actions as function is recognized (loads from cache)", async () => {
  const loader = createDefaultLoader();
  const piece = { actions: () => ({}) };
  loader.cache("func-piece", piece as any);
  const loaded = await loader.load("func-piece");
  assert.equal(loaded, piece);
});

test("piece with actions as object is recognized (loads from cache)", async () => {
  const loader = createDefaultLoader();
  const piece = { actions: {} };
  loader.cache("obj-piece", piece as any);
  const loaded = await loader.load("obj-piece");
  assert.equal(loaded, piece);
});

test("non-object values are not recognized as pieces (findAction throws on number)", () => {
  assert.throws(
    () => findAction(42 as any, "x"),
  );
});

test("null is not recognized as a piece (findAction throws on null)", () => {
  assert.throws(
    () => findAction(null as any, "x"),
  );
});

test("object without actions is not recognized (findAction throws)", () => {
  const notAPiece = { displayName: "nope" } as any;
  assert.throws(
    () => findAction(notAPiece, "x"),
  );
});
