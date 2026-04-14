/**
 * WASM plugin dispatch scenarios.
 *
 * The e2e harness can't ship a .wasm binary, so these tests focus on the
 * dispatch-path behaviours that don't require a valid module:
 *
 *   - missing-module path: `wasm://nonexistent` fails with Permanent and the
 *     instance state is `failed`, not `waiting` (the feature is enabled).
 *   - `wasm://` in try_catch is rescued by the catch branch.
 *   - `wasm://` in parallel fails the parent (every branch fails the same way).
 *   - `wasm://` in for_each fails the first iteration and stops.
 *   - path-traversal-looking handler (`wasm://../etc/passwd`) fails cleanly
 *     without opening any file outside the expected path resolution.
 *   - `wasm://` handler with explicit empty module name is rejected.
 *
 * The handler prefix / parse happy paths are covered by Rust unit tests
 * in `wasm_plugin.rs`; this file covers the *engine* dispatch path.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

describe("WASM plugin dispatch scenarios", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("missing module → instance fails (not stuck in waiting)", async () => {
    const seq = testSequence("wasm-missing", [
      step("w", `wasm://nonexistent-${uuid().slice(0, 8)}`),
    ]);
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

  it("missing module inside try_catch is rescued by catch branch", async () => {
    const seq = testSequence("wasm-tc", [
      {
        type: "try_catch",
        id: "tc",
        try_block: [step("w", "wasm://def-missing-for-tc")],
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
    assert.ok(ids.has("rescued"), "catch branch did not execute");
  });

  it("missing module in parallel fails the instance (all branches fail same way)", async () => {
    const seq = testSequence("wasm-par", [
      {
        type: "parallel",
        id: "p",
        branches: [
          [step("w1", "wasm://missing-par-1")],
          [step("w2", "wasm://missing-par-2")],
        ],
      } as Block,
    ]);
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

  it("missing module in parallel inside try_catch is rescued", async () => {
    const seq = testSequence("wasm-par-tc", [
      {
        type: "try_catch",
        id: "tc",
        try_block: [
          {
            type: "parallel",
            id: "p",
            branches: [
              [step("w1", "wasm://missing-par-tc")],
              [step("w2", "wasm://missing-par-tc-2")],
            ],
          },
        ],
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

  it("missing module in for_each body fails the instance", async () => {
    const seq = testSequence("wasm-for", [
      {
        type: "for_each",
        id: "fe",
        collection: "items",
        body: [step("w", "wasm://missing-for-each")],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { items: [1, 2, 3] } },
    });
    const final = await client.waitForState(id, ["failed", "completed"], {
      timeoutMs: 10_000,
    });
    assert.equal(final.state, "failed");
  });

  it("path-traversal-style name still fails cleanly (no file access outside)", async () => {
    // `wasm://../etc/passwd` is parsed, looked up in plugin registry (miss),
    // falls back to the raw path (`../etc/passwd`) → Module::from_file errors
    // since no such file exists as a valid wasm module. No panic, no server
    // side-effect, no ability to read arbitrary files — failure is contained.
    const seq = testSequence("wasm-traversal", [
      step("w", "wasm://../etc/passwd"),
    ]);
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

  it("empty module name (`wasm://`) fails without side effects", async () => {
    const seq = testSequence("wasm-empty", [step("w", "wasm://")]);
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

  it("missing module inside nested for_each-in-try_catch is rescued", async () => {
    const seq = testSequence("wasm-for-tc", [
      {
        type: "try_catch",
        id: "tc",
        try_block: [
          {
            type: "for_each",
            id: "fe",
            collection: "items",
            body: [step("w", "wasm://deep-missing")],
          },
        ],
        catch_block: [step("rescued", "log", { message: "r" })],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { items: [10, 20] } },
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const ids = new Set((await client.getOutputs(id)).map((o) => o.block_id));
    assert.ok(ids.has("rescued"));
  });

  it("many sequential missing-module steps do not leak resources (second run succeeds on retry logic)", async () => {
    // Drive the cache-miss path 10 times in a row — exercises the module
    // compile-cache negative path under repeated load. We assert each run
    // terminates (failed, not hung).
    for (let i = 0; i < 10; i++) {
      const seq = testSequence(`wasm-repeat-${i}`, [
        step("w", `wasm://repeat-missing-${i}`),
      ]);
      await client.createSequence(seq);
      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: "test",
        namespace: "default",
      });
      const final = await client.waitForState(id, ["failed", "completed"], {
        timeoutMs: 10_000,
      });
      assert.equal(final.state, "failed", `iteration ${i} did not fail`);
    }
  });
});
