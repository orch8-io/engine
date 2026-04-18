/**
 * Deep nested object path templating resolution.
 *
 * Engine note: `resolve_path` in `orch8-engine/src/template.rs` walks the
 * dotted path via `navigate_json`, which short-circuits to `None` on the
 * first missing segment. `resolve_path` then returns `Value::Null` (or a
 * literal fallback if one is supplied). Inline interpolation stringifies
 * a Null as the literal `"null"`.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Nested Object Path Templating", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should resolve deep nested paths like `{{context.a.b.c.d.e.field}}`", async () => {
    // 6-level deep path resolves to the leaf value.
    const seq = testSequence("tpl-nested-6", [
      step("s1", "log", {
        message: "{{context.data.a.b.c.d.e.field}}",
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: {
        data: {
          a: { b: { c: { d: { e: { field: "leaf-value" } } } } },
        },
      },
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);
    const s1 = outputs.find((o) => o.block_id === "s1");
    assert.ok(s1, "s1 output should exist");
    assert.equal(
      (s1.output as { message: string }).message,
      "leaf-value",
      "6-level deep path should resolve to leaf value",
    );
  });

  it("missing intermediate key in a deep path resolves without crashing", async () => {
    // When `c` is absent mid-chain, the whole-string template resolves to
    // JSON null and the engine marks the step completed — no error thrown.
    const seq = testSequence("tpl-nested-missing", [
      step("s1", "log", {
        message: "{{context.data.a.b.c.d.e.field}}",
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: {
        // Intentionally omit `c` onwards — navigation short-circuits.
        data: { a: { b: {} } },
      },
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);
    const s1 = outputs.find((o) => o.block_id === "s1");
    assert.ok(s1, "s1 output should exist despite missing intermediate key");
    // Whole-string `{{missing}}` → JSON null (not an error, not a literal).
    const msg = (s1.output as { message: unknown }).message;
    assert.equal(
      msg,
      null,
      `missing deep path should resolve to JSON null, got ${JSON.stringify(msg)}`,
    );
  });

  it("resolves deep path alongside other templates inline", async () => {
    const seq = testSequence("tpl-nested-inline-6", [
      step("s1", "log", {
        message:
          "leaf={{context.data.a.b.c.d.e.field}} name={{context.data.a.b.c.d.e.name}}",
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: {
        data: {
          a: {
            b: { c: { d: { e: { field: "deep", name: "Alice" } } } },
          },
        },
      },
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);
    const s1 = outputs.find((o) => o.block_id === "s1");
    assert.ok(s1, "s1 output should exist");
    assert.equal(
      (s1.output as { message: string }).message,
      "leaf=deep name=Alice",
    );
  });
});
