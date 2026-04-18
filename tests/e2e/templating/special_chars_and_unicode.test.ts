/**
 * Round-trip fidelity for non-ASCII and punctuation-heavy values.
 *
 * The custom template resolver (orch8-engine/src/template.rs) is string-
 * based but works over owned JSON values. Unicode, quotes, and newlines
 * in the SOURCE VALUE are never pattern-matched by the `{{...}}` scanner,
 * so they should survive unchanged. These tests lock that in from an
 * end-to-end perspective: HTTP JSON → engine context → template resolve →
 * handler output → HTTP JSON.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Templating — unicode and special characters", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("round-trips unicode (CJK, accented latin, emoji)", async () => {
    const name = "日本語 café 🎉";
    const seq = testSequence("tpl-unicode", [
      step("s1", "log", { message: "{{context.data.name}}" }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { name } },
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);
    const s1 = outputs.find((o) => o.block_id === "s1");
    assert.ok(s1, "s1 output exists");
    assert.equal((s1.output as { message: string }).message, name);
  });

  it("round-trips quotes, backslashes, and multi-line strings", async () => {
    const tricky = `she said "hi" and \\ then\nline two\nline three`;
    const seq = testSequence("tpl-special", [
      step("s1", "log", {
        message: "quote={{context.data.q}} end",
      }),
      step("s2", "log", {
        message: "{{context.data.q}}",
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { q: tricky } },
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);
    const byId = new Map(outputs.map((o) => [o.block_id, o]));

    // Whole-string template: the value is returned verbatim, preserving
    // newlines, quotes, and backslashes.
    assert.equal(
      (byId.get("s2")?.output as { message: string } | undefined)?.message,
      tricky,
    );

    // Inline template: the value is spliced into the surrounding string
    // unchanged (no escaping, no corruption).
    assert.equal(
      (byId.get("s1")?.output as { message: string } | undefined)?.message,
      `quote=${tricky} end`,
    );
  });
});
