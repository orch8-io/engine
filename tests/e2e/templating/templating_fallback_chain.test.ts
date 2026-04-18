/**
 * Templating fallback behaviour.
 *
 * Engine note: `resolve_path` in `orch8-engine/src/template.rs` splits on
 * the FIRST `|` only — everything after it is treated as a single literal
 * fallback string. There is no chained walk; `{{x.y|a|b|c}}` resolves to
 * the literal string `"a|b|c"` when `x.y` is missing. The original plan
 * (walk the chain picking the first resolvable value) does not match the
 * current engine, so the assertions below verify the ACTUAL observable
 * behaviour and will serve as a regression guard if the engine ever grows
 * real fallback-chain support.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Templating Fallback Chain", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("uses single fallback after first `|` when primary path is missing", async () => {
    const seq = testSequence("tpl-fallback-single", [
      step("s1", "log", {
        message: "{{context.data.missing|fallback_a}}",
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: {} },
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);
    const s1 = outputs.find((o) => o.block_id === "s1");
    assert.ok(s1, "s1 output should exist");
    assert.equal(
      (s1.output as { message: string }).message,
      "fallback_a",
      "missing primary path should yield the fallback literal",
    );
  });

  it("primary path wins over fallback when present", async () => {
    const seq = testSequence("tpl-fallback-primary-wins", [
      step("s1", "log", {
        message: "{{context.data.greeting|default_val}}",
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { greeting: "hello" } },
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);
    const s1 = outputs.find((o) => o.block_id === "s1");
    assert.ok(s1, "s1 output should exist");
    assert.equal(
      (s1.output as { message: string }).message,
      "hello",
      "primary path value should be used when present",
    );
  });

  it("extra `|` segments are preserved in the fallback literal", async () => {
    // Engine splits on first `|` only; `x.y|a|b|c` → fallback = `a|b|c`.
    // This asserts the observable behaviour of the current engine.
    const seq = testSequence("tpl-fallback-literal-chain", [
      step("s1", "log", {
        message: "{{context.data.missing|default_a|default_b|default_c}}",
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: {} },
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);
    const s1 = outputs.find((o) => o.block_id === "s1");
    assert.ok(s1, "s1 output should exist");
    assert.equal(
      (s1.output as { message: string }).message,
      "default_a|default_b|default_c",
      "extra `|` segments should be treated as part of the fallback literal (single-fallback semantics)",
    );
  });
});
