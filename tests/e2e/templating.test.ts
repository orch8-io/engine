import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "./client.ts";
import { startServer, stopServer } from "./harness.ts";
import type { ServerHandle } from "./harness.ts";

const client = new Orch8Client();

describe("Step param templating", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("resolves {{context.data.X}} from context into step params", async () => {
    // The log builtin returns { message } verbatim — perfect for asserting
    // the resolved param value round-trips to the output.
    const seq = testSequence("tpl-context-data", [
      step("s1", "log", { message: "{{context.data.greeting}}" }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { greeting: "hello world" } },
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);
    const s1 = outputs.find((o) => o.block_id === "s1");
    assert.equal((s1!.output as any).message, "hello world");
  });

  it("resolves {{outputs.STEP_ID.field}} from a previous step's output", async () => {
    const seq = testSequence("tpl-output-ref", [
      step("first", "log", { message: "produced-value" }),
      step("second", "log", { message: "{{outputs.first.message}}" }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);
    const second = outputs.find((o) => o.block_id === "second");
    assert.equal((second!.output as any).message, "produced-value");
  });

  it("uses fallback default when path is missing: {{missing.path|default}}", async () => {
    const seq = testSequence("tpl-default", [
      step("s1", "log", { message: "{{context.data.missing|fallback_val}}" }),
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
    assert.equal((s1!.output as any).message, "fallback_val");
  });

  it("composes inline templates within a string", async () => {
    const seq = testSequence("tpl-inline", [
      step("s1", "log", {
        message: "Hello {{context.data.name}}, count {{context.data.n}}",
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { name: "Alice", n: 42 } },
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);
    const s1 = outputs.find((o) => o.block_id === "s1");
    assert.equal((s1!.output as any).message, "Hello Alice, count 42");
  });
});
