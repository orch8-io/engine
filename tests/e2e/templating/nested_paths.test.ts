/**
 * Deep-path resolution via dotted paths like
 * {{context.data.user.profile.name}} and {{context.data.a.b.c.d}}.
 *
 * The engine uses a custom mini-template (orch8-engine/src/template.rs),
 * NOT Tera/handlebars/minijinja. Paths are split on `.` and walked via
 * object `get` / array index parsing (see navigate_json).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Templating — nested paths", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("resolves a 3-level deep context.data path", async () => {
    const seq = testSequence("tpl-nested-3", [
      step("s1", "log", {
        message: "{{context.data.user.profile.name}}",
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: {
        data: { user: { profile: { name: "Alice" } } },
      },
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);
    const s1 = outputs.find((o) => o.block_id === "s1");
    assert.ok(s1, "s1 output exists");
    assert.equal((s1.output as { message: string }).message, "Alice");
  });

  it("resolves a 4-level deep path: {{context.data.a.b.c.d}}", async () => {
    const seq = testSequence("tpl-nested-4", [
      step("s1", "log", {
        message: "{{context.data.a.b.c.d}}",
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: {
        data: { a: { b: { c: { d: "deep-value" } } } },
      },
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);
    const s1 = outputs.find((o) => o.block_id === "s1");
    assert.ok(s1, "s1 output exists");
    assert.equal((s1.output as { message: string }).message, "deep-value");
  });

  it("composes two deep paths inline in a single string", async () => {
    const seq = testSequence("tpl-nested-inline", [
      step("s1", "log", {
        message:
          "user={{context.data.user.profile.name}} role={{context.data.user.profile.role}}",
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: {
        data: { user: { profile: { name: "Bob", role: "admin" } } },
      },
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);
    const s1 = outputs.find((o) => o.block_id === "s1");
    assert.ok(s1, "s1 output exists");
    assert.equal(
      (s1.output as { message: string }).message,
      "user=Bob role=admin",
    );
  });
});
