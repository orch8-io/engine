/**
 * Array index access in templating.
 *
 * From orch8-engine/src/template.rs :: navigate_json() — each path segment
 * is parsed as a usize when the current node is an Array. So the supported
 * syntax is DOT-INDEXED: {{context.data.items.0}}.
 *
 * Bracket syntax {{items[0]}} is NOT supported: the whole path is split on
 * `.` only, so "items[0]" would be treated as a single segment lookup
 * (which will not match).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Templating — array access", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("resolves via dot-indexed syntax: {{context.data.items.N}}", async () => {
    const seq = testSequence("tpl-arr-dot", [
      step("s0", "log", { message: "{{context.data.items.0}}" }),
      step("s1", "log", { message: "{{context.data.items.1}}" }),
      step("s2", "log", { message: "{{context.data.items.2}}" }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { items: ["alpha", "beta", "gamma"] } },
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);

    const byId = new Map(outputs.map((o) => [o.block_id, o]));
    assert.equal(
      (byId.get("s0")?.output as { message: string } | undefined)?.message,
      "alpha",
    );
    assert.equal(
      (byId.get("s1")?.output as { message: string } | undefined)?.message,
      "beta",
    );
    assert.equal(
      (byId.get("s2")?.output as { message: string } | undefined)?.message,
      "gamma",
    );
  });

  it("does NOT support bracket-index syntax: {{items[0]}} is unresolved", async () => {
    // When the path contains "items[0]" the splitter on "." produces the
    // segments ["context", "data", "items[0]"]; the last segment is looked
    // up on the object as a key named "items[0]" and finds nothing, so the
    // result is JSON null. When substituted into an inline template string,
    // `Value::Null.to_string()` serialises to the literal "null".
    const seq = testSequence("tpl-arr-bracket", [
      step("s1", "log", {
        message: "got=[{{context.data.items[0]}}]",
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { items: ["alpha", "beta"] } },
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);
    const s1 = outputs.find((o) => o.block_id === "s1");
    assert.ok(s1, "s1 output exists");
    const msg = (s1.output as { message: string }).message;
    // Note: current behavior is that bracket syntax resolves to JSON null,
    // which renders as "null" when inlined.
    assert.equal(msg, "got=[null]");
  });
});
