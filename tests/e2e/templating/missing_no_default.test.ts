/**
 * Behaviour of {{path}} when the path is missing AND no `|default` is
 * supplied.
 *
 * orch8-engine/src/template.rs :: resolve_path:
 *   - Missing or null value with no fallback → returns JSON Null.
 *   - When the entire string is a single {{expr}}, the step param becomes
 *     JSON null (NOT an error, NOT a literal "{{...}}").
 *   - When the expression is inlined in a larger string, the null is
 *     stringified via Value::to_string → literal "null".
 *
 * Note: the engine does NOT fail the step on a missing path; it just
 * substitutes null. However, an UNKNOWN ROOT (like {{foo.bar}}) or an
 * unknown context section (like {{context.bogus.x}}) DOES error and fail
 * the step. That's asserted in a separate test below.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Templating — missing path without |default", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("whole-string {{missing}} resolves to JSON null (step still completes)", async () => {
    const seq = testSequence("tpl-missing-whole", [
      step("s1", "inspect_missing_whole", {
        val: "{{context.data.missing}}",
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: {} },
    });

    await client.waitForState(id, "waiting");
    const tasks = await client.pollWorkerTasks(
      "inspect_missing_whole",
      "worker-missing-a",
    );
    assert.equal(tasks.length, 1);
    const params = tasks[0]!.params as Record<string, unknown>;

    // Note: current behavior — the param is JSON null.
    assert.equal(params.val, null);

    await client.completeWorkerTask(tasks[0]!.id, "worker-missing-a", {});
    await client.waitForState(id, "completed");
  });

  it("inline {{missing}} in a larger string stringifies to 'null'", async () => {
    const seq = testSequence("tpl-missing-inline", [
      step("s1", "log", {
        message: "value=[{{context.data.missing}}]",
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
    assert.ok(s1, "s1 output exists");
    // Note: current behavior — missing value renders as literal "null"
    // when inlined inside a larger string.
    assert.equal((s1.output as { message: string }).message, "value=[null]");
  });

  it("unknown template root errors out and fails the step", async () => {
    const seq = testSequence("tpl-missing-unknown-root", [
      step("s1", "log", { message: "{{foo.bar}}" }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Note: current behavior — unknown roots (anything other than
    // `context` or `outputs`) produce an EngineError::TemplateError that
    // fails the node, propagating the instance to a failed state.
    const final = await client.waitForState(id, ["completed", "failed"], {
      timeoutMs: 10_000,
    });
    assert.equal(final.state, "failed");
  });
});
