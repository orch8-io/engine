/**
 * Type preservation vs. string coercion when resolving {{...}} expressions.
 *
 * Engine rule (orch8-engine/src/template.rs :: resolve_string):
 *   - When the ENTIRE value is a single {{expr}} (one template, no surrounding
 *     text), the resolved JSON value is returned VERBATIM — numbers stay
 *     numbers, booleans stay booleans, objects stay objects.
 *   - When the template is inlined inside a larger string ("hello {{x}}"),
 *     the resolved value is stringified (via Value::to_string) — numbers
 *     and booleans become "42", "true", etc.
 *
 * These tests use an external worker handler so we can observe the resolved
 * params verbatim (built-in `log` coerces to string). The worker task
 * payload reflects what the template engine produced BEFORE the handler
 * touches it.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Templating — non-string values", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("preserves number/boolean types for whole-string {{expr}} params", async () => {
    const seq = testSequence("tpl-types-preserve", [
      step("s1", "inspect_params_types_a", {
        count: "{{context.data.n}}",
        active: "{{context.data.flag}}",
        ratio: "{{context.data.r}}",
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { n: 42, flag: true, r: 2.5 } },
    });

    await client.waitForState(id, "waiting");
    const tasks = await client.pollWorkerTasks(
      "inspect_params_types_a",
      "worker-types-a",
    );
    assert.equal(tasks.length, 1, "one worker task should be dispatched");
    const params = tasks[0]!.params;

    // Note: current behavior — types preserved when the whole param is a
    // single {{expr}}. 42 stays number 42, true stays boolean true.
    assert.equal(typeof params.count, "number");
    assert.equal(params.count, 42);
    assert.equal(typeof params.active, "boolean");
    assert.equal(params.active, true);
    assert.equal(typeof params.ratio, "number");
    assert.equal(params.ratio, 2.5);

    await client.completeWorkerTask(tasks[0]!.id, "worker-types-a", {
      ok: true,
    });
    await client.waitForState(id, "completed");
  });

  it("coerces non-string values to strings when inlined in a larger string", async () => {
    const seq = testSequence("tpl-types-coerce", [
      step("s1", "inspect_params_types_b", {
        msg: "count={{context.data.n}} active={{context.data.flag}}",
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { n: 42, flag: true } },
    });

    await client.waitForState(id, "waiting");
    const tasks = await client.pollWorkerTasks(
      "inspect_params_types_b",
      "worker-types-b",
    );
    assert.equal(tasks.length, 1);
    const params = tasks[0]!.params;

    // Note: current behavior — inline templates always produce a string,
    // so numbers and booleans are coerced via Display ("42", "true").
    assert.equal(typeof params.msg, "string");
    assert.equal(params.msg, "count=42 active=true");

    await client.completeWorkerTask(tasks[0]!.id, "worker-types-b", {
      ok: true,
    });
    await client.waitForState(id, "completed");
  });

  it("preserves object values for whole-string {{expr}} params", async () => {
    const seq = testSequence("tpl-types-obj", [
      step("s1", "inspect_params_types_c", {
        user: "{{context.data.user}}",
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { user: { name: "Alice", age: 30 } } },
    });

    await client.waitForState(id, "waiting");
    const tasks = await client.pollWorkerTasks(
      "inspect_params_types_c",
      "worker-types-c",
    );
    assert.equal(tasks.length, 1);
    const params = tasks[0]!.params;

    // Note: current behavior — an entire object value is returned intact
    // when the template is the entire string. Consumers get a real object,
    // not a stringified JSON blob.
    assert.deepEqual(params.user, { name: "Alice", age: 30 });

    await client.completeWorkerTask(tasks[0]!.id, "worker-types-c", {
      ok: true,
    });
    await client.waitForState(id, "completed");
  });
});
