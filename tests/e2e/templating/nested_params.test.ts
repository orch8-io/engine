/**
 * Nested-structure templating: the resolver (orch8-engine/src/template.rs
 * :: resolve) recurses through JSON objects AND arrays, resolving any
 * string values it encounters. Non-strings pass through unchanged.
 *
 * That means templates work at ARBITRARY depth inside params — nested
 * objects, arrays, mixed structures — not just top-level string fields.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Templating — nested params structures", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("resolves templates inside nested objects and arrays", async () => {
    const seq = testSequence("tpl-nested-params", [
      step("s1", "inspect_nested_params", {
        message: "static",
        nested: {
          greet: "hello {{context.data.name}}",
          arr: ["item1", "{{context.data.x}}", "item3"],
          meta: {
            deep: "val={{context.data.x}}",
          },
        },
        list: [
          { k: "first", v: "{{context.data.x}}" },
          { k: "second", v: "{{context.data.name}}" },
        ],
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { name: "Carol", x: "XVAL" } },
    });

    await client.waitForState(id, "waiting");
    const tasks = await client.pollWorkerTasks(
      "inspect_nested_params",
      "worker-nested",
    );
    assert.equal(tasks.length, 1);
    const p = tasks[0]!.params as {
      message: string;
      nested: {
        greet: string;
        arr: unknown[];
        meta: { deep: string };
      };
      list: { k: string; v: string }[];
    };

    assert.equal(p.message, "static");
    assert.equal(p.nested.greet, "hello Carol");
    assert.deepEqual(p.nested.arr, ["item1", "XVAL", "item3"]);
    assert.equal(p.nested.meta.deep, "val=XVAL");
    assert.equal(p.list[0]?.v, "XVAL");
    assert.equal(p.list[1]?.v, "Carol");

    await client.completeWorkerTask(tasks[0]!.id, "worker-nested", {
      ok: true,
    });
    await client.waitForState(id, "completed");
  });
});
