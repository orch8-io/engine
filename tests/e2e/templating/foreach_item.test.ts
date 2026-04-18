/**
 * ForEach iteration variable visibility in body templates.
 *
 * ForEachDef (orch8-types/src/sequence.rs) exposes `item_var` with default
 * "item". HOWEVER: inspecting orch8-engine/src/handlers/for_each.rs shows
 * the handler only activates body children and waits for them — it does
 * NOT inject the current item into the instance context or into the
 * `outputs.*` shape. The template resolver
 * (orch8-engine/src/template.rs :: resolve_path) only recognises the
 * roots `context` and `outputs` (anything else is a TemplateError, NOT
 * a silent empty value).
 *
 * Consequence: `{{item}}`, `{{context.item}}`, and `{{item_var}}` all
 * either error or fail to resolve. The iteration variable is not exposed
 * to the templating scope. Hence these tests are skipped with a note.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

function forEach(
  id: string,
  collection: string,
  body: Block[],
): Block {
  return {
    type: "for_each",
    id,
    collection,
    body,
  } as Block;
}

describe("Templating — forEach item variable", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // Skipped: ForEach handler in orch8-engine does not inject the iteration
  // item into the instance's ExecutionContext or outputs map. The template
  // engine only recognises `context.*` and `outputs.*` roots, so there is
  // no {{item}} binding to reference from the body. Re-enable this test
  // when the engine gains item-binding (e.g., writing
  // context.runtime.<item_var> or similar).
  it.skip("body can reference {{item}} for current iteration value", async () => {
    const seq = testSequence("tpl-foreach-item", [
      forEach("fe", "items", [
        step("body", "log", { message: "item: {{item}}" }),
      ]),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { items: ["a", "b", "c"] } },
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);
    const messages = outputs
      .filter((o) => o.block_id === "body")
      .map((o) => (o.output as { message: string }).message);
    assert.deepEqual(messages.sort(), ["item: a", "item: b", "item: c"]);
  });
});
