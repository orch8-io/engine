/**
 * ForEach iteration variable visibility in body templates.
 *
 * The ForEach handler (orch8-engine/src/handlers/for_each.rs) binds the
 * current iteration element to `context.data.<item_var>` (default "item")
 * via `bind_item_var` before activating body children. Body steps can
 * reference this via `{{context.data.item}}` (or whatever `item_var` is
 * set to). The template resolver only recognises `context.*` and
 * `outputs.*` roots — bare `{{item}}` would be an error.
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

  it("body can reference {{item}} for current iteration value", async () => {
    // The ForEach handler binds `item_var` (default "item") into
    // `context.data.item`, so the correct template path is
    // `{{context.data.item}}`.
    const seq = testSequence("tpl-foreach-item", [
      forEach("fe", "items", [
        step("body", "log", { message: "item: {{context.data.item}}" }),
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
