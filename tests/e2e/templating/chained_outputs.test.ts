/**
 * Verifies that templates can chain through multiple prior step outputs.
 *
 * orch8-engine/src/handlers/param_resolve.rs :: build_outputs_shape
 * fetches ALL prior block outputs and exposes them to the template
 * resolver under the `outputs.*` root, so step C can read step B's output
 * which itself was derived from step A's output.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Templating — chained output references", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("A->B->C each referencing the previous step's output", async () => {
    const seq = testSequence("tpl-chain", [
      step("A", "log", { message: "A-value" }),
      step("B", "log", { message: "{{outputs.A.message}}-then-B" }),
      step("C", "log", { message: "{{outputs.B.message}}-then-C" }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "completed");
    const outputs = await client.getOutputs(id);

    const byId = new Map(outputs.map((o) => [o.block_id, o]));
    assert.equal(
      (byId.get("A")?.output as { message: string } | undefined)?.message,
      "A-value",
    );
    assert.equal(
      (byId.get("B")?.output as { message: string } | undefined)?.message,
      "A-value-then-B",
    );
    assert.equal(
      (byId.get("C")?.output as { message: string } | undefined)?.message,
      "A-value-then-B-then-C",
    );
  });
});
