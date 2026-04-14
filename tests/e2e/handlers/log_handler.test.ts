/**
 * log handler — records message in step output.
 *
 * Per `orch8-engine/src/handlers/builtin.rs::handle_log`, the handler
 * logs the `message` param at the configured level and echoes the
 * value back unchanged in the step output as `{ "message": <value> }`.
 * For absent `message` the placeholder string `"no message"` is used.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("log Handler", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("echoes the configured message into the step output", async () => {
    const tenantId = `log-${uuid().slice(0, 8)}`;
    const namespace = "default";

    const message = `hello world ${uuid().slice(0, 6)}`;
    const seq = testSequence(
      "log-echo",
      [step("say", "log", { message, level: "info" })],
      { tenantId, namespace },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace,
    });

    const final = await client.waitForState(id, ["completed", "failed"], {
      timeoutMs: 10_000,
    });
    assert.equal(final.state, "completed");

    const outputs = await client.getOutputs(id);
    const sayOut = outputs.find((o) => o.block_id === "say");
    assert.ok(sayOut, "expected log block output");
    assert.deepEqual(sayOut.output, { message });
  });

  it("falls back to the 'no message' placeholder when message is absent", async () => {
    const tenantId = `log-default-${uuid().slice(0, 8)}`;
    const namespace = "default";

    const seq = testSequence(
      "log-default",
      [step("silent", "log", {})],
      { tenantId, namespace },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace,
    });

    const final = await client.waitForState(id, ["completed", "failed"], {
      timeoutMs: 10_000,
    });
    assert.equal(final.state, "completed");

    const outputs = await client.getOutputs(id);
    const out = outputs.find((o) => o.block_id === "silent");
    assert.ok(out);
    assert.deepEqual(out.output, { message: "no message" });
  });
});
