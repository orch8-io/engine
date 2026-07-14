import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";

import { Orch8Client, step, testSequence } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Typed Dataflow", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("generates deterministic matching TypeScript and Python bindings", async () => {
    const sequence = testSequence(
      "typed-bindings",
      [
        step(
          "source",
          "noop",
          { order_id: "{{ data.order_id }}" },
          {
            output_schema: {
              type: "object",
              properties: {
                receipt_id: { type: "string" },
                score: { type: ["number", "null"] },
              },
              required: ["receipt_id"],
              additionalProperties: false,
            },
          },
        ),
        step("consumer", "noop", {
          receipt_id: "{{ outputs.source.receipt_id }}",
        }),
      ],
    );
    sequence.input_schema = {
      type: "object",
      properties: { order_id: { type: "string" } },
      required: ["order_id"],
      additionalProperties: false,
    };

    const first = await client.compileSequenceDataflow(sequence);
    const second = await client.compileSequenceDataflow(sequence);
    assert.deepEqual(first, second);
    assert.equal(first.report.references_checked, 2);
    assert.deepEqual(first.report.findings, []);
    assert.match(first.generated.sequence_sha256, /^[0-9a-f]{64}$/);
    assert.match(first.generated.typescript, /"receipt_id": string/);
    assert.match(first.generated.python, /"receipt_id": Required\[str\]/);

    const stored = await client.createSequence(sequence);
    assert.deepEqual(
      (await client.getSequenceDataflow(stored.id)).generated,
      first.generated,
    );
  });

  it("fails preflight with the exact producer-reference-consumer chain", async () => {
    const sequence = testSequence("typed-invalid", [
      step("consumer", "noop", {
        value: "{{ outputs.missing.id }}",
      }),
    ]);
    const compiled = await client.compileSequenceDataflow(sequence);
    assert.equal(compiled.report.findings[0].code, "MISSING_PRODUCER");
    assert.equal(compiled.report.findings[0].consumer, "consumer");
    assert.equal(compiled.report.findings[0].reference, "outputs.missing.id");

    const stored = await client.createSequence(sequence);
    const preflight = await client.getSequencePreflight(stored.id);
    const check = preflight.checks.find(
      (candidate: any) => candidate.id === "typed_dataflow_compatible",
    );
    assert.equal(check.status, "fail");
    assert.match(check.findings[0].summary, /outputs\.missing\.id/);
    assert.match(check.findings[0].summary, /consumer 'consumer'/);
    assert.equal(preflight.overall, "fail");
  });
});
