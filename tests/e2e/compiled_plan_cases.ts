import assert from "node:assert/strict";

import { Orch8Client, step, testSequence } from "./client.ts";

const client = new Orch8Client();

interface DataflowFinding {
  code: string;
  reference: string;
}

interface DataflowResult {
  report: {
    references_checked: number;
    findings: DataflowFinding[];
  };
}

function findingByReference(
  result: DataflowResult,
  reference: string,
): DataflowFinding {
  const finding = result.report.findings.find(
    (candidate) => candidate.reference === reference,
  );
  assert.ok(finding, `missing finding for ${reference}`);
  return finding;
}

export async function expectDeclaredInput(
  caseName: string,
  field: string,
  schemaType: string,
): Promise<void> {
  const reference = `data.${field}`;
  const sequence = testSequence(caseName, [
    step("consumer", "noop", { value: `{{ ${reference} }}` }),
  ]);
  sequence.input_schema = {
    type: "object",
    properties: { [field]: { type: schemaType } },
    required: [field],
    additionalProperties: false,
  };

  const result = (await client.compileSequenceDataflow(
    sequence,
  )) as DataflowResult;

  assert.equal(result.report.references_checked, 1);
  assert.deepEqual(result.report.findings, []);
}

export async function expectMissingInput(
  caseName: string,
  field: string,
): Promise<void> {
  const reference = `data.${field}`;
  const sequence = testSequence(caseName, [
    step("consumer", "noop", { value: `{{ ${reference} }}` }),
  ]);
  sequence.input_schema = {
    type: "object",
    properties: { declared: { type: "string" } },
    required: ["declared"],
    additionalProperties: false,
  };

  const result = (await client.compileSequenceDataflow(
    sequence,
  )) as DataflowResult;

  assert.equal(result.report.references_checked, 1);
  assert.equal(findingByReference(result, reference).code, "SCHEMA_PATH_MISSING");
}

export async function expectMissingProducer(
  caseName: string,
  producerId: string,
): Promise<void> {
  const reference = `outputs.${producerId}.value`;
  const sequence = testSequence(caseName, [
    step("consumer", "noop", { value: `{{ ${reference} }}` }),
  ]);

  const result = (await client.compileSequenceDataflow(
    sequence,
  )) as DataflowResult;

  assert.equal(result.report.references_checked, 1);
  assert.equal(findingByReference(result, reference).code, "MISSING_PRODUCER");
}

export async function expectDeclaredOutput(
  caseName: string,
  field: string,
  schemaType: string,
): Promise<void> {
  const reference = `outputs.source.${field}`;
  const sequence = testSequence(caseName, [
    step(
      "source",
      "noop",
      {},
      {
        output_schema: {
          type: "object",
          properties: { [field]: { type: schemaType } },
          required: [field],
          additionalProperties: false,
        },
      },
    ),
    step("consumer", "noop", { value: `{{ ${reference} }}` }),
  ]);

  const result = (await client.compileSequenceDataflow(
    sequence,
  )) as DataflowResult;

  assert.equal(result.report.references_checked, 1);
  assert.deepEqual(result.report.findings, []);
}

export async function expectUnknownRoot(
  caseName: string,
  root: "state" | "config",
  field: string,
): Promise<void> {
  const reference = `${root}.${field}`;
  const sequence = testSequence(caseName, [
    step("consumer", "noop", { value: `{{ ${reference} }}` }),
  ]);

  const result = (await client.compileSequenceDataflow(
    sequence,
  )) as DataflowResult;

  assert.equal(result.report.references_checked, 1);
  assert.equal(findingByReference(result, reference).code, "TYPE_UNKNOWN");
}
