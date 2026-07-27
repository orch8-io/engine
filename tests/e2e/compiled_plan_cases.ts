import assert from "node:assert/strict";

import { Orch8Client, step, testSequence } from "./client.ts";

const client = new Orch8Client();

interface DataflowFinding {
  code: string;
  severity: "warning" | "error";
  consumer: string;
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
  assert.ok(
    finding,
    `missing finding for ${reference}; got ${JSON.stringify(result.report.findings)}`,
  );
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

  assert.equal(
    result.report.references_checked,
    1,
    `${reference}: references_checked`,
  );
  assert.deepEqual(
    result.report.findings,
    [],
    `${reference}: declared ${schemaType} input must compile clean`,
  );
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

  assert.equal(
    result.report.references_checked,
    1,
    `${reference}: references_checked`,
  );
  assert.equal(
    result.report.findings.length,
    1,
    `${reference}: exactly one finding expected, got ${JSON.stringify(result.report.findings)}`,
  );
  const finding = findingByReference(result, reference);
  assert.equal(finding.code, "SCHEMA_PATH_MISSING", reference);
  assert.equal(finding.severity, "error", reference);
  assert.equal(finding.consumer, "consumer", reference);
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

  assert.equal(
    result.report.references_checked,
    1,
    `${reference}: references_checked`,
  );
  assert.equal(
    result.report.findings.length,
    1,
    `${reference}: exactly one finding expected, got ${JSON.stringify(result.report.findings)}`,
  );
  const finding = findingByReference(result, reference);
  assert.equal(finding.code, "MISSING_PRODUCER", reference);
  assert.equal(finding.severity, "error", reference);
  assert.equal(finding.consumer, "consumer", reference);
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

  assert.equal(
    result.report.references_checked,
    1,
    `${reference}: references_checked`,
  );
  assert.deepEqual(
    result.report.findings,
    [],
    `${reference}: declared ${schemaType} output must compile clean`,
  );
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

  assert.equal(
    result.report.references_checked,
    1,
    `${reference}: references_checked`,
  );
  assert.equal(
    result.report.findings.length,
    1,
    `${reference}: exactly one finding expected, got ${JSON.stringify(result.report.findings)}`,
  );
  const finding = findingByReference(result, reference);
  assert.equal(finding.code, "TYPE_UNKNOWN", reference);
  // Unprovable roots are warnings, not errors — the plan still compiles.
  assert.equal(finding.severity, "warning", reference);
  assert.equal(finding.consumer, "consumer", reference);
}
