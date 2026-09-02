/**
 * Shared case helpers for the `plansem_*` suites — e2e coverage for
 * `feat(engine): compile semantics-safe workflow plans` (09fba97).
 *
 * The optimizer IR itself is engine-internal; the observable compile surface
 * is the dataflow/compile API (`POST /sequences/dataflow` for drafts,
 * `GET /sequences/{id}/dataflow` for stored definitions) plus the runtime
 * dispatch decisions the compiled plan drives (composite/plugin detection
 * and the injected-roots fallback in the scheduler hot path).
 *
 * The previous wave (`compiled_plan_*`) covered flat top-level steps only:
 * input types/failures, output types, producer failures, unknown roots.
 * This wave covers the next layer: composite block compilation, template
 * references through nested blocks, reference dedup, deterministic
 * canonical hashing (equivalence/tamper evidence), nesting budgets,
 * multi-root sequences, and plugin handler scheme dispatch.
 */

import assert from "node:assert/strict";

import { ApiError, Orch8Client, step, testSequence, uuid } from "./client.ts";
import type { Block, SequenceDef } from "./client.ts";

const client = new Orch8Client();

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

export interface DataflowFinding {
  code: string;
  severity: "warning" | "error";
  consumer: string;
  reference: string;
  summary: string;
}

export interface CompiledPlan {
  report: {
    findings: DataflowFinding[];
    references_checked: number;
  };
  generated: {
    generator_version: string;
    sequence_sha256: string;
    schema: {
      generator_version: string;
      sequence: {
        id: string;
        name: string;
        version: number;
        sha256: string;
      };
      input: unknown;
      outputs: Record<string, unknown>;
    };
    typescript: string;
    python: string;
    swift: string;
    kotlin: string;
  };
}

// ---------------------------------------------------------------------------
// Block builders (composite definitions)
// ---------------------------------------------------------------------------

export function parallelBlock(id: string, branches: Block[][]): Block {
  return { type: "parallel", id, branches };
}

export function raceBlock(id: string, branches: Block[][], semantics?: string): Block {
  const block: Record<string, unknown> = { type: "race", id, branches };
  if (semantics) block.semantics = semantics;
  return block as Block;
}

export function routerBlock(
  id: string,
  routes: Array<{ condition: string; blocks: Block[] }>,
  defaultBlocks?: Block[],
): Block {
  const block: Record<string, unknown> = { type: "router", id, routes };
  if (defaultBlocks) block.default = defaultBlocks;
  return block as Block;
}

export function tryCatchBlock(
  id: string,
  tryBlocks: Block[],
  catchBlocks: Block[],
  finallyBlocks?: Block[],
): Block {
  const block: Record<string, unknown> = {
    type: "try_catch",
    id,
    try_block: tryBlocks,
    catch_block: catchBlocks,
  };
  if (finallyBlocks) block.finally_block = finallyBlocks;
  return block as Block;
}

export function loopBlock(
  id: string,
  condition: string,
  body: Block[],
  opts: Record<string, unknown> = {},
): Block {
  return { type: "loop", id, condition, body, ...opts } as Block;
}

export function forEachBlock(
  id: string,
  collection: string,
  body: Block[],
  opts: Record<string, unknown> = {},
): Block {
  return { type: "for_each", id, collection, body, ...opts } as Block;
}

export function subSequenceBlock(
  id: string,
  sequenceName: string,
  input: Record<string, unknown> = {},
): Block {
  return { type: "sub_sequence", id, sequence_name: sequenceName, input } as Block;
}

export function abSplitBlock(
  id: string,
  variants: Array<{ name: string; weight: number; blocks: Block[] }>,
): Block {
  return { type: "a_b_split", id, variants } as Block;
}

export function cancellationScopeBlock(id: string, blocks: Block[]): Block {
  return { type: "cancellation_scope", id, blocks } as Block;
}

export function sagaBlock(
  id: string,
  steps: Array<{ id: string; action: Block; compensation?: Block }>,
): Block {
  return { type: "saga", id, steps } as Block;
}

// ---------------------------------------------------------------------------
// Schema / step helpers
// ---------------------------------------------------------------------------

/** Closed object schema: every listed field required, nothing else allowed. */
export function closedObjectSchema(
  fields: Record<string, string>,
): Record<string, unknown> {
  return {
    type: "object",
    properties: Object.fromEntries(
      Object.entries(fields).map(([name, kind]) => [name, { type: kind }]),
    ),
    required: Object.keys(fields),
    additionalProperties: false,
  };
}

/** A step whose output schema is a closed object over `fields`. */
export function producerStep(
  id: string,
  fields: Record<string, string>,
  params: Record<string, unknown> = {},
): Block {
  return step(id, "noop", params, { output_schema: closedObjectSchema(fields) });
}

/** Closed input schema; all fields required unless `required` is given. */
export function inputSchema(
  fields: Record<string, string>,
  required?: string[],
): Record<string, unknown> {
  return {
    type: "object",
    properties: Object.fromEntries(
      Object.entries(fields).map(([name, kind]) => [name, { type: kind }]),
    ),
    required: required ?? Object.keys(fields),
    additionalProperties: false,
  };
}

// ---------------------------------------------------------------------------
// Compile helpers
// ---------------------------------------------------------------------------

export async function compileDraft(seq: SequenceDef): Promise<CompiledPlan> {
  return (await client.compileSequenceDataflow(seq)) as CompiledPlan;
}

export async function compileStored(id: string): Promise<CompiledPlan> {
  return (await client.getSequenceDataflow(id)) as CompiledPlan;
}

/** Compile the same draft twice and require byte-identical results. */
export async function compileDraftTwice(seq: SequenceDef): Promise<CompiledPlan> {
  const first = await compileDraft(seq);
  const second = await compileDraft(seq);
  assert.deepEqual(second, first, "draft compile must be deterministic");
  return first;
}

export function assertClean(plan: CompiledPlan, referencesChecked: number): void {
  assert.equal(
    plan.report.references_checked,
    referencesChecked,
    `references_checked mismatch; findings: ${JSON.stringify(plan.report.findings)}`,
  );
  assert.deepEqual(
    plan.report.findings,
    [],
    "expected no findings for a fully typed plan",
  );
}

export function singleFinding(plan: CompiledPlan): DataflowFinding {
  assert.equal(
    plan.report.findings.length,
    1,
    `expected exactly one finding, got ${JSON.stringify(plan.report.findings)}`,
  );
  const finding = plan.report.findings[0];
  assert.ok(finding, "finding vanished after length check");
  return finding;
}

/** Deep copy with every object's key insertion order reversed. */
export function reversedKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(reversedKeys);
  if (value !== null && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>).reverse();
    return Object.fromEntries(entries.map(([key, v]) => [key, reversedKeys(v)]));
  }
  return value;
}

/** Structured clone via JSON (definitions are plain JSON). */
export function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

// ---------------------------------------------------------------------------
// Composite block compilation cases
// ---------------------------------------------------------------------------

/** parallel: producers in branch:0 and branch:1 both resolve downstream. */
export async function caseParallelBranchProducersResolve(
  caseName: string,
): Promise<void> {
  const seq = testSequence(caseName, [
    parallelBlock("par", [
      [producerStep("p_a", { value: "string" })],
      [producerStep("p_b", { count: "integer" })],
    ]),
    step("use", "noop", {
      a: "{{ outputs.p_a.value }}",
      b: "{{ outputs.p_b.count }}",
    }),
  ]);
  assertClean(await compileDraftTwice(seq), 2);
}

/** parallel: a reference to a non-existent producer inside a branch is an error. */
export async function caseParallelBranchMissingProducer(
  caseName: string,
): Promise<void> {
  const seq = testSequence(caseName, [
    parallelBlock("par", [
      [producerStep("p_a", { value: "string" })],
      [step("in_branch", "noop", { x: "{{ outputs.ghost.value }}" })],
    ]),
  ]);
  const plan = await compileDraftTwice(seq);
  assert.equal(plan.report.references_checked, 1);
  const finding = singleFinding(plan);
  assert.equal(finding.code, "MISSING_PRODUCER");
  assert.equal(finding.severity, "error");
  assert.equal(finding.consumer, "in_branch");
  assert.equal(finding.reference, "outputs.ghost.value");
}

/** race: a producer inside a race branch resolves for a later consumer. */
export async function caseRaceBranchProducerResolves(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    raceBlock(
      "rc",
      [[producerStep("r_fast", { result: "string" })], [step("r_slow", "noop")]],
      "first_to_succeed",
    ),
    step("use", "noop", { x: "{{ outputs.r_fast.result }}" }),
  ]);
  assertClean(await compileDraftTwice(seq), 1);
}

/** loop: body producer resolves and the loop condition reference is checked. */
export async function caseLoopBodyProducerAndConditionChecked(
  caseName: string,
): Promise<void> {
  const seq = testSequence(caseName, [
    loopBlock("lp", "data.keep_going", [producerStep("lp_src", { tick: "integer" })]),
    step("use", "noop", { x: "{{ outputs.lp_src.tick }}" }),
  ]);
  seq.input_schema = inputSchema({ keep_going: "boolean" });
  assertClean(await compileDraftTwice(seq), 2);
}

/** loop: a condition over undeclared input fails closed on the loop block. */
export async function caseLoopConditionUndeclaredInput(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    loopBlock("lp", "data.missing_flag", [step("body_step", "noop")]),
  ]);
  seq.input_schema = inputSchema({ declared: "string" });
  const plan = await compileDraftTwice(seq);
  assert.equal(plan.report.references_checked, 1);
  const finding = singleFinding(plan);
  assert.equal(finding.code, "SCHEMA_PATH_MISSING");
  assert.equal(finding.consumer, "lp");
  assert.equal(finding.reference, "data.missing_flag");
}

/** loop: break_on expression references are checked against producer schemas. */
export async function caseLoopBreakOnReferenceChecked(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    loopBlock("lp", "true", [producerStep("lp_src", { tick: "integer" })], {
      break_on: "outputs.lp_src.tick > 5",
    }),
  ]);
  assertClean(await compileDraftTwice(seq), 1);
}

/** for_each: collection expression resolves against the input schema. */
export async function caseForEachCollectionDeclared(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    forEachBlock("fe", "{{ data.items }}", [producerStep("fe_src", { ok: "boolean" })]),
    step("use", "noop", { x: "{{ outputs.fe_src.ok }}" }),
  ]);
  seq.input_schema = inputSchema({ items: "array" });
  assertClean(await compileDraftTwice(seq), 2);
}

/** for_each: a collection over undeclared input fails closed on the block. */
export async function caseForEachCollectionUndeclared(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    forEachBlock("fe", "{{ data.missing_items }}", [step("body_step", "noop")]),
  ]);
  seq.input_schema = inputSchema({ declared: "string" });
  const plan = await compileDraftTwice(seq);
  assert.equal(plan.report.references_checked, 1);
  const finding = singleFinding(plan);
  assert.equal(finding.code, "SCHEMA_PATH_MISSING");
  assert.equal(finding.consumer, "fe");
  assert.equal(finding.reference, "data.missing_items");
}

/** router: route conditions are inspected as references owned by the router. */
export async function caseRouterRouteConditionsChecked(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    producerStep("source", { ok: "boolean" }),
    routerBlock("rt", [
      {
        condition: "outputs.source.ok && state.phase == config.expected_phase",
        blocks: [],
      },
    ]),
  ]);
  const plan = await compileDraftTwice(seq);
  assert.equal(plan.report.references_checked, 3);
  // outputs.source.ok is fully typed → clean. state/config roots can never be
  // proven → warnings, sorted by reference within the same consumer+code.
  assert.equal(plan.report.findings.length, 2);
  for (const finding of plan.report.findings) {
    assert.equal(finding.code, "TYPE_UNKNOWN");
    assert.equal(finding.severity, "warning");
    assert.equal(finding.consumer, "rt");
  }
  assert.deepEqual(
    plan.report.findings.map((finding) => finding.reference),
    ["config.expected_phase", "state.phase"],
  );
}

/** router: producers in route blocks and in the default branch resolve. */
export async function caseRouterRouteAndDefaultProducersResolve(
  caseName: string,
): Promise<void> {
  const seq = testSequence(caseName, [
    routerBlock(
      "rt",
      [{ condition: "true", blocks: [producerStep("r_src", { v: "string" })] }],
      [producerStep("d_src", { w: "string" })],
    ),
    step("use", "noop", {
      a: "{{ outputs.r_src.v }}",
      b: "{{ outputs.d_src.w }}",
    }),
  ]);
  assertClean(await compileDraftTwice(seq), 2);
}

/** try_catch: producers in try, catch, and finally all resolve. */
export async function caseTryCatchFinallyProducersResolve(
  caseName: string,
): Promise<void> {
  const seq = testSequence(caseName, [
    tryCatchBlock(
      "tc",
      [producerStep("t_src", { a: "string" })],
      [producerStep("c_src", { b: "string" })],
      [producerStep("f_src", { c: "string" })],
    ),
    step("use", "noop", {
      a: "{{ outputs.t_src.a }}",
      b: "{{ outputs.c_src.b }}",
      c: "{{ outputs.f_src.c }}",
    }),
  ]);
  assertClean(await compileDraftTwice(seq), 3);
}

/** try_catch: a catch-region consumer with a wrong path fails closed. */
export async function caseTryCatchCatchConsumerWrongPath(
  caseName: string,
): Promise<void> {
  const seq = testSequence(caseName, [
    tryCatchBlock(
      "tc",
      [producerStep("t_src", { a: "string" })],
      [step("catch_use", "noop", { x: "{{ outputs.t_src.nope }}" })],
    ),
  ]);
  const plan = await compileDraftTwice(seq);
  assert.equal(plan.report.references_checked, 1);
  const finding = singleFinding(plan);
  assert.equal(finding.code, "SCHEMA_PATH_MISSING");
  assert.equal(finding.consumer, "catch_use");
  assert.equal(finding.reference, "outputs.t_src.nope");
}

/**
 * sub_sequence: its `input` is reference-checked, and the block itself is a
 * schema-less producer — references to its outputs are warnings, not errors.
 */
export async function caseSubSequenceInputCheckedAndOutputUnknown(
  caseName: string,
): Promise<void> {
  const seq = testSequence(caseName, [
    subSequenceBlock("sub", "child-seq", { user: "{{ data.user_id }}" }),
    step("after", "noop", { x: "{{ outputs.sub.result }}" }),
  ]);
  seq.input_schema = inputSchema({ user_id: "string" });
  const plan = await compileDraftTwice(seq);
  assert.equal(plan.report.references_checked, 2);
  const finding = singleFinding(plan);
  assert.equal(finding.code, "TYPE_UNKNOWN");
  assert.equal(finding.severity, "warning");
  assert.equal(finding.consumer, "after");
  assert.equal(finding.reference, "outputs.sub.result");
}

/** a_b_split: producers inside weighted variants resolve downstream. */
export async function caseAbSplitVariantProducersResolve(
  caseName: string,
): Promise<void> {
  const seq = testSequence(caseName, [
    abSplitBlock("ab", [
      { name: "control", weight: 50, blocks: [producerStep("v_control", { v: "string" })] },
      { name: "treatment", weight: 50, blocks: [producerStep("v_treatment", { w: "string" })] },
    ]),
    step("use", "noop", {
      a: "{{ outputs.v_control.v }}",
      b: "{{ outputs.v_treatment.w }}",
    }),
  ]);
  assertClean(await compileDraftTwice(seq), 2);
}

/** cancellation_scope: a producer inside the scope resolves downstream. */
export async function caseCancellationScopeProducerResolves(
  caseName: string,
): Promise<void> {
  const seq = testSequence(caseName, [
    cancellationScopeBlock("scope", [producerStep("sc_src", { token: "string" })]),
    step("use", "noop", { x: "{{ outputs.sc_src.token }}" }),
  ]);
  assertClean(await compileDraftTwice(seq), 1);
}

/** saga: action and compensation blocks are compiled; both produce outputs. */
export async function caseSagaActionAndCompensationProducersResolve(
  caseName: string,
): Promise<void> {
  const seq = testSequence(caseName, [
    sagaBlock("saga", [
      {
        id: "s1",
        action: producerStep("act_one", { tx_id: "string" }),
        compensation: producerStep("comp_one", { rolled_back: "boolean" }),
      },
      // Second step deliberately has no compensation — the plan must still
      // compile and expose the action as a producer.
      { id: "s2", action: producerStep("act_two", { receipt: "string" }) },
    ]),
    step("use", "noop", {
      a: "{{ outputs.act_one.tx_id }}",
      b: "{{ outputs.comp_one.rolled_back }}",
      c: "{{ outputs.act_two.receipt }}",
    }),
  ]);
  assertClean(await compileDraftTwice(seq), 3);
}

/** step guards: `when` expressions are reference-checked against schemas. */
export async function caseWhenGuardReferenceChecked(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    step("guarded", "noop", {}, { when: "data.enabled" }),
  ]);
  seq.input_schema = inputSchema({ enabled: "boolean" });
  assertClean(await compileDraftTwice(seq), 1);
}

// ---------------------------------------------------------------------------
// Nested-block reference resolution, dedup, and budget cases
// ---------------------------------------------------------------------------

/** parallel → try_catch → loop → step: a 4-level-deep producer resolves. */
export async function caseDeepParallelTryLoopProducer(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    parallelBlock("par", [
      [
        tryCatchBlock(
          "tc",
          [loopBlock("lp", "true", [producerStep("deep_src", { v: "string" })])],
          [step("catcher", "noop")],
        ),
      ],
      [step("other", "noop")],
    ]),
    step("use", "noop", { x: "{{ outputs.deep_src.v }}" }),
  ]);
  assertClean(await compileDraftTwice(seq), 1);
}

/** router route → for_each body: nested producer resolves across edge kinds. */
export async function caseRouterForEachNestedProducer(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    routerBlock("rt", [
      {
        condition: "true",
        blocks: [
          forEachBlock("fe", "{{ data.items }}", [
            producerStep("fe_deep", { row_id: "string" }),
          ]),
        ],
      },
    ]),
    step("use", "noop", { x: "{{ outputs.fe_deep.row_id }}" }),
  ]);
  seq.input_schema = inputSchema({ items: "array" });
  assertClean(await compileDraftTwice(seq), 2);
}

/** parallel → cancellation_scope → saga action: producer resolves at depth 3. */
export async function caseSagaInsideScopeInsideParallel(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    parallelBlock("par", [
      [
        cancellationScopeBlock("scope", [
          sagaBlock("saga", [
            {
              id: "s1",
              action: producerStep("saga_deep", { commit_id: "string" }),
              compensation: step("saga_comp", "noop"),
            },
          ]),
        ]),
      ],
    ]),
    step("use", "noop", { x: "{{ outputs.saga_deep.commit_id }}" }),
  ]);
  assertClean(await compileDraftTwice(seq), 1);
}

/** One template string carrying two references counts both. */
export async function caseMultipleReferencesInOneTemplate(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    step("use", "noop", { text: "{{ data.first_name }} {{ data.last_name }}" }),
  ]);
  seq.input_schema = inputSchema({ first_name: "string", last_name: "string" });
  assertClean(await compileDraftTwice(seq), 2);
}

/** The same reference repeated across param fields is interned once. */
export async function caseRepeatedReferenceDeduped(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    step("use", "noop", {
      a: "{{ data.x }}",
      b: "prefix {{ data.x }} suffix",
      c: ["{{ data.x }}", "{{ data.x }}"],
    }),
  ]);
  seq.input_schema = inputSchema({ x: "string" });
  assertClean(await compileDraftTwice(seq), 1);
}

/** The same reference in two different consumers is checked per consumer. */
export async function caseSameReferencePerConsumerCounted(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    step("use_one", "noop", { a: "{{ data.x }}" }),
    step("use_two", "noop", { b: "{{ data.x }}" }),
  ]);
  seq.input_schema = inputSchema({ x: "string" });
  assertClean(await compileDraftTwice(seq), 2);
}

/**
 * A reference and its fallback-carrying twin are distinct checks: the
 * unguarded optional read errors, the fallback read is accepted.
 */
export async function caseFallbackVariantsCountedSeparately(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    step("use", "noop", {
      a: "{{ data.opt }}",
      b: "{{ data.opt | fallback }}",
    }),
  ]);
  seq.input_schema = inputSchema({ opt: "string" }, []);
  const plan = await compileDraftTwice(seq);
  assert.equal(plan.report.references_checked, 2);
  const finding = singleFinding(plan);
  assert.equal(finding.code, "VALUE_MAY_BE_ABSENT");
  assert.equal(finding.severity, "error");
  assert.equal(finding.consumer, "use");
  assert.equal(finding.reference, "data.opt");
}

/** Findings are sorted by (consumer, code, reference) regardless of block order. */
export async function caseFindingsSortedDeterministically(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    step("z_consumer", "noop", {
      r1: "{{ outputs.ghost.a }}",
      r2: "{{ data.missing }}",
    }),
    step("a_consumer", "noop", { r: "{{ outputs.ghost.b }}" }),
  ]);
  seq.input_schema = inputSchema({ declared: "string" });
  const plan = await compileDraftTwice(seq);
  assert.equal(plan.report.references_checked, 3);
  assert.deepEqual(
    plan.report.findings.map((finding) => [
      finding.consumer,
      finding.code,
      finding.reference,
    ]),
    [
      ["a_consumer", "MISSING_PRODUCER", "outputs.ghost.b"],
      ["z_consumer", "MISSING_PRODUCER", "outputs.ghost.a"],
      ["z_consumer", "SCHEMA_PATH_MISSING", "data.missing"],
    ],
  );
}

/** Nested producers are flattened into the generated output bindings. */
export async function caseNestedProducerInGeneratedOutputs(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    parallelBlock("par", [[producerStep("nested_src", { v: "string" })]]),
  ]);
  const plan = await compileDraftTwice(seq);
  const outputs = plan.generated.schema.outputs;
  assert.ok(outputs.nested_src, `nested_src missing from ${JSON.stringify(outputs)}`);
  assert.deepEqual(outputs.nested_src, closedObjectSchema({ v: "string" }));
  assert.ok(
    plan.generated.typescript.includes('"nested_src"'),
    "nested producer missing from TypeScript bindings",
  );
  assert.ok(
    plan.generated.python.includes('"nested_src"'),
    "nested producer missing from Python bindings",
  );
}

/** Output schemas from every nesting level land in the generated model. */
export async function caseMultiLevelOutputsCollected(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    producerStep("root_src", { a: "string" }),
    loopBlock("lp", "true", [producerStep("loop_src", { b: "string" })]),
    tryCatchBlock("tc", [producerStep("try_src", { c: "string" })], [step("h", "noop")]),
  ]);
  const plan = await compileDraftTwice(seq);
  const outputs = plan.generated.schema.outputs;
  for (const id of ["root_src", "loop_src", "try_src"]) {
    assert.ok(outputs[id], `${id} missing from generated outputs`);
    assert.ok(plan.generated.typescript.includes(`"${id}"`), `${id} missing from TS`);
  }
}

/** 40 levels of cancellation_scope nesting compile deterministically. */
export async function caseDeepScopeNestingCompilesDeterministically(
  caseName: string,
): Promise<void> {
  let inner: Block[] = [producerStep("deep_src", { v: "string" })];
  for (let level = 0; level < 40; level += 1) {
    inner = [cancellationScopeBlock(`scope_${level}`, inner)];
  }
  const seq = testSequence(caseName, [
    ...inner,
    step("use", "noop", { x: "{{ outputs.deep_src.v }}" }),
  ]);
  assertClean(await compileDraftTwice(seq), 1);
}

/** Object schema nested `depth` levels deep around a string leaf. */
function nestedObjectSchema(depth: number): Record<string, unknown> {
  let schema: Record<string, unknown> = { type: "string" };
  for (let level = 0; level < depth; level += 1) {
    schema = {
      type: "object",
      properties: { child: schema },
      required: ["child"],
      additionalProperties: false,
    };
  }
  return schema;
}

/** Input schemas deeper than the generator budget (32) are rejected, 400. */
export async function caseInputSchemaDepthLimitRejected(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [step("s", "noop")]);
  seq.input_schema = nestedObjectSchema(33);
  await assert.rejects(
    () => client.compileSequenceDataflow(seq),
    (error: unknown) => {
      assert.ok(error instanceof ApiError);
      assert.equal(error.status, 400);
      assert.match(error.body, /depth limit/);
      return true;
    },
  );
}

/** An input schema just inside the depth budget compiles cleanly. */
export async function caseInputSchemaDepthWithinBudget(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [step("s", "noop")]);
  seq.input_schema = nestedObjectSchema(30);
  assertClean(await compileDraftTwice(seq), 0);
}

/** Input schemas with more nodes than the generator budget are rejected. */
export async function caseInputSchemaNodeLimitRejected(caseName: string): Promise<void> {
  const properties: Record<string, unknown> = {};
  for (let index = 0; index < 4200; index += 1) {
    properties[`field_${index}`] = { type: "string" };
  }
  const seq = testSequence(caseName, [step("s", "noop")]);
  seq.input_schema = { type: "object", properties };
  await assert.rejects(
    () => client.compileSequenceDataflow(seq),
    (error: unknown) => {
      assert.ok(error instanceof ApiError);
      assert.equal(error.status, 400);
      assert.match(error.body, /node limit/);
      return true;
    },
  );
}

// ---------------------------------------------------------------------------
// Equivalence: canonical hashing, determinism, tamper evidence
// ---------------------------------------------------------------------------

/** The source hash is a 64-hex SHA-256 and the whole plan is deterministic. */
export async function caseShaFormatAndDeterminism(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    producerStep("src", { token: "string" }),
    step("use", "noop", { x: "{{ outputs.src.token }}" }),
  ]);
  const plan = await compileDraftTwice(seq);
  assert.match(plan.generated.sequence_sha256, /^[0-9a-f]{64}$/);
}

/**
 * Canonicalization: the same definition with every object's keys in reverse
 * order hashes identically — the plan binds to semantics, not serialization.
 */
export async function caseFlatKeyOrderInvariant(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    step("use", "noop", { alpha: "1", beta: "2", gamma: "3" }),
  ]);
  const reordered = reversedKeys(seq) as SequenceDef;
  const first = await compileDraft(seq);
  const second = await compileDraft(reordered);
  assert.equal(second.generated.sequence_sha256, first.generated.sequence_sha256);
  assert.deepEqual(second.report, first.report);
}

/** Key-order invariance also holds through composite block structures. */
export async function caseCompositeKeyOrderInvariant(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    routerBlock("rt", [
      { condition: "true", blocks: [producerStep("r_src", { v: "string" })] },
      { condition: "false", blocks: [step("other", "noop", { z: 1, a: 2 })] },
    ]),
    tryCatchBlock("tc", [step("t", "noop")], [step("c", "noop")], [step("f", "noop")]),
  ]);
  const reordered = reversedKeys(seq) as SequenceDef;
  const first = await compileDraft(seq);
  const second = await compileDraft(reordered);
  assert.equal(second.generated.sequence_sha256, first.generated.sequence_sha256);
  assert.deepEqual(second.report, first.report);
}

/** Tamper evidence: changing one param value changes the source hash. */
export async function caseTamperedParamValueChangesHash(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [step("s", "noop", { amount: 100 })]);
  const tampered = clone(seq);
  (tampered.blocks[0] as { params: Record<string, unknown> }).params.amount = 101;
  const first = await compileDraft(seq);
  const second = await compileDraft(tampered);
  assert.notEqual(second.generated.sequence_sha256, first.generated.sequence_sha256);
}

/** Tamper evidence: changing a handler changes the source hash. */
export async function caseTamperedHandlerChangesHash(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [step("s", "noop")]);
  const tampered = clone(seq);
  (tampered.blocks[0] as { handler: string }).handler = "log";
  const first = await compileDraft(seq);
  const second = await compileDraft(tampered);
  assert.notEqual(second.generated.sequence_sha256, first.generated.sequence_sha256);
}

/**
 * Fail-closed: renaming a producer changes the hash AND the recompiled
 * report flags the now-dangling reference — a stale plan cannot pass.
 */
export async function caseTamperedProducerIdChangesHashAndReport(
  caseName: string,
): Promise<void> {
  const seq = testSequence(caseName, [
    producerStep("src", { token: "string" }),
    step("use", "noop", { x: "{{ outputs.src.token }}" }),
  ]);
  const tampered = clone(seq);
  (tampered.blocks[0] as { id: string }).id = "src_renamed";
  const first = await compileDraft(seq);
  assertClean(first, 1);
  const second = await compileDraft(tampered);
  assert.notEqual(second.generated.sequence_sha256, first.generated.sequence_sha256);
  const finding = singleFinding(second);
  assert.equal(finding.code, "MISSING_PRODUCER");
  assert.equal(finding.reference, "outputs.src.token");
}

/**
 * Evolving a producer's output schema rehashes the plan while compatible
 * references still compile clean — equivalence is exact, not approximate.
 */
export async function caseSchemaEvolutionChangesHash(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    producerStep("src", { token: "string" }),
    step("use", "noop", { x: "{{ outputs.src.token }}" }),
  ]);
  const evolved = clone(seq);
  const producer = evolved.blocks[0] as unknown as {
    output_schema: { properties: Record<string, unknown> };
  };
  producer.output_schema.properties.extra = { type: "string" };
  const first = await compileDraft(seq);
  const second = await compileDraft(evolved);
  assert.notEqual(second.generated.sequence_sha256, first.generated.sequence_sha256);
  assertClean(second, 1);
}

/**
 * Stored equivalence: compiling the stored definition (GET) and compiling
 * the same definition as a draft (POST) produce the same plan — storage
 * round-trips must not perturb the canonical source.
 */
export async function caseStoredRoundTripEquivalence(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    producerStep("src", { token: "string" }),
    parallelBlock("par", [[step("branch_step", "noop", { x: "{{ outputs.src.token }}" })]]),
  ]);
  await client.createSequence(seq);
  const stored = await client.getSequence(seq.id);
  const viaStored = await compileStored(seq.id);
  const viaDraft = await compileDraft(stored);
  assert.equal(viaDraft.generated.sequence_sha256, viaStored.generated.sequence_sha256);
  assert.deepEqual(viaDraft.report, viaStored.report);
  assertClean(viaStored, 1);
}

/**
 * Fail-closed across versions: two stored versions of one name compile
 * against their own definitions only — v2 cannot borrow v1's producers.
 */
export async function caseVersionIsolationFailClosed(caseName: string): Promise<void> {
  const v1 = testSequence(caseName, [producerStep("src", { token: "string" })]);
  await client.createSequence(v1);
  const v2: SequenceDef = {
    ...clone(v1),
    id: uuid(),
    version: 2,
    blocks: [step("use", "noop", { x: "{{ outputs.src.token }}" })],
  };
  await client.createSequence(v2);

  const planV1 = await compileStored(v1.id);
  const planV2 = await compileStored(v2.id);
  assertClean(planV1, 0);
  assert.notEqual(planV2.generated.sequence_sha256, planV1.generated.sequence_sha256);
  const finding = singleFinding(planV2);
  assert.equal(finding.code, "MISSING_PRODUCER");
  assert.equal(finding.consumer, "use");
  assert.equal(finding.reference, "outputs.src.token");
}

/** Draft compilation is pure: nothing is persisted as a side effect. */
export async function caseDraftCompileIsPure(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [producerStep("src", { token: "string" })]);
  await compileDraft(seq);
  await assert.rejects(
    () => client.getSequence(seq.id),
    (error: unknown) => error instanceof ApiError && error.status === 404,
  );
  await assert.rejects(
    () => client.getSequenceByName(seq.tenant_id, seq.namespace, seq.name),
    (error: unknown) => error instanceof ApiError && error.status === 404,
  );
}

/** Stored compilation is deterministic across repeated GETs. */
export async function caseStoredCompileDeterministic(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    producerStep("src", { token: "string" }),
    step("use", "noop", { x: "{{ outputs.src.token }}" }),
  ]);
  await client.createSequence(seq);
  const first = await compileStored(seq.id);
  const second = await compileStored(seq.id);
  assert.deepEqual(second, first);
}

/** Compiling a stored plan for an unknown sequence id is a 404. */
export async function caseUnknownSequenceDataflowIs404(): Promise<void> {
  await assert.rejects(
    () => client.getSequenceDataflow(uuid()),
    (error: unknown) => error instanceof ApiError && error.status === 404,
  );
}

/** Every generated binding embeds the same canonical source hash. */
export async function caseGeneratedBindingsEmbedSourceHash(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [producerStep("src", { token: "string" })]);
  const plan = await compileDraftTwice(seq);
  const sha = plan.generated.sequence_sha256;
  assert.equal(plan.generated.generator_version, "orch8-dataflow-v2");
  assert.equal(plan.generated.schema.generator_version, "orch8-dataflow-v2");
  assert.equal(plan.generated.schema.sequence.sha256, sha);
  for (const language of [
    plan.generated.typescript,
    plan.generated.python,
    plan.generated.swift,
    plan.generated.kotlin,
  ]) {
    assert.ok(language.includes(sha), "generated binding missing the source hash");
  }
}

// ---------------------------------------------------------------------------
// Plugin handler schemes: compile acceptance + runtime dispatch decisions
// ---------------------------------------------------------------------------

/** ap:// steps compile like any other; their params are reference-checked. */
export async function caseApHandlerDraftCompiles(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    step("ap_step", "ap://slack.send_channel_message", {
      props: { text: "{{ data.message }}" },
    }),
  ]);
  seq.input_schema = inputSchema({ message: "string" });
  assertClean(await compileDraftTwice(seq), 1);
}

/** grpc:// steps compile like any other; their params are reference-checked. */
export async function caseGrpcHandlerDraftCompiles(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    step("grpc_step", "grpc://127.0.0.1:50051/mailer.Mailer/Send", {
      to: "{{ data.email }}",
    }),
  ]);
  seq.input_schema = inputSchema({ email: "string" });
  assertClean(await compileDraftTwice(seq), 1);
}

/** wasm:// steps compile like any other; their params are reference-checked. */
export async function caseWasmHandlerDraftCompiles(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    step("wasm_step", "wasm://transform", { input: "{{ data.payload }}" }),
  ]);
  seq.input_schema = inputSchema({ payload: "object" });
  assertClean(await compileDraftTwice(seq), 1);
}

/** A plugin step with a declared output schema is a fully typed producer. */
export async function casePluginStepAsTypedProducer(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    step(
      "ap_step",
      "ap://slack.send_channel_message",
      { props: { text: "hello" } },
      { output_schema: closedObjectSchema({ message_id: "string" }) },
    ),
    step("use", "noop", { x: "{{ outputs.ap_step.message_id }}" }),
  ]);
  assertClean(await compileDraftTwice(seq), 1);
}

/** All three plugin schemes in one plan compile deterministically. */
export async function caseMixedPluginSchemesDeterministic(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    step("ap_step", "ap://gmail.send_email", { props: { to: "{{ data.email }}" } }),
    step("grpc_step", "grpc://127.0.0.1:50051/echo.Echo/Ping", { v: "{{ data.email }}" }),
    step("wasm_step", "wasm://transform", { input: "{{ data.email }}" }),
  ]);
  seq.input_schema = inputSchema({ email: "string" });
  assertClean(await compileDraftTwice(seq), 3);
}

/**
 * Runtime dispatch: create + run a two-root sequence, expecting it to reach
 * `failed` because the plugin step is dispatched in-process (tree evaluator)
 * and its backend is unreachable. If the compiled plan's plugin flag were
 * lost, the flat path would queue the plugin step for an external worker
 * and the instance would never reach a terminal state.
 */
async function runMixedPluginExpectFailed(
  caseName: string,
  pluginStep: Block,
): Promise<void> {
  const seq = testSequence(caseName, [step("pre", "noop"), pluginStep]);
  await client.createSequence(seq);
  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: seq.tenant_id,
    namespace: seq.namespace,
  });
  const final = await client.waitForState(id, ["failed", "completed"], {
    timeoutMs: 30_000,
  });
  assert.equal(
    final.state,
    "failed",
    `expected failed (plugin dispatched in-process), got ${final.state}`,
  );
}

/** ap:// root detected via the compiled plan's plugin flag → tree dispatch. */
export async function caseRunMixedRootsApPluginFails(caseName: string): Promise<void> {
  await runMixedPluginExpectFailed(
    caseName,
    step("ap_step", "ap://slack.send_channel_message", { props: { text: "hi" } }),
  );
}

/** wasm:// root detected via the compiled plan's plugin flag → tree dispatch. */
export async function caseRunMixedRootsWasmPluginFails(caseName: string): Promise<void> {
  await runMixedPluginExpectFailed(
    caseName,
    step("wasm_step", "wasm://nonexistent-plansem-module"),
  );
}

/** grpc:// root detected via the compiled plan's plugin flag → tree dispatch. */
export async function caseRunMixedRootsGrpcPluginFails(caseName: string): Promise<void> {
  await runMixedPluginExpectFailed(
    caseName,
    step("grpc_step", "grpc://127.0.0.1:1/plansem.Echo/Run"),
  );
}

/**
 * Injected-roots fallback: the cached plan for a flat step-only sequence
 * says "no composite blocks", but injecting a parallel block at runtime
 * changes the roots — dispatch must fall back to scanning the merged
 * definition and still route through the tree evaluator to completion.
 */
export async function caseRunInjectedCompositeRootFallsBack(
  caseName: string,
): Promise<void> {
  const seq = testSequence(caseName, [step("s1", "sleep", { duration_ms: 3000 })]);
  await client.createSequence(seq);
  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: seq.tenant_id,
    namespace: seq.namespace,
  });
  await client.waitForState(id, "running", { timeoutMs: 5_000 });
  await client.injectBlocks(id, [
    parallelBlock("injected_par", [
      [step("inj_a", "noop")],
      [step("inj_b", "noop")],
    ]),
  ]);
  const final = await client.waitForState(id, "completed", { timeoutMs: 15_000 });
  assert.equal(final.state, "completed");
}

// ---------------------------------------------------------------------------
// Multi-root sequences
// ---------------------------------------------------------------------------

/** Three roots of different kinds all expose their nested producers. */
export async function caseThreeRootProducersResolve(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    producerStep("root_src", { a: "string" }),
    parallelBlock("par", [[producerStep("par_src", { b: "string" })]]),
    tryCatchBlock("tc", [producerStep("tc_src", { c: "string" })], [step("h", "noop")]),
    step("use", "noop", {
      a: "{{ outputs.root_src.a }}",
      b: "{{ outputs.par_src.b }}",
      c: "{{ outputs.tc_src.c }}",
    }),
  ]);
  assertClean(await compileDraftTwice(seq), 3);
}

/** Compilation is root-order independent: consumers may precede producers. */
export async function caseConsumerBeforeProducerRootOrder(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    step("early_use", "noop", { x: "{{ outputs.late_src.token }}" }),
    producerStep("late_src", { token: "string" }),
  ]);
  assertClean(await compileDraftTwice(seq), 1);
}

/** Reference checks aggregate across roots, conditions, and bodies. */
export async function caseAggregateReferencesAcrossRoots(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    producerStep("src", { v: "string" }, { seed: "{{ data.seed }}" }),
    loopBlock("lp", "data.keep", [step("body_use", "noop", { x: "{{ outputs.src.v }}" })]),
    routerBlock("rt", [
      { condition: "outputs.src.v == state.mode", blocks: [] },
    ]),
  ]);
  seq.input_schema = inputSchema({ seed: "string", keep: "boolean" });
  const plan = await compileDraftTwice(seq);
  // src params (1) + lp condition (1) + body_use params (1) + rt condition (2)
  assert.equal(plan.report.references_checked, 5);
  const finding = singleFinding(plan);
  assert.equal(finding.code, "TYPE_UNKNOWN");
  assert.equal(finding.severity, "warning");
  assert.equal(finding.consumer, "rt");
  assert.equal(finding.reference, "state.mode");
}

/** A sequence whose roots are all composite still compiles every producer. */
export async function caseCompositeOnlyRootsCompile(caseName: string): Promise<void> {
  const seq = testSequence(caseName, [
    parallelBlock("par", [[producerStep("pa_src", { v: "string" })]]),
    tryCatchBlock(
      "tc",
      [producerStep("tc_src", { w: "string" })],
      [step("h", "noop")],
      [
        step("fin_use", "noop", {
          a: "{{ outputs.pa_src.v }}",
          b: "{{ outputs.tc_src.w }}",
        }),
      ],
    ),
  ]);
  const plan = await compileDraftTwice(seq);
  assertClean(plan, 2);
  const outputs = plan.generated.schema.outputs;
  assert.ok(outputs.pa_src, "pa_src missing from generated outputs");
  assert.ok(outputs.tc_src, "tc_src missing from generated outputs");
}

/** A wide fan-out of typed roots compiles deterministically. */
export async function caseTwelveRootFanOutDeterministic(caseName: string): Promise<void> {
  const producers: Block[] = [];
  const params: Record<string, unknown> = {};
  for (let index = 0; index < 12; index += 1) {
    producers.push(producerStep(`fan_${index}`, { v: "string" }));
    params[`p${index}`] = `{{ outputs.fan_${index}.v }}`;
  }
  const seq = testSequence(caseName, [...producers, step("use", "noop", params)]);
  const plan = await compileDraftTwice(seq);
  assertClean(plan, 12);
  // Every block id is registered in the generated outputs — the 12 typed
  // producers plus the schema-less consumer (recorded as `true`).
  const outputs = plan.generated.schema.outputs;
  assert.equal(Object.keys(outputs).length, 13);
  assert.equal(outputs.use, true);
}

/** An empty block list is a valid, trivially deterministic plan. */
export async function caseEmptyBlocksCompile(caseName: string): Promise<void> {
  const seq = testSequence(caseName, []);
  const plan = await compileDraftTwice(seq);
  assert.equal(plan.report.references_checked, 0);
  assert.deepEqual(plan.report.findings, []);
  assert.deepEqual(plan.generated.schema.outputs, {});
  assert.match(plan.generated.sequence_sha256, /^[0-9a-f]{64}$/);
}
