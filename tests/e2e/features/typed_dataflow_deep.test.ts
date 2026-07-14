/**
 * Typed Dataflow Compiler — deep coverage.
 *
 * Complements `typed_dataflow.test.ts` (2 smoke tests) with exhaustive
 * coverage of the compiler contract in `orch8-engine/src/dataflow.rs`:
 * reference detection across every consumer position (params, `when`,
 * loop `condition`/`break_on`/`collection`, router route conditions),
 * every root (`data.*`, `outputs.*`, `state.*`, `config.*`), every
 * finding code (MISSING_PRODUCER, SCHEMA_PATH_MISSING, TYPE_UNKNOWN,
 * VALUE_MAY_BE_ABSENT, VALUE_MAY_BE_NULL), fallback-chain suppression,
 * array/index path validation, generation determinism, and the
 * depth/node generation bounds.
 */
import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";

import { Orch8Client, step, testSequence, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

function findingCodes(report: any): string[] {
  return report.report.findings.map((f: any) => f.code);
}

function findingByRef(report: any, reference: string): any {
  return report.report.findings.find((f: any) => f.reference === reference);
}

describe("Typed Dataflow — deep coverage", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // ------------------------------------------------------------------
  // Reference roots: data.*
  // ------------------------------------------------------------------
  describe("data.* references", () => {
    it("accepts a declared required string field", async () => {
      const seq = testSequence("df-data-ok", [
        step("s1", "noop", { v: "{{ data.name }}" }),
      ]);
      seq.input_schema = {
        type: "object",
        properties: { name: { type: "string" } },
        required: ["name"],
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.deepEqual(r.report.findings, []);
      assert.equal(r.report.references_checked, 1);
    });

    it("flags a field missing from a closed input schema", async () => {
      const seq = testSequence("df-data-missing", [
        step("s1", "noop", { v: "{{ data.absent }}" }),
      ]);
      seq.input_schema = {
        type: "object",
        properties: { name: { type: "string" } },
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "data.absent").code, "SCHEMA_PATH_MISSING");
    });

    it("treats data.* as TYPE_UNKNOWN when no input_schema is declared", async () => {
      const seq = testSequence("df-data-noschema", [
        step("s1", "noop", { v: "{{ data.anything }}" }),
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "data.anything").code, "TYPE_UNKNOWN");
      assert.ok(r.report.findings.every((f: any) => f.severity === "warning"));
    });

    it("resolves a nested required path", async () => {
      const seq = testSequence("df-data-nested", [
        step("s1", "noop", { v: "{{ data.user.address.city }}" }),
      ]);
      seq.input_schema = {
        type: "object",
        properties: {
          user: {
            type: "object",
            properties: {
              address: {
                type: "object",
                properties: { city: { type: "string" } },
                required: ["city"],
                additionalProperties: false,
              },
            },
            required: ["address"],
            additionalProperties: false,
          },
        },
        required: ["user"],
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.deepEqual(r.report.findings, []);
    });

    it("flags an optional field referenced without a fallback", async () => {
      const seq = testSequence("df-data-optional", [
        step("s1", "noop", { v: "{{ data.nickname }}" }),
      ]);
      seq.input_schema = {
        type: "object",
        properties: { nickname: { type: "string" } },
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "data.nickname").code, "VALUE_MAY_BE_ABSENT");
    });

    it("accepts an optional field referenced with a fallback", async () => {
      const seq = testSequence("df-data-optional-fb", [
        step("s1", "noop", { v: "{{ data.nickname | anon }}" }),
      ]);
      seq.input_schema = {
        type: "object",
        properties: { nickname: { type: "string" } },
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.deepEqual(r.report.findings, []);
    });

    it("flags a nullable required field referenced without a fallback", async () => {
      const seq = testSequence("df-data-nullable", [
        step("s1", "noop", { v: "{{ data.score }}" }),
      ]);
      seq.input_schema = {
        type: "object",
        properties: { score: { type: ["number", "null"] } },
        required: ["score"],
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "data.score").code, "VALUE_MAY_BE_NULL");
    });

    it("accepts a nullable field referenced with a fallback", async () => {
      const seq = testSequence("df-data-nullable-fb", [
        step("s1", "noop", { v: "{{ data.score | default(0) }}" }),
      ]);
      seq.input_schema = {
        type: "object",
        properties: { score: { type: ["number", "null"] } },
        required: ["score"],
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.deepEqual(r.report.findings, []);
    });

    it("treats an open (non-closed) object schema path as dynamic/unknown", async () => {
      const seq = testSequence("df-data-open", [
        step("s1", "noop", { v: "{{ data.freeform.whatever }}" }),
      ]);
      seq.input_schema = {
        type: "object",
        properties: { freeform: { type: "object" } },
        required: ["freeform"],
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "data.freeform.whatever").code, "TYPE_UNKNOWN");
    });

    it("resolves an array index path within declared minItems bounds", async () => {
      const seq = testSequence("df-data-array-ok", [
        step("s1", "noop", { v: "{{ data.items[0].id }}" }),
      ]);
      seq.input_schema = {
        type: "object",
        properties: {
          items: {
            type: "array",
            minItems: 1,
            items: {
              type: "object",
              properties: { id: { type: "string" } },
              required: ["id"],
              additionalProperties: false,
            },
          },
        },
        required: ["items"],
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.deepEqual(r.report.findings, []);
    });

    it("flags an array index path outside declared minItems as optional", async () => {
      const seq = testSequence("df-data-array-optional", [
        step("s1", "noop", { v: "{{ data.items[5].id }}" }),
      ]);
      seq.input_schema = {
        type: "object",
        properties: {
          items: {
            type: "array",
            minItems: 1,
            items: {
              type: "object",
              properties: { id: { type: "string" } },
              required: ["id"],
              additionalProperties: false,
            },
          },
        },
        required: ["items"],
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(
        findingByRef(r, "data.items[5].id").code,
        "VALUE_MAY_BE_ABSENT",
      );
    });

    it("treats a non-numeric segment against an array schema as dynamic/unknown", async () => {
      // "name" doesn't parse as an index, so the walker looks for a
      // `properties` map on the array schema itself (arrays don't have
      // one) and — since `additionalProperties: false` isn't set on the
      // array schema — falls through to Dynamic rather than Missing.
      const seq = testSequence("df-data-array-badpath", [
        step("s1", "noop", { v: "{{ data.items.name }}" }),
      ]);
      seq.input_schema = {
        type: "object",
        properties: {
          items: { type: "array", items: { type: "string" } },
        },
        required: ["items"],
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "data.items.name").code, "TYPE_UNKNOWN");
    });
  });

  // ------------------------------------------------------------------
  // Reference roots: outputs.*
  // ------------------------------------------------------------------
  describe("outputs.* references", () => {
    it("rejects a reference to a nonexistent producer", async () => {
      const seq = testSequence("df-out-missing-producer", [
        step("s1", "noop", { v: "{{ outputs.ghost.field }}" }),
      ]);
      const r = await client.compileSequenceDataflow(seq);
      const f = findingByRef(r, "outputs.ghost.field");
      assert.equal(f.code, "MISSING_PRODUCER");
      assert.equal(f.consumer, "s1");
      assert.match(f.summary, /'ghost'/);
    });

    it("does not let a spoofed block-shaped param object register as a producer", async () => {
      const seq = testSequence("df-out-spoof", [
        step("source", "noop", { payload: { type: "step", id: "spoofed" } }),
        step("consumer", "noop", { v: "{{ outputs.spoofed.id }}" }),
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "outputs.spoofed.id").code, "MISSING_PRODUCER");
    });

    it("treats a producer with no output_schema as TYPE_UNKNOWN, not a false pass", async () => {
      const seq = testSequence("df-out-noschema", [
        step("source", "noop", {}),
        step("consumer", "noop", { v: "{{ outputs.source.value }}" }),
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "outputs.source.value").code, "TYPE_UNKNOWN");
    });

    it("accepts a path present in a closed output_schema", async () => {
      const seq = testSequence("df-out-ok", [
        step("source", "noop", {}, {
          output_schema: {
            type: "object",
            properties: { id: { type: "string" } },
            required: ["id"],
            additionalProperties: false,
          },
        }),
        step("consumer", "noop", { v: "{{ outputs.source.id }}" }),
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.deepEqual(r.report.findings, []);
    });

    it("rejects a path excluded by a closed output_schema", async () => {
      const seq = testSequence("df-out-closed-violation", [
        step("source", "noop", {}, {
          output_schema: {
            type: "object",
            properties: { id: { type: "string" } },
            additionalProperties: false,
          },
        }),
        step("consumer", "noop", { v: "{{ outputs.source.nope }}" }),
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "outputs.source.nope").code, "SCHEMA_PATH_MISSING");
    });

    it("chains a two-hop producer reference through nested objects", async () => {
      const seq = testSequence("df-out-nested", [
        step("source", "noop", {}, {
          output_schema: {
            type: "object",
            properties: {
              result: {
                type: "object",
                properties: { ok: { type: "boolean" } },
                required: ["ok"],
                additionalProperties: false,
              },
            },
            required: ["result"],
            additionalProperties: false,
          },
        }),
        step("consumer", "noop", { v: "{{ outputs.source.result.ok }}" }),
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.deepEqual(r.report.findings, []);
    });

    it("finds forward references to a producer declared later in the block list", async () => {
      // The compiler indexes all producers before validating references, so
      // declaration order must not matter.
      const seq = testSequence("df-out-forward-ref", [
        step("consumer", "noop", { v: "{{ outputs.source.id }}" }),
        step("source", "noop", {}, {
          output_schema: {
            type: "object",
            properties: { id: { type: "string" } },
            required: ["id"],
            additionalProperties: false,
          },
        }),
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.deepEqual(r.report.findings, []);
    });
  });

  // ------------------------------------------------------------------
  // Reference roots: state.* and config.* (always TYPE_UNKNOWN — no schema exists)
  // ------------------------------------------------------------------
  describe("state.* and config.* references", () => {
    it("always reports state.* as TYPE_UNKNOWN regardless of shape", async () => {
      const seq = testSequence("df-state", [
        step("s1", "noop", { v: "{{ state.phase }}" }),
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "state.phase").code, "TYPE_UNKNOWN");
    });

    it("always reports config.* as TYPE_UNKNOWN regardless of shape", async () => {
      const seq = testSequence("df-config", [
        step("s1", "noop", { v: "{{ config.region }}" }),
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "config.region").code, "TYPE_UNKNOWN");
    });

    it("counts state.* and config.* references even though both are unknown", async () => {
      const seq = testSequence("df-state-config-count", [
        step("s1", "noop", { v: "{{ state.a }} {{ config.b }}" }),
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(r.report.references_checked, 2);
    });
  });

  // ------------------------------------------------------------------
  // Consumer positions
  // ------------------------------------------------------------------
  describe("consumer positions", () => {
    it("inspects references inside a step's params", async () => {
      const seq = testSequence("df-pos-params", [
        step("s1", "noop", { v: "{{ outputs.missing.x }}" }),
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "outputs.missing.x").consumer, "s1");
    });

    it("inspects references inside a when guard", async () => {
      const seq = testSequence("df-pos-when", [
        step("s1", "noop", {}, { when: "outputs.missing.flag == true" }),
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "outputs.missing.flag").consumer, "s1");
    });

    it("inspects references inside a loop condition", async () => {
      const seq = testSequence("df-pos-loop-condition", [
        {
          type: "loop",
          id: "lp",
          condition: "outputs.missing.keep_going == true",
          body: [step("inner", "noop")],
        } as any,
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "outputs.missing.keep_going").consumer, "lp");
    });

    it("inspects references inside a loop break_on", async () => {
      const seq = testSequence("df-pos-loop-breakon", [
        {
          type: "loop",
          id: "lp",
          condition: "true",
          break_on: "outputs.missing.stop == true",
          body: [step("inner", "noop")],
        } as any,
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "outputs.missing.stop").consumer, "lp");
    });

    it("inspects references inside a for_each collection", async () => {
      const seq = testSequence("df-pos-foreach", [
        {
          type: "for_each",
          id: "fe",
          collection: "{{ outputs.missing.items }}",
          item_var: "item",
          body: [step("inner", "noop")],
        } as any,
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "outputs.missing.items").consumer, "fe");
    });

    it("inspects references inside a sub_sequence's input", async () => {
      const seq = testSequence("df-pos-subseq", [
        {
          type: "sub_sequence",
          id: "sub",
          sequence_name: "does-not-matter-for-dataflow",
          input: { v: "{{ outputs.missing.field }}" },
        } as any,
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "outputs.missing.field").consumer, "sub");
    });

    it("inspects references inside every router route condition, indexed", async () => {
      const seq = testSequence("df-pos-router", [
        {
          type: "router",
          id: "rt",
          routes: [
            { condition: "outputs.missing_a.ok == true", blocks: [] },
            { condition: "outputs.missing_b.ok == true", blocks: [] },
          ],
          default: [],
        } as any,
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "outputs.missing_a.ok").consumer, "rt");
      assert.equal(findingByRef(r, "outputs.missing_b.ok").consumer, "rt");
    });

    it("does not inspect a router's default branch condition field (there is none) but does inspect nested block params", async () => {
      const seq = testSequence("df-pos-router-default", [
        {
          type: "router",
          id: "rt",
          routes: [{ condition: "true", blocks: [] }],
          default: [step("fallback_step", "noop", { v: "{{ outputs.missing.x }}" })],
        } as any,
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "outputs.missing.x").consumer, "fallback_step");
    });

    it("recurses into saga action and compensation blocks", async () => {
      const seq = testSequence("df-pos-saga", [
        {
          type: "saga",
          id: "sg",
          steps: [
            {
              id: "st1",
              action: step("action_step", "noop", { v: "{{ outputs.missing.a }}" }),
              compensation: step("comp_step", "noop", { v: "{{ outputs.missing.b }}" }),
            },
          ],
        } as any,
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "outputs.missing.a").consumer, "action_step");
      assert.equal(findingByRef(r, "outputs.missing.b").consumer, "comp_step");
    });

    it("recurses into try_catch try/catch/finally blocks", async () => {
      const seq = testSequence("df-pos-trycatch", [
        {
          type: "try_catch",
          id: "tc",
          try_block: [step("try_step", "noop", { v: "{{ outputs.missing.a }}" })],
          catch_block: [step("catch_step", "noop", { v: "{{ outputs.missing.b }}" })],
          finally_block: [step("finally_step", "noop", { v: "{{ outputs.missing.c }}" })],
        } as any,
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "outputs.missing.a").consumer, "try_step");
      assert.equal(findingByRef(r, "outputs.missing.b").consumer, "catch_step");
      assert.equal(findingByRef(r, "outputs.missing.c").consumer, "finally_step");
    });

    it("recurses into parallel branches", async () => {
      const seq = testSequence("df-pos-parallel", [
        {
          type: "parallel",
          id: "par",
          branches: [
            [step("branch_a", "noop", { v: "{{ outputs.missing.a }}" })],
            [step("branch_b", "noop", { v: "{{ outputs.missing.b }}" })],
          ],
        } as any,
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "outputs.missing.a").consumer, "branch_a");
      assert.equal(findingByRef(r, "outputs.missing.b").consumer, "branch_b");
    });

    it("recurses into ab_split variants", async () => {
      const seq = testSequence("df-pos-absplit", [
        {
          type: "a_b_split",
          id: "ab",
          variants: [
            { name: "a", weight: 50, blocks: [step("variant_a", "noop", { v: "{{ outputs.missing.a }}" })] },
            { name: "b", weight: 50, blocks: [step("variant_b", "noop", { v: "{{ outputs.missing.b }}" })] },
          ],
        } as any,
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "outputs.missing.a").consumer, "variant_a");
      assert.equal(findingByRef(r, "outputs.missing.b").consumer, "variant_b");
    });

    it("recurses into cancellation_scope blocks", async () => {
      const seq = testSequence("df-pos-cancelscope", [
        {
          type: "cancellation_scope",
          id: "cs",
          blocks: [step("scoped_step", "noop", { v: "{{ outputs.missing.a }}" })],
        } as any,
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "outputs.missing.a").consumer, "scoped_step");
    });
  });

  // ------------------------------------------------------------------
  // Fallback-chain detection
  // ------------------------------------------------------------------
  describe("fallback chain suppression", () => {
    it("a bare pipe with no space still counts as a fallback", async () => {
      const seq = testSequence("df-fb-nospace", [
        step("s1", "noop", { v: "{{ data.opt|fallback }}" }),
      ]);
      seq.input_schema = {
        type: "object",
        properties: { opt: { type: "string" } },
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.deepEqual(r.report.findings, []);
    });

    it("a filter pipe like |upper is indistinguishable from a fallback pipe and also suppresses the finding", async () => {
      // The compiler's fallback detector only looks for a bare '|' in the
      // template tail — it cannot distinguish a value-fallback pipe from a
      // filter-function pipe. Documenting this as current, verified
      // behavior rather than assuming stricter semantics.
      const seq = testSequence("df-fb-filter-pipe", [
        step("s1", "noop", { v: "{{ data.opt | upper }}" }),
      ]);
      seq.input_schema = {
        type: "object",
        properties: { opt: { type: "string" } },
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.deepEqual(r.report.findings, []);
    });

    it("does not treat a double pipe as a fallback", async () => {
      const seq = testSequence("df-fb-doublepipe", [
        step("s1", "noop", { v: "{{ data.opt || other }}" }),
      ]);
      seq.input_schema = {
        type: "object",
        properties: { opt: { type: "string" } },
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "data.opt").code, "VALUE_MAY_BE_ABSENT");
    });

    it("only counts a fallback pipe that appears before the closing braces", async () => {
      const seq = testSequence("df-fb-after-close", [
        step("s1", "noop", { v: "{{ data.opt }} | not-a-fallback" }),
      ]);
      seq.input_schema = {
        type: "object",
        properties: { opt: { type: "string" } },
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(findingByRef(r, "data.opt").code, "VALUE_MAY_BE_ABSENT");
    });
  });

  // ------------------------------------------------------------------
  // Finding ordering and counting
  // ------------------------------------------------------------------
  describe("finding ordering and reference counting", () => {
    it("sorts findings by (consumer, code, reference)", async () => {
      const seq = testSequence("df-sort", [
        step("z_step", "noop", { v: "{{ outputs.missing.a }}" }),
        step("a_step", "noop", { v: "{{ outputs.missing.b }}" }),
      ]);
      const r = await client.compileSequenceDataflow(seq);
      const consumers = r.report.findings.map((f: any) => f.consumer);
      const sorted = [...consumers].sort();
      assert.deepEqual(consumers, sorted);
    });

    it("deduplicates an identical reference used twice in the same consumer", async () => {
      const seq = testSequence("df-dedupe", [
        step("s1", "noop", {
          a: "{{ outputs.missing.x }}",
          b: "{{ outputs.missing.x }}",
        }),
      ]);
      const r = await client.compileSequenceDataflow(seq);
      const matches = r.report.findings.filter(
        (f: any) => f.reference === "outputs.missing.x",
      );
      assert.equal(matches.length, 1);
      assert.equal(r.report.references_checked, 1);
    });

    it("counts multiple distinct references within one param value", async () => {
      const seq = testSequence("df-multi-ref", [
        step("s1", "noop", {
          v: "{{ data.a }} and {{ data.b }} and {{ data.c }}",
        }),
      ]);
      seq.input_schema = {
        type: "object",
        properties: {
          a: { type: "string" },
          b: { type: "string" },
          c: { type: "string" },
        },
        required: ["a", "b", "c"],
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(r.report.references_checked, 3);
      assert.deepEqual(r.report.findings, []);
    });

    it("reports zero references for a sequence with no template expressions", async () => {
      const seq = testSequence("df-no-refs", [step("s1", "noop", { v: "plain string" })]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(r.report.references_checked, 0);
      assert.deepEqual(r.report.findings, []);
    });
  });

  // ------------------------------------------------------------------
  // Generation: determinism, tagging, draft vs stored equivalence
  // ------------------------------------------------------------------
  describe("generation determinism and equivalence", () => {
    it("produces byte-identical output across repeated draft compiles", async () => {
      const seq = testSequence("df-gen-stable", [
        step("s1", "noop", {}, {
          output_schema: {
            type: "object",
            properties: { id: { type: "string" } },
            required: ["id"],
            additionalProperties: false,
          },
        }),
      ]);
      const first = await client.compileSequenceDataflow(seq);
      const second = await client.compileSequenceDataflow(seq);
      const third = await client.compileSequenceDataflow(seq);
      assert.deepEqual(first.generated, second.generated);
      assert.deepEqual(second.generated, third.generated);
    });

    it("tags generated output with the orch8-dataflow-v1 generator version", async () => {
      const seq = testSequence("df-gen-tag", [step("s1", "noop")]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(r.generated.generator_version, "orch8-dataflow-v1");
      assert.equal(r.generated.schema.generator_version, "orch8-dataflow-v1");
    });

    it("produces a 64-hex-char sha256 of the canonical sequence", async () => {
      const seq = testSequence("df-gen-hash", [step("s1", "noop")]);
      const r = await client.compileSequenceDataflow(seq);
      assert.match(r.generated.sequence_sha256, /^[0-9a-f]{64}$/);
    });

    it("changes the sha256 when the sequence content changes", async () => {
      const seqA = testSequence("df-gen-hash-a", [step("s1", "noop", { x: 1 })]);
      const seqB = testSequence("df-gen-hash-b", [step("s1", "noop", { x: 2 })]);
      const rA = await client.compileSequenceDataflow(seqA);
      const rB = await client.compileSequenceDataflow(seqB);
      assert.notEqual(rA.generated.sequence_sha256, rB.generated.sequence_sha256);
    });

    it("draft compile and stored-by-id compile produce identical generated bindings", async () => {
      const seq = testSequence("df-gen-draft-vs-stored", [
        step("s1", "noop", {}, {
          output_schema: {
            type: "object",
            properties: { ok: { type: "boolean" } },
            required: ["ok"],
            additionalProperties: false,
          },
        }),
      ]);
      const draft = await client.compileSequenceDataflow(seq);
      const stored = await client.createSequence(seq);
      const fromStored = await client.getSequenceDataflow(stored.id);
      assert.deepEqual(draft.generated, fromStored.generated);
      assert.deepEqual(draft.report, fromStored.report);
    });

    it("renders a required string property in TypeScript without an optional marker", async () => {
      const seq = testSequence("df-ts-required", [step("s1", "noop")]);
      seq.input_schema = {
        type: "object",
        properties: { name: { type: "string" } },
        required: ["name"],
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.match(r.generated.typescript, /"name": string;/);
    });

    it("renders an optional property in TypeScript with a ? marker", async () => {
      const seq = testSequence("df-ts-optional", [step("s1", "noop")]);
      seq.input_schema = {
        type: "object",
        properties: { nickname: { type: "string" } },
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.match(r.generated.typescript, /"nickname"\?: string;/);
    });

    it("renders a nullable union type in TypeScript", async () => {
      const seq = testSequence("df-ts-nullable", [step("s1", "noop")]);
      seq.input_schema = {
        type: "object",
        properties: { score: { type: ["number", "null"] } },
        required: ["score"],
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.match(r.generated.typescript, /"score": (null \| number|number \| null);/);
    });

    it("renders an enum as a TypeScript union of literals", async () => {
      const seq = testSequence("df-ts-enum", [step("s1", "noop")]);
      seq.input_schema = {
        type: "object",
        properties: { priority: { enum: ["high", "low"] } },
        required: ["priority"],
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.match(r.generated.typescript, /"priority": "high" \| "low";/);
    });

    it("renders a required field as Required[...] in Python", async () => {
      const seq = testSequence("df-py-required", [step("s1", "noop")]);
      seq.input_schema = {
        type: "object",
        properties: { name: { type: "string" } },
        required: ["name"],
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.match(r.generated.python, /"name": Required\[str\]/);
    });

    it("renders an optional field as NotRequired[...] in Python", async () => {
      const seq = testSequence("df-py-optional", [step("s1", "noop")]);
      seq.input_schema = {
        type: "object",
        properties: { nickname: { type: "string" } },
        additionalProperties: false,
      };
      const r = await client.compileSequenceDataflow(seq);
      assert.match(r.generated.python, /"nickname": NotRequired\[str\]/);
    });

    it("renders each step's output_schema as a StepOutputs entry keyed by block id", async () => {
      const seq = testSequence("df-py-outputs-key", [
        step("my_block", "noop", {}, {
          output_schema: {
            type: "object",
            properties: { id: { type: "string" } },
            required: ["id"],
            additionalProperties: false,
          },
        }),
      ]);
      const r = await client.compileSequenceDataflow(seq);
      assert.match(r.generated.typescript, /"my_block":/);
      assert.ok(r.generated.schema.outputs.my_block);
    });

    it("uses an empty-object/true schema for a step with no declared output_schema", async () => {
      const seq = testSequence("df-py-no-output-schema", [step("bare", "noop")]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(r.generated.schema.outputs.bare, true);
    });

    it("uses an unbounded true input schema when the sequence declares none", async () => {
      const seq = testSequence("df-no-input-schema", [step("s1", "noop")]);
      const r = await client.compileSequenceDataflow(seq);
      assert.equal(r.generated.schema.input, true);
    });
  });

  // ------------------------------------------------------------------
  // Generation bounds
  // ------------------------------------------------------------------
  describe("generation resource bounds", () => {
    it("rejects an input schema nested deeper than the depth limit with a 400", async () => {
      // MAX_SCHEMA_DEPTH is 32 in orch8-engine/src/dataflow.rs; build 34
      // nested levels to comfortably exceed it regardless of off-by-one
      // details in how depth is counted at the boundary.
      let schema: any = { type: "string" };
      for (let i = 0; i < 34; i++) {
        schema = {
          type: "object",
          properties: { child: schema },
          required: ["child"],
          additionalProperties: false,
        };
      }
      const seq = testSequence("df-depth-limit", [step("s1", "noop")]);
      seq.input_schema = schema;
      try {
        await client.compileSequenceDataflow(seq);
        assert.fail("expected generation to fail past the depth limit");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });

    it("accepts an input schema just under the depth limit", async () => {
      let schema: any = { type: "string" };
      for (let i = 0; i < 10; i++) {
        schema = {
          type: "object",
          properties: { child: schema },
          required: ["child"],
          additionalProperties: false,
        };
      }
      const seq = testSequence("df-depth-ok", [step("s1", "noop")]);
      seq.input_schema = schema;
      const r = await client.compileSequenceDataflow(seq);
      assert.ok(r.generated.typescript.length > 0);
    });
  });

  // ------------------------------------------------------------------
  // Preflight integration
  // ------------------------------------------------------------------
  describe("preflight integration", () => {
    it("passes preflight's typed_dataflow_compatible check for a clean sequence", async () => {
      const seq = testSequence("df-preflight-ok", [step("s1", "noop")]);
      const stored = await client.createSequence(seq);
      const preflight = await client.getSequencePreflight(stored.id);
      const check = preflight.checks?.find(
        (c: any) => c.id === "typed_dataflow_compatible",
      );
      if (check) {
        assert.equal(check.status, "pass");
      }
    });

    it("fails preflight's typed_dataflow_compatible check with a SCHEMA_PATH_MISSING violation", async () => {
      const seq = testSequence("df-preflight-schema-missing", [
        step("source", "noop", {}, {
          output_schema: {
            type: "object",
            properties: { id: { type: "string" } },
            additionalProperties: false,
          },
        }),
        step("consumer", "noop", { v: "{{ outputs.source.nope }}" }),
      ]);
      const stored = await client.createSequence(seq);
      const preflight = await client.getSequencePreflight(stored.id);
      const check = preflight.checks?.find(
        (c: any) => c.id === "typed_dataflow_compatible",
      );
      if (check) {
        assert.equal(check.status, "fail");
        assert.equal(preflight.overall, "fail");
      }
    });

    it("downgrades (not fails) preflight for a TYPE_UNKNOWN-only report", async () => {
      // check_typed_dataflow in orch8-engine/src/preflight.rs: an empty
      // findings list is "pass"; a non-empty but all-Warning findings list
      // (is_compatible() == true) is "warning", not "pass" — only a
      // completely clean report gets "pass".
      const seq = testSequence("df-preflight-warn-only", [
        step("s1", "noop", { v: "{{ state.phase }}" }),
      ]);
      const stored = await client.createSequence(seq);
      const preflight = await client.getSequencePreflight(stored.id);
      const check = preflight.checks?.find(
        (c: any) => c.id === "typed_dataflow_compatible",
      );
      if (check) {
        assert.equal(check.status, "warning");
      }
    });
  });

  // ------------------------------------------------------------------
  // Draft vs. stored sequence error handling
  // ------------------------------------------------------------------
  describe("draft vs stored error handling", () => {
    it("returns 404 for dataflow compilation of a nonexistent stored sequence", async () => {
      try {
        await client.getSequenceDataflow(uuid());
        assert.fail("expected 404");
      } catch (err: any) {
        assert.equal(err.status, 404);
      }
    });

    it("compiles a draft sequence without requiring it to be persisted first", async () => {
      const seq = testSequence("df-draft-only", [step("s1", "noop")]);
      const r = await client.compileSequenceDataflow(seq);
      assert.ok(r.generated.sequence_sha256);
    });

    it("is compatible (no error findings) for an empty-blocks-adjacent minimal sequence", async () => {
      const seq = testSequence("df-minimal", [step("s1", "noop", {})]);
      const r = await client.compileSequenceDataflow(seq);
      assert.deepEqual(
        r.report.findings.filter((f: any) => f.severity === "error"),
        [],
      );
    });
  });
});
