import { test } from "node:test";
import * as assert from "node:assert/strict";
import { BLOCK_SCHEMAS, coerceField, fieldText, schemaFields } from "../src/lib/schemaForm.ts";

test("schemaFields maps JSON Schema property types to form kinds", () => {
  const fields = schemaFields({
    type: "object",
    required: ["email"],
    properties: {
      email: { type: "string", title: "Email", description: "who" },
      count: { type: "integer" },
      ratio: { type: ["number", "null"] },
      vip: { type: "boolean" },
      tier: { type: "string", enum: ["gold", "silver"] },
      meta: { type: "object" },
      tags: { type: "array" },
      untyped: {},
    },
  });
  assert.deepEqual(
    fields.map((f) => [f.key, f.kind, f.required]),
    [
      ["email", "string", true],
      ["count", "integer", false],
      ["ratio", "number", false],
      ["vip", "boolean", false],
      ["tier", "enum", false],
      ["meta", "json", false],
      ["tags", "json", false],
      ["untyped", "json", false],
    ],
  );
  assert.equal(fields[0]!.title, "Email");
  assert.deepEqual(fields[4]!.options, ["gold", "silver"]);
});

test("schemaFields tolerates non-object schemas", () => {
  assert.deepEqual(schemaFields(null), []);
  assert.deepEqual(schemaFields({ type: "string" }), []);
});

test("coerceField converts and validates raw input", () => {
  const [str, int, num, bool, json] = schemaFields({
    type: "object",
    required: ["s"],
    properties: { s: { type: "string" }, i: { type: "integer" }, n: { type: "number" }, b: { type: "boolean" }, j: { type: "object" } },
  });
  assert.deepEqual(coerceField(str!, "hi"), { ok: true, value: "hi", remove: false });
  assert.deepEqual(coerceField(str!, ""), { ok: true, value: "", remove: false });
  assert.deepEqual(coerceField(int!, "5"), { ok: true, value: 5, remove: false });
  assert.equal(coerceField(int!, "5.5").ok, false);
  assert.deepEqual(coerceField(int!, ""), { ok: true, value: undefined, remove: true });
  assert.deepEqual(coerceField(num!, "0.25"), { ok: true, value: 0.25, remove: false });
  assert.equal(coerceField(num!, "abc").ok, false);
  assert.deepEqual(coerceField(bool!, true), { ok: true, value: true, remove: false });
  assert.deepEqual(coerceField(json!, '{"a":1}'), { ok: true, value: { a: 1 }, remove: false });
  assert.equal(coerceField(json!, "{").ok, false);
});

test("fieldText renders stored values", () => {
  const [j, s] = schemaFields({ type: "object", properties: { j: { type: "object" }, s: { type: "string" } } });
  assert.equal(fieldText(j!, { a: 1 }), '{\n  "a": 1\n}');
  assert.equal(fieldText(s!, undefined), "");
  assert.equal(fieldText(s!, "x"), "x");
});

test("every block type has a property schema whose first field is id", () => {
  for (const type of [
    "step",
    "parallel",
    "race",
    "loop",
    "for_each",
    "router",
    "try_catch",
    "sub_sequence",
    "ab_split",
    "cancellation_scope",
    "saga",
  ]) {
    const fields = schemaFields(BLOCK_SCHEMAS[type]);
    assert.equal(fields[0]!.key, "id", type);
    assert.ok(fields[0]!.required, type);
  }
  assert.ok(schemaFields(BLOCK_SCHEMAS.step).some((f) => f.key === "when"), "step exposes when");
});
