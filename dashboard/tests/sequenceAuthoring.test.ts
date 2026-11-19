import assert from "node:assert/strict";
import test from "node:test";
import { draftInputSchema, referencePaths, replayDivergences, safeTimelineEntry, sequenceDependencies } from "../src/lib/sequenceAuthoring.ts";
import type { JsonObject } from "../src/lib/sequenceModel.ts";

test("infers JSON Schema without retaining sample values or implicitly requiring fields", () => {
  const schema = draftInputSchema({
    id: "sample-secret",
    amount: 1.5,
    notes: [null, 3, 8],
    extra: { accepted: true },
    empty: [],
  }, new Set(["id", "unknown"]));
  assert.deepEqual(schema, {
    type: "object",
    required: ["id"],
    properties: {
      id: { type: "string" },
      amount: { type: "number" },
      notes: { type: "array", items: { anyOf: [{ type: "null" }, { type: "integer" }] } },
      extra: { type: "object", properties: { accepted: { type: "boolean" } } },
      empty: { type: "array" },
    },
  });
  assert.equal(draftInputSchema({ id: "a" }, new Set()).required, undefined);
  assert.throws(() => draftInputSchema([], new Set()), /JSON object/);
});

test("reference suggestions group input schema fields and prior outputs in lexical scope", () => {
  const doc: JsonObject = {
    input_schema: { properties: { customer: { properties: { id: { type: "string" } } } } },
    blocks: [
      { type: "step", id: "fetch", output_schema: { properties: { result: { type: "number" }, "with.dot": {} } } },
      { type: "parallel", id: "parallel", branches: [
        [{ type: "step", id: "convert" }, { type: "step", id: "selected" }],
        [{ type: "step", id: "other-branch" }],
      ] },
      { type: "step", id: "future" },
    ],
  };
  assert.deepEqual(referencePaths(doc, ["blocks", 1, "branches", 0, 1]), [
    "context.data", "context.data.customer", "context.data.customer.id",
    "outputs.fetch", "outputs.fetch.result", "outputs.convert",
  ]);
});

test("distinguishes pinned and floating dependencies in nested blocks and cleanup", () => {
  assert.deepEqual(sequenceDependencies({
    blocks: [{ type: "try_catch", id: "outer", try_block: [{ type: "sub_sequence", id: "invoice", sequence_name: "build", version: 2 }] }],
    on_failure: [{ type: "sub_sequence", id: "recovery", sequence_name: "recover" }],
  }), [
    { blockId: "invoice", name: "build", version: 2 },
    { blockId: "recovery", name: "recover", version: null },
  ]);
});

test("redacted timeline entries keep recognized sentinels and omit all payloads and artifact references", () => {
  const source = {
    block_id: "dispatch", attempt: 2, completed_at: "2026-11-18T18:00:00Z",
    output: { token: "private" }, output_ref: "private-artifact-path", is_sentinel: false,
  };
  const safe = safeTimelineEntry(source);
  assert.equal(safe.output, undefined);
  assert.equal(safe.output_ref, undefined);
  assert.equal(safe.attempt, 2);
  assert.equal(safeTimelineEntry({ ...source, is_sentinel: true, output_ref: "__retry__" }).output_ref, "__retry__");
  assert.equal(safeTimelineEntry({ ...source, is_sentinel: true }).output_ref, undefined);
});

test("replay divergence projects only supported evidence from persisted validation summaries", () => {
  assert.deepEqual(replayDivergences({ divergences: [
    { instance_id: "historical", recorded_state: "failed", replayed_state: "completed", blocks_added: ["new", 1], blocks_removed: ["retired"], private_field: "ignored" },
    { instance_id: "malformed" },
  ] }), [
    { instanceId: "historical", recordedState: "failed", replayedState: "completed", added: ["new"], removed: ["retired"] },
  ]);
});
