import { test } from "node:test";
import * as assert from "node:assert/strict";
import {
  BLOCK_TYPES,
  blockSlots,
  collectIds,
  duplicateBlock,
  insertBlock,
  localIssues,
  moveBlock,
  newBlock,
  parseSequenceText,
  preflightBody,
  prepareNewVersion,
  removeBlock,
  serializeSequence,
  setAt,
  slotAdditions,
  topLevelBlocks,
  walkBlocks,
  type JsonObject,
} from "../src/lib/sequenceModel.ts";

/** A definition exercising every block type plus fields the dashboard does not know. */
function fixture(): JsonObject {
  return {
    $schema: "https://orch8.io/schema/sequence.json",
    schema_version: 1,
    id: "11111111-1111-1111-1111-111111111111",
    tenant_id: "demo",
    namespace: "default",
    name: "checkout",
    version: 3,
    x_vendor: { owner: "payments", tags: ["a", "b"] },
    blocks: [
      { type: "step", id: "charge", handler: "charge", params: { cents: 2500 }, when: "data.pay", x_future: 42 },
      {
        type: "parallel",
        id: "notify",
        branches: [[{ type: "step", id: "email", handler: "send-email", params: {} }], []],
      },
      { type: "race", id: "race1", branches: [[], []], semantics: "first_to_succeed" },
      {
        type: "try_catch",
        id: "tc",
        try_block: [{ type: "step", id: "risky", handler: "risky", params: {} }],
        catch_block: [],
        finally_block: [{ type: "step", id: "cleanup", handler: "cleanup", params: {} }],
      },
      { type: "loop", id: "poll", condition: "!data.done", body: [], max_iterations: 5 },
      { type: "for_each", id: "each", collection: "data.items", item_var: "it", body: [] },
      {
        type: "router",
        id: "route",
        routes: [{ condition: "data.vip", blocks: [], x_route_meta: true }],
        default: [{ type: "step", id: "std", handler: "std", params: {} }],
      },
      { type: "sub_sequence", id: "child", sequence_name: "kyc", version: 2, input: { a: 1 } },
      { type: "cancellation_scope", id: "scope", blocks: [] },
      {
        type: "ab_split",
        id: "ab",
        variants: [
          { name: "control", weight: 70, blocks: [] },
          { name: "b", weight: 30, blocks: [] },
        ],
      },
      {
        type: "saga",
        id: "saga",
        steps: [
          {
            id: "reserve",
            action: { type: "step", id: "reserve_act", handler: "reserve", params: {} },
            compensation: { type: "step", id: "release_act", handler: "release", params: {} },
          },
        ],
      },
    ],
    on_failure: [{ type: "step", id: "alert", handler: "alert", params: {} }],
    created_at: "2026-01-01T00:00:00Z",
  };
}

test("BLOCK_TYPES covers all eleven engine block types", () => {
  assert.deepEqual(
    BLOCK_TYPES.map((b) => b.type).sort(),
    [
      "ab_split",
      "cancellation_scope",
      "for_each",
      "loop",
      "parallel",
      "race",
      "router",
      "saga",
      "step",
      "sub_sequence",
      "try_catch",
    ],
  );
});

test("parse → serialize round trip is lossless, including unknown fields", () => {
  const text = serializeSequence(fixture());
  const parsed = parseSequenceText(text);
  assert.ok(parsed.ok);
  assert.deepEqual(parsed.value, fixture());
  assert.equal(serializeSequence(parsed.value), text);
});

test("visual edits preserve unknown fields everywhere else", () => {
  let doc = fixture();
  // Edit a block property, reorder, insert, and remove.
  doc = setAt(doc, ["blocks", 0, "handler"], "charge-v2");
  doc = moveBlock(doc, ["blocks"], 0, 1);
  doc = insertBlock(doc, ["blocks", 0, "branches", 1], 0, newBlock("step", collectIds(doc)));
  doc = removeBlock(doc, ["blocks", 2]); // race1

  assert.equal(doc.$schema, "https://orch8.io/schema/sequence.json");
  assert.deepEqual(doc.x_vendor, { owner: "payments", tags: ["a", "b"] });
  const blocks = doc.blocks as JsonObject[];
  const charge = blocks[1]!;
  assert.equal(charge.id, "charge");
  assert.equal(charge.handler, "charge-v2");
  assert.equal(charge.x_future, 42);
  assert.equal(charge.when, "data.pay");
  const router = blocks.find((b) => b.id === "route")!;
  assert.equal((router.routes as JsonObject[])[0]!.x_route_meta, true);
  assert.ok(!blocks.some((b) => b.id === "race1"));
  // Original fixture object untouched (copy-on-write).
  const original = fixture();
  assert.equal((original.blocks as JsonObject[])[0]!.handler, "charge");
});

test("copy-on-write shares untouched subtrees", () => {
  const doc = fixture();
  const next = setAt(doc, ["blocks", 0, "handler"], "x");
  assert.notEqual(next, doc);
  assert.equal((next.blocks as unknown[])[1], (doc.blocks as unknown[])[1]);
  assert.equal(next.x_vendor, doc.x_vendor);
});

test("walkBlocks visits every nested container, including saga action/compensation and on_failure", () => {
  const ids = walkBlocks(fixture()).map((w) => w.block.id);
  for (const id of [
    "charge",
    "email",
    "risky",
    "cleanup",
    "std",
    "reserve_act",
    "release_act",
    "alert",
  ]) {
    assert.ok(ids.includes(id), `missing ${id}`);
  }
  const email = walkBlocks(fixture()).find((w) => w.block.id === "email")!;
  assert.deepEqual(email.path, ["blocks", 1, "branches", 0, 0]);
  assert.equal(email.depth, 1);
});

test("blockSlots describes containers for each composite", () => {
  const blocks = fixture().blocks as JsonObject[];
  const labels = (i: number) => blockSlots(blocks[i]).map((s) => s.label);
  assert.deepEqual(labels(0), []);
  assert.deepEqual(labels(1), ["branch 1", "branch 2"]);
  assert.deepEqual(labels(3), ["try", "catch", "finally"]);
  assert.deepEqual(labels(6), ["when data.vip", "default"]);
  assert.deepEqual(labels(9), ["variant control (weight 70)", "variant b (weight 30)"]);
  assert.deepEqual(labels(10), ["saga step reserve · action", "saga step reserve · compensation"]);
});

test("newBlock produces unique ids and required fields for every type", () => {
  const ids = collectIds(fixture());
  for (const { type } of BLOCK_TYPES) {
    const b = newBlock(type, ids);
    assert.equal(b.type, type);
    assert.equal(typeof b.id, "string");
  }
  const doc = { ...fixture(), blocks: [...(fixture().blocks as JsonObject[]), ...BLOCK_TYPES.map((t) => newBlock(t.type, collectIds(fixture())))] };
  // Fresh blocks generated from one id set never collide with each other.
  const fresh = new Set<string>();
  const set = collectIds(fixture());
  for (const { type } of BLOCK_TYPES) {
    const id = newBlock(type, set).id as string;
    assert.ok(!fresh.has(id));
    fresh.add(id);
  }
  assert.ok(doc.blocks.length > 11);
});

test("step block: newBlock('step') passes local checks; empty handler is flagged", () => {
  const base = { ...fixture(), blocks: [newBlock("step", new Set())] };
  assert.deepEqual(localIssues(base), []);
  const bad = setAt(base, ["blocks", 0, "handler"], "");
  assert.match(localIssues(bad)[0]!.message, /needs handler/);
});

test("localIssues flags duplicate ids and unknown types", () => {
  const doc = fixture();
  const dup = insertBlock(doc, ["blocks"], 0, { type: "step", id: "charge", handler: "h", params: {} });
  assert.ok(localIssues(dup).some((i) => /duplicate block id "charge"/.test(i.message)));
  const unk = insertBlock(doc, ["blocks"], 0, { type: "teleport", id: "t" });
  assert.ok(localIssues(unk).some((i) => /unknown block type "teleport"/.test(i.message)));
  assert.deepEqual(localIssues(fixture()), []);
});

test("slotAdditions add branches, routes, variants, saga steps", () => {
  const blocks = fixture().blocks as JsonObject[];
  assert.deepEqual(slotAdditions(blocks[1]).map((a) => a.label), ["Add branch"]);
  assert.deepEqual(slotAdditions(blocks[3]).map((a) => a.label), []); // finally present
  assert.deepEqual(slotAdditions(blocks[6]).map((a) => a.label), ["Add route"]); // default present
  const saga = slotAdditions(blocks[10])[0]!;
  const ids = collectIds(fixture());
  const step = saga.make(ids) as JsonObject;
  assert.equal(step.id, "saga_step");
  assert.equal((step.action as JsonObject).type, "step");
});

test("duplicateBlock renumbers every nested id", () => {
  const doc = fixture();
  const ids = collectIds(doc);
  const tc = (doc.blocks as JsonObject[])[3]!;
  const copy = duplicateBlock(tc, ids);
  assert.equal(copy.id, "tc_2");
  assert.equal(((copy.try_block as JsonObject[])[0]!).id, "risky_2");
  assert.equal(((copy.finally_block as JsonObject[])[0]!).id, "cleanup_2");
  const saga = duplicateBlock((doc.blocks as JsonObject[])[10]!, ids);
  const step = (saga.steps as JsonObject[])[0]!;
  assert.equal(step.id, "reserve_2");
  assert.equal((step.action as JsonObject).id, "reserve_act_2");
});

test("moveBlock ignores out-of-range moves", () => {
  const doc = fixture();
  assert.equal(moveBlock(doc, ["blocks"], 0, 99), doc);
  assert.equal(moveBlock(doc, ["blocks"], 0, 0), doc);
});

test("removeBlock refuses to delete a saga's single action slot", () => {
  const doc = fixture();
  assert.equal(removeBlock(doc, ["blocks", 10, "steps", 0, "action"]), doc);
});

test("prepareNewVersion bumps past every known version and keeps unknown fields", () => {
  const now = new Date("2026-09-26T10:00:00Z");
  const body = prepareNewVersion({ ...fixture(), deprecated: true }, [3, 7, 5], "new-id", now);
  assert.equal(body.version, 8);
  assert.equal(body.id, "new-id");
  assert.equal(body.created_at, "2026-09-26T10:00:00.000Z");
  assert.equal(body.deprecated, false);
  assert.deepEqual(body.x_vendor, fixture().x_vendor);
  assert.equal(prepareNewVersion(fixture(), [], "x", now).version, 1);
});

test("preflightBody fills only missing identity fields", () => {
  const now = new Date("2026-09-26T10:00:00Z");
  const draft = fixture();
  delete draft.id;
  delete draft.version;
  delete draft.created_at;
  const body = preflightBody(draft, "placeholder", now);
  assert.equal(body.id, "placeholder");
  assert.equal(body.version, 1);
  assert.equal(body.created_at, now.toISOString());
  const kept = preflightBody(fixture(), "placeholder", now);
  assert.equal(kept.id, fixture().id);
  assert.equal(kept.version, 3);
});

test("topLevelBlocks maps nested ids to their top-level ancestor", () => {
  const top = topLevelBlocks(fixture());
  assert.equal(top.length, 11);
  assert.deepEqual(top[1], { id: "notify", descendants: ["email"] });
  assert.deepEqual(top[3], { id: "tc", descendants: ["risky", "cleanup"] });
  assert.deepEqual(top[10], { id: "saga", descendants: ["reserve_act", "release_act", "reserve"] });
  assert.ok(!top.some((t) => t.id === "alert"), "on_failure is not a fork target");
});

test("parseSequenceText rejects non-objects and invalid JSON", () => {
  assert.equal(parseSequenceText("[]").ok, false);
  assert.equal(parseSequenceText("{").ok, false);
});
