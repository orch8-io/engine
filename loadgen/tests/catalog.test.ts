import { test } from "node:test";
import * as assert from "node:assert/strict";
import { ALL_TEMPLATES, buildSequencesFor, uniqueHandlers } from "../src/catalog/index.ts";
import { Rng } from "../src/lib/rng.ts";
import { orderFulfillment } from "../src/catalog/orderFulfillment.ts";
import { approvalFlow } from "../src/catalog/approvalFlow.ts";
import { dataPipeline } from "../src/catalog/dataPipeline.ts";
import { etaEscalation } from "../src/catalog/etaEscalation.ts";
import { abSplitRollout } from "../src/catalog/abSplitRollout.ts";
import { loopAggregator } from "../src/catalog/loopAggregator.ts";
import { raceFetch } from "../src/catalog/raceFetch.ts";
import { nestedSubsequence } from "../src/catalog/nestedSubsequence.ts";
import { llmChain } from "../src/catalog/llmChain.ts";

// ── helpers ──────────────────────────────────────────────────────────

function rng(seed = 42): Rng {
  return new Rng(seed);
}

// Recursively find blocks (and nested blocks) of a given type.
function findBlocks(blocks: Record<string, unknown>[], type: string): Record<string, unknown>[] {
  const found: Record<string, unknown>[] = [];
  for (const b of blocks) {
    if (b.type === type) found.push(b);
  }
  return found;
}

// ══════════════════════════════════════════════════════════════════════
//  Catalog index tests (1-10)
// ══════════════════════════════════════════════════════════════════════

test("1 - ALL_TEMPLATES has 9 templates", () => {
  assert.equal(ALL_TEMPLATES.length, 9);
});

test("2 - ALL_TEMPLATES names are unique", () => {
  const names = ALL_TEMPLATES.map((t) => t.name);
  assert.equal(new Set(names).size, names.length);
});

test("3 - buildSequencesFor creates one sequence per template", () => {
  const seqs = buildSequencesFor("tenant-1", "ns", rng(), ALL_TEMPLATES);
  assert.equal(seqs.length, ALL_TEMPLATES.length);
});

test("4 - buildSequencesFor assigns correct tenant_id", () => {
  const seqs = buildSequencesFor("acme", "prod", rng(), ALL_TEMPLATES);
  for (const s of seqs) {
    assert.equal(s.tenant_id, "acme");
  }
});

test("5 - buildSequencesFor assigns correct namespace", () => {
  const seqs = buildSequencesFor("acme", "staging", rng(), ALL_TEMPLATES);
  for (const s of seqs) {
    assert.equal(s.namespace, "staging");
  }
});

test("6 - buildSequencesFor generates unique IDs (with Rng)", () => {
  const seqs = buildSequencesFor("t", "n", rng(), ALL_TEMPLATES);
  const ids = seqs.map((s) => s.id);
  assert.equal(new Set(ids).size, ids.length, "IDs are not unique");
});

test("7 - uniqueHandlers collects all handler names", () => {
  const handlers = uniqueHandlers(ALL_TEMPLATES);
  // orderFulfillment alone contributes 5 unique handlers
  for (const h of orderFulfillment.handlers) {
    assert.ok(handlers.includes(h), `missing handler: ${h}`);
  }
});

test("8 - uniqueHandlers deduplicates shared handlers", () => {
  // Fabricate two templates that share a handler name
  const fake = [
    { name: "a", handlers: ["shared", "unique_a"], weight: 1, buildBlocks: () => [], buildContext: () => ({}) },
    { name: "b", handlers: ["shared", "unique_b"], weight: 1, buildBlocks: () => [], buildContext: () => ({}) },
  ] as const;
  const handlers = uniqueHandlers(fake);
  assert.equal(handlers.filter((h) => h === "shared").length, 1);
  assert.equal(handlers.length, 3); // shared, unique_a, unique_b
});

test("9 - uniqueHandlers with no templates returns empty", () => {
  assert.deepEqual(uniqueHandlers([]), []);
});

test("10 - buildSequencesFor with empty templates returns empty", () => {
  assert.deepEqual(buildSequencesFor("t", "n", rng(), []), []);
});

// ══════════════════════════════════════════════════════════════════════
//  orderFulfillment (11-14)
// ══════════════════════════════════════════════════════════════════════

test("13 - orderFulfillment buildBlocks returns blocks starting with parallel", () => {
  const blocks = orderFulfillment.buildBlocks(rng());
  assert.ok(blocks.length >= 1, "expected at least one block");
  assert.equal(blocks[0]!.type, "parallel");
});

test("14 - orderFulfillment buildContext returns data with order_id, amount_cents, currency", () => {
  const ctx = orderFulfillment.buildContext(rng()) as { data: Record<string, unknown> };
  assert.ok(ctx.data, "missing data key");
  assert.ok("order_id" in ctx.data, "missing order_id");
  assert.ok("amount_cents" in ctx.data, "missing amount_cents");
  assert.ok("currency" in ctx.data, "missing currency");
});

// ══════════════════════════════════════════════════════════════════════
//  approvalFlow (15-18)
// ══════════════════════════════════════════════════════════════════════

test("17 - approvalFlow buildBlocks includes router block", () => {
  const blocks = approvalFlow.buildBlocks(rng());
  const routers = findBlocks(blocks as Record<string, unknown>[], "router");
  assert.ok(routers.length >= 1, "no router block found");
});

test("18 - approvalFlow buildContext returns data with request_id, requester", () => {
  const ctx = approvalFlow.buildContext(rng()) as { data: Record<string, unknown> };
  assert.ok(ctx.data, "missing data key");
  assert.ok("request_id" in ctx.data, "missing request_id");
  assert.ok("requester" in ctx.data, "missing requester");
});

// ══════════════════════════════════════════════════════════════════════
//  dataPipeline (19-21)
// ══════════════════════════════════════════════════════════════════════

test("20 - dataPipeline buildBlocks includes for_each block", () => {
  const blocks = dataPipeline.buildBlocks(rng());
  const forEach = findBlocks(blocks as Record<string, unknown>[], "for_each");
  assert.ok(forEach.length >= 1, "no for_each block found");
});

test("21 - dataPipeline buildContext returns data.items as array", () => {
  const ctx = dataPipeline.buildContext(rng()) as { data: { items: unknown } };
  assert.ok(ctx.data, "missing data key");
  assert.ok(Array.isArray(ctx.data.items), "items should be an array");
  assert.ok(ctx.data.items.length >= 10, "items should have at least 10 elements");
});

// ══════════════════════════════════════════════════════════════════════
//  etaEscalation (22-24)
// ══════════════════════════════════════════════════════════════════════

test("23 - etaEscalation buildBlocks includes try_catch block", () => {
  const blocks = etaEscalation.buildBlocks(rng());
  const tryCatch = findBlocks(blocks as Record<string, unknown>[], "try_catch");
  assert.ok(tryCatch.length >= 1, "no try_catch block found");
});

test("24 - etaEscalation buildContext returns data with task_id", () => {
  const ctx = etaEscalation.buildContext(rng()) as { data: Record<string, unknown> };
  assert.ok(ctx.data, "missing data key");
  assert.ok("task_id" in ctx.data, "missing task_id");
});

// ══════════════════════════════════════════════════════════════════════
//  abSplitRollout (25-27)
// ══════════════════════════════════════════════════════════════════════

test("26 - abSplitRollout buildBlocks includes a_b_split block", () => {
  const blocks = abSplitRollout.buildBlocks(rng());
  const splits = findBlocks(blocks as Record<string, unknown>[], "a_b_split");
  assert.ok(splits.length >= 1, "no a_b_split block found");
});

test("27 - abSplitRollout buildContext returns data with experiment_id", () => {
  const ctx = abSplitRollout.buildContext(rng()) as { data: Record<string, unknown> };
  assert.ok(ctx.data, "missing data key");
  assert.ok("experiment_id" in ctx.data, "missing experiment_id");
});

// ══════════════════════════════════════════════════════════════════════
//  loopAggregator (28-30)
// ══════════════════════════════════════════════════════════════════════

test("29 - loopAggregator buildBlocks includes loop block", () => {
  const blocks = loopAggregator.buildBlocks(rng());
  const loops = findBlocks(blocks as Record<string, unknown>[], "loop");
  assert.ok(loops.length >= 1, "no loop block found");
});

test("30 - loopAggregator buildContext returns data with remaining", () => {
  const ctx = loopAggregator.buildContext(rng()) as { data: Record<string, unknown> };
  assert.ok(ctx.data, "missing data key");
  assert.ok("remaining" in ctx.data, "missing remaining");
  assert.equal(typeof ctx.data.remaining, "number");
});

// ══════════════════════════════════════════════════════════════════════
//  raceFetch (31-33)
// ══════════════════════════════════════════════════════════════════════

test("32 - raceFetch buildBlocks includes race block with 3 branches", () => {
  const blocks = raceFetch.buildBlocks(rng());
  const races = findBlocks(blocks as Record<string, unknown>[], "race");
  assert.ok(races.length >= 1, "no race block found");
  const raceBlock = races[0]!;
  const branches = raceBlock.branches as unknown[];
  assert.equal(branches.length, 3, "expected 3 branches in race block");
});

test("33 - raceFetch buildContext returns data with request_id", () => {
  const ctx = raceFetch.buildContext(rng()) as { data: Record<string, unknown> };
  assert.ok(ctx.data, "missing data key");
  assert.ok("request_id" in ctx.data, "missing request_id");
});

// ══════════════════════════════════════════════════════════════════════
//  nestedSubsequence (34-36)
// ══════════════════════════════════════════════════════════════════════

test("35 - nestedSubsequence buildBlocks includes sub_sequence block", () => {
  const blocks = nestedSubsequence.buildBlocks(rng());
  const subs = findBlocks(blocks as Record<string, unknown>[], "sub_sequence");
  assert.ok(subs.length >= 1, "no sub_sequence block found");
});

test("36 - nestedSubsequence buildContext returns data with parent_id", () => {
  const ctx = nestedSubsequence.buildContext(rng()) as { data: Record<string, unknown> };
  assert.ok(ctx.data, "missing data key");
  assert.ok("parent_id" in ctx.data, "missing parent_id");
});

// ══════════════════════════════════════════════════════════════════════
//  llmChain (37-40)
// ══════════════════════════════════════════════════════════════════════

test("40 - llmChain buildBlocks includes llm_call handler steps", () => {
  const blocks = llmChain.buildBlocks(rng());
  const steps = findBlocks(blocks as Record<string, unknown>[], "step");
  assert.ok(steps.length >= 2, "expected at least 2 step blocks");
  const llmSteps = steps.filter((s) => s.handler === "llm_call");
  assert.ok(llmSteps.length >= 2, `expected at least 2 llm_call steps, found ${llmSteps.length}`);
});
