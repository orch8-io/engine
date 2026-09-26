import { test } from "node:test";
import * as assert from "node:assert/strict";
import {
  blocksAt,
  clampPosition,
  contextAt,
  forkBlockForPosition,
  instanceStateAt,
  outputsAt,
  pointTime,
  topLevelIndex,
  type TimelineEntryDto,
} from "../src/lib/timeTravel.ts";

const entries: TimelineEntryDto[] = [
  { block_id: "fetch", attempt: 0, completed_at: "2026-09-01T10:00:01Z", output: { n: 1 }, is_sentinel: false },
  { block_id: "charge", attempt: 0, completed_at: "2026-09-01T10:00:02Z", output_ref: "__in_progress__", is_sentinel: true },
  { block_id: "charge", attempt: 1, completed_at: "2026-09-01T10:00:03Z", output_ref: "__retry__", is_sentinel: true },
  { block_id: "charge", attempt: 1, completed_at: "2026-09-01T10:00:04Z", output: { ok: true }, is_sentinel: false },
  { block_id: "email", attempt: 0, completed_at: "2026-09-01T10:00:05Z", output: null, is_sentinel: false },
];

test("clampPosition keeps positions within the timeline", () => {
  assert.equal(clampPosition(-3, 5), 0);
  assert.equal(clampPosition(9, 5), 5);
  assert.equal(clampPosition(2.7, 5), 2);
  assert.equal(clampPosition(Number.NaN, 5), 5);
});

test("blocksAt derives states, including retries and in-progress sentinels", () => {
  assert.deepEqual(blocksAt(entries, 0, ["fetch"]).map((b) => [b.block_id, b.state]), [["fetch", "pending"]]);
  assert.deepEqual(
    blocksAt(entries, 2).map((b) => [b.block_id, b.state]),
    [["fetch", "completed"], ["charge", "running"]],
  );
  assert.deepEqual(blocksAt(entries, 3).find((b) => b.block_id === "charge")!.state, "retrying");
  const at4 = blocksAt(entries, 4, ["fetch", "charge", "email", "later"]);
  assert.deepEqual(
    at4.map((b) => [b.block_id, b.state]),
    [["fetch", "completed"], ["charge", "completed"], ["email", "pending"], ["later", "pending"]],
  );
  assert.equal(at4[1]!.attempt, 1);
});

test("outputsAt returns only what had been produced by the point", () => {
  assert.deepEqual(outputsAt(entries, 0), {});
  assert.deepEqual(outputsAt(entries, 2), { fetch: { n: 1 } });
  assert.deepEqual(outputsAt(entries, 5), { fetch: { n: 1 }, charge: { ok: true }, email: null });
});

test("pointTime uses creation time at position 0", () => {
  assert.equal(pointTime(entries, 0, "2026-09-01T10:00:00Z"), "2026-09-01T10:00:00Z");
  assert.equal(pointTime(entries, 3, "x"), "2026-09-01T10:00:03Z");
});

test("instanceStateAt replays recorded transitions", () => {
  const tr = [
    { from_state: "scheduled", to_state: "running", at: "2026-09-01T10:00:00.5Z" },
    { from_state: "running", to_state: "waiting", at: "2026-09-01T10:00:03.5Z" },
  ];
  assert.equal(instanceStateAt(tr, "2026-09-01T10:00:00Z"), null);
  assert.equal(instanceStateAt(tr, "2026-09-01T10:00:02Z"), "running");
  assert.equal(instanceStateAt(tr, "2026-09-01T10:00:04Z"), "waiting");
});

test("contextAt prefers live context at the end, else the latest earlier checkpoint", () => {
  const cps = [
    { id: "c1", created_at: "2026-09-01T10:00:01.5Z", checkpoint_data: { data: { step: 1 } } },
    { id: "c2", created_at: "2026-09-01T10:00:04.5Z", checkpoint_data: { data: { step: 2 } } },
  ];
  assert.deepEqual(contextAt(cps, "2026-09-01T10:00:05Z", true, { live: 1 }), { source: "current", context: { live: 1 } });
  assert.deepEqual(contextAt(cps, "2026-09-01T10:00:03Z", false, {}), {
    source: "checkpoint",
    context: { data: { step: 1 } },
    checkpointId: "c1",
    checkpointAt: "2026-09-01T10:00:01.5Z",
  });
  assert.deepEqual(contextAt(cps, "2026-09-01T10:00:00Z", false, {}), { source: "none" });
});

test("forkBlockForPosition targets the top-level block of the next entry", () => {
  const topOf = topLevelIndex([
    { id: "fetch", descendants: [] },
    { id: "pay", descendants: ["charge"] },
    { id: "notify", descendants: ["email"] },
  ]);
  const order = ["fetch", "pay", "notify"];
  assert.equal(forkBlockForPosition(entries, 0, topOf, order), "fetch");
  assert.equal(forkBlockForPosition(entries, 1, topOf, order), "pay");
  assert.equal(forkBlockForPosition(entries, 4, topOf, order), "notify");
  // End of timeline: fork re-runs the last top-level block.
  assert.equal(forkBlockForPosition(entries, 5, topOf, order), "notify");
  assert.equal(forkBlockForPosition([], 0, topOf, order), "fetch");
  assert.equal(forkBlockForPosition([{ ...entries[0]!, block_id: "_sla:x" }], 1, topOf, order), null);
});
