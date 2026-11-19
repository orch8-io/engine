import assert from "node:assert/strict";
import test from "node:test";
import { parseReplayFixtureSelection } from "../src/lib/replayFixtureSelection.ts";

test("fixture fields are omitted by default and repeated simple fields are deduplicated", () => {
  assert.deepEqual(parseReplayFixtureSelection("", ""), { input_keys: [], output_keys: {} });
  assert.deepEqual(parseReplayFixtureSelection(" account , account", "check.status, check.reason, check.status"), {
    input_keys: ["account"], output_keys: { check: ["status", "reason"] },
  });
});

test("exact JSON selections preserve punctuation and prototype-like block IDs", () => {
  const selection = parseReplayFixtureSelection(
    '["full,name"," space "]', '{"review.final":["score.total","a,b"],"__proto__":["status"],"unused":[]}',
  );
  assert.deepEqual(selection.input_keys, ["full,name", " space "]);
  assert.deepEqual(Object.entries(selection.output_keys), [
    ["review.final", ["score.total", "a,b"]], ["__proto__", ["status"]],
  ]);
  assert.equal(Object.getPrototypeOf(selection.output_keys), Object.prototype);
});

test("invalid selections cannot be silently converted into different field keys", () => {
  for (const [input, output] of [
    ["[]", "no-separator"], ["[]", "block."], ["[1]", ""],
    ['{"field":"value"}', ""], ["[]", '["step.score"]'], ["[]", '{"step":"score"}'], ["[]", '{"step":[null]}'],
    ["[]", '{"":["score"]}'], ["[]", '{"step":[""]}'],
  ]) assert.throws(() => parseReplayFixtureSelection(input, output));
});
