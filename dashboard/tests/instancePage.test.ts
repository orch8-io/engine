import assert from "node:assert/strict";
import { test } from "node:test";
import { decodeInstancePage } from "../src/lib/instancePage.ts";

const item = {
  id: "run-1",
  sequence_id: "sequence-1",
  tenant_id: "tenant-1",
  namespace: "default",
  state: "scheduled",
  next_fire_at: null,
  priority: "Normal",
  timezone: "UTC",
  metadata: null,
  context: {},
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
};

test("decodes both the current execution page and legacy arrays", () => {
  assert.deepEqual(decodeInstancePage({ items: [item], has_more: true }), { items: [item], has_more: true });
  assert.deepEqual(decodeInstancePage([item]), { items: [item], has_more: false });
});

test("rejects malformed envelopes and reports the first malformed row", () => {
  assert.throws(() => decodeInstancePage({ items: [], has_more: "false" }), /execution page/);
  assert.throws(() => decodeInstancePage({ items: [item, { id: "broken" }], has_more: false }), /execution at index 1/);
});
