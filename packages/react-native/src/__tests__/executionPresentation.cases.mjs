import assert from "node:assert/strict";
import test from "node:test";
import {
  ApprovalDecisionLedger, approvalExpired, canDecide, visibleApprovals,
} from "../executionPresentation.ts";

const gate = {
  id: "inspection-approval",
  revision: 1,
  status: "pending",
  expiresAtMs: 200,
  title: "Inspection",
  prompt: "Is the inspection ready?",
  choices: [{ label: "Approve", value: "yes" }, { label: "Reject", value: "no" }],
};

test("deduplicates repeated activations using the latest revision and terminal precedence", () => {
  const newer = { ...gate, revision: 2, title: "Updated" };
  const historical = { ...gate, status: "expired" };
  const other = { ...gate, id: "different-activation" };
  assert.deepEqual(visibleApprovals([gate, newer, newer, historical, other]), [newer, other]);

  const sameRevisionResolved = { ...gate, status: "resolved" };
  assert.deepEqual(visibleApprovals([gate, historical, sameRevisionResolved]), []);
  assert.deepEqual(visibleApprovals([sameRevisionResolved, historical, gate]), []);
});

test("expires at the exact deadline and rejects malformed deadlines and choices", () => {
  assert.equal(approvalExpired(gate, 199), false);
  assert.equal(approvalExpired(gate, 200), true);
  assert.equal(canDecide({ ...gate, expiresAtMs: NaN }, 0), false);
  assert.equal(canDecide({ ...gate, expiresAtMs: Infinity }, 0), false);
  assert.equal(canDecide({ ...gate, expiresAtMs: 9e15 }, 0), false);
  assert.equal(canDecide({ ...gate, status: "expired", expiresAtMs: null }, 0), false);
  assert.equal(canDecide({ ...gate, choices: [gate.choices[0], gate.choices[0]] }, 0), false);
  assert.equal(new ApprovalDecisionLedger("scope").begin(gate, "yes", 200), null);
});

test("blocks duplicate taps and preserves the choice and idempotency key on an ambiguous retry", () => {
  const ledger = new ApprovalDecisionLedger("catalog");
  const first = ledger.begin(gate, "yes", 100);
  assert.equal(first.expectedRevision, 1);
  assert.equal(ledger.begin(gate, "yes", 100), null);
  assert.equal(ledger.begin(gate, "no", 100), null);
  ledger.finish(first, "retry");
  assert.equal(ledger.begin(gate, "no", 100), null);
  const retry = ledger.begin(gate, "yes", 100);
  assert.deepEqual(retry, first);
  ledger.finish(retry, "accepted");
  assert.equal(ledger.begin({ ...gate, revision: 2 }, "yes", 100), null);

  const restarted = new ApprovalDecisionLedger("catalog").begin(gate, "yes", 100);
  const differentScope = new ApprovalDecisionLedger("other").begin(gate, "yes", 100);
  assert.equal(restarted.idempotencyKey, first.idempotencyKey);
  assert.notEqual(differentScope.idempotencyKey, first.idempotencyKey);
});

test("refreshes stale revisions and binds submissions to the new expected revision", () => {
  const ledger = new ApprovalDecisionLedger("catalog");
  const first = ledger.begin(gate, "no", 100);
  ledger.finish(first, "stale");
  assert.equal(ledger.begin(gate, "no", 100), null);
  const refreshed = ledger.begin({ ...gate, revision: 2 }, "yes", 100);
  assert.equal(refreshed.expectedRevision, 2);
  assert.equal(refreshed.idempotencyKey, first.idempotencyKey);
});
