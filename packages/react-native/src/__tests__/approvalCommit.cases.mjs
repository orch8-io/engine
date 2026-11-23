import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { promisify } from "node:util";
import { SqliteApprovalCommitAdapter } from "../approvalCommit.ts";
import { sqliteAdapter } from "./sqliteFixture.mjs";

const approval = {
  id: "approval-1",
  revision: 6,
  status: "pending",
  title: "Publish",
  prompt: "Publish the revision?",
  choices: [{ label: "Allow", value: "allow" }, { label: "Reject", value: "reject" }],
};

function decision(value, key = "idempotency") {
  return { approvalId: approval.id, expectedRevision: approval.revision, value, idempotencyKey: key };
}

test("approval commit checks revision, scope, allowed choice, and deadline", async () => {
  const connection = new DatabaseSync(":memory:");
  try {
    const catalog = await SqliteApprovalCommitAdapter.open(sqliteAdapter(connection), "account/catalog");
    const other = await SqliteApprovalCommitAdapter.open(sqliteAdapter(connection), "another/catalog");
    await catalog.recordApproval(approval);
    await catalog.recordApproval({ ...approval, revision: 5, status: "resolved" });
    assert.equal(await catalog.commit({ ...decision("allow"), expectedRevision: 5 }), "stale");
    assert.equal(await catalog.commit(decision("unlisted")), "stale");
    assert.equal(await other.commit(decision("allow")), "stale");

    await catalog.recordApproval({ ...approval, expiresAtMs: Date.now() - 1, revision: 7 });
    assert.equal(await catalog.commit({ ...decision("allow"), expectedRevision: 7 }), "stale");
    await catalog.recordApproval({ ...approval, revision: 8 });
    const valid = { ...decision("allow"), expectedRevision: 8 };
    assert.equal(await catalog.commit(valid), "accepted");
    assert.equal(await catalog.commit(valid), "accepted");
    assert.equal(await catalog.commit({ ...valid, value: "reject" }), "stale");
    await catalog.recordApproval({ ...approval, revision: 9 });
    assert.equal((await catalog.getDecision(approval.id)).expectedRevision, 8);
  } finally {
    connection.close();
  }
});

test("two processes commit one approval decision and the winner can retry after reopening", async () => {
  const directory = await mkdtemp(join(tmpdir(), "orch8-approval-"));
  try {
    const path = join(directory, "approvals.db");
    const first = new DatabaseSync(path);
    try {
      const adapter = await SqliteApprovalCommitAdapter.open(sqliteAdapter(first), "account/catalog");
      await adapter.recordApproval(approval);
    } finally {
      first.close();
    }

    const script = `
      import { DatabaseSync } from "node:sqlite";
      import { SqliteApprovalCommitAdapter } from ${JSON.stringify(new URL("../approvalCommit.ts", import.meta.url).href)};
      const db = new DatabaseSync(process.argv[1]);
      db.exec("PRAGMA busy_timeout=5000");
      const adapter = await SqliteApprovalCommitAdapter.open((${sqliteAdapter.toString()})(db), "account/catalog");
      console.log(await adapter.commit(JSON.parse(process.argv[2])));
      db.close();
    `;
    const decisions = [decision("allow", "submit-a"), decision("reject", "submit-b")];
    const result = await Promise.all(decisions.map((submission) => promisify(execFile)(process.execPath, [
      "--experimental-strip-types", "--input-type=module", "--eval", script, path, JSON.stringify(submission),
    ])));
    const outcomes = result.map(({ stdout }) => stdout.trim());
    assert.deepEqual([...outcomes].sort(), ["accepted", "stale"]);

    const reopened = new DatabaseSync(path);
    try {
      const adapter = await SqliteApprovalCommitAdapter.open(sqliteAdapter(reopened), "account/catalog");
      const persisted = await adapter.getDecision(approval.id);
      const winner = decisions[outcomes.indexOf("accepted")];
      assert.equal(persisted.value, winner.value);
      assert.equal(persisted.idempotencyKey, winner.idempotencyKey);
      assert.equal(await adapter.commit(winner), "accepted");
      assert.equal(reopened.prepare("SELECT COUNT(*) AS count FROM orch8_approvals WHERE decision_key IS NOT NULL").get().count, 1);
    } finally {
      reopened.close();
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("a full SQLite database preserves saved decisions and admits an exact retry after space is reclaimed", async () => {
  const directory = await mkdtemp(join(tmpdir(), "orch8-approval-full-"));
  try {
    const path = join(directory, "approvals.db");
    const secondApproval = { ...approval, id: "needs-space" };
    const retry = { ...decision("allow", "recovery-key-" + "x".repeat(16_384)), approvalId: secondApproval.id };
    const first = new DatabaseSync(path);
    let cap;
    try {
      first.exec("PRAGMA page_size=1024");
      const adapter = await SqliteApprovalCommitAdapter.open(sqliteAdapter(first), "account/catalog");
      await adapter.recordApproval(approval);
      await adapter.recordApproval(secondApproval);
      assert.equal(await adapter.commit(decision("allow", "saved-before-full")), "accepted");

      first.exec("CREATE TABLE reclaimable (payload BLOB); INSERT INTO reclaimable VALUES (zeroblob(65536));");
      cap = first.prepare("PRAGMA page_count").get().page_count;
      first.exec(`PRAGMA max_page_count=${cap}`);
      assert.equal(await adapter.commit(retry), "storage_full");
      assert.equal(await adapter.getDecision(secondApproval.id), null);
    } finally {
      first.close();
    }

    const reopened = new DatabaseSync(path);
    try {
      reopened.exec(`PRAGMA max_page_count=${cap}`);
      const adapter = await SqliteApprovalCommitAdapter.open(sqliteAdapter(reopened), "account/catalog");
      assert.equal((await adapter.getDecision(approval.id)).idempotencyKey, "saved-before-full");
      assert.equal(await adapter.getDecision(secondApproval.id), null);
      reopened.exec("DELETE FROM reclaimable");
      assert.equal(await adapter.commit(retry), "accepted");
      assert.equal((await adapter.getDecision(secondApproval.id)).idempotencyKey, retry.idempotencyKey);
    } finally {
      reopened.close();
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
