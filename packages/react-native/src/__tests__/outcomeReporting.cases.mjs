import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { AccountOutcomeReporter, SqliteAccountOutcomeJournal } from "../outcomeReporting.ts";
import { sqliteAdapter } from "./sqliteFixture.mjs";

const alice = { accountId: "alice", catalogId: "catalog" };
const bob = { accountId: "bob", catalogId: "catalog" };
const completion = { submissionId: "same-offline-key", revision: "r1", outputJson: '{"owner":"alice"}' };

test("mismatched credentials and receipts leave the original outcome pending", async () => {
  const database = new DatabaseSync(":memory:");
  try {
    const journal = await SqliteAccountOutcomeJournal.open(sqliteAdapter(database));
    await journal.enqueue(alice, completion);
    await assert.rejects(journal.enqueue(alice, { ...completion, outputJson: "null" }), /different data/);
    const wrong = new AccountOutcomeReporter(
      alice, journal, async () => ({ ...bob, bearer: "bob-token" }),
      async () => assert.fail("a foreign credential must never be used"), () => {},
    );
    await assert.rejects(wrong.reportNext(), /credentials belong to another/);
    const wrongReceipt = new AccountOutcomeReporter(
      alice, journal, async () => ({ ...alice, bearer: "alice-token" }),
      async (outcome) => ({ ...outcome, ...bob, outcome: "acknowledged" }), () => {},
    );
    await assert.rejects(wrongReceipt.reportNext(), /receipt does not match/);
    assert.deepEqual(await journal.next(alice), completion);
  } finally {
    database.close();
  }
});

test("a reopened outcome journal never sends Alice's result with Bob's token", async () => {
  const directory = await mkdtemp(join(tmpdir(), "orch8-account-outcomes-"));
  const path = join(directory, "queue.db");
  const received = [];
  const server = createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const outcome = JSON.parse(Buffer.concat(chunks).toString());
    const principal = req.headers.authorization === "Bearer alice-token" ? alice : bob;
    received.push({ outcome, principal });
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify({ ...principal, submissionId: outcome.submissionId, revision: outcome.revision, outcome: "acknowledged" }));
  });
  server.listen(0, "127.0.0.1");
  try {
    await once(server, "listening");
    const source = new DatabaseSync(path);
    const initial = await SqliteAccountOutcomeJournal.open(sqliteAdapter(source));
    await initial.enqueue(alice, completion);
    source.close();

    const database = new DatabaseSync(path);
    try {
      const journal = await SqliteAccountOutcomeJournal.open(sqliteAdapter(database));
      const deliver = async (outcome, credential) => {
        const response = await fetch(`http://127.0.0.1:${server.address().port}/outcome`, {
          method: "POST", headers: { Authorization: `Bearer ${credential.bearer}` }, body: JSON.stringify(outcome),
        });
        return response.json();
      };
      let active = alice;
      let entered;
      const starting = new Promise(resolve => { entered = resolve; });
      let resume;
      const token = new Promise(resolve => { resume = resolve; });
      const stale = new AccountOutcomeReporter(alice, journal, () => { entered(); return token; }, deliver, () => {
        if (active !== alice) throw new Error("scope retired");
      });
      const pending = assert.rejects(stale.reportNext(), /scope retired/);
      await starting;
      active = bob;
      resume({ ...alice, bearer: "alice-token" });
      await pending;

      const reporter = (owner) => new AccountOutcomeReporter(
        owner, journal, async () => ({ ...owner, bearer: `${owner.accountId}-token` }), deliver, () => {},
      );
      const bobReporter = reporter(bob);
      assert.equal(await bobReporter.reportNext(), null);
      await bobReporter.enqueue({ ...completion, outputJson: '{"owner":"bob"}' });
      assert.equal((await bobReporter.reportNext()).outcome, "acknowledged");
      assert.equal((await reporter(alice).reportNext()).outcome, "acknowledged");
      assert.deepEqual(received.map(({ outcome, principal }) => [
        outcome.accountId, JSON.parse(outcome.outputJson).owner, principal.accountId,
      ]), [["bob", "bob", "bob"], ["alice", "alice", "alice"]]);
      assert.equal(await reporter(alice).reportNext(), null);
    } finally {
      database.close();
    }
  } finally {
    server.close();
    await rm(directory, { recursive: true, force: true });
  }
});
