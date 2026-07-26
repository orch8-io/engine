/**
 * GET /changes — resumable tenant change feed: page shape, ordering,
 * cursor exclusivity, limit clamping, and malformed-cursor handling.
 *
 * The feed is audit-backed: only instance lifecycle events create entries
 * (sequence definitions alone never do). Change counts are made
 * deterministic by driving `batch_action` audit entries against a
 * long-sleeping instance — each batch-action call appends exactly one
 * feed entry. Every test uses its own tenant so feeds never overlap.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { ApiError, Orch8Client, testSequence, step, uuid } from "../client.ts";
import type { ChangePage } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import { craftCursor, produceChanges } from "./changefeed_helpers.ts";

const client = new Orch8Client();

/** Fetch every change for a tenant in one oversized page. */
async function fullFeed(tenantId: string): Promise<ChangePage> {
  const page = await client.listChanges({ tenant_id: tenantId, limit: 500 });
  assert.equal(page.has_more, false, "500 entries should cover any test feed");
  return page;
}

describe("Change Feed — page shape, ordering, cursors", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("empty feed for a fresh tenant has no cursor and no more pages", async () => {
    const tenantId = `feed-empty-${uuid().slice(0, 8)}`;
    const page = await client.listChanges({ tenant_id: tenantId });
    assert.deepEqual(page.changes, []);
    assert.equal(page.has_more, false);
    assert.equal(page.next_cursor, undefined);
  });

  it("creating a sequence without instances produces no changes", async () => {
    const tenantId = `feed-seqonly-${uuid().slice(0, 8)}`;
    const seq = testSequence("feed-seq-only", [step("a", "noop")], { tenantId });
    await client.createSequence(seq);

    const page = await client.listChanges({ tenant_id: tenantId });
    assert.deepEqual(page.changes, []);
    assert.equal(page.has_more, false);
  });

  it("missing tenant scope is rejected with 400", async () => {
    await assert.rejects(client.listChanges({}), (err: unknown) => {
      assert.ok(err instanceof ApiError);
      assert.equal(err.status, 400);
      assert.match(err.body, /tenant scope is required/);
      return true;
    });
  });

  it("empty-string tenant is treated as missing and rejected with 400", async () => {
    await assert.rejects(
      client.listChanges({ tenant_id: "" }),
      (err: unknown) => {
        assert.ok(err instanceof ApiError);
        assert.equal(err.status, 400);
        return true;
      },
    );
  });

  it("completed lifecycle ends the feed with a terminal transition", async () => {
    const tenantId = `feed-life-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "feed-life",
      [step("a", "noop"), step("b", "noop")],
      { tenantId },
    );
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });

    const { changes } = await fullFeed(tenantId);
    assert.ok(changes.length >= 1, "completion must be recorded");
    const last = changes.at(-1)!;
    assert.equal(last.event_type, "state_transition");
    assert.equal(last.to_state, "completed");
    assert.equal(last.from_state, "running");
    assert.equal(last.instance_id, id);
    assert.equal(last.tenant_id, tenantId);
  });

  it("batch-action producer appends exactly one well-formed entry per call", async () => {
    const tenantId = `feed-prod-${uuid().slice(0, 8)}`;
    const instanceId = await produceChanges(tenantId, 5);

    const { changes } = await fullFeed(tenantId);
    assert.equal(changes.length, 5);
    for (const [i, entry] of changes.entries()) {
      assert.equal(entry.event_type, "batch_action");
      assert.equal(entry.instance_id, instanceId);
      assert.equal(entry.tenant_id, tenantId);
      assert.equal(entry.from_state, "running");
      assert.deepEqual(entry.details, { action: "signal" });
      assert.match(entry.id, /^[0-9a-f-]{36}$/);
      assert.ok(!Number.isNaN(Date.parse(entry.created_at)));
      // Distinct entries must have distinct ids.
      assert.ok(
        changes.findIndex((c) => c.id === entry.id) === i,
        "entry ids must be unique",
      );
    }
  });

  it("feed is ascending in (created_at, id) even for same-millisecond bursts", async () => {
    const tenantId = `feed-order-${uuid().slice(0, 8)}`;
    await produceChanges(tenantId, 8);

    const { changes } = await fullFeed(tenantId);
    assert.equal(changes.length, 8);
    for (let i = 1; i < changes.length; i++) {
      const prev = changes[i - 1]!;
      const cur = changes[i]!;
      // chrono's AutoSi serialization emits variable fractional precision
      // (".945Z" vs ".945123Z"), so lexicographic string order can invert
      // real chronological order — compare parsed timestamps instead. Ids
      // are fixed-width 36-char UUIDs, so string compare is fine for the
      // tiebreaker.
      const prevTs = Date.parse(prev.created_at);
      const curTs = Date.parse(cur.created_at);
      assert.ok(
        !Number.isNaN(prevTs) && !Number.isNaN(curTs),
        "created_at must be parseable",
      );
      assert.ok(
        prevTs < curTs || (prevTs === curTs && prev.id < cur.id),
        `feed must be ascending at index ${i}: ${prev.created_at}/${prev.id} vs ${cur.created_at}/${cur.id}`,
      );
    }
  });

  it("limit=1 walks the whole feed with no duplicates and no gaps", async () => {
    const tenantId = `feed-walk-${uuid().slice(0, 8)}`;
    await produceChanges(tenantId, 7);

    const oneShot = await fullFeed(tenantId);
    assert.equal(oneShot.changes.length, 7);

    const walked: string[] = [];
    let cursor: string | undefined;
    let pages = 0;
    for (;;) {
      const page: ChangePage = await client.listChanges({
        tenant_id: tenantId,
        limit: 1,
        ...(cursor ? { cursor } : {}),
      });
      pages += 1;
      assert.equal(page.changes.length, 1, `page ${pages} must hold 1 entry`);
      walked.push(page.changes[0]!.id);
      if (!page.has_more) break;
      assert.ok(page.next_cursor, "has_more pages must carry a cursor");
      cursor = page.next_cursor;
      assert.ok(pages < 50, "walk must terminate");
    }

    assert.equal(pages, 7);
    assert.deepEqual(
      walked,
      oneShot.changes.map((c) => c.id),
      "walked ids must equal the one-shot feed exactly",
    );
  });

  it("cursor is exclusive: resume starts strictly after the cursor entry", async () => {
    const tenantId = `feed-excl-${uuid().slice(0, 8)}`;
    await produceChanges(tenantId, 6);

    const { changes } = await fullFeed(tenantId);
    const k = 2; // resume from the third entry
    const cursor = craftCursor(changes[k]!.created_at, changes[k]!.id);

    const resumed = await client.listChanges({ tenant_id: tenantId, cursor });
    assert.deepEqual(
      resumed.changes.map((c) => c.id),
      changes.slice(k + 1).map((c) => c.id),
      "resumed feed must contain exactly the entries after the cursor",
    );
    assert.ok(
      !resumed.changes.some((c) => c.id === changes[k]!.id),
      "cursor entry itself must not be replayed",
    );
  });

  it("server-issued next_cursor resumes identically to a crafted cursor", async () => {
    const tenantId = `feed-equiv-${uuid().slice(0, 8)}`;
    await produceChanges(tenantId, 5);

    const page = await client.listChanges({ tenant_id: tenantId, limit: 2 });
    assert.equal(page.changes.length, 2);
    assert.equal(page.has_more, true);
    const serverCursor = page.next_cursor!;
    const crafted = craftCursor(page.changes[1]!.created_at, page.changes[1]!.id);

    const viaServer = await client.listChanges({
      tenant_id: tenantId,
      cursor: serverCursor,
    });
    const viaCrafted = await client.listChanges({
      tenant_id: tenantId,
      cursor: crafted,
    });
    assert.deepEqual(
      viaServer.changes.map((c) => c.id),
      viaCrafted.changes.map((c) => c.id),
    );
    assert.equal(viaServer.changes.length, 3);
  });

  it("exact-size page reports has_more=false and its cursor resumes to empty", async () => {
    const tenantId = `feed-exact-${uuid().slice(0, 8)}`;
    await produceChanges(tenantId, 4);

    const page = await client.listChanges({ tenant_id: tenantId, limit: 4 });
    assert.equal(page.changes.length, 4);
    assert.equal(page.has_more, false);
    assert.ok(
      page.next_cursor,
      "a non-empty page always yields a resume cursor",
    );

    const tail = await client.listChanges({
      tenant_id: tenantId,
      cursor: page.next_cursor,
    });
    assert.deepEqual(tail.changes, []);
    assert.equal(tail.has_more, false);
    assert.equal(tail.next_cursor, undefined);
  });

  it("limit=0 is clamped to a single entry", async () => {
    const tenantId = `feed-zero-${uuid().slice(0, 8)}`;
    await produceChanges(tenantId, 3);

    const page = await client.listChanges({ tenant_id: tenantId, limit: 0 });
    assert.equal(page.changes.length, 1);
    assert.equal(page.has_more, true);
    assert.ok(page.next_cursor);
  });

  it("limit far above the 500 cap is accepted and returns the whole feed", async () => {
    const tenantId = `feed-cap-${uuid().slice(0, 8)}`;
    await produceChanges(tenantId, 4);

    const huge = await client.listChanges({
      tenant_id: tenantId,
      limit: 100_000,
    });
    assert.equal(huge.changes.length, 4);
    assert.equal(huge.has_more, false);
  });

  it("malformed cursors are all rejected with 400", async () => {
    const tenantId = `feed-bad-${uuid().slice(0, 8)}`;

    const badCursors: Record<string, string> = {
      "not base64url at all": "not+url/base64",
      "valid base64url but not JSON": Buffer.from("hello world").toString(
        "base64url",
      ),
      "JSON missing fields": Buffer.from(`{"foo":1}`).toString("base64url"),
      "JSON wrong types": Buffer.from(
        `{"created_at":"nope","id":"nope"}`,
      ).toString("base64url"),
      "JSON null": Buffer.from("null").toString("base64url"),
      "JSON array": Buffer.from("[]").toString("base64url"),
    };

    for (const [label, cursor] of Object.entries(badCursors)) {
      await assert.rejects(
        client.listChanges({ tenant_id: tenantId, cursor }),
        (err: unknown) => {
          assert.ok(err instanceof ApiError, label);
          assert.equal(err.status, 400, `${label}: ${err.body}`);
          assert.match(err.body, /malformed change cursor/);
          return true;
        },
        label,
      );
    }
  });
});
