/**
 * GET /changes — tenant isolation, header scoping, crafted-cursor
 * boundaries, and feed immutability as new changes arrive.
 *
 * Every entry carries the producing tenant; the feed must never leak
 * across tenants regardless of cursor provenance. The `X-Tenant-Id`
 * header, when present, overrides the `tenant_id` query parameter.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import type { ChangePage } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import { craftCursor, produceChanges } from "./changefeed_helpers.ts";

const client = new Orch8Client();

/** Raw GET with explicit headers (the typed client sends none). */
async function rawGetChanges(
  query: Record<string, string>,
  headers: Record<string, string> = {},
): Promise<{ status: number; body: ChangePage }> {
  const qs = new URLSearchParams(query).toString();
  const res = await fetch(`${client.baseUrl}/changes${qs ? `?${qs}` : ""}`, {
    headers,
  });
  return { status: res.status, body: (await res.json()) as ChangePage };
}

describe("Change Feed — tenancy, immutability, cursor boundaries", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("two tenants producing changes concurrently see disjoint feeds", async () => {
    const tenantA = `ten-a-${uuid().slice(0, 8)}`;
    const tenantB = `ten-b-${uuid().slice(0, 8)}`;
    const instA = await produceChanges(tenantA, 3, "tenant-a-producer");
    const instB = await produceChanges(tenantB, 4, "tenant-b-producer");

    const feedA = await client.listChanges({ tenant_id: tenantA, limit: 500 });
    const feedB = await client.listChanges({ tenant_id: tenantB, limit: 500 });

    assert.equal(feedA.changes.length, 3);
    assert.equal(feedB.changes.length, 4);
    for (const entry of feedA.changes) {
      assert.equal(entry.tenant_id, tenantA);
      assert.equal(entry.instance_id, instA);
    }
    for (const entry of feedB.changes) {
      assert.equal(entry.tenant_id, tenantB);
      assert.equal(entry.instance_id, instB);
    }
    const idsA = new Set(feedA.changes.map((c) => c.id));
    assert.ok(
      feedB.changes.every((c) => !idsA.has(c.id)),
      "feeds must not share entries",
    );
  });

  it("tenant B never sees tenant A's changes even with A's cursor", async () => {
    const tenantA = `ten-ca-${uuid().slice(0, 8)}`;
    const tenantB = `ten-cb-${uuid().slice(0, 8)}`;
    // B's changes happen FIRST, then A's — so A's final cursor is a
    // position strictly after every B entry.
    await produceChanges(tenantB, 2, "tenant-b-first");
    await produceChanges(tenantA, 3, "tenant-a-second");

    const feedA = await client.listChanges({ tenant_id: tenantA, limit: 500 });
    const cursorA = feedA.next_cursor!;
    assert.ok(cursorA);

    const leaked = await client.listChanges({
      tenant_id: tenantB,
      cursor: cursorA,
    });
    assert.deepEqual(
      leaked.changes,
      [],
      "a cursor past all of B's entries must yield an empty page",
    );

    // And from the start, B's feed contains zero A entries.
    const feedB = await client.listChanges({ tenant_id: tenantB, limit: 500 });
    assert.ok(feedB.changes.every((c) => c.tenant_id === tenantB));
  });

  it("X-Tenant-Id header overrides the tenant_id query parameter", async () => {
    const tenantA = `ten-ha-${uuid().slice(0, 8)}`;
    const tenantB = `ten-hb-${uuid().slice(0, 8)}`;
    await produceChanges(tenantA, 2, "header-a");
    await produceChanges(tenantB, 3, "header-b");

    // Query says A, header says B — the header must win.
    const res = await rawGetChanges(
      { tenant_id: tenantA, limit: "500" },
      { "X-Tenant-Id": tenantB },
    );
    assert.equal(res.status, 200);
    assert.equal(res.body.changes.length, 3);
    assert.ok(res.body.changes.every((c) => c.tenant_id === tenantB));

    // Header alone (no query param) is a valid tenant scope.
    const headerOnly = await rawGetChanges(
      { limit: "500" },
      { "X-Tenant-Id": tenantA },
    );
    assert.equal(headerOnly.status, 200);
    assert.equal(headerOnly.body.changes.length, 2);
    assert.ok(headerOnly.body.changes.every((c) => c.tenant_id === tenantA));
  });

  it("header scoped feed paginates within the header tenant only", async () => {
    const tenantA = `ten-pa-${uuid().slice(0, 8)}`;
    const tenantB = `ten-pb-${uuid().slice(0, 8)}`;
    await produceChanges(tenantA, 3, "header-page-a");
    await produceChanges(tenantB, 1, "header-page-b");

    const page1 = await rawGetChanges(
      { tenant_id: tenantB, limit: "2" },
      { "X-Tenant-Id": tenantA },
    );
    assert.equal(page1.body.changes.length, 2);
    assert.equal(page1.body.has_more, true);

    const page2 = await rawGetChanges(
      { tenant_id: tenantB, limit: "2", cursor: page1.body.next_cursor! },
      { "X-Tenant-Id": tenantA },
    );
    assert.equal(page2.body.changes.length, 1);
    assert.equal(page2.body.has_more, false);
    assert.ok(
      [...page1.body.changes, ...page2.body.changes].every(
        (c) => c.tenant_id === tenantA,
      ),
    );
  });

  it("crafted far-future cursor returns an empty page, not an error", async () => {
    const tenantId = `ten-future-${uuid().slice(0, 8)}`;
    await produceChanges(tenantId, 2, "future-cursor");

    const future = craftCursor(
      new Date(Date.now() + 3_600_000).toISOString(),
      crypto.randomUUID(),
    );
    const page = await client.listChanges({ tenant_id: tenantId, cursor: future });
    assert.deepEqual(page.changes, []);
    assert.equal(page.has_more, false);
    assert.equal(page.next_cursor, undefined);
  });

  it("crafted epoch cursor replays the entire feed", async () => {
    const tenantId = `ten-epoch-${uuid().slice(0, 8)}`;
    await produceChanges(tenantId, 3, "epoch-cursor");

    const epoch = craftCursor(
      "1970-01-01T00:00:00Z",
      "00000000-0000-0000-0000-000000000000",
    );
    const fromEpoch = await client.listChanges({
      tenant_id: tenantId,
      cursor: epoch,
      limit: 500,
    });
    const fromStart = await client.listChanges({
      tenant_id: tenantId,
      limit: 500,
    });
    assert.deepEqual(
      fromEpoch.changes.map((c) => c.id),
      fromStart.changes.map((c) => c.id),
    );
    assert.equal(fromEpoch.changes.length, 3);
  });

  it("cursor with a valid timestamp but unknown id still positions correctly", async () => {
    const tenantId = `ten-unkid-${uuid().slice(0, 8)}`;
    await produceChanges(tenantId, 4, "unknown-id-cursor");

    const full = await client.listChanges({ tenant_id: tenantId, limit: 500 });
    // Same created_at as entry 1 but a random id: tuple ordering decides.
    const pivot = full.changes[1]!;
    const randomId = crypto.randomUUID();
    const cursor = craftCursor(pivot.created_at, randomId);
    const page = await client.listChanges({ tenant_id: tenantId, cursor });

    // Every returned entry must be strictly after (created_at, random id)
    // and the page must be a clean suffix — no interleaving, no error.
    for (const entry of page.changes) {
      assert.ok(
        entry.created_at > pivot.created_at ||
          (entry.created_at === pivot.created_at && entry.id > randomId),
        `entry ${entry.id} must sort strictly after the crafted cursor`,
      );
    }
    const returnedIds = new Set(page.changes.map((c) => c.id));
    const fullIds = full.changes.map((c) => c.id);
    const firstReturned = fullIds.findIndex((id) => returnedIds.has(id));
    if (page.changes.length > 0) {
      assert.deepEqual(
        page.changes.map((c) => c.id),
        fullIds.slice(firstReturned),
        "resume from an unknown-id cursor must return a clean suffix",
      );
    }
  });

  it("earlier pages are immutable as new changes arrive", async () => {
    const tenantId = `ten-immut-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "immut-producer",
      [step("s", "sleep", { duration_ms: 60_000 })],
      { tenantId },
    );
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "running", { timeoutMs: 10_000 });

    for (let i = 0; i < 2; i++) {
      await client.batchAction({
        filter: { tenant_id: tenantId, states: ["running"] },
        action: "signal",
        signal_type: `first_${i}`,
      });
    }
    const before = await client.listChanges({ tenant_id: tenantId, limit: 500 });
    assert.equal(before.changes.length, 2);

    // More changes arrive afterwards.
    for (let i = 0; i < 3; i++) {
      await client.batchAction({
        filter: { tenant_id: tenantId, states: ["running"] },
        action: "signal",
        signal_type: `second_${i}`,
      });
    }

    const after = await client.listChanges({ tenant_id: tenantId, limit: 500 });
    assert.equal(after.changes.length, 5);
    // The first two entries are byte-identical: audit entries are immutable
    // and rollback appears as new events rather than cursor rewinding.
    assert.deepEqual(
      after.changes.slice(0, 2),
      before.changes,
      "previously returned entries must never change",
    );
    // And re-reading the original page's resume cursor yields only the new 3.
    const resumed = await client.listChanges({
      tenant_id: tenantId,
      cursor: before.next_cursor,
    });
    assert.equal(resumed.changes.length, 3);
  });

  it("fresh tenant with only another tenant's cursor sees its own newer changes", async () => {
    const tenantA = `ten-xa-${uuid().slice(0, 8)}`;
    const tenantB = `ten-xb-${uuid().slice(0, 8)}`;
    await produceChanges(tenantA, 2, "cross-a");
    const feedA = await client.listChanges({ tenant_id: tenantA, limit: 500 });

    // B produces changes AFTER A's cursor position.
    const instB = await produceChanges(tenantB, 2, "cross-b");

    const feedB = await client.listChanges({
      tenant_id: tenantB,
      cursor: feedA.next_cursor,
    });
    // Cursors are positions, not tenant-bound tokens: B's newer entries are
    // returned, and every one of them belongs to B.
    assert.equal(feedB.changes.length, 2);
    assert.ok(
      feedB.changes.every(
        (c) => c.tenant_id === tenantB && c.instance_id === instB,
      ),
    );
  });
});
