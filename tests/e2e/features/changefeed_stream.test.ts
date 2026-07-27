/**
 * GET /changes/stream — resumable SSE change stream: backlog replay,
 * live delivery, Last-Event-ID resume, cursor precedence, and error
 * handling. Each SSE event's `id` is the next opaque cursor; reconnecting
 * with `Last-Event-ID` (or an explicit `cursor`, which wins) must replay
 * only changes after that position.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import { poke, startProducer } from "./changefeed_helpers.ts";

const client = new Orch8Client();

interface SseEvent {
  event: string;
  id: string;
  data: string;
}

/** Open the change stream; caller must `close()` when done. */
async function openStream(
  query: Record<string, string>,
  headers: Record<string, string> = {},
): Promise<{
  res: Response;
  next: (timeoutMs?: number) => Promise<SseEvent>;
  close: () => Promise<void>;
}> {
  const qs = new URLSearchParams(query).toString();
  const res = await fetch(
    `${client.baseUrl}/changes/stream${qs ? `?${qs}` : ""}`,
    { headers },
  );
  if (res.status !== 200) {
    assert.fail(`stream must open (got ${res.status}): ${await res.text()}`);
  }
  assert.match(
    res.headers.get("content-type") ?? "",
    /text\/event-stream/,
    "stream must be SSE",
  );

  const reader = res.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  async function next(timeoutMs = 10_000): Promise<SseEvent> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const boundary = buffer.indexOf("\n\n");
      if (boundary >= 0) {
        const block = buffer.slice(0, boundary);
        buffer = buffer.slice(boundary + 2);
        const parsed: SseEvent = { event: "message", id: "", data: "" };
        for (const line of block.split("\n")) {
          if (line.startsWith(":")) continue; // keep-alive comment
          if (line.startsWith("event:")) parsed.event = line.slice(6).trim();
          else if (line.startsWith("id:")) parsed.id = line.slice(3).trim();
          else if (line.startsWith("data:")) parsed.data += line.slice(5).trim();
        }
        if (parsed.event || parsed.data) return parsed;
        continue;
      }
      const remaining = deadline - Date.now();
      assert.ok(remaining > 0, `timed out waiting for SSE event (buf=${buffer.length}b)`);
      let timer: NodeJS.Timeout | undefined;
      const chunk = await Promise.race([
        reader.read(),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error("SSE read timeout")), remaining);
        }),
      ]).finally(() => clearTimeout(timer));
      assert.ok(!chunk.done, "stream closed before an event arrived");
      buffer += decoder.decode(chunk.value, { stream: true });
    }
  }

  async function close(): Promise<void> {
    try {
      await reader.cancel();
    } catch {
      /* already closed */
    }
  }

  return { res, next, close };
}

describe("Change Feed — SSE stream", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("replays the existing backlog on connect, in order", async () => {
    const tenantId = `sse-back-${uuid().slice(0, 8)}`;
    const instanceId = await startProducer(tenantId);
    await poke(tenantId, "backlog_0");
    await poke(tenantId, "backlog_1");

    const feed = await client.listChanges({ tenant_id: tenantId, limit: 500 });
    assert.equal(feed.changes.length, 2);

    const stream = await openStream({ tenant_id: tenantId });
    try {
      const first = await stream.next();
      assert.equal(first.event, "change");
      const firstData = JSON.parse(first.data);
      assert.equal(firstData.id, feed.changes[0]!.id);
      assert.equal(firstData.instance_id, instanceId);

      const second = await stream.next();
      assert.equal(second.event, "change");
      assert.equal(JSON.parse(second.data).id, feed.changes[1]!.id);
    } finally {
      await stream.close();
    }
  });

  it("delivers changes produced after connect, with a resumable event id", async () => {
    const tenantId = `sse-live-${uuid().slice(0, 8)}`;
    const instanceId = await startProducer(tenantId);

    const stream = await openStream({ tenant_id: tenantId });
    try {
      // Produce AFTER the stream is open.
      await poke(tenantId, "live_0");
      const event = await stream.next();
      assert.equal(event.event, "change");
      assert.ok(event.id.length > 0, "event id is the resume cursor");

      const data = JSON.parse(event.data);
      assert.equal(data.tenant_id, tenantId);
      assert.equal(data.instance_id, instanceId);
      assert.equal(data.event_type, "batch_action");

      // The event id must be a valid exclusive cursor for the JSON feed.
      const resumed = await client.listChanges({
        tenant_id: tenantId,
        cursor: event.id,
      });
      assert.ok(
        resumed.changes.every((c) => c.id !== data.id),
        "event id cursor must exclude the delivered change",
      );
    } finally {
      await stream.close();
    }
  });

  it("reconnecting with Last-Event-ID replays nothing already consumed", async () => {
    const tenantId = `sse-leid-${uuid().slice(0, 8)}`;
    await startProducer(tenantId);
    await poke(tenantId, "consumed_0");
    await poke(tenantId, "consumed_1");

    const feed = await client.listChanges({ tenant_id: tenantId, limit: 500 });
    assert.equal(feed.changes.length, 2);
    const lastCursor = feed.next_cursor!;

    const stream = await openStream(
      { tenant_id: tenantId },
      { "Last-Event-ID": lastCursor },
    );
    try {
      // Nothing new yet — the first event must be the change we produce
      // now, not a replay of the two consumed entries.
      await poke(tenantId, "fresh_0");
      const event = await stream.next();
      assert.equal(event.event, "change");
      const data = JSON.parse(event.data);
      assert.ok(
        ![feed.changes[0]!.id, feed.changes[1]!.id].includes(data.id),
        "consumed entries must not be replayed",
      );
      assert.equal(data.event_type, "batch_action");
    } finally {
      await stream.close();
    }
  });

  it("explicit cursor query parameter takes precedence over Last-Event-ID", async () => {
    const tenantId = `sse-prec-${uuid().slice(0, 8)}`;
    await startProducer(tenantId);
    await poke(tenantId, "prec_0");
    await poke(tenantId, "prec_1");
    await poke(tenantId, "prec_2");

    const feed = await client.listChanges({ tenant_id: tenantId, limit: 500 });
    assert.equal(feed.changes.length, 3);
    const firstCursor = Buffer.from(
      JSON.stringify({
        created_at: feed.changes[0]!.created_at,
        id: feed.changes[0]!.id,
      }),
    ).toString("base64url");
    const lastCursor = feed.next_cursor!;

    // Query cursor points after entry 0; Last-Event-ID after entry 2.
    // The query parameter must win → entry 1 is delivered first.
    const stream = await openStream(
      { tenant_id: tenantId, cursor: firstCursor },
      { "Last-Event-ID": lastCursor },
    );
    try {
      const event = await stream.next();
      assert.equal(JSON.parse(event.data).id, feed.changes[1]!.id);
      const second = await stream.next();
      assert.equal(JSON.parse(second.data).id, feed.changes[2]!.id);
    } finally {
      await stream.close();
    }
  });

  it("event data is a well-formed audit entry scoped to the stream tenant", async () => {
    const tenantId = `sse-shape-${uuid().slice(0, 8)}`;
    await startProducer(tenantId);
    await poke(tenantId, "shape_0");

    const stream = await openStream({ tenant_id: tenantId });
    try {
      const event = await stream.next();
      assert.equal(event.event, "change");
      const data = JSON.parse(event.data);
      assert.match(data.id, /^[0-9a-f-]{36}$/);
      assert.equal(data.tenant_id, tenantId);
      assert.equal(typeof data.instance_id, "string");
      assert.equal(typeof data.event_type, "string");
      assert.ok(!Number.isNaN(Date.parse(data.created_at)));
      // The SSE id decodes to exactly this entry's (created_at, id) cursor.
      const decoded = JSON.parse(
        Buffer.from(event.id, "base64url").toString("utf8"),
      );
      assert.equal(decoded.id, data.id);
      assert.equal(decoded.created_at, data.created_at);
    } finally {
      await stream.close();
    }
  });

  it("stream with a malformed cursor is rejected with 400", async () => {
    const res = await fetch(
      `${client.baseUrl}/changes/stream?tenant_id=t-${uuid().slice(0, 8)}&cursor=not%2Burl%2Fbase64`,
    );
    assert.equal(res.status, 400);
    assert.match(await res.text(), /malformed change cursor/);
  });

  it("stream without any tenant scope is rejected with 400", async () => {
    const res = await fetch(`${client.baseUrl}/changes/stream`);
    assert.equal(res.status, 400);
    assert.match(await res.text(), /tenant scope is required/);
  });

  it("stream scoped by X-Tenant-Id header only delivers that tenant", async () => {
    const tenantA = `sse-ha-${uuid().slice(0, 8)}`;
    const tenantB = `sse-hb-${uuid().slice(0, 8)}`;
    await startProducer(tenantA);
    const instB = await startProducer(tenantB);
    await poke(tenantA, "header_a_0");
    await poke(tenantB, "header_b_0");

    const stream = await openStream({}, { "X-Tenant-Id": tenantB });
    try {
      const event = await stream.next();
      const data = JSON.parse(event.data);
      assert.equal(data.tenant_id, tenantB);
      assert.equal(data.instance_id, instB);
    } finally {
      await stream.close();
    }
  });
});
