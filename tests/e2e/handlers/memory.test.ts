/**
 * Agent-memory handlers — embed / memory_store / memory_search.
 *
 * memory_store + memory_search share the per-instance KV store, so a single
 * sequence stores several memories then searches them. Ranking is a cosine
 * scan in the engine (no vector DB), verified here end-to-end.
 *
 * Covered:
 *   - store memories with precomputed embeddings, then search with a
 *     precomputed query vector → correct nearest neighbour, with score.
 *   - top_k caps the result count.
 *   - the `embed` handler calls a mock /embeddings endpoint and returns a vector.
 */
import { describe, it, before, after, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

async function readBody(req: IncomingMessage): Promise<any> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const raw = Buffer.concat(chunks).toString("utf-8");
  return raw ? JSON.parse(raw) : null;
}

interface EmbedMockHandle {
  baseUrl: string;
  calls: number;
  close(): Promise<void>;
}

/** Mock /embeddings endpoint: returns a fixed 3-dim vector per input. */
function startEmbedMock(): Promise<EmbedMockHandle> {
  return new Promise((resolvePromise) => {
    const state = { calls: 0 };
    const server = createServer((req, res) => {
      void (async () => {
        const body = (await readBody(req)) as { input: string[] } | null;
        state.calls += 1;
        const inputs = body?.input ?? [];
        const data = inputs.map((_, index) => ({
          index,
          embedding: [0.1, 0.2, 0.3],
        }));
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ data, model: "text-embedding-3-small", usage: {} }));
      })();
    });
    server.listen(0, "127.0.0.1", () => {
      const port = (server.address() as AddressInfo).port;
      resolvePromise({
        baseUrl: `http://127.0.0.1:${port}`,
        get calls() {
          return state.calls;
        },
        close: () => new Promise<void>((r) => server.close(() => r())),
      });
    });
  });
}

describe("Agent memory — store / search / embed", () => {
  let server: ServerHandle | undefined;
  let mock: EmbedMockHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  beforeEach(async () => {
    if (mock) await mock.close();
    mock = undefined;
  });

  after(async () => {
    if (mock) await mock.close();
  });

  it("stores memories and returns the nearest by cosine similarity", async () => {
    const tenantId = `mem-search-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "mem-search",
      [
        step("store_sky", "memory_store", {
          key: "sky",
          text: "the sky is blue",
          embedding: [1.0, 0.0, 0.0],
          metadata: { topic: "weather" },
        }),
        step("store_cat", "memory_store", {
          key: "cat",
          text: "cats are mammals",
          embedding: [0.0, 1.0, 0.0],
        }),
        step("store_sea", "memory_store", {
          key: "sea",
          text: "the sea is also blue",
          embedding: [0.9, 0.1, 0.0],
        }),
        step("search", "memory_search", {
          query_embedding: [1.0, 0.0, 0.0],
          top_k: 2,
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 20_000 });

    const outputs = await client.getOutputs(id);
    const storeOut = outputs.find((o) => o.block_id === "store_sky")!.output as {
      key: string;
      stored: boolean;
      dimensions: number;
    };
    assert.equal(storeOut.key, "sky");
    assert.equal(storeOut.stored, true);
    assert.equal(storeOut.dimensions, 3);

    const searchOut = outputs.find((o) => o.block_id === "search")!.output as {
      results: Array<{ key: string; text: string; score: number; metadata: unknown }>;
      count: number;
    };
    assert.equal(searchOut.count, 2, "top_k=2 must cap results");
    assert.equal(searchOut.results[0]!.key, "sky", "exact match ranks first");
    assert.equal(searchOut.results[1]!.key, "sea", "near match ranks second");
    assert.ok(searchOut.results[0]!.score > 0.99, "exact match score ~1.0");
    assert.equal((searchOut.results[0]!.metadata as { topic: string }).topic, "weather");
    // The unrelated "cat" memory is excluded by top_k.
    assert.ok(!searchOut.results.some((r) => r.key === "cat"));
  });

  it("embed handler returns a vector from the provider", async () => {
    mock = await startEmbedMock();
    const tenantId = `mem-embed-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "mem-embed",
      [
        step("e1", "embed", {
          input: "hello world",
          base_url: mock.baseUrl,
          api_key: "test-key",
          model: "text-embedding-3-small",
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 20_000 });

    assert.equal(mock.calls, 1);
    const outputs = await client.getOutputs(id);
    const out = outputs.find((o) => o.block_id === "e1")!.output as {
      embedding: number[];
      dimensions: number;
    };
    assert.deepEqual(out.embedding, [0.1, 0.2, 0.3]);
    assert.equal(out.dimensions, 3);
  });

  it("memory_store computes the embedding when only text is given", async () => {
    mock = await startEmbedMock();
    const tenantId = `mem-store-embed-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "mem-store-embed",
      [
        step("s1", "memory_store", {
          key: "doc1",
          text: "some document",
          base_url: mock.baseUrl,
          api_key: "test-key",
        }),
        step("search", "memory_search", {
          query_embedding: [0.1, 0.2, 0.3],
          top_k: 1,
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 20_000 });

    // One embed call for the stored text (search used a precomputed vector).
    assert.equal(mock.calls, 1);
    const outputs = await client.getOutputs(id);
    const searchOut = outputs.find((o) => o.block_id === "search")!.output as {
      results: Array<{ key: string }>;
      count: number;
    };
    assert.equal(searchOut.count, 1);
    assert.equal(searchOut.results[0]!.key, "doc1");
  });
});
