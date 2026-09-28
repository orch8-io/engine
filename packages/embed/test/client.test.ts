import { describe, expect, it, vi } from "vitest";
import { EmbedClient, Orch8EmbedError } from "../src/client.js";
import { decodeEmbedToken, tokenAllows } from "../src/token.js";
import { BASE, makeToken, mockEngine } from "./helpers.js";

const noSleep = () => Promise.resolve();

describe("EmbedClient", () => {
  it("sends the bearer token and targets /api/v1/embed", async () => {
    const token = makeToken();
    const engine = mockEngine({ "GET /runs": { body: { items: [], next_cursor: null } } });
    const client = new EmbedClient({ baseUrl: `${BASE}/`, token, fetch: engine.fetch });
    await client.listRuns({ limit: 5 });
    expect(engine.calls[0]!.auth).toBe(`Bearer ${token}`);
    expect(engine.calls[0]!.url).toBe(`${BASE}/api/v1/embed/runs?limit=5`);
  });

  it("maps 403 to a forbidden error carrying the engine message and code", async () => {
    const engine = mockEngine({
      "POST /approvals/a1": { status: 403, body: { error: { code: "forbidden", message: "missing scope approvals:resolve" } } },
    });
    const client = new EmbedClient({ baseUrl: BASE, token: makeToken(), fetch: engine.fetch, sleep: noSleep });
    const err = await client.resolveApproval("a1", { choice: "yes" }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(Orch8EmbedError);
    expect(err).toMatchObject({ kind: "forbidden", status: 403, code: "forbidden", message: "missing scope approvals:resolve" });
    expect(engine.calls).toHaveLength(1); // never retried
  });

  it("refreshes the token via tokenProvider on 401 and retries once", async () => {
    const stale = makeToken({ jti: "stale" });
    const fresh = makeToken({ jti: "fresh" });
    const provider = vi.fn(() => Promise.resolve(fresh));
    const engine = mockEngine({
      "GET /approvals": (call) =>
        call.auth === `Bearer ${fresh}` ? { body: { items: [] } } : { status: 401, body: { error: { code: "unauthorized", message: "expired" } } },
    });
    const client = new EmbedClient({ baseUrl: BASE, token: stale, tokenProvider: provider, fetch: engine.fetch });
    await expect(client.listApprovals()).resolves.toEqual({ items: [] });
    expect(provider).toHaveBeenCalledWith({ reason: "unauthorized" });
    expect(engine.calls.map((c) => c.auth)).toEqual([`Bearer ${stale}`, `Bearer ${fresh}`]);
  });

  it("asks the provider proactively when the token is about to expire", async () => {
    const now = Date.now();
    const expiring = makeToken({ exp: Math.floor(now / 1000) + 10 });
    const fresh = makeToken({ jti: "new" });
    const provider = vi.fn(() => fresh);
    const engine = mockEngine({ "GET /runs": { body: { items: [] } } });
    const client = new EmbedClient({ baseUrl: BASE, token: expiring, tokenProvider: provider, fetch: engine.fetch, now: () => now });
    await client.listRuns();
    expect(provider).toHaveBeenCalledWith({ reason: "expiring" });
    expect(engine.calls[0]!.auth).toBe(`Bearer ${fresh}`);
  });

  it("dedupes concurrent provider calls", async () => {
    let resolve!: (t: string) => void;
    const provider = vi.fn(() => new Promise<string>((r) => (resolve = r)));
    const engine = mockEngine({ "GET /runs": { body: { items: [] } }, "GET /approvals": { body: { items: [] } } });
    const client = new EmbedClient({ baseUrl: BASE, tokenProvider: provider, fetch: engine.fetch });
    const both = Promise.all([client.listRuns(), client.listApprovals()]);
    resolve(makeToken());
    await both;
    expect(provider).toHaveBeenCalledTimes(1);
    expect(provider).toHaveBeenCalledWith({ reason: "initial" });
  });

  it("retries GET on 5xx with exponential backoff, then succeeds", async () => {
    const sleeps: number[] = [];
    const engine = mockEngine({
      "GET /runs/r1": [{ status: 502 }, { status: 503 }, { body: { id: "r1", sequence: "s", state: "running", steps: [] } }],
    });
    const client = new EmbedClient({
      baseUrl: BASE,
      token: makeToken(),
      fetch: engine.fetch,
      retryBaseMs: 100,
      sleep: (ms) => (sleeps.push(ms), Promise.resolve()),
    });
    const run = await client.getRun("r1");
    expect(run.id).toBe("r1");
    expect(engine.calls).toHaveLength(3);
    expect(sleeps[0]).toBeGreaterThanOrEqual(100);
    expect(sleeps[1]).toBeGreaterThanOrEqual(200);
  });

  it("gives up after the retry budget", async () => {
    const engine = mockEngine({ "GET /runs": { status: 500 } });
    const client = new EmbedClient({ baseUrl: BASE, token: makeToken(), fetch: engine.fetch, retries: 2, sleep: noSleep });
    await expect(client.listRuns()).rejects.toMatchObject({ kind: "server", status: 500 });
    expect(engine.calls).toHaveLength(3);
  });

  it("honours Retry-After on 429", async () => {
    const sleeps: number[] = [];
    const engine = mockEngine({ "GET /runs": [{ status: 429, headers: { "Retry-After": "2" } }, { body: { items: [] } }] });
    const client = new EmbedClient({ baseUrl: BASE, token: makeToken(), fetch: engine.fetch, sleep: (ms) => (sleeps.push(ms), Promise.resolve()) });
    await client.listRuns();
    expect(sleeps).toEqual([2000]);
  });

  it("does not retry POST (not idempotent)", async () => {
    const engine = mockEngine({ "POST /runs": { status: 503 } });
    const client = new EmbedClient({ baseUrl: BASE, token: makeToken(), fetch: engine.fetch, sleep: noSleep });
    await expect(client.startRun({ sequence: "onboard", input: {} })).rejects.toMatchObject({ kind: "server" });
    expect(engine.calls).toHaveLength(1);
  });

  it("retries network failures for GET", async () => {
    let n = 0;
    const f = vi.fn(async () => {
      n += 1;
      if (n === 1) throw new TypeError("Failed to fetch");
      return new Response(JSON.stringify({ items: [] }), { status: 200 });
    }) as unknown as typeof fetch;
    const client = new EmbedClient({ baseUrl: BASE, token: makeToken(), fetch: f, sleep: noSleep });
    await expect(client.listApprovals()).resolves.toEqual({ items: [] });
  });

  it("errors with kind=config when there is no token source", async () => {
    const client = new EmbedClient({ baseUrl: BASE, fetch: mockEngine({}).fetch });
    await expect(client.listRuns()).rejects.toMatchObject({ kind: "config" });
  });

  it("normalises the sequences payload and handler palette", async () => {
    const engine = mockEngine({
      "GET /sequences": { body: { items: [{ name: "onboard" }], handlers: ["http_request", { name: "send_email", label: "Send email" }, { bogus: 1 }] } },
    });
    const client = new EmbedClient({ baseUrl: BASE, token: makeToken(), fetch: engine.fetch });
    const list = await client.listSequences();
    expect(list.items).toEqual([{ name: "onboard" }]);
    expect(list.handlers).toEqual([
      { name: "http_request" },
      { name: "send_email", label: "Send email", description: undefined, default_params: undefined },
    ]);
  });
});

describe("token helpers", () => {
  it("decodes payload and checks scopes", () => {
    const t = makeToken({ scp: ["runs:read"] });
    expect(decodeEmbedToken(t)?.sub).toBe("customer-1");
    expect(tokenAllows(t, "runs:read")).toBe(true);
    expect(tokenAllows(t, "builder:edit")).toBe(false);
  });

  it("treats opaque tokens as undecodable and allows optimistically", () => {
    expect(decodeEmbedToken("opaque")).toBeNull();
    expect(decodeEmbedToken("o8e1.!!!.x")).toBeNull();
    expect(tokenAllows("opaque", "builder:edit")).toBe(true);
  });
});
