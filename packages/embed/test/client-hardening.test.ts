import { describe, expect, it, vi } from "vitest";
import { EmbedClient, Orch8EmbedError } from "../src/client.js";
import { BASE, makeToken, mockEngine } from "./helpers.js";

describe("EmbedClient response validation (EW-01)", () => {
  it("rejects a 2xx with an HTML body (captive portal) as invalid_response", async () => {
    const engine = mockEngine({ "GET /runs": { raw: "<html>Please sign in to the Wi-Fi</html>" } });
    const client = new EmbedClient({ baseUrl: BASE, token: makeToken(), fetch: engine.fetch, sleep: () => Promise.resolve() });
    const err = await client.listRuns().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(Orch8EmbedError);
    expect(err).toMatchObject({ kind: "invalid_response", status: 200 });
    expect(engine.calls).toHaveLength(1); // a proxy page is not retried
  });

  it("rejects an empty, truncated or literal-null 2xx for reads", async () => {
    const empty = mockEngine({ "GET /approvals": { body: undefined } });
    const c1 = new EmbedClient({ baseUrl: BASE, token: makeToken(), fetch: empty.fetch });
    await expect(c1.listApprovals()).rejects.toMatchObject({ kind: "invalid_response" });
    const truncated = mockEngine({ "GET /runs/r1": { raw: '{"id":"r1","steps":[' } });
    const c2 = new EmbedClient({ baseUrl: BASE, token: makeToken(), fetch: truncated.fetch });
    await expect(c2.getRun("r1")).rejects.toMatchObject({ kind: "invalid_response" });
    const literalNull = mockEngine({ "GET /runs": { raw: "null" } });
    const c3 = new EmbedClient({ baseUrl: BASE, token: makeToken(), fetch: literalNull.fetch });
    await expect(c3.listRuns()).rejects.toMatchObject({ kind: "invalid_response" });
  });

  it("still accepts an empty 2xx for mutations whose response is ignored", async () => {
    const engine = mockEngine({ "POST /approvals/a1": { body: undefined }, "PUT /sequences/s": { body: undefined } });
    const client = new EmbedClient({ baseUrl: BASE, token: makeToken(), fetch: engine.fetch });
    await expect(client.resolveApproval("a1", { choice: "yes" })).resolves.toBeUndefined();
    await expect(client.putSequence("s", { blocks: [] } as never)).resolves.toBeUndefined();
    // ...but a non-empty non-JSON body is still a failure.
    const html = mockEngine({ "POST /approvals/a1": { raw: "<html></html>" } });
    const c2 = new EmbedClient({ baseUrl: BASE, token: makeToken(), fetch: html.fetch });
    await expect(c2.resolveApproval("a1", { choice: "yes" })).rejects.toMatchObject({ kind: "invalid_response" });
  });
});

describe("EmbedClient shared token cache (EW-07)", () => {
  it("asks a shared provider once when several clients hit expiry together", async () => {
    const now = Date.now();
    const expiring = makeToken({ exp: Math.floor(now / 1000) + 5 });
    const fresh = makeToken({ jti: "fresh" });
    const provider = vi.fn(() => Promise.resolve(fresh));
    const engine = mockEngine({ "GET /runs": { body: { items: [] } } });
    const clients = Array.from(
      { length: 5 },
      () => new EmbedClient({ baseUrl: BASE, token: expiring, tokenProvider: provider, fetch: engine.fetch, now: () => now }),
    );
    await Promise.all(clients.map((c) => c.listRuns()));
    expect(provider).toHaveBeenCalledTimes(1);
    expect(engine.calls.every((c) => c.auth === `Bearer ${fresh}`)).toBe(true);
  });

  it("does not call the provider on every request when the clock makes fresh tokens look expired", async () => {
    let now = Date.now();
    // Tokens expire "in 10 s" by the client's clock, i.e. always inside the 30 s skew window.
    const provider = vi.fn(() => makeToken({ exp: Math.floor(now / 1000) + 10 }));
    const engine = mockEngine({ "GET /runs": { body: { items: [] } } });
    const client = new EmbedClient({ baseUrl: BASE, tokenProvider: provider, fetch: engine.fetch, now: () => now });
    await client.listRuns();
    await client.listRuns();
    await client.listRuns();
    expect(provider).toHaveBeenCalledTimes(1);
    now += 6000; // past the minimum refresh gap
    await client.listRuns();
    expect(provider).toHaveBeenCalledTimes(2);
  });

  it("adopts another client's refreshed token on 401 instead of asking again", async () => {
    const stale = makeToken({ jti: "stale" });
    const fresh = makeToken({ jti: "fresh" });
    const provider = vi.fn(() => Promise.resolve(fresh));
    const engine = mockEngine({
      "GET /runs": (call) => (call.auth === `Bearer ${fresh}` ? { body: { items: [] } } : { status: 401 }),
    });
    const a = new EmbedClient({ baseUrl: BASE, token: stale, tokenProvider: provider, fetch: engine.fetch });
    const b = new EmbedClient({ baseUrl: BASE, token: stale, tokenProvider: provider, fetch: engine.fetch });
    await a.listRuns();
    await b.listRuns();
    expect(provider).toHaveBeenCalledTimes(1);
  });
});
