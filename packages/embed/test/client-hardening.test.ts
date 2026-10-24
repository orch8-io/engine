import { describe, expect, it, vi } from "vitest";
import { decodeEmbedToken } from "../src/token.js";
import { EmbedClient, Orch8EmbedError } from "../src/client.js";
import { BASE, b64url, makeToken, mockEngine } from "./helpers.js";

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

describe("EmbedClient credential boundary isolation", () => {
  it("does not reuse a provider token across engine origins", async () => {
    const firstToken = makeToken({ jti: "engine-a" });
    const secondToken = makeToken({ jti: "engine-b" });
    const provider = vi.fn().mockResolvedValueOnce(firstToken).mockResolvedValueOnce(secondToken);
    const engine = mockEngine({ "GET /runs": { body: { items: [] } } });
    const a = new EmbedClient({ baseUrl: BASE, tokenProvider: provider, fetch: engine.fetch });
    const b = new EmbedClient({ baseUrl: "https://other.example", tokenProvider: provider, fetch: engine.fetch });
    await a.listRuns();
    await b.listRuns();
    expect(engine.calls[1]?.auth).toBe(`Bearer ${secondToken}`);
    expect(provider).toHaveBeenCalledTimes(2);
  });

  it("does not adopt another customer's refresh after a 401", async () => {
    const initialA = makeToken({ sub: "customer-a" });
    const initialB = makeToken({ sub: "customer-b" });
    const freshA = makeToken({ sub: "customer-a", jti: "fresh-a" });
    const freshB = makeToken({ sub: "customer-b", jti: "fresh-b" });
    const provider = vi.fn().mockResolvedValueOnce(freshB).mockResolvedValueOnce(freshA);
    const engine = mockEngine({ "GET /runs": (call) => {
      if (call.auth === `Bearer ${freshA}`) return { body: { items: [{ id: "a-private-run" }] } };
      if (call.auth === `Bearer ${freshB}`) return { body: { items: [{ id: "b-private-run" }] } };
      return { status: 401 };
    } });
    const a = new EmbedClient({ baseUrl: BASE, token: initialA, tokenProvider: provider, fetch: engine.fetch });
    const b = new EmbedClient({ baseUrl: BASE, token: initialB, tokenProvider: provider, fetch: engine.fetch });
    await b.listRuns();
    expect((await a.listRuns()).items[0]?.id).toBe("a-private-run");
    expect(provider).toHaveBeenCalledTimes(2);
  });

  it.each([{ tid: "another-tenant" }, { sub: "another-customer" }])(
    "rejects a provider refresh which changes the configured principal: %j", async (principal) => {
      const provider = vi.fn(() => makeToken(principal));
      const engine = mockEngine({ "GET /runs": { body: { items: [] } } });
      const client = new EmbedClient({ baseUrl: BASE, token: makeToken({ exp: 1 }), tokenProvider: provider, fetch: engine.fetch });
      await expect(client.listRuns()).rejects.toMatchObject({ kind: "unauthorized", code: "token_scope_mismatch" });
      expect(engine.calls).toHaveLength(0);
    },
  );
});

describe("EmbedClient provider lifecycle", () => {
  it("recovers after a provider throws synchronously", async () => {
    const provider = vi.fn().mockImplementationOnce(() => { throw new Error("session unavailable"); }).mockReturnValue(makeToken());
    const engine = mockEngine({ "GET /runs": { body: { items: [] } } });
    const client = new EmbedClient({ baseUrl: BASE, tokenProvider: provider, fetch: engine.fetch });
    await expect(client.listRuns()).rejects.toThrow("session unavailable");
    await expect(client.listRuns()).resolves.toMatchObject({ items: [] });
    expect(provider).toHaveBeenCalledTimes(2);
  });

  it("isolates provider-only account sessions and supplies the requesting context", async () => {
    const provider = vi.fn(({ authContext }: { authContext?: string }) => makeToken({ sub: authContext }));
    const engine = mockEngine({ "GET /runs": { body: { items: [] } } });
    const a = new EmbedClient({ baseUrl: BASE, authContext: "account-a", tokenProvider: provider, fetch: engine.fetch });
    const b = new EmbedClient({ baseUrl: BASE, authContext: "account-b", tokenProvider: provider, fetch: engine.fetch });
    await Promise.all([a.listRuns(), b.listRuns()]);
    expect(provider).toHaveBeenCalledTimes(2);
    expect(provider).toHaveBeenCalledWith({ reason: "initial", baseUrl: BASE, authContext: "account-a" });
    expect(provider).toHaveBeenCalledWith({ reason: "initial", baseUrl: BASE, authContext: "account-b" });
    expect(engine.calls[0]?.auth).not.toBe(engine.calls[1]?.auth);
  });

  it("keeps distinct initial permission scopes in separate refresh groups", async () => {
    const narrow = makeToken({ scp: ["runs:read"] });
    const wide = makeToken();
    const provider = vi.fn().mockReturnValueOnce(narrow).mockReturnValueOnce(wide);
    const a = new EmbedClient({ baseUrl: BASE, token: makeToken({ exp: 1, scp: ["runs:read"] }), tokenProvider: provider });
    const b = new EmbedClient({ baseUrl: BASE, token: makeToken({ exp: 1 }), tokenProvider: provider });
    expect(await a.getToken()).toBe(narrow);
    expect(await b.getToken()).toBe(wide);
    expect(provider).toHaveBeenCalledTimes(2);
  });

  it("does not poison refresh state after a principal mismatch", async () => {
    const fresh = makeToken();
    const provider = vi.fn().mockReturnValueOnce(makeToken({ sub: "other" })).mockReturnValueOnce(fresh);
    const client = new EmbedClient({ baseUrl: BASE, token: makeToken({ exp: 1 }), tokenProvider: provider });
    await expect(client.getToken()).rejects.toMatchObject({ code: "token_scope_mismatch" });
    expect(await client.getToken()).toBe(fresh);
    expect(provider).toHaveBeenCalledTimes(2);
  });
});

describe("embed token payload shape", () => {
  it.each([
    { tid: null }, { sub: 3 }, { jti: null }, { iat: "now" },
    { exp: 1.5 }, { exp: 1e30 }, { scp: ["runs:read", 3] },
    { seq: "all" }, { seq: [null] }, { v: 2 },
  ])("treats a malformed payload as undecodable: %j", (invalid) => {
    const payload = { ...decodeEmbedToken(makeToken()), ...invalid };
    expect(decodeEmbedToken(`o8e1.${b64url(JSON.stringify(payload))}.sig`)).toBeNull();
  });
});

describe("long-lived provider context retention", () => {
  it("can safely refresh an evicted account context after many account switches", async () => {
    const provider = vi.fn(() => makeToken());
    for (let i = 0; i < 80; i++) {
      const client = new EmbedClient({ baseUrl: BASE, authContext: `account-${i}`, tokenProvider: provider });
      await client.getToken();
    }
    const original = new EmbedClient({ baseUrl: BASE, authContext: "account-0", tokenProvider: provider });
    await original.getToken();
    expect(provider).toHaveBeenCalledTimes(81);
  });
});

describe("refresh authorization continuity", () => {
  it("does not merge sequence-restricted and unrestricted refresh groups", async () => {
    const restricted = makeToken({ seq: ["allowed-sequence"] });
    const unrestricted = makeToken();
    const provider = vi.fn().mockReturnValueOnce(restricted).mockReturnValueOnce(unrestricted);
    const a = new EmbedClient({ baseUrl: BASE, token: makeToken({ exp: 1, seq: ["allowed-sequence"] }), tokenProvider: provider });
    const b = new EmbedClient({ baseUrl: BASE, token: makeToken({ exp: 1 }), tokenProvider: provider });
    expect(await a.getToken()).toBe(restricted);
    expect(await b.getToken()).toBe(unrestricted);
    expect(provider).toHaveBeenCalledTimes(2);
  });

  it("coalesces rotated initial credentials with the same authorization", async () => {
    const provider = vi.fn(() => makeToken());
    const clients = ["rotation-a", "rotation-b"].map((jti) => new EmbedClient({
      baseUrl: BASE, token: makeToken({ exp: 1, jti }), tokenProvider: provider,
    }));
    await Promise.all(clients.map((client) => client.getToken()));
    expect(provider).toHaveBeenCalledTimes(1);
  });

  it("does not replace a known principal with an unreadable credential", async () => {
    const provider = vi.fn(() => "opaque-token");
    const client = new EmbedClient({ baseUrl: BASE, token: makeToken({ exp: 1 }), tokenProvider: provider });
    await expect(client.getToken()).rejects.toMatchObject({ code: "token_scope_mismatch" });
  });

  it("does not extend the minimum refresh gap after a backwards clock jump", async () => {
    let now = Date.now();
    const provider = vi.fn(() => makeToken({ exp: Math.floor(now / 1000) + 10 }));
    const client = new EmbedClient({ baseUrl: BASE, tokenProvider: provider, now: () => now });
    await client.getToken();
    now -= 10_000;
    await client.getToken();
    expect(provider).toHaveBeenCalledTimes(2);
  });
});
