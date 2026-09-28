import { describe, expect, it, vi } from "vitest";
import { getConfig } from "@/lib/config";
import { EmbedTokenError, mintEmbedToken, mockToken } from "@/lib/embed-token";
import { DEMO_USERS, scopesFor, subTenantFor } from "@/lib/users";

const [jane, raj] = DEMO_USERS as [(typeof DEMO_USERS)[0], (typeof DEMO_USERS)[0]];
const live = getConfig({ ORCH8_URL: "http://engine:8080/api/v1/", ORCH8_API_KEY: "sk_secret", ORCH8_TENANT_ID: "acme", ORCH8_EMBED_SEQUENCES: "customer-onboarding, other" });

describe("config", () => {
  it("defaults to mock mode and strips /api/v1 from ORCH8_URL", () => {
    expect(getConfig({}).mode).toBe("mock");
    expect(getConfig({}).publicUrl).toBe("/mock");
    expect(live).toMatchObject({ mode: "live", url: "http://engine:8080", publicUrl: "http://engine:8080", sequences: ["customer-onboarding", "other"] });
  });
});

describe("users", () => {
  it("maps the customer org to a valid sub-tenant and role to scopes", () => {
    expect(subTenantFor(jane)).toBe("org:northwind");
    expect(subTenantFor(jane)).toMatch(/^[A-Za-z0-9._:-]{1,128}$/);
    expect(scopesFor(jane)).toContain("builder:edit");
    expect(scopesFor(raj)).not.toContain("builder:edit");
  });
});

describe("mintEmbedToken", () => {
  it("POSTs /api/v1/embed/tokens with the server-side API key and returns only the token", async () => {
    const fetchMock = vi.fn(async () => new Response(JSON.stringify({ token: "o8e1.a.b", expires_at: "2026-09-28T12:00:00Z" }), { status: 200 }));
    const minted = await mintEmbedToken(jane, live, fetchMock as unknown as typeof fetch);
    expect(minted).toEqual({ token: "o8e1.a.b", expires_at: "2026-09-28T12:00:00Z" });
    expect(JSON.stringify(minted)).not.toContain("sk_secret");
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("http://engine:8080/api/v1/embed/tokens");
    expect(init.method).toBe("POST");
    expect(init.headers).toMatchObject({ "x-api-key": "sk_secret", "x-tenant-id": "acme" });
    expect(JSON.parse(String(init.body))).toEqual({
      sub_tenant: "org:northwind",
      scopes: ["runs:read", "runs:start", "approvals:resolve", "sequences:read", "builder:edit"],
      sequences: ["customer-onboarding", "other"],
      ttl_seconds: 900,
    });
  });

  it("maps engine failures to a 502 with an actionable hint", async () => {
    const fetchMock = vi.fn(async () => new Response("{}", { status: 404 }));
    const err = await mintEmbedToken(raj, live, fetchMock as unknown as typeof fetch).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(EmbedTokenError);
    expect((err as EmbedTokenError).status).toBe(502);
    expect((err as Error).message).toContain("ORCH8_EMBED_TOKEN_SECRET");
  });

  it("refuses to run live without an API key", async () => {
    await expect(mintEmbedToken(jane, { ...live, apiKey: "" })).rejects.toMatchObject({ status: 500 });
  });

  it("mints a decodable mock token in mock mode without calling fetch", async () => {
    const fetchMock = vi.fn();
    const { token } = await mintEmbedToken(raj, getConfig({}), fetchMock as unknown as typeof fetch);
    expect(fetchMock).not.toHaveBeenCalled();
    const payload = JSON.parse(Buffer.from(token.split(".")[1]!, "base64url").toString());
    expect(payload).toMatchObject({ v: 1, sub: "org:northwind", scp: scopesFor(raj) });
    expect(payload.exp - payload.iat).toBe(900);
    expect(mockToken(jane, getConfig({})).token.startsWith("o8e1.")).toBe(true);
  });
});

describe("POST /api/orch8/embed-token", () => {
  it("returns a token for the signed-in user and refuses cross-origin calls", async () => {
    vi.resetModules();
    vi.doMock("next/headers", () => ({ cookies: async () => ({ get: () => ({ value: "u_li" }) }) }));
    const { POST } = await import("@/app/api/orch8/embed-token/route");
    const { NextRequest } = await import("next/server");
    delete process.env.ORCH8_URL;

    const ok = await POST(new NextRequest("http://localhost:3000/api/orch8/embed-token", { method: "POST", headers: { origin: "http://localhost:3000" } }));
    expect(ok.status).toBe(200);
    expect(ok.headers.get("cache-control")).toBe("no-store");
    const body = (await ok.json()) as { token: string };
    const payload = JSON.parse(Buffer.from(body.token.split(".")[1]!, "base64url").toString());
    expect(payload.sub).toBe("org:globex");

    const evil = await POST(new NextRequest("http://localhost:3000/api/orch8/embed-token", { method: "POST", headers: { origin: "https://evil.example" } }));
    expect(evil.status).toBe(403);
    vi.doUnmock("next/headers");
  });
});
