import { describe, expect, it, vi } from "vitest";
import { Orch8Client, Orch8Error } from "./client.js";
import { workflow } from "./builder.js";

function mockFetch(status: number, body: unknown): typeof fetch {
  return vi.fn(async () => {
    return new Response(JSON.stringify(body), {
      status,
      headers: { "content-type": "application/json" },
    });
  }) as unknown as typeof fetch;
}

describe("Orch8Client", () => {
  it("sends tenant header and auth bearer on createSequence", async () => {
    const fetchFn = vi.fn(async (input: Request | URL | string, init?: RequestInit) => {
      const headers = new Headers(init?.headers);
      expect(headers.get("x-tenant-id")).toBe("acme");
      expect(headers.get("authorization")).toBe("Bearer secret");
      expect(String(input)).toBe("https://api.example.com/sequences");
      expect(init?.method).toBe("POST");
      return new Response(JSON.stringify({ id: "seq_1", version: 1 }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }) as unknown as typeof fetch;

    const client = new Orch8Client({
      baseUrl: "https://api.example.com",
      tenantId: "acme",
      apiKey: "secret",
      fetch: fetchFn,
    });

    const res = await client.createSequence(
      workflow("demo").step("s", "handler"),
    );
    expect(res).toEqual({ id: "seq_1", version: 1 });
  });

  it("throws Orch8Error on non-2xx", async () => {
    const client = new Orch8Client({
      baseUrl: "https://api.example.com",
      tenantId: "acme",
      fetch: mockFetch(500, { error: "boom" }),
    });

    await expect(client.getInstance("i1")).rejects.toBeInstanceOf(Orch8Error);
  });

  it("strips trailing slash from base URL", async () => {
    const fetchFn = vi.fn(async (input: Request | URL | string) => {
      expect(String(input)).toBe("https://api.example.com/instances/i1");
      return new Response(JSON.stringify({}), { status: 200 });
    }) as unknown as typeof fetch;

    const client = new Orch8Client({
      baseUrl: "https://api.example.com/",
      tenantId: "t",
      fetch: fetchFn,
    });
    await client.getInstance("i1");
  });
});
