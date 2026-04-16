import { describe, it, expect, vi, beforeEach } from "vitest";
import { Orch8Client, Orch8Error } from "../client.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const mockFetch = vi.fn();
globalThis.fetch = mockFetch;

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(JSON.stringify(body)),
  } as unknown as Response;
}

function noContentResponse(): Response {
  return {
    ok: true,
    status: 204,
    json: () => Promise.reject(new Error("no body")),
    text: () => Promise.resolve(""),
  } as unknown as Response;
}

function errorResponse(status: number, body: unknown = { error: "not found" }): Response {
  return {
    ok: false,
    status,
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(JSON.stringify(body)),
  } as unknown as Response;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Orch8Client", () => {
  let client: Orch8Client;

  beforeEach(() => {
    mockFetch.mockReset();
    client = new Orch8Client({
      baseUrl: "http://localhost:8080",
      tenantId: "tenant-1",
      namespace: "default",
    });
  });

  // ---- Header tests -------------------------------------------------------

  it("sends X-Tenant-Id header when configured", async () => {
    mockFetch.mockResolvedValueOnce(jsonResponse({ id: "seq-1" }));
    await client.getSequence("seq-1");

    const [, init] = mockFetch.mock.calls[0];
    expect(init.headers["X-Tenant-Id"]).toBe("tenant-1");
  });

  it("does not send X-Tenant-Id header when not configured", async () => {
    const bare = new Orch8Client({ baseUrl: "http://localhost:8080" });
    mockFetch.mockResolvedValueOnce(jsonResponse({ id: "seq-1" }));
    await bare.getSequence("seq-1");

    const [, init] = mockFetch.mock.calls[0];
    expect(init.headers["X-Tenant-Id"]).toBeUndefined();
  });

  // ---- Sequences ----------------------------------------------------------

  it("createSequence POSTs to /sequences", async () => {
    const seq = { id: "seq-1", name: "my-seq", version: 1 };
    mockFetch.mockResolvedValueOnce(jsonResponse(seq));

    const result = await client.createSequence({ name: "my-seq", blocks: [] });

    const [url, init] = mockFetch.mock.calls[0];
    expect(url).toBe("http://localhost:8080/sequences");
    expect(init.method).toBe("POST");
    expect(result).toEqual(seq);
  });

  it("getSequence GETs /sequences/:id", async () => {
    const seq = { id: "seq-1", name: "my-seq" };
    mockFetch.mockResolvedValueOnce(jsonResponse(seq));

    const result = await client.getSequence("seq-1");

    const [url, init] = mockFetch.mock.calls[0];
    expect(url).toBe("http://localhost:8080/sequences/seq-1");
    expect(init.method).toBe("GET");
    expect(result).toEqual(seq);
  });

  // ---- Instances ----------------------------------------------------------

  it("createInstance POSTs to /instances", async () => {
    const inst = { id: "inst-1", state: "pending" };
    mockFetch.mockResolvedValueOnce(jsonResponse(inst));

    const result = await client.createInstance({ sequence_id: "seq-1" });

    const [url, init] = mockFetch.mock.calls[0];
    expect(url).toBe("http://localhost:8080/instances");
    expect(init.method).toBe("POST");
    expect(result).toEqual(inst);
  });

  it("getInstance GETs /instances/:id", async () => {
    const inst = { id: "inst-1", state: "running" };
    mockFetch.mockResolvedValueOnce(jsonResponse(inst));

    const result = await client.getInstance("inst-1");

    const [url] = mockFetch.mock.calls[0];
    expect(url).toBe("http://localhost:8080/instances/inst-1");
    expect(result).toEqual(inst);
  });

  it("retryInstance POSTs to /instances/:id/retry", async () => {
    const inst = { id: "inst-1", state: "pending" };
    mockFetch.mockResolvedValueOnce(jsonResponse(inst));

    const result = await client.retryInstance("inst-1");

    const [url, init] = mockFetch.mock.calls[0];
    expect(url).toBe("http://localhost:8080/instances/inst-1/retry");
    expect(init.method).toBe("POST");
    expect(result).toEqual(inst);
  });

  // ---- Triggers -----------------------------------------------------------

  it("createTrigger POSTs to /triggers", async () => {
    const trigger = { slug: "t-1", sequence_name: "my-seq" };
    mockFetch.mockResolvedValueOnce(jsonResponse(trigger));

    const result = await client.createTrigger({ slug: "t-1", sequence_name: "my-seq" });

    const [url, init] = mockFetch.mock.calls[0];
    expect(url).toBe("http://localhost:8080/triggers");
    expect(init.method).toBe("POST");
    expect(result).toEqual(trigger);
  });

  it("getTrigger GETs /triggers/:slug", async () => {
    const trigger = { slug: "t-1" };
    mockFetch.mockResolvedValueOnce(jsonResponse(trigger));

    const result = await client.getTrigger("t-1");

    const [url] = mockFetch.mock.calls[0];
    expect(url).toBe("http://localhost:8080/triggers/t-1");
    expect(result).toEqual(trigger);
  });

  it("fireTrigger POSTs to /triggers/:slug/fire", async () => {
    const inst = { id: "inst-1" };
    mockFetch.mockResolvedValueOnce(jsonResponse(inst));

    const result = await client.fireTrigger("t-1", { data: "hello" });

    const [url, init] = mockFetch.mock.calls[0];
    expect(url).toBe("http://localhost:8080/triggers/t-1/fire");
    expect(init.method).toBe("POST");
    expect(result).toEqual(inst);
  });

  // ---- Error handling -----------------------------------------------------

  it("throws Orch8Error on 404", async () => {
    mockFetch.mockResolvedValueOnce(errorResponse(404));

    await expect(client.getSequence("nonexistent")).rejects.toThrow(Orch8Error);

    try {
      mockFetch.mockResolvedValueOnce(errorResponse(404));
      await client.getSequence("nonexistent");
    } catch (err) {
      expect(err).toBeInstanceOf(Orch8Error);
      const orch8Err = err as Orch8Error;
      expect(orch8Err.status).toBe(404);
      expect(orch8Err.path).toBe("/sequences/nonexistent");
    }
  });

  // ---- 204 No Content -----------------------------------------------------

  it("handles 204 No Content (deleteTrigger returns undefined)", async () => {
    mockFetch.mockResolvedValueOnce(noContentResponse());

    const result = await client.deleteTrigger("t-1");

    expect(result).toBeUndefined();
    const [url, init] = mockFetch.mock.calls[0];
    expect(url).toBe("http://localhost:8080/triggers/t-1");
    expect(init.method).toBe("DELETE");
  });
});
