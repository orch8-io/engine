import { beforeEach, describe, expect, it } from "vitest";
import { getConfig } from "@/lib/config";
import { mockToken } from "@/lib/embed-token";
import { handleMock, resetMockEngine } from "@/lib/mock-engine";
import { DEMO_USERS } from "@/lib/users";

const [jane, raj, li] = DEMO_USERS as [(typeof DEMO_USERS)[0], (typeof DEMO_USERS)[0], (typeof DEMO_USERS)[0]];
const auth = (u: (typeof DEMO_USERS)[0]) => `Bearer ${mockToken(u, getConfig({})).token}`;
const q = new URLSearchParams();

describe("mock embed engine", () => {
  beforeEach(() => resetMockEngine());

  it("rejects missing tokens and isolates sub-tenants", () => {
    expect(handleMock("GET", ["runs"], null, undefined, q).status).toBe(401);
    const started = handleMock("POST", ["runs"], auth(jane), { sequence: "customer-onboarding", input: {} }, q);
    expect(started.status).toBe(201);
    const id = (started.body as { id: string }).id;
    expect(handleMock("GET", ["runs", id], auth(raj), undefined, q).status).toBe(200); // same org
    expect(handleMock("GET", ["runs", id], auth(li), undefined, q).status).toBe(404); // other org
  });

  it("lists a pending approval and resolves it once", () => {
    const list = handleMock("GET", ["approvals"], auth(jane), undefined, q).body as { items: { id: string; choices: unknown[] }[] };
    expect(list.items).toHaveLength(1);
    expect(list.items[0]!.choices).toHaveLength(2);
    const id = list.items[0]!.id;
    expect(handleMock("POST", ["approvals", id], auth(jane), { choice: "activate", comment: "ok" }, q).status).toBe(200);
    expect(handleMock("POST", ["approvals", id], auth(jane), { choice: "activate" }, q).status).toBe(409);
  });

  it("enforces builder:edit on PUT and round-trips the definition", () => {
    const got = handleMock("GET", ["sequences", "customer-onboarding"], auth(raj), undefined, q).body as { definition: { blocks: unknown[] } };
    const def = { ...got.definition, blocks: got.definition.blocks.slice(0, 2) };
    expect(handleMock("PUT", ["sequences", "customer-onboarding"], auth(raj), def, q).status).toBe(403);
    const saved = handleMock("PUT", ["sequences", "customer-onboarding"], auth(jane), def, q);
    expect(saved.status).toBe(200);
    expect((saved.body as { definition: { blocks: unknown[] } }).definition.blocks).toHaveLength(2);
  });
});
