import { describe, expect, it } from "vitest";
import { BASE, makeToken, mockEngine, mount, text, THEME, waitFor } from "./helpers.js";

const tokenA = makeToken({ sub: "account-a" });
const tokenB = makeToken({ sub: "account-b" });
const definition = (id: string) => ({ name: "flow", blocks: [{ type: "step", id, handler: "noop", params: {} }] });
const approval = (prompt: string) => ({ id: "shared-id", instance_id: "run", step_id: "review", prompt, choices: [], created_at: "" });
function gate() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => { release = resolve; });
  return { promise, release };
}

describe("component account ownership", () => {
  it.each(["token", "base-url"])("clears dirty builder state synchronously on %s switches", async (attribute) => {
    const next = gate();
    const engine = mockEngine({
      "GET /sequences": { body: { items: [], handlers: ["noop"] } },
      "GET /sequences/flow": (call) => ({ body: { definition: definition(call.auth === `Bearer ${tokenA}` && new URL(call.url).origin === BASE ? "private-a" : "private-b") } }),
      "PUT /sequences/flow": { body: {} }, "GET /theme": { body: THEME },
    });
    const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
      if (init?.method === "GET" && String(input).endsWith("/sequences/flow") && (new Headers(init.headers).get("authorization") === `Bearer ${tokenB}` || new URL(String(input)).origin !== BASE)) await next.promise;
      return engine.fetch(input, init);
    }) as typeof fetch;
    const { el, root } = mount("orch8-builder", { "base-url": BASE, token: tokenA, sequence: "flow" }, { fetchImpl });
    await waitFor(() => root.querySelector("ol.steps"));
    const field = root.querySelector<HTMLInputElement>('input[data-field="id"]') ?? root.querySelector<HTMLInputElement>("input");
    field!.value = "secret-edit-a";
    field!.dispatchEvent(new Event("input", { bubbles: true }));
    expect(el.isDirty).toBe(true);
    el.setAttribute(attribute, attribute === "token" ? tokenB : "https://other.example");
    const dirtyAfterSwitch = el.isDirty;
    const visibleAfterSwitch = text(root);
    const saved = await el.save();
    next.release();
    expect(dirtyAfterSwitch).toBe(false);
    expect(visibleAfterSwitch).not.toContain("private-a");
    expect(saved).toBe(false);
    expect(engine.calls.filter((call) => call.method === "PUT")).toHaveLength(0);
    await waitFor(() => text(root).includes("private-b") ? true : null);
  });

  it("does not transfer an approval comment to another account's matching approval id", async () => {
    const engine = mockEngine({ "GET /approvals": (call) => ({ body: { items: [approval(call.auth === `Bearer ${tokenA}` ? "private-a" : "private-b")] } }), "GET /theme": { body: THEME } });
    const { el, root } = mount("orch8-approvals", { "base-url": BASE, token: tokenA, "poll-interval": "0" }, { fetchImpl: engine.fetch });
    const comment = await waitFor(() => root.querySelector<HTMLTextAreaElement>("textarea"));
    comment.value = "account-a confidential comment";
    el.setAttribute("token", tokenB);
    expect(text(root)).not.toContain("private-a");
    await waitFor(() => engine.calls.filter((call) => call.path === "/approvals").length === 2 ? true : null);
    await waitFor(() => text(root).includes("private-b") ? true : null);
    expect(root.querySelector("textarea")).not.toBe(comment);
    expect(root.querySelector<HTMLTextAreaElement>("textarea")?.value).toBe("");
  });

  it("drops late initial reads even when the transport ignores cancellation", async () => {
    const old = gate();
    const entered = gate();
    const engine = mockEngine({ "GET /runs": (call) => ({ body: { items: [{ id: "run", sequence: call.auth === `Bearer ${tokenA}` ? "private-a" : "private-b", state: "running", created_at: "", updated_at: "" }] } }), "GET /theme": { body: THEME } });
    const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
      if (new URL(String(input)).pathname.endsWith("/runs") && new Headers(init?.headers).get("authorization") === `Bearer ${tokenA}`) {
        entered.release(); await old.promise;
      }
      return engine.fetch(input, init);
    }) as typeof fetch;
    const { el, root } = mount("orch8-runs", { "base-url": BASE, token: tokenA, "poll-interval": "0" }, { fetchImpl });
    await entered.promise;
    el.setAttribute("token", tokenB);
    await waitFor(() => text(root).includes("private-b") ? true : null);
    old.release();
    await waitFor(() => engine.calls.some((call) => call.path === "/runs" && call.auth === `Bearer ${tokenA}`) ? true : null);
    await new Promise((resolve) => setTimeout(resolve, 0));
    el.setAttribute("locale", "en"); // force a render from the retained model
    expect(text(root)).toContain("private-b");
    expect(text(root)).not.toContain("private-a");
  });

  it("does not apply an old save to another account using the same sequence name", async () => {
    const old = gate();
    const engine = mockEngine({
      "GET /sequences": { body: { items: [], handlers: ["noop"] } },
      "GET /sequences/flow": (call) => ({ body: { definition: definition(call.auth === `Bearer ${tokenA}` ? "private-a" : "private-b") } }),
      "PUT /sequences/flow": { body: { definition: definition("private-a-saved") } }, "GET /theme": { body: THEME },
    });
    const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
      if (init?.method === "PUT") await old.promise;
      return engine.fetch(input, init);
    }) as typeof fetch;
    const { el, root } = mount("orch8-builder", { "base-url": BASE, token: tokenA, sequence: "flow" }, { fetchImpl });
    await waitFor(() => root.querySelector("ol.steps"));
    const events: unknown[] = [];
    el.addEventListener("orch8-sequence-saved", (event) => events.push((event as CustomEvent).detail));
    const saving = el.save();
    el.setAttribute("token", tokenB);
    await waitFor(() => text(root).includes("private-b") ? true : null);
    old.release();
    expect(await saving).toBe(false);
    expect(text(root)).toContain("private-b");
    expect(text(root)).not.toContain("private-a");
    expect(events).toEqual([]);
  });
});

describe("late mutation ownership", () => {
  it("does not remove a new account's approval or emit its predecessor's comment", async () => {
    const old = gate();
    const engine = mockEngine({
      "GET /approvals": (call) => ({ body: { items: [approval(call.auth === `Bearer ${tokenA}` ? "private-a" : "private-b")] } }),
      "POST /approvals/shared-id": { body: {} }, "GET /theme": { body: THEME },
    });
    const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
      if (init?.method === "POST") await old.promise;
      return engine.fetch(input, init);
    }) as typeof fetch;
    const { el, root } = mount("orch8-approvals", { "base-url": BASE, token: tokenA, "poll-interval": "0" }, { fetchImpl });
    const comment = await waitFor(() => root.querySelector<HTMLTextAreaElement>("textarea"));
    comment.value = "private comment a";
    root.querySelector<HTMLInputElement>('input[type="radio"]')!.checked = true;
    const events: unknown[] = [];
    el.addEventListener("orch8-approval-resolved", (event) => events.push((event as CustomEvent).detail));
    const pending = el.resolve("shared-id");
    el.setAttribute("token", tokenB);
    await waitFor(() => text(root).includes("private-b") ? true : null);
    old.release();
    await pending;
    expect(text(root)).toContain("private-b");
    expect(root.querySelector("form")).not.toBeNull();
    expect(events).toEqual([]);
  });

  it("does not emit a run-start result into a different account", async () => {
    const old = gate();
    const engine = mockEngine({ "GET /runs": { body: { items: [] } }, "POST /runs": { body: { id: "private-a-started" } }, "GET /theme": { body: THEME } });
    const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
      if (init?.method === "POST") await old.promise;
      return engine.fetch(input, init);
    }) as typeof fetch;
    const { el } = mount("orch8-runs", { "base-url": BASE, token: tokenA, "poll-interval": "0" }, { fetchImpl });
    await waitFor(() => engine.calls.some((call) => call.path === "/runs") ? true : null);
    const events: unknown[] = [];
    el.addEventListener("orch8-run-started", (event) => events.push((event as CustomEvent).detail));
    const pending = el.startRun("flow");
    const checked = expect(pending).rejects.toMatchObject({ kind: "aborted" });
    el.setAttribute("token", tokenB);
    old.release();
    await checked;
    expect(events).toEqual([]);
  });
});

describe("theme result ownership", () => {
  it("does not apply a late theme from the previous account", async () => {
    const old = gate();
    const entered = gate();
    const engine = mockEngine({ "GET /runs": { body: { items: [] } }, "GET /theme": (call) => ({ body: { ...THEME, logo_url: call.auth === `Bearer ${tokenA}` ? "https://cdn.example/private-a.png" : "https://cdn.example/private-b.png" } }) });
    const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).endsWith("/theme") && new Headers(init?.headers).get("authorization") === `Bearer ${tokenA}`) {
        entered.release(); await old.promise;
      }
      return engine.fetch(input, init);
    }) as typeof fetch;
    const { el, root } = mount("orch8-runs", { "base-url": BASE, token: tokenA, "poll-interval": "0" }, { fetchImpl });
    await entered.promise;
    el.setAttribute("token", tokenB);
    await waitFor(() => root.querySelector<HTMLImageElement>("img.logo")?.src.includes("private-b") ? true : null);
    old.release();
    await waitFor(() => engine.calls.some((call) => call.path === "/theme" && call.auth === `Bearer ${tokenA}`) ? true : null);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(root.querySelector<HTMLImageElement>("img.logo")?.src).toContain("private-b");
  });
});

it("reloads a ready widget when its provider identity changes", async () => {
  const engine = mockEngine({ "GET /runs": (call) => ({ body: { items: [{ id: "run", sequence: call.auth === `Bearer ${tokenA}` ? "private-a" : "private-b", state: "running", created_at: "", updated_at: "" }] } }), "GET /theme": { body: THEME } });
  const { el, root } = mount("orch8-runs", { "base-url": BASE, "poll-interval": "0" }, { fetchImpl: engine.fetch, tokenProvider: () => tokenA });
  await waitFor(() => text(root).includes("private-a") ? true : null);
  el.tokenProvider = () => tokenB;
  expect(text(root)).not.toContain("private-a");
  await waitFor(() => text(root).includes("private-b") ? true : null);
});
