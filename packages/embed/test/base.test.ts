import { afterEach, describe, expect, it, vi } from "vitest";
import { clearThemeCache } from "../src/theme.js";
import { BASE, makeToken, mockEngine, mount, styleText, text, THEME, waitFor } from "./helpers.js";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const RUNS = {
  items: [{ id: "r1", sequence: "onboard", state: "running", created_at: "2026-09-28T10:00:00Z", updated_at: "2026-09-28T10:01:00Z", current_step: null }],
  next_cursor: null,
};
const APPROVAL = (id: string) => ({ id, instance_id: "r1", step_id: "s", prompt: `Prompt ${id}`, choices: [], created_at: "2026-09-28T10:00:00Z" });
const RUN = {
  id: "r1",
  sequence: "onboard",
  state: "running",
  steps: [{ id: "fetch", name: "Fetch", state: "completed", started_at: "2026-09-28T10:00:00Z", finished_at: "2026-09-28T10:00:01Z", output: { plan: "pro" } }],
};

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("lifecycle: disconnectedCallback cleanup", () => {
  it("aborts the in-flight request and removes the document listener", async () => {
    let signal: AbortSignal | undefined;
    const hang = vi.fn((_url: unknown, init?: RequestInit) => {
      signal = init?.signal ?? undefined;
      return new Promise<Response>(() => {});
    }) as unknown as typeof fetch;
    const removeSpy = vi.spyOn(document, "removeEventListener");
    const { el } = mount("orch8-runs", { "base-url": BASE, token: makeToken() }, { fetchImpl: hang });
    await waitFor(() => signal);
    expect(signal!.aborted).toBe(false);
    el.remove();
    expect(signal!.aborted).toBe(true);
    expect(removeSpy).toHaveBeenCalledWith("visibilitychange", expect.any(Function));
  });

  it("clears the poll timer so a removed element never fetches again", async () => {
    const engine = mockEngine({ "GET /runs": { body: RUNS }, "GET /theme": { body: THEME } });
    const { el, root } = mount("orch8-runs", { "base-url": BASE, token: makeToken(), "poll-interval": "1000" }, { fetchImpl: engine.fetch });
    await waitFor(() => root.querySelector("table"));
    await waitFor(() => (engine.calls.some((c) => c.path === "/theme") ? true : null));
    el.remove();
    const before = engine.calls.length;
    await sleep(1300);
    expect(engine.calls.length).toBe(before);
  });
});

describe("refresh-mode errors (EW-05)", () => {
  it("surfaces an expired session instead of 'Connection lost' forever", async () => {
    const engine = mockEngine({ "GET /runs": [{ body: RUNS }, { status: 401 }], "GET /theme": { body: THEME } });
    const { el, root } = mount("orch8-runs", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    await waitFor(() => root.querySelector("table"));
    el.refresh();
    const alert = await waitFor(() => root.querySelector('[role="alert"]'));
    expect(text(alert)).toContain("Your session has expired");
    expect(root.querySelector(".banner")).toBeNull();
    expect(text(root)).not.toContain("Connection lost");
  });

  it("surfaces not-found when the run disappears during polling", async () => {
    const engine = mockEngine({ "GET /runs/r1": [{ body: RUN }, { status: 404 }], "GET /theme": { body: THEME } });
    const { el, root } = mount("orch8-run-timeline", { "base-url": BASE, token: makeToken(), "run-id": "r1", "poll-interval": "0" }, { fetchImpl: engine.fetch });
    await waitFor(() => root.querySelector("ol.timeline"));
    el.refresh();
    const alert = await waitFor(() => root.querySelector('[role="alert"]'));
    expect(text(alert)).toContain("couldn't find");
    expect(root.querySelector(".banner")).toBeNull();
  });

  it("keeps the 'Connection lost' banner for transient failures", async () => {
    const throttled = { status: 429, headers: { "Retry-After": "0.001" } };
    const engine = mockEngine({ "GET /runs": [{ body: RUNS }, throttled, throttled, throttled, throttled], "GET /theme": { body: THEME } });
    const { el, root } = mount("orch8-runs", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    await waitFor(() => root.querySelector("table"));
    el.refresh();
    await waitFor(() => root.querySelector(".banner"));
    expect(root.querySelector("table")).not.toBeNull();
  });
});

describe("non-JSON 200 shows the error state (EW-01)", () => {
  it("runs: error, not 'No runs yet'", async () => {
    const engine = mockEngine({ "GET /runs": { raw: "<html>captive portal</html>" }, "GET /theme": { body: THEME } });
    const { root } = mount("orch8-runs", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    const alert = await waitFor(() => root.querySelector('[role="alert"]'));
    expect(text(alert)).toContain("Something went wrong");
    expect(text(root)).not.toContain("No runs yet");
  });

  it("approvals: error, not 'Nothing is waiting'", async () => {
    const engine = mockEngine({ "GET /approvals": { body: undefined }, "GET /theme": { body: THEME } });
    const { root } = mount("orch8-approvals", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    const alert = await waitFor(() => root.querySelector('[role="alert"]'));
    expect(text(alert)).toContain("Something went wrong");
    expect(root.querySelector('[part="empty"]')).toBeNull();
  });
});

describe("token attribute rotation (EW-02)", () => {
  it("swaps the credential without reloading or flashing when the identity is unchanged", async () => {
    const engine = mockEngine({ "GET /runs": { body: RUNS }, "GET /theme": { body: THEME } });
    const first = makeToken({ jti: "one" });
    const second = makeToken({ jti: "two" });
    const { el, root } = mount("orch8-runs", { "base-url": BASE, token: first, "poll-interval": "0" }, { fetchImpl: engine.fetch });
    const table = await waitFor(() => root.querySelector("table"));
    const loads = engine.calls.filter((c) => c.path === "/runs").length;
    el.setAttribute("token", second);
    await sleep(30);
    expect(engine.calls.filter((c) => c.path === "/runs").length).toBe(loads);
    expect(root.querySelector("table")).toBe(table);
    el.refresh();
    await waitFor(() => (engine.calls.filter((c) => c.path === "/runs").length > loads ? true : null));
    expect(engine.calls.filter((c) => c.path === "/runs").at(-1)!.auth).toBe(`Bearer ${second}`);
  });

  it("reloads when the token belongs to a different sub-tenant", async () => {
    const engine = mockEngine({ "GET /runs": { body: RUNS }, "GET /theme": { body: THEME } });
    const { el, root } = mount("orch8-runs", { "base-url": BASE, token: makeToken({ sub: "c1" }), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    await waitFor(() => root.querySelector("table"));
    const next = makeToken({ sub: "c2" });
    el.setAttribute("token", next);
    await waitFor(() => (engine.calls.filter((c) => c.path === "/runs").length === 2 ? true : null));
    expect(engine.calls.filter((c) => c.path === "/runs")[1]!.auth).toBe(`Bearer ${next}`);
  });

  it("recovers a broken view when a token arrives", async () => {
    const engine = mockEngine({ "GET /runs": { body: RUNS }, "GET /theme": { body: THEME } });
    const { el, root } = mount("orch8-runs", { "base-url": BASE, "poll-interval": "0" }, { fetchImpl: engine.fetch });
    await waitFor(() => root.querySelector('[role="alert"]'));
    el.setAttribute("token", makeToken());
    await waitFor(() => root.querySelector("table"));
  });

  it("builder keeps unsaved edits across a token or base-url change", async () => {
    const def = {
      name: "onboard",
      blocks: [
        { type: "step", id: "a", handler: "noop", params: {} },
        { type: "step", id: "b", handler: "noop", params: {} },
      ],
    };
    const engine = mockEngine({
      "GET /sequences": { body: { items: [], handlers: ["noop"] } },
      "GET /sequences/onboard": { body: { definition: def } },
      "GET /theme": { body: THEME },
    });
    const { el, root } = mount("orch8-builder", { "base-url": BASE, token: makeToken({ sub: "c1" }), sequence: "onboard" }, { fetchImpl: engine.fetch });
    await waitFor(() => root.querySelector("ol.steps"));
    root.querySelector<HTMLButtonElement>('button[aria-label="Move b up"]')!.click();
    expect(el.isDirty).toBe(true);
    const order = () => Array.from(root.querySelectorAll("li.step h3")).map((h) => text(h));
    const edited = order();
    expect(edited[0]).toContain("b");

    el.setAttribute("token", makeToken({ sub: "someone-else" }));
    el.setAttribute("base-url", "https://other.test");
    await sleep(50);
    expect(el.isDirty).toBe(true);
    expect(order()).toEqual(edited);
    expect(engine.calls.filter((c) => c.path === "/sequences/onboard")).toHaveLength(1);
  });

  it("approvals keeps a half-typed comment across a credential swap", async () => {
    const engine = mockEngine({ "GET /approvals": { body: { items: [APPROVAL("a1")] } }, "GET /theme": { body: THEME } });
    const { el, root } = mount("orch8-approvals", { "base-url": BASE, token: makeToken({ sub: "c1" }), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    const ta = await waitFor(() => root.querySelector<HTMLTextAreaElement>("textarea"));
    ta.value = "half typed";
    el.setAttribute("token", makeToken({ sub: "c2" }));
    await sleep(50);
    expect(root.querySelector("textarea")).toBe(ta);
    expect(ta.value).toBe("half typed");
    expect(engine.calls.filter((c) => c.path === "/approvals")).toHaveLength(1);
  });
});

describe("loadMore stale race (EW-10)", () => {
  it("drops a late page that belongs to the previous sub-tenant", async () => {
    const tokenA = makeToken({ sub: "a" });
    const tokenB = makeToken({ sub: "b" });
    let releaseOld!: () => void;
    const gate = new Promise<void>((r) => (releaseOld = r));
    const engine = mockEngine({
      "GET /runs": (call) =>
        call.auth === `Bearer ${tokenB}`
          ? { body: { items: [{ ...RUNS.items[0], id: "b1", sequence: "b-seq" }], next_cursor: null } }
          : call.url.includes("cursor=c2")
            ? { body: { items: [{ ...RUNS.items[0], id: "old-late", sequence: "old-seq" }], next_cursor: null } }
            : { body: { items: RUNS.items, next_cursor: "c2" } },
      "GET /theme": { body: THEME },
    });
    const delayed = (async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).includes("cursor=c2")) await gate;
      return engine.fetch(input, init);
    }) as typeof fetch;
    const { el, root } = mount("orch8-runs", { "base-url": BASE, token: tokenA, "poll-interval": "0" }, { fetchImpl: delayed });
    const more = await waitFor(() => root.querySelector<HTMLButtonElement>('button[data-key="more"]'));
    more.click();
    el.setAttribute("token", tokenB);
    await waitFor(() => root.querySelector('[data-key="run-b1"]'));
    releaseOld();
    await sleep(50);
    expect(root.querySelector('[data-key="run-old-late"]')).toBeNull();
    expect(root.querySelector('[data-key="run-r1"]')).toBeNull();
    expect(root.querySelector('[data-key="run-b1"]')).not.toBeNull();
  });
});

describe("reconnect banner vs approvals header (EW-08)", () => {
  it("renders a single #approvals-title after a failed refresh then a successful one", async () => {
    const throttled = { status: 429, headers: { "Retry-After": "0.001" } };
    const engine = mockEngine({
      "GET /approvals": [{ body: { items: [APPROVAL("a1")] } }, throttled, throttled, throttled, throttled, { body: { items: [APPROVAL("a1"), APPROVAL("a2")] } }],
      "GET /theme": { body: THEME },
    });
    const { el, root } = mount("orch8-approvals", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    await waitFor(() => root.querySelector("li.card"));
    el.refresh();
    await waitFor(() => root.querySelector(".banner"));
    el.refresh();
    await waitFor(() => (root.querySelectorAll("li.card").length === 2 ? true : null));
    expect(root.querySelectorAll("#approvals-title")).toHaveLength(1);
    expect(root.querySelector(".banner")).toBeNull();
  });
});

describe("run timeline refresh (EW-09)", () => {
  it("leaves the DOM alone when a poll returns identical data", async () => {
    const engine = mockEngine({ "GET /runs/r1": { body: RUN }, "GET /theme": { body: THEME } });
    const { el, root } = mount("orch8-run-timeline", { "base-url": BASE, token: makeToken(), "run-id": "r1", "poll-interval": "0" }, { fetchImpl: engine.fetch });
    const list = await waitFor(() => root.querySelector("ol.timeline"));
    el.refresh();
    await waitFor(() => (engine.calls.filter((c) => c.path === "/runs/r1").length === 2 ? true : null));
    await sleep(20);
    expect(root.querySelector("ol.timeline")).toBe(list);
  });

  it("restores focus on the output pane when the run did change", async () => {
    const changed = { ...RUN, steps: [{ ...RUN.steps[0], output: { plan: "enterprise" } }] };
    const engine = mockEngine({ "GET /runs/r1": [{ body: RUN }, { body: changed }], "GET /theme": { body: THEME } });
    const { el, root } = mount("orch8-run-timeline", { "base-url": BASE, token: makeToken(), "run-id": "r1", "poll-interval": "0" }, { fetchImpl: engine.fetch });
    const pre = await waitFor(() => root.querySelector<HTMLElement>("pre"));
    pre.focus();
    expect(root.activeElement).toBe(pre);
    el.refresh();
    await waitFor(() => (root.querySelector("pre")?.textContent?.includes("enterprise") ? true : null));
    expect(root.activeElement?.getAttribute("data-key")).toBe("pre-fetch");
  });
});

describe("strict CSP (EW-06)", () => {
  it("uses no inline style attributes", async () => {
    const engine = mockEngine({ "GET /runs": { body: { items: [] } }, "GET /theme": { body: THEME } });
    const { root } = mount("orch8-runs", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    await waitFor(() => root.querySelector('[part="empty"]'));
    expect(root.querySelectorAll("[style]")).toHaveLength(0);
    expect(styleText(root)).toContain(".empty-title");
  });

  it("adopts constructable stylesheets when supported (no <style> element)", async () => {
    const engine = mockEngine({ "GET /runs": { body: RUNS }, "GET /theme": { body: THEME } });
    const { root } = mount("orch8-runs", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    await waitFor(() => root.querySelector("table"));
    if (typeof CSSStyleSheet === "function" && "adoptedStyleSheets" in root) {
      expect(root.adoptedStyleSheets.length).toBe(2);
      expect(root.querySelector("style")).toBeNull();
    }
  });

  it("falls back to <style nonce> and follows nonce changes", async () => {
    vi.stubGlobal("CSSStyleSheet", undefined);
    const engine = mockEngine({ "GET /runs": { body: RUNS }, "GET /theme": { body: THEME } });
    const { el, root } = mount(
      "orch8-runs",
      { "base-url": BASE, token: makeToken(), "poll-interval": "0", nonce: "abc123", theme: '{"accent":"#123456"}' },
      { fetchImpl: engine.fetch },
    );
    await waitFor(() => root.querySelector("table"));
    const styles = Array.from(root.querySelectorAll("style"));
    expect(styles.length).toBe(2);
    expect(styles.every((s) => s.getAttribute("nonce") === "abc123")).toBe(true);
    expect(styleText(root)).toContain("--orch8-accent: #123456;");
    el.setAttribute("nonce", "next");
    expect(Array.from(root.querySelectorAll("style")).every((s) => s.getAttribute("nonce") === "next")).toBe(true);
  });
});

describe("vendor logo (EW-17)", () => {
  it("sends no referrer and refuses plain http logos", async () => {
    const themed = (logo: string) =>
      mockEngine({ "GET /runs": { body: RUNS }, "GET /theme": { body: { css_vars: {}, logo_url: logo, hide_badge: false } } });
    const https = themed("https://cdn.acme.com/l.png");
    const a = mount("orch8-runs", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: https.fetch });
    const img = await waitFor(() => a.root.querySelector<HTMLImageElement>("img.logo"));
    expect(img.getAttribute("referrerpolicy")).toBe("no-referrer");

    clearThemeCache();
    const http = themed("http://cdn.acme.com/l.png");
    const b = mount("orch8-runs", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: http.fetch });
    await waitFor(() => b.root.querySelector("table"));
    await waitFor(() => (http.calls.some((c) => c.path === "/theme") ? true : null));
    await sleep(20);
    expect(b.root.querySelector("img.logo")).toBeNull();
  });
});

describe("strings assignment (EW-03)", () => {
  it("ignores shallow-equal strings so half-typed input survives re-renders", async () => {
    const engine = mockEngine({ "GET /approvals": { body: { items: [APPROVAL("a1")] } }, "GET /theme": { body: THEME } });
    const { el, root } = mount("orch8-approvals", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    await waitFor(() => root.querySelector("textarea"));
    el.strings = { approvalsSubmit: "Go" };
    const ta = root.querySelector<HTMLTextAreaElement>("textarea")!;
    ta.value = "typing";
    el.strings = { approvalsSubmit: "Go" };
    expect(root.querySelector("textarea")).toBe(ta);
    expect(ta.value).toBe("typing");
  });
});
