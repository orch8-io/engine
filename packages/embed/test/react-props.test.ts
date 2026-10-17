import { createElement, act } from "react";
import { createRoot } from "react-dom/client";
import { renderToString } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";
import { Orch8Approvals, Orch8Runs } from "../src/react.js";
import { BASE, makeToken, mockEngine, THEME, waitFor } from "./helpers.js";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const RUNS = { items: [{ id: "r1", sequence: "onboard", state: "running", created_at: "", updated_at: "" }], next_cursor: null };

function host() {
  const container = document.createElement("div");
  document.body.appendChild(container);
  return { container, root: createRoot(container) };
}

describe("React wrapper prop stability (EW-03, EW-04, EW-11)", () => {
  it("does not reload when inline tokenProvider/strings literals change identity every render", async () => {
    const engine = mockEngine({ "GET /runs": { body: RUNS }, "GET /theme": { body: THEME } });
    const token = makeToken();
    const { container, root } = host();
    const render = (tag: string) =>
      root.render(
        createElement(Orch8Runs, {
          baseUrl: BASE,
          pollInterval: 0,
          fetchImpl: engine.fetch,
          tokenProvider: () => `${token}${tag === "never" ? "x" : ""}`,
          strings: { runsTitle: "My runs" },
        }),
      );
    await act(async () => render("a"));
    const el = container.querySelector("orch8-runs") as unknown as { tokenProvider: unknown; shadowRoot: ShadowRoot };
    await waitFor(() => el.shadowRoot.querySelector("table"));
    const provider = el.tokenProvider;
    const table = el.shadowRoot.querySelector("table");
    const loads = engine.calls.filter((c) => c.path === "/runs").length;
    for (let i = 0; i < 5; i++) await act(async () => render(`r${i}`));
    await new Promise((r) => setTimeout(r, 30));
    expect(el.tokenProvider).toBe(provider); // stable trampoline, no invalidate()
    expect(engine.calls.filter((c) => c.path === "/runs")).toHaveLength(loads);
    expect(el.shadowRoot.querySelector("table")).toBe(table);
    expect(el.shadowRoot.getElementById("runs-title")?.textContent).toBe("My runs");
    await act(async () => root.unmount());
  });

  it("the trampoline always calls the latest tokenProvider", async () => {
    const engine = mockEngine({ "GET /runs": { body: RUNS }, "GET /theme": { body: THEME } });
    const { container, root } = host();
    const first = vi.fn(() => makeToken({ jti: "first" }));
    const second = vi.fn(() => makeToken({ jti: "second" }));
    await act(async () => root.render(createElement(Orch8Runs, { baseUrl: BASE, pollInterval: 0, fetchImpl: engine.fetch, tokenProvider: first })));
    const el = container.querySelector("orch8-runs") as unknown as { tokenProvider: (c: { reason: string }) => unknown };
    await act(async () => root.render(createElement(Orch8Runs, { baseUrl: BASE, pollInterval: 0, fetchImpl: engine.fetch, tokenProvider: second })));
    await el.tokenProvider({ reason: "expiring" });
    expect(second).toHaveBeenCalledWith({ reason: "expiring" });
    await act(async () => root.unmount());
  });

  it("assigns props before the first load so there is no 'not configured' flash", async () => {
    const engine = mockEngine({ "GET /approvals": { body: { items: [] } }, "GET /theme": { body: THEME } });
    const { container, root } = host();
    const seen: string[] = [];
    const observer = new MutationObserver(() => {
      const sr = container.querySelector("orch8-approvals")?.shadowRoot;
      if (sr?.querySelector('[role="alert"]')) seen.push("alert");
    });
    await act(async () => {
      root.render(createElement(Orch8Approvals, { baseUrl: BASE, pollInterval: 0, fetchImpl: engine.fetch, tokenProvider: () => makeToken() }));
      const el = container.querySelector("orch8-approvals");
      if (el?.shadowRoot) observer.observe(el.shadowRoot, { childList: true, subtree: true });
    });
    const sr = container.querySelector("orch8-approvals")!.shadowRoot!;
    await waitFor(() => sr.querySelector('[part="empty"]'));
    observer.disconnect();
    expect(seen).toEqual([]);
    expect(sr.querySelector('[role="alert"]')).toBeNull();
    await act(async () => root.unmount());
  });

  it("maps className to class on the custom element (React 18 would emit 'classname')", () => {
    const html = renderToString(createElement(Orch8Runs, { baseUrl: BASE, className: "my-widget" }));
    expect(html).toMatch(/<orch8-runs [^>]*class="my-widget"/);
    expect(html).not.toContain("classname");
  });
});
