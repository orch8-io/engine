import { describe, expect, it, vi } from "vitest";
import { BASE, makeToken, mockEngine, mount, text, THEME, waitFor } from "./helpers.js";

const RUNS = {
  items: [
    { id: "r1", sequence: "onboard", state: "running", created_at: "2026-09-28T10:00:00Z", updated_at: "2026-09-28T10:01:00Z", current_step: "send_email" },
    { id: "r2", sequence: "invoice", state: "failed", created_at: "2026-09-28T09:00:00Z", updated_at: "2026-09-28T09:05:00Z", current_step: null },
  ],
  next_cursor: "c2",
};

describe("<orch8-runs>", () => {
  it("renders a labelled table with state text (not colour only) and the badge", async () => {
    const engine = mockEngine({ "GET /runs": { body: RUNS }, "GET /theme": { body: THEME } });
    const { root } = mount("orch8-runs", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    const table = await waitFor(() => root.querySelector("table")!);
    expect(table.getAttribute("aria-labelledby")).toBe("runs-title");
    expect(root.getElementById("runs-title")?.textContent).toBe("Runs");
    expect(Array.from(table.querySelectorAll("th")).every((th) => th.getAttribute("scope") === "col")).toBe(true);
    expect(text(table)).toContain("Running");
    expect(text(table)).toContain("Failed");
    const view = root.querySelector('button[aria-label="View run r1 (onboard)"]');
    expect(view).not.toBeNull();
    const badge = await waitFor(() => root.querySelector("a.badge-link") as HTMLAnchorElement);
    expect(badge.href).toBe("https://orch8.io/?ref=embed&v=acme");
  });

  it("shows a loading state first and an empty state when there are no runs", async () => {
    const engine = mockEngine({ "GET /runs": { body: { items: [], next_cursor: null } }, "GET /theme": { body: THEME } });
    const { root, el } = mount("orch8-runs", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    await Promise.resolve();
    expect(text(root)).toContain("Loading");
    await waitFor(() => expect(root.querySelector('[part="empty"]')).not.toBeNull());
    expect(text(root)).toContain("No runs yet.");
    expect(el.hasAttribute("aria-busy")).toBe(false);
  });

  it("emits orch8-run-select and loads more with the cursor", async () => {
    const engine = mockEngine({
      "GET /runs": (call) =>
        call.url.includes("cursor=c2")
          ? { body: { items: [{ ...RUNS.items[0], id: "r3", sequence: "export" }], next_cursor: null } }
          : { body: RUNS },
      "GET /theme": { body: THEME },
    });
    const { root, el } = mount("orch8-runs", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    const onSelect = vi.fn();
    el.addEventListener("orch8-run-select", (e) => onSelect((e as CustomEvent).detail.id));
    const btn = await waitFor(() => root.querySelector<HTMLButtonElement>('button[data-key="run-r2"]')!);
    btn.click();
    expect(onSelect).toHaveBeenCalledWith("r2");
    root.querySelector<HTMLButtonElement>('button[data-key="more"]')!.click();
    await waitFor(() => expect(root.querySelector('[data-key="run-r3"]')).not.toBeNull());
    expect(root.querySelector('button[data-key="more"]')).toBeNull();
  });

  it("renders links when href-template is set", async () => {
    const engine = mockEngine({ "GET /runs": { body: RUNS }, "GET /theme": { body: THEME } });
    const { root } = mount("orch8-runs", { "base-url": BASE, token: makeToken(), "poll-interval": "0", "href-template": "/runs/{id}" }, { fetchImpl: engine.fetch });
    const a = await waitFor(() => root.querySelector<HTMLAnchorElement>('a[data-key="run-r1"]')!);
    expect(a.getAttribute("href")).toBe("/runs/r1");
  });

  it("starts a run when start-sequence is set and the token has runs:start", async () => {
    const engine = mockEngine({ "GET /runs": { body: RUNS }, "POST /runs": { body: { id: "r9" } }, "GET /theme": { body: THEME } });
    const { root, el } = mount("orch8-runs", { "base-url": BASE, token: makeToken(), "poll-interval": "0", "start-sequence": "onboard" }, { fetchImpl: engine.fetch });
    const started = new Promise((r) => el.addEventListener("orch8-run-started", (e) => r((e as CustomEvent).detail)));
    const btn = await waitFor(() => root.querySelector<HTMLButtonElement>('button[data-key="start"]')!);
    expect(btn.textContent).toBe("Start onboard");
    btn.click();
    await expect(started).resolves.toEqual({ id: "r9", sequence: "onboard" });
    expect(engine.calls.find((c) => c.method === "POST")!.body).toEqual({ sequence: "onboard", input: {} });
  });

  it("hides the start button without runs:start", async () => {
    const engine = mockEngine({ "GET /runs": { body: RUNS }, "GET /theme": { body: THEME } });
    const { root } = mount("orch8-runs", { "base-url": BASE, token: makeToken({ scp: ["runs:read"] }), "poll-interval": "0", "start-sequence": "onboard" }, { fetchImpl: engine.fetch });
    await waitFor(() => root.querySelector("table")!);
    expect(root.querySelector('[data-key="start"]')).toBeNull();
  });

  it("renders a scope error (403) as an alert without a retry button", async () => {
    const engine = mockEngine({
      "GET /runs": { status: 403, body: { error: { code: "forbidden", message: "token lacks runs:read" } } },
      "GET /theme": { body: THEME },
    });
    const { root } = mount("orch8-runs", { "base-url": BASE, token: makeToken({ scp: [] }), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    const alert = await waitFor(() => root.querySelector('[role="alert"]')!);
    expect(text(alert)).toContain("You don't have permission to do this.");
    expect(text(alert)).toContain("token lacks runs:read");
    expect(alert.querySelector("button")).toBeNull();
  });

  it("shows a configuration error when no token is provided", async () => {
    const { root } = mount("orch8-runs", { "base-url": BASE });
    const alert = await waitFor(() => root.querySelector('[role="alert"]')!);
    expect(text(alert)).toContain("isn't configured");
  });

  it("offers retry after a 5xx and recovers", async () => {
    const engine = mockEngine({
      "GET /runs": [{ status: 500 }, { status: 500 }, { status: 500 }, { status: 500 }, { body: RUNS }],
      "GET /theme": { body: THEME },
    });
    const { root } = mount("orch8-runs", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    const alert = await waitFor(() => root.querySelector('[role="alert"]')!, 15_000);
    expect(text(alert)).toContain("Something went wrong");
    alert.querySelector("button")!.click();
    await waitFor(() => root.querySelector("table")!);
  }, 20_000);

  it("uses tokenProvider when no token attribute is set", async () => {
    const engine = mockEngine({ "GET /runs": { body: RUNS }, "GET /theme": { body: THEME } });
    const token = makeToken();
    const provider = vi.fn(() => Promise.resolve(token));
    const { root } = mount("orch8-runs", { "base-url": BASE, "poll-interval": "0" }, { fetchImpl: engine.fetch, tokenProvider: provider });
    await waitFor(() => root.querySelector("table")!);
    expect(provider).toHaveBeenCalled();
    expect(engine.calls[0]!.auth).toBe(`Bearer ${token}`);
  });
});
