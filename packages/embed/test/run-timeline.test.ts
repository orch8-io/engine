import { describe, expect, it } from "vitest";
import { BASE, makeToken, mockEngine, mount, text, THEME, waitFor } from "./helpers.js";

const RUN = {
  id: "r1",
  sequence: "onboard",
  state: "running",
  steps: [
    { id: "fetch", name: "Fetch profile", state: "completed", started_at: "2026-09-28T10:00:00Z", finished_at: "2026-09-28T10:00:02.500Z", output: { plan: "pro" } },
    { id: "email", name: null, state: "running", started_at: "2026-09-28T10:00:03Z", finished_at: null },
  ],
};

describe("<orch8-run-timeline>", () => {
  it("renders an ordered, labelled list of steps with state text, duration and output", async () => {
    const engine = mockEngine({ "GET /runs/r1": { body: RUN }, "GET /theme": { body: THEME } });
    const { root } = mount("orch8-run-timeline", { "base-url": BASE, token: makeToken(), "run-id": "r1", "poll-interval": "0" }, { fetchImpl: engine.fetch });
    const list = await waitFor(() => root.querySelector("ol.timeline"));
    expect(list.getAttribute("aria-labelledby")).toBe("timeline-title");
    expect(root.getElementById("timeline-title")?.textContent).toBe("Run of onboard");
    const items = list.querySelectorAll("li");
    expect(items).toHaveLength(2);
    expect(text(items[0]!)).toContain("Fetch profile Completed");
    expect(text(items[0]!)).toContain("took 2.5 s");
    expect(items[0]!.querySelector("pre")?.textContent).toContain('"plan": "pro"');
    expect(text(items[1]!)).toContain("email Running");
    // Decorative dots are hidden from assistive tech.
    expect(items[0]!.querySelector(".dot")?.getAttribute("aria-hidden")).toBe("true");
    // A polite live region exists for updates.
    expect(root.querySelector('[role="status"][aria-live="polite"]')).not.toBeNull();
  });

  it("polls until terminal and announces step changes", async () => {
    const done = { ...RUN, state: "completed", steps: [RUN.steps[0], { ...RUN.steps[1], state: "completed", finished_at: "2026-09-28T10:00:04Z" }] };
    const engine = mockEngine({ "GET /runs/r1": [{ body: RUN }, { body: done }], "GET /theme": { body: THEME } });
    const { root, el } = mount("orch8-run-timeline", { "base-url": BASE, token: makeToken(), "run-id": "r1", "poll-interval": "500" }, { fetchImpl: engine.fetch });
    const updates: string[] = [];
    el.addEventListener("orch8-run-update", (e) => updates.push((e as CustomEvent).detail.run.state));
    await waitFor(() => (updates.includes("completed") ? true : null), 3000);
    const live = root.querySelector('[role="status"][aria-live="polite"]')!;
    await waitFor(() => (live.textContent ? live : null));
    expect(live.textContent).toContain("email: Completed");
    expect(live.textContent).toContain("Status: Completed");
    const count = engine.calls.filter((c) => c.path === "/runs/r1").length;
    await new Promise((r) => setTimeout(r, 700));
    expect(engine.calls.filter((c) => c.path === "/runs/r1").length).toBe(count); // stopped polling
  });

  it("shows a prompt when no run-id is set, and a not-found message on 404", async () => {
    const engine = mockEngine({ "GET /theme": { body: THEME } });
    const { root, el } = mount("orch8-run-timeline", { "base-url": BASE, token: makeToken() }, { fetchImpl: engine.fetch });
    await waitFor(() => (text(root).includes("Select a run") ? true : null));
    el.setAttribute("run-id", "missing");
    const alert = await waitFor(() => root.querySelector('[role="alert"]'));
    expect(text(alert)).toContain("couldn't find");
  });
});
