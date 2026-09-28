import { createElement } from "react";
import { act } from "react";
import { createRoot } from "react-dom/client";
import { renderToString } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";
import { Orch8Approvals, Orch8Badge, Orch8Builder, Orch8Runs, Orch8RunTimeline } from "../src/react.js";
import { BASE, makeToken, mockEngine, THEME, waitFor } from "./helpers.js";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

describe("React wrappers", () => {
  it("server-render to plain custom-element tags with kebab-case attributes", () => {
    const html = renderToString(
      createElement("div", null,
        createElement(Orch8Runs, { baseUrl: BASE, hrefTemplate: "/runs/{id}", pollInterval: 0, theme: { accent: "#f00" }, tokenProvider: () => "t" }),
        createElement(Orch8RunTimeline, { runId: "r1" }),
        createElement(Orch8Approvals, { instanceId: "r1" }),
        createElement(Orch8Builder, { sequence: "onboard" }),
        createElement(Orch8Badge, { vendor: "acme" }),
      ),
    );
    const runsTag = /<orch8-runs [^>]*>/.exec(html)![0];
    expect(runsTag).toContain('base-url="https://engine.test"');
    expect(runsTag).toContain('theme="{&quot;accent&quot;:&quot;#f00&quot;}"');
    expect(runsTag).toContain('href-template="/runs/{id}"');
    expect(runsTag).toContain('poll-interval="0"');
    expect(html).toContain('<orch8-run-timeline run-id="r1">');
    expect(html).toContain('<orch8-approvals instance-id="r1">');
    expect(html).toContain('<orch8-builder sequence="onboard">');
    expect(html).toContain('<orch8-badge vendor="acme">');
    expect(html).not.toContain("tokenProvider");
  });

  it("assigns tokenProvider as a property and forwards custom events to typed callbacks", async () => {
    const engine = mockEngine({
      "GET /runs": { body: { items: [{ id: "r1", sequence: "onboard", state: "running", created_at: "", updated_at: "" }] } },
      "GET /theme": { body: THEME },
    });
    const token = makeToken();
    const tokenProvider = vi.fn(() => token);
    const onRunSelect = vi.fn();
    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);
    await act(async () => {
      root.render(createElement(Orch8Runs, { baseUrl: BASE, pollInterval: 0, tokenProvider, fetchImpl: engine.fetch, onRunSelect }));
    });
    const el = container.querySelector("orch8-runs")!;
    const btn = await waitFor(() => el.shadowRoot!.querySelector<HTMLButtonElement>('button[data-key="run-r1"]'));
    expect(tokenProvider).toHaveBeenCalled();
    btn.click();
    expect(onRunSelect).toHaveBeenCalledWith(expect.objectContaining({ id: "r1" }), expect.any(CustomEvent));
    await act(async () => root.unmount());
  });
});
