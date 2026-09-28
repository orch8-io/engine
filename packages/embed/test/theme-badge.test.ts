import { describe, expect, it } from "vitest";
import { badgeHref } from "../src/badge.js";
import { cssVarsRule, safeLogoUrl, sanitizeCssVars } from "../src/theme.js";
import { BASE, makeToken, mockEngine, mount, THEME, waitFor } from "./helpers.js";

describe("theme", () => {
  it("normalises names and drops unsafe values", () => {
    expect(
      sanitizeCssVars({
        accent: "#ff0066",
        "--orch8-radius": "4px",
        "--other-thing": "1px",
        bg: "red; } body { display:none",
        font: "url(https://evil)",
        fg: 12,
      }),
    ).toEqual({ "--orch8-accent": "#ff0066", "--orch8-radius": "4px" });
    expect(cssVarsRule({ "--orch8-accent": "#000" })).toBe(":host { --orch8-accent: #000; }");
    expect(safeLogoUrl("javascript:alert(1)")).toBeNull();
    expect(safeLogoUrl("https://cdn.acme.com/logo.svg")).toBe("https://cdn.acme.com/logo.svg");
  });

  it("applies server css_vars, lets the theme attribute override them, and shows the logo", async () => {
    const engine = mockEngine({
      "GET /runs": { body: { items: [] } },
      "GET /theme": { body: { css_vars: { accent: "#123456", radius: "2px" }, logo_url: "https://cdn.acme.com/l.png", hide_badge: false } },
    });
    const { root } = mount(
      "orch8-runs",
      { "base-url": BASE, token: makeToken(), "poll-interval": "0", theme: JSON.stringify({ accent: "#abcdef" }) },
      { fetchImpl: engine.fetch },
    );
    const img = await waitFor(() => root.querySelector<HTMLImageElement>("img.logo"));
    expect(img.getAttribute("alt")).toBe("");
    const styles = Array.from(root.querySelectorAll("style")).map((s) => s.textContent).join("\n");
    expect(styles).toContain("--orch8-accent: #abcdef;");
    expect(styles).toContain("--orch8-radius: 2px;");
  });

  it("hides the badge when the theme says hide_badge (white-label)", async () => {
    const engine = mockEngine({ "GET /runs": { body: { items: [] } }, "GET /theme": { body: { ...THEME, hide_badge: true } } });
    const { root } = mount("orch8-runs", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    await waitFor(() => root.querySelector('[part="empty"]'));
    await waitFor(() => (engine.calls.some((c) => c.path === "/theme") ? true : null));
    await new Promise((r) => setTimeout(r, 20));
    expect(root.querySelector("a.badge-link")).toBeNull();
  });

  it("fetches the theme once for many widgets on a page", async () => {
    const engine = mockEngine({ "GET /runs": { body: { items: [] } }, "GET /approvals": { body: { items: [] } }, "GET /theme": { body: THEME } });
    const token = makeToken();
    const a = mount("orch8-runs", { "base-url": BASE, token, "poll-interval": "0" }, { fetchImpl: engine.fetch });
    const b = mount("orch8-approvals", { "base-url": BASE, token, "poll-interval": "0" }, { fetchImpl: engine.fetch });
    await waitFor(() => a.root.querySelector("a.badge-link"));
    await waitFor(() => b.root.querySelector("a.badge-link"));
    expect(engine.calls.filter((c) => c.path === "/theme")).toHaveLength(1);
  });
});

describe("<orch8-badge>", () => {
  it("links to orch8.io with ref + vendor and safe rel", () => {
    const { root } = mount("orch8-badge", { vendor: "acme co" });
    const a = root.querySelector("a")!;
    expect(a.href).toBe("https://orch8.io/?ref=embed&v=acme+co");
    expect(a.getAttribute("rel")).toBe("noopener");
    expect(a.getAttribute("target")).toBe("_blank");
    expect(a.getAttribute("aria-label")).toBe("Powered by Orch8 (opens in a new tab)");
    expect(a.textContent).toBe("Powered by Orch8");
    expect(badgeHref(null)).toBe("https://orch8.io/?ref=embed");
  });

  it("accepts translated strings", () => {
    const { el, root } = mount("orch8-badge", {});
    (el as unknown as { strings: object }).strings = { poweredBy: "Bereitgestellt von Orch8" };
    expect(root.querySelector("a")!.textContent).toBe("Bereitgestellt von Orch8");
  });
});

describe("i18n", () => {
  it("overrides strings on a widget", async () => {
    const engine = mockEngine({ "GET /runs": { body: { items: [] } }, "GET /theme": { body: THEME } });
    const { root } = mount("orch8-runs", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: engine.fetch, strings: { runsEmpty: "Noch keine Läufe." } });
    await waitFor(() => root.querySelector('[part="empty"]'));
    expect(root.textContent).toContain("Noch keine Läufe.");
  });
});
