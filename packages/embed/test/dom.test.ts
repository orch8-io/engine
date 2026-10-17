import { describe, expect, it, vi } from "vitest";
import { ELEMENTS, defineOrch8Elements } from "../src/define.js";
import { formatDateTime, formatDuration, formatRelative, h, jsonPreview, uid } from "../src/dom.js";
import { safeLogoUrl } from "../src/theme.js";
import { VERSION } from "../src/version.js";
import pkg from "../package.json" with { type: "json" };

describe("dom helpers", () => {
  it("h() sets text through text nodes (no markup injection) and wires on* listeners", () => {
    const onclick = vi.fn();
    const el = h("button", { type: "button", onclick, disabled: false, "data-x": 1 }, "<img src=x onerror=alert(1)>", null, [h("b", null, "n")]);
    expect(el.innerHTML).toBe("&lt;img src=x onerror=alert(1)&gt;<b>n</b>");
    expect(el.hasAttribute("disabled")).toBe(false);
    expect(el.getAttribute("data-x")).toBe("1");
    el.click();
    expect(onclick).toHaveBeenCalledOnce();
    expect(h("input", { value: "v", required: true }).value).toBe("v");
  });

  it("uid() is unique and prefixed", () => {
    const a = uid("p");
    expect(a).toMatch(/^p-\d+$/);
    expect(uid("p")).not.toBe(a);
  });

  it("formatDuration scales units and rejects bad input", () => {
    expect(formatDuration(250)).toBe("250 ms");
    expect(formatDuration(2500)).toBe("2.5 s");
    expect(formatDuration(42_000)).toBe("42 s");
    expect(formatDuration(125_000)).toBe("2 min 5 s");
    expect(formatDuration(3_900_000)).toBe("1 h 5 min");
    expect(formatDuration(-1)).toBe("");
    expect(formatDuration(Number.NaN)).toBe("");
  });

  it("formatDateTime handles empty, invalid and valid input", () => {
    expect(formatDateTime(null, "en")).toBe("");
    expect(formatDateTime(undefined, "en")).toBe("");
    expect(formatDateTime("not a date", "en")).toBe("not a date");
    expect(formatDateTime("2026-09-28T10:00:00Z", "en")).toMatch(/2026/);
    expect(formatDateTime("2026-09-28T10:00:00Z", "not_a_locale!!")).toContain("2026"); // falls back to ISO
  });

  it("formatRelative picks the largest sensible unit", () => {
    const now = Date.parse("2026-09-28T12:00:00Z");
    expect(formatRelative("", "en", now)).toBe("");
    expect(formatRelative("garbage", "en", now)).toBe("garbage");
    expect(formatRelative("2026-09-28T11:55:00Z", "en", now)).toBe("5 minutes ago");
    expect(formatRelative("2026-09-28T09:00:00Z", "en", now)).toBe("3 hours ago");
    expect(formatRelative("2026-09-26T12:00:00Z", "en", now)).toBe("2 days ago");
    expect(formatRelative("2026-09-28T11:59:30Z", "en", now)).toBe("30 seconds ago");
  });

  it("jsonPreview stringifies, passes strings through, truncates and survives cycles", () => {
    expect(jsonPreview({ a: 1 })).toBe('{\n  "a": 1\n}');
    expect(jsonPreview("plain")).toBe("plain");
    expect(jsonPreview("x".repeat(20), 5)).toBe("xxxxx\n…");
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(jsonPreview(cyclic)).toBe("[object Object]");
    expect(jsonPreview(undefined)).toBe("undefined");
  });
});

describe("safeLogoUrl", () => {
  it("allows only absolute https URLs", () => {
    expect(safeLogoUrl("https://cdn.acme.com/l.svg")).toBe("https://cdn.acme.com/l.svg");
    for (const bad of ["http://cdn.acme.com/l.svg", "javascript:alert(1)", "data:image/svg+xml,<svg/>", "//cdn.acme.com/l.png", "/rel.png", "", null, undefined]) {
      expect(safeLogoUrl(bad)).toBeNull();
    }
  });
});

describe("defineOrch8Elements", () => {
  it("is idempotent: calling it again neither throws nor re-registers", () => {
    const before = customElements.get("orch8-runs");
    expect(() => {
      defineOrch8Elements();
      defineOrch8Elements();
    }).not.toThrow();
    expect(customElements.get("orch8-runs")).toBe(before);
    expect(before).toBe(ELEMENTS["orch8-runs"]);
  });

  it("registers into a custom registry only the tags it is missing", () => {
    const defined = new Map<string, unknown>();
    const registry = { get: (t: string) => defined.get(t), define: (t: string, c: unknown) => defined.set(t, c) } as unknown as CustomElementRegistry;
    defineOrch8Elements(registry);
    defineOrch8Elements(registry);
    expect([...defined.keys()].sort()).toEqual(Object.keys(ELEMENTS).sort());
  });

  it("warns once when another version already registered a tag", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    class Foreign {
      static orch8Version = "0.0.1-foreign";
    }
    const registry = { get: () => Foreign, define: vi.fn() } as unknown as CustomElementRegistry;
    defineOrch8Elements(registry);
    defineOrch8Elements(registry);
    expect(warn).toHaveBeenCalledTimes(Object.keys(ELEMENTS).length); // once per tag, not per call
    expect(warn.mock.calls[0]![0]).toContain("0.0.1-foreign");
    expect(warn.mock.calls[0]![0]).toContain(VERSION);
    expect(registry.define).not.toHaveBeenCalled();
  });

  it("does not warn when the same version is registered by an identical class", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const registry = { get: (t: string) => ELEMENTS[t as keyof typeof ELEMENTS], define: vi.fn() } as unknown as CustomElementRegistry;
    defineOrch8Elements(registry);
    expect(warn).not.toHaveBeenCalled();
  });

  it("keeps VERSION in sync with package.json", () => {
    expect(VERSION).toBe(pkg.version);
  });
});
