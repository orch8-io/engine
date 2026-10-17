import { describe, expect, it } from "vitest";
import { BASE, makeToken, mockEngine, mount, text, THEME, waitFor } from "./helpers.js";

const def = (name: string, ids: string[]) => ({
  name,
  blocks: ids.map((id) => ({ type: "step", id, handler: "noop", params: {} })),
});

describe("<orch8-builder> save() while the sequence changes (EW-18)", () => {
  it("does not overwrite the newly loaded sequence with the saved one", async () => {
    const engine = mockEngine({
      "GET /sequences": { body: { items: [], handlers: ["noop"] } },
      "GET /sequences/first": { body: { definition: def("first", ["a", "b"]) } },
      "GET /sequences/second": { body: { definition: def("second", ["x"]) } },
      "PUT /sequences/first": { body: { definition: def("first", ["b", "a"]) } },
      "GET /theme": { body: THEME },
    });
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const delayed = (async (input: RequestInfo | URL, init?: RequestInit) => {
      if (init?.method === "PUT") await gate;
      return engine.fetch(input, init);
    }) as typeof fetch;
    const { el, root } = mount("orch8-builder", { "base-url": BASE, token: makeToken(), sequence: "first" }, { fetchImpl: delayed });
    await waitFor(() => root.querySelector("ol.steps"));
    root.querySelector<HTMLButtonElement>('button[aria-label="Move b up"]')!.click();

    const saved: string[] = [];
    el.addEventListener("orch8-sequence-saved", (e) => saved.push((e as CustomEvent).detail.name));
    const pending = el.save();
    el.setAttribute("sequence", "second");
    await waitFor(() => (text(root).includes("x") && root.querySelectorAll("li.step").length === 1 ? true : null));
    release();
    await expect(pending).resolves.toBe(true);

    expect(saved).toEqual(["first"]); // the PUT did land and is reported under its own name
    expect(root.querySelectorAll("li.step")).toHaveLength(1);
    expect(text(root.querySelector("li.step h3")!)).toContain("x");
    expect(el.isDirty).toBe(false);
  });
});
