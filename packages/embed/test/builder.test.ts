import { describe, expect, it } from "vitest";
import type { Orch8BuilderElement } from "../src/elements/builder.js";
import { BASE, makeToken, mockEngine, mount, text, THEME, waitFor } from "./helpers.js";

const DEF = {
  name: "onboard",
  version: 3,
  blocks: [
    { type: "step", id: "fetch", handler: "http_request", params: { url: "https://x" }, retry: { max_attempts: 3 } },
    { type: "parallel", id: "fanout", branches: [[{ type: "step", id: "inner", handler: "noop", params: {} }]] },
    { type: "step", id: "notify", handler: "send_email", params: {} },
  ],
};
const SEQS = { items: [{ name: "onboard" }], handlers: ["http_request", { name: "send_email", label: "Send email" }, { name: "slack_post", default_params: { channel: "#ops" } }] };

async function setup(scopes?: string[], putReply: unknown = { definition: { ...DEF, version: 4 } }) {
  const engine = mockEngine({
    "GET /sequences": { body: SEQS },
    "GET /sequences/onboard": { body: { definition: DEF } },
    "PUT /sequences/onboard": typeof putReply === "function" ? (putReply as never) : { body: putReply },
    "GET /theme": { body: THEME },
  });
  const { root, el } = mount("orch8-builder", { "base-url": BASE, token: makeToken(scopes ? { scp: scopes } : {}), sequence: "onboard" }, { fetchImpl: engine.fetch });
  await waitFor(() => root.querySelector("ol.steps"));
  return { root, el: el as Orch8BuilderElement, engine };
}

describe("<orch8-builder>", () => {
  it("renders steps with labelled fields, handler palette and locked composite blocks", async () => {
    const { root } = await setup();
    const steps = root.querySelectorAll("li.step");
    expect(steps).toHaveLength(3);
    expect(text(steps[1]!)).toContain("parallel block “fanout” (edit it in Orch8)");
    const idInput = steps[0]!.querySelector("input")!;
    expect(root.querySelector(`label[for="${idInput.id}"]`)?.textContent).toBe("Step ID");
    const options = Array.from(steps[0]!.querySelectorAll("select option")).map((o) => o.textContent);
    expect(options).toEqual(["http_request", "Send email", "slack_post"]);
    expect(root.querySelector('button[aria-label="Move fetch up"]')!.hasAttribute("disabled")).toBe(true);
    expect(root.querySelector('button[aria-label="Remove notify"]')).not.toBeNull();
    expect(root.querySelector<HTMLButtonElement>('button[data-key="save"]')!.disabled).toBe(true);
  });

  it("reorders, adds with default params, validates JSON and saves via PUT", async () => {
    const { root, el, engine } = await setup();
    // Move "notify" up above the parallel block; focus stays on the control.
    const up = root.querySelector<HTMLButtonElement>('button[aria-label="Move notify up"]')!;
    up.focus();
    up.click();
    expect(Array.from(root.querySelectorAll("li.step h3")).map((h) => text(h))).toEqual([
      "Step 1: fetch",
      "Step 2: notify",
      expect.stringContaining("Step 3: parallel block"),
    ]);
    expect(el.isDirty).toBe(true);

    // Add a slack step: gets default params and unique id, focus moves to its id field.
    root.querySelector<HTMLSelectElement>('select[data-key="add-handler"]')!.value = "slack_post";
    root.querySelector<HTMLButtonElement>('button[data-key="add"]')!.click();
    const last = Array.from(root.querySelectorAll("li.step")).at(-1)!;
    expect(last.querySelector("input")!.value).toBe("slack_post");
    expect(JSON.parse(last.querySelector("textarea")!.value)).toEqual({ channel: "#ops" });
    expect(root.activeElement).toBe(last.querySelector("input"));

    // Invalid JSON blocks save and marks the field.
    const ta = last.querySelector("textarea")!;
    ta.value = "{ nope";
    ta.dispatchEvent(new Event("input"));
    expect(ta.getAttribute("aria-invalid")).toBe("true");
    expect(await el.save()).toBe(false);
    expect(engine.calls.some((c) => c.method === "PUT")).toBe(false);
    expect(text(root)).toContain("Fix the highlighted fields before saving.");

    const ta2 = Array.from(root.querySelectorAll("li.step")).at(-1)!.querySelector("textarea")!;
    ta2.value = '{"channel":"#sales"}';
    ta2.dispatchEvent(new Event("input"));
    expect(await el.save()).toBe(true);
    const put = engine.calls.find((c) => c.method === "PUT")!;
    const body = put.body as { name: string; blocks: { id: string; type: string; params?: unknown; retry?: unknown }[] };
    expect(body.name).toBe("onboard");
    expect(body.blocks.map((b) => b.id)).toEqual(["fetch", "notify", "fanout", "slack_post"]);
    expect(body.blocks[0]!.retry).toEqual({ max_attempts: 3 }); // untouched fields preserved
    expect(body.blocks[3]).toEqual({ type: "step", id: "slack_post", handler: "slack_post", params: { channel: "#sales" } });
    expect(el.isDirty).toBe(false);
    expect(text(root)).toContain("Changes saved.");
  });

  it("rejects duplicate ids, including ids nested inside locked blocks", async () => {
    const { root, el } = await setup();
    const input = root.querySelector<HTMLInputElement>("li.step input")!;
    input.value = "inner";
    input.dispatchEvent(new Event("input"));
    expect(input.getAttribute("aria-invalid")).toBe("true");
    expect(text(root)).toContain("Another step already uses this ID.");
    expect(await el.save()).toBe(false);
  });

  it("removes a step and announces it", async () => {
    const { root } = await setup();
    root.querySelector<HTMLButtonElement>('button[aria-label="Remove fetch"]')!.click();
    expect(root.querySelectorAll("li.step")).toHaveLength(2);
    const live = root.querySelector('[role="status"][aria-live="polite"]')!;
    await waitFor(() => (live.textContent ? live : null));
    expect(live.textContent).toBe("Removed fetch.");
  });

  it("surfaces a 403 on save (sequence not owned by the sub-tenant)", async () => {
    const { root, el } = await setup(undefined, () => ({ status: 403, body: { error: { code: "forbidden", message: "sequence is not owned by this sub-tenant" } } }));
    root.querySelector<HTMLButtonElement>('button[aria-label="Move notify up"]')!.click();
    expect(await el.save()).toBe(false);
    const alert = root.querySelector('.savebar [role="alert"]')!;
    expect(alert.textContent).toContain("You don't have permission to do this.");
    expect(alert.textContent).toContain("not owned");
  });

  it("is read-only without builder:edit", async () => {
    const { root } = await setup(["sequences:read"]);
    expect(text(root)).toContain("You can view these steps but not edit them.");
    expect(root.querySelector('button[data-key="save"]')).toBeNull();
    expect(root.querySelector('button[aria-label="Remove fetch"]')).toBeNull();
    expect(root.querySelector<HTMLInputElement>("li.step input")!.disabled).toBe(true);
  });
});
