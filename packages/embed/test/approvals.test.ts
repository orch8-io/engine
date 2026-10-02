import { describe, expect, it } from "vitest";
import { BASE, makeToken, mockEngine, mount, text, THEME, waitFor } from "./helpers.js";

const APPROVALS = {
  items: [
    { id: "a1", instance_id: "r1", step_id: "review", prompt: "Refund $120 to Jane?", choices: [{ label: "Approve", value: "yes" }, { label: "Deny", value: "no" }], created_at: "2026-09-28T10:00:00Z" },
    { id: "a2", instance_id: "r2", step_id: "review", prompt: "Publish post?", choices: [], created_at: "2026-09-28T10:05:00Z" },
  ],
};

describe("<orch8-approvals>", () => {
  it("renders accessible forms: labelled card, fieldset+legend radios, labelled comment", async () => {
    const engine = mockEngine({ "GET /approvals": { body: APPROVALS }, "GET /theme": { body: THEME } });
    const { root } = mount("orch8-approvals", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    const forms = await waitFor(() => (root.querySelectorAll("form").length === 2 ? root.querySelectorAll("form") : null));
    const form = forms[0]!;
    const labelled = root.getElementById(form.getAttribute("aria-labelledby")!);
    expect(labelled?.textContent).toBe("Refund $120 to Jane?");
    expect(form.querySelector("fieldset legend")?.textContent).toBe("Decision");
    const radios = form.querySelectorAll<HTMLInputElement>('input[type="radio"]');
    expect(Array.from(radios).map((r) => r.value)).toEqual(["yes", "no"]);
    const textarea = form.querySelector("textarea")!;
    expect(root.querySelector(`label[for="${textarea.id}"]`)?.textContent).toBe("Comment (optional)");
    // Missing choices fall back to approve / reject.
    expect(Array.from(forms[1]!.querySelectorAll<HTMLInputElement>('input[type="radio"]')).map((r) => r.value)).toEqual(["approve", "reject"]);
  });

  it("requires a choice, then resolves with choice + comment and removes the card", async () => {
    const engine = mockEngine({ "GET /approvals": { body: APPROVALS }, "POST /approvals/a1": { body: { ok: true } }, "GET /theme": { body: THEME } });
    const { root, el } = mount("orch8-approvals", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    const resolved = new Promise((r) => el.addEventListener("orch8-approval-resolved", (e) => r((e as CustomEvent).detail)));
    const form = await waitFor(() => root.querySelector("form"));
    form.dispatchEvent(new Event("submit", { cancelable: true }));
    await waitFor(() => (text(form).includes("Choose an option first.") ? true : null));
    expect(engine.calls.some((c) => c.method === "POST")).toBe(false);

    form.querySelector<HTMLInputElement>('input[value="no"]')!.checked = true;
    form.querySelector("textarea")!.value = "  duplicate request  ";
    form.dispatchEvent(new Event("submit", { cancelable: true }));
    await expect(resolved).resolves.toEqual({ id: "a1", instance_id: "r1", choice: "no", comment: "duplicate request" });
    expect(engine.calls.find((c) => c.method === "POST")!.body).toEqual({ choice: "no", comment: "duplicate request" });
    await waitFor(() => (root.querySelectorAll("form").length === 1 ? true : null));
    expect(text(root)).not.toContain("Refund");
  });

  it("shows a 403 scope error inline and keeps the form usable", async () => {
    const engine = mockEngine({
      "GET /approvals": { body: APPROVALS },
      "POST /approvals/a1": { status: 403, body: { error: { code: "forbidden", message: "missing scope approvals:resolve" } } },
      "GET /theme": { body: THEME },
    });
    const { root } = mount("orch8-approvals", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    const form = await waitFor(() => root.querySelector("form"));
    form.querySelector<HTMLInputElement>('input[value="yes"]')!.checked = true;
    form.dispatchEvent(new Event("submit", { cancelable: true }));
    const err = form.querySelector('[role="alert"]')!;
    await waitFor(() => (err.textContent ? err : null));
    expect(err.textContent).toContain("You don't have permission");
    expect(err.textContent).toContain("missing scope approvals:resolve");
    expect(form.querySelector<HTMLButtonElement>('button[type="submit"]')!.disabled).toBe(false);
  });

  it("is read-only when the token lacks approvals:resolve", async () => {
    const engine = mockEngine({ "GET /approvals": { body: APPROVALS }, "GET /theme": { body: THEME } });
    const { root } = mount("orch8-approvals", { "base-url": BASE, token: makeToken({ scp: ["runs:read"] }), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    const form = await waitFor(() => root.querySelector("form"));
    expect(form.querySelector<HTMLButtonElement>('button[type="submit"]')!.disabled).toBe(true);
    expect(text(root)).toContain("You can view this request but not respond to it.");
  });

  it("filters by instance-id and keeps a half-typed comment across polls", async () => {
    const engine = mockEngine({ "GET /approvals": { body: APPROVALS }, "GET /theme": { body: THEME } });
    const { root, el } = mount("orch8-approvals", { "base-url": BASE, token: makeToken(), "poll-interval": "0", "instance-id": "r1" }, { fetchImpl: engine.fetch });
    const form = await waitFor(() => root.querySelector("form"));
    expect(root.querySelectorAll("form")).toHaveLength(1);
    form.querySelector("textarea")!.value = "draft";
    el.refresh();
    await waitFor(() => (engine.calls.filter((c) => c.path === "/approvals").length >= 2 ? true : null));
    await new Promise((r) => setTimeout(r, 20));
    expect(root.querySelector("form")).toBe(form);
    expect(form.querySelector("textarea")!.value).toBe("draft");
  });

  it("shows the empty state", async () => {
    const engine = mockEngine({ "GET /approvals": { body: { items: [] } }, "GET /theme": { body: THEME } });
    const { root } = mount("orch8-approvals", { "base-url": BASE, token: makeToken(), "poll-interval": "0" }, { fetchImpl: engine.fetch });
    await waitFor(() => root.querySelector('[part="empty"]'));
    expect(text(root)).toContain("Nothing is waiting for your decision.");
  });
});
