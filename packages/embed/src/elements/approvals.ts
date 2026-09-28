import { BASE_ATTRIBUTES, Orch8Element, type LoadMode } from "../base.js";
import { Orch8EmbedError } from "../client.js";
import { formatDateTime, formatRelative, h, uid } from "../dom.js";
import type { Approval, ApprovalChoice } from "../types.js";

export interface ApprovalResolvedDetail {
  id: string;
  instance_id: string;
  choice: string;
  comment?: string;
}

interface Card {
  approval: Approval;
  el: HTMLElement;
  form: HTMLFormElement;
  error: HTMLElement;
  submit: HTMLButtonElement;
}

/**
 * `<orch8-approvals>` — pending human approvals with choice + comment.
 *
 * Attributes: `instance-id` (only this run's approvals), `poll-interval` ms
 * (15000, 0 = off). Resolving needs the `approvals:resolve` scope.
 * Events: `orch8-approval-resolved` {id, instance_id, choice, comment}.
 *
 * Cards are reconciled by id, so polling never discards a half-typed comment.
 */
export class Orch8ApprovalsElement extends Orch8Element {
  static observedAttributes = [...BASE_ATTRIBUTES, "instance-id", "poll-interval"];

  private approvals: Approval[] = [];
  private cards = new Map<string, Card>();
  private list: HTMLUListElement | null = null;
  private renderedLocale: string | undefined | null = null;
  private renderedStrings: unknown = null;

  protected override extraCss(): string {
    return `
      .card { border: 1px solid var(--_border); border-radius: var(--_radius); padding: calc(var(--_space) * 1.5);
        margin-bottom: var(--_space); background: var(--_surface); }
      .prompt { font-weight: 600; margin: 0 0 4px; font-size: 1em; }
      fieldset { border: none; margin: var(--_space) 0; padding: 0; }
      legend { font-weight: 500; padding: 0; margin-bottom: 4px; }
      .choices { display: flex; flex-wrap: wrap; gap: var(--_space); }
      .choice { display: inline-flex; align-items: center; gap: 6px; font-weight: 400; border: 1px solid var(--_border);
        border-radius: calc(var(--_radius) - 2px); padding: 4px 10px; background: var(--_bg); cursor: pointer; margin: 0; }
      .choice input { width: auto; margin: 0; }
      .choice:has(input:checked) { border-color: var(--_accent); }
      .row { display: flex; justify-content: flex-end; gap: var(--_space); align-items: center; }
    `;
  }

  protected override pollInterval(): number {
    const raw = this.getAttribute("poll-interval");
    if (raw === null) return 15_000;
    const n = Number(raw);
    return Number.isFinite(n) && n >= 0 ? (n === 0 ? 0 : Math.max(n, 1000)) : 15_000;
  }

  protected override onAttributeChanged(): void {
    this.cards.clear();
    this.list = null;
  }

  protected async load(signal: AbortSignal, _mode: LoadMode): Promise<void> {
    const res = await this.client.listApprovals(signal);
    const only = this.getAttribute("instance-id");
    this.approvals = only ? res.items.filter((a) => a.instance_id === only) : res.items;
  }

  private choicesFor(a: Approval): ApprovalChoice[] {
    if (Array.isArray(a.choices) && a.choices.length) return a.choices;
    return [
      { label: this.t("approvalsApprove"), value: "approve" },
      { label: this.t("approvalsReject"), value: "reject" },
    ];
  }

  private buildCard(a: Approval, canResolve: boolean): Card {
    const promptId = uid("o8-prompt");
    const errId = uid("o8-err");
    const commentId = uid("o8-comment");
    const group = uid("o8-choice");
    const error = h("div", { class: "field-error", id: errId, role: "alert" });
    const submit = h(
      "button",
      { type: "submit", class: "primary", "data-key": `submit-${a.id}`, disabled: !canResolve },
      this.t("approvalsSubmit"),
    );
    const choices = this.choicesFor(a).map((c, i) =>
      h(
        "label",
        { class: "choice" },
        h("input", {
          type: "radio",
          name: group,
          value: c.value,
          required: i === 0 ? true : null,
          disabled: !canResolve,
          "data-key": `choice-${a.id}-${c.value}`,
        }),
        c.label,
      ),
    );
    const form = h(
      "form",
      { "aria-labelledby": promptId, novalidate: true },
      h(
        "fieldset",
        { "aria-describedby": errId },
        h("legend", null, this.t("approvalsDecision")),
        h("div", { class: "choices" }, choices),
      ),
      h(
        "div",
        { class: "field" },
        h("label", { for: commentId }, this.t("approvalsComment")),
        h("textarea", { id: commentId, name: "comment", rows: "2", maxlength: "2000", disabled: !canResolve, "data-key": `comment-${a.id}` }),
      ),
      error,
      h("div", { class: "row" }, submit),
    );
    form.addEventListener("submit", (ev) => {
      ev.preventDefault();
      void this.resolve(a.id);
    });
    const el = h(
      "li",
      { class: "card", part: "approval", "data-id": a.id },
      h("h3", { class: "prompt", id: promptId }, a.prompt),
      h(
        "div",
        { class: "small muted" },
        h("time", { datetime: a.created_at, title: formatDateTime(a.created_at, this.displayLocale) }, this.t("approvalsRequested", { time: formatRelative(a.created_at, this.displayLocale) })),
      ),
      canResolve ? null : h("p", { class: "small muted" }, this.t("approvalsReadOnly")),
      form,
    );
    return { approval: a, el, form, error, submit };
  }

  /** Resolves approval `id` with the card's selected choice and comment. */
  async resolve(id: string): Promise<void> {
    const card = this.cards.get(id);
    if (!card) return;
    const data = new FormData(card.form);
    const radio = card.form.querySelector<HTMLInputElement>('input[type="radio"]:checked');
    const choice = radio?.value ?? "";
    const comment = String(data.get("comment") ?? "").trim();
    card.error.textContent = "";
    if (!choice) {
      card.error.textContent = this.t("approvalsChooseOne");
      card.form.querySelector<HTMLInputElement>('input[type="radio"]')?.focus();
      return;
    }
    const controls = Array.from(card.form.querySelectorAll<HTMLInputElement | HTMLTextAreaElement | HTMLButtonElement>("input, textarea, button"));
    controls.forEach((c) => (c.disabled = true));
    card.form.setAttribute("aria-busy", "true");
    card.submit.textContent = this.t("approvalsSubmitting");
    try {
      await this.client.resolveApproval(id, comment ? { choice, comment } : { choice });
      this.removeCard(id, this.t("approvalsResolved"));
      this.emit<ApprovalResolvedDetail>("orch8-approval-resolved", {
        id,
        instance_id: card.approval.instance_id,
        choice,
        ...(comment ? { comment } : {}),
      });
    } catch (err) {
      if (err instanceof Orch8EmbedError && (err.kind === "conflict" || err.kind === "not_found")) {
        this.removeCard(id, this.t("approvalsAlreadyResolved"));
        return;
      }
      controls.forEach((c) => (c.disabled = false));
      card.form.removeAttribute("aria-busy");
      card.submit.textContent = this.t("approvalsSubmit");
      const { message, detail } = this.errorMessage(err);
      card.error.textContent = detail ? `${message} ${detail}` : message;
      card.submit.focus();
    }
  }

  private removeCard(id: string, message: string): void {
    const card = this.cards.get(id);
    const wasFocused = card?.el.contains(this.root.activeElement);
    const next = card?.el.nextElementSibling ?? card?.el.previousElementSibling;
    card?.el.remove();
    this.cards.delete(id);
    this.approvals = this.approvals.filter((a) => a.id !== id);
    this.announce(message);
    if (this.approvals.length === 0) this.renderContent();
    else if (wasFocused) (next?.querySelector("input, button") as HTMLElement | null)?.focus();
  }

  protected renderContent(): void {
    // Strings/locale changed: rebuild cards from scratch.
    if (this.renderedLocale !== this.displayLocale || this.renderedStrings !== this.strings) {
      this.cards.clear();
      this.list = null;
      this.renderedLocale = this.displayLocale;
      this.renderedStrings = this.strings;
    }
    const header = this.renderHeader(this.t("approvalsTitle"));
    const titleId = "approvals-title";
    header.querySelector("h2")?.setAttribute("id", titleId);

    if (this.approvals.length === 0) {
      this.cards.clear();
      this.list = null;
      this.body.replaceChildren(header, h("div", { class: "status", part: "empty" }, this.t("approvalsEmpty")));
      return;
    }

    const canResolve = this.hasScope("approvals:resolve");
    if (!this.list || !this.body.contains(this.list)) {
      this.cards.clear();
      this.list = h("ul", { class: "plain", part: "list", "aria-labelledby": titleId });
      this.body.replaceChildren(header, this.list);
    } else {
      // Refresh header in place (logo may have arrived).
      this.body.firstElementChild?.replaceWith(header);
    }

    const wanted = new Set(this.approvals.map((a) => a.id));
    for (const [id, card] of this.cards) {
      if (!wanted.has(id)) {
        card.el.remove();
        this.cards.delete(id);
      }
    }
    let prev: Element | null = null;
    for (const a of this.approvals) {
      let card = this.cards.get(a.id);
      if (!card) {
        card = this.buildCard(a, canResolve);
        this.cards.set(a.id, card);
      }
      const expectedNext: Element | null = prev ? prev.nextElementSibling : this.list.firstElementChild;
      if (expectedNext !== card.el) {
        if (prev) prev.after(card.el);
        else this.list.prepend(card.el);
      }
      prev = card.el;
    }
  }
}
