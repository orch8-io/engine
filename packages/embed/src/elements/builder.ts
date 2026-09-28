import { BASE_ATTRIBUTES, Orch8Element, type LoadMode } from "../base.js";
import { unwrapDefinition } from "../client.js";
import { h, uid } from "../dom.js";
import type { BlockJson, HandlerInfo, SequenceDefinitionJson } from "../types.js";

interface StepItem {
  kind: "step";
  key: string;
  id: string;
  handler: string;
  paramsText: string;
  /** Other StepDef fields (retry, timeout, …) preserved untouched. */
  rest: Record<string, unknown>;
}

interface LockedItem {
  kind: "locked";
  key: string;
  block: BlockJson;
}

type Item = StepItem | LockedItem;

interface FieldErrors {
  id?: string;
  params?: string;
}

const ID_RE = /^[A-Za-z0-9_.-]{1,128}$/;

export interface SequenceSavedDetail {
  name: string;
  definition: SequenceDefinitionJson;
}

/** Collects every `id` in a block tree (nested blocks of composites included). */
function collectIds(value: unknown, into: Set<string>): void {
  if (Array.isArray(value)) value.forEach((v) => collectIds(v, into));
  else if (value && typeof value === "object") {
    const obj = value as Record<string, unknown>;
    if (typeof obj.id === "string" && typeof obj.type === "string") into.add(obj.id);
    for (const [k, v] of Object.entries(obj)) if (k !== "params") collectIds(v, into);
  }
}

/**
 * `<orch8-builder sequence="name">` — a compact, list-based step editor.
 * Add / reorder / remove top-level steps, pick the handler from the list
 * served by `GET /embed/sequences`, edit params as validated JSON, and save
 * with `PUT /embed/sequences/{name}` (scope `builder:edit`). Composite blocks
 * (parallel, loop, …) are shown locked and can be reordered but not edited.
 * Events: `orch8-sequence-saved` {name, definition}, `orch8-dirty-change` {dirty}.
 */
export class Orch8BuilderElement extends Orch8Element {
  static observedAttributes = [...BASE_ATTRIBUTES, "sequence"];

  private definition: SequenceDefinitionJson | null = null;
  private items: Item[] = [];
  private handlers: HandlerInfo[] = [];
  private errors = new Map<string, FieldErrors>();
  private dirty = false;
  private saving = false;
  private saveMessage: { text: string; error: boolean } | null = null;
  private pendingFocus: string | null = null;
  private dirtyEl: HTMLElement | null = null;
  private saveBtn: HTMLButtonElement | null = null;

  protected override extraCss(): string {
    return `
      ol.steps { list-style: none; margin: 0; padding: 0; }
      li.step { border: 1px solid var(--_border); border-radius: var(--_radius); padding: calc(var(--_space) * 1.5);
        margin-bottom: var(--_space); background: var(--_surface); }
      li.step.locked { background: transparent; border-style: dashed; }
      .step-head { display: flex; align-items: center; justify-content: space-between; gap: var(--_space); margin-bottom: var(--_space); }
      .step-head h3 { font-size: 1em; margin: 0; }
      .step-actions { display: flex; gap: 4px; }
      .step-actions button { min-width: 32px; justify-content: center; padding: 0 8px; }
      .grid { display: grid; grid-template-columns: 1fr 1fr; gap: var(--_space); }
      @media (max-width: 520px) { .grid { grid-template-columns: 1fr; } }
      .add { display: flex; gap: var(--_space); align-items: end; flex-wrap: wrap; border-top: 1px solid var(--_border);
        padding-top: calc(var(--_space) * 1.5); margin-top: var(--_space); }
      .add .field { flex: 1 1 200px; margin: 0; }
      .savebar { display: flex; justify-content: flex-end; align-items: center; gap: var(--_space); margin-top: calc(var(--_space) * 1.5); }
      .dirty { color: var(--_warning); font-size: 0.875em; }
      .ok { color: var(--_success); font-size: 0.875em; }
    `;
  }

  get sequence(): string | null {
    return this.getAttribute("sequence");
  }
  set sequence(value: string | null) {
    if (value) this.setAttribute("sequence", value);
    else this.removeAttribute("sequence");
  }

  /** True when there are unsaved edits. */
  get isDirty(): boolean {
    return this.dirty;
  }

  protected override ready(): boolean {
    return Boolean(this.sequence);
  }

  protected override onAttributeChanged(name: string): void {
    if (name === "sequence") {
      this.definition = null;
      this.items = [];
      this.setDirty(false);
    }
  }

  private get readOnly(): boolean {
    return !this.hasScope("builder:edit");
  }

  protected async load(signal: AbortSignal, _mode: LoadMode): Promise<void> {
    const name = this.sequence;
    if (!name) return;
    const [list, raw] = await Promise.all([
      // The handler palette is optional: a token without `sequences:read` still edits.
      this.client.listSequences(signal).catch(() => ({ items: [], handlers: [] as HandlerInfo[] })),
      this.client.getSequence(name, signal),
    ]);
    const def = unwrapDefinition(raw);
    if (!def) throw new Error("Unexpected sequence payload: missing blocks");
    this.handlers = list.handlers;
    this.setModel(def);
  }

  private setModel(def: SequenceDefinitionJson): void {
    this.definition = def;
    this.items = def.blocks.map((block) => {
      if (block.type === "step") {
        const { type: _type, id, handler, params, ...rest } = block;
        return {
          kind: "step",
          key: uid("o8-step"),
          id: String(id ?? ""),
          handler: String(handler ?? ""),
          paramsText: JSON.stringify(params ?? {}, null, 2),
          rest,
        } satisfies StepItem;
      }
      return { kind: "locked", key: uid("o8-block"), block } satisfies LockedItem;
    });
    this.errors.clear();
    this.setDirty(false);
  }

  private setDirty(dirty: boolean): void {
    if (this.dirty !== dirty) {
      this.dirty = dirty;
      this.emit("orch8-dirty-change", { dirty });
    }
    if (this.dirtyEl) this.dirtyEl.textContent = dirty ? this.t("builderUnsaved") : "";
    if (this.saveBtn) this.saveBtn.disabled = !dirty || this.saving || this.readOnly;
  }

  private handlerNames(): string[] {
    const names = new Set(this.handlers.map((hd) => hd.name));
    for (const it of this.items) if (it.kind === "step" && it.handler) names.add(it.handler);
    return Array.from(names).sort();
  }

  // ---- validation ---------------------------------------------------------

  private validateStep(item: StepItem): FieldErrors {
    const errs: FieldErrors = {};
    const id = item.id.trim();
    if (!id) errs.id = this.t("builderIdRequired");
    else if (!ID_RE.test(id)) errs.id = this.t("builderIdInvalid");
    else {
      const others = new Set<string>();
      for (const it of this.items) {
        if (it === item) continue;
        if (it.kind === "step") others.add(it.id.trim());
        else collectIds(it.block, others);
      }
      if (others.has(id)) errs.id = this.t("builderIdDuplicate");
    }
    try {
      const parsed: unknown = JSON.parse(item.paramsText || "{}");
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) errs.params = this.t("builderInvalidJson");
    } catch {
      errs.params = this.t("builderInvalidJson");
    }
    return errs;
  }

  private validateAll(): boolean {
    this.errors.clear();
    let ok = true;
    for (const it of this.items) {
      if (it.kind !== "step") continue;
      const errs = this.validateStep(it);
      if (errs.id || errs.params) {
        ok = false;
        this.errors.set(it.key, errs);
      }
    }
    return ok;
  }

  // ---- mutations ----------------------------------------------------------

  private labelOf(item: Item): string {
    return item.kind === "step" ? item.id || this.t("builderStep", { n: this.items.indexOf(item) + 1 }) : item.block.id;
  }

  /** Moves the item at `from` by `delta` positions. */
  moveStep(from: number, delta: -1 | 1): void {
    const to = from + delta;
    if (this.readOnly || to < 0 || to >= this.items.length) return;
    const [item] = this.items.splice(from, 1);
    if (!item) return;
    this.items.splice(to, 0, item);
    this.setDirty(true);
    this.saveMessage = null;
    // Keep focus on the same control; if it is now disabled, use its sibling.
    const edge = to === 0 || to === this.items.length - 1;
    const key = delta < 0 ? (to === 0 ? `down-${item.key}` : `up-${item.key}`) : edge ? `up-${item.key}` : `down-${item.key}`;
    this.pendingFocus = key;
    this.renderContent();
    this.announce(this.t("builderMoved", { step: this.labelOf(item), n: to + 1 }));
  }

  removeStep(index: number): void {
    if (this.readOnly) return;
    const [item] = this.items.splice(index, 1);
    if (!item) return;
    this.errors.delete(item.key);
    this.setDirty(true);
    this.saveMessage = null;
    const neighbour = this.items[index] ?? this.items[index - 1];
    this.pendingFocus = neighbour ? `remove-${neighbour.key}` : "add-handler";
    this.renderContent();
    this.announce(this.t("builderRemoved", { step: this.labelOf(item) }));
  }

  addStep(handler: string): void {
    if (this.readOnly || !handler) return;
    const taken = new Set<string>();
    for (const it of this.items) {
      if (it.kind === "step") taken.add(it.id);
      else collectIds(it.block, taken);
    }
    const base = handler.replace(/[^A-Za-z0-9_.-]+/g, "_").replace(/^_+|_+$/g, "") || "step";
    let id = base;
    for (let n = 2; taken.has(id); n++) id = `${base}_${n}`;
    const info = this.handlers.find((hd) => hd.name === handler);
    const item: StepItem = {
      kind: "step",
      key: uid("o8-step"),
      id,
      handler,
      paramsText: JSON.stringify(info?.default_params ?? {}, null, 2),
      rest: {},
    };
    this.items.push(item);
    this.setDirty(true);
    this.saveMessage = null;
    this.pendingFocus = `id-${item.key}`;
    this.renderContent();
    this.announce(this.t("builderAdded", { step: id }));
  }

  /** Validates and saves via PUT. Resolves false when validation fails. */
  async save(): Promise<boolean> {
    const name = this.sequence;
    if (!name || !this.definition || this.readOnly || this.saving) return false;
    if (!this.validateAll()) {
      this.saveMessage = { text: this.t("builderFixErrors"), error: true };
      const first = this.items.find((it) => this.errors.has(it.key));
      const errs = first ? this.errors.get(first.key) : undefined;
      this.pendingFocus = first ? (errs?.id ? `id-${first.key}` : `params-${first.key}`) : null;
      this.renderContent();
      return false;
    }
    const blocks: BlockJson[] = this.items.map((it) =>
      it.kind === "step"
        ? { ...it.rest, type: "step", id: it.id.trim(), handler: it.handler, params: JSON.parse(it.paramsText || "{}") as unknown }
        : it.block,
    );
    const definition: SequenceDefinitionJson = { ...this.definition, blocks };
    this.saving = true;
    this.saveMessage = null;
    this.renderContent();
    try {
      const res = await this.client.putSequence(name, definition);
      const saved = unwrapDefinition(res) ?? definition;
      this.setModel(saved);
      this.saveMessage = { text: this.t("builderSaved"), error: false };
      this.announce(this.t("builderSaved"));
      this.emit<SequenceSavedDetail>("orch8-sequence-saved", { name, definition: saved });
      return true;
    } catch (err) {
      const { message, detail } = this.errorMessage(err);
      this.saveMessage = { text: detail ? `${message} ${detail}` : message, error: true };
      return false;
    } finally {
      this.saving = false;
      this.pendingFocus = this.saveMessage?.error ? "save" : this.pendingFocus;
      this.renderContent();
    }
  }

  // ---- rendering ----------------------------------------------------------

  private field(
    label: string,
    control: HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement,
    error: string | undefined,
  ): { wrap: HTMLElement; errorEl: HTMLElement } {
    const id = uid("o8-f");
    const errId = `${id}-err`;
    control.id = id;
    control.setAttribute("aria-describedby", errId);
    if (error) control.setAttribute("aria-invalid", "true");
    const errorEl = h("div", { class: "field-error", id: errId }, error ?? "");
    return { wrap: h("div", { class: "field" }, h("label", { for: id }, label), control, errorEl), errorEl };
  }

  private handlerSelect(current: string, attrs: Record<string, string | boolean | null>): HTMLSelectElement {
    const names = this.handlerNames();
    if (current && !names.includes(current)) names.unshift(current);
    const select = h("select", attrs);
    for (const name of names) {
      const info = this.handlers.find((hd) => hd.name === name);
      select.append(h("option", { value: name, title: info?.description ?? null }, info?.label ?? name));
    }
    select.value = current || names[0] || "";
    return select;
  }

  private renderStep(item: StepItem, index: number, total: number, ro: boolean): HTMLElement {
    const errs = this.errors.get(item.key) ?? {};
    const label = item.id || this.t("builderStep", { n: index + 1 });
    const headingId = uid("o8-h");
    const heading = h("h3", { id: headingId }, `${this.t("builderStep", { n: index + 1 })}: `, h("span", { "data-role": "label" }, label));

    const idInput = h("input", { type: "text", value: item.id, disabled: ro, "data-key": `id-${item.key}`, autocomplete: "off", spellcheck: "false" });
    const idField = this.field(this.t("builderStepId"), idInput, errs.id);
    idInput.addEventListener("input", () => {
      item.id = idInput.value;
      (heading.querySelector('[data-role="label"]') as HTMLElement).textContent = item.id || this.t("builderStep", { n: index + 1 });
      this.liveValidate(item, "id", idInput, idField.errorEl);
      this.setDirty(true);
    });

    const select = this.handlerSelect(item.handler, { disabled: ro, "data-key": `handler-${item.key}` });
    const handlerField = this.field(this.t("builderHandler"), select, undefined);
    select.addEventListener("change", () => {
      item.handler = select.value;
      this.setDirty(true);
    });

    const params = h("textarea", { rows: "4", disabled: ro, "data-key": `params-${item.key}`, spellcheck: "false" });
    params.value = item.paramsText;
    const paramsField = this.field(this.t("builderParams"), params, errs.params);
    params.addEventListener("input", () => {
      item.paramsText = params.value;
      this.liveValidate(item, "params", params, paramsField.errorEl);
      this.setDirty(true);
    });

    return h(
      "li",
      { class: "step", part: "step", "aria-labelledby": headingId },
      h("div", { class: "step-head" }, heading, ro ? null : this.stepActions(item, index, total)),
      h("div", { class: "grid" }, idField.wrap, handlerField.wrap),
      paramsField.wrap,
    );
  }

  private liveValidate(item: StepItem, field: keyof FieldErrors, control: HTMLElement, errorEl: HTMLElement): void {
    const errs = this.validateStep(item);
    const msg = errs[field];
    const current = this.errors.get(item.key) ?? {};
    current[field] = msg;
    this.errors.set(item.key, current);
    errorEl.textContent = msg ?? "";
    if (msg) control.setAttribute("aria-invalid", "true");
    else control.removeAttribute("aria-invalid");
  }

  private stepActions(item: Item, index: number, total: number): HTMLElement {
    const name = this.labelOf(item);
    return h(
      "div",
      { class: "step-actions" },
      h(
        "button",
        { type: "button", "aria-label": this.t("builderMoveUp", { step: name }), title: this.t("builderMoveUp", { step: name }), disabled: index === 0, "data-key": `up-${item.key}`, onclick: () => this.moveStep(this.items.indexOf(item), -1) },
        h("span", { "aria-hidden": "true" }, "↑"),
      ),
      h(
        "button",
        { type: "button", "aria-label": this.t("builderMoveDown", { step: name }), title: this.t("builderMoveDown", { step: name }), disabled: index === total - 1, "data-key": `down-${item.key}`, onclick: () => this.moveStep(this.items.indexOf(item), 1) },
        h("span", { "aria-hidden": "true" }, "↓"),
      ),
      h(
        "button",
        { type: "button", "aria-label": this.t("builderRemove", { step: name }), title: this.t("builderRemove", { step: name }), "data-key": `remove-${item.key}`, onclick: () => this.removeStep(this.items.indexOf(item)) },
        h("span", { "aria-hidden": "true" }, "✕"),
      ),
    );
  }

  private renderLocked(item: LockedItem, index: number, total: number, ro: boolean): HTMLElement {
    const headingId = uid("o8-h");
    return h(
      "li",
      { class: "step locked", part: "step", "aria-labelledby": headingId },
      h(
        "div",
        { class: "step-head" },
        h("h3", { id: headingId }, `${this.t("builderStep", { n: index + 1 })}: `, this.t("builderLocked", { type: item.block.type, id: item.block.id })),
        ro ? null : this.stepActions(item, index, total),
      ),
    );
  }

  protected renderContent(): void {
    const name = this.sequence;
    if (!name || !this.definition) {
      this.body.replaceChildren(h("div", { class: "status", part: "empty" }, this.t("builderNoSequence")));
      return;
    }
    const ro = this.readOnly;
    this.dirtyEl = h("span", { class: "dirty", "aria-live": "off" }, this.dirty ? this.t("builderUnsaved") : "");
    const header = this.renderHeader(this.t("builderTitle", { sequence: name }), this.dirtyEl);

    const total = this.items.length;
    const list =
      total === 0
        ? h("p", { class: "status", part: "empty" }, this.t("builderEmpty"))
        : h(
            "ol",
            { class: "steps", part: "steps" },
            this.items.map((it, i) => (it.kind === "step" ? this.renderStep(it, i, total, ro) : this.renderLocked(it, i, total, ro))),
          );

    let addRow: HTMLElement | null = null;
    if (!ro) {
      const names = this.handlerNames();
      let control: HTMLInputElement | HTMLSelectElement;
      if (names.length) {
        control = this.handlerSelect(names[0] ?? "", { "data-key": "add-handler" });
      } else {
        control = h("input", { type: "text", "data-key": "add-handler", autocomplete: "off", spellcheck: "false" });
      }
      const f = this.field(this.t("builderAddHandler"), control, undefined);
      addRow = h(
        "div",
        { class: "add" },
        f.wrap,
        h("button", { type: "button", "data-key": "add", onclick: () => this.addStep(control.value.trim()) }, this.t("builderAdd")),
      );
    }

    this.saveBtn = h(
      "button",
      { type: "button", class: "primary", "data-key": "save", disabled: ro || !this.dirty || this.saving, "aria-busy": this.saving ? "true" : null, onclick: () => void this.save() },
      this.saving ? this.t("builderSaving") : this.t("builderSave"),
    );
    const msg = this.saveMessage
      ? h("span", { class: this.saveMessage.error ? "field-error" : "ok", role: this.saveMessage.error ? "alert" : null }, this.saveMessage.text)
      : null;
    const saveBar = ro ? null : h("div", { class: "savebar" }, msg, this.saveBtn);

    this.withFocusPreserved(() =>
      this.body.replaceChildren(
        header,
        ro ? h("p", { class: "small muted" }, this.t("builderReadOnly")) : "",
        list,
        addRow ?? "",
        saveBar ?? "",
      ),
    );
    if (this.pendingFocus) {
      const target = this.root.querySelector<HTMLElement>(`[data-key="${CSS.escape(this.pendingFocus)}"]`);
      this.pendingFocus = null;
      target?.focus();
    }
  }
}
