import { renderBadge } from "../badge.js";
import { HTMLElementBase } from "../base.js";
import { defaultStrings, type Orch8Strings } from "../i18n.js";

/**
 * `<orch8-badge vendor="acme">` — standalone "Powered by Orch8" link. The other
 * widgets render the same link in their footer unless the vendor's theme sets
 * `hide_badge` (a `white_label` license feature, enforced by the engine).
 * The badge has no network dependency; place it wherever you like.
 */
export class Orch8BadgeElement extends HTMLElementBase {
  static observedAttributes = ["vendor", "color-scheme"];
  private _strings: Partial<Orch8Strings> = {};
  private readonly root: ShadowRoot;

  constructor() {
    super();
    this.root = this.attachShadow({ mode: "open" });
  }

  get strings(): Partial<Orch8Strings> {
    return this._strings;
  }
  set strings(value: Partial<Orch8Strings>) {
    this._strings = value && typeof value === "object" ? { ...value } : {};
    this.render();
  }

  connectedCallback(): void {
    this.render();
  }

  attributeChangedCallback(): void {
    if (this.isConnected) this.render();
  }

  private render(): void {
    const style = document.createElement("style");
    style.textContent = `
      :host { display: inline-block; font-family: var(--orch8-font, system-ui, -apple-system, "Segoe UI", Roboto, sans-serif); }
      :host([hidden]) { display: none; }
      a { font-size: var(--orch8-badge-font-size, 12px); color: var(--orch8-muted, #555c69); text-decoration: none;
        display: inline-flex; align-items: center; gap: 4px; }
      a:hover { text-decoration: underline; color: var(--orch8-fg, #1a1d23); }
      a:focus-visible { outline: 2px solid var(--orch8-focus, #3346c4); outline-offset: 2px; border-radius: 2px; }
      .badge-mark { width: 12px; height: 12px; border-radius: 3px; background: var(--orch8-accent, #3346c4); display: inline-block; }
      :host([color-scheme="dark"]) a { color: var(--orch8-dark-muted, #a6adb9); }
      @media (prefers-color-scheme: dark) { :host(:not([color-scheme="light"])) a { color: var(--orch8-dark-muted, #a6adb9); } }
    `;
    const text = this._strings.poweredBy ?? defaultStrings.poweredBy;
    const label = this._strings.poweredByLabel ?? defaultStrings.poweredByLabel;
    this.root.replaceChildren(style, renderBadge(this.getAttribute("vendor"), text, label));
  }
}
