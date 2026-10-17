import { renderBadge } from "../badge.js";
import { HTMLElementBase } from "../base.js";
import { createStyleSlot, type StyleSlot } from "../styles.js";
import { VERSION } from "../version.js";
import { defaultStrings, type Orch8Strings } from "../i18n.js";

const BADGE_CSS = `
  :host { display: inline-block; font-family: var(--orch8-font, system-ui, -apple-system, "Segoe UI", Roboto, sans-serif); }
  :host([hidden]) { display: none; }
  a { font-size: var(--orch8-badge-font-size, 12px); color: var(--orch8-muted, #555c69); text-decoration: none;
    display: inline-flex; align-items: center; gap: 4px; }
  a:hover { text-decoration: underline; color: var(--orch8-fg, #1a1d23); }
  a:focus-visible { outline: 2px solid var(--orch8-focus, #3346c4); outline-offset: 2px; border-radius: 2px; }
  .badge-mark { width: 12px; height: 12px; border-radius: 3px; background: var(--orch8-accent, #3346c4); display: inline-block; }
  :host([color-scheme="dark"]) a { color: var(--orch8-dark-muted, #a6adb9); }
  @media (prefers-color-scheme: dark) { :host(:not([color-scheme="light"])) a { color: var(--orch8-dark-muted, #a6adb9); } }
  @media (forced-colors: active) { .badge-mark { background: CanvasText; forced-color-adjust: none; } }
`;

/**
 * `<orch8-badge vendor="acme">` — standalone "Powered by Orch8" link. The other
 * widgets render the same link in their footer unless the vendor's theme sets
 * `hide_badge` (a `white_label` license feature, enforced by the engine).
 * The badge has no network dependency; place it wherever you like.
 */
export class Orch8BadgeElement extends HTMLElementBase {
  static observedAttributes = ["vendor", "color-scheme", "nonce"];
  static readonly orch8Version: string = VERSION;
  private _strings: Partial<Orch8Strings> = {};
  private readonly root: ShadowRoot;
  private styleSlot: StyleSlot | null = null;
  private anchor: Element | null = null;

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

  attributeChangedCallback(name: string, _old: string | null, value: string | null): void {
    if (name === "nonce") this.styleSlot?.setNonce(value);
    else if (this.isConnected) this.render();
  }

  private render(): void {
    this.styleSlot ??= createStyleSlot(this.root, BADGE_CSS, this.getAttribute("nonce"), true);
    const text = this._strings.poweredBy ?? defaultStrings.poweredBy;
    const label = this._strings.poweredByLabel ?? defaultStrings.poweredByLabel;
    const next = renderBadge(this.getAttribute("vendor"), text, label);
    if (this.anchor?.parentNode === this.root) this.anchor.replaceWith(next);
    else this.root.append(next);
    this.anchor = next;
  }
}
