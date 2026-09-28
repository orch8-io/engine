import { renderBadge } from "./badge.js";
import { EmbedClient, Orch8EmbedError } from "./client.js";
import { h } from "./dom.js";
import { defaultStrings, formatString, type Orch8StringKey, type Orch8Strings } from "./i18n.js";
import { baseCss } from "./styles.js";
import { cssVarsRule, DEFAULT_THEME, loadTheme, safeLogoUrl, sanitizeCssVars } from "./theme.js";
import { decodeEmbedToken, type TokenProvider } from "./token.js";
import type { EmbedTheme } from "./types.js";

/**
 * SSR guard: on the server `HTMLElement` doesn't exist, so the classes extend an
 * inert stand-in and can still be imported (they are never instantiated there).
 */
export const HTMLElementBase: typeof HTMLElement =
  typeof HTMLElement === "undefined" ? (class {} as unknown as typeof HTMLElement) : HTMLElement;

export const BASE_ATTRIBUTES = ["token", "base-url", "color-scheme", "theme", "locale", "vendor"] as const;

type LoadMode = "initial" | "refresh";

/**
 * Shared plumbing: shadow root, client construction, theme, i18n, lifecycle,
 * loading/error states, polling and the live region.
 */
export abstract class Orch8Element extends HTMLElementBase {
  protected readonly root: ShadowRoot;
  protected readonly body: HTMLDivElement;
  private readonly themeStyle: HTMLStyleElement;
  private readonly live: HTMLDivElement;
  private readonly footer: HTMLDivElement;
  private _client: EmbedClient | null = null;
  private _tokenProvider: TokenProvider | null = null;
  private _fetch: typeof fetch | undefined;
  private _strings: Partial<Orch8Strings> = {};
  private abortCtl: AbortController | null = null;
  private pollTimer: ReturnType<typeof setTimeout> | null = null;
  private pollFailures = 0;
  private scheduled = false;
  private visibilityHandler: (() => void) | null = null;
  protected serverTheme: EmbedTheme | null = null;
  /** Rendering phase; `render()` dispatches on it. */
  protected phase: "loading" | "error" | "ready" = "loading";
  private lastError: unknown = null;

  /** Whether the "Powered by Orch8" badge is rendered in this element's footer. */
  protected showsBadge = true;

  constructor() {
    super();
    this.root = this.attachShadow({ mode: "open" });
    const style = document.createElement("style");
    style.textContent = baseCss + this.extraCss();
    this.themeStyle = document.createElement("style");
    this.body = h("div", { part: "body", class: "body" });
    this.live = h("div", { class: "sr-only", role: "status", "aria-live": "polite", "aria-atomic": "true" });
    this.footer = h("div", { class: "footer", part: "footer" });
    this.root.append(style, this.themeStyle, this.body, this.footer, this.live);
  }

  /** Element-specific CSS appended to the base sheet. */
  protected extraCss(): string {
    return "";
  }

  // ---- public API -------------------------------------------------------

  get token(): string | null {
    return this.getAttribute("token");
  }
  set token(value: string | null) {
    if (value) this.setAttribute("token", value);
    else this.removeAttribute("token");
  }

  get baseUrl(): string | null {
    return this.getAttribute("base-url");
  }
  set baseUrl(value: string | null) {
    if (value) this.setAttribute("base-url", value);
    else this.removeAttribute("base-url");
  }

  /** Called whenever a (new) token is needed. Takes precedence over expiry of `token`. */
  get tokenProvider(): TokenProvider | null {
    return this._tokenProvider;
  }
  set tokenProvider(fn: TokenProvider | null) {
    this._tokenProvider = typeof fn === "function" ? fn : null;
    this.invalidate();
  }

  /** Custom fetch (proxies, tests). */
  get fetchImpl(): typeof fetch | undefined {
    return this._fetch;
  }
  set fetchImpl(fn: typeof fetch | undefined) {
    this._fetch = fn;
    this.invalidate();
  }

  /** Partial overrides of {@link defaultStrings}. */
  get strings(): Partial<Orch8Strings> {
    return this._strings;
  }
  set strings(value: Partial<Orch8Strings>) {
    this._strings = value && typeof value === "object" ? { ...value } : {};
    if (this.isConnected) this.render();
  }

  /** Forces a reload from the server. */
  refresh(): void {
    this.scheduleLoad("refresh");
  }

  // ---- lifecycle --------------------------------------------------------

  connectedCallback(): void {
    this.visibilityHandler = () => {
      if (!document.hidden && this.pollTimer === null && this.wantsPolling()) this.schedulePoll(0);
    };
    document.addEventListener("visibilitychange", this.visibilityHandler);
    this.scheduleLoad("initial");
  }

  disconnectedCallback(): void {
    this.abortCtl?.abort();
    this.abortCtl = null;
    this.clearPoll();
    if (this.visibilityHandler) document.removeEventListener("visibilitychange", this.visibilityHandler);
    this.visibilityHandler = null;
  }

  attributeChangedCallback(name: string, oldValue: string | null, newValue: string | null): void {
    if (oldValue === newValue) return;
    if (name === "color-scheme" || name === "locale") {
      if (name === "locale") this.render();
      return;
    }
    if (name === "theme") {
      this.applyTheme();
      return;
    }
    if (name === "vendor") {
      this.renderFooter();
      return;
    }
    if (name === "token" || name === "base-url") this._client = null;
    this.onAttributeChanged(name);
    if (this.isConnected) this.scheduleLoad("initial");
  }

  /** Hook for subclass-specific attribute handling before reload. */
  protected onAttributeChanged(_name: string): void {}

  // ---- helpers for subclasses ------------------------------------------

  protected t(key: Orch8StringKey, vars?: Record<string, string | number>): string {
    return formatString(this._strings[key] ?? defaultStrings[key], vars);
  }

  protected stateLabel(state: string): string {
    const key = `state_${state}` as Orch8StringKey;
    return key in defaultStrings ? this.t(key) : state;
  }

  protected renderState(state: string): HTMLSpanElement {
    return h("span", { class: `state state-${state.replace(/[^a-z_-]/gi, "")}`, part: "state" }, this.stateLabel(state));
  }

  protected get displayLocale(): string | undefined {
    return this.getAttribute("locale") || undefined;
  }

  protected get client(): EmbedClient {
    if (!this._client) {
      const baseUrl = this.baseUrl ?? (typeof location !== "undefined" ? location.origin : "");
      this._client = new EmbedClient({
        baseUrl,
        token: this.token,
        tokenProvider: this._tokenProvider,
        fetch: this._fetch,
      });
    }
    return this._client;
  }

  /** Decoded payload of the current token, if any. */
  protected get tokenPayload() {
    return decodeEmbedToken(this._client?.currentToken ?? this.token);
  }

  protected hasScope(scope: string): boolean {
    const payload = this.tokenPayload;
    return payload ? payload.scp.includes(scope) : true;
  }

  protected announce(message: string): void {
    // Clear first so repeating the same message is announced again.
    this.live.textContent = "";
    setTimeout(() => {
      this.live.textContent = message;
    }, 50);
  }

  protected emit<T>(name: string, detail: T): void {
    this.dispatchEvent(new CustomEvent(name, { detail, bubbles: true, composed: true }));
  }

  protected errorMessage(err: unknown): { message: string; detail: string | null } {
    const e = err instanceof Orch8EmbedError ? err : null;
    switch (e?.kind) {
      case "config":
        return { message: this.t("errorConfig"), detail: null };
      case "unauthorized":
        return { message: this.t("errorUnauthorized"), detail: null };
      case "forbidden":
        return { message: this.t("errorForbidden"), detail: e.message || null };
      case "not_found":
        return { message: this.t("errorNotFound"), detail: null };
      case "rate_limited":
        return { message: this.t("errorRateLimited"), detail: null };
      case "network":
        return { message: this.t("errorNetwork"), detail: null };
      case "invalid":
      case "conflict":
        return { message: this.t("errorInvalid", { message: e.message }), detail: null };
      default:
        return { message: this.t("errorGeneric"), detail: null };
    }
  }

  protected setBusy(busy: boolean): void {
    if (busy) this.setAttribute("aria-busy", "true");
    else this.removeAttribute("aria-busy");
  }

  protected renderLoading(): void {
    this.body.replaceChildren(
      h("div", { class: "status", part: "loading" }, h("span", { class: "spinner", "aria-hidden": "true" }), this.t("loading")),
    );
  }

  protected renderError(err: unknown): void {
    const { message, detail } = this.errorMessage(err);
    const retry =
      err instanceof Orch8EmbedError && (err.kind === "config" || err.kind === "forbidden")
        ? null
        : h("button", { type: "button", onclick: () => this.scheduleLoad("initial") }, this.t("retry"));
    this.body.replaceChildren(
      h(
        "div",
        { class: "status", role: "alert", part: "error" },
        message,
        detail ? h("span", { class: "detail" }, detail) : null,
        retry ? h("div", null, retry) : null,
      ),
    );
  }

  protected renderHeader(title: string, ...actions: (Node | null)[]): HTMLElement {
    const logo = safeLogoUrl(this.serverTheme?.logo_url);
    return h(
      "div",
      { class: "header", part: "header" },
      h(
        "h2",
        { class: "title", part: "title" },
        logo ? h("img", { class: "logo", src: logo, alt: "", part: "logo" }) : null,
        title,
      ),
      actions.some(Boolean) ? h("div", { class: "actions" }, ...actions) : null,
    );
  }

  /**
   * Preserves keyboard focus across a re-render: the focused element inside the
   * shadow root must carry `data-key`, and the replacement with the same key
   * gets focus back.
   */
  protected withFocusPreserved(fn: () => void): void {
    const active = this.root.activeElement as HTMLElement | null;
    const key = active?.getAttribute?.("data-key") ?? null;
    fn();
    if (key) {
      const next = this.root.querySelector<HTMLElement>(`[data-key="${CSS.escape(key)}"]`);
      next?.focus();
    }
  }

  // ---- data loading -----------------------------------------------------

  /** Subclasses fetch into their own state. Throwing renders the error state. */
  protected abstract load(signal: AbortSignal, mode: LoadMode): Promise<void>;
  /** Renders `this.body` from loaded state (status is "ready"). */
  protected abstract renderContent(): void;

  /** Re-renders according to the current status. */
  protected render(): void {
    if (this.phase === "error") this.renderError(this.lastError);
    else if (this.phase === "loading" && this.ready()) this.renderLoading();
    else this.renderContent();
  }
  /** Poll interval in ms; 0 disables polling. */
  protected pollInterval(): number {
    return 0;
  }
  /** Whether polling should continue given the current state. */
  protected wantsPolling(): boolean {
    return this.pollInterval() > 0;
  }
  /** Whether the element has enough attributes to load at all. */
  protected ready(): boolean {
    return true;
  }

  private invalidate(): void {
    this._client = null;
    if (this.isConnected) this.scheduleLoad("initial");
  }

  protected scheduleLoad(mode: LoadMode): void {
    if (this.scheduled) return;
    this.scheduled = true;
    queueMicrotask(() => {
      this.scheduled = false;
      if (this.isConnected) void this.runLoad(mode);
    });
  }

  private clearPoll(): void {
    if (this.pollTimer !== null) clearTimeout(this.pollTimer);
    this.pollTimer = null;
  }

  private schedulePoll(delay: number): void {
    this.clearPoll();
    if (!this.isConnected) return;
    this.pollTimer = setTimeout(() => {
      this.pollTimer = null;
      if (typeof document !== "undefined" && document.hidden) return; // resumed by visibilitychange
      void this.runLoad("refresh");
    }, delay);
  }

  private async runLoad(mode: LoadMode): Promise<void> {
    this.abortCtl?.abort();
    this.clearPoll();
    const ctl = new AbortController();
    this.abortCtl = ctl;
    if (!this.ready()) {
      this.phase = "ready";
      this.render();
      return;
    }
    if (!this.token && !this._tokenProvider) {
      this.fail(new Orch8EmbedError("config", "missing token"));
      return;
    }
    if (mode === "initial") {
      this.phase = "loading";
      this.render();
      this.setBusy(true);
    }
    try {
      await this.load(ctl.signal, mode);
      if (ctl.signal.aborted) return;
      this.pollFailures = 0;
      this.phase = "ready";
      this.lastError = null;
      this.render();
      // Theme after the first successful call so a refreshed token is used.
      if (!this.serverTheme) await this.fetchTheme();
    } catch (err) {
      if (ctl.signal.aborted || (err instanceof Orch8EmbedError && err.kind === "aborted")) return;
      if (mode === "refresh") {
        this.pollFailures += 1;
        this.showReconnecting(true);
      } else {
        this.fail(err);
        if (!this.serverTheme) await this.fetchTheme();
      }
      if (err instanceof Orch8EmbedError && ["config", "forbidden", "unauthorized", "not_found"].includes(err.kind)) {
        return; // polling won't fix these
      }
    } finally {
      if (this.abortCtl === ctl) this.setBusy(false);
    }
    if (mode === "refresh" && this.pollFailures === 0) this.showReconnecting(false);
    if (this.wantsPolling()) {
      const base = this.pollInterval();
      const delay = this.pollFailures ? Math.min(base * 2 ** this.pollFailures, 60_000) : base;
      this.schedulePoll(delay);
    }
  }

  private fail(err: unknown): void {
    this.phase = "error";
    this.lastError = err;
    this.render();
  }

  private showReconnecting(on: boolean): void {
    const existing = this.root.querySelector(".banner");
    if (on && !existing) {
      this.body.prepend(h("div", { class: "banner", part: "banner", role: "status" }, this.t("reconnecting")));
    } else if (!on && existing) {
      existing.remove();
    }
  }

  private async fetchTheme(): Promise<void> {
    let theme: EmbedTheme;
    try {
      theme = await loadTheme(this.client);
    } catch {
      theme = DEFAULT_THEME;
    }
    this.serverTheme = theme;
    this.applyTheme();
    if (theme.logo_url) this.render();
  }

  private applyTheme(): void {
    const server = sanitizeCssVars(this.serverTheme?.css_vars);
    let attr: Record<string, string> = {};
    const raw = this.getAttribute("theme");
    if (raw) {
      try {
        attr = sanitizeCssVars(JSON.parse(raw) as Record<string, unknown>);
      } catch {
        attr = {};
      }
    }
    // Attribute values override the server theme; page CSS on the host still wins over both.
    this.themeStyle.textContent = cssVarsRule({ ...server, ...attr });
    this.renderFooter();
  }

  protected renderFooter(): void {
    const show = this.showsBadge && this.serverTheme !== null && !this.serverTheme.hide_badge;
    if (!show) {
      this.footer.replaceChildren();
      this.footer.hidden = true;
      return;
    }
    const vendor = this.getAttribute("vendor") || this.tokenPayload?.tid || null;
    this.footer.hidden = false;
    this.footer.replaceChildren(this.footerExtras() ?? "", renderBadge(vendor, this.t("poweredBy"), this.t("poweredByLabel")));
  }

  /** Optional content placed at the start of the footer. */
  protected footerExtras(): Node | null {
    return null;
  }
}

export type { LoadMode };
