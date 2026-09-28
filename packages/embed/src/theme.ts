import type { EmbedClient } from "./client.js";
import { decodeEmbedToken } from "./token.js";
import type { EmbedTheme } from "./types.js";

const CACHE_TTL_MS = 5 * 60_000;
const cache = new Map<string, { at: number; promise: Promise<EmbedTheme> }>();

export const DEFAULT_THEME: EmbedTheme = { css_vars: {}, logo_url: null, hide_badge: false };

/** Clears the per-page theme cache (e.g. after a vendor updates their theme). */
export function clearThemeCache(): void {
  cache.clear();
}

/**
 * Fetches `/embed/theme` once per (engine, tenant, sub-tenant) per 5 minutes and
 * shares it across all widgets on the page. Never rejects: failures fall back to
 * the default theme (badge shown).
 */
export function loadTheme(client: EmbedClient): Promise<EmbedTheme> {
  const payload = decodeEmbedToken(client.currentToken);
  const key = `${client.baseUrl}|${payload?.tid ?? "?"}|${payload?.sub ?? "?"}`;
  const hit = cache.get(key);
  if (hit && Date.now() - hit.at < CACHE_TTL_MS) return hit.promise;
  const promise = client.getTheme().catch(() => {
    cache.delete(key);
    return DEFAULT_THEME;
  });
  cache.set(key, { at: Date.now(), promise });
  return promise;
}

const NAME_RE = /^(?:--orch8-)?([a-z0-9]+(?:-[a-z0-9]+)*)$/;
const FORBIDDEN_VALUE = /[;{}<>\\]|url\s*\(|expression\s*\(|@import|\/\*/i;

/**
 * Turns `{ "accent": "#f00", "--orch8-radius": "4px" }` into safe
 * `--orch8-*` declarations. Anything that could break out of a declaration
 * or load a resource is dropped.
 */
export function sanitizeCssVars(vars: Record<string, unknown> | null | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  if (!vars || typeof vars !== "object") return out;
  for (const [rawName, rawValue] of Object.entries(vars)) {
    const m = NAME_RE.exec(rawName.trim().toLowerCase());
    if (!m || typeof rawValue !== "string") continue;
    const value = rawValue.trim();
    if (!value || value.length > 200 || FORBIDDEN_VALUE.test(value)) continue;
    out[`--orch8-${m[1]}`] = value;
  }
  return out;
}

export function cssVarsRule(vars: Record<string, string>): string {
  const decls = Object.entries(vars)
    .map(([k, v]) => `${k}: ${v};`)
    .join(" ");
  return decls ? `:host { ${decls} }` : "";
}

/** Only absolute http(s) URLs are allowed for the vendor logo. */
export function safeLogoUrl(url: string | null | undefined): string | null {
  if (!url) return null;
  try {
    const parsed = new URL(url);
    return parsed.protocol === "https:" || parsed.protocol === "http:" ? parsed.href : null;
  } catch {
    return null;
  }
}
