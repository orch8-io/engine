import type { EmbedClient } from "./client.js";
import { tokenAuthorizationIdentity } from "./token.js";
import type { EmbedTheme } from "./types.js";

const CACHE_TTL_MS = 5 * 60_000;
const MAX_CACHED_THEMES = 128;
const cache = new Map<string, { at: number; promise: Promise<EmbedTheme> }>();

export const DEFAULT_THEME: EmbedTheme = { css_vars: {}, logo_url: null, hide_badge: false };

/** Clears the per-page theme cache (e.g. after a vendor updates their theme). */
export function clearThemeCache(): void {
  cache.clear();
}

/**
 * Shares themes for the same engine and authorization identity for 5 minutes.
 * Opaque credentials stay isolated. Failures fall back to the default theme.
 */
export async function loadTheme(client: EmbedClient): Promise<EmbedTheme> {
  let token: string;
  try {
    token = await client.getToken();
  } catch {
    return DEFAULT_THEME;
  }
  const key = JSON.stringify([client.baseUrl, tokenAuthorizationIdentity(token) ?? token]);
  const hit = cache.get(key);
  const age = hit ? Date.now() - hit.at : Number.POSITIVE_INFINITY;
  if (hit && age >= 0 && age < CACHE_TTL_MS) {
    cache.delete(key);
    cache.set(key, hit);
    return hit.promise;
  }
  const promise = client.getTheme().catch(() => {
    // A failed expired request must not delete a newer replacement's entry.
    if (cache.get(key)?.promise === promise) cache.delete(key);
    return DEFAULT_THEME;
  });
  cache.delete(key);
  cache.set(key, { at: Date.now(), promise });
  if (cache.size > MAX_CACHED_THEMES) {
    const oldest = cache.keys().next();
    if (!oldest.done) cache.delete(oldest.value);
  }
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

/** Only absolute https URLs are allowed for the vendor logo (no mixed content, no data:/javascript:). */
export function safeLogoUrl(url: string | null | undefined): string | null {
  if (!url) return null;
  try {
    const parsed = new URL(url);
    return parsed.protocol === "https:" ? parsed.href : null;
  } catch {
    return null;
  }
}
