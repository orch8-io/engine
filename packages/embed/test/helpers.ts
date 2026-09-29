import { vi } from "vitest";

export function b64url(s: string): string {
  return Buffer.from(s, "utf8").toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/** Builds an unsigned-but-well-formed o8e1 token (signature is not checked client side). */
export function makeToken(overrides: Partial<{ scp: string[]; exp: number; tid: string; sub: string; jti: string }> = {}): string {
  const now = Math.floor(Date.now() / 1000);
  const payload = {
    v: 1,
    tid: "acme",
    sub: "customer-1",
    scp: ["runs:read", "runs:start", "approvals:resolve", "sequences:read", "builder:edit"],
    seq: null,
    iat: now,
    exp: now + 600,
    jti: overrides.jti ?? Math.random().toString(36).slice(2),
    ...overrides,
  };
  return `o8e1.${b64url(JSON.stringify(payload))}.${b64url("sig")}`;
}

export interface Call {
  method: string;
  path: string;
  url: string;
  auth: string | null;
  body: unknown;
}

type StaticReply = { status?: number; body?: unknown; headers?: Record<string, string> };
type Reply = StaticReply | ((call: Call) => StaticReply);

/**
 * Mock engine: routes are `"GET /runs"` style keys relative to /api/v1/embed.
 * A route value may be a list, consumed in order (last one repeats).
 */
export function mockEngine(routes: Record<string, Reply | Reply[]>) {
  const calls: Call[] = [];
  const counters = new Map<string, number>();
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input));
    const method = (init?.method ?? "GET").toUpperCase();
    const path = url.pathname.replace(/^\/api\/v1\/embed/, "");
    const headers = new Headers(init?.headers);
    const call: Call = {
      method,
      path,
      url: url.href,
      auth: headers.get("authorization"),
      body: init?.body ? JSON.parse(String(init.body)) : undefined,
    };
    calls.push(call);
    const key = `${method} ${path}`;
    let route = routes[key];
    if (route === undefined) {
      const pattern = Object.keys(routes).find((k) => {
        const [m, p] = k.split(" ");
        if (m !== method || !p?.includes("*")) return false;
        return new RegExp(`^${p.replace(/[.+?^${}()|[\]\\]/g, "\\$&").replace(/\*/g, "[^/]+")}$`).test(path);
      });
      route = pattern ? routes[pattern] : undefined;
    }
    if (route === undefined) return new Response(JSON.stringify({ error: { code: "not_found", message: `no mock for ${key}` } }), { status: 404 });
    let reply: Reply;
    if (Array.isArray(route)) {
      const n = counters.get(key) ?? 0;
      counters.set(key, n + 1);
      reply = route[Math.min(n, route.length - 1)]!;
    } else reply = route;
    const r = typeof reply === "function" ? reply(call) : reply;
    return new Response(r.body === undefined ? "" : JSON.stringify(r.body), {
      status: r.status ?? 200,
      headers: { "content-type": "application/json", ...(r.headers ?? {}) },
    });
  });
  return { fetch: fetchMock as unknown as typeof fetch, calls };
}

export const BASE = "https://engine.test";

export const THEME = { css_vars: {}, logo_url: null, hide_badge: false };

/** Waits until `fn` stops throwing (or times out). */
export async function waitFor<T>(fn: () => T, timeout = 2000): Promise<NonNullable<T>> {
  const start = Date.now();
  let lastErr: unknown;
  while (Date.now() - start < timeout) {
    try {
      const v = fn();
      if (v === null || v === undefined) throw new Error("waitFor: value not ready");
      return v as NonNullable<T>;
    } catch (err) {
      lastErr = err;
      await new Promise((r) => setTimeout(r, 10));
    }
  }
  throw lastErr;
}

/** Creates, configures and attaches an element; returns it and its shadow root. */
export function mount<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  attrs: Record<string, string>,
  props: Partial<Record<string, unknown>> = {},
) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(props)) (el as unknown as Record<string, unknown>)[k] = v;
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
  document.body.appendChild(el);
  return { el, root: el.shadowRoot! };
}

export function text(root: ParentNode): string {
  return (root.textContent ?? "").replace(/\s+/g, " ").trim();
}
