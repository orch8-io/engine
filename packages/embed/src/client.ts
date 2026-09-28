import { decodeEmbedToken, type TokenProvider } from "./token.js";
import type {
  ApprovalList,
  EmbedTheme,
  HandlerInfo,
  ResolveApprovalBody,
  RunDetail,
  RunList,
  SequenceDefinitionJson,
  SequenceList,
  SequenceSummary,
  StartRunBody,
} from "./types.js";

export type Orch8ErrorKind =
  | "config"
  | "unauthorized"
  | "forbidden"
  | "not_found"
  | "conflict"
  | "invalid"
  | "rate_limited"
  | "server"
  | "network"
  | "aborted";

/** Error thrown by {@link EmbedClient}. `code` is the engine's `error.code` when present. */
export class Orch8EmbedError extends Error {
  readonly kind: Orch8ErrorKind;
  readonly status: number;
  readonly code: string | undefined;
  readonly details: unknown;

  constructor(kind: Orch8ErrorKind, message: string, status = 0, code?: string, details?: unknown) {
    super(message);
    this.name = "Orch8EmbedError";
    this.kind = kind;
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

export interface EmbedClientOptions {
  /** Engine origin, e.g. `https://orch8.example.com`. `/api/v1/embed` is appended. */
  baseUrl: string;
  /** Static token. Used as the initial token when a provider is also set. */
  token?: string | null;
  /** Called for a new token when none is cached, it is about to expire, or on 401. */
  tokenProvider?: TokenProvider | null;
  /** Retries for idempotent requests (GET/PUT) on 5xx, 429 and network errors. */
  retries?: number;
  /** Base backoff in ms; attempt n waits `base * 2^n` (+ jitter), capped at 8s. */
  retryBaseMs?: number;
  /** Refresh tokens this many seconds before `exp`. */
  refreshSkewSeconds?: number;
  fetch?: typeof fetch;
  /** Injected for tests. */
  sleep?: (ms: number, signal?: AbortSignal) => Promise<void>;
  now?: () => number;
}

interface RequestOptions {
  method?: "GET" | "POST" | "PUT";
  body?: unknown;
  signal?: AbortSignal;
  query?: Record<string, string | number | null | undefined>;
}

const EMBED_PREFIX = "/api/v1/embed";

function defaultSleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(new Orch8EmbedError("aborted", "aborted"));
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(new Orch8EmbedError("aborted", "aborted"));
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function kindForStatus(status: number): Orch8ErrorKind {
  if (status === 401) return "unauthorized";
  if (status === 403) return "forbidden";
  if (status === 404) return "not_found";
  if (status === 409) return "conflict";
  if (status === 429) return "rate_limited";
  if (status >= 500) return "server";
  return "invalid";
}

function isRetryable(err: Orch8EmbedError): boolean {
  return err.kind === "server" || err.kind === "network" || err.kind === "rate_limited";
}

function normalizeHandlers(raw: unknown): HandlerInfo[] {
  if (!Array.isArray(raw)) return [];
  const out: HandlerInfo[] = [];
  for (const entry of raw) {
    if (typeof entry === "string" && entry) out.push({ name: entry });
    else if (entry && typeof entry === "object") {
      const e = entry as Record<string, unknown>;
      const name = typeof e.name === "string" ? e.name : typeof e.handler === "string" ? e.handler : "";
      if (!name) continue;
      out.push({
        name,
        label: typeof e.label === "string" ? e.label : undefined,
        description: typeof e.description === "string" ? e.description : undefined,
        default_params:
          e.default_params && typeof e.default_params === "object"
            ? (e.default_params as Record<string, unknown>)
            : undefined,
      });
    }
  }
  return out;
}

/**
 * Minimal typed client for the embed API. Handles bearer auth, token refresh
 * (proactive before `exp` and reactive on 401), and retry with exponential
 * backoff for idempotent requests.
 */
export class EmbedClient {
  readonly baseUrl: string;
  private token: string | null;
  private readonly provider: TokenProvider | null;
  private inflightToken: Promise<string> | null = null;
  private readonly retries: number;
  private readonly retryBaseMs: number;
  private readonly skew: number;
  private readonly fetchImpl: typeof fetch;
  private readonly sleep: (ms: number, signal?: AbortSignal) => Promise<void>;
  private readonly now: () => number;

  constructor(options: EmbedClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.token = options.token || null;
    this.provider = options.tokenProvider ?? null;
    this.retries = options.retries ?? 3;
    this.retryBaseMs = options.retryBaseMs ?? 300;
    this.skew = options.refreshSkewSeconds ?? 30;
    const f = options.fetch ?? (typeof fetch === "function" ? fetch : null);
    if (!f) throw new Orch8EmbedError("config", "fetch is not available");
    // Bind so a global fetch keeps its receiver.
    this.fetchImpl = f === globalThis.fetch ? f.bind(globalThis) : f;
    this.sleep = options.sleep ?? defaultSleep;
    this.now = options.now ?? (() => Date.now());
  }

  /** The token currently in use (may be null before the first request). */
  get currentToken(): string | null {
    return this.token;
  }

  private tokenIsFresh(token: string): boolean {
    const payload = decodeEmbedToken(token);
    if (!payload) return true; // opaque token: let the server decide
    return payload.exp - this.skew > this.now() / 1000;
  }

  /** Resolves the bearer token, calling the provider when needed. */
  async getToken(force: "unauthorized" | null = null): Promise<string> {
    if (!force && this.token && (this.tokenIsFresh(this.token) || !this.provider)) return this.token;
    if (!this.provider) {
      throw new Orch8EmbedError("config", "No embed token: set the `token` attribute or a `tokenProvider`.");
    }
    if (!this.inflightToken) {
      const reason = force ?? (this.token ? "expiring" : "initial");
      const provider = this.provider;
      this.inflightToken = (async () => {
        try {
          const next = await provider({ reason });
          if (typeof next !== "string" || !next) {
            throw new Orch8EmbedError("config", "tokenProvider returned an empty token");
          }
          this.token = next;
          return next;
        } finally {
          this.inflightToken = null;
        }
      })();
    }
    return this.inflightToken;
  }

  private url(path: string, query?: RequestOptions["query"]): string {
    const url = `${this.baseUrl}${EMBED_PREFIX}${path}`;
    if (!query) return url;
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(query)) {
      if (v !== undefined && v !== null && v !== "") params.set(k, String(v));
    }
    const qs = params.toString();
    return qs ? `${url}?${qs}` : url;
  }

  private async once<T>(opts: RequestOptions, path: string, token: string): Promise<T> {
    const headers: Record<string, string> = {
      Accept: "application/json",
      Authorization: `Bearer ${token}`,
    };
    if (opts.body !== undefined) headers["Content-Type"] = "application/json";
    let res: Response;
    try {
      res = await this.fetchImpl(this.url(path, opts.query), {
        method: opts.method ?? "GET",
        headers,
        body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
        signal: opts.signal,
        credentials: "omit",
        mode: "cors",
      });
    } catch (err) {
      if (opts.signal?.aborted) throw new Orch8EmbedError("aborted", "aborted");
      throw new Orch8EmbedError("network", err instanceof Error ? err.message : "network error");
    }
    const text = await res.text().catch(() => "");
    let json: unknown = undefined;
    if (text) {
      try {
        json = JSON.parse(text);
      } catch {
        json = undefined;
      }
    }
    if (!res.ok) {
      const envelope = (json as { error?: { code?: string; message?: string; details?: unknown } } | undefined)
        ?.error;
      const err = new Orch8EmbedError(
        kindForStatus(res.status),
        envelope?.message || res.statusText || `HTTP ${res.status}`,
        res.status,
        envelope?.code,
        envelope?.details,
      );
      const retryAfter = Number(res.headers.get("Retry-After"));
      if (Number.isFinite(retryAfter) && retryAfter > 0) {
        (err as { retryAfterMs?: number }).retryAfterMs = Math.min(retryAfter * 1000, 30_000);
      }
      throw err;
    }
    return json as T;
  }

  /** Low-level request against `/api/v1/embed{path}`. */
  async request<T>(path: string, opts: RequestOptions = {}): Promise<T> {
    const method = opts.method ?? "GET";
    const idempotent = method === "GET" || method === "PUT";
    let token = await this.getToken();
    let refreshed = false;
    for (let attempt = 0; ; attempt++) {
      try {
        return await this.once<T>(opts, path, token);
      } catch (e) {
        const err = e as Orch8EmbedError;
        if (err.kind === "unauthorized" && this.provider && !refreshed) {
          refreshed = true;
          token = await this.getToken("unauthorized");
          attempt--; // a refresh is not a retry
          continue;
        }
        if (!idempotent || !isRetryable(err) || attempt >= this.retries) throw err;
        const hinted = (err as { retryAfterMs?: number }).retryAfterMs;
        const backoff = Math.min(this.retryBaseMs * 2 ** attempt, 8000);
        const jitter = Math.random() * this.retryBaseMs;
        await this.sleep(hinted ?? backoff + jitter, opts.signal);
      }
    }
  }

  listRuns(params: { limit?: number; cursor?: string | null } = {}, signal?: AbortSignal): Promise<RunList> {
    return this.request<RunList>("/runs", { query: { limit: params.limit, cursor: params.cursor }, signal }).then(
      (r) => ({ items: Array.isArray(r?.items) ? r.items : [], next_cursor: r?.next_cursor ?? null }),
    );
  }

  getRun(id: string, signal?: AbortSignal): Promise<RunDetail> {
    return this.request<RunDetail>(`/runs/${encodeURIComponent(id)}`, { signal }).then((r) => ({
      ...r,
      steps: Array.isArray(r?.steps) ? r.steps : [],
    }));
  }

  startRun(body: StartRunBody, signal?: AbortSignal): Promise<{ id: string }> {
    return this.request<{ id: string }>("/runs", { method: "POST", body, signal });
  }

  listApprovals(signal?: AbortSignal): Promise<ApprovalList> {
    return this.request<ApprovalList>("/approvals", { signal }).then((r) => ({
      items: Array.isArray(r?.items) ? r.items : [],
    }));
  }

  resolveApproval(id: string, body: ResolveApprovalBody, signal?: AbortSignal): Promise<unknown> {
    return this.request(`/approvals/${encodeURIComponent(id)}`, { method: "POST", body, signal });
  }

  async listSequences(signal?: AbortSignal): Promise<SequenceList> {
    const raw = await this.request<unknown>("/sequences", { signal });
    const obj = (raw && typeof raw === "object" ? raw : {}) as Record<string, unknown>;
    const items = (Array.isArray(raw) ? raw : Array.isArray(obj.items) ? obj.items : []) as SequenceSummary[];
    return { items, handlers: normalizeHandlers(obj.handlers) };
  }

  /** Returns the raw response; see {@link unwrapDefinition}. */
  getSequence(name: string, signal?: AbortSignal): Promise<unknown> {
    return this.request<unknown>(`/sequences/${encodeURIComponent(name)}`, { signal });
  }

  putSequence(name: string, definition: SequenceDefinitionJson, signal?: AbortSignal): Promise<unknown> {
    return this.request<unknown>(`/sequences/${encodeURIComponent(name)}`, {
      method: "PUT",
      body: definition,
      signal,
    });
  }

  async getTheme(signal?: AbortSignal): Promise<EmbedTheme> {
    const raw = await this.request<Partial<EmbedTheme>>("/theme", { signal });
    return {
      css_vars: raw?.css_vars && typeof raw.css_vars === "object" ? raw.css_vars : {},
      logo_url: typeof raw?.logo_url === "string" ? raw.logo_url : null,
      hide_badge: raw?.hide_badge === true,
    };
  }
}

/** Accepts `{ definition: {...} }` or a bare definition; returns null if neither has `blocks`. */
export function unwrapDefinition(raw: unknown): SequenceDefinitionJson | null {
  if (!raw || typeof raw !== "object") return null;
  const obj = raw as Record<string, unknown>;
  const inner = obj.definition && typeof obj.definition === "object" ? (obj.definition as Record<string, unknown>) : obj;
  if (!Array.isArray(inner.blocks)) return null;
  return inner as SequenceDefinitionJson;
}
